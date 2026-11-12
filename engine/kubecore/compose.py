"""compose-and-validate: what the first DAG step's container runs.

Invoked in-cluster as `python -m kubecore.compose`. Takes Hydra override
tokens (argv) plus the optional ADVANCED override YAML, composes the config
tree + overrides via the Hydra Compose API, validates, pins every declared
input, and writes each step's context (docs/step-contract.md): the resolved
configuration, the step's inputs at their commit, and what it produces.
Downstream steps receive that one value; they never see override tokens.

Override semantics (group swap vs touched leaves):
- GROUP tokens (key contains '/', or names a top-level config/<group>/
  directory) are applied FIRST.
- A scalar-leaf token whose submitted value equals its RENDER-TIME
  default (--render-defaults manifest, baked into the WFT args) is
  DROPPED — an untouched form field follows whatever group you
  selected. A field you explicitly changed is applied AFTER the
  groups, so it wins.
Without this elision a full form submission would freeze every leaf at
the original defaults and silently revert any group swap.

Override typing: a changed leaf keeps the type its config leaf DECLARES (the
structured-config node type, else the type of its default in the tree
composed with the group tokens). A string leaf is always Hydra-quoted, so a
form value such as `3e286280`, `1234`, `true` or `null` stays a string
instead of being parsed by its look (3e286280 -> inf). See _typed_override.

Failure modes concentrated here, all before any GPU time:
- unknown/typo'd override key   -> Hydra strict mode rejects it
- type/enum-invalid value       -> structured-config validation, or a
                                   ComposeError for an int/float/bool leaf
- step reads= section missing   -> --require check below
- ADVANCED override with unknown keys -> struct-mode merge rejects it
- a declared input that does not resolve (empty, no such dataset or version,
  lakeFS unreachable, no credentials) -> hard error naming the input (below)
- the ADVANCED override is over 16 KiB, or a dataset reference has a version over 255 characters or a
  repository id over 63 -> hard error
- the app's config uses a resolver, or defines a key the platform reserves -> hard error naming it
- a step's context is over its size cap -> hard error naming the step,
  the size and the limit

Pinned inputs: every config value a step names in inputs= holds a dataset reference
(`<dataset>@<version>`; no version means `latest`, the head of the dataset's `main`).
Each is resolved ONCE here, with kaos-lib, to a commit. The step's context carries its
own inputs as a ledger (version 2, named reads), and --ledger-output holds all of them:
the first part of the run's ledger. The whole run reads those commits whatever happens
to a dataset meanwhile. Nothing is skipped: a reference that cannot be resolved fails
the run here.

Config never reads the environment: a value that uses a resolver (`${oc.env:NAME}`) is refused, and the
config is resolved with an empty environment besides, because this pod holds the platform's credentials.

The run's own keys: `plan` and `pipeline-version` are the platform's; the ADVANCED override may carry
neither. Whether a run promotes a dataset it produces is the run form's `promote-<dataset>` field
(--promote); the step that produces it finds `promote` on that entry of its context.
"""

import argparse
import contextlib
import json
import os
import re
import sys
from enum import Enum
from pathlib import Path

import yaml
from omegaconf import DictConfig, ListConfig, OmegaConf
from omegaconf.nodes import BooleanNode, EnumNode, FloatNode, IntegerNode, StringNode

from kubecore import derive_tree
from kubecore.run_settings import (
    checkpoint_field,
    minutes,
    promote_field,
    promote_problem,
    time_problem,
    walltime_field,
)
from kubecore.step_contract import (
    CONTEXT_CAP,
    CONTEXT_VERSION,
    DATASET_PREFIX,
    LEDGER_VERSION,
    MAX_OVERRIDE,
    MAX_REF_VERSION,
    MAX_REPO,
    RESERVED_CONFIG_KEYS,
    RESULT_CAP,
    base_context_cap,
    base_cost,
    cost,
    dataset_name_problem,
    produced_repo,
    wire,
)

LATEST = "latest"                  # no version asked: the head of the dataset's main when the run starts
EXPERIMENT_SECTION = "experiment"  # config `experiment.name` names the run's experiment
PLAN_KEY = "plan"                  # the run's own key in the ADVANCED override
PLAN_PROMOTE = "promote"           # plan.promote.<dataset>: true
STEP_IMAGES_ENV = "KAOS_STEP_IMAGES"   # {"<step>": "<the image the run resolved for it>"}, set by the enhancer
# registry[:port]/repository[/...] then :tag or @digest. A reference without a registry and a repository is what
# the image record holds for a step whose image was never built (":<step>-latest").
# A tag, a digest, or both (a tag with its digest is a valid reference).
_IMAGE_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]*(:[0-9]+)?(/[A-Za-z0-9][A-Za-z0-9._-]*)+"
                        r"(:[A-Za-z0-9_][A-Za-z0-9._-]{0,127}(@sha256:[0-9a-f]{64})?|@sha256:[0-9a-f]{64})\Z")



class ComposeError(Exception):
    pass


def _normalize_override_token(token: str) -> str:
    """Strip surrounding whitespace/control chars off an override token.

    Submitted form values travel UI -> Argo parameter -> argv, and that
    plumbing can attach a trailing CR/newline/space the submitter cannot see.
    A trailing '\\r' makes Hydra's override lexer throw
    LexerNoViableAltException on a perfectly ordinary token — live #867:
    `train.epochs=3\\r` failed EVERY numeric-tuning run with the caret
    pointing one past the digit. Surrounding whitespace is never semantic in
    a key=value token, so strip it from both the whole token and the value.
    """
    token = token.strip()
    key, sep, value = token.partition("=")
    if not sep:
        return token
    return f"{key.strip()}={value.strip()}"


def _hydra_quote(value: str) -> str:
    """Wrap a string in double quotes per Hydra's override grammar.

    Inside a quoted value Hydra treats a backslash literally EXCEPT in a run of
    backslashes that precedes a quote or the end of the string, where each one
    must be doubled; an embedded double quote is escaped as \\". Doubling every
    backslash (the old behaviour) corrupted `C:\\data` into `C:\\\\data`."""
    escaped = re.sub(
        r'(\\*)("|$)',
        lambda m: m.group(1) * 2 + ('\\"' if m.group(2) else ""),
        value,
    )
    return f'"{escaped}"'


def _is_quoted(value: str) -> bool:
    return len(value) >= 2 and value[0] in "\"'" and value[-1] == value[0]


def _leaf_kind(base_cfg, key: str):
    """The declared type of the config leaf `key` in the composed base config.

    Returns one of "str", "int", "float", "bool", "enum", "list", "dict", or
    None when the key does not exist. Source of truth, in order:
      1. the node's DECLARED type (structured-config schema: StringNode,
         IntegerNode, FloatNode, BooleanNode, EnumNode) — holds even when the
         default is None (Optional[int] stays an int);
      2. for untyped yaml leaves (AnyNode), the type of the DEFAULT value.
    An untyped leaf whose default is null or ??? carries no type, so it is a
    string: a submitted form value is text, and we never guess a type from
    the value's look."""
    if base_cfg is None:
        return None
    parent_key, _, leaf = key.rpartition(".")
    parent = OmegaConf.select(base_cfg, parent_key) if parent_key else base_cfg
    if not isinstance(parent, DictConfig):
        return None
    node = parent._get_node(leaf)
    if node is None:
        return None
    if isinstance(node, ListConfig):
        return "list"
    if isinstance(node, DictConfig):
        return "dict"
    for cls, kind in ((BooleanNode, "bool"), (IntegerNode, "int"), (FloatNode, "float"),
                      (EnumNode, "enum"), (StringNode, "str")):
        if isinstance(node, cls):
            return kind
    default = node._value()
    if isinstance(default, bool):  # before int: bool is an int subclass
        return "bool"
    if isinstance(default, int):
        return "int"
    if isinstance(default, float):
        return "float"
    return "str"  # str, Enum value, None (null) or ??? (missing)


def _typed_override(token: str, base_cfg) -> str:
    """Render a scalar override so Hydra yields the value in the type its
    config leaf DECLARES — never the type the value happens to look like.

    Form values are all text (Argo parameters carry no type), and Hydra's
    override grammar parses a BARE value by its look: `probe.label=3e286280`
    became float inf (written `label: .inf`), `name=1234` an int, `tag=true` a
    bool, `x=null` None. So the type comes from the composed base config
    (see _leaf_kind):
      - str / enum / untyped-null leaf -> always Hydra-quoted (and escaped);
      - int / float / bool leaf        -> emitted as a canonical bare literal
        (float leaves keep a float: `lr=1` -> `lr=1.0`); a value that is not
        a valid literal of that type fails compose with a clear error;
      - list / dict leaf               -> passed through (Hydra list/dict
        syntax is the only way to write one);
      - unknown key                    -> quoted as a string (a typo still
        fails Hydra's strict mode; a `+new` key gets a string).
    Already-quoted and empty values, group swaps (key has '/'), deletions
    (`~key`) and hydra.* keys pass through unchanged.

    HISTORY (#867): quoting originally shipped only as a hotfix on an app's
    VENDORED copy and was lost when the app re-synced to template HEAD — it
    lives here now so the template is the single source of truth. The first
    version quoted only values failing a bare-token regex, which let numeric-
    looking strings through unquoted (the 3e286280 -> inf bug)."""
    key, sep, value = token.partition("=")
    if not sep or _is_group_key(key) or key.startswith("~"):
        return token  # not a scalar leaf assignment (group swap / deletion)
    lookup = key.lstrip("+")
    if lookup.startswith("hydra.") or value == "" or _is_quoted(value):
        return token  # hydra internals, empty, or already quoted
    kind = _leaf_kind(base_cfg, lookup)
    if kind in ("list", "dict"):
        return token
    if kind == "bool":
        if value.lower() not in ("true", "false"):
            raise ComposeError(f"{lookup}={value!r}: '{lookup}' is a bool; use true or false")
        return f"{key}={value.lower()}"
    if kind == "int":
        try:
            return f"{key}={int(value, 10)}"
        except ValueError:
            raise ComposeError(f"{lookup}={value!r}: '{lookup}' is an int") from None
    if kind == "float":
        try:
            return f"{key}={float(value)!r}"
        except ValueError:
            raise ComposeError(f"{lookup}={value!r}: '{lookup}' is a float") from None
    return f"{key}={_hydra_quote(value)}"


def _is_group_key(key: str) -> bool:
    """A config-group swap: a nested group path (`train/optimizer`) or a
    top-level group (`model`, i.e. a `config/model/` directory)."""
    key = key.lstrip("+~")
    return "/" in key or (key != "" and (derive_tree.CONFIG_DIR / key).is_dir())


def _split_overrides(overrides: list, render_defaults: dict):
    """(group tokens, changed scalar tokens): tokens whitespace-normalized
    (#867: invisible trailing CR from the submit plumbing); untouched scalar
    leaves (value == render-time default) elided."""
    groups, changed = [], []
    for token in overrides:
        token = _normalize_override_token(token)
        key, _, value = token.partition("=")
        if _is_group_key(key):
            groups.append(token)
        elif render_defaults and key in render_defaults and value == render_defaults[key]:
            continue  # untouched -> follows the selected group
        else:
            changed.append(token)
    return groups, changed


def order_overrides(overrides: list, render_defaults: dict, base_cfg=None) -> list:
    """Groups first; untouched scalar leaves elided (see module docstring);
    changed scalar values rendered in their declared type (_typed_override).

    `base_cfg` is the tree composed with the group tokens only — the type
    source. When omitted it is composed here."""
    groups, changed = _split_overrides(overrides, render_defaults)
    if changed and base_cfg is None:
        base_cfg = derive_tree.compose_tree(groups)
    return groups + [_typed_override(token, base_cfg) for token in changed]


def _plain(value):
    """Enum members (from structured schemas) -> their plain values; keys -> text (the context is JSON)."""
    if isinstance(value, dict):
        return {k if isinstance(k, str) else str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, Enum):
        return value.value
    return value


def check_advanced(advanced: str):
    """The ADVANCED override, parsed, with the run's own keys refused.

    `plan` and `pipeline-version` are the platform's: they aim the run and are never part of the app's
    config. This version acts on neither, so a key it would silently ignore fails the run instead.
    `plan.promote` was the way to promote a dataset before the run form had a field for it.
    """
    if len(advanced.encode()) > MAX_OVERRIDE:
        raise ComposeError("the override is too large; put the values in the app's config")
    parsed = yaml.safe_load(advanced) if advanced.strip() else None
    if not isinstance(parsed, dict):
        return parsed
    for key in RESERVED_CONFIG_KEYS:
        if key != PLAN_KEY and key in parsed:
            raise ComposeError(f"'{key}' is reserved for the platform and is not available yet; remove it "
                               f"from the ADVANCED override")
    plan = parsed.pop(PLAN_KEY, None)
    if plan is None:
        return parsed
    if isinstance(plan, dict) and PLAN_PROMOTE in plan:
        raise ComposeError(f"'{PLAN_KEY}.{PLAN_PROMOTE}' is no longer read. Tick the field "
                           f"{promote_field('<dataset>')} of the run form, one for each dataset a step produces.")
    raise ComposeError(f"'{PLAN_KEY}' is reserved for the platform and is not available yet; remove it from the "
                       f"ADVANCED override")


def run_promote(steps: list, promotes: list) -> dict:
    """{dataset: True or False} from the run form's `promote-<dataset>` fields, checked before any step starts."""
    produced = sorted({e["name"] for step in steps for e in step["produces"] if e["type"] == "dataset"})
    flags = {}
    for name, value in _pairs(promotes).items():
        if name not in produced:
            raise ComposeError(f"{promote_field(name)}: no step of this pipeline produces the dataset '{name}' "
                               f"(it produces: {', '.join(produced) or 'none'})")
        problem = promote_problem(promote_field(name), value)
        if problem:
            raise ComposeError(problem)
        flags[name] = value.strip().lower() == "true"
    return flags


@contextlib.contextmanager
def _no_environment():
    """The config is resolved with an empty environment. The compose pod holds the platform's credentials
    (the lake's key pair among them); whatever a config value might do, it cannot read them here. The
    environment is back when the block ends: the run's inputs are pinned with it."""
    saved = dict(os.environ)
    os.environ.clear()
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def compose_config(overrides: list, advanced=None, required: list = None, render_defaults: dict = None) -> dict:
    """The resolved config as plain values. `advanced` is the ADVANCED override, parsed (check_advanced)."""
    cfg = derive_tree.compose_tree(order_overrides(overrides, render_defaults or {}))

    if advanced:
        # ADVANCED override merged LAST. Struct mode: unknown keys fail.
        cfg = OmegaConf.merge(cfg, OmegaConf.create(advanced))

    derive_tree.refuse_resolvers(OmegaConf.to_container(cfg, resolve=False))
    with _no_environment():
        resolved = _plain(OmegaConf.to_container(cfg, resolve=True))
    derive_tree.refuse_reserved_keys(resolved)
    for section in required or []:
        if section not in resolved:
            raise ComposeError(
                f"a step declares reads=['{section}'] but the composed config has no "
                f"'{section}' section (top-level sections: {', '.join(resolved)})"
            )
    return resolved


def check_step_images(raw) -> None:
    """Refuse the run when a step has no real image, before any step starts.

    A step added to pipeline.py appears in the released pipeline before its first image is built; until then
    its image reference has no registry and no repository, and the run would fail later with an image pull
    error (or, on HPC, after a queue wait). `raw` is $KAOS_STEP_IMAGES; absent means nothing to check.
    """
    if not raw:
        return
    try:
        images = json.loads(raw)
        if not isinstance(images, dict):
            raise ValueError("not a JSON object")
    except ValueError as exc:
        raise ComposeError(f"cannot read the steps' images ({STEP_IMAGES_ENV}: {exc})") from None
    # Every step is checked, also one a `when=` would skip in this run: whether it is skipped is not known
    # here, and a pipeline whose step has never been built is not a pipeline to start.
    bad = {step: ref for step, ref in images.items()
           if not isinstance(ref, str) or not _IMAGE_REF.match(ref.strip())}
    if bad:
        listed = ", ".join(f"{step} ({ref!r})" for step, ref in sorted(bad.items()))
        raise ComposeError(
            f"no image has been built yet for: {listed}. The platform builds a step's image after you push; "
            "wait for the app's build to finish (or see why it failed), then run again"
        )


_COMMIT = re.compile(r"^[0-9a-f]{64}$")
LAKEFS_TIMEOUT_S = 30     # per request; kaos-lib retries a failing GET a few times, so a dead lakeFS ends the run in minutes


def _kaos_lib():
    """kaos-lib's datasets module and a lakeFS client with a short per-request timeout."""
    try:
        from kaos_lib import datasets
        from kaos_lib.lakefs import LakeFS
    except ImportError as exc:
        raise ComposeError(f"cannot resolve the run's inputs: kaos-lib is not installed in the compose image "
                           f"({exc}). The platform's compose image installs it.") from None
    try:
        client = LakeFS.connect()
        client.timeout = LAKEFS_TIMEOUT_S
    except Exception as exc:   # no endpoint or no credentials: the run cannot read its data either
        raise ComposeError(f"cannot reach the project's data: {exc}") from None
    return datasets, client


def pin_inputs(resolved: dict, steps: list, datasets=None, client=None) -> dict:
    """{input name: its ledger read} for every input a step declares, each resolved once to a commit.

    An input is the name of a top-level config value that holds a dataset reference,
    `[<project>/]dataset-<name>[@<version>]`. The version comes from the run's config (the form), never
    from step code; without one it is `latest`, the head of the dataset's `main` now. Anything that cannot
    be pinned ends the run here, naming the input: an empty value, a value that is no reference, a dataset
    or version that does not exist, a store that does not answer. `datasets`/`client` default to kaos-lib's
    (tests pass their own).
    """
    names = [name for step in steps for name in step["inputs"]]
    names = list(dict.fromkeys(names))
    if not names:
        return {}
    reads = {}
    for name in names:
        users = ", ".join(step["name"] for step in steps if name in step["inputs"])
        wanted = resolved.get(name)
        if not isinstance(wanted, str) or not wanted.strip():
            raise ComposeError(
                f"input '{name}' (step {users}): the config value '{name}' is "
                f"{'empty' if wanted in (None, '') or isinstance(wanted, str) else 'not a dataset reference'}. "
                f"Pick a dataset and a version for '{name}' on the form, for example dataset-default@{LATEST}")
        wanted = wanted.strip()
        reference, _, version = wanted.partition("@")
        repo = reference.rpartition("/")[2]
        if len(version) > MAX_REF_VERSION or len(repo) > MAX_REPO:
            raise ComposeError(
                f"input '{name}' (step {users}): the reference is not valid: "
                + (f"the version is {len(version)} characters, at most {MAX_REF_VERSION}" if len(version) > MAX_REF_VERSION
                   else f"the repository id '{repo}' is {len(repo)} characters, at most {MAX_REPO}"))
        if datasets is None:
            datasets, client = _kaos_lib()
        try:
            ref = datasets.DatasetRef.parse(wanted if "@" in wanted else f"{wanted}@{LATEST}")
            if not str(ref.repo).startswith(DATASET_PREFIX):
                raise ValueError(f"{ref.repo} is not a dataset: a dataset is a repository named {DATASET_PREFIX}<name> "
                                 f"(for example {DATASET_PREFIX}default@{LATEST})")
            ref, source = datasets.source(ref, client)   # an owner that is the run's own project is dropped
            commit = datasets.resolve(ref, source)
        except Exception as exc:   # no such dataset or version, lakeFS unreachable, no credentials: all end the run
            raise ComposeError(f"input '{name}' (step {users}): cannot use {wanted!r}: {exc}") from None
        if not isinstance(commit, str) or not _COMMIT.match(commit):
            raise ComposeError(f"input '{name}' (step {users}): cannot use {wanted!r}: the data store answered "
                               f"with {commit!r} where a commit id was expected")
        reads[name] = {"name": name, "repo": ref.repo, "owner": ref.owner or None, "ref": ref.version,
                       "commit": commit}
    return reads


def ledger(reads: list) -> dict:
    """A ledger as compose writes it: the pinned reads, and no writes yet."""
    return {"version": LEDGER_VERSION, "reads": reads, "writes": []}


def experiment_of(resolved: dict, app: str) -> str:
    """The experiment a run belongs to: config `experiment.name`, else the app's name."""
    section = resolved.get(EXPERIMENT_SECTION)
    name = section.get("name") if isinstance(section, dict) else None
    return name.strip() if isinstance(name, str) and name.strip() else app


def step_params(step: dict, resolved: dict) -> dict:
    """What a step gets of the run's config: the sections it names in reads=, in that order."""
    return {section: resolved[section] for section in step["reads"] if section in resolved}


def _pairs(items: list) -> dict:
    """{step: value} from the repeated `--walltime step=value` arguments."""
    return {key: value for key, _, value in (item.partition("=") for item in items)}


def run_settings(steps: list, walltimes: list, checkpoints: list) -> dict:
    """What the run form says about each step's time (`<step>-walltime`, `<step>-checkpoint-every`), checked
    here before any step starts: {step: {"walltime": minutes, "checkpoint-every": minutes}} for the steps that
    have such a field. The refusal names the field."""
    known = {step["name"] for step in steps}
    settings = {}
    for key, flag, field in (("walltime", walltimes, walltime_field), ("checkpoint-every", checkpoints, checkpoint_field)):
        for step, value in _pairs(flag).items():
            if step not in known:
                raise ComposeError(f"{field(step)}: the pipeline has no step '{step}'")
            problem = time_problem(field(step), value)
            if problem:
                raise ComposeError(problem)
            settings.setdefault(step, {})[key] = minutes(value)
    return settings


def base_context(step: dict, run: str, app: str, project: str, experiment: str, params: dict, reads: dict,
                 promote: dict = None, checkpoint_every: int = 0) -> dict:
    """A step's context without `upstream` (the DAG task argument adds it), in the contract's field order.
    `params` is the step's own (step_params)."""
    produces = []
    for entry in step["produces"]:
        produced = {"name": entry["name"], "type": entry["type"],
                    "repo": produced_repo(app, entry["type"], entry["name"])}
        if entry["type"] == "dataset" and (promote or {}).get(entry["name"]):
            produced["promote"] = True
        produces.append(produced)
    context = {
        "version": CONTEXT_VERSION, "run": run, "app": app, "project": project, "step": step["name"],
        "kind": step["kind"], "experiment": experiment, "params": params,
        "ledger": ledger([reads[name] for name in step["inputs"] if name in reads]),
        "produces": produces,
    }
    if checkpoint_every:     # only a step that declares checkpoint_every= has it (minutes)
        context["checkpoint_every"] = checkpoint_every
    return context


def step_contexts(steps: list, run: str, app: str, project: str, resolved: dict, reads: dict,
                  promote: dict = None, settings: dict = None) -> dict:
    """{step: its base context in wire form}, each within its cap.

    `params` holds the config sections the step names in reads=. The cap is checked on the real text,
    before any step starts: a step's context may cost
    96 KiB less one full result (8 KiB) for each step it needs, so whatever those steps hand back, the
    step's definition stays under what a pod accepts and the step starts.
    """
    for step in steps:
        for entry in step["produces"]:
            problem = entry["type"] == "dataset" and dataset_name_problem(app, step["name"], entry["name"])
            if problem:
                raise ComposeError(problem)
    promote = dict(promote or {})
    experiment = experiment_of(resolved, app)
    out = {}
    for step in steps:
        try:
            params = step_params(step, resolved)
            text = wire(base_context(step, run, app, project, experiment, params, reads, promote,
                                      (settings or {}).get(step["name"], {}).get("checkpoint-every", 0)))
        except (TypeError, ValueError) as exc:   # a value JSON cannot hold (inf, nan)
            raise ComposeError(f"the resolved config cannot be handed to the steps: {exc}") from None
        cap, size = base_context_cap(len(step["needs"])), base_cost(text, step["needs"])
        if size > cap:
            raise ComposeError(
                f"step '{step['name']}': its context costs {size} bytes; the limit is {cap} "
                f"({CONTEXT_CAP} less {RESULT_CAP} for each of the {len(step['needs'])} steps it needs). The config "
                f"sections it reads ({', '.join(params) or 'none'}) are {cost(wire(params))} of them. Move large "
                f"values out of the config into a dataset or an artifact, or read fewer sections")
        out[step["name"]] = text
    return out


def _steps(raw: str) -> list:
    """The pipeline's steps as authoring wrote them into --steps."""
    try:
        steps = json.loads(raw)
        if not isinstance(steps, list) or not all(
                isinstance(s, dict) and isinstance(s.get("name"), str) and isinstance(s.get("kind"), str)
                and isinstance(s.get("reads"), list)
                and isinstance(s.get("needs"), list)
                and isinstance(s.get("inputs"), list) and isinstance(s.get("produces"), list) for s in steps):
            raise ValueError("not a list of steps")
    except ValueError as exc:
        raise ComposeError(f"cannot read the pipeline's steps (--steps: {exc})") from None
    return steps


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Compose and validate the experiment config.")
    parser.add_argument("--advanced", default="", help="ADVANCED override YAML, merged last.")
    parser.add_argument("--require", action="append", default=[],
                        help="Top-level section a step declared via reads= (repeatable).")
    parser.add_argument("--render-defaults", default="{}",
                        help="JSON manifest {override_key: render-time default} for elision.")
    parser.add_argument("--output", default="",
                        help="Where to write the composed config as YAML (kaosctl's local run reads it); "
                             "nothing is written without it.")
    parser.add_argument("--steps", default="",
                        help="The pipeline's steps (JSON): name, kind, reads, inputs, needs, produces. With it, every step's "
                             "context is written; --run, --app, --project and --context-dir are then required.")
    parser.add_argument("--run", default="", help="The run's name.")
    parser.add_argument("--app", default="", help="The app.")
    parser.add_argument("--project", default="", help="The project.")
    parser.add_argument("--context-dir", default="", help="Where to write each step's context: <dir>/<step>.json.")
    parser.add_argument("--walltime", action="append", default=[], metavar="STEP=VALUE",
                        help="The run form's <step>-walltime (repeatable).")
    parser.add_argument("--checkpoint-every", action="append", default=[], metavar="STEP=VALUE",
                        help="The run form's <step>-checkpoint-every (repeatable).")
    parser.add_argument("--promote", action="append", default=[], metavar="DATASET=VALUE",
                        help="The run form's promote-<dataset>, true or false (repeatable).")
    parser.add_argument("--settings-output", default="",
                        help="Where to write the numbers the form gave each step: {step: {walltime, checkpoint-every}}.")
    parser.add_argument("--ledger-output", default="", help="Where to write the run's ledger: every pinned input.")
    parser.add_argument("overrides", nargs="*", help="Hydra override tokens (key=value).")
    args = parser.parse_args(argv)
    if args.steps:
        missing = [flag for flag, value in (("--run", args.run), ("--app", args.app), ("--project", args.project),
                                            ("--context-dir", args.context_dir)) if not value]
        if missing:
            parser.error(f"--steps needs {', '.join(missing)}")

    contexts, reads, settings = {}, {}, {}
    try:
        check_step_images(os.environ.get(STEP_IMAGES_ENV))
        advanced = check_advanced(args.advanced)
        resolved = compose_config(args.overrides, advanced, args.require, json.loads(args.render_defaults))
        if args.steps:
            steps = _steps(args.steps)
            settings = run_settings(steps, args.walltime, args.checkpoint_every)
            promote = run_promote(steps, args.promote)
            reads = pin_inputs(resolved, steps)
            contexts = step_contexts(steps, args.run, args.app, args.project, resolved, reads, promote, settings)
    except Exception as exc:  # any composition failure = hard error, pre-GPU
        message = str(exc)
        if exc.__cause__ is not None:  # e.g. enum ValidationError inside Hydra's wrapper
            message += f"\n  cause: {exc.__cause__}"
        print(f"compose-and-validate FAILED: {message}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        text = OmegaConf.to_yaml(OmegaConf.create(resolved), resolve=True)
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        print(f"wrote {out} ({len(text.encode())} bytes)")
    for name, read in reads.items():
        print(f"input {name}: {read['repo']}@{read['ref']} is commit {read['commit']}")
    for step, body in contexts.items():
        target = Path(args.context_dir) / f"{step}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)
        print(f"wrote {target} ({len(body)} bytes)")
    if args.settings_output:
        Path(args.settings_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.settings_output).write_text(wire(settings))
        print(f"wrote {args.settings_output}")
    if args.ledger_output:
        Path(args.ledger_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.ledger_output).write_text(wire(ledger(list(reads.values()))))
        print(f"wrote {args.ledger_output}")


if __name__ == "__main__":
    main()
