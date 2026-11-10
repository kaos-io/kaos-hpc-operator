"""compose-and-validate: what the first DAG step's container runs.

Invoked in-cluster as `python -m kubecore.compose`. Takes Hydra override
tokens (argv) plus the optional ADVANCED override YAML, composes the config
tree + overrides via the Hydra Compose API, validates, and emits the single
resolved params.yaml that determines the entire experiment. Downstream steps
receive this file's content as an input parameter; they never see override
tokens.

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
- ADVANCED override touching platform.* -> hard error (below)
- params.yaml too large         -> explicit size check (>200KB)
- a data input a step reads does not resolve (no such dataset or version, lakeFS
  unreachable, no credentials)  -> hard error naming the form field (below)

The run's data lock: every data input a step reads (today the `dataset` leaf, when a
step declares reads=["dataset"]) is resolved ONCE here, with kaos-lib, to a commit,
and written as --lock-output in kaos-lib's lock format. Every step receives it as
$KAOS_LIB_LOCK, an HPC step as its job's dataset commit, so the whole run reads one
commit whatever happens to the dataset meanwhile. Nothing is skipped: a reference
that cannot be resolved fails the run here. --offline (a laptop, set by kaosctl
only, never in a rendered pipeline) resolves nothing and writes an empty lock.
"""

import argparse
import json
import re
import sys
from enum import Enum
from pathlib import Path

import yaml
from omegaconf import DictConfig, ListConfig, OmegaConf
from omegaconf.nodes import BooleanNode, EnumNode, FloatNode, IntegerNode, StringNode

from kubecore import derive_tree

MAX_PARAMS_BYTES = 200 * 1024
MAX_LOCK_BYTES = 16 * 1024
DATASET_FIELD = "dataset"          # the top-level config leaf and form field that names the run's dataset
EMPTY_LOCK = '{"version":1,"reads":[]}'

HERE = Path(__file__).parent  # kubecore/
DEFAULT_OUTPUT = HERE.parent / "out" / "params.yaml"


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
    """Enum members (from structured schemas) -> their plain values."""
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, Enum):
        return value.value
    return value


def compose_and_validate(overrides: list, advanced: str = "", required: list = None,
                         render_defaults: dict = None) -> str:
    """Returns the resolved params.yaml text."""
    cfg = derive_tree.compose_tree(order_overrides(overrides, render_defaults or {}))

    if advanced.strip():
        parsed = yaml.safe_load(advanced)
        if isinstance(parsed, dict) and "platform" in parsed:
            raise ComposeError("ADVANCED override may not modify platform.*")
        # ADVANCED override merged LAST. Struct mode: unknown keys fail.
        cfg = OmegaConf.merge(cfg, OmegaConf.create(parsed))

    resolved = _plain(OmegaConf.to_container(cfg, resolve=True))
    for section in required or []:
        if section not in resolved:
            raise ComposeError(
                f"a step declares reads=['{section}'] but the composed config has no "
                f"'{section}' section (top-level sections: {', '.join(resolved)})"
            )

    text = OmegaConf.to_yaml(OmegaConf.create(resolved), resolve=True)
    if len(text.encode()) > MAX_PARAMS_BYTES:
        raise ComposeError(
            f"resolved params.yaml is {len(text.encode())} bytes — exceeds the "
            f"{MAX_PARAMS_BYTES}-byte Argo parameter budget; move bulk data out of config"
        )
    return text


def data_lock(resolved: dict, required: list, offline: bool = False, datasets=None, lock=None) -> str:
    """The run's data lock (JSON text): the commit of every data input a step reads.

    `resolved` is the composed config, `required` the sections steps declared with reads=. Only a data
    input some step reads is resolved: a config that carries a `dataset` no step reads costs nothing and
    cannot fail the run. `datasets`/`lock` default to kaos-lib's modules (tests pass their own).
    """
    wanted = resolved.get(DATASET_FIELD) if DATASET_FIELD in (required or []) else None
    if offline or wanted in (None, ""):
        return EMPTY_LOCK
    if not isinstance(wanted, str):
        raise ComposeError(f"{DATASET_FIELD}: expected <dataset>@<version> (for example dataset-default@latest), "
                           f"got {wanted!r}")
    if datasets is None or lock is None:
        try:
            from kaos_lib import datasets as _datasets
            from kaos_lib import lock as _lock
        except ImportError as exc:
            raise ComposeError(f"{DATASET_FIELD}: cannot resolve {wanted!r}: kaos-lib is not installed in the "
                               f"compose image ({exc}). The platform's compose image installs it.") from None
        datasets, lock = datasets or _datasets, lock or _lock
    try:
        datasets.resolve(datasets.DatasetRef.parse(wanted))
    except Exception as exc:   # no such dataset or version, lakeFS unreachable, no credentials: all end the run
        raise ComposeError(f"{DATASET_FIELD}: cannot use {wanted!r}: {exc}") from None
    text = lock.current().to_json()
    if len(text.encode()) > MAX_LOCK_BYTES:
        raise ComposeError(f"the run's data lock is {len(text.encode())} bytes, above the {MAX_LOCK_BYTES}-byte limit")
    return text


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Compose and validate the experiment config.")
    parser.add_argument("--advanced", default="", help="ADVANCED override YAML, merged last.")
    parser.add_argument("--require", action="append", default=[],
                        help="Top-level section a step declared via reads= (repeatable).")
    parser.add_argument("--render-defaults", default="{}",
                        help="JSON manifest {override_key: render-time default} for elision.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--lock-output", default="",
                        help="Where to write the run's data lock (JSON). Without it nothing is resolved.")
    parser.add_argument("--offline", action="store_true",
                        help="Resolve nothing and write an empty data lock (a laptop without the platform).")
    parser.add_argument("overrides", nargs="*", help="Hydra override tokens (key=value).")
    args = parser.parse_args(argv)

    try:
        text = compose_and_validate(
            args.overrides, args.advanced, args.require, json.loads(args.render_defaults)
        )
        lock_text = ""
        if args.lock_output:
            if args.offline:
                print("compose-and-validate: OFFLINE - no data input was resolved; the data lock is empty",
                      file=sys.stderr)
            lock_text = data_lock(yaml.safe_load(text) or {}, args.require, args.offline)
    except Exception as exc:  # any composition failure = hard error, pre-GPU
        message = str(exc)
        if exc.__cause__ is not None:  # e.g. enum ValidationError inside Hydra's wrapper
            message += f"\n  cause: {exc.__cause__}"
        print(f"compose-and-validate FAILED: {message}", file=sys.stderr)
        sys.exit(1)

    out = Path(args.output)
    out.parent.mkdir(exist_ok=True)
    out.write_text(text)
    print(f"wrote {out} ({len(text.encode())} bytes)")
    if args.lock_output:
        lock_out = Path(args.lock_output)
        lock_out.parent.mkdir(exist_ok=True)
        lock_out.write_text(lock_text)
        print(f"wrote {lock_out} ({lock_text})")


if __name__ == "__main__":
    main()
