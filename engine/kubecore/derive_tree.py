"""Platform-owned form derivation: Hydra config tree -> workflow form.

The single source of truth for the tunable surface is the experiment-scoped
config/ tree; this module composes it (Hydra Compose API) and derives the Argo
submit form with the structural rule — no allowlists:

  config GROUP   -> ONE dropdown parameter
                    (options = the yaml files in the group dir,
                     default = the defaults-list choice)
  SCALAR LEAF    -> one form field; dotted path mapped dots->dashes
                    (train.optimizer.lr -> train-optimizer-lr) with a
                    hard-error collision check
  COMPLEX NODE   -> (lists, class_name+params structures) NOT
                    flattened; changed via group selection or the
                    ADVANCED override only

Booleans are plain values: a bool leaf derives as a form field whose default is
"true"/"false" and whose override token Hydra parses back to a real bool. No
enum workaround.

Each derived entry also carries its Hydra override token
(`train.epochs={{workflow.parameters.train-epochs}}`) — these become the
compose step's args — plus a RENDER-DEFAULTS manifest
({override_key: rendered_default}) baked into the compose step's args so it can
elide untouched leaves at submit time: a scalar whose submitted value equals its
render-time default is dropped, letting a group swap actually swap (group tokens
apply first; only explicitly changed leaves override the selected group).

Structured-config schemas: sections listed in SCHEMA_SECTIONS are validated by
dataclasses registered on Hydra's ConfigStore — bad values
(quantization.mode=banana) hard-fail at compose time, and Enum-typed fields
derive as form dropdowns.
"""

import os
import re
import stat
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import yaml
from hydra import compose, initialize_config_dir
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from kubecore.step_contract import OLD_APP_NOTICE, REMOVED_CONFIG_KEYS, RESERVED_CONFIG_KEYS

# Hera is a RENDER-TIME dependency (form derivation only). The compose step
# runs at RUN time in a lean image without Hera, and only calls compose_tree()
# — so Hera is imported lazily inside derive(), never at module load.

HERE = Path(__file__).parent  # kubecore/


class _LazyConfigDir(os.PathLike):
    """ENGINE PATCH (D-79): the config tree dir, resolved at USE time from KUBECORE_CONFIG_DIR.

    Upstream pins CONFIG_DIR to <repo>/config at import. The platform engine lives outside the app
    repo, so the runner (and the compose image) point it at the app's config/ by environment. It
    stays lazy so a monkeypatch of derive_tree.CONFIG_DIR (a real Path) still wins, and so
    `derive_tree.CONFIG_DIR / "group"` in compose.py keeps working unchanged.
    """

    def _path(self) -> Path:
        return Path(os.environ.get("KUBECORE_CONFIG_DIR", str(HERE.parent / "config")))

    def __fspath__(self) -> str:
        return str(self._path())

    def __str__(self) -> str:
        return str(self._path())

    def __repr__(self) -> str:
        return f"CONFIG_DIR({self._path()})"

    def __truediv__(self, other):
        return self._path() / other

    def __getattr__(self, name):  # is_dir, glob, parent, ... behave like the Path
        return getattr(self._path(), name)


CONFIG_DIR = _LazyConfigDir()  # the developer's experiment config tree
# The config group the platform used to add to every app's config (endpoints, the outputs repository).
# It is gone: a step gets what it needs from its context and its environment. See refuse_platform_group.
REMOVED_GROUP = "platform"

# Sections with structured-config (dataclass) schemas: types + enums validated
# at compose time; Enum fields become form dropdowns. Extend this as the
# template declares more schemas (see SCHEMAS below).
SCHEMA_SECTIONS = ("data", "quantization")

ADVANCED_PARAM_KWARGS = dict(
    name="config",
    value="",
    description="Extra settings for this run, as YAML. They replace the values above.",
)


class DeriveError(Exception):
    pass


# ---------------------------------------------------- structured schemas


class DataSource(Enum):
    lakefs = "lakefs"
    s3 = "s3"


class QuantizationMode(Enum):
    none = "none"
    ptq = "ptq"
    qat = "qat"


# ENGINE PATCH (D-79): the structured-config schemas differ between the two templates. The
# profile is chosen by KUBECORE_SCHEMA_PROFILE before this module is imported; default hera = upstream.
#   hera       current hera template:   DataConfig.ref, QuantizationConfig.output_prefix=""
#   yolo       current yolo template:   neither
SCHEMA_PROFILE = os.environ.get("KUBECORE_SCHEMA_PROFILE", "hera")
if SCHEMA_PROFILE not in ("hera", "yolo"):
    raise ImportError(
        f"KUBECORE_SCHEMA_PROFILE={SCHEMA_PROFILE!r} is not a known schema profile (hera | yolo)"
    )

if SCHEMA_PROFILE == "hera":

    @dataclass
    class DataConfig:
        ref: str = "main"
        sample_size: str = ""
        seed: int = 42

    @dataclass
    class QuantizationConfig:
        mode: QuantizationMode = QuantizationMode.none
        image_size: int = 640
        calibration_frames: int = 512
        calibration_seed: int = 42
        parity_frames: int = 100
        parity_max_abs_error: float = 0.05
        output_prefix: str = ""

elif SCHEMA_PROFILE == "yolo":

    @dataclass
    class DataConfig:
        sample_size: str = ""
        seed: int = 42

    @dataclass
    class QuantizationConfig:
        mode: QuantizationMode = QuantizationMode.none
        image_size: int = 640
        calibration_frames: int = 512
        calibration_seed: int = 42
        parity_frames: int = 100
        parity_max_abs_error: float = 0.05


# {config-store name registered in config.yaml's defaults list: node}
SCHEMAS = {
    "data_schema": {"data": DataConfig},
    "quantization_schema": {"quantization": QuantizationConfig},
}

# Top-level sections that a structured-config schema backfills. A schema injects
# its section (with dataclass defaults) into the composed tree EVEN IF the
# section's yaml file was renamed/deleted — which would otherwise silently mask
# the reads= gate (a rename typo renders "successfully" with only schema
# defaults, and the developer's real values become an unread orphan section).
# So the gate must additionally require a real on-disk source for these.
SCHEMA_BACKED = {sec for node in SCHEMAS.values() for sec in node}


def _register_schemas() -> None:
    """Schemas the yaml sections merge ONTO (defaults list puts the schema
    entries first) — compose+overrides are then type/enum validated by
    OmegaConf. Extend SCHEMAS + config.yaml defaults to cover more sections."""
    cs = ConfigStore.instance()
    for name, node in SCHEMAS.items():
        cs.store(name=name, node=node)


_register_schemas()


@dataclass
class DerivedForm:
    parameters: list = field(default_factory=list)  # hera Parameters (groups first)
    tokens: list = field(default_factory=list)  # hydra override tokens for compose args
    sections: list = field(default_factory=list)  # top-level config sections
    render_defaults: dict = field(default_factory=dict)  # {override_key: rendered default}
    tree: dict = field(default_factory=dict)  # the composed config tree
    # The raw data for the run form (1533/F-21): read by kubecore.run_form, never by the Argo parameters.
    types: dict = field(default_factory=dict)  # {form field name: string|integer|number|boolean|enum}
    option_defaults: dict = field(default_factory=dict)  # {group name: {option: {leaf: value}}}; every group
    leaf_options: dict = field(default_factory=dict)  # {leaf: {"group": name, "options": [...]}}
    overrides: dict = field(default_factory=dict)  # the validated config/_form.yaml, {} when there is none


def refuse_platform_group() -> None:
    """An app whose config still lists the `platform` group in its defaults was written before the step
    contract: say what to delete instead of letting the config library report a missing file."""
    import yaml
    try:
        defaults = (yaml.safe_load((CONFIG_DIR / "config.yaml").read_text()) or {}).get("defaults") or []
    except (OSError, yaml.YAMLError, AttributeError):
        return  # the compose below reports a config tree it cannot read
    for entry in defaults:
        for key in (entry if isinstance(entry, dict) else {}):
            if str(key).replace("optional ", "").split("@")[0].strip() == REMOVED_GROUP:
                raise DeriveError(
                    f"config/config.yaml lists `- {key}: {entry[key]}` in its defaults, and the platform no "
                    f"longer adds a `{REMOVED_GROUP}` section to an app's config. {OLD_APP_NOTICE}"
                )


def _compose(overrides: list, return_hydra_config: bool = False):
    """Single choke point for Hydra compose. Reframes Hydra's
    MissingConfigException (a defaults-list entry points at a group option file
    that was deleted/renamed) as a clean, actionable DeriveError instead of a
    raw Hydra stack trace + search-path dump."""
    from hydra.errors import MissingConfigException
    refuse_platform_group()
    try:
        with initialize_config_dir(config_dir=str(CONFIG_DIR), version_base=None):
            return compose(
                config_name="config",
                overrides=list(overrides or []),
                return_hydra_config=return_hydra_config,
            )
    except MissingConfigException as exc:
        # message looks like: In 'config': Could not find '<group>/<option>'
        raise DeriveError(
            f"config tree is broken: {exc}. A defaults-list entry in config/config.yaml "
            f"points at a group option that no longer exists — restore the option file "
            f"(e.g. config/<group>/<option>.yaml) or fix the choice in config/config.yaml."
        ) from exc


def compose_tree(overrides: list = None):
    """Compose the config tree."""
    return _compose(overrides)


def _group_choices() -> dict:
    """{group_path: default_option} from the defaults list, via Hydra.
    Filtered to real group directories (schema entries etc. are not groups)."""
    hydra_cfg = _compose([], return_hydra_config=True)
    return {
        k: v
        for k, v in hydra_cfg.hydra.runtime.choices.items()
        if not k.startswith("hydra/") and (CONFIG_DIR / k).is_dir()
    }


def _group_options(group: str) -> list:
    return sorted(p.stem for p in (CONFIG_DIR / group).glob("*.yaml"))


def _mapped(dotted: str) -> str:
    return dotted.replace(".", "-").replace("/", "-")


def _value_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _walk_scalars(node: dict, path: str = "") -> list:
    """(dotted_path, value) for every scalar leaf; complex nodes (lists)
    are not flattened, plain dicts are recursed. Enum values (from
    structured schemas) count as scalars."""
    out = []
    for key, value in node.items():
        dotted = f"{path}.{key}" if path else str(key)
        if isinstance(value, dict):
            out.extend(_walk_scalars(value, dotted))
        elif isinstance(value, list):
            continue  # complex node: group selection or ADVANCED override only
        else:
            out.append((dotted, value))
    return out


_KEY_LINE = re.compile(r"^(\s*)(?:([A-Za-z0-9_.-]+)|\"([^\"]+)\"|'([^']+)'):(?:\s|$)")


def _trailing_comment(rest: str) -> str:
    """The `# ...` that ends a value's line, outside quotes; '' when there is none."""
    quote = ""
    for i, char in enumerate(rest):
        if quote:
            quote = "" if char == quote else quote
        elif char in "\"'":
            quote = char
        elif char == "#" and (i == 0 or rest[i - 1] in " \t"):
            return rest[i + 1:].strip()
    return ""


def _file_comments(text: str, prefix: str = "") -> dict:
    """{dotted setting: its comment} for one YAML file: the `# ...` at the end of the setting's line, else the
    comment lines directly above it (no blank line between), joined into one line. Hydra drops YAML comments,
    so the form reads them from the file text."""
    found, stack, above = {}, [], []
    for line in text.splitlines():
        comment = re.match(r"^\s*#\s?(.*)$", line)
        if comment:
            above.append(comment.group(1).strip())
            continue
        key = _KEY_LINE.match(line)
        if key:
            indent = len(key.group(1))
            while stack and stack[-1][0] >= indent:
                stack.pop()
            name = key.group(2) or key.group(3) or key.group(4)
            stack.append((indent, name))
            text_here = _trailing_comment(line[key.end():]) or " ".join(c for c in above if c)
            # A description is shown, never evaluated: keep template braces from being read as a template.
            text_here = text_here.replace("{{", "{ {").replace("}}", "} }")
            if text_here:
                found[".".join(([prefix] if prefix else []) + [n for _, n in stack])] = text_here
        above = []
    return found


def _comments() -> dict:
    """{dotted setting: comment} over the config tree: the files at its root, and the option files of each
    group (the default option last, so its comments win)."""
    found = {}
    for path in sorted(CONFIG_DIR.glob("*.yaml")):
        if path.name == FORM_FILE:
            continue  # the form's own file is read by read_form_overrides, never for comments
        found.update(_file_comments(path.read_text()))
    for group, default in sorted(_group_choices().items()):
        options = sorted((CONFIG_DIR / group).glob("*.yaml"), key=lambda p: p.stem == default)
        for path in options:
            found.update(_file_comments(path.read_text(), group.replace("/", ".")))
    return found


FORM_FILE = "_form.yaml"
FORM_FILE_CAP = 64 * 1024
FORM_FIELD_KEYS = ("label", "tier", "minimum", "maximum", "group")   # D13: no `pattern` in v1
FORM_GROUP_KEYS = ("id", "label", "tier", "order")
FORM_HIDDEN_GROUPS = ("info", "platform")   # fixed groups the form never shows


class _NoAliasLoader(yaml.SafeLoader):
    """SafeLoader that refuses anchors and aliases (no expansion bombs, nothing shared)."""

    def compose_node(self, parent, index):
        event = self.peek_event()
        if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None) is not None:
            raise yaml.YAMLError("anchors and aliases (&name, *name) are not allowed")
        return super().compose_node(parent, index)


def read_form_overrides() -> dict:
    """config/_form.yaml, the developer's changes to the run form: {"fields": {name: {label, tier, minimum,
    maximum, group}}, "groups": [{id, label, tier, order}]}; {} when there is no file. Read by the untrusted
    render, so the file is opened without following a link, must be a regular file and is read bounded. Any
    problem is a DeriveError (the build fails, nothing is guessed). The values are checked by run_form."""
    path = CONFIG_DIR / FORM_FILE
    here = f"config/{FORM_FILE}"
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise DeriveError(f"{here} cannot be read ({exc.strerror}); it must be a regular file, not a link") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise DeriveError(f"{here} must be a regular file")
        data = b""
        while len(data) <= FORM_FILE_CAP:
            chunk = os.read(fd, FORM_FILE_CAP + 1 - len(data))
            if not chunk:
                break
            data += chunk
    finally:
        os.close(fd)
    if len(data) > FORM_FILE_CAP:
        raise DeriveError(f"{here} is over {FORM_FILE_CAP // 1024} KiB: shorten it")
    try:
        doc = yaml.load(data.decode("utf-8"), Loader=_NoAliasLoader)  # noqa: S506 - a SafeLoader subclass
    except (yaml.YAMLError, UnicodeDecodeError, RecursionError) as exc:
        raise DeriveError(f"{here} is not valid YAML without anchors or aliases: {exc}") from exc
    if doc is None:
        return {}
    if not isinstance(doc, dict):
        raise DeriveError(f"{here} must be a mapping with `fields` and `groups`")
    extra = sorted(str(k) for k in doc if k not in ("fields", "groups"))
    if extra:
        raise DeriveError(f"{here} has unknown keys {extra}: allowed are fields and groups")
    out = {}
    fields = doc.get("fields")
    if fields is not None:
        if not isinstance(fields, dict):
            raise DeriveError(f"{here}: fields must be a mapping of field name to its changes")
        for name, spec in fields.items():
            if not isinstance(spec, dict):
                raise DeriveError(f"{here}: fields.{name} must be a mapping")
            if "pattern" in spec:
                raise DeriveError(
                    f"{here}: fields.{name}.pattern is not supported in v1: remove it (a value is checked by its "
                    f"type, minimum and maximum)"
                )
            bad = sorted(str(k) for k in spec if k not in FORM_FIELD_KEYS)
            if bad:
                raise DeriveError(
                    f"{here}: fields.{name} has unknown keys {bad}: allowed are {', '.join(FORM_FIELD_KEYS)}"
                )
            if spec.get("group") in FORM_HIDDEN_GROUPS:
                raise DeriveError(
                    f"{here}: fields.{name}.group is `{spec['group']}`, a group the run form never shows: pick "
                    f"another group"
                )
        out["fields"] = fields
    groups = doc.get("groups")
    if groups is not None:
        if not isinstance(groups, list):
            raise DeriveError(f"{here}: groups must be a list")
        for i, group in enumerate(groups):
            if not isinstance(group, dict):
                raise DeriveError(f"{here}: groups[{i}] must be a mapping")
            bad = sorted(str(k) for k in group if k not in FORM_GROUP_KEYS)
            if bad:
                raise DeriveError(
                    f"{here}: groups[{i}] has unknown keys {bad}: allowed are {', '.join(FORM_GROUP_KEYS)}"
                )
        out["groups"] = groups
    return out


def _leaf_type(value) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, Enum):
        return "enum"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _option_data(group: str, default: str, options: list, defaults: dict) -> tuple:
    """For one group: ({option: {leaf: value}} for the leaves that are form fields and differ from the default
    option, {leaf: [options that have it]}). `defaults` is {leaf name: default value text}, the form fields.
    The default option has no entry, but counts as an option that has its leaves."""
    from omegaconf.errors import OmegaConfBaseException

    name = _mapped(group)
    changed, having = {}, {}
    for option in options:
        if option == default:
            leaves = {_mapped(d): _value_str(v) for d, v in _walk_scalars(OmegaConf.to_container(
                compose_tree(), resolve=True))}
        else:
            try:
                tree = OmegaConf.to_container(_compose([f"{group}={option}"]), resolve=True)
            except (OmegaConfBaseException, ValueError) as exc:
                raise DeriveError(
                    f"the option `{option}` of the config group {group}/ does not compose ({exc}): fix "
                    f"config/{group}/{option}.yaml"
                ) from exc
            leaves = {_mapped(d): _value_str(v) for d, v in _walk_scalars(tree)}
        for leaf, value in leaves.items():
            if leaf not in defaults:
                continue
            having.setdefault(leaf, []).append(option)
            if option != default and value != defaults[leaf]:
                changed.setdefault(option, {})[leaf] = value
    partial = {leaf: opts for leaf, opts in having.items() if len(opts) < len(options)}
    return name, changed, partial


def derive() -> DerivedForm:
    # Hera imported HERE (render time only) — keeps the run-time compose image
    # free of Hera. See module docstring / the hera-note above the imports.
    from hera.workflows import Parameter

    composed = compose_tree()
    refuse_resolvers(OmegaConf.to_container(composed, resolve=False))   # before anything is resolved
    tree = OmegaConf.to_container(composed, resolve=True)
    refuse_reserved_keys(tree)
    form = DerivedForm(sections=list(tree.keys()), tree=tree)
    seen = {}

    def claim(name: str, origin: str) -> None:
        if name in RESERVED_CONFIG_KEYS:
            raise DeriveError(
                f"{origin} becomes the form field '{name}', a name the platform reserves "
                f"({', '.join(RESERVED_CONFIG_KEYS)}); rename the config key"
            )
        if name in seen:
            raise DeriveError(
                f"form-name collision: '{name}' derived from both {seen[name]} "
                f"and {origin}; rename one of the config keys"
            )
        seen[name] = origin

    # groups -> dropdowns (form order: groups first)
    choices = sorted(_group_choices().items())
    for group, default in choices:
        name = _mapped(group)
        form.types[name] = "enum"
        claim(name, f"config group {group}/")
        options = _group_options(group)
        form.parameters.append(
            Parameter(
                name=name,
                value=default,  # arguments.parameters need `value`, not `default`
                enum=options,
                description=f"One of: {', '.join(options)}.",
            )
        )
        form.tokens.append(f"{group}={{{{workflow.parameters.{name}}}}}")
        form.render_defaults[group] = default

    # scalar leaves -> fields; the description is the comment written next to the setting in config/
    comments = _comments()
    for dotted, value in _walk_scalars(tree):
        name = _mapped(dotted)
        claim(name, f"config leaf {dotted}")
        default = _value_str(value)
        enum = [str(m.value) for m in type(value)] if isinstance(value, Enum) else None
        form.types[name] = _leaf_type(value)
        form.parameters.append(
            Parameter(name=name, value=default, enum=enum, description=comments.get(dotted, ""))
        )
        form.tokens.append(f"{dotted}={{{{workflow.parameters.{name}}}}}")
        form.render_defaults[dotted] = default

    form.parameters.append(Parameter(**ADVANCED_PARAM_KWARGS))

    # the raw data the run form needs: what each group option changes, and which leaves only some options have
    defaults = {_mapped(dotted): _value_str(value) for dotted, value in _walk_scalars(tree)}
    for group, default in choices:
        name, changed, partial = _option_data(group, default, _group_options(group), defaults)
        form.option_defaults[name] = changed
        for leaf, opts in partial.items():
            form.leaf_options[leaf] = {"group": name, "options": opts}
    form.overrides = read_form_overrides()
    return form


_RESOLVER = re.compile(r"(?<!\\)\$\{\s*([A-Za-z_][\w.\-]*)\s*:")


def refuse_resolvers(node, path: str = "") -> None:
    """Config never reads the environment or a file. A value written `${oc.env:NAME}` (or any other
    `${resolver:...}`) is resolved by whoever composes the run, in a process that holds the platform's own
    credentials, and the value would travel into the steps' contexts. No resolver is allowed, so none can
    be used for that. `node` is the composed config, NOT yet resolved; references to other config values,
    `${train.lr}`, are fine."""
    if isinstance(node, dict):
        items = ((f"{path}.{k}" if path else str(k), v) for k, v in node.items())
    elif isinstance(node, list):
        items = ((f"{path}[{i}]", v) for i, v in enumerate(node))
    else:
        found = _RESOLVER.search(node) if isinstance(node, str) else None
        if found:
            raise DeriveError(
                f"config value '{path}' uses the resolver `${{{found.group(1)}:...}}`. Config is not a place for "
                f"secrets or platform values: a resolver reads the environment or the files of the machine that "
                f"composes the run, so none is allowed. Write the value in config/, or choose it on the form."
            )
        return
    for item_path, value in items:
        refuse_resolvers(value, item_path)


def refuse_reserved_keys(tree: dict) -> None:
    """The platform keeps a few top-level config keys for the run itself (the run's plan, which
    pipeline version it runs). An app's config tree may not define them."""
    for key in RESERVED_CONFIG_KEYS:
        if key in tree:
            raise DeriveError(
                f"the config key '{key}' is reserved for the platform (reserved: "
                f"{', '.join(RESERVED_CONFIG_KEYS)}); rename '{key}' in your config/ tree"
            )
    for key in REMOVED_CONFIG_KEYS:
        if key in tree:
            raise DeriveError(
                f"the config tree defines a top-level `{key}` section, which the platform no longer uses. "
                f"{OLD_APP_NOTICE}"
            )


def validate_inputs(step_inputs: dict, tree: dict) -> None:
    """Render gate: every name a step declares via inputs= must be a top-level config value that can hold
    a dataset reference (text), so the run has something to pin."""
    for step_name, inputs in step_inputs.items():
        for name in inputs:
            if name not in tree:
                raise DeriveError(
                    f"step '{step_name}' declares the input '{name}', but the config tree has no top-level "
                    f"value '{name}' (top-level keys: {', '.join(tree)}). Add `{name}: <dataset>@<version>` "
                    f"to config/, for example `{name}: dataset-default@latest`."
                )
            if tree[name] is not None and not isinstance(tree[name], str):
                raise DeriveError(
                    f"step '{step_name}' declares the input '{name}', but the config value '{name}' is "
                    f"{'a section' if isinstance(tree[name], dict) else 'not text'}. An input is one value "
                    f"that holds a dataset reference, for example `{name}: dataset-default@latest`."
                )


def _has_config_source(section: str) -> bool:
    """True if the section is backed by a real on-disk config source that
    actually contributes the section — either a `config/{section}/` group dir,
    or a `config/{section}.yaml` whose top-level content includes the `{section}`
    key. The key check matters: renaming the key INSIDE data.yaml (data: ->
    dataX:) leaves the file present but no longer contributes `data`, so the
    schema would silently backfill it. Detect that."""
    if (CONFIG_DIR / section).is_dir():
        return True
    f = CONFIG_DIR / f"{section}.yaml"
    if not f.is_file():
        return False
    try:
        import yaml as _yaml
        doc = _yaml.safe_load(f.read_text()) or {}
        return isinstance(doc, dict) and section in doc
    except Exception:
        # If we can't parse it, assume it's a real source (don't false-fire).
        return True


def validate_reads(step_reads: dict, sections: list) -> None:
    """Render gate: every section a step declares via reads= must exist as a
    top-level section of the composed tree (closes the silent-failure mode:
    tree section renamed but a step still reads it).

    For SCHEMA_BACKED sections the composed tree alone is not enough — a
    structured-config schema backfills the section even when its yaml file was
    renamed/deleted, silently masking the rename. So a schema-backed section
    also requires a real on-disk config source; otherwise the gate fires with a
    rename-specific message."""
    for step_name, reads in step_reads.items():
        for section in reads:
            if section not in sections:
                raise DeriveError(
                    f"step '{step_name}' reads config section '{section}' which does "
                    f"not exist in the config tree (top-level sections: {', '.join(sections)})"
                )
            if section in SCHEMA_BACKED and not _has_config_source(section):
                raise DeriveError(
                    f"step '{step_name}' reads config section '{section}', which has a "
                    f"structured-config schema but no config/{section}.yaml (or "
                    f"config/{section}/ group) — its yaml was likely renamed or deleted. "
                    f"The schema backfills defaults, so the render would silently drop your "
                    f"real values. Restore config/{section}.yaml (or update reads= + the schema)."
                )
