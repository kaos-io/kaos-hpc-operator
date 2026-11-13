"""The run form's trusted side (PRD 1533/F-21 and 1533/F-22): take the render's raw form off the template and
check it. Trusted code: standard library and PyYAML only. It never imports hera, hydra or omegaconf (a test
asserts that), so the render side (derive_tree, authoring) is the only code that may use them.
"""
import json
import re
import sys
from typing import Callable, Optional

from kubecore import step_contract
from kubecore import form_grammar
from kubecore.enhance import platform_annotations, tier_default
from kubecore.meluxina import HPC_TWIN_TEMPLATE, STEP_KIND_LABEL, hpc_classes
from kubecore.run_settings import LONGEST_MINUTES, checkpoint_field, class_field, promote_field, walltime_field

from kubecore.step_contract import (
    COMMIT_RE, ENGINE_VERSION_ANNOTATION, ENGINE_VERSION_RE, PARAMS_DIGEST_KEY, PIPELINE_VERSION_KEY,
    RAW_FORM_ANNOTATION, RAW_FORM_CAP, RUN_FORM_ANNOTATION, RUN_FORM_CAP, RUN_FORM_WARN, SOURCE_COMMIT_KEY, STAMP_BY,
    wire,
)

UI_PREFIX = "ui.kubecore.io/"
PLATFORM_PREFIX = "platform.kubecore.io/"
FORGED_KEYS = (SOURCE_COMMIT_KEY, ENGINE_VERSION_ANNOTATION, PARAMS_DIGEST_KEY, PIPELINE_VERSION_KEY)

RAW_KEYS = {"version", "types", "optionDefaults", "leafOptions", "when", "overrides"}
TYPES = {"string", "integer", "number", "boolean", "enum"}
TIERS = {"basic", "advanced", "hidden"}
OVERRIDE_FIELD_KEYS = {"label", "tier", "minimum", "maximum", "group"}   # D13: no `pattern` in v1
OVERRIDE_GROUP_KEYS = {"id", "label", "tier", "order"}
GROUP_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
# The groups the run form always has; a developer's group must not reuse one of these ids.
FIXED_GROUP_IDS = frozenset({"info", "experiment", "settings", "datasets", "steps", "promote", "advanced",
                             "platform"})
MAX_LABEL = 80
MAX_GROUPS = 32
MAX_OPTION_VALUE = 4096


class RunFormError(Exception):
    """A refusal; the message names the field and the fix. Callers turn it into a build failure."""


class _Dict(dict):
    """A decoded object that remembers the keys it repeated."""
    repeated = ()


def _object(pairs):
    out = _Dict()
    repeated = []
    for key, value in pairs:
        if key in out:
            repeated.append(key)
        out[key] = value
    out.repeated = repeated
    return out


def _refuse_constant(name):
    raise RunFormError("the raw form contains %s: use a finite number" % name)


def _find_repeat(node, path="raw form"):
    if isinstance(node, _Dict):
        if node.repeated:
            raise RunFormError("%s repeats the key %r: each key may appear once" % (path, node.repeated[0]))
        for key, value in node.items():
            _find_repeat(value, "%s.%s" % (path, key))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            _find_repeat(value, "%s[%d]" % (path, i))


def _mapping(holder, where):
    """`holder` as a dict with text keys, or None when absent."""
    if holder is None:
        return None
    if not isinstance(holder, dict):
        raise RunFormError("%s must be a mapping, not %s: fix the template" % (where, type(holder).__name__))
    for key in holder:
        if not isinstance(key, str):
            raise RunFormError("%s has the key %r, which is not text: fix the template" % (where, key))
    return holder


def take_raw_form(wft: dict) -> Optional[dict]:
    """Pop the raw form off `wft` and strip every key a developer could use to forge the finished form.
    Returns the decoded raw form, or None when the render wrote none."""
    meta = wft.setdefault("metadata", {})
    if not isinstance(meta, dict):
        raise RunFormError("metadata must be a mapping, not %s" % type(meta).__name__)
    annotations = _mapping(meta.get("annotations"), "metadata.annotations") or {}
    labels = _mapping(meta.get("labels"), "metadata.labels")
    spec = wft.get("spec", {})
    if not isinstance(spec, dict):
        raise RunFormError("spec must be a mapping, not %s" % type(spec).__name__)
    workflow_metadata = spec.get("workflowMetadata")
    if workflow_metadata is not None and not isinstance(workflow_metadata, dict):
        raise RunFormError("spec.workflowMetadata must be a mapping, not %s" % type(workflow_metadata).__name__)
    workflow_metadata = workflow_metadata or {}
    wm = [_mapping(workflow_metadata.get(f), "spec.workflowMetadata." + f) for f in ("labels", "annotations")]
    text = annotations.pop(RAW_FORM_ANNOTATION, None)
    raw = None
    if text is not None:
        if not isinstance(text, str):
            raise RunFormError("the raw form annotation must be a JSON string")
        size = len(text.encode("utf-8"))
        if size > RAW_FORM_CAP:
            raise RunFormError("the raw form is %d bytes, over the %d byte cap: shorten the config comments, "
                               "option lists or _form.yaml" % (size, RAW_FORM_CAP))
        try:
            raw = json.loads(text, object_pairs_hook=_object, parse_constant=_refuse_constant)
        except ValueError as exc:
            raise RunFormError("the raw form is not JSON: %s" % exc) from None
        _find_repeat(raw)
        if not isinstance(raw, dict):
            raise RunFormError("the raw form must be a JSON object")
    for holder in (annotations, labels):
        for key in [k for k in holder or () if k.startswith(UI_PREFIX) or k in FORGED_KEYS]:
            del holder[key]
    for holder in wm:
        for key in [k for k in holder or () if k.startswith(UI_PREFIX) or k.startswith(PLATFORM_PREFIX)]:
            del holder[key]
    return raw


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _label_ok(where: str, label) -> None:
    if not isinstance(label, str) or not label or len(label) > MAX_LABEL:
        raise RunFormError("%s: label must be a text of 1 to %d characters" % (where, MAX_LABEL))
    if any(ord(c) < 32 or ord(c) == 127 for c in label):
        raise RunFormError("%s: label must not contain control characters" % where)


def _check_type(name: str, kind: str, param: dict) -> None:
    value = param.get("value")
    if not isinstance(kind, str) or kind not in TYPES:
        raise RunFormError("types[%r]: %r is not a type; use one of %s" % (name, kind, sorted(TYPES)))
    if kind == "enum":
        if not param.get("enum"):
            raise RunFormError("types[%r] is an enum but the Argo parameter has no enum" % name)
        return
    if value is None:
        return
    text = str(value)
    if kind == "boolean" and text not in ("true", "false"):
        raise RunFormError("types[%r] is a boolean but the Argo default is %r: use true or false" % (name, text))
    if kind == "integer":
        try:
            int(text)
        except ValueError:
            raise RunFormError("types[%r] is an integer but the Argo default is %r" % (name, text)) from None
    if kind == "number":
        try:
            float(text)
        except ValueError:
            raise RunFormError("types[%r] is a number but the Argo default is %r" % (name, text)) from None


def validate_raw(raw: dict, params: list, step_names, parse_when: Optional[Callable[[str], object]] = None) -> None:
    """Refuse a raw form that does not fit the template. `params` are the Argo parameters, `step_names` the
    pipeline's own steps. `parse_when` re-parses each `when` text; the default is form_grammar's parser."""
    if parse_when is None:
        parse_when = form_grammar.parse_when
    if not isinstance(raw, dict):
        raise RunFormError("the raw form must be an object")
    extra = set(raw) - RAW_KEYS
    if extra:
        raise RunFormError("the raw form has unknown keys %s: allowed are %s" % (sorted(extra), sorted(RAW_KEYS)))
    missing = RAW_KEYS - set(raw)
    if missing:
        raise RunFormError("the raw form lacks the keys %s" % sorted(missing))
    if raw["version"] != 1 or isinstance(raw["version"], bool):
        raise RunFormError("the raw form version must be 1, not %r" % (raw["version"],))
    by_name = {p["name"]: p for p in params}
    steps = set(step_names)

    def param(name, where):
        if not isinstance(name, str) or name not in by_name:
            raise RunFormError("%s names %r, which is not a run form parameter: check the spelling" % (where, name))
        return by_name[name]

    types = raw["types"]
    if not isinstance(types, dict):
        raise RunFormError("types must be an object")
    for name, kind in types.items():
        _check_type(name, kind, param(name, "types"))

    option_defaults = raw["optionDefaults"]
    if not isinstance(option_defaults, dict):
        raise RunFormError("optionDefaults must be an object")
    for group, options in option_defaults.items():
        enum = param(group, "optionDefaults").get("enum") or []
        if not isinstance(options, dict):
            raise RunFormError("optionDefaults[%r] must be an object" % group)
        for option, leaves in options.items():
            if option not in enum:
                raise RunFormError("optionDefaults[%r] names the option %r, which is not in the group's options %s"
                                   % (group, option, enum))
            if not isinstance(leaves, dict):
                raise RunFormError("optionDefaults[%r][%r] must be an object" % (group, option))
            for leaf, value in leaves.items():
                param(leaf, "optionDefaults[%r][%r]" % (group, option))
                if not isinstance(value, str) or len(value) > MAX_OPTION_VALUE:
                    raise RunFormError("optionDefaults[%r][%r][%r] must be a text of at most %d characters"
                                       % (group, option, leaf, MAX_OPTION_VALUE))

    leaf_options = raw["leafOptions"]
    if not isinstance(leaf_options, dict):
        raise RunFormError("leafOptions must be an object")
    for leaf, options in leaf_options.items():
        param(leaf, "leafOptions")
        if not isinstance(options, list) or not all(isinstance(o, str) for o in options):
            raise RunFormError("leafOptions[%r] must be a list of option names" % leaf)

    when = raw["when"]
    if not isinstance(when, dict):
        raise RunFormError("when must be an object")
    for step, text in when.items():
        if step not in steps:
            raise RunFormError("when names %r, which is not a step of this pipeline" % step)
        if not isinstance(text, str):
            raise RunFormError("when[%r] must be text" % step)
        try:
            parse_when(text)
        except Exception as exc:
            raise RunFormError("when[%r] does not parse: %s" % (step, exc)) from None

    _validate_overrides(raw["overrides"], types, param)


def _validate_overrides(overrides, types: dict, param) -> None:
    if not isinstance(overrides, dict):
        raise RunFormError("overrides must be an object")
    extra = set(overrides) - {"fields", "groups"}
    if extra:
        raise RunFormError("overrides has unknown keys %s: allowed are fields and groups" % sorted(extra))
    groups = overrides.get("groups") or []
    if not isinstance(groups, list):
        raise RunFormError("overrides.groups must be a list")
    if len(groups) > MAX_GROUPS:
        raise RunFormError("overrides.groups has %d groups: at most %d" % (len(groups), MAX_GROUPS))
    ids = set()
    for group in groups:
        if not isinstance(group, dict):
            raise RunFormError("each overrides.groups entry must be an object")
        bad = set(group) - OVERRIDE_GROUP_KEYS
        if bad:
            raise RunFormError("overrides.groups entry has unknown keys %s: allowed are %s"
                               % (sorted(bad), sorted(OVERRIDE_GROUP_KEYS)))
        gid = group.get("id")
        if not isinstance(gid, str) or not GROUP_ID_RE.match(gid):
            raise RunFormError("overrides.groups id %r must match %s" % (gid, GROUP_ID_RE.pattern))
        if gid in FIXED_GROUP_IDS:
            raise RunFormError("overrides.groups id %r is one of the fixed groups %s: pick another id"
                               % (gid, sorted(FIXED_GROUP_IDS)))
        if gid in ids:
            raise RunFormError("overrides.groups id %r is declared twice" % gid)
        ids.add(gid)
        if "label" in group:
            _label_ok("overrides.groups[%s]" % gid, group["label"])
        if "tier" in group and (not isinstance(group["tier"], str) or group["tier"] not in TIERS):
            raise RunFormError("overrides.groups[%s]: tier must be one of %s" % (gid, sorted(TIERS)))
        if "order" in group and (not isinstance(group["order"], int) or isinstance(group["order"], bool)
                                 or group["order"] < 0):
            raise RunFormError("overrides.groups[%s]: order must be an integer of 0 or more" % gid)
    fields = overrides.get("fields") or {}
    if not isinstance(fields, dict):
        raise RunFormError("overrides.fields must be an object")
    for name, spec in fields.items():
        where = "overrides.fields[%s]" % name
        param(name, "overrides.fields")
        if not isinstance(spec, dict):
            raise RunFormError("%s must be an object" % where)
        if "pattern" in spec:
            raise RunFormError("%s: pattern is not supported in _form.yaml for v1: remove it (the value is "
                               "checked by its type, minimum and maximum)" % where)
        bad = set(spec) - OVERRIDE_FIELD_KEYS
        if bad:
            raise RunFormError("%s has unknown keys %s: allowed are %s" % (where, sorted(bad),
                                                                         sorted(OVERRIDE_FIELD_KEYS)))
        if "label" in spec:
            _label_ok(where, spec["label"])
        if "tier" in spec and (not isinstance(spec["tier"], str) or spec["tier"] not in TIERS):
            raise RunFormError("%s: tier must be one of %s" % (where, sorted(TIERS)))
        for bound in ("minimum", "maximum"):
            if bound in spec:
                if types.get(name) not in ("integer", "number"):
                    raise RunFormError("%s: %s applies only to integer and number fields, and this is %s"
                                       % (where, bound, types.get(name, "not typed")))
                if not _is_number(spec[bound]):
                    raise RunFormError("%s: %s must be a number" % (where, bound))
        if "minimum" in spec and "maximum" in spec and spec["minimum"] > spec["maximum"]:
            raise RunFormError("%s: minimum %s is above maximum %s" % (where, spec["minimum"], spec["maximum"]))
        if "group" in spec:
            gid = spec["group"]
            if not isinstance(gid, str) or not GROUP_ID_RE.match(gid):
                raise RunFormError("%s: group %r must match %s" % (where, gid, GROUP_ID_RE.pattern))
            # check_form refuses it again on the finished form (a field's group must be one of its groups)
            if gid not in ids and gid not in FIXED_GROUP_IDS:
                raise RunFormError("%s: group %r is not declared: add it to groups in _form.yaml, or use one of "
                                   "the fixed groups %s" % (where, gid, sorted(FIXED_GROUP_IDS)))


# ------------------------------------------------------------------------------ order

# The run form's fixed groups, in the order a developer reads them: (id, tier). `settings` is where the config
# leaves sit, and where the groups of _form.yaml sort (by their `order`, after the plain settings).
GROUPS = (
    ("info", "hidden"),
    ("experiment", "basic"),
    ("settings", "basic"),
    ("datasets", "basic"),
    ("steps", "basic"),
    ("promote", "advanced"),
    ("advanced", "advanced"),
    ("platform", "hidden"),
)
_STEP_SUFFIXES = ("class", "walltime", "checkpoint-every")


def form_rank(params: list, step_table: list, overrides: Optional[dict] = None) -> dict:
    """{parameter name: (group id, index)} for each of `params`, the Argo parameters. The group id is one of
    GROUPS, or the id of a _form.yaml group (in `overrides["groups"]`) a field was moved to with `group`. The
    index orders a step's own fields (class, time limit, checkpoint interval, step by step); everywhere else
    it is 0, and a caller sorts stably so the Argo order holds inside a group. Sort with `rank_key`.
    `step_table` is the compose step's `--steps` table."""
    inputs = {name for entry in step_table for name in entry["inputs"]}
    own = [entry["name"] for entry in step_table]
    step_fields = {"%s-%s" % (step, suffix): (i * 3 + k) for i, step in enumerate(own)
                   for k, suffix in enumerate(_STEP_SUFFIXES)}
    moved = ((overrides or {}).get("fields")) or {}
    rank = {}
    for p in params:
        name = p["name"]
        if name in step_fields:
            rank[name] = ("steps", step_fields[name])
        elif name == "pipeline-info":
            rank[name] = ("info", 0)
        elif name.startswith("experiment-"):
            rank[name] = ("experiment", 0)
        elif name in inputs:
            rank[name] = ("datasets", 0)
        elif name.startswith("promote-"):
            rank[name] = ("promote", 0)
        elif name == "config":
            rank[name] = ("advanced", 0)
        elif name.startswith("image-"):
            rank[name] = ("platform", 0)
        else:
            rank[name] = ("settings", 0)
        group = (moved.get(name) or {}).get("group")
        if group:
            rank[name] = (group, 0)
    return rank


def rank_key(entry: tuple, overrides: Optional[dict] = None) -> tuple:
    """The sort key of a `form_rank` entry. A fixed group sorts by its place in GROUPS. A _form.yaml group sorts
    inside `settings`, after the plain settings, by its `order` (then its declaration). A group that is neither
    sorts last, so a stale id never reorders the rest."""
    group, index = entry
    fixed = [g for g, _ in GROUPS]
    if group in fixed:
        return (fixed.index(group), 0, 0, index)
    listed = (overrides.get("groups") if isinstance(overrides, dict) else None) or []
    listed = listed if isinstance(listed, list) else []
    declared = [g.get("id") if isinstance(g, dict) else None for g in listed]
    if group is not None and group in declared:
        order = listed[declared.index(group)].get("order")
        order = order if isinstance(order, int) and not isinstance(order, bool) else 0
        return (fixed.index("settings"), 1, order, declared.index(group), index)
    return (len(fixed), 0, 0, index)


# ------------------------------------------------------------------------------ machines and steps

MAX_DESCRIPTION = 512     # the schema's limit on a machine's description (bytes: safe for a count in either unit)
COMPOSE_STEP = "compose-and-validate"
# Steps that are not the developer's: the compose step, and (once open PR 79 lands) its promote step. The name
# is read from the constant if there is one; main has none today, so only the compose step is excluded.
EXCLUDED_STEPS = frozenset(
    {COMPOSE_STEP} | ({getattr(step_contract, "PROMOTE_STEP")} if hasattr(step_contract, "PROMOTE_STEP") else set()))


def truncate_text(text, limit=MAX_DESCRIPTION):
    """Platform text over `limit` UTF-8 bytes, cut on a character boundary with a trailing ellipsis (D11)."""
    text = text if isinstance(text, str) else ("" if text is None else str(text))
    if len(text.encode("utf-8")) <= limit:
        return text
    ellipsis = "\u2026"
    room = limit - len(ellipsis.encode("utf-8"))
    return text.encode("utf-8")[:room].decode("utf-8", "ignore") + ellipsis


def _number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def machines(ctx: dict) -> list:
    """The machines the run form offers: the pool's `computeClasses.all` plus the tier defaults not in it
    (de-duplicated by name, a missing `allocatable` taken from the tier default), then the HPC classes when
    the project has HPC. Only the fields of the schema's machine definition are copied; the pool's
    tolerations, node counts, disk sizes and the HPC account, paths and controller never leave the platform."""
    classes = ctx["computeClasses"]
    defaults = {c["name"]: c for c in (classes.get("cpu"), classes.get("gpu")) if c}
    catalog = list(classes.get("all") or [])
    seen = {c["name"] for c in catalog}
    catalog += [c for name, c in defaults.items() if name not in seen]
    out, names = [], set()
    for cls in catalog:
        if cls["name"] in names:
            continue
        names.add(cls["name"])
        alloc = cls.get("allocatable") or (defaults.get(cls["name"]) or {}).get("allocatable") or {}
        accel = cls.get("guestAccelerator")
        out.append({
            "name": cls["name"],
            "tier": cls.get("tier") or "cpu",
            "machineType": cls.get("machineType"),
            "accelerator": ({"type": accel.get("type"), "count": accel["count"]}
                            if accel and accel.get("count") else None),
            "allocatable": {"cpu": _number(alloc.get("cpu")), "memoryGiB": _number(alloc.get("memoryGiB")),
                            "ephemeralStorageGiB": _number(alloc.get("ephemeralStorageGiB"))},
            "description": truncate_text(cls.get("description")),
            "kind": "cluster",
        })
    provider = (ctx.get("hpc") or {}).get("provider", "meluxina")
    for cls in hpc_classes(ctx):
        if cls["name"] in names:
            continue
        names.add(cls["name"])
        gpus = cls.get("gpus") or 0
        hpc = {"provider": provider, "partition": cls.get("partition", ""), "qos": cls.get("qos", ""),
               "gpus": gpus}
        if isinstance(cls.get("maxTimeLimitMinutes"), int) and cls["maxTimeLimitMinutes"] >= 1:
            hpc["maxTimeLimitMinutes"] = cls["maxTimeLimitMinutes"]
        hpc["sharesNode"] = True
        out.append({
            "name": cls["name"],
            "tier": cls.get("tier") or "cpu",
            "machineType": None,
            "accelerator": {"type": None, "count": gpus} if gpus > 0 else None,
            "allocatable": {"cpu": _number(cls.get("cpus")), "memoryGiB": _number(cls.get("memoryGiB")),
                            "ephemeralStorageGiB": None},
            "description": truncate_text(cls.get("description")),
            "kind": "hpc",
            "hpc": hpc,
        })
    return out


def step_table(steps: list) -> list:
    """The compose step's `--steps` table: the developer's own steps, as authoring wrote them ([] without one)."""
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    args = ((compose or {}).get("container") or {}).get("args") or []
    if "--steps" not in args or args.index("--steps") + 1 >= len(args):
        return []
    try:
        table = json.loads(args[args.index("--steps") + 1])
    except ValueError:
        raise RunFormError("the compose step's --steps table is not JSON: render the template again") from None
    if not isinstance(table, list) or not all(isinstance(e, dict) and isinstance(e.get("name"), str) for e in table):
        raise RunFormError("the compose step's --steps table is not a list of steps: render the template again")
    return table


def steps(steps: list, offers: dict, ctx: dict, params: list) -> list:
    """The run form's steps: for each step of the developer's pipeline, the class choice it offers (the
    output of enhance.class_offer, verbatim), its default, its kind and the names of its run-setting fields.
    `steps` are the workflow's step templates, `offers` is {step: class_offer or None (pinned)}, `params` the
    workflow's Argo parameters (a field is named only when the form has it)."""
    have = {p["name"] for p in params}
    values = {p["name"]: p.get("value") for p in params}
    table = step_table(steps)
    datasets = {e["name"]: [p["name"] for p in e.get("produces", []) if p.get("type") == "dataset"] for e in table}
    out = []
    for step in steps:
        name = step["name"]
        # the compose step, the HPC twin (a template the platform adds) and any template that is not a step
        # of the developer's pipeline (the compose step's table lists those) are not steps of the form
        if name in EXCLUDED_STEPS or name == HPC_TWIN_TEMPLATE or (table and name not in datasets):
            continue
        offer = offers.get(name)
        if offer is None:
            pinned = platform_annotations(step).get("compute-class")
            if not pinned:
                raise RunFormError("step %r is not pinned to a compute class and has no class offer: pin it with "
                                   "the compute-class annotation, or pass its class_offer in offers" % name)
            entry = {"classField": None, "default": pinned, "offered": [pinned], "refused": {}}
        else:
            # enhance_class_param sets the `<step>-class` value to tier_default; once the step is enhanced its typed
            # GPU request is gone (enhance_gpu_routing), so tier_default would read a GPU step as CPU. The
            # parameter is the authority; tier_default is only for a caller that has no such parameter.
            default = values.get(class_field(name)) or tier_default(ctx, step)["name"]
            entry = {"classField": class_field(name), "default": default,
                     "offered": list(offer["offered"]),
                     "refused": {k: [v[0], v[1]] for k, v in offer["refused"].items()}}
        entry = {"name": name, "kind": ((step.get("metadata") or {}).get("labels") or {}).get(STEP_KIND_LABEL),
                 **entry}
        if walltime_field(name) in have:
            entry["walltimeField"] = walltime_field(name)
        if checkpoint_field(name) in have:
            entry["checkpointField"] = checkpoint_field(name)
        promote = [promote_field(d) for d in datasets.get(name, []) if promote_field(d) in have]
        if promote:
            entry["promoteFields"] = promote
        out.append(entry)
    return out


build_steps = steps    # `attach` takes a parameter named steps


# ------------------------------------------------------------------------------ fields and groups

DURATION_PATTERN = r"^[0-9]+[mh]?$"     # run_settings._TIME: minutes or hours, the platform's own pattern
GROUP_LABELS = {"info": "About this form", "experiment": "Experiment", "settings": "Settings",
                "datasets": "Datasets", "steps": "Steps", "promote": "Promote datasets", "advanced": "Advanced",
                "platform": "Set by the platform"}
DEVELOPER_SOURCES = ("config", "group", "input")   # the fields _form.yaml may change
EMPTY_RAW = {"version": 1, "types": {}, "optionDefaults": {}, "leafOptions": {}, "when": {}, "overrides": {}}
MAX_SHOW_IF = 1024
MAX_OPTION_DEFAULTS = 64
_RUN_SETTING_LABELS = {"class": "%s: machine", "walltime": "%s: time limit", "checkpoint-every": "%s: checkpoint every"}


def is_image_field(name) -> bool:
    """OSR's rule: every `image-*` parameter (any case) is the platform's, whatever the form says."""
    return isinstance(name, str) and name.lower().startswith("image-")


def step_show_if(step: str, text) -> Optional[str]:
    """The showIf of a step's fields: its `when=` text, normalized; None without one. A `when=` that reads a
    step's value (`<step>.<value>`) gives none (D12): a form cannot know that value when the run is submitted,
    so the fields are always shown."""
    if text is None:
        return None
    try:
        compares, joiners = form_grammar.parse_when(text)
    except form_grammar.GrammarError as exc:
        raise RunFormError("step %r: %s" % (step, exc)) from None
    if any(form_grammar.is_step_value(field) for field, _, _ in compares):
        return None
    out = form_grammar.normalize(compares, joiners)
    if len(out) > MAX_SHOW_IF:
        raise RunFormError("step %r: its when= is %d characters, over the run form's %d: shorten it"
                           % (step, len(out), MAX_SHOW_IF))
    return out


def _leaf_show_if(leaf: str, options, choices: dict) -> Optional[str]:
    """`<group> == a or <group> == b` for a config leaf that only the options `options` of its group have. The
    group is the longest group field whose name prefixes the leaf's and whose options hold them all; None
    when there is none (the field is then always shown)."""
    if not options:
        return None
    found = [g for g, enum in choices.items() if leaf.startswith(g + "-") and set(options) <= set(enum)]
    if not found:
        return None
    group = max(found, key=len)
    out = form_grammar.normalize([(group, "==", o) for o in options], ["or"] * (len(options) - 1))
    if len(out) > MAX_SHOW_IF:
        raise RunFormError("field %r: the options that have it make a condition of %d characters, over the run "
                           "form's %d: move the setting to fewer options" % (leaf, len(out), MAX_SHOW_IF))
    return out


def fields(params: list, table: list, raw: Optional[dict] = None, step_shows: Optional[dict] = None) -> list:
    """The run form's fields, one per Argo parameter in Argo order (design §1, the Fields table). `table` is
    the compose step's `--steps` table, `raw` the validated raw form (None: the render wrote none), and
    `step_shows` {step: showIf} from step_show_if. Group dropdowns carry optionDefaults filtered to the
    config fields the form serves; groups() gives the group list."""
    raw = raw if raw is not None else EMPTY_RAW
    types = raw.get("types") or {}
    option_defaults = raw.get("optionDefaults") or {}
    leaf_options = raw.get("leafOptions") or {}
    overrides = raw.get("overrides") or {}
    moved = overrides.get("fields") or {}
    step_shows = step_shows or {}
    rank = form_rank(params, table, overrides)
    own = [e["name"] for e in table]
    per_step = {"%s-%s" % (step, suffix): (step, suffix) for step in own for suffix in _STEP_SUFFIXES}
    producer = {promote_field(p["name"]): (e["name"], p["name"]) for e in table for p in e.get("produces", [])
                if p.get("type") == "dataset"}
    inputs = {name for e in table for name in e.get("inputs", [])}
    choices = {q["name"]: q.get("enum") or [] for q in params if q["name"] in option_defaults}
    out = []
    for p in params:
        name = p["name"]
        f = {"name": name, "group": rank[name][0], "platform": False}
        if is_image_field(name):
            f.update(source="platform", type="string", tier="hidden", platform=True, group="platform")
        elif name == "pipeline-info":
            f.update(source="info", type="text", tier="hidden", platform=True, group="info")
        elif name in per_step:
            step, suffix = per_step[name]
            f.update(source="run-setting", step=step, label=_RUN_SETTING_LABELS[suffix] % step)
            if suffix == "class":
                f.update(type="enum", picker="machine", tier="basic")
            else:
                f.update(type="duration", tier="advanced", pattern=DURATION_PATTERN, minMinutes=1,
                         maxMinutes=LONGEST_MINUTES)
        elif name in producer:
            step, dataset = producer[name]
            f.update(source="run-setting", type="enum", tier="advanced", step=step, label="Promote %s" % dataset)
        elif name == "config":
            f.update(source="advanced", type="yaml", tier="advanced")
        elif name in inputs:
            f.update(source="input", type="string", picker="dataset", tier="basic")
        elif name in option_defaults:
            f.update(source="group", type="enum", tier="basic")
        else:
            f.update(source="config", type=types.get(name) or ("enum" if p.get("enum") else "string"), tier="basic")
        if f.get("step") and step_shows.get(f["step"]):
            f["showIf"] = step_shows[f["step"]]
        spec = moved.get(name)
        if spec:
            if f["source"] not in DEVELOPER_SOURCES:
                raise RunFormError("_form.yaml changes %r, which the platform sets: remove it from fields in "
                                   "_form.yaml" % name)
            for key in ("label", "tier", "minimum", "maximum"):
                if key in spec:
                    f[key] = spec[key]
        if f["source"] == "config" and name in leaf_options:
            show = _leaf_show_if(name, leaf_options[name], choices)
            if show:
                f["showIf"] = show
        out.append(f)
    _option_defaults(out, option_defaults, groups(out, overrides))
    return out


def _option_defaults(field_list: list, option_defaults: dict, group_list: list) -> None:
    """Each group dropdown's optionDefaults, keeping only the leaves the form serves as config fields."""
    served_groups = {g["id"] for g in group_list if g["tier"] != "hidden"}
    served = {f["name"] for f in field_list if f["source"] == "config" and not f["platform"]
              and f["tier"] != "hidden" and f["group"] in served_groups and not is_image_field(f["name"])}
    for f in field_list:
        if f["source"] != "group":
            continue
        kept = {}
        for option, leaves in (option_defaults.get(f["name"]) or {}).items():
            leaves = {leaf: value for leaf, value in leaves.items() if leaf in served}
            if leaves:
                kept[option] = leaves
        if len(kept) > MAX_OPTION_DEFAULTS:
            raise RunFormError("group %r has %d options that change settings, over the run form's %d: split the "
                               "group" % (f["name"], len(kept), MAX_OPTION_DEFAULTS))
        if kept:
            f["optionDefaults"] = kept


def groups(field_list: list, overrides: Optional[dict] = None) -> list:
    """The groups the fields use, in the order a developer reads them (rank_key): the fixed GROUPS, and the
    _form.yaml groups inside `settings`. `order` is the place in that order."""
    fixed = dict(GROUPS)
    listed = (overrides or {}).get("groups") or []
    declared = {g["id"]: g for g in listed if isinstance(g, dict) and isinstance(g.get("id"), str)}
    used = []
    for f in field_list:
        if f["group"] not in used:
            used.append(f["group"])
    out = []
    for gid in sorted(used, key=lambda g: rank_key((g, 0), overrides)):
        if gid in fixed:
            out.append({"id": gid, "label": GROUP_LABELS[gid], "tier": fixed[gid]})
        elif gid in declared:
            out.append({"id": gid, "label": declared[gid].get("label") or gid,
                        "tier": declared[gid].get("tier") or "basic"})
        else:
            raise RunFormError("a field is in the group %r, which is not declared: add it to groups in _form.yaml"
                               % gid)
    if len(out) > MAX_GROUPS:
        raise RunFormError("the run form has %d groups, over the %d it may have: use fewer groups in _form.yaml"
                           % (len(out), MAX_GROUPS))
    for i, g in enumerate(out):
        g["order"] = i
    return out


# ------------------------------------------------------------------------------ check_form

def _anchored(pattern: str):
    """`pattern` (^...$) as a regex whose end is the end of the text: Python's `$` also matches before a final
    newline, which the schema's other readers (OSR's Go regexp) do not."""
    assert pattern.startswith("^") and pattern.endswith("$")
    return re.compile(pattern[:-1] + r"\Z")


_NAME_RE = _anchored(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$")
_GROUP_RE = _anchored(r"^[a-z][a-z0-9-]{0,62}$")
_ENGINE_RE = _anchored(ENGINE_VERSION_RE)
_COMMIT_RE = _anchored(COMMIT_RE)
_DIGEST_RE = _anchored(r"^sha256:[0-9a-f]{64}$")
FIELD_TYPES = ("string", "integer", "number", "boolean", "enum", "duration", "yaml", "text")
FIELD_SOURCES = ("info", "group", "config", "input", "run-setting", "advanced", "platform")
PICKERS = ("dataset", "machine")
TIER_ENUM = ("basic", "advanced", "hidden")
STEP_KINDS = step_contract.KINDS
_FIELD_REQUIRED = {"name", "group", "type", "tier", "platform", "source"}
_FIELD_KEYS = _FIELD_REQUIRED | {"picker", "label", "step", "showIf", "minimum", "maximum", "pattern", "minMinutes",
                                 "maxMinutes", "optionDefaults"}
_MACHINE_REQUIRED = {"name", "tier", "machineType", "accelerator", "allocatable", "description", "kind"}
_STEP_REQUIRED = {"name", "kind", "classField", "default", "offered", "refused"}
_STEP_KEYS = _STEP_REQUIRED | {"walltimeField", "checkpointField", "promoteFields", "showIf"}
_HPC_REQUIRED = {"provider", "partition", "qos", "gpus"}


def _bad(path: str, why: str):
    raise RunFormError("run form %s %s" % (path, why))


def _obj(value, path, required, allowed):
    if not isinstance(value, dict):
        _bad(path, "must be an object")
    extra = sorted(set(value) - set(allowed), key=str)
    if extra:
        _bad(path, "has unknown keys %s" % extra)
    missing = sorted(set(required) - set(value))
    if missing:
        _bad(path, "lacks the keys %s" % missing)
    return value


def _text(value, path, max_len=None, regex=None, nullable=False):
    if value is None and nullable:
        return
    if not isinstance(value, str):
        _bad(path, "must be text")
    if max_len is not None and len(value) > max_len:
        _bad(path, "is longer than %d characters" % max_len)
    if regex is not None and not regex.match(value):
        _bad(path, "does not match %s" % regex.pattern)


def _is_num(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _num(value, path, nullable=False):
    if value is None and nullable:
        return
    if not _is_num(value):
        _bad(path, "must be a number")


def _int(value, path, minimum=None, maximum=None):
    # JSON Schema: an integer is a number with no fraction, so 3.0 is one
    if not _is_num(value) or (isinstance(value, float) and not value.is_integer()):
        _bad(path, "must be an integer")
    if minimum is not None and value < minimum:
        _bad(path, "must be at least %s" % minimum)
    if maximum is not None and value > maximum:
        _bad(path, "must be at most %s" % maximum)


def _one_of(value, path, choices):
    if not isinstance(value, str) or value not in choices:
        _bad(path, "must be one of %s" % list(choices))


def _array(value, path, max_items=None, min_items=None):
    if not isinstance(value, list):
        _bad(path, "must be a list")
    if max_items is not None and len(value) > max_items:
        _bad(path, "has %d items, at most %d" % (len(value), max_items))
    if min_items is not None and len(value) < min_items:
        _bad(path, "has %d items, at least %d" % (len(value), min_items))
    return value


def _check_field(f, path):
    _obj(f, path, _FIELD_REQUIRED, _FIELD_KEYS)
    _text(f["name"], path + ".name", regex=_NAME_RE)
    _text(f["group"], path + ".group")
    _one_of(f["type"], path + ".type", FIELD_TYPES)
    _one_of(f["source"], path + ".source", FIELD_SOURCES)
    _one_of(f["tier"], path + ".tier", TIER_ENUM)
    if not isinstance(f["platform"], bool):
        _bad(path + ".platform", "must be true or false")
    if "picker" in f:
        _one_of(f["picker"], path + ".picker", PICKERS)
    if "label" in f:
        _text(f["label"], path + ".label", MAX_LABEL)
    if "step" in f:
        _text(f["step"], path + ".step", regex=_NAME_RE)
    if "showIf" in f:
        _text(f["showIf"], path + ".showIf", MAX_SHOW_IF)
    for bound in ("minimum", "maximum"):
        if bound in f:
            _num(f[bound], "%s.%s" % (path, bound))
    if "pattern" in f:
        _text(f["pattern"], path + ".pattern", 256)
    if "minMinutes" in f:
        _int(f["minMinutes"], path + ".minMinutes", minimum=1)
    if "maxMinutes" in f:
        _int(f["maxMinutes"], path + ".maxMinutes", maximum=LONGEST_MINUTES)
    if "optionDefaults" in f:
        od = f["optionDefaults"]
        if not isinstance(od, dict):
            _bad(path + ".optionDefaults", "must be an object")
        if len(od) > MAX_OPTION_DEFAULTS:
            _bad(path + ".optionDefaults", "has %d options, at most %d" % (len(od), MAX_OPTION_DEFAULTS))
        for option, leaves in od.items():
            where = "%s.optionDefaults[%s]" % (path, option)
            if not isinstance(leaves, dict):
                _bad(where, "must be an object")
            for leaf, value in leaves.items():
                _text(value, "%s[%s]" % (where, leaf), MAX_OPTION_VALUE)


def _check_machine(m, path):
    _obj(m, path, _MACHINE_REQUIRED, _MACHINE_REQUIRED | {"hpc"})
    _text(m["name"], path + ".name", regex=_NAME_RE)
    _one_of(m["tier"], path + ".tier", ("cpu", "gpu"))
    _text(m["machineType"], path + ".machineType", nullable=True)
    accel = m["accelerator"]
    if accel is not None:
        _obj(accel, path + ".accelerator", {"type", "count"}, {"type", "count"})
        _text(accel["type"], path + ".accelerator.type", nullable=True)
        _int(accel["count"], path + ".accelerator.count", minimum=1)
    alloc = _obj(m["allocatable"], path + ".allocatable", {"cpu", "memoryGiB", "ephemeralStorageGiB"},
                 {"cpu", "memoryGiB", "ephemeralStorageGiB"})
    _num(alloc["cpu"], path + ".allocatable.cpu")
    _num(alloc["memoryGiB"], path + ".allocatable.memoryGiB")
    _num(alloc["ephemeralStorageGiB"], path + ".allocatable.ephemeralStorageGiB", nullable=True)
    _text(m["description"], path + ".description", MAX_DESCRIPTION)
    _one_of(m["kind"], path + ".kind", ("cluster", "hpc"))
    if m["kind"] == "hpc" and "hpc" not in m:
        _bad(path, "is an hpc machine without hpc")
    if m["kind"] != "hpc" and "hpc" in m:
        _bad(path, "is a cluster machine with hpc")
    if "hpc" in m:
        hpc = _obj(m["hpc"], path + ".hpc", _HPC_REQUIRED, _HPC_REQUIRED | {"maxTimeLimitMinutes", "sharesNode"})
        for key in ("provider", "partition", "qos"):
            _text(hpc[key], "%s.hpc.%s" % (path, key))
        _int(hpc["gpus"], path + ".hpc.gpus", minimum=0)
        if "maxTimeLimitMinutes" in hpc:
            _int(hpc["maxTimeLimitMinutes"], path + ".hpc.maxTimeLimitMinutes", minimum=1)
        if "sharesNode" in hpc and hpc["sharesNode"] is not True:
            _bad(path + ".hpc.sharesNode", "must be true")


def _check_step(s, path):
    _obj(s, path, _STEP_REQUIRED, _STEP_KEYS)
    _text(s["name"], path + ".name", regex=_NAME_RE)
    _one_of(s["kind"], path + ".kind", STEP_KINDS)
    _text(s["classField"], path + ".classField", nullable=True)
    _text(s["default"], path + ".default")
    for i, name in enumerate(_array(s["offered"], path + ".offered", min_items=1)):
        _text(name, "%s.offered[%d]" % (path, i))
    if not isinstance(s["refused"], dict):
        _bad(path + ".refused", "must be an object")
    for cls, why in s["refused"].items():
        where = "%s.refused[%s]" % (path, cls)
        if not isinstance(why, list) or len(why) != 2:
            _bad(where, "must be [reason, remedy]")
        for i, text in enumerate(why):
            _text(text, "%s[%d]" % (where, i))
    for key in ("walltimeField", "checkpointField"):
        if key in s:
            _text(s[key], "%s.%s" % (path, key))
    if "promoteFields" in s:
        for i, name in enumerate(_array(s["promoteFields"], path + ".promoteFields")):
            _text(name, "%s.promoteFields[%d]" % (path, i))
    if "showIf" in s:
        _text(s["showIf"], path + ".showIf", MAX_SHOW_IF)


def check_form(form, stamped: bool = True) -> None:
    """Refuse a run form that the schema (docs/run-form.schema.json) refuses, by hand: closed keys, enums,
    patterns, lengths, item caps and the machine kind/hpc rule. Beyond the schema, each field's group must be
    one of `groups`, each field's step one of `steps`, and names are not repeated. `stamped=False` checks an
    unsealed form, whose stamp holds empty placeholders."""
    _obj(form, "", {"version", "stamp", "groups", "fields", "machines", "steps"},
         {"version", "stamp", "groups", "fields", "machines", "steps"})
    if not _is_num(form["version"]) or form["version"] != 1:
        _bad("version", "must be 1")
    stamp = _obj(form["stamp"], "stamp", {"by", "engine", "commit", "paramsDigest"},
                 {"by", "engine", "commit", "paramsDigest"})
    if stamp["by"] != STAMP_BY or not isinstance(stamp["by"], str):
        _bad("stamp.by", "must be %r" % STAMP_BY)
    for key, regex in (("engine", _ENGINE_RE), ("commit", _COMMIT_RE), ("paramsDigest", _DIGEST_RE)):
        if stamped:
            _text(stamp[key], "stamp." + key, regex=regex)
        elif stamp[key] != "":
            _bad("stamp." + key, "must be empty until the form is sealed")
    ids = []
    for i, g in enumerate(_array(form["groups"], "groups", MAX_GROUPS)):
        path = "groups[%d]" % i
        _obj(g, path, {"id", "label", "order", "tier"}, {"id", "label", "order", "tier"})
        _text(g["id"], path + ".id", regex=_GROUP_RE)
        _text(g["label"], path + ".label", MAX_LABEL)
        _int(g["order"], path + ".order", minimum=0)
        _one_of(g["tier"], path + ".tier", TIER_ENUM)
        ids.append(g["id"])
    field_list = _array(form["fields"], "fields", 1000)
    for i, f in enumerate(field_list):
        _check_field(f, "fields[%d]" % i)
    for i, m in enumerate(_array(form["machines"], "machines", 200)):
        _check_machine(m, "machines[%d]" % i)
    step_list = _array(form["steps"], "steps", 200)
    for i, s in enumerate(step_list):
        _check_step(s, "steps[%d]" % i)
    # beyond the schema
    if len(set(ids)) != len(ids):
        _bad("groups", "repeat a group id")
    step_names = [s["name"] for s in step_list]
    if len(set(step_names)) != len(step_names):
        _bad("steps", "repeat a step name")
    seen = set()
    for i, f in enumerate(field_list):
        if f["name"] in seen:
            _bad("fields[%d]" % i, "repeats the field %r" % f["name"])
        seen.add(f["name"])
        if f["group"] not in ids:
            _bad("fields[%d].group" % i, "is %r, which is not one of the groups %s" % (f["group"], ids))
        if "step" in f and f["step"] not in step_names:
            _bad("fields[%d].step" % i, "is %r, which is not one of the steps %s" % (f["step"], step_names))


def withheld_names(form: dict) -> set:
    """OSR's excludedNames: the fields a served form withholds (platform, source platform, tier hidden, or in a
    group that is hidden or not one of `groups`). `image-*` is withheld too, by prefix: see is_image_field."""
    served = {g["id"] for g in form["groups"] if g["tier"] != "hidden"}
    return {f["name"] for f in form["fields"]
            if f["platform"] or f["source"] == "platform" or f["tier"] == "hidden" or f["group"] not in served}


def check_join(form: dict, params: list) -> None:
    """OSR's checkJoin, which answers form_invalid when it fails: every Argo parameter that is not withheld has a
    field, and no showIf (of a served field or of a step) and no step field reference names a withheld one.
    A showIf is read as OSR reads it, every name-shaped token (quoted values and joiners too)."""
    excluded = withheld_names(form)

    def withheld(name):
        return is_image_field(name) or name in excluded

    names = {f["name"] for f in form["fields"]}
    for p in params:
        if p.get("name") and not withheld(p["name"]) and p["name"] not in names:
            raise RunFormError("the Argo parameter %r has no run form field" % p["name"])
    for s in form["steps"]:
        hit = [t for t in form_grammar.names(s.get("showIf", "")) if withheld(t)]
        if hit:
            raise RunFormError("step %r: its when= reads %r, which the run form does not serve (platform, hidden "
                               "or image-*): compare a field the form shows" % (s["name"], hit[0]))
        refs = [s.get("walltimeField"), s.get("checkpointField"), s.get("classField")] + list(s.get("promoteFields") or [])
        for ref in refs:
            if ref and withheld(ref):
                raise RunFormError("step %r refers to %r, which the run form does not serve (platform, hidden or "
                                   "image-*): rename the step if its name starts with image" % (s["name"], ref))
    for f in form["fields"]:
        if f["name"] in excluded:
            continue
        hit = [t for t in form_grammar.names(f.get("showIf", "")) if withheld(t)]
        if hit:
            raise RunFormError("the field %r is shown on a condition that reads %r, which the run form does not "
                               "serve (platform, hidden or image-*): change the condition or the field" % (f["name"], hit[0]))


# ------------------------------------------------------------------------------ encode and attach

def _key_sorted(value):
    if isinstance(value, dict):
        return {k: _key_sorted(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_key_sorted(v) for v in value]
    return value


def encode(form: dict) -> str:
    """The annotation's text: step_contract.wire of the form with every object's keys sorted (arrays keep their
    order), so two renders of one template are byte-identical."""
    return wire(_key_sorted(form))


def _placeholder_stamp() -> dict:
    return {"by": STAMP_BY, "engine": "", "commit": "", "paramsDigest": ""}


def build(wft: dict, ctx: dict, steps: list, offers: dict, raw: Optional[dict]) -> dict:
    """The unsealed run form of an enhanced template (its stamp holds empty placeholders)."""
    params = wft["spec"]["arguments"]["parameters"]
    table = step_table(steps)
    own = [e["name"] for e in table]
    raw = raw if raw is not None else EMPTY_RAW
    validate_raw(raw, params, own)
    shows = {step: step_show_if(step, raw["when"].get(step)) for step in own}
    form_steps = build_steps(steps, offers, ctx, params)
    for s in form_steps:
        if shows.get(s["name"]):
            s["showIf"] = shows[s["name"]]
    field_list = fields(params, table, raw, shows)
    form = {"version": 1, "stamp": _placeholder_stamp(), "groups": groups(field_list, raw["overrides"]),
            "fields": field_list, "machines": machines(ctx), "steps": form_steps}
    check_form(form, stamped=False)
    check_join(form, params)
    return form


def enforce_cap(form: dict) -> str:
    """The form's text, refused over RUN_FORM_CAP naming its largest section; a warning above RUN_FORM_WARN."""
    text = encode(form)
    size = len(text.encode("utf-8"))
    if size > RUN_FORM_CAP:
        sizes = {key: len(encode(value).encode("utf-8")) for key, value in form.items()}
        largest = max(sizes, key=sizes.get)
        raise RunFormError("the run form is %d bytes, over the %d byte cap; its largest section is %s (%d bytes): "
                           "make it smaller (fewer machine classes, steps, settings or group options)"
                           % (size, RUN_FORM_CAP, largest, sizes[largest]))
    if size > RUN_FORM_WARN:
        print("WARNING: the run form is %d bytes, near the %d byte cap" % (size, RUN_FORM_CAP), file=sys.stderr)
    return text


def attach(wft: dict, ctx: dict, steps: list, offers: dict, raw: Optional[dict]) -> dict:
    """Write the unsealed run form on `wft` as the annotation ui.kubecore.io/run-form and return it. The stamp
    holds empty placeholders, so until the seal writes it OSR answers form_invalid (the schema refuses an
    empty engine, commit and digest) and serves only the safe fallback. The Argo parameters are untouched."""
    form = build(wft, ctx, steps, offers, raw)
    text = enforce_cap(form)
    meta = wft.setdefault("metadata", {})
    if not isinstance(meta.get("annotations"), dict):
        meta["annotations"] = {}
    meta["annotations"][RUN_FORM_ANNOTATION] = text
    return form
