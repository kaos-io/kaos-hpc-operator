"""The run form's trusted side (PRD 1533/F-21 and 1533/F-22): take the render's raw form off the template and
check it. Trusted code: standard library and PyYAML only. It never imports hera, hydra or omegaconf (a test
asserts that), so the render side (derive_tree, authoring) is the only code that may use them.
"""
import json
import re
from typing import Callable, Optional

from kubecore import step_contract
from kubecore import form_grammar
from kubecore.enhance import platform_annotations, tier_default
from kubecore.meluxina import HPC_TWIN_TEMPLATE, STEP_KIND_LABEL, hpc_classes
from kubecore.run_settings import checkpoint_field, class_field, promote_field, walltime_field

from kubecore.step_contract import (
    ENGINE_VERSION_ANNOTATION, PARAMS_DIGEST_KEY, PIPELINE_VERSION_KEY, RAW_FORM_ANNOTATION, RAW_FORM_CAP,
    SOURCE_COMMIT_KEY,
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
            # TODO(T8): enforce that `gid` is a declared override group or a fixed group (check_form).
            if not isinstance(gid, str) or not GROUP_ID_RE.match(gid):
                raise RunFormError("%s: group %r must match %s" % (where, gid, GROUP_ID_RE.pattern))


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
    declared = [g.get("id") for g in ((overrides or {}).get("groups") or [])]
    if group in declared:
        order = (overrides["groups"][declared.index(group)] or {}).get("order")
        return (fixed.index("settings"), 1, order if isinstance(order, int) else 0, declared.index(group), index)
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


def steps(steps: list, offers: dict, ctx: dict, params: list) -> list:
    """The run form's steps: for each step of the developer's pipeline, the class choice it offers (the
    output of enhance.class_offer, verbatim), its default, its kind and the names of its run-setting fields.
    `steps` are the workflow's step templates, `offers` is {step: class_offer or None (pinned)}, `params` the
    workflow's Argo parameters (a field is named only when the form has it)."""
    have = {p["name"] for p in params}
    values = {p["name"]: p.get("value") for p in params}
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    args = ((compose or {}).get("container") or {}).get("args") or []
    table = json.loads(args[args.index("--steps") + 1]) if "--steps" in args else []
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
