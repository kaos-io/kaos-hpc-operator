"""The run form's trusted side (PRD 1533/F-21 and 1533/F-22): take the render's raw form off the template and
check it. Trusted code: standard library and PyYAML only. It never imports hera, hydra or omegaconf (a test
asserts that), so the render side (derive_tree, authoring) is the only code that may use them.
"""
import json
import re
from typing import Callable, Optional

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
    pipeline's own steps. `parse_when` re-parses each `when` text.
    TODO(T2a): pass form_grammar's parser from enhance(); until then (None) the re-parse is skipped."""
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
        if parse_when is not None:
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
