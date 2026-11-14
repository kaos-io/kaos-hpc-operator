"""The render is untrusted: a pipeline definition may carry only the fields the step contract documents.

`kubecore.authoring` builds every template of a pipeline from `step(...)`. Nothing in that surface reaches a pod
spec beyond the image placeholder, the command, the step's own context variable, the GPU request and a few
platform labels and annotations. A render that carries anything else (a pipeline.py that skips `step(...)` and
writes the template by hand, or edits it afterwards) is refused here, before the platform adds its own wiring.

This is an allow-list: a field that is not named below is refused, whatever it is. The class of problem it closes:
a definition that makes a step read a Secret or ConfigMap, mount a service-account token or a host path, run as
another identity, weaken the pod's security settings, or place a pod outside the app's own pools. Because no
developer volume or variable survives, the platform's own volumes and variables cannot be shadowed: a name clash
does not exist.

Every refusal names the step, the field and the fix.
"""

from kubecore.step_contract import CONTEXT_ENV   # the one variable the authoring module sets

# The keys the platform's own authoring module writes (kubecore/authoring.py). Keep them in step with it.
WORKFLOW_KEYS = {"apiVersion", "kind", "metadata", "spec"}
METADATA_KEYS = {"name", "annotations", "labels"}   # annotations and labels: only the platform's own prefixes, which are removed
SPEC_KEYS = {"arguments", "entrypoint", "templates"}
TEMPLATE_KINDS = ("container", "dag", "steps")
TEMPLATE_KEYS = {"container": {"name", "container", "inputs", "outputs", "metadata"},
                 "dag": {"name", "dag", "inputs", "outputs", "metadata"},
                 "steps": {"name", "steps", "inputs", "outputs", "metadata"}}
CONTAINER_KEYS = {"name", "image", "command", "args", "env", "resources"}
ENV_KEYS = {"name", "value"}
RESOURCE_KEYS = {"requests", "limits"}
RESOURCE_NAMES = {"cpu", "memory", "ephemeral-storage", "nvidia.com/gpu"}
PARAMETER_KEYS = {"name", "value", "default", "description", "enum"}
VALUE_FROM_KEYS = {"path", "default", "expression"}
DAG_KEYS = {"tasks"}
TASK_KEYS = {"name", "template", "depends", "when", "arguments"}
STEP_KEYS = {"name", "template", "arguments", "when", "withParam", "continueOn"}
LABEL_KEYS = {"platform.kubecore.io/step", "platform.kubecore.io/step-kind", "platform.kubecore.io/produces-dataset"}
# `platform.kubecore.io/image` is not here: it would run any image with the app's identity.
ANNOTATION_KEYS = {"platform.kubecore.io/" + k for k in (
    "compute-class", "inject", "source", "shm", "workspace", "hpc", "hpc-resumable", "checkpoint-every", "idle-timeout")}

# What to tell the author, by the field that was found. The default covers any other field.
FIXES = {
    "volumes": "a step reads data through its context and kaos_lib; it mounts no Secret, ConfigMap, token or host path",
    "volumeMounts": "a step reads data through its context and kaos_lib; it mounts no Secret, ConfigMap, token or host path",
    "artifacts": "a step reads and writes data as datasets, models and artifacts through kaos_lib, not through an artifact store of its own",
    "volumeClaimTemplates": "ask for a workspace with the step's platform workspace setting, not a claim of your own",
    "envFrom": "a step gets its settings through its context (step.start()); it reads no Secret or ConfigMap as variables",
    "valueFrom": "a step gets its settings through its context (step.start()); it reads no Secret or ConfigMap by reference",
    "securityContext": "the platform sets the pod's security settings",
    "hostNetwork": "the platform sets the pod's security settings",
    "hostPID": "the platform sets the pod's security settings",
    "hostIPC": "the platform sets the pod's security settings",
    "hostAliases": "the platform sets the pod's networking",
    "nodeSelector": "choose a machine class with the step's class field on the run form",
    "tolerations": "choose a machine class with the step's class field on the run form",
    "affinity": "choose a machine class with the step's class field on the run form",
    "schedulerName": "choose a machine class with the step's class field on the run form",
    "priorityClassName": "choose a machine class with the step's class field on the run form",
    "initContainers": "the platform adds the helper containers a step needs",
    "sidecars": "the platform adds the helper containers a step needs",
    "imagePullSecrets": "the platform pulls the step's image",
    "automountServiceAccountToken": "the platform chooses the account every step runs as and what it mounts",
    "retryStrategy": "the platform re-runs a step lost with its machine; set resumable= on step(...)",
    "activeDeadlineSeconds": "set the step's time limit with walltime= on step(...)",
    "timeout": "set the step's time limit with walltime= on step(...)",
    "script": "write the step's code in steps/<name>/ and declare it with step(...)",
    "resource": "a step cannot create cluster objects",
    "templateRef": "a step runs a template of this pipeline only",
    "inline": "declare the step with step(...)",
    "image": "the platform sets the step's image from what CI built",
    "workingDir": "set the working directory in the step's Dockerfile",
    "ports": "a step opens no port",
    "env": "a step gets its settings through its context (step.start()); the platform sets its variables",
}
DEFAULT_FIX = "declare the step with step(...) and put its settings in config/; the platform adds everything else"


class DefinitionError(Exception):
    """A definition outside the contract. enhance raises it as EnhanceError."""


def _refuse(where: str, field: str, fix: str = None) -> None:
    fix = (fix or FIXES.get(field.rsplit(".", 1)[-1]) or DEFAULT_FIX).rstrip(".")
    raise DefinitionError(
        f"{where} carries '{field}', which a pipeline definition may not set. {fix[0].upper()}{fix[1:]}. "
        f"Remove it from your pipeline.")


def _only(where: str, mapping, allowed, prefix: str = "") -> None:
    if not isinstance(mapping, dict):
        raise DefinitionError(f"{where} is not a mapping. Remove it from your pipeline.")
    for key in mapping:
        if key not in allowed:
            _refuse(where, f"{prefix}{key}")


def _parameters(where: str, holder, kind: str) -> None:
    """Parameters (a name, a value) only. A parameter that takes its value from a ConfigMap, a Secret or another
    object (valueFrom) is refused, as is every artifact (an artifact can name a Secret for its store)."""
    if holder is None:
        return
    _only(where, holder, {"parameters"})
    for p in holder.get("parameters") or []:
        _only(f"{where} parameter '{p.get('name', '?')}'", p, PARAMETER_KEYS | ({"valueFrom"} if kind == "outputs" else set()))
        if "valueFrom" in p:
            _only(f"{where} parameter '{p.get('name', '?')}'", p["valueFrom"], VALUE_FROM_KEYS, "valueFrom.")


def _container(where: str, container) -> None:
    _only(where, container, CONTAINER_KEYS, "container.")
    for entry in container.get("env") or []:
        _only(f"{where} variable '{entry.get('name', '?')}'", entry, ENV_KEYS)
    for entry in container.get("env") or []:
        if entry.get("name") != CONTEXT_ENV:
            _refuse(f"{where} variable '{entry.get('name', '?')}'", "container.env",
                    "a step reads no environment variable of its own: its settings come through its context "
                    "(step.start()) and the platform sets the rest")
    resources = container.get("resources") or {}
    _only(where, resources, RESOURCE_KEYS, "container.resources.")
    for side in RESOURCE_KEYS & set(resources):
        for name in resources[side] or {}:
            if name not in RESOURCE_NAMES:
                _refuse(where, f"container.resources.{side}.{name}",
                        "a step asks for cpu, memory, disk or GPUs only, and the machine class gives the sizes")


def _metadata(where: str, metadata) -> None:
    _only(where, metadata, {"labels", "annotations"}, "metadata.")
    for key in (metadata or {}).get("labels") or {}:
        if key not in LABEL_KEYS:
            _refuse(where, f"metadata.labels.{key}", "labels and annotations of a step carry meaning for the platform "
                    "and the cluster; the platform sets the ones a step needs")
    for key in (metadata or {}).get("annotations") or {}:
        if key not in ANNOTATION_KEYS:
            _refuse(where, f"metadata.annotations.{key}", "labels and annotations of a step carry meaning for the "
                    "platform and the cluster; set a step's limits and settings with step(...) arguments")


def _tasks(where: str, tasks, allowed) -> None:
    for task in tasks:
        label = f"{where}, task '{task.get('name', '?')}'"
        _only(label, task, allowed)
        arguments = task.get("arguments")
        if arguments is not None:
            _only(label, arguments, {"parameters"}, "arguments.")
            for p in arguments.get("parameters") or []:
                _only(f"{label} argument '{p.get('name', '?')}'", p, {"name", "value"})


def refuse_outside_contract(wft: dict) -> None:
    """Refuse a raw WorkflowTemplate that carries a field the step contract does not document."""
    _only("the pipeline", wft, WORKFLOW_KEYS)
    metadata = wft.get("metadata") or {}
    _only("the pipeline's metadata", metadata, METADATA_KEYS, "metadata.")
    for field in ("labels", "annotations"):
        for key in metadata.get(field) or {}:
            if not key.startswith(("ui.kubecore.io/", "platform.kubecore.io/")):
                _refuse("the pipeline's metadata", f"metadata.{field}.{key}", "the platform sets the pipeline's labels and annotations")
    spec = wft.get("spec") or {}
    _only("the pipeline", spec, SPEC_KEYS, "spec.")
    _parameters("the pipeline's arguments", spec.get("arguments"), "arguments")
    for tpl in spec.get("templates") or []:
        name = tpl.get("name", "?")
        kinds = [k for k in tpl if k not in TEMPLATE_KEYS["container"] | TEMPLATE_KEYS["dag"] | TEMPLATE_KEYS["steps"]]
        for key in kinds:
            _refuse(f"step '{name}'", key)
        present = [k for k in TEMPLATE_KINDS if k in tpl]
        if len(present) != 1:
            _refuse(f"step '{name}'", "template type", "a template is one container, one dag or one list of steps")
        where = f"step '{name}'"
        kind = present[0]
        _only(where, tpl, TEMPLATE_KEYS[kind])
        _parameters(f"{where} inputs", tpl.get("inputs"), "inputs")
        _parameters(f"{where} outputs", tpl.get("outputs"), "outputs")
        if "metadata" in tpl:
            _metadata(where, tpl["metadata"])
        if kind == "container":
            _container(where, tpl["container"])
        elif kind == "dag":
            _only(where, tpl["dag"], DAG_KEYS, "dag.")
            _tasks(where, tpl["dag"].get("tasks") or [], TASK_KEYS)
        else:
            for group in tpl["steps"] or []:
                _tasks(where, group, STEP_KEYS)
