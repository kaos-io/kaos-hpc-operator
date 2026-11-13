"""Platform-owned enhancer. The successor of render_wft.py's platform half.

Takes the RAW WorkflowTemplate a developer's vanilla-Hera pipeline.py rendered
and a platform context (pipeline-context.yaml, sourced from the
`{app}-pipeline-context` ConfigMap the operator writes into the app namespace)
and injects everything the developer never had to think about: namespace,
service account, secrets, compute-class routing, node scheduling, image
supply-chain wiring, sizing (a step gets the node of the class it is given, see
enhance_class_sizing), per-step compute-class dropdowns ({step}-class, matching the
live render-wft, listing the pool's compute classes), and the read-only
pipeline-info form parameter.

Golden rule: only fill ABSENT keys. If a developer already set a field (a
nodeSelector, an env var, a memory request) we leave it alone. The one
deliberate exception is `image`: the platform always overwrites it, because the
image running in a pipeline step is a supply-chain concern (Zot registry,
scanned, versioned) that a developer's local placeholder must never leak into
(see CON-18 / Zot registry). The `platform.kubecore.io/image` annotation is the
sanctioned escape hatch for unbuilt utility images.

Compute model (matches the live in-cluster WorkflowTemplate): each step gets a
`{step}-class` submit-form parameter defaulting to the pool's cpu/gpu tier
class; the step's nodeSelector is
`platform.kubecore.io/nodegroup-type: {{workflow.parameters.{step}-class}}`, so
the submitter routes each step onto any of the KubePool's allowed classes. The
allowed classes come from the pool (computeClasses.all).

GPU convention (matches live render-wft): a template is a GPU step iff its
rendered resources request `nvidia.com/gpu`. No annotation.

Platform annotation vocabulary (all optional, validated — an unknown
`platform.kubecore.io/*` key FAILS the enhance, typo protection; this is the
release-gate rule):

  platform.kubecore.io/compute-class  pin a named class from the pool
                                      catalog (computeClasses.all); disables
                                      the {step}-class dropdown for this step
  platform.kubecore.io/inject         "none" or comma list like
                                      "mlflow=false" — opt out of
                                      data-source env/secret wiring
  platform.kubecore.io/source         images-ConfigMap key differs
                                      from step name (shared image)
  platform.kubecore.io/image          use this image verbatim, skip
                                      the ConfigMap indirection
  platform.kubecore.io/shm            /dev/shm emptyDir sizeLimit (e.g. "4Gi")
  platform.kubecore.io/workspace      per-run RWO PVC at /workspace (e.g. "50Gi")
  platform.kubecore.io/hpc            the default of the step's time limit (authoring:
                                      walltime=; the run form's <step>-walltime sets it)
  platform.kubecore.io/checkpoint-every  default minutes between checkpoint copies of
                                      the step's KAOS_CHECKPOINT_DIR (the run form's <step>-checkpoint-every sets it); "off"
                                      removes the persistence helper (see
                                      kubecore/resilience.py)
  platform.kubecore.io/idle-timeout   minutes a GPU step may show no GPU use
                                      and no new log line before the pool's
                                      idle watchdog ends it as Idle; "0" turns
                                      it off (carried to the pod unchanged)

GPU driver prelude (enhance_gpu_driver): a gpu step that runs on a KAOS GPU node has its command wrapped in a
POSIX `sh -c` that prepends the node's NVIDIA driver paths to LD_LIBRARY_PATH / PATH, fails the step in its
first second when libcuda cannot be initialised, then `exec`s the original command. Never on an hpc (Slurm)
step: that runs through the follower and Apptainer `--nv`.

connections (pipeline-context `connections[]`): optional envFrom ConfigMap refs on every
developer step, never on hpc-follower.

This file deals in plain dicts (parsed with pyyaml), not Hera objects. The
enhancer runs at CI render time, after Hera has already produced Argo-native
YAML, so it has no reason to depend on Hera at all — this is what keeps the
platform layer decoupled from whatever authoring library or version a
developer's repo pins.
"""

import argparse
import copy
import json
from pathlib import Path

import yaml

from kubecore.kaos_lib_support import ENV as KAOS_LIB_SUPPORT_ENV
from kubecore.kaos_lib_support import support_value
from kubecore.meluxina import (
    HPC_TWIN_TEMPLATE,
    STEP_KIND_LABEL,
    enhance_hpc,
    hpc_classes,
    parse_time_limit,
    runs_on_hpc,
    step_resumable_lead,
)
from kubecore.outputs_repo import outputs_repo
from kubecore.run_settings import (
    DEFAULT_WALLTIME,
    PROMOTE_CHOICES,
    SETTINGS_PARAM,
    SETTINGS_PATH,
    WALLTIME_EXAMPLE,
    add_input,
    add_task_argument,
    calls,
    checkpoint_field,
    class_field,
    DEFAULT_PARALLEL,
    combinations,
    matrix_field,
    matrix_setting,
    parallel_field,
    promote_field,
    setting,
    walltime_field,
)
from kubecore.resilience import enhance_resilience, every_field_default, every_minutes, shared_templates, write_helper_configmap
from kubecore.step_contract import (
    CLUSTER_ONLY_KINDS,
    CONTEXT_PARAM,
    DATASET_PRODUCER_LABEL,
    DATASET_PRODUCERS_STAY_ON_CLUSTER,
    IMAGE_INPUT,
    IMAGES_PARAM,
    IMAGES_PATH,
    MAX_APP_NAME,
    PROMOTE_PARAM,
    PROMOTE_STEP,
    RESULT_PARAM,
    TEMPLATE_CAP,
    dataset_name_problem,
    image_of,
    gather_task,
    result_of,
    template_bytes,
)

ANNOTATION_PREFIX = "platform.kubecore.io/"
KNOWN_ANNOTATIONS = {"compute-class", "inject", "source", "image", "shm", "workspace", "hpc", "hpc-resumable",
                     "checkpoint-every", "idle-timeout"}
WORKSPACE_MOUNT = "/workspace"
# Mount path for the Zitadel machine-key secret backing MLflow bearer auth
# (#868) — matches the previous DSL's working reference (/etc/mlflow-svc).
MLFLOW_SVC_MOUNT = "/etc/mlflow-svc"


# GPU driver prelude (see enhance_gpu_driver). GKE and EKS GPU nodes mount the NVIDIA driver under
# /usr/local/nvidia; a step image built on a plain python/pip base has none of it on its search paths, so torch
# cannot load libcuda and silently trains on CPU. The prelude PREPENDS to the image's own LD_LIBRARY_PATH and
# PATH (a fixed value would break conda and nvidia/cuda images). Positional args: $1 = GPUs the step was given
# ("0" when a cpu class was chosen: no check), then the original command and args, which `exec "$@"` keeps
# exactly (exit code, signals, argv). No "{{" and no "$(NAME)" form may appear in it: Argo and Kubernetes
# would expand them inside the command.
GPU_PRELUDE_NAME = "kaos-gpu-prelude"
GPU_DRIVER_CHECK_PY = (
    'import ctypes, sys\n'
    'try:\n'
    '    lib = ctypes.CDLL("libcuda.so.1")\n'
    'except OSError as e:\n'
    '    sys.exit("libcuda.so.1 not found: " + str(e).replace(chr(10), " "))\n'
    'rc = lib.cuInit(0)\n'
    'if rc != 0:\n'
    '    sys.exit("cuInit returned error " + str(rc))\n'
)
GPU_PRELUDE_SCRIPT = (
    "LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/usr/local/nvidia/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}\n"
    "export LD_LIBRARY_PATH\n"
    "PATH=/usr/local/nvidia/bin:$PATH\n"
    "export PATH\n"
    "kaos_gpus=$1\n"
    "shift\n"
    'if [ "$kaos_gpus" != "0" ]; then\n'
    "  if command -v python3 >/dev/null 2>&1; then\n"
    "    if ! kaos_why=`python3 -c '" + GPU_DRIVER_CHECK_PY + "' 2>&1`; then\n"
    '      echo "KAOS: this GPU step cannot reach the NVIDIA driver ($kaos_why); '
    'the image or node is not set up for GPU" >&2\n'
    "      exit 1\n"
    "    fi\n"
    "  else\n"
    '    echo "KAOS: python3 not found in the image; skipping the NVIDIA driver check" >&2\n'
    "  fi\n"
    "fi\n"
    'exec "$@"\n'
)


class EnhanceError(Exception):
    pass


# PRD-HPC-1461 D-79 F-37: template names only the platform may produce (the follower twin, and its
# pre-AC-12 name that a Workflow submitted before the rename may still call; the run's final task).
RESERVED_TEMPLATE_NAMES = (HPC_TWIN_TEMPLATE, "meluxina-run", PROMOTE_STEP)


def refuse_client_identity(wft: dict) -> None:
    """Refuse a raw WorkflowTemplate that picks its own service account or patches its pod spec.

    Why: the ServiceAccount `hpc-submitter` may create HPC jobs (AC-18) and only the platform follower
    template may run as it. A pipeline.py that set `serviceAccountName`, `executor.serviceAccountName`
    or `podSpecPatch` could run its own code as that account, so the render fails instead. The platform
    adds all of these itself, later, in enhance(); a raw template never carries any of them.
    """
    spec = wft.get("spec") or {}
    if "serviceAccountName" in spec:
        raise EnhanceError(
            "pipeline.py sets a service account for the whole pipeline (serviceAccountName). The platform "
            "chooses the account every step runs as; remove it from your pipeline.")
    if "podSpecPatch" in spec:
        raise EnhanceError(
            "pipeline.py sets podSpecPatch on the pipeline. The platform sizes and places pods itself; "
            "use step(...) resources and compute classes instead.")
    for tpl in spec.get("templates") or []:
        name = tpl.get("name", "?")
        if name in RESERVED_TEMPLATE_NAMES:
            raise EnhanceError(
                f"step or template '{name}' uses a name the platform reserves for its own templates "
                f"({', '.join(RESERVED_TEMPLATE_NAMES)}). Rename it.")
        if "serviceAccountName" in tpl or "serviceAccountName" in (tpl.get("executor") or {}):
            raise EnhanceError(
                f"step '{name}' sets a service account (serviceAccountName). The platform chooses the "
                f"account every step runs as; remove it from your pipeline.")
        if "podSpecPatch" in tpl:
            raise EnhanceError(
                f"step '{name}' sets podSpecPatch. The platform sizes and places pods itself; use "
                f"step(...) resources and compute classes instead.")


def _param_names(parameters: list) -> set:
    return {p["name"] for p in parameters}


def images_configmap(ctx: dict) -> str:
    """The pipeline-images ConfigMap name — {app}-pipeline-images, from
    context (NOT a hardcoded literal)."""
    app = ctx.get("app") or ctx.get("pipelineName")
    if not app:
        raise EnhanceError("context is missing 'app' — cannot derive the pipeline-images ConfigMap name")
    return f"{app}-pipeline-images"


# ---------------------------------------------------------------- annotations


def platform_annotations(step: dict) -> dict:
    """Extract + validate this step's platform.kubecore.io/* annotations.

    Unknown platform.kubecore.io/* keys fail the enhance (typo
    protection — the release-gate rule).
    """
    annotations = step.get("metadata", {}).get("annotations", {})
    result = {}
    for key, value in annotations.items():
        if not key.startswith(ANNOTATION_PREFIX):
            continue
        short = key[len(ANNOTATION_PREFIX):]
        if short not in KNOWN_ANNOTATIONS:
            known = ", ".join(sorted(ANNOTATION_PREFIX + k for k in KNOWN_ANNOTATIONS))
            raise EnhanceError(
                f"step '{step['name']}': unknown platform annotation '{key}' (known: {known})"
            )
        result[short] = value
    return result


def inject_disabled(annots: dict, source_name: str) -> bool:
    """True if `platform.kubecore.io/inject` opts this step out of
    the named data source ("mlflow", "lakefs")."""
    spec = annots.get("inject")
    if spec is None:
        return False
    if spec.strip() == "none":
        return True
    return any(item.strip() == f"{source_name}=false" for item in spec.split(","))


# ------------------------------------------------------------------ metadata


def pipeline_wft_name(ctx: dict) -> str:
    """The authoritative WorkflowTemplate name: {app}-pipeline. This is the
    convention the k8smlapp composition seeds the placeholder with
    ({{ $appName }}-pipeline), so the CI-rendered WFT must match it — otherwise
    it lands as a DIFFERENT object in the shared ml-{project} namespace."""
    app = ctx.get("app")
    if not app:
        raise EnhanceError("context is missing 'app' — cannot derive the WorkflowTemplate name")
    return f"{app}-pipeline"


def enhance_metadata(wft: dict, ctx: dict) -> None:
    meta = wft["metadata"]

    # FORCE the WFT name to the app-authoritative {app}-pipeline, OVERRIDING
    # whatever pipeline.py declared. The name is an IDENTITY / multi-tenant
    # isolation concern the platform must own — not a developer choice. A
    # template's hardcoded pipeline("ml-pipeline") copied into every
    # seeded app would otherwise make app B's render OVERWRITE app A's live WFT
    # in the shared ml-{project} namespace (the cross-app contamination class).
    # This is the one-and-only name authority, same rationale as force-
    # overriding `image` (a supply-chain concern) — see enhance_image.
    authoritative = pipeline_wft_name(ctx)
    if meta.get("name") != authoritative:
        meta["name"] = authoritative
    meta.setdefault("namespace", ctx["namespace"])

    labels = meta.setdefault("labels", {})
    labels.setdefault("platform.kubecore.io/project", ctx["project"])
    labels.setdefault("platform.kubecore.io/app", ctx["app"])
    labels.setdefault("platform.kubecore.io/managed-by", "kubecore-ci")

    annotations = meta.setdefault("annotations", {})
    annotations.setdefault("platform.kubecore.io/generated-by", "ml-ci-build/hera-render")
    annotations.setdefault("platform.kubecore.io/cost-tracking", "enabled")


RUN_RETENTION_DAYS = 35
HPC_MIN_RETENTION_DAYS = 32
POD_GC_STRATEGY, POD_GC_DELAY = "OnWorkflowCompletion", "168h"


def run_retention_seconds(ctx: dict, hpc_steps: bool = False) -> int:
    """How long a finished run is kept: the context's `runRetentionDays` (a whole number, 1 to 3650), else 35 days.

    Why 35 and not 30: the HPC budget is a calendar month, and the budget ledger is derived from the HPC job records,
    which are deleted with their run. A run that ends at t is therefore kept longer than any month (31 days at most), so
    the record of its spend is never collected before the month of t has closed. A pipeline with a step that can run on
    HPC may not be set below 32 days; one with no such step may go down to 1."""
    days = ctx.get("runRetentionDays", RUN_RETENTION_DAYS) if "runRetentionDays" in ctx else RUN_RETENTION_DAYS
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= 3650:
        raise EnhanceError(f"context runRetentionDays={days!r}: expected a whole number of days from 1 to 3650")
    if hpc_steps and days < HPC_MIN_RETENTION_DAYS:
        raise EnhanceError(f"context runRetentionDays={days}: runs with HPC steps are kept at least {HPC_MIN_RETENTION_DAYS} "
                           "days, so the month's HPC spend stays counted")
    return days * 86400


def has_hpc_steps(steps: list, ctx: dict) -> bool:
    """A step that can run on HPC (Slurm) in this pipeline: the project has HPC classes and the step is not of a kind
    that never runs there."""
    return bool(hpc_classes(ctx)) and any(runs_on_hpc(step) for step in steps)


def enhance_spec_top_level(spec: dict, ctx: dict) -> None:
    spec.setdefault("serviceAccountName", ctx["serviceAccountName"])
    # Finished runs expire (a Workflow made from this template takes both from it: ttlStrategy and podGC are fields of
    # the template spec that a Workflow with a workflowTemplateRef is built from): without it they pile up in the
    # cluster, each with its HPC job records. The run record is kept for the retention (35 days by default, see
    # run_retention_seconds). The pods are kept for 7 days after the run ends, succeeded or failed, then deleted: a
    # step's log lives only in its pod until logs are stored. podGC.deleteDelayDuration needs Argo Workflows 3.5 or later.
    seconds = run_retention_seconds(ctx, has_hpc_steps(step_templates(spec), ctx))
    spec.setdefault("ttlStrategy", {"secondsAfterSuccess": seconds, "secondsAfterFailure": seconds})
    spec.setdefault("podGC", {"strategy": POD_GC_STRATEGY, "deleteDelayDuration": POD_GC_DELAY})


def step_templates(spec: dict) -> list:
    """Container-backed templates only (skip the DAG template itself)."""
    return [t for t in spec["templates"] if "container" in t]


# ----------------------------------------------------------------- arguments


# ------------------------------------------------------------------- compute


def is_gpu_step(step: dict) -> bool:
    """Live render-wft convention: GPU iff resources request nvidia.com/gpu."""
    resources = step["container"].get("resources", {})
    return "nvidia.com/gpu" in resources.get("requests", {}) or "nvidia.com/gpu" in resources.get(
        "limits", {}
    )


def step_tier(step: dict) -> str:
    return "gpu" if is_gpu_step(step) else "cpu"


def tier_default(ctx: dict, step: dict) -> dict:
    """The class a step defaults to: its own tier's default, else the OTHER
    tier's (an ml environment may declare only cpu or only gpu flavours —
    PRD-1124 D-02 — and a gpu-declaring step submitted with a cpu class
    legitimately runs CPU-only, see enhance_gpu_routing). Neither tier
    present is an authoring error: the environment declares no compute."""
    classes = ctx["computeClasses"]
    tier = step_tier(step)
    for candidate in (tier, "cpu" if tier == "gpu" else "gpu"):
        if classes.get(candidate):
            return classes[candidate]
    raise EnhanceError(
        f"step '{step['name']}': the target ml environment declares no compute flavours "
        f"(environments[].ml.cpu / .gpu are both empty) — add at least one"
    )


def default_class_name(step: dict, ctx: dict, annots: dict) -> str:
    """The step's default compute class name: a pinned compute-class
    annotation wins, else the pool's gpu/cpu tier default."""
    pinned = annots.get("compute-class")
    if pinned is not None:
        produces_dataset = ((step.get("metadata") or {}).get("labels") or {}).get(DATASET_PRODUCER_LABEL) == "true"
        if DATASET_PRODUCERS_STAY_ON_CLUSTER and produces_dataset and pinned in {c["name"] for c in hpc_classes(ctx)}:
            raise EnhanceError(
                f"step '{step['name']}': a step that produces a dataset runs on the cluster for now; HPC (Slurm) "
                f"classes are offered to it once jobs there hold the app's own rights.")
        if pinned not in {c["name"] for c in ctx["computeClasses"]["all"]}:
            known = ", ".join(c["name"] for c in ctx["computeClasses"]["all"])
            raise EnhanceError(
                f"step '{step['name']}': compute-class '{pinned}' not in pool catalog ({known})"
            )
        return pinned
    return tier_default(ctx, step)["name"]


def resolve_compute_class(step: dict, ctx: dict, annots: dict) -> dict:
    """The class config used for whole-node sizing/tolerations. Pinned class
    merges over its tier default (catalog entries omit toleration config)."""
    name = default_class_name(step, ctx, annots)
    for cls in ctx["computeClasses"]["all"]:
        if cls["name"] == name:
            base = ctx["computeClasses"].get(cls.get("tier", step_tier(step))) or tier_default(ctx, step)
            return {**base, **cls}
    # tier default itself (name came straight from computeClasses.{cpu,gpu})
    return tier_default(ctx, step)


def catalog_toleration_pairs(ctx: dict, step: dict, annots: dict) -> list:
    """Unique (tolerationKey, tolerationValue) pairs a step must tolerate.

    A pinned (compute-class annotation) step tolerates only its pinned
    class's taint — it can never be routed elsewhere. Every other step
    tolerates the FULL catalog (#865): the {step}-class dropdown may route
    it onto any class pool at submit time, and tolerations render statically.
    """
    classes = list(ctx["computeClasses"]["all"])
    if "compute-class" in annots:
        classes = [c for c in classes if c["name"] == annots["compute-class"]] or classes
    pairs = []
    for cls in classes:
        tier_default = ctx["computeClasses"].get(cls.get("tier", "cpu"), {})
        merged = {**tier_default, **cls}
        pair = (merged.get("tolerationKey"), merged.get("tolerationValue"))
        if pair[0] and pair not in pairs:
            pairs.append(pair)
    return pairs


def gpu_class_names(ctx: dict) -> list:
    """Names of every gpu-tier class in the pool catalog (dedup, ordered)."""
    names = [c["name"] for c in ctx["computeClasses"]["all"] if c.get("tier") == "gpu"]
    tier_default = ctx["computeClasses"].get("gpu", {}).get("name")
    if tier_default and tier_default not in names:
        names.insert(0, tier_default)
    return names


def node_sizes(ctx: dict) -> dict:
    """{class: (cpu, memoryGiB)}: what one pod may ask of a node of each class the pool offers.

    The operator computes it per class (`allocatable` in the pipeline context): the machine's capacity
    less the node system's reservation, the DaemonSets that run on every node, a safety margin and, on a
    GPU node, the accelerator's headroom, rounded down to whole units. The engine adds nothing and knows
    no machine type. A class without it cannot be sized, so a pool that offers one fails the render.
    """
    classes = ctx["computeClasses"]
    tier_defaults = {c["name"]: c for c in (classes.get("cpu"), classes.get("gpu")) if c}
    sizes, unsized = {}, []
    for cls in list(classes["all"]) + [c for n, c in tier_defaults.items() if n not in {a["name"] for a in classes["all"]}]:
        alloc = cls.get("allocatable") or (tier_defaults.get(cls["name"]) or {}).get("allocatable")
        if alloc and alloc.get("cpu") and alloc.get("memoryGiB"):
            sizes[cls["name"]] = (alloc["cpu"], alloc["memoryGiB"])
        else:
            unsized.append(f"'{cls['name']}' (machine type '{cls.get('machineType') or '?'}')")
    if unsized:
        raise EnhanceError(
            f"the pool offers the compute class{'es' if len(unsized) > 1 else ''} {', '.join(unsized)}, but the "
            f"platform cannot say how much of such a node a pod may use, so a step could not be sized on "
            f"{'them' if len(unsized) > 1 else 'it'}. The operator knows the sizes of the machine types in its "
            f"table only: ask the platform team to add the machine type.")
    return sizes


# Of a class's usable disk the images of the node's pods share it: a step asks for the rest.
IMAGE_RESERVE_GIB = 10


def node_disks(ctx: dict) -> dict:
    """{class: ephemeral-storage request}: the disk a step asks for on each class the pool offers.

    The operator puts the class's usable disk in the pipeline context, a whole number of GiB, next to
    `allocatable` cpu and memory: `allocatable.ephemeralStorageGiB` (absent when the disk is unknown). It is
    the node's allocatable ephemeral storage less a margin, and container images share that disk, so a step
    asks for it less IMAGE_RESERVE_GIB (at least 1Gi, and never more than the node can give). A class
    without it gets DISK_REQUEST_DEFAULT: unlike cpu and memory, an unknown disk does not refuse the render.
    The key is read here and nowhere else.
    """
    classes = ctx["computeClasses"]
    tier_defaults = {c["name"]: c for c in (classes.get("cpu"), classes.get("gpu")) if c}
    disks = {}
    for cls in list(classes["all"]) + list(tier_defaults.values()):
        alloc = cls.get("allocatable") or (tier_defaults.get(cls["name"]) or {}).get("allocatable") or {}
        usable = alloc.get("ephemeralStorageGiB")
        disks[cls["name"]] = f"{max(1, usable - IMAGE_RESERVE_GIB)}Gi" if usable else DISK_REQUEST_DEFAULT
    return disks


def enhance_class_sizing(spec: dict, step: dict, ctx: dict, annots: dict) -> None:
    """A step gets the node of the class it is given: its requests are what a node of that class
    offers a pod (`node_sizes`), cpu and memory, limits equal to requests, and the disk of its machine
    (`node_disks`), a request and no limit.

    Nothing to type and nothing to mistype: the run form has no cpu, memory or disk field. The class is
    chosen at submit time, so the numbers follow it the way the GPU does (enhance_gpu_routing): the
    template takes inputs `cpu`, `mem` and `disk`, a `podSpecPatch` puts them in the main container's
    requests (and cpu and memory in its limits), and every DAG task that runs the step computes them
    from `{step}-class` with an expression. A class that is not in the table (an HPC class: that twin never runs this task) gets the
    default class's numbers. A step pinned to one class has the same numbers as plain container resources.
    Limits equal requests so a step that overruns memory is stopped inside its own cgroup instead of
    pressing on the node's daemons, and cannot starve them.
    """
    if "compute-class" in annots:
        return  # pinned: enhance_scheduling sets the static requests
    sizes = node_sizes(ctx)
    disks = node_disks(ctx)
    default_name = default_class_name(step, ctx, annots)
    default_cpu, default_mem = sizes[default_name]
    default_disk = disks[default_name]
    step_name = step["name"]
    input_params = step.setdefault("inputs", {}).setdefault("parameters", [])
    for name, value in (("cpu", str(default_cpu)), ("mem", f"{default_mem}Gi"), ("disk", default_disk)):
        if not any(p.get("name") == name for p in input_params):
            input_params.append({"name": name, "default": value})
    patch_obj = yaml.safe_load(step.get("podSpecPatch") or "") or {}
    containers = patch_obj.setdefault("containers", [])
    main = next((c for c in containers if c.get("name") == "main"), None)
    if main is None:
        main = {"name": "main"}
        containers.append(main)
    res = main.setdefault("resources", {})
    for field_name, param in (("cpu", "cpu"), ("memory", "mem")):
        res.setdefault("requests", {})[field_name] = "{{inputs.parameters.%s}}" % param
        res.setdefault("limits", {})[field_name] = "{{inputs.parameters.%s}}" % param
    res.setdefault("requests", {})["ephemeral-storage"] = "{{inputs.parameters.disk}}"   # a request, no limit
    step["podSpecPatch"] = yaml.safe_dump(patch_obj, default_flow_style=False)
    cpu_map = "{" + ", ".join(f'"{n}": "{c}"' for n, (c, _) in sizes.items()) + "}"
    mem_map = "{" + ", ".join(f'"{n}": "{m}Gi"' for n, (_, m) in sizes.items()) + "}"
    disk_map = "{" + ", ".join(f'"{n}": "{d}"' for n, d in disks.items() if n in sizes) + "}"
    klass = f"workflow.parameters['{step_name}-class']"
    # A map literal never closes with `}}`, which would end the expression.
    values = {"cpu": f'{{{{=({cpu_map}[{klass}]) ?? "{default_cpu}"}}}}',
              "mem": f'{{{{=({mem_map}[{klass}]) ?? "{default_mem}Gi"}}}}',
              "disk": f'{{{{=({disk_map}[{klass}]) ?? "{default_disk}"}}}}'}
    for task, _ in calls(spec, step_name):
        params = task.setdefault("arguments", {}).setdefault("parameters", [])
        for name, expression in values.items():
            if not any(p.get("name") == name for p in params):
                params.append({"name": name, "value": expression})


def enhance_gpu_routing(spec: dict, step: dict, ctx: dict, annots: dict) -> None:
    """Make a gpu-declaring step class-routable at submit time (#865 fix 2).

    A gpu=True step used to carry a STATIC nvidia.com/gpu request +
    GPU-only scheduling, so submitting {step}-class=<cpu class> produced a
    pod that targets a CPU node while requesting a GPU — permanently
    Unschedulable. CPU smoke tests and CPU-only quantization (routine on the
    previous DSL, which had separate cpu/gpu step variants) were impossible.

    Mechanics — static render, runtime routing, using only PROVEN eval
    sites (the platform's podSpecPatch sizing model + {{=}} in DAG task
    arguments, the one expression site Argo reliably evaluates):
      1. strip the developer's typed nvidia.com/gpu request/limit (it stays
         the gpu-capability DECLARATION, detected before this runs);
      2. give the template a `gpus` input (default "1" — direct invocation
         keeps GPU behavior);
      3. extend the sizing podSpecPatch with nvidia.com/gpu:
         "{{inputs.parameters.gpus}}" under requests+limits — a "0" request
         for an extended resource is ignored by the scheduler, so the pod
         lands on CPU nodes;
      4. every DAG task invoking this step passes gpus via an {{=}}
         expression mapping the {step}-class param to '1' iff it names a
         gpu-tier class.

    Pinned (compute-class annotation) steps are untouched: their class is a
    render-time constant, and enhance_scheduling keeps the static request.
    """
    if "compute-class" in annots or not is_gpu_step(step):
        return
    step_name = step["name"]

    # 1. The typed request was the capability declaration; routing owns it now.
    resources = step["container"].get("resources", {})
    resources.get("requests", {}).pop("nvidia.com/gpu", None)
    resources.get("limits", {}).pop("nvidia.com/gpu", None)

    # 2. Template input with a GPU-preserving default.
    inputs = step.setdefault("inputs", {})
    input_params = inputs.setdefault("parameters", [])
    if not any(p.get("name") == "gpus" for p in input_params):
        input_params.append({"name": "gpus", "default": "1"})

    # 3. Extend the sizing patch (enhance_class_sizing ran first and sets it
    #    for a step that is not pinned) with the parameterized GPU quantity — YAML-level merge into
    #    the same `main` container entry, never a second patch document.
    patch_obj = yaml.safe_load(step.get("podSpecPatch") or "") or {}
    containers = patch_obj.setdefault("containers", [])
    main = next((c for c in containers if c.get("name") == "main"), None)
    if main is None:
        main = {"name": "main"}
        containers.append(main)
    res = main.setdefault("resources", {})
    res.setdefault("requests", {})["nvidia.com/gpu"] = "{{inputs.parameters.gpus}}"
    res.setdefault("limits", {})["nvidia.com/gpu"] = "{{inputs.parameters.gpus}}"
    step["podSpecPatch"] = yaml.safe_dump(patch_obj, default_flow_style=False)

    # 4. DAG tasks pass gpus from the class param.
    gpu_names = gpu_class_names(ctx)
    expr_list = ", ".join(f"'{n}'" for n in gpu_names)
    expr = (
        f"{{{{=workflow.parameters['{step_name}-class'] in [{expr_list}] ? '1' : '0'}}}}"
    )
    for task, _ in calls(spec, step_name):
        args = task.setdefault("arguments", {})
        params = args.setdefault("parameters", [])
        if not any(p.get("name") == "gpus" for p in params):
            params.append({"name": "gpus", "value": expr})


def class_offer(step: dict, ctx: dict, annots: dict) -> dict:
    """The classes the run form offers `step`, and why it withholds each other class of the platform:
    {"offered": [names], "refused": {class: [reason, what to do]}}. The `{step}-class` choice is `offered`;
    compose refuses a run whose class is not (run_settings.class_problem). One function, so the two agree.

    The pool catalog is ALWAYS attached (#865): the old tier-filtered list collapsed to length 1 on a
    one-class-per-tier pool, and enum-only-when->1 then rendered a FREE-TEXT box (typo -> forever-Pending
    run). Cross-tier entries are legitimate choices: a gpu-capable step submitted with a cpu class runs
    CPU-only (see the gpus routing in enhance_gpu_routing). A step that does not declare gpu=True is offered the
    cluster's cpu classes only: a gpu class would put it on a machine it cannot use. A pool with no cpu class at
    all keeps offering what exists. The HPC classes are offered to a step that can run on HPC (meluxina.py
    runs_on_hpc: not an ingest, publish or control step, not a step that produces a dataset), and an HPC class with
    `gpus` above 0 only to a step that declares gpu=True (a GPU step may pick a CPU HPC class)."""
    default = tier_default(ctx, step)["name"]
    catalog = ctx["computeClasses"]["all"]
    offered = catalog
    if not is_gpu_step(step):
        offered = [c for c in catalog if c.get("tier", "cpu") == "cpu"] or catalog
    options = [c["name"] for c in offered]
    if default not in options:
        options.insert(0, default)
    hpc = hpc_classes(ctx)
    hpc_all = [c["name"] for c in hpc]
    # A GPU partition (`gpus` above 0) is offered to a step that declares gpu=True only, as the cluster's GPU classes
    # are (D-25): a CPU step on it would take a GPU slot of a shared node for nothing. A GPU step may still pick a CPU
    # HPC class.
    gpu_partitions = {c["name"] for c in hpc if (c.get("gpus") or 0) > 0}
    if runs_on_hpc(step):
        options += [n for n in hpc_all if n not in options and (is_gpu_step(step) or n not in gpu_partitions)]
    choose = f"Choose one of: {', '.join(options)}."
    labels = (step.get("metadata") or {}).get("labels") or {}
    refused = {}
    for cls in catalog:
        if cls["name"] not in options:
            refused[cls["name"]] = ["it is a GPU machine and the step does not declare gpu=True",
                                    "Add gpu=True to the step in pipeline.py to run it on a GPU machine."]
    for name in hpc_all:
        if name not in options:
            if name in gpu_partitions and runs_on_hpc(step):
                refused[name] = ["it is a GPU machine and the step does not declare gpu=True",
                                 "Add gpu=True to the step in pipeline.py to run it on a GPU machine."]
                continue
            if labels.get(STEP_KIND_LABEL) in CLUSTER_ONLY_KINDS:
                reason = f"a {labels.get(STEP_KIND_LABEL)} step never runs on HPC (Slurm)"
            else:
                reason = "a step that produces a dataset runs on the cluster for now"
            refused[name] = [reason, choose]
    return {"offered": options, "refused": refused}


def enhance_class_param(spec: dict, step: dict, ctx: dict, annots: dict, offer: dict) -> None:
    """Per-step compute-class dropdown ({step}-class), matching the live WFT.

    Default = the step's tier class; enum = the pool's cpu and gpu classes for a
    step that declares gpu=True, its cpu classes only for any other step. A `compute-class` annotation PINS the class (no dropdown —
    the nodeSelector uses the literal name)."""
    if "compute-class" in annots:
        return  # pinned: nodeSelector uses the literal name (enhance_scheduling)
    parameters = spec["arguments"]["parameters"]
    existing = _param_names(parameters)
    step_name = step["name"]
    param_name = f"{step_name}-class"
    if param_name in existing:
        return
    default = tier_default(ctx, step)["name"]
    node_sizes(ctx)   # refuses a pool with a class that cannot be sized, so none is offered unsized
    options = offer["offered"]
    hpc_names = [n for n in options if n in {c["name"] for c in hpc_classes(ctx)}]
    param = {
        "name": param_name,
        "value": default,
        "enum": options,
        "description": (
            f"The machine class the '{step_name}' step runs on. Default: {default}."
            + (" Classes that start with "
               f"'{hpc_names[0].split('-')[0]}-' run the step as a job on HPC (Slurm): queue time applies."
               if hpc_names else "")
        ),
    }
    parameters.append(param)


def enhance_env(step: dict, ctx: dict, annots: dict) -> None:
    container = step["container"]
    env = container.setdefault("env", [])
    existing = {e["name"] for e in env}  # developer-set keys win

    def add(entry: dict) -> None:
        if entry["name"] not in existing:
            env.append(entry)
            existing.add(entry["name"])

    if not inject_disabled(annots, "mlflow"):
        add({"name": "MLFLOW_TRACKING_URI", "value": ctx["mlflow"]["trackingUri"]})
        # MLflow AUTH (#868): the tracking server sits behind mlflow-oidc-auth,
        # so a bare MLFLOW_TRACKING_URI 401s on any real logging/registry call.
        # Mirror the previous DSL's working reference: the step mints a Zitadel machine
        # JWT from the key file (mounted by enhance_volumes from
        # mlflow.svcSecret) and sends it as the bearer. Injected only when the
        # context carries the OIDC coordinates — a context without them (OIDC
        # off) keeps today's env exactly.
        mlflow_ctx = ctx.get("mlflow") or {}
        oidc_domain = (mlflow_ctx.get("oidcDomain") or "").strip()
        oidc_project = (mlflow_ctx.get("oidcProjectId") or "").strip()
        if oidc_domain and oidc_project and (mlflow_ctx.get("svcSecret") or "").strip():
            add({"name": "MLFLOW_TRACKING_AUTH", "value": "zitadel"})
            add({"name": "ZITADEL_DOMAIN", "value": oidc_domain})
            add({"name": "ZITADEL_MLFLOW_PROJECT_ID", "value": oidc_project})
            add({"name": "ZITADEL_MACHINE_KEY_FILE",
                 "value": f"{MLFLOW_SVC_MOUNT}/ZITADEL_MACHINE_KEY"})
    if not inject_disabled(annots, "lakefs"):
        add({"name": "LAKEFS_ENDPOINT", "value": ctx["lakefs"]["endpoint"]})
        keys = ctx["lakefs"]["adminSecretKeys"]
        secret_name = ctx["lakefs"]["adminSecret"]
        add(
            {
                "name": "LAKEFS_ACCESS_KEY",
                "valueFrom": {"secretKeyRef": {"name": secret_name, "key": keys["accessKey"]}},
            }
        )
        add(
            {
                "name": "LAKEFS_SECRET_KEY",
                "valueFrom": {"secretKeyRef": {"name": secret_name, "key": keys["secretKey"]}},
            }
        )
    # The run, the app and where results go are in the step's context (KAOS_STEP_CONTEXT): no variable
    # repeats them.
    # kaos-lib support window (PRD novelcore/kaos#1147): checked by the step at start.
    # Absent from the context = not injected, so the env is unchanged.
    if support_value(ctx):
        add({"name": KAOS_LIB_SUPPORT_ENV, "value": support_value(ctx)})


# The platform's own compose step keeps a short fixed limit. Every step of the developer's pipeline has one time
# limit instead, the run form's `<step>-walltime` (enhance_walltime), wherever it runs.
#
# That limit is not what bounds a step that never gets a machine (cloud stockout, exhausted quota, a pool at
# max): a deadline counts from the pod's start, so a Pending pod accrues none. The CI workflows chart's
# pending reaper stops a run whose step sits Pending past its threshold (45 minutes).
COMPOSE_STEP = "compose-and-validate"
COMPOSE_DEADLINE_SECONDS = 1800


def declared_walltime(annots: dict) -> str:
    """The step's walltime= as written in pipeline.py, else the default: the form field's default."""
    return annots.get("hpc") or DEFAULT_WALLTIME


def enhance_walltime(spec: dict, step: dict, annots: dict) -> None:
    """A step of the developer's pipeline is ended when the run form's `<step>-walltime` elapses: its deadline
    is an input of the template, which every DAG task of the step gives from what compose wrote
    (`kaos-run-settings`). The step's walltime= (else 4h) is the form's default (enhance_run_form)."""
    if step["name"] == COMPOSE_STEP:
        return
    add_input(step, "deadline", str(parse_time_limit(declared_walltime(annots)) * 60))
    step["activeDeadlineSeconds"] = "{{inputs.parameters.deadline}}"
    add_task_argument(spec, step["name"], "deadline", "{{=%s * 60}}" % setting(step["name"], "walltime"))


def enhance_run_form(spec: dict, steps: list, offers: dict) -> None:
    """The run form's own fields: `<step>-walltime` for every step (default: walltime= in code, else 4h),
    `<step>-checkpoint-every` for a step that declares checkpoint_every= or resumable= (not checkpoint_every=False;
    default: the value in code, else the engine's for resumable= alone), and
    `promote-<dataset>` for every dataset a step produces. Compose gets each value (checks it,
    and writes the numbers as its output `kaos-run-settings`)."""
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    if compose is None:
        return
    args = compose["container"].setdefault("args", [])
    parameters = spec["arguments"]["parameters"]
    existing = _param_names(parameters)
    declared = False

    def field(name: str, value: str, description: str, flag: str, key: str, enum: list = None) -> None:
        nonlocal declared
        if name in existing:
            raise EnhanceError(f"the run form already has a field '{name}'; rename the setting or the step "
                               f"that would give the form a second one")
        parameters.append({"name": name, "value": value, **({"enum": enum} if enum else {}),
                           "description": description})
        existing.add(name)
        args.extend([flag, "%s={{workflow.parameters.%s}}" % (key, name)])
        declared = True

    for step in steps:
        step_name = step["name"]
        if step_name == COMPOSE_STEP:
            continue
        annots = platform_annotations(step)
        field(walltime_field(step_name), declared_walltime(annots),
              f"The longest the step may run, as minutes or hours: {WALLTIME_EXAMPLE}. Default from pipeline.py.",
              "--walltime", step_name)
        lead = step_resumable_lead(step)
        if lead:
            args.extend(["--resumable-lead", f"{step_name}={lead}"])
        every = every_field_default(annots)
        if every:
            field(checkpoint_field(step_name), f"{every}m",
                  f"How often the '{step_name}' step's checkpoint is saved: minutes or hours, "
                  f"for example 10m or 1h.", "--checkpoint-every", step_name)
    table = json.loads(args[args.index("--steps") + 1]) if "--steps" in args else []
    chosen = {name: offer for name, offer in offers.items() if offer and name != COMPOSE_STEP}
    for name in chosen:
        args.extend(["--class", "%s={{workflow.parameters.%s}}" % (name, class_field(name))])
    if chosen:
        args.extend(["--class-offers", json.dumps(chosen, separators=(",", ":"))])
    for entry in table:
        if entry.get("matrix"):
            axes, name = list(entry["matrix"]), entry["name"]
            default = combinations(entry["matrix"])
            field(matrix_field(name), json.dumps(default, separators=(",", ":")),
                  f"The combinations the '{name}' step runs, one run each, as a JSON list of objects with the names "
                  f"{', '.join(axes)}. Default: every combination of the values in pipeline.py ({len(default)}). At "
                  f"most 32; a value is a number, true or false, or a short text.", "--matrix", name)
            field(parallel_field(name), str(DEFAULT_PARALLEL),
                  f"How many of the '{name}' step's runs go at the same time: a whole number from 1 to 32, or all. "
                  f"Default {DEFAULT_PARALLEL}: the GPU steps one machine holds, so that on an HPC (Slurm) class the "
                  f"runs share one machine. A larger number asks for more machines.", "--parallel", name)
        for produced in entry["produces"]:
            if produced["type"] == "dataset":
                field(promote_field(produced["name"]), "false",
                      f"Make this run's version of {produced['name']} the one other pipelines read as latest.",
                      "--promote", produced["name"], list(PROMOTE_CHOICES))
    if declared:
        args.extend(["--settings-output", SETTINGS_PATH])
        compose.setdefault("outputs", {}).setdefault("parameters", []).append(
            {"name": SETTINGS_PARAM, "valueFrom": {"path": SETTINGS_PATH}})


def enhance_scheduling(step: dict, ctx: dict, annots: dict) -> None:
    compute_class = resolve_compute_class(step, ctx, annots)
    gpu = is_gpu_step(step)

    # nodeSelector: pinned class -> literal name; else the {step}-class param.
    if "compute-class" in annots:
        selector_value = annots["compute-class"]
    else:
        selector_value = f"{{{{workflow.parameters.{step['name']}-class}}}}"
    step.setdefault("nodeSelector", {}).setdefault(
        "platform.kubecore.io/nodegroup-type", selector_value
    )

    # PRD-858: an ml-environment-targeted app pins EVERY step onto that
    # environment's own scale-to-zero class pools — the nodeSelector gains the
    # environment label and the tolerations gain the kubenv env-exclusivity
    # taint, mirroring exactly what kubenv-gcp stamps on those pools. Absent
    # mlEnvironment (legacy project-wide apps) leaves output byte-for-byte
    # unchanged. Same fill-absent discipline as everything else here.
    ml_env = ctx.get("mlEnvironment") or {}
    ml_env_name = (ml_env.get("name") or "").strip()
    if ml_env_name:
        # The operator's selector is the authority (PRD-1124): project +
        # environment, plus app for an APP-dedicated ml environment (D-07,
        # spec.dedicatedEnvironment.ml → app-tainted pools). Copy it verbatim
        # so the keys never drift from what kubenv-gcp stamps on the pools;
        # fall back to the legacy project/environment pair for a context
        # that predates the map (pipeline-context rendered by an older
        # operator). Project scoping is load-bearing either way: environment
        # names repeat across projects (live cross-tenant leak, 2026-08-27).
        selector = ml_env.get("nodeSelector") or {}
        if not selector:
            selector = {"platform.kubecore.io/environment": ml_env_name}
            project = (ctx.get("project") or "").strip()
            if project:
                selector["platform.kubecore.io/project"] = project
        for key, value in selector.items():
            step["nodeSelector"].setdefault(key, value)

    # The compose step's fixed limit; a developer's step has its walltime (enhance_walltime).
    if step["name"] == COMPOSE_STEP:
        step.setdefault("activeDeadlineSeconds", COMPOSE_DEADLINE_SECONDS)

    tolerations = step.setdefault("tolerations", [])
    # Tolerate EVERY catalog class's taint, not just the default class's
    # (#865): the {step}-class dropdown routes at SUBMIT time via the
    # nodeSelector param, but tolerations are rendered statically — a run
    # submitted onto any non-default class (including cross-tier) must
    # already tolerate that pool's taint or it Pends forever. Tolerations
    # are permissive; the nodeSelector still pins the exact pool.
    for pair in catalog_toleration_pairs(ctx, step, annots):
        key, value = pair
        if not any(
            t.get("key") == key and t.get("value") == value for t in tolerations
        ):
            tolerations.append(
                {"key": key, "operator": "Equal", "value": value, "effect": "NoSchedule"}
            )
    if gpu and not any(t.get("key") == "nvidia.com/gpu" for t in tolerations):
        # GKE auto-taints accelerator nodes; tolerate it so the pod schedules.
        # Harmless on CPU nodes (tolerations never attract, only permit).
        tolerations.append({"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"})
    if ml_env_name:
        # The pools carry the env-exclusivity taint (kubenv={env}) — or the
        # app-exclusivity taint (app={app}) for a dedicated ml environment
        # (D-07); the operator's mlEnvironment.tolerations says which. Without
        # it every step Pends forever on its own pools (PRD-858).
        env_tolerations = ml_env.get("tolerations") or [
            {"key": "platform.kubecore.io/kubenv", "operator": "Equal",
             "value": ml_env_name, "effect": "NoSchedule"}
        ]
        for tol in env_tolerations:
            if not any(t.get("key") == tol.get("key") and t.get("value") == tol.get("value") for t in tolerations):
                tolerations.append({
                    "key": tol.get("key"), "operator": tol.get("operator", "Equal"),
                    "value": tol.get("value"), "effect": tol.get("effect", "NoSchedule"),
                })

    # Sizing. Developer-set requests always win; a missing limit mirrors the
    # (possibly developer-set) request. Only fields the developer left entirely
    # unset get whole-node allocatable sizing (requests==limits).
    resources = step["container"].setdefault("resources", {})
    requests = resources.setdefault("requests", {})
    limits = resources.setdefault("limits", {})
    cpu, memory_gib = node_sizes(ctx)[compute_class["name"]]
    defaults = {"cpu": str(cpu), "memory": f"{memory_gib}Gi"}
    for field_name, default in defaults.items():
        requests.setdefault(field_name, default)
        limits.setdefault(field_name, requests[field_name])  # limits follow requests
    # The disk of the class's machine (node_disks) — request only, deliberately NO limit (see
    # DISK_REQUEST_DEFAULT): an eviction shield for every step. A step on the form's class gets it from the
    # podSpecPatch (enhance_class_sizing); this is the pinned step's and the default class's.
    requests.setdefault("ephemeral-storage", node_disks(ctx)[compute_class["name"]])
    # NOTE (#865): the GPU request is deliberately NOT set here for
    # class-routable steps — enhance_gpu_routing moves it into the
    # podSpecPatch keyed to the runtime class choice, so a gpu-capable step
    # submitted with a cpu class carries nvidia.com/gpu: "0" (which the
    # scheduler ignores) instead of an unsatisfiable GPU request on a CPU
    # node. Pinned (compute-class annotation) gpu steps keep the static
    # request: their class never changes at submit time.
    if gpu and "compute-class" in annots:
        requests.setdefault("nvidia.com/gpu", "1")
        limits.setdefault("nvidia.com/gpu", "1")


def enhance_gpu_driver(step: dict) -> None:
    """Wrap a gpu step's container command in the driver prelude (GPU_PRELUDE_SCRIPT).

    Why: on a KAOS GPU node the driver sits in /usr/local/nvidia, which a plain python image does not search,
    so the step would fall back to CPU unnoticed. Called from enhance() AFTER enhance_hpc so the Slurm twin
    (which copies the step's command) never carries the prelude. A step without an explicit command (image
    ENTRYPOINT only) is left alone: its argv cannot be preserved. Idempotent: a wrapped step is skipped.
    """
    container = step.get("container") or {}
    command = container.get("command") or []
    if not command or command[0] == "sh" and GPU_PRELUDE_NAME in command[:4]:
        return
    has_param = any(p.get("name") == "gpus" for p in (step.get("inputs") or {}).get("parameters") or [])
    gpus = "{{inputs.parameters.gpus}}" if has_param else "1"  # class-routable steps get gpus="0" on a cpu class
    container["command"] = ["sh", "-c", GPU_PRELUDE_SCRIPT, GPU_PRELUDE_NAME, gpus] + list(command)


def _parse_cpu(value) -> float:
    """Kubernetes CPU quantity -> cores ("500m" -> 0.5, "2" -> 2.0)."""
    text = str(value).strip()
    if text.endswith("m"):
        return float(text[:-1]) / 1000.0
    return float(text)


_MEM_UNITS = {
    "Ki": 1 / (1024 * 1024), "Mi": 1 / 1024, "Gi": 1.0, "Ti": 1024.0,
    "K": 1000 / 1024**3, "M": 1000**2 / 1024**3, "G": 1000**3 / 1024**3,
}


def _parse_mem_gib(value) -> float:
    """Kubernetes memory quantity -> GiB ("512Mi" -> 0.5, "27Gi" -> 27.0)."""
    text = str(value).strip()
    for suffix, factor in _MEM_UNITS.items():
        if text.endswith(suffix):
            return float(text[: -len(suffix)]) * factor
    return float(text) / 1024**3  # bare bytes


def validate_scheduling(step: dict, compute_class: dict, gpu: bool = None, env_has_gpu: bool = True) -> None:
    """Fail the render if this step can NEVER schedule on its compute class.

    The step's requests are compared against the class's whole-node budget
    (capacity minus the node's kube-reservation and DaemonSet footprint, computed
    by the platform). A request above that budget produces a pod that sits
    Pending on "Insufficient cpu/memory" forever — a run that burns hours before
    anyone notices. Catching it at render time turns a silent hang into a build
    error naming the step, the class, and the number that does not fit.

    Only what is knowable at render time is checked: a per-run {step}-cpu/-mem
    override can still overshoot, so the same budget is what those knobs default
    to.
    """
    allocatable = compute_class.get("allocatable")
    if not allocatable:
        return  # unknown machine type: the platform omits allocatable, nothing to check
    requests = step["container"].get("resources", {}).get("requests", {})
    step_name = step["name"]
    class_name = compute_class.get("name", "?")
    machine = compute_class.get("machineType", "?")

    if "cpu" in requests:
        want = _parse_cpu(requests["cpu"])
        if want > allocatable["cpu"]:
            raise EnhanceError(
                f"step '{step_name}' requests {requests['cpu']} cpu but compute class "
                f"'{class_name}' ({machine}) can only give a pod {allocatable['cpu']} cpu — "
                f"the pod would never schedule. Lower the request or pick a bigger class."
            )
    if "memory" in requests:
        want = _parse_mem_gib(requests["memory"])
        if want > allocatable["memoryGiB"]:
            raise EnhanceError(
                f"step '{step_name}' requests {requests['memory']} memory but compute class "
                f"'{class_name}' ({machine}) can only give a pod {allocatable['memoryGiB']}Gi — "
                f"the pod would never schedule. Lower the request or pick a bigger class."
            )
    # A gpu-declaring step on an accelerator-less class can never schedule —
    # unless the target ml environment declares NO gpu flavours at all
    # (PRD-1124 D-02: cpu-only environment). Then the step defaults to the cpu
    # class and the gpus routing (enhance_gpu_routing) sets its nvidia.com/gpu
    # request to 0, so it schedules and runs CPU-only by design.
    if (is_gpu_step(step) if gpu is None else gpu) and env_has_gpu and not compute_class.get("guestAccelerator"):
        raise EnhanceError(
            f"step '{step_name}' requests a GPU but compute class '{class_name}' "
            f"({machine}) has no accelerator — the pod would never schedule."
        )


# Ephemeral-storage request of a step on a class whose usable disk the platform did not say (node_disks).
# Request-only, never a limit: the
# request is the eviction shield (a pod under its request is evicted LAST and
# the scheduler won't place it on a nearly-full node); a limit would instead
# kill legitimate spiky export scratch (onnx2tf temp trees).
DISK_REQUEST_DEFAULT = "10Gi"


# ------------------------------------------------------------------- volumes


def enhance_volumes(step: dict, annots: dict, ctx: dict = None) -> None:
    """Inject /dev/shm (sizable via shm annotation), opt-in per-run RWO
    workspace PVC (workspace annotation), and — when the context carries the
    MLflow OIDC coordinates (#868) — the Zitadel machine-key secret mount the
    MLflow auth env from enhance_env points at. Raw developer volumes win
    (only-fill-absent)."""
    container = step["container"]
    volume_mounts = container.setdefault("volumeMounts", [])
    mounted = {m.get("mountPath") for m in volume_mounts}
    volumes = step.setdefault("volumes", [])
    vol_names = {v.get("name") for v in volumes}

    # MLflow Zitadel machine key (#868) — same gating as the env injection in
    # enhance_env: only when the context has domain + project id + secret, and
    # the developer hasn't opted the step out of mlflow injection.
    mlflow_ctx = (ctx or {}).get("mlflow") or {}
    if (
        not inject_disabled(annots, "mlflow")
        and (mlflow_ctx.get("oidcDomain") or "").strip()
        and (mlflow_ctx.get("oidcProjectId") or "").strip()
        and (mlflow_ctx.get("svcSecret") or "").strip()
        and MLFLOW_SVC_MOUNT not in mounted
    ):
        if "mlflow-svc" not in vol_names:
            volumes.append({
                "name": "mlflow-svc",
                "secret": {"secretName": mlflow_ctx["svcSecret"].strip()},
            })
        volume_mounts.append({
            "name": "mlflow-svc", "mountPath": MLFLOW_SVC_MOUNT, "readOnly": True,
        })
        mounted.add(MLFLOW_SVC_MOUNT)

    # /dev/shm — PyTorch dataloader shared memory. Default 1Gi; shm annotation
    # overrides. Only if the developer didn't already mount /dev/shm.
    if "/dev/shm" not in mounted:
        shm_size = annots.get("shm", "1Gi")
        if "dshm" not in vol_names:
            volumes.append({"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": shm_size}})
        volume_mounts.append({"name": "dshm", "mountPath": "/dev/shm"})

    # Opt-in per-run workspace PVC (RWO). volumeClaimTemplates live at the
    # workflow spec level; the enhancer records the request on the step so the
    # workflow-level pass can emit it. Only if the developer asked (annotation)
    # and didn't already mount /workspace.
    if "workspace" in annots and WORKSPACE_MOUNT not in mounted:
        step.setdefault("_workspace_request", annots["workspace"])
        volume_mounts.append({"name": "workspace", "mountPath": WORKSPACE_MOUNT})


def enhance_workspace_pvc(spec: dict, steps: list) -> None:
    """If any step requested a workspace PVC, emit one workflow-level
    volumeClaimTemplate (the max requested size) mounted in every step."""
    requests = [s.pop("_workspace_request") for s in steps if "_workspace_request" in s]
    if not requests:
        return

    def _gi(size: str) -> int:
        return int(str(size).rstrip("Gi") or 0)

    size = max(requests, key=_gi)
    vcts = spec.setdefault("volumeClaimTemplates", [])
    if not any(v.get("metadata", {}).get("name") == "workspace" for v in vcts):
        vcts.append(
            {
                "metadata": {"name": "workspace"},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": size}},
                },
            }
        )


def enhance_compose_identity(steps: list, ctx: dict) -> None:
    """Tell the compose step whose run it composes: the app and the project of every step's context.
    The app's name is also a field of every HPC job, which holds 55 characters."""
    if len(ctx["app"]) > MAX_APP_NAME:
        raise EnhanceError(
            f"the app name '{ctx['app']}' is {len(ctx['app'])} characters; an app that runs pipelines may have at "
            f"most {MAX_APP_NAME}. Create the app under a shorter name.")
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    if compose is None:
        return
    args = compose["container"].setdefault("args", [])
    # A produced dataset is the repository dataset-<app>-<name>: refuse a pair the lake cannot hold now, with
    # the app known, instead of when the run starts.
    table = json.loads(args[args.index("--steps") + 1]) if "--steps" in args else []
    for step in table:
        for entry in step["produces"]:
            problem = entry["type"] == "dataset" and dataset_name_problem(ctx["app"], step["name"], entry["name"])
            if problem:
                raise EnhanceError(problem)
    for flag, value in (("--app", ctx["app"]), ("--project", ctx["project"])):
        if flag not in args:
            args += [flag, value]


def refuse_large_templates(spec: dict) -> None:
    """Refuse a step whose rendered definition is too large to start with a full context.

    A step's whole definition reaches its pod as one environment string of at most 128 KiB, the step's
    context inside it. The context may take 96 KiB of that; the rest of the definition gets 24 KiB
    (docs/step-contract.md, "Size caps"). Over it the step could error before it runs, so the render
    fails instead, naming the step. The HPC follower's template is measured for the steps that have an
    HPC variant.
    """
    by_name = {t["name"]: t for t in spec["templates"]}
    for template in spec["templates"]:
        tasks = (template.get("dag") or {}).get("tasks", []) + [e for g in template.get("steps") or [] for e in g]
        for task in tasks:
            called = by_name.get(task.get("template")) or {}
            if not any(p.get("name") == CONTEXT_PARAM for p in (called.get("inputs") or {}).get("parameters") or []):
                continue
            size = template_bytes(called)
            if size > TEMPLATE_CAP:
                raise EnhanceError(
                    f"step '{task['name']}': its rendered definition is {size} bytes; the limit is "
                    f"{TEMPLATE_CAP}. A step's definition and its context reach the step together and share "
                    f"one size limit. Remove what makes the definition large (long command lines, many "
                    f"environment values or volumes).")


STEP_IMAGES_ENV = "KAOS_STEP_IMAGES"
IMAGE_RECORD_PARAM = "image"        # the compose-like template's own image: an input read from the image record


def _record_input(name: str, cm: str, key: str) -> dict:
    """A template input that the workflow engine reads from the image record when the task starts. It is an input of
    the template, not an argument of the workflow: nobody can set it when a run is submitted."""
    return {"name": name, "valueFrom": {"configMapKeyRef": {"name": cm, "key": key}}}


def enhance_images(spec: dict, ctx: dict, steps: list) -> None:
    """Where every image comes from. The image of a step is never a workflow parameter (no field on the run form, no
    parameter a submit can set): the app's image record, the ConfigMap `<app>-pipeline-images` that CI writes after a
    build, is read in two places.

    - Compose's own template and the final task's have an input `image` read from the record (key `compose-and-validate`).
    - Compose's template also has one input `image-<step>` for each step, read from the record when compose starts. They
      go to compose as KAOS_STEP_IMAGES (compose refuses a step that was never built: kubecore/compose.py
      check_step_images) and compose writes them as its output `kaos-images`.
    - Every step (and its HPC variant) is given its image by its DAG task, from that output (step_contract.image_of): one
      reading of the record for the whole run, the same string on the cluster and on HPC.

    A step with a `platform.kubecore.io/image` annotation keeps that verbatim image and reads no record."""
    cm = images_configmap(ctx)
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    images = {}
    for step in steps:
        annots = platform_annotations(step)
        if "image" in annots:
            step["container"]["image"] = annots["image"]      # the sanctioned escape hatch: a verbatim utility image
        elif step is compose:
            step["container"]["image"] = IMAGE_INPUT
            step.setdefault("inputs", {}).setdefault("parameters", []).append(
                _record_input(IMAGE_RECORD_PARAM, cm, annots.get("source", COMPOSE_STEP)))
        else:
            step["container"]["image"] = IMAGE_INPUT
            step.setdefault("inputs", {}).setdefault("parameters", []).append({"name": "image"})
            add_task_argument(spec, step["name"], "image", image_of(step["name"]))
            images[step["name"]] = annots.get("source", step["name"])
    if compose is None or not images:
        return
    inputs = compose["inputs"]["parameters"]
    inputs += [_record_input(f"image-{name}", cm, key) for name, key in sorted(images.items())]
    env = compose["container"].setdefault("env", [])
    if not any(e.get("name") == STEP_IMAGES_ENV for e in env):
        env.append({"name": STEP_IMAGES_ENV, "value": json.dumps(
            {name: "{{inputs.parameters.image-%s}}" % name for name in sorted(images)}, separators=(",", ":"))})
    args = compose["container"].setdefault("args", [])
    args += ["--images-output", IMAGES_PATH]
    compose.setdefault("outputs", {}).setdefault("parameters", []).append(
        {"name": IMAGES_PARAM, "valueFrom": {"path": IMAGES_PATH}})


PROMOTE_COMMAND = ["python", "-I", "-m", "kaos_engine.promote"]
PROMOTE_SIZE = {"cpu": "100m", "mem": "256Mi", "disk": "1Gi"}   # what the final task asks of a node: a few commits and merges


GATHER_RESULT_PATH = "/tmp/outputs/kaos-result.json"
GATHER_COMMAND = ["python", "-I", "-m", "kaos_engine.gather"]


def add_gather_tasks(spec: dict, steps: list) -> None:
    """For each matrix step, the task `kaos-gather-<step>` (kubecore/gather.py) after it: it joins what the step's
    items handed back into the one entry the steps that need the matrix step read, and fails the run when no item
    succeeded. Like `kaos-promote` it is the compose template again (same image, service account, credential and
    fixed class), added here and never by pipeline.py, under a name a step cannot take."""
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    if compose is None:
        return
    args = compose["container"].get("args", [])
    table = json.loads(args[args.index("--steps") + 1]) if "--steps" in args else []
    dag = next(t for t in spec["templates"] if t.get("name") == spec.get("entrypoint") and "dag" in t)["dag"]
    compose_task = next(t for t in dag["tasks"] if t["name"] == COMPOSE_STEP)
    given = copy.deepcopy((compose_task.get("arguments") or {}).get("parameters") or [])
    inputs = ("results", "items", "plan", "status")
    for entry in (e for e in table if e.get("matrix")):
        step = entry["name"]
        tpl = copy.deepcopy(compose)
        tpl["name"] = gather_task(step)
        tpl.setdefault("inputs", {}).setdefault("parameters", []).extend({"name": n} for n in inputs)
        tpl["outputs"] = {"parameters": [{"name": RESULT_PARAM, "valueFrom": {"path": GATHER_RESULT_PATH}}]}
        container = tpl["container"]
        container["command"], container["args"] = list(GATHER_COMMAND), []
        container["env"] = [e for e in container.get("env", []) if e["name"] != STEP_IMAGES_ENV] + [
            {"name": "KAOS_GATHER_STEP", "value": step},
            {"name": "KAOS_GATHER_ALLOW_FAILED", "value": "true" if entry.get("allow_failed") else "false"}] + [
            {"name": "KAOS_GATHER_" + n.upper(), "value": "{{inputs.parameters.%s}}" % n} for n in inputs]
        tpl.setdefault("metadata", {}).setdefault("labels", {})["platform.kubecore.io/step"] = gather_task(step)
        spec["templates"].insert(spec["templates"].index(compose) + 1, tpl)
        document = 'tasks["%s"].outputs.parameters[%s]' % (step, "%s")
        dag["tasks"].append({
            "name": gather_task(step), "template": gather_task(step),
            "depends": "%s && (%s.Succeeded || %s.Skipped || %s.Omitted)" % (COMPOSE_STEP, step, step, step),
            "arguments": {"parameters": given + [
                {"name": "results", "value": '{{=sprig.default("[]", %s)}}' % (document % '"kaos-result"')},
                {"name": "items", "value": '{{=sprig.default("[]", %s)}}' % (document % '"kaos-item"')},
                {"name": "plan", "value": "{{=%s}}" % matrix_setting(step, "matrix")},
                {"name": "status", "value": "{{tasks.%s.status}}" % step}]}})


def add_promote_task(spec: dict, steps: list) -> None:
    """The run's final task `kaos-promote` (kubecore/promote.py): it moves `latest` of every dataset the run form
    asked to promote, all or none, once every step has succeeded or been skipped. Steps only seal their versions.

    It is the compose template again, as the passes before this one made it (the same image, the same lake
    credential and service account, the same fixed limit, the cluster), with another command: so it holds exactly
    the identity a step has, and nothing a step does not. It is added here, never by pipeline.py, and the name is one
    a pipeline cannot use (refuse_client_identity). A pipeline with no dataset producer has no such task."""
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    if compose is None:
        return
    args = compose["container"].get("args", [])
    table = json.loads(args[args.index("--steps") + 1]) if "--steps" in args else []
    producers = [(s["name"], e["name"]) for s in table for e in s["produces"] if e["type"] == "dataset"]
    if not producers:
        return
    tpl = copy.deepcopy(compose)
    tpl["name"] = PROMOTE_STEP
    # Beside the class's and its own `image` (read from the image record): not compose's one input for each step.
    tpl["inputs"]["parameters"] = [p for p in tpl["inputs"]["parameters"] if not p["name"].startswith("image-")]
    tpl["inputs"]["parameters"].append({"name": PROMOTE_PARAM})
    tpl.pop("outputs", None)
    container = tpl["container"]
    container["command"], container["args"] = list(PROMOTE_COMMAND), []
    container["env"] = [e for e in container.get("env", []) if e["name"] != STEP_IMAGES_ENV] + [
        {"name": "KAOS_RUN", "value": "{{workflow.name}}"},
        {"name": "KAOS_PROMOTE_PLAN", "value": "{{inputs.parameters.%s}}" % PROMOTE_PARAM}]
    labels = tpl.setdefault("metadata", {}).setdefault("labels", {})
    labels["platform.kubecore.io/step"] = PROMOTE_STEP
    spec["templates"].insert(spec["templates"].index(compose) + 1, tpl)
    # What each producer wrote (its result's writes; the sentinel when it did not run) and the form's choice, as JSON.
    # Every `}` is followed by `,` or `]`: two closing braces would end the expression.
    plan = ",".join(
        '{"step":%s,"name":%s,"promote":workflow.parameters[%s],"writes":fromJSON(%s)?.writes ?? []}'
        % (json.dumps(step), json.dumps(name), json.dumps(promote_field(name)), result_of(step))
        for step, name in producers)
    dag = next(t for t in spec["templates"] if t.get("name") == spec.get("entrypoint") and "dag" in t)["dag"]
    compose_task = next(t for t in dag["tasks"] if t["name"] == COMPOSE_STEP)
    # The compose step's class, but with the small fixed size of this task (it reads a few commits and merges): the compose
    # step's own size (7 CPU, 27 GiB on the standard class) would ask the pool for a node at the end of every run.
    given = [dict(p, value=PROMOTE_SIZE[p["name"]]) if p["name"] in PROMOTE_SIZE else p
             for p in copy.deepcopy((compose_task.get("arguments") or {}).get("parameters") or [])]
    for p in tpl["inputs"]["parameters"]:
        if p["name"] in PROMOTE_SIZE:
            p["default"] = PROMOTE_SIZE[p["name"]]
    # No pod at all when no dataset is ticked on the form (the default): the task is Skipped. The compose step's log says
    # which datasets stay where they are (kubecore/compose.py).
    ticked = " || ".join("workflow.parameters[%s] == 'true'" % json.dumps(promote_field(name))
                         for name in dict.fromkeys(name for _, name in producers))
    ends = " && ".join("(%s.Succeeded || %s.Skipped)" % (n, n) for n in dict.fromkeys(s["name"] for s in table))
    ends += "".join(" && %s.Succeeded" % gather_task(s["name"]) for s in table if s.get("matrix"))
    dag["tasks"].append({
        "name": PROMOTE_STEP, "template": PROMOTE_STEP, "depends": "%s && %s" % (COMPOSE_STEP, ends),
        "when": "{{=%s}}" % ticked,
        "arguments": {"parameters": given + [{"name": PROMOTE_PARAM, "value": "{{=toJSON([%s])}}" % plan}]}})


def enhance_pipeline_info(spec: dict, ctx: dict, steps: list) -> None:
    """The text at the top of the run form: how to run, the machine classes, the developer's steps. It is a
    field because the form shows nothing else; it speaks about the developer's pipeline, not the platform."""
    parameters = spec["arguments"]["parameters"]
    if "pipeline-info" in _param_names(parameters):
        return
    own = [s for s in steps if s["name"] != COMPOSE_STEP]
    lines = [
        "The defaults run as they are: press Submit.",
        "Each step has a class (the machine it runs on) and a time limit (the longest it may run, as minutes or "
        "hours). Only a step that declares gpu=True is offered the GPU classes.",
        "Machine classes:",
    ]
    for cls in ctx["computeClasses"]["all"]:
        acc = cls.get("guestAccelerator")
        acc_txt = f" + {acc['count']}x {acc['type']}" if acc else ""
        lines.append(f"  - {cls['name']} ({cls.get('tier', 'cpu')}) — {cls['machineType']}{acc_txt}")
    hpc = hpc_classes(ctx)
    if hpc:
        lines.append(
            "HPC (Slurm) classes: pick one in a step's class field and the step runs there as a job. Queue time "
            "applies; the step's time limit is the job's limit. Classes:")
        for c in hpc:
            lines.append(f"  - {c['name']} ({c.get('tier', 'cpu')}) — partition {c.get('partition')}"
                         f"{', ' + str(c.get('gpus')) + ' GPUs' if c.get('gpus') else ''}"
                         f", {c.get('cpus', '?')} cores, {c.get('memoryGiB', '?')} GiB"
                         f"{' — ' + c['description'] if c.get('description') else ''}")
    lines.append("Steps: " + " -> ".join(s["name"] for s in own))
    parameters.insert(0, {"name": "pipeline-info", "value": "\n".join(lines),
                          "description": "How to fill this form."})


def enhance_compose_class(spec: dict, steps: list, ctx: dict) -> None:
    """The platform's own compose step runs on the default class, fixed in the template: it is not a field the
    developer chooses. Its class parameter is replaced by that class's name wherever the template reads it."""
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    if compose is None:
        return
    field = class_field(COMPOSE_STEP)
    chosen = tier_default(ctx, compose)["name"]
    replacements = (("{{workflow.parameters.%s}}" % field, chosen),
                    ("workflow.parameters['%s']" % field, "'%s'" % chosen))

    def fixed(node):      # in place: the templates are shared with the passes that follow
        items = node.items() if isinstance(node, dict) else enumerate(node) if isinstance(node, list) else ()
        for key, value in list(items):
            if isinstance(value, str):
                for old, new in replacements:
                    value = value.replace(old, new)
                node[key] = value
            else:
                fixed(value)

    fixed(spec["templates"])
    parameters = spec["arguments"]["parameters"]
    parameters[:] = [p for p in parameters if p["name"] != field]


def order_form(spec: dict, steps: list) -> None:
    """The form in the order a developer reads it: the text, the experiment fields, the settings in config
    order, the dataset fields, then each step's class, time limit and checkpoint interval, the promote ticks,
    the extra YAML, and last what the platform owns."""
    parameters = spec["arguments"]["parameters"]
    compose = next((s for s in steps if s["name"] == COMPOSE_STEP), None)
    table = json.loads(compose["container"]["args"][compose["container"]["args"].index("--steps") + 1]) \
        if compose and "--steps" in compose["container"]["args"] else []
    inputs = [name for entry in table for name in entry["inputs"]]
    own = [e["name"] for e in table]
    rank = {}

    def place(name: str, group: int, index: int = 0) -> None:
        rank[name] = (group, index)

    for p in parameters:
        name = p["name"]
        if name == "pipeline-info":
            place(name, 0)
        elif name.startswith("experiment-"):
            place(name, 1)
        elif name in inputs:
            place(name, 3)
        elif name.startswith("promote-"):
            place(name, 5)
        elif name == "config":
            place(name, 6)
    suffixes = ("class", "walltime", "checkpoint-every", "matrix", "parallel")
    for i, step in enumerate(own):
        for suffix in suffixes:
            place(f"{step}-{suffix}", 4, i * len(suffixes) + suffixes.index(suffix))
    parameters.sort(key=lambda p: rank.get(p["name"], (2, 0)))     # sort is stable: config order within a group


def connection_env_from(ctx: dict) -> list:
    """Optional envFrom refs for the app connections in the pipeline context.

    Mirrors render-wft's `_connection_env_from`: entries without a configMapName
    are skipped, duplicate configMapNames collapse, and an entry carrying a
    non-empty `environment` is kept only when it equals ctx mlEnvironment.name.
    Always returns fresh dicts (shared objects would make yaml.dump emit aliases).
    """
    env_name = (ctx.get("mlEnvironment") or {}).get("name") or ""
    out = []
    seen = set()
    for c in ctx.get("connections") or []:
        if not isinstance(c, dict):
            continue
        cm = c.get("configMapName")
        if not isinstance(cm, str) or not cm.strip():
            continue
        if c.get("environment") and c.get("environment") != env_name:
            continue
        if cm in seen:
            continue
        seen.add(cm)
        out.append({"configMapRef": {"name": cm, "optional": True}})
    return out


def enhance_connections(step: dict, ctx: dict) -> None:
    refs = connection_env_from(ctx)
    if not refs:
        return
    container = step["container"]
    existing = container.get("envFrom")
    if not isinstance(existing, list):  # absent, null or malformed: treat as absent
        container["envFrom"] = refs
        return
    # developer entries stay first
    have = {
        (e.get("configMapRef") or {}).get("name")
        for e in existing
        if isinstance(e, dict) and e.get("configMapRef")
    }
    for ref in refs:
        if ref["configMapRef"]["name"] not in have:
            existing.append(ref)


# ---------------------------------------------------------------------- main


def enhance(wft: dict, ctx: dict) -> dict:
    refuse_client_identity(wft)  # before the platform adds its own account and patches
    enhance_metadata(wft, ctx)
    spec = wft["spec"]
    enhance_spec_top_level(spec, ctx)
    steps = step_templates(spec)
    gpu_step_names: set = set()
    offers: dict = {}    # step -> what its {step}-class choice offers (None: pinned to one class)
    shared = shared_templates(spec)  # before enhance_hpc adds twin tasks
    for step in steps:
        annots = platform_annotations(step)  # validates; raises on unknown keys
        # Captured BEFORE enhance_gpu_routing strips the typed request — the
        # developer's nvidia.com/gpu declaration is the capability marker.
        gpu = is_gpu_step(step)
        if gpu:
            gpu_step_names.add(step["name"])
        enhance_env(step, ctx, annots)
        # Before enhance_gpu_routing strips the typed request, which class_offer reads.
        offer = class_offer(step, ctx, annots) if "compute-class" not in annots else None
        offers[step["name"]] = offer
        enhance_class_param(spec, step, ctx, annots, offer)
        compute_class = resolve_compute_class(step, ctx, annots)
        enhance_class_sizing(spec, step, ctx, annots)  # the node of the chosen class
        enhance_scheduling(step, ctx, annots)
        enhance_walltime(spec, step, annots)
        # After scheduling fill, so the final requests (developer-set or
        # platform whole-node) are what get checked against the class budget.
        validate_scheduling(step, compute_class, gpu, env_has_gpu=bool(gpu_class_names(ctx)))
        # After validation: strips the typed GPU request and installs the
        # runtime class->gpus routing (#865), so the checks above still saw
        # the full gpu-tier picture.
        enhance_gpu_routing(spec, step, ctx, annots)
        enhance_volumes(step, annots, ctx)
        enhance_resilience(spec, step, ctx, annots, gpu, inject_disabled(annots, "lakefs"), shared)
        enhance_connections(step, ctx)
    enhance_images(spec, ctx, steps)
    enhance_workspace_pvc(spec, steps)
    enhance_compose_identity(steps, ctx)
    enhance_run_form(spec, steps, offers)
    enhance_compose_class(spec, steps, ctx)
    enhance_pipeline_info(spec, ctx, steps)
    order_form(spec, steps)
    add_gather_tasks(spec, steps)
    add_promote_task(spec, steps)   # before enhance_hpc: its depends are rewritten for the steps that have a twin
    # PRD-1016: the MeluXina leg — must run LAST (twin tasks copy each routed
    # step's FINAL container image/command, and the depends rewrite needs the
    # final DAG). See kubecore/meluxina.py.
    enhance_hpc(spec, ctx, steps, gpu_step_names)
    # After enhance_hpc on purpose: the twin copied the step's plain command; only the in-cluster (KAOS GPU
    # node) container gets the driver prelude.
    for step in steps:
        if step["name"] in gpu_step_names:
            enhance_gpu_driver(step)
    refuse_large_templates(spec)  # last: it measures the templates as they are released
    return wft


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Enhance a raw Hera WorkflowTemplate with platform wiring.")
    parser.add_argument("--raw", required=True, help="Path to the raw WorkflowTemplate YAML (from pipeline.py).")
    parser.add_argument("--context", required=True, help="Path to pipeline-context.yaml.")
    # Ignored: the `dataset` parameter is filled from the platform's dataset
    # catalog (hera-enhance-commit), not here. Still accepted so a CI that passes
    # it keeps rendering.
    parser.add_argument("--catalog", default="", help=argparse.SUPPRESS)
    parser.add_argument("--output", required=True, help="Path to write the enhanced WorkflowTemplate YAML.")
    parser.add_argument("--helper-configmap", default="",
                        help="Path to write the ConfigMap that carries the checkpoint helper source once for "
                             "every step that mounts it (removed when no step does). The CI commits it next to "
                             "the WorkflowTemplate; without it a render that uses the helper is not runnable.")
    args = parser.parse_args(argv)

    raw = yaml.safe_load(Path(args.raw).read_text())
    ctx = yaml.safe_load(Path(args.context).read_text())

    enhanced = enhance(raw, ctx)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(enhanced, sort_keys=False, default_flow_style=False))
    print(f"wrote {out}")
    if args.helper_configmap:
        wrote = write_helper_configmap(enhanced, ctx, Path(args.helper_configmap))
        print(f"{'wrote' if wrote else 'no steps use the helper; removed'} {args.helper_configmap}")


if __name__ == "__main__":
    main()
