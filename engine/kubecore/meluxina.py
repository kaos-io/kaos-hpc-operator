"""PRD-1016 — the MeluXina HPC leg the `platform.kubecore.io/hpc` annotation reserved.

`enhance_hpc` runs as the last enhancement pass. Active only when the pipeline
context carries `hpc.enabled` (Derive idiom: the operator sets it iff the
parent KubePool is HPC-ready, so non-HPC pools render byte-identically).

What it does (F-02/F-03/F-06):
  * re-emits the `target` submit-form parameter (gcp | <provider>);
  * every GPU step WITHOUT an existing task `when` (quantization-gated steps
    keep in-cluster-only behaviour this slice) gains a `<step>-hpc` twin DAG
    task with complementary `when:` clauses on `target`;
  * downstream `depends` tokens that reference a routed step are rewritten to
    gate on the Succeeded||Skipped twin pair (exactly one twin runs);
  * one `hpc-follower` CONTAINER template is appended (the release gate
    rejects script templates).

MeluXina runs only through the pool's HPC controller (PRD-HPC-1461 D-58, D-59,
F-35). The operator publishes pipeline-context hpc.controller "serve" (and
hpc.followerImage) when the controller serves the project; `hpc-follower` is
then a credential-free follower (see follower_template): it creates the step's
HPCArgoWorkflowJob, prints its progress and writes the step's one result from
its status. A step's HPC variant follows the same step contract as its pod
(docs/step-contract.md): the twin task gets the same `kaos-context` argument,
the follower hands it to the job unchanged, and the job's result comes back as
the same `kaos-result` output. The controller's runner Job owns submission, staging, the Slurm token,
continuation and resubmits. The in-pod waiter this module used to embed is
gone: for a project the controller does not serve, `hpc-follower` is a step
that fails at once with HPC_NOT_SERVED_MESSAGE (see refusal_template) and
nothing is submitted.
"""
import json
import re

from kubecore.kaos_lib_support import ENV as KAOS_LIB_SUPPORT_ENV
from kubecore.kaos_lib_support import support_value
from kubecore.outputs_repo import OUTPUTS_REF, outputs_repo
from kubecore.step_contract import (
    CLUSTER_ONLY_KINDS,
    CONTEXT_ENV,
    CONTEXT_PARAM,
    RESULT_PARAM,
    pair_results,
)


DEFAULT_TIME_LIMIT_MINUTES = 240
# Argo budget on top of the Slurm limit: the twin pod also waits in the
# MeluXina queue (evening waits run to hours), builds the SIF and stages the
# dataset in before the job's own clock starts. If the twin's deadline fires
# first, on_term leaves a RUNNING job to finish unobserved — so the deadline
# must comfortably exceed queue + prep + time limit.
QUEUE_ALLOWANCE_MINUTES = 480
# PRD-HPC-1461 D-44: a step that saves on the stop signal continues in a new job,
# at most this many times (MAX_CONTINUATIONS in the waiter). Each job may queue,
# prepare and run again, so the waiter's deadline covers all of them.
HPC_MAX_CONTINUATIONS = 5

# PRD-HPC-1461 AC-18, D-22: the follower is the only pod that may create an HPCArgoWorkflowJob. It runs
# as this ServiceAccount (kubecore-operator kubenv renders it in every ml environment namespace and binds
# it, alone, to the controller's submitter role plus Argo's workflowtaskresults Role); ordinary step pods
# run as the pipeline's account, which holds no HPC right.
HPC_SUBMITTER_SA = "hpc-submitter"
# The WorkflowTemplate template every `<step>-hpc` twin task calls (follower, or the refusal
# when the controller does not serve the project). Was `meluxina-run` until PRD-HPC-1461 AC-12; a
# Workflow submitted before the rename keeps the old name in its own spec and runs to the end.
HPC_TWIN_TEMPLATE = "hpc-follower"
# The twin task of a step is `<step>-hpc` on every site (PRD-HPC-1461 D-06, D-82): no site name in a run's graph.
HPC_TWIN_SUFFIX = "hpc"
STEP_KIND_LABEL = "platform.kubecore.io/step-kind"
# Where the follower writes the step's result for the workflow: the job's result, or the sentinel when the
# job handed none back. It always writes the file, so the output has no default: a follower that ends well
# without it fails the step.
FOLLOWER_RESULT_PATH = "/tmp/outputs/kaos-result.json"
HPC_ANNOTATION = "platform.kubecore.io/hpc"
# step(resumable=...): how long before its wall time the step is told to save (D-44 as amended
# 2026-11-05). Only resumable steps get the signal and continue.
HPC_RESUMABLE_ANNOTATION = "platform.kubecore.io/hpc-resumable"
# How the kubelet words a waiter pod lost with its node: Argo marks such a pod
# Failed (a pod with a status message is never an Error), so OnError alone
# never retries a Spot preemption or a node-pressure eviction.
POD_LOSS_MESSAGES = ("imminent node shutdown|node is shutting down|node is draining"
                     "|The node was low on resource|The node had condition"
                     "|Pod was rejected|Evicted|evicted|pod deleted|Pod was deleted")
POD_LOSS_RETRY = ("lastRetry.status == 'Error' or lastRetry.message matches '.*(%s).*'"
                  % POD_LOSS_MESSAGES)


def sharing_env(ctx: dict) -> list:
    """PRD-1609 node sharing, as the operator published it in pipeline-context hpc.*
    (sharing: on, scope: the project's hash). Nothing is added unless it is on, so a
    project without sharing renders the waiter exactly as before."""
    hpc = ctx.get("hpc") or {}
    # "on" as the operator renders it (quoted); a YAML 1.1 reader turns a bare on into True.
    if hpc.get("sharing") not in ("on", True) or not hpc.get("scope"):
        return []
    return [{"name": "HPC_SHARING", "value": "on"},
            {"name": "HPC_SCOPE", "value": str(hpc["scope"])}]


def controller_serves(ctx: dict) -> bool:
    """PRD-HPC-1461 D-58/D-63: the operator publishes hpc.controller "serve" once the pool's
    controller serves the project. The twin is then the credential-free follower. Any other value (or
    none) means the controller does not serve the project and the twin refuses (refusal_template)."""
    return (ctx.get("hpc") or {}).get("controller") == "serve"


def follower_template(ctx: dict, tpl: dict) -> dict:
    """The `hpc-follower` template as the follower (PRD-HPC-1461 D-59): it creates the step's
    HPCArgoWorkflowJob, prints its progress and writes the step's result from its status. It holds no
    Slurm, staging or MLflow credential and no Secret volume: the controller's runner Job carries them,
    and the endpoints come from the runner's trusted ConfigMap, not from here. Inputs, outputs, deadline,
    retry and labels are `tpl`'s (the shared base from enhance_hpc)."""
    image = (ctx.get("hpc") or {}).get("followerImage")
    if not image:
        raise ValueError("meluxina: pipeline-context hpc.controller is 'serve' but hpc.followerImage is "
                         "missing — the operator publishes the kaos-hpc-operator image there")
    env = [
        {"name": "HPC_JOB_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
        {"name": "WF_NAME", "value": "{{workflow.name}}"},
        {"name": "WF_UID", "value": "{{workflow.uid}}"},
        {"name": "STEP_NAME", "value": "{{inputs.parameters.step-name}}"},
        # The twin's DAG task name: unique per task in the entrypoint DAG, and the same on every Argo
        # retry of the twin (a retry is a new pod of the same task; only Argo's node name gets a
        # "(n)" suffix, which is not in this parameter). The job name derives from it and WF_UID.
        {"name": "NODE_KEY", "value": "{{inputs.parameters.step-name}}-%s" % HPC_TWIN_SUFFIX},
        {"name": "IMAGE_REF", "value": "{{inputs.parameters.image}}"},
        {"name": "STEP_COMMAND", "value": "{{inputs.parameters.step-command}}"},
        {"name": "SLURM_TIME_LIMIT", "value": "{{inputs.parameters.time-limit}}"},
        {"name": "HPC_CLASS", "value": "{{inputs.parameters.hpc-class}}"},
        {"name": "HPC_RESUMABLE_LEAD", "value": "{{inputs.parameters.resumable-lead}}"},
        {"name": "HPC_RESUMABLE", "value": "{{inputs.parameters.resumable}}"},
        # step(idle_timeout=...): minutes, "0" = off, "" = the platform's 20 (D-72); the follower copies it
        # to the job's idleTimeoutMinutes. The same annotation drives the in-cluster idle watchdog.
        {"name": "HPC_IDLE_TIMEOUT_MINUTES", "value": "{{inputs.parameters.idle-timeout}}"},
        {"name": "HPC_MAX_CONTINUATIONS", "value": str(HPC_MAX_CONTINUATIONS)},
        # The step's context, exactly as its pod would get it: the follower puts it in the job unchanged
        # (spec.context) and fills the job's inputs from it (every read of its ledger, every dataset an
        # upstream step wrote). Neither the follower nor the runner ever resolves a version.
        {"name": CONTEXT_ENV, "value": "{{inputs.parameters.%s}}" % CONTEXT_PARAM},
        # Where the runner stages the job's outputs to (the controller admits these by an allowlist).
        {"name": "OUTPUTS_REPO", "value": outputs_repo(ctx)},
        {"name": "OUTPUTS_REF", "value": OUTPUTS_REF},
    ] + ([{"name": KAOS_LIB_SUPPORT_ENV, "value": support_value(ctx)}] if support_value(ctx) else []) \
      + sharing_env(ctx)
    follower = dict(tpl)
    # A follower that cannot start the job says why in its last lines: show them as the step's message.
    follower["container"] = {"image": str(image), "command": ["/follower"], "env": env,
                             "terminationMessagePolicy": "FallbackToLogsOnError"}
    # Template-level: overrides the workflow's account for this one template (AC-18).
    follower["serviceAccountName"] = HPC_SUBMITTER_SA
    return follower


# What the twin says when the pool's HPC controller does not serve the project (F-35: there is no
# in-pod fallback). Shown in the step's log and as the workflow's failure; the text names the fix.
HPC_NOT_SERVED_MESSAGE = ("MeluXina steps run through the pool's HPC controller, which does not serve this "
                          "project: enable operators.hpc on the pool (KubePool spec.operators.hpc) and wait "
                          "until the project's MeluXina budget is known, or pick an in-cluster class. "
                          "Nothing was submitted.")


def refusal_template(tpl: dict) -> dict:
    """The `hpc-follower` template for a project the controller does not serve (PRD-HPC-1461 F-35).
    It only prints HPC_NOT_SERVED_MESSAGE and fails, so the run stops with a clear reason; it holds no
    credential, never submits to the site and is not retried. The template still renders (rather than
    failing the whole pipeline's render) because the author may never pick a MeluXina class; the
    refusal happens only when one is picked. Inputs, outputs and labels are `tpl`'s."""
    refusal = {k: v for k, v in tpl.items() if k not in ("retryStrategy", "activeDeadlineSeconds")}
    refusal["container"] = {"image": "python:3.12-alpine", "command": ["sh", "-c"],
                            "args": ["echo \"$HPC_NOT_SERVED\" >&2; exit 1"],
                            "env": [{"name": "HPC_NOT_SERVED", "value": HPC_NOT_SERVED_MESSAGE}]}
    return refusal


def parse_time_limit(value) -> int:
    """'90' / '90m' -> 90 minutes, '12h' -> 720. Raises ValueError otherwise."""
    text = str(value).strip()
    m = re.match(r"^([0-9]+)([mh]?)$", text)
    if not m or int(m.group(1)) == 0:
        raise ValueError(
            "%s=%r: expected minutes or hours like '90m', '12h' or '720'"
            % (HPC_ANNOTATION, value))
    minutes = int(m.group(1))
    return minutes * 60 if m.group(2) == "h" else minutes


RESUMABLE_OFF = "off"   # step(resumable=False): never continued from a checkpoint


def step_idle_timeout(step: dict) -> str:
    """The step's idle_timeout in minutes as text ("0" = off), or "" for the platform default."""
    raw = ((step.get("metadata") or {}).get("annotations") or {}).get("platform.kubecore.io/idle-timeout")
    return str(raw) if raw is not None else ""


def step_resumable_opt_out(step: dict) -> bool:
    """True when the step declared resumable=False (the follower then sets HPC_RESUMABLE=0, which the
    controller maps to HPCArgoWorkflowJob spec.resumableOptOut)."""
    raw = ((step.get("metadata") or {}).get("annotations") or {}).get(HPC_RESUMABLE_ANNOTATION)
    return raw == RESUMABLE_OFF


def step_resumable_lead(step: dict) -> int:
    """Minutes before the wall time a resumable step gets the stop signal; 0 when it is not resumable
    or opted out."""
    raw = ((step.get("metadata") or {}).get("annotations") or {}).get(HPC_RESUMABLE_ANNOTATION)
    return parse_time_limit(raw) if raw not in (None, "", RESUMABLE_OFF) else 0


def step_time_limit(step: dict) -> int:
    """Slurm wall-clock minutes for a routed step (annotation or default)."""
    raw = ((step.get("metadata") or {}).get("annotations") or {}).get(HPC_ANNOTATION)
    return parse_time_limit(raw) if raw not in (None, "") else DEFAULT_TIME_LIMIT_MINUTES


HPC_CLASS_PREFIX_FMT = "%s-"  # class names are provider-prefixed: meluxina-gpu


def runs_on_hpc(step: dict) -> bool:
    """False for a step whose kind never runs on an HPC class (ingest, publish, control): it is offered
    no HPC class and gets no HPC variant."""
    kind = ((step.get("metadata") or {}).get("labels") or {}).get(STEP_KIND_LABEL)
    return kind not in CLUSTER_ONLY_KINDS


def hpc_classes(ctx: dict) -> list:
    """The HPC compute classes the account can use, from the pipeline
    context (k8smlapp composition, gated on hpcReady). Each entry:
    {name, tier, partition, qos, gpus, cpus, memoryGiB, description}."""
    hpc = ctx.get("hpc") or {}
    if not hpc.get("enabled"):
        return []
    return [dict(c) for c in (hpc.get("classes") or []) if c.get("name")]


def _when_expr(when: str) -> str:
    """Translate a simple-tag `when` ({{workflow.parameters.X}} == Y / != Y)
    into an Argo expression; an expression `when` ({{=...}}) is passed
    through. Anything else fails the render loudly: a silently dropped gate
    would run a step the author disabled."""
    w = (when or "").strip()
    if not w:
        return ""
    if w.startswith("{{=") and w.endswith("}}"):
        return w[3:-2].strip()
    m = re.match(r"^\{\{\s*workflow\.parameters\.([A-Za-z0-9_.-]+)\s*\}\}\s*(==|!=)\s*(\S+)$", w)
    if not m:
        raise ValueError(
            "meluxina: cannot combine the step's when %r with the HPC class gate — "
            "use {{workflow.parameters.X}} == Y / != Y or an {{= expression }}" % when)
    param, op, value = m.groups()
    value = value.strip("'\"")
    return "workflow.parameters['%s'] %s '%s'" % (param, op, value)


def enhance_hpc(spec: dict, ctx: dict, steps: list, gpu_step_names: set) -> None:
    """Per-step HPC placement. Every step's
    {step}-class dropdown carries the HPC classes (enhance_class_param);
    here each un-pinned step gets a Slurm twin, and the pair is gated on
    the chosen class: in-cluster when the class is not HPC, twin when it is.
    A step's own `when` is preserved on both."""
    classes = hpc_classes(ctx)
    if not classes:
        return
    provider = (ctx.get("hpc") or {}).get("provider", "meluxina")
    prefix = HPC_CLASS_PREFIX_FMT % provider

    # The global `target` switch is gone: placement is per step.
    parameters = spec["arguments"]["parameters"]
    parameters[:] = [p for p in parameters if p.get("name") != "target"]

    entry = spec.get("entrypoint")
    dag_tpl = next((t for t in spec.get("templates", [])
                    if t.get("name") == entry and "dag" in t), None)
    if dag_tpl is None:
        return
    tasks = dag_tpl["dag"]["tasks"]
    by_tpl = {s["name"]: s for s in steps}

    def _routable(task):
        step = by_tpl.get(task.get("template"))
        annots = ((step or {}).get("metadata") or {}).get("annotations") or {}
        # pinned to one in-cluster class: no HPC twin. Nor a step whose kind never runs on HPC (the
        # compose step is one: its kind is control).
        return (step is not None and runs_on_hpc(step)
                and "platform.kubecore.io/compute-class" not in annots)

    routable = {t["name"] for t in tasks if _routable(t)}
    routed = []
    for task in list(tasks):
        if task["name"] not in routable:
            continue
        step = by_tpl[task["template"]]
        class_param = "workflow.parameters['%s-class']" % step["name"]
        own = _when_expr(task.get("when", ""))
        hpc_gate = "hasPrefix(%s, '%s')" % (class_param, prefix)
        task["when"] = "{{=%s!%s}}" % ("(%s) && " % own if own else "", hpc_gate)
        twin_when = "{{=%s%s}}" % ("(%s) && " % own if own else "", hpc_gate)
        container = step.get("container") or {}
        # The step's command, the same on HPC as in its pod: `python -m app.entry`, no arguments.
        cmd = list(container.get("command") or []) + list(container.get("args") or [])
        context = next((p.get("value") for p in ((task.get("arguments") or {}).get("parameters") or [])
                        if p.get("name") == CONTEXT_PARAM), None)
        if context is None:
            raise ValueError("meluxina: task %r passes no %s, so step %r cannot get its HPC variant"
                             % (task["name"], CONTEXT_PARAM, step["name"]))
        minutes = step_time_limit(step)
        lead = step_resumable_lead(step)
        if lead >= minutes:
            raise ValueError("step %s: resumable lead %d min is not shorter than its wall time %d min"
                             % (step["name"], lead, minutes))
        # Resumability is discovered (D-74): the controller may continue ANY step that is not opted out
        # (resumable=False), up to (1 + HPC_MAX_CONTINUATIONS) jobs, each queuing, preparing and running
        # again, so the waiter's Argo deadline must cover all of them or Argo would cut a continuing step.
        # Only an opted-out step runs one job and keeps a one-job deadline.
        jobs = 1 if step_resumable_opt_out(step) else 1 + HPC_MAX_CONTINUATIONS
        if any(t["name"] == task["name"] + "-" + HPC_TWIN_SUFFIX for t in tasks):
            raise ValueError("meluxina: a task named %r already exists, so step %r cannot get its HPC variant"
                             % (task["name"] + "-" + HPC_TWIN_SUFFIX, task["name"]))
        twin = {
            "name": task["name"] + "-" + HPC_TWIN_SUFFIX,
            "template": HPC_TWIN_TEMPLATE,
            "when": twin_when,
            "arguments": {"parameters": [
                {"name": "step-name", "value": task["name"]},
                {"name": "image", "value": container.get("image", "")},
                {"name": "step-command", "value": json.dumps(cmd, separators=(",", ":"))},
                {"name": "step-kind", "value": ((step.get("metadata") or {}).get("labels") or {}).get(STEP_KIND_LABEL, "")},
                {"name": "time-limit", "value": str(minutes)},
                {"name": "deadline-seconds",
                 "value": str(jobs * (minutes + QUEUE_ALLOWANCE_MINUTES) * 60)},
                {"name": "resumable-lead", "value": str(lead)},
                # "0" = resumable=False; "auto" lets the controller discover resumability (D-74)
                {"name": "resumable", "value": "0" if step_resumable_opt_out(step) else "auto"},
                {"name": "idle-timeout", "value": step_idle_timeout(step)},
                {"name": "hpc-class", "value": "{{workflow.parameters.%s-class}}" % step["name"]},
                {"name": CONTEXT_PARAM, "value": context},
            ]},
        }
        if task.get("depends"):
            twin["depends"] = task["depends"]
        # Right after its in-cluster task, not at the end: the Argo UI's
        # template graph needs every dependency listed above its dependent,
        # and later steps depend on both twins (crashes reading 'genre').
        tasks.insert(tasks.index(task) + 1, twin)
        routed.append(task["name"])

    if not routed:
        return

    # A step that needs a routed step reads its result from the pair: exactly one of the two tasks ran,
    # the other was skipped and has no result. Twins included: a twin whose upstream also ran on HPC
    # must read the upstream twin, not the skipped in-cluster task.
    for task in tasks:
        for p in ((task.get("arguments") or {}).get("parameters") or []):
            if p.get("name") == CONTEXT_PARAM and isinstance(p.get("value"), str):
                for r in routed:
                    p["value"] = pair_results(p["value"], r, r + "-" + HPC_TWIN_SUFFIX)

    # A bare task token in Argo depends grammar means Succeeded; a routed dep
    # is now a twin pair where exactly one twin runs and the other is Skipped.
    # Twins included: a twin whose upstream also ran on HPC must depend on
    # the upstream PAIR, not on the (Skipped) in-cluster task.
    # Status-qualified references (step.Succeeded || step.Skipped
    # || step.Omitted) must name the twin too: Argo exposes tasks.X to
    # a task's expressions ONLY for X in its depends, so a consumer whose
    # args pick tasks['X-hpc'] hangs forever with "tasks.X-hpc is
    # missing" (observed at run time, argo v4.0.6). Pair semantics:
    # Succeeded/Failed/Errored/Daemoned = either twin; Skipped/Omitted = both.
    def _status_pair(r):
        def sub(m):
            st = m.group(1)
            op = "&&" if st in ("Skipped", "Omitted") else "||"
            return "(%s.%s %s %s-%s.%s)" % (r, st, op, r, HPC_TWIN_SUFFIX, st)
        return sub

    for task in tasks:
        dep = task.get("depends")
        if not dep:
            continue
        for r in routed:
            if task["name"] in (r, r + "-" + HPC_TWIN_SUFFIX):
                continue
            dep = re.sub(r"(?<![\w.-])%s\.(Succeeded|Skipped|Omitted|Failed|Errored|Daemoned)(?![\w.-])"
                         % re.escape(r), _status_pair(r), dep)
            pair = ("((%s.Succeeded || %s.Skipped) && "
                    "(%s-%s.Succeeded || %s-%s.Skipped))"
                    % (r, r, r, HPC_TWIN_SUFFIX, r, HPC_TWIN_SUFFIX))
            dep = re.sub(r"(?<![\w.-])%s(?![\w.-])" % re.escape(r), pair, dep)
        task["depends"] = dep

    tpl = {
        "name": HPC_TWIN_TEMPLATE,
        "inputs": {"parameters": [
            {"name": "step-name"}, {"name": "image"}, {"name": "step-command"}, {"name": "step-kind"},
            {"name": "time-limit"}, {"name": "deadline-seconds"},
            {"name": "hpc-class"},
            {"name": "resumable-lead", "value": "0"}, {"name": "resumable", "value": "auto"},
            {"name": "idle-timeout", "value": ""},
            {"name": CONTEXT_PARAM}]},
        # Per-step: Slurm time limit + queue/prep allowance (see
        # QUEUE_ALLOWANCE_MINUTES). Argo resolves the parameter here.
        "activeDeadlineSeconds": "{{inputs.parameters.deadline-seconds}}",
        # A lost follower pod (node preemption, eviction, deletion) is not a
        # step failure: retry it - the job's name is deterministic (workflow UID
        # + step), so the retried follower re-attaches to the same
        # HPCArgoWorkflowJob instead of creating a duplicate. A job that failed
        # on MeluXina is not retried.
        "retryStrategy": {"limit": "2", "retryPolicy": "Always",
                          "expression": POD_LOSS_RETRY},
        "metadata": {"labels": {"platform.kubecore.io/compute-type": "hpc",
                                "platform.kubecore.io/hpc-class": "{{inputs.parameters.hpc-class}}",
                                STEP_KIND_LABEL: "{{inputs.parameters.step-kind}}"}},
        # slurm-job-id, and the step's one result (the same output its pod would have).
        "outputs": {"parameters": [{"name": "slurm-job-id",
                                    "valueFrom": {"path": "/tmp/slurm-job-id"}},
                                   {"name": RESULT_PARAM, "valueFrom": {"path": FOLLOWER_RESULT_PATH}}]},
    }
    # F-35: the controller's follower, or a refusal; never an in-pod submitter.
    tpl = follower_template(ctx, tpl) if controller_serves(ctx) else refusal_template(tpl)
    spec["templates"].append(tpl)
