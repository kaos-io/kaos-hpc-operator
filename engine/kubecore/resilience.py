"""PRD-HPC-1461 D-76 — the Kubernetes backend of the one resilience contract (platform-owned).

Every step can fail and be run again (PRD kaos#1911 D-64): nothing is declared in `pipeline.py`.
`enhance_resilience` runs once per developer step and injects, with no client code:

  Retry       one rule for every step (the table in `endings.json`, read through `kubecore.endings`): a step lost
              with its machine (spot preemption, eviction, node shutdown, deleted pod), one that reached its time
              limit, and one that asked to be continued (exit 85) are started again, at most MAX_CONTINUATIONS +
              MAX_RESTARTS times, with backoff. NEVER on the step's own non-zero exit, out of memory, a missing
              result or the pool's idle watchdog (message "KaosIdle: ..."). Safe because step outputs are
              published only on success. A step that several tasks or a matrix share one checkpoint folder, so it
              is started again only when its machine was lost.
  Folder      KAOS_CHECKPOINT_DIR   every step gets the env var and a pod-local emptyDir behind it.
  Attempt     KAOS_ATTEMPT          which attempt this is, 1 for the first (the same meaning on every executor).
  Persistence an init container restores the newest verified copy of the folder from lakeFS, a sidecar copies it
              periodically and on SIGTERM. The code is `kubecore/checkpoint_helper.py`, shipped
              ONCE per app as the ConfigMap `<app>-ckpt-helper` (helper_configmap) that every
              helper container mounts read-only. It used to be embedded (compressed) in every
              helper container, ~19 KB per step: five GPU steps with their twins pushed the
              rendered WorkflowTemplate past the 256 KiB last-applied annotation limit.

The helper image comes from the platform registry only (`checkpoints.helperImage`, see helper_image).

Opt-out per step: annotation `platform.kubecore.io/checkpoint-every: "off"` (authoring:
`checkpoint_every=False`, refused for a step with `gpu=`) removes the persistence helper only; the retry and the folder stay.

Where copies live: the app's lakeFS outputs repository `outputs-<app>` (see outputs_repo.py), on branch
`kaos-checkpoints` (never committed or merged), under `resume/<workflow name>/<step>/`.

Fill-absent like every pass of the enhancer: a developer-set retryStrategy, env var, volume or sidecar of
the same name is left alone.
"""
import re
import sys
from pathlib import Path

import yaml

from kubecore.endings import (
    CONTINUE_EXIT_CODE,
    DEADLINE_MESSAGES,
    IDLE_END_PREFIX,
    MAX_CONTINUATIONS,
    MAX_RESTARTS,
    POD_LOSS_RETRY,
)
from kubecore.outputs_repo import outputs_repo
from kubecore.run_settings import add_input, add_task_argument, setting

CHECKPOINT_ENV = "KAOS_CHECKPOINT_DIR"
CHECKPOINT_MOUNT = "/kaos/checkpoint"
CHECKPOINT_VOLUME = "kaos-checkpoint"
STAGE_VOLUME = "kaos-checkpoint-stage"
STAGE_MOUNT = "/stage"
CHECKPOINT_BRANCH = "kaos-checkpoints"
CHECKPOINT_ROOT = "resume"
DEFAULT_EVERY_MINUTES = 10
ATTEMPT_ENV = "KAOS_ATTEMPT"
# Argo counts retries from 0; the step is told 1 for its first attempt, as on an HPC class.
ATTEMPT_VALUE = "{{=asInt(retries) + 1}}"
RETRY_BACKOFF = {"duration": "30s", "factor": "2", "maxDuration": "10m"}
INIT_NAME = "kaos-ckpt-restore"
SIDECAR_NAME = "kaos-ckpt-copy"
HELPER_PATH = Path(__file__).with_name("checkpoint_helper.py")
HELPER_FILE = "checkpoint_helper.py"   # the ConfigMap key and the file name under HELPER_MOUNT
HELPER_VOLUME = "kaos-ckpt-helper"
HELPER_MOUNT = "/ckpt-helper"
HELPER_CONFIGMAP_SUFFIX = "-ckpt-helper"
CHECKPOINT_PARAM = "checkpoint-every"
CHECKPOINT_PARAM_REF = "{{inputs.parameters.%s}}" % CHECKPOINT_PARAM
EVERY_RE = re.compile(r"^(off|[0-9]+)$")


# Machine lost: started again, except a step the idle watchdog ended: whatever else its message says (a pod that
# was deleted after it, a node-level word), a message that starts with the prefix is not run again.
RETRY_EXPRESSION = "(%s) and not (lastRetry.message startsWith '%s')" % (POD_LOSS_RETRY, IDLE_END_PREFIX)
# Every other step also continues when it reached its time limit and when it asked to be continued; each attempt
# restores the newest copy of its checkpoint folder (the restore init container).
CONTINUE_EXPRESSION = ("((%s) or lastRetry.exitCode == '%s' or lastRetry.message matches '.*(%s).*')"
                       " and not (lastRetry.message startsWith '%s')"
                       % (POD_LOSS_RETRY, CONTINUE_EXIT_CODE, DEADLINE_MESSAGES, IDLE_END_PREFIX))


# A step of these kinds acts on the outside world (it publishes, or decides for the run). After it may have started,
# it is not started again by the platform: a lost machine is retried only when the pod was refused before it ran.
SIDE_EFFECT_KINDS = ("publish", "control")
NOT_STARTED_EXPRESSION = "lastRetry.message contains 'Pod was rejected'"


def retry_strategy(shared: bool = False, side_effects: bool = False) -> dict:
    """The one retry rule. A step that tasks or a matrix share has one checkpoint folder for all of them, so
    it is started again only when its machine was lost, and only as often as a restart from zero is allowed."""
    if side_effects:
        return {"limit": str(MAX_RESTARTS), "retryPolicy": "Always", "expression": NOT_STARTED_EXPRESSION,
                "backoff": dict(RETRY_BACKOFF)}
    if shared:
        return {"limit": str(MAX_RESTARTS), "retryPolicy": "Always", "expression": RETRY_EXPRESSION,
                "backoff": dict(RETRY_BACKOFF)}
    return {"limit": str(MAX_CONTINUATIONS + MAX_RESTARTS), "retryPolicy": "Always", "expression": CONTINUE_EXPRESSION,
            "backoff": dict(RETRY_BACKOFF)}


def helper_image(ctx: dict) -> str:
    """The image the helper containers run, from the platform registry: `checkpoints.helperImage`, which the
    operator publishes in every app's pipeline context (the pool's pinned kaos-hpc-operator image), else the
    HPC follower image (the same image). Empty when the context has neither: there is deliberately NO public
    registry fallback (no third-party pull in tenant pods); the step then gets the re-run and the folder
    but no copies, and the app picks the image up on its next render."""
    named = (ctx.get("checkpoints") or {}).get("helperImage")
    follower = (ctx.get("hpc") or {}).get("followerImage")
    return str(named or follower or "")


def helper_configmap_name(ctx: dict) -> str:
    """The app's helper ConfigMap: `<app>-ckpt-helper`, in the WorkflowTemplate's namespace."""
    app = ctx.get("app")
    if not app:
        raise ValueError("context is missing 'app' - cannot name the checkpoint helper ConfigMap")
    return f"{app}{HELPER_CONFIGMAP_SUFFIX}"


def helper_configmap(ctx: dict) -> dict:
    """The ConfigMap manifest that carries the helper source once for every helper container of the app."""
    return {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": helper_configmap_name(ctx), "namespace": ctx["namespace"],
            "labels": {"platform.kubecore.io/project": ctx["project"], "platform.kubecore.io/app": ctx["app"],
                       "platform.kubecore.io/managed-by": "kubecore-ci"},
        },
        "data": {HELPER_FILE: HELPER_PATH.read_text()},
    }


def uses_helper_configmap(wft: dict, ctx: dict) -> bool:
    """True when a template of the enhanced WorkflowTemplate mounts the helper ConfigMap."""
    name = helper_configmap_name(ctx)
    return any((v.get("configMap") or {}).get("name") == name
               for t in wft["spec"].get("templates", []) for v in t.get("volumes", []))


def write_helper_configmap(wft: dict, ctx: dict, path: Path) -> bool:
    """Write the helper ConfigMap to `path` when the WorkflowTemplate needs it, else remove a stale file.
    Returns whether the file was written. The CI commits it next to workflow-template.yaml."""
    if not uses_helper_configmap(wft, ctx):
        path.unlink(missing_ok=True)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(helper_configmap(ctx), sort_keys=False, default_flow_style=False))
    return True


def every_minutes(annots: dict) -> str:
    """The step's copy interval: 'off', minutes as text, or '' for the default."""
    raw = (annots.get("checkpoint-every") or "").strip().lower()
    if raw and not EVERY_RE.match(raw):
        raise ValueError("platform.kubecore.io/checkpoint-every=%r: expected 'off' or minutes like '20'" % raw)
    return raw


def every_field_default(annots: dict) -> str:
    """The default of the run form's `<step>-checkpoint-every`, in minutes as text, or '' when the step has no such field. The
    form has the field for a step that declares `checkpoint_every=` (its value is the default); `checkpoint_every=False`
    removes it. Any other step is copied at the engine's DEFAULT_EVERY_MINUTES."""
    every = every_minutes(annots)
    return every if every.isdigit() else ""


def shared_templates(spec: dict) -> set:
    """Templates run by more than one DAG task or by a fan-out: one checkpoint key would be shared."""
    uses, fanned = {}, set()
    for tpl in spec.get("templates", []):
        # A matrix step runs once for each combination inside a steps template: its checkpoint key would be shared.
        for entry in (e for group in tpl.get("steps") or [] for e in group):
            if "withParam" in entry:
                fanned.add(entry.get("template"))
        for task in (tpl.get("dag") or {}).get("tasks", []):
            name = task.get("template")
            uses[name] = uses.get(name, 0) + 1
            if any(k in task for k in ("withItems", "withParam", "withSequence")):
                fanned.add(name)
    return fanned | {n for n, c in uses.items() if c > 1}


def wants_helper(step: dict, every: str, ctx: dict, inject_off: bool, shared: set) -> bool:
    if every == "off" or inject_off or step["name"] in shared:
        return False
    lakefs = ctx.get("lakefs") or {}
    if not (lakefs.get("endpoint") and lakefs.get("adminSecret") and lakefs.get("adminSecretKeys")):
        return False
    if not helper_image(ctx):
        print("kubecore: step %r: no checkpoints.helperImage in the pipeline context; its checkpoint copies are "
              "off until the platform publishes it (re-run and folder are still injected)" % step["name"],
              file=sys.stderr)
        return False
    return True


def _add_named(items: list, entry: dict) -> None:
    if not any(i.get("name") == entry["name"] for i in items):
        items.append(entry)


def helper_env(step: dict, ctx: dict, every_field: str) -> list:
    keys, secret = ctx["lakefs"]["adminSecretKeys"], ctx["lakefs"]["adminSecret"]
    ref = lambda key: {"secretKeyRef": {"name": secret, "key": keys[key]}}  # noqa: E731
    return [
        {"name": CHECKPOINT_ENV, "value": CHECKPOINT_MOUNT},
        {"name": "LAKEFS_ENDPOINT", "value": ctx["lakefs"]["endpoint"]},
        {"name": "LAKEFS_ACCESS_KEY", "valueFrom": ref("accessKey")},
        {"name": "LAKEFS_SECRET_KEY", "valueFrom": ref("secretKey")},
        {"name": "CKPT_REPO", "value": outputs_repo(ctx)},
        {"name": "CKPT_BRANCH", "value": CHECKPOINT_BRANCH},
        {"name": "CKPT_ROOT", "value": CHECKPOINT_ROOT},
        {"name": "CKPT_KEY", "value": "{{workflow.name}}/%s" % step["name"]},
        # A step with the run form's `<step>-checkpoint-every` is copied at that interval; any other step at the default.
        {"name": "CKPT_EVERY_MIN",
         "value": CHECKPOINT_PARAM_REF if every_field else str(DEFAULT_EVERY_MINUTES)},
        {"name": "CKPT_STAGE", "value": STAGE_MOUNT},
        {"name": ATTEMPT_ENV, "value": ATTEMPT_VALUE},
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
    ]


def helper_capabilities(mode: str) -> dict:
    """The step may run as any user and write files only it can read (mode 0600, folders 0700). The copy
    sidecar runs as root with DAC_OVERRIDE (admitted by the baseline Pod Security level, unlike
    DAC_READ_SEARCH) so it can read them. The restore init container writes into the emptyDir (0777) as
    root and chmods only what it extracted itself, so it needs no capability."""
    return {"drop": ["ALL"], "add": ["DAC_OVERRIDE"]} if mode == "watch" else {"drop": ["ALL"]}


def helper_container(name: str, mode: str, step: dict, ctx: dict, every_field: str) -> dict:
    """One helper container (init or sidecar): small requests, locked down, never the step's GPU."""
    return {
        "name": name, "image": helper_image(ctx),
        "command": ["python3", "%s/%s" % (HELPER_MOUNT, HELPER_FILE), mode],
        "env": helper_env(step, ctx, every_field),
        "resources": {"requests": {"cpu": "10m", "memory": "64Mi"}, "limits": {"memory": "512Mi"}},
        "securityContext": {"runAsUser": 0, "runAsGroup": 0, "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True, "capabilities": helper_capabilities(mode)},
        "volumeMounts": [{"name": CHECKPOINT_VOLUME, "mountPath": CHECKPOINT_MOUNT},
                         {"name": STAGE_VOLUME, "mountPath": STAGE_MOUNT},
                         {"name": HELPER_VOLUME, "mountPath": HELPER_MOUNT, "readOnly": True}],
    }


def inject_folder(step: dict, helper: bool, ctx: dict) -> bool:
    """Env vars (the attempt, the folder) + emptyDir for the folder. Returns False when the developer chose their own folder."""
    container = step["container"]
    env = container.setdefault("env", [])
    if not any(e.get("name") == ATTEMPT_ENV for e in env):
        env.append({"name": ATTEMPT_ENV, "value": ATTEMPT_VALUE})
    if any(e.get("name") == CHECKPOINT_ENV for e in env):
        return False
    env.append({"name": CHECKPOINT_ENV, "value": CHECKPOINT_MOUNT})
    mounts = container.setdefault("volumeMounts", [])
    if not any(m.get("mountPath") == CHECKPOINT_MOUNT for m in mounts):
        mounts.append({"name": CHECKPOINT_VOLUME, "mountPath": CHECKPOINT_MOUNT})
    _add_named(step.setdefault("volumes", []), {"name": CHECKPOINT_VOLUME, "emptyDir": {}})
    if helper:
        _add_named(step["volumes"], {"name": STAGE_VOLUME, "emptyDir": {}})
        _add_named(step["volumes"], {"name": HELPER_VOLUME,
                                     "configMap": {"name": helper_configmap_name(ctx), "defaultMode": 0o444}})
    return True


def enhance_resilience(spec: dict, step: dict, ctx: dict, annots: dict, inject_off: bool,
                       shared: set) -> None:
    """Apply the Kubernetes backend of the resilience contract to one step template."""
    kind = ((step.get("metadata") or {}).get("labels") or {}).get("platform.kubecore.io/step-kind")
    side_effects = kind in SIDE_EFFECT_KINDS and step["name"] not in shared
    step.setdefault("retryStrategy", retry_strategy(step["name"] in shared, side_effects))
    every = every_minutes(annots)
    helper = not side_effects and wants_helper(step, every, ctx, inject_off, shared)
    if not inject_folder(step, helper, ctx) or not helper:
        return
    every_field = every_field_default(annots)
    if every_field:
        add_input(step, CHECKPOINT_PARAM, every_field)
        add_task_argument(spec, step["name"], CHECKPOINT_PARAM, "{{=%s}}" % setting(step["name"], "checkpoint-every"))
    step.setdefault("initContainers", [])
    step.setdefault("sidecars", [])
    _add_named(step["initContainers"], helper_container(INIT_NAME, "restore", step, ctx, every_field))
    _add_named(step["sidecars"], helper_container(SIDECAR_NAME, "watch", step, ctx, every_field))
