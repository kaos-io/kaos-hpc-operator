"""PRD-HPC-1461 D-76 — the Kubernetes backend of the one resilience contract (platform-owned).

`enhance_resilience` runs once per developer step and injects, with no client code:

  Tier 1  retryStrategy   one rule for every step: a step lost with its node (spot preemption, eviction,
                          node shutdown, deleted pod: `meluxina.POD_LOSS_RETRY`) is re-run, and so is a
                          step that ends itself with exit code 85 ("continue", kaos_lib.checkpoint
                          exit_for_restart). NEVER on the step's own other non-zero exit, and NEVER at its
                          deadline: the deadline is the platform's cap and is final.
                          Safe because step outputs are published only on success.
                          A step the pool's idle watchdog ended (reason Idle, message "KaosIdle: ...")
                          is not re-run either: the expression excludes that message.
  Folder  KAOS_CHECKPOINT_DIR   every step gets the env var and a pod-local emptyDir behind it.
  Tier 2  persistence     for every step that may compute (every kind but control and publish): an init container
                          restores the newest verified copy of the folder from lakeFS, a sidecar copies it
                          periodically and on SIGTERM. The code is `kubecore/checkpoint_helper.py`, shipped
                          ONCE per app as the ConfigMap `<app>-ckpt-helper` (helper_configmap) that every
                          helper container mounts read-only. It used to be embedded (compressed) in every
                          helper container, ~19 KB per step: five GPU steps with their twins pushed the
                          rendered WorkflowTemplate past the 256 KiB last-applied annotation limit.

The helper image comes from the platform registry only (`checkpoints.helperImage`, see helper_image).

Where copies live: the app's lakeFS outputs repository `outputs-<app>` (see outputs_repo.py), on branch
`kaos-checkpoints` (never committed or merged), under `resume/<workflow name>/<step>/`.

Fill-absent like every pass of the enhancer: a developer-set retryStrategy, env var, volume or sidecar of
the same name is left alone.
"""
import sys
from pathlib import Path

import yaml

from kubecore.meluxina import POD_LOSS_RETRY, STEP_KIND_LABEL
from kubecore.outputs_repo import outputs_repo

CHECKPOINT_ENV = "KAOS_CHECKPOINT_DIR"
CHECKPOINT_MOUNT = "/kaos/checkpoint"
CHECKPOINT_VOLUME = "kaos-checkpoint"
STAGE_VOLUME = "kaos-checkpoint-stage"
STAGE_MOUNT = "/stage"
CHECKPOINT_BRANCH = "kaos-checkpoints"
CHECKPOINT_ROOT = "resume"
DEFAULT_EVERY_MINUTES = 10
RETRY_BACKOFF = {"duration": "30s", "factor": "2", "maxDuration": "10m"}
INIT_NAME = "kaos-ckpt-restore"
SIDECAR_NAME = "kaos-ckpt-copy"
HELPER_PATH = Path(__file__).with_name("checkpoint_helper.py")
HELPER_FILE = "checkpoint_helper.py"   # the ConfigMap key and the file name under HELPER_MOUNT
HELPER_VOLUME = "kaos-ckpt-helper"
HELPER_MOUNT = "/ckpt-helper"
HELPER_CONFIGMAP_SUFFIX = "-ckpt-helper"
# Kinds whose steps never compute: they steer the run or hand a result over, so they get no checkpoint helper.
NO_HELPER_KINDS = ("control", "publish")


# The pool's idle watchdog ends a step by setting its pod Failed with a status message that starts with
# this prefix (kaos-hpc-operator internal/idlewatch: MessagePrefix). Argo copies it to the step's message.
IDLE_END_PREFIX = "KaosIdle"
# An Idle end is the step's own outcome, never a lost node: whatever else the message says (a pod that
# was deleted after it, a node-level word), a message that starts with the prefix is not re-run.
# The one retry rule. A step that ends itself with exit code 85 asks to be run again (kaos_lib.checkpoint
# exit_for_restart): the restore init container gives it its newest saved copy. A step lost with its node is run
# again the same way. Argo has one `limit` for both and keeps no count of its own for either, so the limit is a
# bound, not the count: a step that saves progress ends by itself or by its cap (the pod's deadline, never retried).
CONTINUE_EXIT_CODE = "85"
RETRY_LIMIT = "50"
RETRY_EXPRESSION = ("((%s) or lastRetry.exitCode == '%s') and not (lastRetry.message startsWith '%s')"
                    % (POD_LOSS_RETRY, CONTINUE_EXIT_CODE, IDLE_END_PREFIX))


def retry_strategy() -> dict:
    """Tier 1: retry a step lost with its node or one that exits 85; never one that failed by itself, reached its
    deadline or was ended as Idle."""
    return {"limit": RETRY_LIMIT, "retryPolicy": "Always", "expression": RETRY_EXPRESSION, "backoff": dict(RETRY_BACKOFF)}


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


def wants_helper(step: dict, ctx: dict, inject_off: bool, shared: set) -> bool:
    kind = ((step.get("metadata") or {}).get("labels") or {}).get(STEP_KIND_LABEL)
    if kind in NO_HELPER_KINDS or inject_off or step["name"] in shared:
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


def helper_env(step: dict, ctx: dict) -> list:
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
        {"name": "CKPT_EVERY_MIN", "value": str(DEFAULT_EVERY_MINUTES)},
        {"name": "CKPT_STAGE", "value": STAGE_MOUNT},
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
    ]


def helper_capabilities(mode: str) -> dict:
    """The step may run as any user and write files only it can read (mode 0600, folders 0700). The copy
    sidecar runs as root with DAC_OVERRIDE (admitted by the baseline Pod Security level, unlike
    DAC_READ_SEARCH) so it can read them. The restore init container writes into the emptyDir (0777) as
    root and chmods only what it extracted itself, so it needs no capability."""
    return {"drop": ["ALL"], "add": ["DAC_OVERRIDE"]} if mode == "watch" else {"drop": ["ALL"]}


def helper_container(name: str, mode: str, step: dict, ctx: dict) -> dict:
    """One helper container (init or sidecar): small requests, locked down, never the step's GPU."""
    return {
        "name": name, "image": helper_image(ctx),
        "command": ["python3", "%s/%s" % (HELPER_MOUNT, HELPER_FILE), mode],
        "env": helper_env(step, ctx),
        "resources": {"requests": {"cpu": "10m", "memory": "64Mi"}, "limits": {"memory": "512Mi"}},
        "securityContext": {"runAsUser": 0, "runAsGroup": 0, "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True, "capabilities": helper_capabilities(mode)},
        "volumeMounts": [{"name": CHECKPOINT_VOLUME, "mountPath": CHECKPOINT_MOUNT},
                         {"name": STAGE_VOLUME, "mountPath": STAGE_MOUNT},
                         {"name": HELPER_VOLUME, "mountPath": HELPER_MOUNT, "readOnly": True}],
    }


def inject_folder(step: dict, helper: bool, ctx: dict) -> bool:
    """Env var + emptyDir for the folder. Returns False when the developer chose their own folder."""
    container = step["container"]
    env = container.setdefault("env", [])
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


def enhance_resilience(step: dict, ctx: dict, inject_off: bool, shared: set) -> None:
    """Apply the Kubernetes backend of the resilience contract to one step template."""
    step.setdefault("retryStrategy", retry_strategy())
    helper = wants_helper(step, ctx, inject_off, shared)
    if not inject_folder(step, helper, ctx) or not helper:
        return
    step.setdefault("initContainers", [])
    step.setdefault("sidecars", [])
    _add_named(step["initContainers"], helper_container(INIT_NAME, "restore", step, ctx))
    _add_named(step["sidecars"], helper_container(SIDECAR_NAME, "watch", step, ctx))
