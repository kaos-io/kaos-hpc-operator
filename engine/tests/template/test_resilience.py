"""The Kubernetes backend of the resilience contract (PRD-HPC-1461 D-76), as the enhancer renders it.

Tier 1 re-run (retryStrategy), the KAOS_CHECKPOINT_DIR folder on every step, and the restore init
container + copy sidecar on GPU steps (or any step with checkpoint_every). The GPU step's injected pieces
are compared with tests/golden/resilience_gpu_step.yaml (regenerate: UPDATE_GOLDEN=1 pytest).
Run:  python -m pytest tests/ -v
"""
import copy
import os
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import authoring, enhance, meluxina, resilience  # noqa: E402

CONTEXT = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
CONTEXT.pop("hpc")      # in-cluster only: no MeluXina twins in these renders
GOLDEN = ROOT / "tests" / "template" / "golden" / "resilience_gpu_step.yaml"
INJECTED = ("retryStrategy", "volumes", "initContainers", "sidecars")


def render(build, ctx=None):
    with authoring.pipeline("t") as p:
        build()
    wf = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx or CONTEXT)
    return {t["name"]: t for t in wf["spec"]["templates"]}


def env_of(tpl, container="container"):
    return {e["name"]: e for e in tpl[container]["env"]}


def pipeline_steps():
    authoring.step("prep", kind="evaluate")
    authoring.step("train", kind="evaluate", gpu=True)
    authoring.step("cpu-opt-in", kind="evaluate", checkpoint_every="20m")
    authoring.step("gpu-off", kind="evaluate", gpu=True, checkpoint_every=False)


@pytest.fixture(scope="module")
def tpls():
    return render(pipeline_steps)


def test_should_retry_every_step_only_when_the_node_is_lost(tpls):
    for name in ("prep", "train", "cpu-opt-in", "gpu-off", "compose-and-validate"):
        retry = tpls[name]["retryStrategy"]
        assert retry["retryPolicy"] == "Always" and retry["limit"] == "2"
        assert retry["expression"] == resilience.RETRY_EXPRESSION
        assert retry["backoff"]["duration"] == "30s"


def test_should_keep_the_pod_loss_rule_and_exclude_a_step_ended_as_idle():
    """The pool's idle watchdog (kaos-hpc-operator internal/idlewatch) ends a step with a pod status
    message that starts with KaosIdle. Checked against the real Argo expression engine (expr-lang/expr
    v1.17) for these inputs: Failed+KaosIdle -> not re-run, Error+KaosIdle -> not re-run, "pod deleted"
    -> re-run, node shutdown -> re-run, Error (exit code 1) -> not re-run."""
    expr = resilience.RETRY_EXPRESSION
    assert expr.startswith("(%s)" % meluxina.POD_LOSS_RETRY)     # the pod-loss rule is unchanged
    assert expr.endswith("and not (lastRetry.message startsWith '%s')" % resilience.IDLE_END_PREFIX)
    assert resilience.IDLE_END_PREFIX == "KaosIdle"


def test_should_not_retry_the_steps_own_failure(tpls):
    msgs = meluxina.POD_LOSS_MESSAGES
    for own in ("Error (exit code 1)", "OOMKilled (exit code 137)", "Error (exit code 85)"):
        assert not re.search(".*(%s).*" % msgs, own), own


def test_should_refuse_a_retry_strategy_the_developer_set():
    def build():
        authoring.step("s", kind="evaluate")
    with authoring.pipeline("t") as p:
        build()
    raw = yaml.safe_load(p.wt.to_yaml())
    next(t for t in raw["spec"]["templates"] if t["name"] == "s")["retryStrategy"] = {"limit": "5"}
    with pytest.raises(enhance.EnhanceError, match="step 's' carries 'retryStrategy'"):
        enhance.enhance(raw, CONTEXT)


def test_should_give_every_step_the_folder_env_and_an_empty_dir(tpls):
    for name in ("prep", "train", "cpu-opt-in", "gpu-off"):
        tpl = tpls[name]
        assert env_of(tpl)["KAOS_CHECKPOINT_DIR"]["value"] == resilience.CHECKPOINT_MOUNT
        assert {"name": "kaos-checkpoint", "emptyDir": {}} in tpl["volumes"]
        assert {"name": "kaos-checkpoint", "mountPath": "/kaos/checkpoint"} in tpl["container"]["volumeMounts"]


def test_should_add_the_helper_to_gpu_steps_and_opt_in_cpu_steps_only(tpls):
    assert [c["name"] for c in tpls["train"]["initContainers"]] == ["kaos-ckpt-restore"]
    assert [c["name"] for c in tpls["train"]["sidecars"]] == ["kaos-ckpt-copy"]
    assert "sidecars" in tpls["cpu-opt-in"]
    for name in ("prep", "gpu-off", "compose-and-validate"):
        assert "sidecars" not in tpls[name] and "initContainers" not in tpls[name]


def test_should_use_the_interval_the_step_asked_for(tpls):
    side = {e["name"]: e for e in tpls["cpu-opt-in"]["sidecars"][0]["env"]}
    assert side["CKPT_EVERY_MIN"]["value"] == "{{inputs.parameters.checkpoint-every}}"   # the run form's value
    assert {"name": "checkpoint-every", "default": "20"} in tpls["cpu-opt-in"]["inputs"]["parameters"]
    assert {e["name"]: e for e in tpls["train"]["sidecars"][0]["env"]}["CKPT_EVERY_MIN"]["value"] == "10"


def test_should_key_the_copies_by_workflow_and_step_in_the_outputs_repository(tpls):
    env = {e["name"]: e for e in tpls["train"]["sidecars"][0]["env"]}
    assert env["CKPT_KEY"]["value"] == "{{workflow.name}}/train"
    assert env["CKPT_REPO"]["value"] == "outputs-PLACEHOLDER-app"
    assert env["CKPT_BRANCH"]["value"] == "kaos-checkpoints"
    assert env["LAKEFS_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"] == "PLACEHOLDER-lakefs-admin"


def test_should_lock_down_the_helper_and_give_it_small_requests(tpls):
    for c in tpls["train"]["initContainers"] + tpls["train"]["sidecars"]:
        assert c["securityContext"]["readOnlyRootFilesystem"] is True
        assert c["securityContext"]["allowPrivilegeEscalation"] is False
        assert "nvidia.com/gpu" not in str(c["resources"])
        assert c["resources"]["requests"] == {"cpu": "10m", "memory": "64Mi"}


def test_should_let_the_copy_sidecar_read_files_the_step_made_private(tpls):
    init, = tpls["train"]["initContainers"]
    side, = tpls["train"]["sidecars"]
    for c in (init, side):
        assert (c["securityContext"]["runAsUser"], c["securityContext"]["runAsGroup"]) == (0, 0)
    assert init["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert side["securityContext"]["capabilities"] == {"drop": ["ALL"], "add": ["DAC_OVERRIDE"]}


def test_should_not_embed_the_helper_source_in_any_step(tpls):
    """The helper ships once, as a ConfigMap: no step carries it (the render stays under the etcd limits)."""
    for name in ("train", "cpu-opt-in"):
        for c in tpls[name]["initContainers"] + tpls[name]["sidecars"]:
            assert c["command"] == ["python3", "/ckpt-helper/checkpoint_helper.py", c["command"][2]]
            assert c["command"][2] in ("restore", "watch")
            assert {"name": "kaos-ckpt-helper", "mountPath": "/ckpt-helper", "readOnly": True} in c["volumeMounts"]
        vol = next(v for v in tpls[name]["volumes"] if v["name"] == "kaos-ckpt-helper")
        assert vol["configMap"] == {"name": "PLACEHOLDER-app-ckpt-helper", "defaultMode": 0o444}
    assert "kaos-ckpt-helper" not in str(tpls["gpu-off"]) and "kaos-ckpt-helper" not in str(tpls["prep"])


def test_should_grow_by_a_small_constant_per_helper_step():
    """Regression for the 256 KiB last-applied limit: five GPU steps with helpers stay far below it."""
    def five():
        for i in range(5):
            authoring.step("s%d" % i, kind="evaluate", gpu=True)
    tpls = render(five)
    per_step = max(len(yaml.safe_dump(tpls["s%d" % i])) for i in range(5))
    assert per_step < 8000, "a helper step weighs %d bytes: is the helper source embedded again?" % per_step


def test_should_write_the_helper_configmap_for_the_gitops_commit(tmp_path):
    with authoring.pipeline("t") as p:
        authoring.step("train", kind="evaluate", gpu=True)
    wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), CONTEXT)
    out = tmp_path / "ckpt-helper.yaml"
    assert resilience.write_helper_configmap(wft, CONTEXT, out) is True
    cm = yaml.safe_load(out.read_text())
    assert cm["kind"] == "ConfigMap" and cm["metadata"]["name"] == "PLACEHOLDER-app-ckpt-helper"
    assert cm["metadata"]["namespace"] == CONTEXT["namespace"]
    assert cm["data"]["checkpoint_helper.py"] == resilience.HELPER_PATH.read_text()


def test_should_remove_a_stale_helper_configmap_when_no_step_uses_it(tmp_path):
    with authoring.pipeline("t") as p:
        authoring.step("prep", kind="evaluate")
    wft = enhance.enhance(yaml.safe_load(p.wt.to_yaml()), CONTEXT)
    out = tmp_path / "ckpt-helper.yaml"
    out.write_text("stale")
    assert resilience.write_helper_configmap(wft, CONTEXT, out) is False
    assert not out.exists()


def test_should_write_the_helper_configmap_from_the_cli(tmp_path):
    with authoring.pipeline("t") as p:
        authoring.step("train", kind="evaluate", gpu=True)
    raw, ctx = tmp_path / "raw.yaml", tmp_path / "ctx.yaml"
    raw.write_text(p.wt.to_yaml())
    ctx.write_text(yaml.safe_dump(CONTEXT))
    enhance.main(["--raw", str(raw), "--context", str(ctx), "--output", str(tmp_path / "wft.yaml"),
                  "--helper-configmap", str(tmp_path / "cm.yaml")])
    assert yaml.safe_load((tmp_path / "cm.yaml").read_text())["kind"] == "ConfigMap"


def test_should_take_the_helper_image_from_the_platform_registry_only():
    assert resilience.helper_image({}) == ""      # no public-registry fallback
    ctx = {"hpc": {"followerImage": "reg/kaos-hpc-operator:v1"}}
    assert resilience.helper_image(ctx) == "reg/kaos-hpc-operator:v1"
    ctx["checkpoints"] = {"helperImage": "reg/helper:v2"}
    assert resilience.helper_image(ctx) == "reg/helper:v2"


def test_should_run_the_helper_from_the_context_image(tpls):
    for c in tpls["train"]["initContainers"] + tpls["train"]["sidecars"]:
        assert c["image"] == "registry.example/kaos-hpc-operator:v0.0.0"


def test_should_keep_retry_and_folder_but_skip_copies_and_say_so_without_a_platform_image(capsys):
    ctx = copy.deepcopy(CONTEXT)
    ctx["checkpoints"].pop("helperImage")
    tpls_no_image = render(pipeline_steps, ctx)
    assert "sidecars" not in tpls_no_image["train"] and "initContainers" not in tpls_no_image["train"]
    assert tpls_no_image["train"]["retryStrategy"]["limit"] == "2"
    assert env_of(tpls_no_image["train"])["KAOS_CHECKPOINT_DIR"]
    assert "no checkpoints.helperImage" in capsys.readouterr().err


def test_should_refuse_a_fan_out_the_definition_declares_itself():
    """A pipeline that fans a task out with its own withItems is outside the contract: matrix= is the way."""
    with authoring.pipeline("t") as p:
        authoring.step("a", kind="evaluate", gpu=True)
    raw = yaml.safe_load(p.wt.to_yaml())
    main = next(t for t in raw["spec"]["templates"] if t["name"] == "main")
    next(t for t in main["dag"]["tasks"] if t["name"] == "a")["withItems"] = [1, 2]
    with pytest.raises(enhance.EnhanceError, match="task 'a' carries 'withItems'"):
        enhance.enhance(raw, CONTEXT)


def test_should_skip_the_helper_when_lakefs_injection_is_off():
    def build():
        authoring.step("a", kind="evaluate", gpu=True)
    with authoring.pipeline("t") as p:
        build()
    raw = yaml.safe_load(p.wt.to_yaml())
    tpl = next(t for t in raw["spec"]["templates"] if t["name"] == "a")
    tpl.setdefault("metadata", {}).setdefault("annotations", {})["platform.kubecore.io/inject"] = "lakefs=false"
    a = next(t for t in enhance.enhance(raw, CONTEXT)["spec"]["templates"] if t["name"] == "a")
    assert "sidecars" not in a


def test_should_refuse_the_developers_own_checkpoint_folder():
    with authoring.pipeline("t") as p:
        authoring.step("a", kind="evaluate", gpu=True)
    raw = yaml.safe_load(p.wt.to_yaml())
    tpl = next(t for t in raw["spec"]["templates"] if t["name"] == "a")
    tpl["container"]["env"] = [{"name": "KAOS_CHECKPOINT_DIR", "value": "/mine"}]
    with pytest.raises(enhance.EnhanceError, match="container.env"):
        enhance.enhance(raw, CONTEXT)


@pytest.mark.parametrize("bad", ["soon", 0, "0m", True, "10 min"])
def test_should_reject_a_bad_checkpoint_every(bad):
    with pytest.raises(authoring.AuthoringError):
        with authoring.pipeline("t"):
            authoring.step("a", kind="evaluate", gpu=True, checkpoint_every=bad)


@pytest.mark.parametrize("given,annotation", [("20m", "20"), ("1h", "60"), (30, "30"), (False, "off")])
def test_should_map_checkpoint_every_to_the_annotation(given, annotation):
    with authoring.pipeline("t") as p:
        authoring.step("a", kind="evaluate", checkpoint_every=given)
    tpl = next(t for t in yaml.safe_load(p.wt.to_yaml())["spec"]["templates"] if t["name"] == "a")
    assert tpl["metadata"]["annotations"]["platform.kubecore.io/checkpoint-every"] == annotation


def test_should_not_change_the_hpc_follower_template():
    ctx = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
    tpls = render(pipeline_steps, ctx)
    follower = tpls["hpc-follower"]
    assert follower["retryStrategy"]["limit"] == "2" and "sidecars" not in follower
    assert "KAOS_CHECKPOINT_DIR" not in str(follower["container"]["env"])


def test_should_render_the_gpu_step_like_the_golden_file(tpls):
    got = {k: copy.deepcopy(tpls["train"][k]) for k in INJECTED}
    got["checkpointEnv"] = [e for e in tpls["train"]["container"]["env"] if e["name"] == "KAOS_CHECKPOINT_DIR"]
    got["checkpointMount"] = [m for m in tpls["train"]["container"]["volumeMounts"] if m["name"] == "kaos-checkpoint"]
    text = yaml.safe_dump(got, sort_keys=False)
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.write_text(text)
    assert text == GOLDEN.read_text()


@pytest.mark.parametrize("given,annotation", [
    (None, None), (0, "0"), (False, "0"), ("0", "0"), (40, "40"), ("40m", "40"), ("2h", "120")])
def test_should_map_idle_timeout_to_the_annotation(given, annotation):
    def steps():
        authoring.step("a", kind="evaluate", gpu=True, idle_timeout=given)
    tpl = render(steps)["a"]
    got = (tpl.get("metadata") or {}).get("annotations", {}).get(authoring.IDLE_TIMEOUT_ANNOTATION)
    assert got == annotation


@pytest.mark.parametrize("bad", [True, -5, "soon", "1d", "20s", 1.5])
def test_should_reject_a_bad_idle_timeout(bad):
    with pytest.raises(authoring.AuthoringError, match="idle_timeout"):
        with authoring.pipeline("p"):
            authoring.step("a", kind="evaluate", gpu=True, idle_timeout=bad)
