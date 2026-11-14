"""Threat T-20: a pipeline definition may carry only the fields the step contract documents.

The render is untrusted. Whatever is not on the allow-list (kubecore/definition.py) is refused before the platform
adds its own wiring, with a sentence that names the step, the field and the fix. One refusal test per field; the
two templates' renders still enhance to exactly what they did before the rule (golden/templates/).
Run:  python -m pytest tests/template/test_definition_allowlist.py -v   (UPDATE_GOLDEN=1 rewrites the goldens)
"""
import copy
import os
import runpy
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]  # engine/
sys.path.insert(0, str(ROOT))

from kubecore import enhance  # noqa: E402

CTX = yaml.safe_load((ROOT / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
GOLDEN = ROOT / "tests" / "template" / "golden" / "templates"
SECRET_ENV = {"name": "STORE_KEY", "valueFrom": {"secretKeyRef": {"name": "another-app-store", "key": "key"}}}


def _raw():
    ns = runpy.run_path(str(ROOT / "tests" / "app" / "pipeline.py"), run_name="__pipeline__")
    return yaml.safe_load(ns["p"].wt.to_yaml())


def _step(wft):
    return next(t for t in wft["spec"]["templates"] if "container" in t and t["name"] != "compose-and-validate")


def _dag(wft):
    return next(t for t in wft["spec"]["templates"] if "dag" in t)


def _tpl(key, value):
    return lambda w: _step(w).__setitem__(key, value)


def _container(key, value):
    return lambda w: _step(w)["container"].__setitem__(key, value)


def _spec(key, value):
    return lambda w: w["spec"].__setitem__(key, value)


def _meta(kind, key, value):
    return lambda w: _step(w).setdefault("metadata", {}).setdefault(kind, {}).__setitem__(key, value)


def _task(key, value):
    return lambda w: _dag(w)["dag"]["tasks"][1].__setitem__(key, value)


def _env(entry):
    return lambda w: _step(w)["container"].setdefault("env", []).append(entry)


def _more(path, value):
    def apply(w):
        node = w
        for key in path[:-1]:
            node = node[key] if isinstance(node, dict) else node[key]
        node[path[-1]] = value
    return apply


SECRET_VOLUME = [{"name": "k", "secret": {"secretName": "another-app-store"}}]
CASES = [
    # (id, mutation, text the message must name: the step or the pipeline, then the field)
    ("container_volume_mounts", _container("volumeMounts", [{"name": "k", "mountPath": "/k"}]), "container.volumeMounts"),
    ("container_env_from", _container("envFrom", [{"secretRef": {"name": "another-app-store"}}]), "container.envFrom"),
    ("container_env_secret_key_ref", _env(SECRET_ENV), "valueFrom"),
    ("container_env_config_map_key_ref",
     _env({"name": "X", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}), "valueFrom"),
    ("container_env_field_ref", _env({"name": "X", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}), "valueFrom"),
    ("container_env_platform_name_clash", _env({"name": "LAKEFS_ACCESS_KEY", "value": "x"}), "container.env"),
    ("container_env_plain_variable", _env({"name": "OTHER", "value": "x"}), "container.env"),
    ("container_security_context", _container("securityContext", {"privileged": True}), "container.securityContext"),
    ("container_working_dir", _container("workingDir", "/"), "container.workingDir"),
    ("container_ports", _container("ports", [{"containerPort": 1}]), "container.ports"),
    ("container_lifecycle", _container("lifecycle", {"postStart": {}}), "container.lifecycle"),
    ("container_image_pull_policy", _container("imagePullPolicy", "Always"), "container.imagePullPolicy"),
    ("container_resources_claims", _container("resources", {"claims": [{"name": "x"}]}), "container.resources.claims"),
    ("container_resources_other_resource",
     _container("resources", {"limits": {"hugepages-2Mi": "1Gi"}}), "container.resources.limits.hugepages-2Mi"),
    ("template_volumes_secret", _tpl("volumes", SECRET_VOLUME), "volumes"),
    ("template_volumes_config_map", _tpl("volumes", [{"name": "k", "configMap": {"name": "c"}}]), "volumes"),
    ("template_volumes_host_path", _tpl("volumes", [{"name": "k", "hostPath": {"path": "/"}}]), "volumes"),
    ("template_volumes_projected_token",
     _tpl("volumes", [{"name": "k", "projected": {"sources": [{"serviceAccountToken": {"path": "t"}}]}}]), "volumes"),
    ("template_host_network", _tpl("hostNetwork", True), "hostNetwork"),
    ("template_host_pid", _tpl("hostPID", True), "hostPID"),
    ("template_host_ipc", _tpl("hostIPC", True), "hostIPC"),
    ("template_host_aliases", _tpl("hostAliases", [{"ip": "1.1.1.1"}]), "hostAliases"),
    ("template_pod_security_context", _tpl("securityContext", {"runAsUser": 0}), "securityContext"),
    ("template_node_selector", _tpl("nodeSelector", {"platform.kubecore.io/project": "other"}), "nodeSelector"),
    ("template_tolerations", _tpl("tolerations", [{"operator": "Exists"}]), "tolerations"),
    ("template_affinity", _tpl("affinity", {"podAffinity": {}}), "affinity"),
    ("template_scheduler_name", _tpl("schedulerName", "x"), "schedulerName"),
    ("template_priority_class_name", _tpl("priorityClassName", "system-node-critical"), "priorityClassName"),
    ("template_init_containers", _tpl("initContainers", [{"name": "i", "image": "x"}]), "initContainers"),
    ("template_sidecars", _tpl("sidecars", [{"name": "s", "image": "x"}]), "sidecars"),
    ("template_image_pull_secrets", _tpl("imagePullSecrets", [{"name": "s"}]), "imagePullSecrets"),
    ("template_automount_service_account_token", _tpl("automountServiceAccountToken", True), "automountServiceAccountToken"),
    ("template_retry_strategy", _tpl("retryStrategy", {"limit": 99}), "retryStrategy"),
    ("template_active_deadline", _tpl("activeDeadlineSeconds", 1), "activeDeadlineSeconds"),
    ("template_timeout", _tpl("timeout", "1h"), "timeout"),
    ("template_synchronization", _tpl("synchronization", {"mutex": {"name": "m"}}), "synchronization"),
    ("template_script_instead_of_container", lambda w: _step(w).update(script={"image": "x", "source": "x"}), "script"),
    ("template_resource_template", lambda w: w["spec"]["templates"].append(
        {"name": "r", "resource": {"action": "create", "manifest": "x"}}), "resource"),
    ("template_two_kinds", lambda w: _step(w).update(dag={"tasks": []}), "template type"),
    ("template_annotation_workload_identity", _meta("annotations", "iam.gke.io/gcp-service-account", "x"),
     "metadata.annotations.iam.gke.io/gcp-service-account"),
    ("template_annotation_sidecar_injection", _meta("annotations", "gke-gcsfuse/volumes", "true"),
     "metadata.annotations.gke-gcsfuse/volumes"),
    ("template_annotation_image", _meta("annotations", "platform.kubecore.io/image", "busybox"),
     "metadata.annotations.platform.kubecore.io/image"),
    ("template_label_foreign", _meta("labels", "sidecar.istio.io/inject", "true"), "metadata.labels.sidecar.istio.io/inject"),
    ("template_inputs_artifacts", lambda w: _step(w).__setitem__("inputs", {"parameters": [{"name": "kaos-context"}],
        "artifacts": [{"name": "a", "path": "/a", "s3": {"secretKeySelector": {"name": "s"}}}]}), "artifacts"),
    ("template_inputs_parameter_config_map",
     lambda w: _step(w)["inputs"]["parameters"].append({"name": "p", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}),
     "valueFrom"),
    ("template_outputs_artifacts", lambda w: _step(w)["outputs"].__setitem__(
        "artifacts", [{"name": "a", "path": "/a", "gcs": {"serviceAccountKeySecret": {"name": "s"}}}]), "artifacts"),
    ("template_outputs_parameter_config_map", lambda w: _step(w)["outputs"]["parameters"].append(
        {"name": "p", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}), "valueFrom.configMapKeyRef"),
    ("task_template_ref", _task("templateRef", {"name": "other-app-pipeline", "template": "x"}), "templateRef"),
    ("task_inline_template", _task("inline", {"container": {"image": "x"}}), "inline"),
    ("task_arguments_artifacts", lambda w: _dag(w)["dag"]["tasks"][1]["arguments"].__setitem__(
        "artifacts", [{"name": "a", "s3": {}}]), "arguments.artifacts"),
    ("task_argument_config_map", lambda w: _dag(w)["dag"]["tasks"][1]["arguments"]["parameters"].append(
        {"name": "p", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}), "valueFrom"),
    ("task_with_items", _task("withItems", [1]), "withItems"),
    ("dag_fail_fast", lambda w: _dag(w)["dag"].__setitem__("failFast", False), "dag.failFast"),
    ("spec_volumes", _spec("volumes", SECRET_VOLUME), "spec.volumes"),
    ("spec_volume_claim_templates", _spec("volumeClaimTemplates", [{"metadata": {"name": "v"}}]), "spec.volumeClaimTemplates"),
    ("spec_pod_metadata", _spec("podMetadata", {"annotations": {"a": "b"}}), "spec.podMetadata"),
    ("spec_workflow_metadata", _spec("workflowMetadata", {"labels": {"a": "b"}}), "spec.workflowMetadata"),
    ("spec_on_exit", _spec("onExit", "x"), "spec.onExit"),
    ("spec_hooks", _spec("hooks", {"x": {}}), "spec.hooks"),
    ("spec_image_pull_secrets", _spec("imagePullSecrets", [{"name": "s"}]), "spec.imagePullSecrets"),
    ("spec_security_context", _spec("securityContext", {"runAsUser": 0}), "spec.securityContext"),
    ("spec_node_selector", _spec("nodeSelector", {"a": "b"}), "spec.nodeSelector"),
    ("spec_tolerations", _spec("tolerations", [{"operator": "Exists"}]), "spec.tolerations"),
    ("spec_automount_token", _spec("automountServiceAccountToken", True), "spec.automountServiceAccountToken"),
    ("spec_arguments_artifacts", lambda w: w["spec"]["arguments"].__setitem__("artifacts", [{"name": "a"}]), "artifacts"),
    ("spec_arguments_parameter_config_map", lambda w: w["spec"]["arguments"]["parameters"].append(
        {"name": "p", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}), "valueFrom"),
    ("pipeline_metadata_label", lambda w: w["metadata"].__setitem__("labels", {"a": "b"}), "metadata.labels.a"),
    ("pipeline_metadata_annotation", lambda w: w["metadata"].__setitem__("annotations", {"a": "b"}), "metadata.annotations.a"),
    ("pipeline_metadata_owner_references", lambda w: w["metadata"].__setitem__("ownerReferences", []), "metadata.ownerReferences"),
    ("pipeline_top_level_status", lambda w: w.__setitem__("status", {}), "status"),
]


@pytest.mark.parametrize("mutate,field", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_refuses_a_field_outside_the_contract(mutate, field):
    wft = _raw()
    mutate(wft)
    with pytest.raises(enhance.EnhanceError) as e:
        enhance.enhance(wft, copy.deepcopy(CTX))
    message = str(e.value)
    assert f"'{field}'" in message, message
    assert "Remove it from your pipeline" in message, message
    assert ("step '" in message) or ("the pipeline" in message), message
    assert message.count(".") >= 2, "a fix is given: " + message


def test_a_refusal_names_the_step_that_carries_the_field():
    wft = _raw()
    _tpl("volumes", SECRET_VOLUME)(wft)
    with pytest.raises(enhance.EnhanceError, match=f"step '{_step(wft)['name']}' carries 'volumes'"):
        enhance.enhance(wft, copy.deepcopy(CTX))


def test_the_authoring_output_passes_untouched():
    enhance.enhance(_raw(), copy.deepcopy(CTX))


def test_a_pinned_class_the_contract_documents_stays_allowed():
    wft = _raw()
    _meta("annotations", "platform.kubecore.io/compute-class", CTX["computeClasses"]["all"][0]["name"]
          if isinstance(CTX["computeClasses"]["all"][0], dict) else CTX["computeClasses"]["all"][0])(wft)
    enhance.enhance(wft, copy.deepcopy(CTX))


def test_the_steps_of_a_template_cannot_call_another_template_by_reference():
    wft = _raw()
    wft["spec"]["templates"].append({"name": "fan", "steps": [[{"name": "run", "templateRef": {"name": "x", "template": "y"}}]]})
    with pytest.raises(enhance.EnhanceError, match="templateRef"):
        enhance.enhance(wft, copy.deepcopy(CTX))


@pytest.mark.parametrize("name", ["hera", "yolo"])
def test_a_template_renders_to_what_it_rendered_before_the_rule(name):
    """The raw render of each template (branch dev) is kept in golden/templates/; its enhanced form is byte-identical to the
    one the engine produced before the allow-list existed."""
    raw = yaml.safe_load((GOLDEN / f"{name}-raw.yaml").read_text())
    got = yaml.safe_dump(enhance.enhance(raw, copy.deepcopy(CTX)), sort_keys=True)
    golden = GOLDEN / f"{name}-enhanced.yaml"
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_text(got)
    assert got == golden.read_text()
