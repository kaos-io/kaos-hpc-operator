"""A step signs in to MLflow as its own app when the context says so (PRD kaos#1911 AC-43)."""
import copy
import runpy

import yaml

from kubecore import enhance
from paths import APP, ENGINE

CTX = yaml.safe_load((ENGINE / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())
OIDC = {"oidcDomain": "id.example", "oidcProjectId": "321", "svcSecret": "proj-mlflow-svc"}


def _steps(mlflow=None, identity=None):
    ctx = copy.deepcopy(CTX)
    ctx["mlflow"] = {**ctx["mlflow"], **OIDC, **(mlflow or {})}
    if identity is not None:
        ctx["workloadIdentity"] = identity
    ns = runpy.run_path(str(APP / "pipeline.py"), run_name="__pipeline__")
    wft = enhance.enhance(yaml.safe_load(ns["p"].wt.to_yaml()), ctx)
    return [t for t in enhance.step_templates(wft["spec"]) if "MLFLOW_TRACKING_URI" in _env(t)]


def _env(step):
    return {e["name"]: e.get("value") for e in step["container"].get("env", [])}


def _volume(step, name):
    return next((v for v in step.get("volumes", []) if v["name"] == name), None)


def _mounts(step):
    return {m["name"]: m["mountPath"] for m in step["container"].get("volumeMounts", [])}


def test_should_give_a_step_the_token_of_its_own_app_and_not_the_project_key():
    steps = _steps({"appIdentity": True}, {"audience": "kaos-lake"})
    assert steps
    for step in steps:
        env = _env(step)
        assert env["KAOS_APP_TOKEN_FILE"] == "/var/run/secrets/kaos-lake/token"
        assert env["MLFLOW_TRACKING_AUTH"] == "zitadel"
        assert not [name for name in env if name.startswith("ZITADEL_")]
        assert _volume(step, "mlflow-svc") is None and "mlflow-svc" not in _mounts(step)


def test_should_keep_the_project_key_when_the_switch_is_off():
    for step in _steps(None, {"audience": "kaos-lake"}):
        env = _env(step)
        assert "KAOS_APP_TOKEN_FILE" not in env
        assert env["ZITADEL_MACHINE_KEY_FILE"] == "/etc/mlflow-svc/ZITADEL_MACHINE_KEY"
        assert _volume(step, "mlflow-svc") and _mounts(step)["mlflow-svc"] == "/etc/mlflow-svc"


def test_should_keep_the_project_key_for_an_app_without_its_own_account():
    for identity in (None, {}, {"audience": " "}):
        for step in _steps({"appIdentity": True}, identity):
            assert "KAOS_APP_TOKEN_FILE" not in _env(step)
            assert _volume(step, "mlflow-svc") and _mounts(step)["mlflow-svc"] == "/etc/mlflow-svc"
