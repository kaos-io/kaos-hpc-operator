"""Shared test setup: a runner environment as the controller would set it."""
import json

from kaos_hpc_runner.settings import Settings

CLASSES = [{"name": "meluxina-gpu", "partition": "gpu", "qos": "default", "gpus": 4, "maxTimeLimitMinutes": 2880},
           {"name": "meluxina-cpu", "partition": "cpu", "gpus": 0}]

BASE_ENV = {
    "HPC_JOB_NAMESPACE": "ml-proj", "HPC_JOB_NAME": "hpc-0123456789abcdef0123", "KAOS_ATTEMPT": "1",
    "WF_UID": "0b7f6c1e-1111-2222-3333-444455556666", "STEP_NAME": "model-training",
    "KUBECORE_WORKFLOW_NAME": "yolo-pipeline-x7k2p", "IMAGE_REF": "zot.example/proj/yolo:v0.0.0",
    "STEP_COMMAND": json.dumps(["python", "-m", "train", "--epochs", "3"]),
    "SLURM_TIME_LIMIT": "720", "HPC_CLASS": "meluxina-gpu", "HPC_CLASSES_JSON": json.dumps(CLASSES),
    "HPC_USER": "u1", "HPC_ACCOUNT": "p1", "HPC_SCRATCH": "/scr", "HPC_HOME": "/home/u1",
    "STEP_OUTPUTS": "training-result", "DATASET": "dataset-default@latest", "OUTPUTS_REPO": "outputs-yolo",
    "LAKEFS_EXTERNAL_URL": "https://lakefs.example", "MLFLOW_EXTERNAL_URL": "https://mlflow.example",
}


def settings(**over):
    env = dict(BASE_ENV)
    env.update({k: v for k, v in over.items() if v is not None})
    for k in [k for k, v in over.items() if v is None]:
        env.pop(k, None)
    return Settings(env)


# ---------------------------------------------------------------- a Kubernetes API with one HPCArgoWorkflowJob

import copy  # noqa: E402
import io  # noqa: E402
import urllib.error  # noqa: E402


def merge_patch(target, patch):
    """RFC 7386."""
    if not isinstance(patch, dict):
        return patch
    target = dict(target) if isinstance(target, dict) else {}
    for k, v in patch.items():
        if v is None:
            target.pop(k, None)
        else:
            target[k] = merge_patch(target.get(k), v)
    return target


class FakeKube:
    """Answers GET of the job and merge-patches of its status subresource, like the API server."""

    def __init__(self, cfg=None, status=None, exists=True):
        ns, name = (cfg.namespace, cfg.job_name) if cfg else ("ml-proj", "hpc-0123456789abcdef0123")
        self.path = "/apis/hpc.kaos.io/v1alpha1/namespaces/%s/hpcargoworkflowjobs/%s" % (ns, name)
        self.job = {"metadata": {"name": name, "namespace": ns}, "status": status or {}} if exists else None
        self.calls, self.fail = [], []     # fail: HTTP codes / exceptions answered to the next calls

    def __call__(self, method, path, body=None, ctype="application/json", timeout=30):
        self.calls.append((method, path, body, ctype))
        if self.fail:
            f = self.fail.pop(0)
            if isinstance(f, Exception):
                raise f
            raise urllib.error.HTTPError(path, f, "x", {}, io.BytesIO(b"{}"))
        if self.job is None:
            raise urllib.error.HTTPError(path, 404, "gone", {}, io.BytesIO(b"{}"))
        if method == "GET":
            assert path == self.path
            return copy.deepcopy(self.job)
        if method == "PATCH":
            assert path == self.path + "/status", path
            assert ctype == "application/merge-patch+json"
            self.job["status"] = merge_patch(self.job["status"], body["status"])
            return copy.deepcopy(self.job)
        raise AssertionError(method)

    @property
    def status(self):
        return self.job["status"]

    @property
    def run(self):
        return self.job["status"].get("run") or {}

    def patches(self):
        return [c for c in self.calls if c[0] == "PATCH"]
