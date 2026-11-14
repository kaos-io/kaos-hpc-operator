"""Shared test setup: a runner environment as the controller would set it."""
import json
import pathlib

from kaos_hpc_runner.settings import Settings

CLASSES = [{"name": "meluxina-gpu", "partition": "gpu", "qos": "default", "gpus": 4, "maxTimeLimitMinutes": 2880},
           {"name": "meluxina-cpu", "partition": "cpu", "gpus": 0}]

# The step contract's fixtures (docs/step-contract/fixtures): loaded, never copied (PRD kaos#1911 CON-01).
FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "docs" / "step-contract" / "fixtures"


def fixture(name):
    return (FIXTURES / name).read_text()


COMMIT = "c0ffee1200000000000000000000000000000000000000000000000000000000"
INPUTS = [{"name": "dataset", "repo": "dataset-default", "commit": COMMIT, "ref": "latest"}]

BASE_ENV = {
    "HPC_JOB_NAMESPACE": "ml-proj", "HPC_JOB_NAME": "hpc-0123456789abcdef0123", "KAOS_ATTEMPT": "1",
    "WF_UID": "0b7f6c1e-1111-2222-3333-444455556666", "STEP_NAME": "model-training",
    "WF_NAME": "yolo-pipeline-x7k2p", "IMAGE_REF": "zot.example/proj/yolo:v0.0.0",
    "STEP_COMMAND": json.dumps(["python", "-m", "app.entry"]),
    "SLURM_TIME_LIMIT": "720", "HPC_CLASS": "meluxina-gpu", "HPC_CLASSES_JSON": json.dumps(CLASSES),
    "HPC_USER": "u1", "HPC_ACCOUNT": "p1", "HPC_SCRATCH": "/scr", "HPC_HOME": "/home/u1",
    "KAOS_APP": "yolo", "KAOS_STEP_CONTEXT": fixture("context-one-upstream.wire"), "HPC_INPUTS": json.dumps(INPUTS),
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


# ---------------------------------------------------------------- the app's key in the data store (AC-14)

APP_KEY_ID, APP_KEY_SECRET = "KAOSXAPPKEY1", "app-secret-1"


class FakeKeys:
    """What identity.app_key gives the runner: one key pair of the app per call, a counter in its secret.
    expires: seconds since the epoch; fail: exceptions or None answered to the next calls."""

    def __init__(self, expires=1e12, fail=None):
        self.calls, self.expires, self.fail = 0, expires, list(fail or [])

    @staticmethod
    def auth_of(n):
        """The Authorization value of the key the n-th call gave."""
        from kaos_hpc_runner import identity
        return identity.Key(APP_KEY_ID, APP_KEY_SECRET + str(n), 1e12, "").auth()

    def __call__(self):
        from kaos_hpc_runner import identity
        self.calls += 1
        if self.fail:
            f = self.fail.pop(0)
            if f:
                raise f
        return identity.Key(APP_KEY_ID, APP_KEY_SECRET + str(self.calls), self.expires, "2099-01-01T00:00:00Z")


def app_key():
    """A fixed key of the app, for the calls that take a key getter."""
    from kaos_hpc_runner import identity
    return identity.Key(APP_KEY_ID, APP_KEY_SECRET, 1e12, "2099-01-01T00:00:00Z")


def app_auth():
    return app_key().auth()


def write_key_file(folder, key=None):
    """The key file as the site's staging tool places it: a folder 0700, the file 0600, the record as JSON."""
    import os
    folder = str(folder)
    os.makedirs(folder, mode=0o700, exist_ok=True)
    path = os.path.join(folder, "lakefs-key.json")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump((key or app_key()).record(), f)
    return path
