"""Settings (what the controller sets), identity, the Slurm readings, sharing, the wallet signature."""
import base64
import io
import json
import os
import re
import shutil
import subprocess
import urllib.error
from pathlib import Path

import helpers
import pytest

from kaos_hpc_runner import settings, slurm, wallet
from kaos_hpc_runner.settings import Settings

PKG = Path(__file__).resolve().parents[1] / "kaos_hpc_runner"
LEASE_RE = re.compile(r'kaos-commit=([0-9a-f]{6,64})')


# ---------------------------------------------------------------- identity and the controller's environment

def test_the_runner_has_no_identity_literals():
    for f in PKG.rglob("*.py"):
        text = f.read_text()
        for literal in ("u104378", "p201342", "/home/users/u", "/project/scratch/p"):
            assert literal not in text, f"{f.name}: identity literal {literal!r} must come from the HPCSite"


def test_identity_comes_from_the_site_and_is_never_defaulted(capsys):
    assert helpers.settings().identity() == {"user": "u1", "account": "p1", "scratch": "/scr", "home": "/home/u1"}
    for missing in ("HPC_USER", "HPC_ACCOUNT", "HPC_SCRATCH", "HPC_HOME"):
        cfg = helpers.settings(**{missing: None})
        with pytest.raises(SystemExit) as e:
            cfg.identity()
        assert e.value.code == 2
        assert "HPC identity missing" in capsys.readouterr().out


def test_an_unknown_class_is_a_mismatch_not_a_default(capsys):
    assert helpers.settings().hpc_class()["partition"] == "gpu"
    with pytest.raises(SystemExit) as e:
        helpers.settings(HPC_CLASS="nope").hpc_class()
    assert e.value.code == 2 and "unknown HPC class 'nope'" in capsys.readouterr().out


def test_the_class_cap_is_read_from_either_spelling():
    assert Settings.class_cap({"maxTimeLimitMinutes": 300}) == 300
    assert Settings.class_cap({"maxWallTimeMinutes": 120}) == 120
    assert Settings.class_cap({}) == 0


def test_the_job_to_report_to_is_required(capsys):
    with pytest.raises(SystemExit):
        helpers.settings(HPC_JOB_NAME=None)
    assert "HPC_JOB_NAME" in capsys.readouterr().out


def test_the_image_digest_form_is_normalised_and_the_command_quoted():
    cfg = helpers.settings(IMAGE_REF="zot.example/p/app:v1@sha256:" + "a" * 64,
                           STEP_COMMAND=json.dumps(["python", "-c", "import torch; print('x y')"]))
    assert cfg.image == "zot.example/p/app@sha256:" + "a" * 64
    assert cfg.command == "python -c 'import torch; print('\"'\"'x y'\"'\"')'"


def test_the_site_endpoint_and_login_node_come_from_the_site_with_meluxina_defaults():
    cfg = helpers.settings()
    assert cfg.api == "https://slurm-api.lxp.lu/slurm/v0.0.44" and cfg.sdb.endswith("/slurmdb/v0.0.44")
    assert (cfg.login_host, cfg.login_port) == ("login.lxp.lu", "8822")
    cfg = helpers.settings(HPC_ENDPOINT="https://slurm.example.org/", HPC_REST_VERSION="v0.0.45",
                           HPC_LOGIN_HOST="login.example.org:2200")
    assert cfg.api == "https://slurm.example.org/slurm/v0.0.45"
    assert (cfg.login_host, cfg.login_port) == ("login.example.org", "2200")


def test_the_resume_point_is_a_commit_with_its_prefix_or_the_checkpoint_uri():
    cid, path = "ab" * 32, "hpc-checkpoints/wf/model-training/attempt-1/"
    assert helpers.settings().resume is None
    assert helpers.settings(KAOS_RESUME_REF=cid, KAOS_RESUME_PREFIX=path).resume == (cid, path)
    assert helpers.settings(KAOS_RESUME_REF="lakefs://outputs-yolo/%s/%s" % (cid, path)).resume == (cid, path)
    # the controller's form: a commit and the attempt that saved it
    assert helpers.settings(KAOS_ATTEMPT="3", KAOS_RESUME_REF=cid, KAOS_RESUME_FROM_ATTEMPT="2").resume == \
        (cid, "hpc-checkpoints/0b7f6c1e-1111-2222-3333-444455556666/model-training/attempt-2/")
    with pytest.raises(SystemExit):
        helpers.settings(KAOS_RESUME_REF="not-a-commit")


def test_the_lead_and_attempt_come_from_the_controller():
    cfg = helpers.settings()
    assert cfg.resumable_lead == 0 and cfg.attempt == 1
    cfg = helpers.settings(HPC_RESUMABLE_LEAD="20", KAOS_ATTEMPT="3")
    assert cfg.resumable_lead == 20 and cfg.attempt == 3


# ---------------------------------------------------------------- node sharing switches

def test_sharing_stays_off_without_a_valid_scope(capsys):
    assert helpers.settings().sharing is False
    assert helpers.settings(HPC_SHARING="on").sharing is False
    assert helpers.settings(HPC_SHARING="on", HPC_SCOPE="Bad Scope!").sharing is False
    assert "running alone" in capsys.readouterr().out
    assert helpers.settings(HPC_SHARING="off", HPC_SCOPE="a1b2c3d4e5").sharing is False
    assert helpers.settings(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5").sharing is True
    assert helpers.settings(HPC_SHARING="1", HPC_SCOPE="a1b2c3d4e5").sharing is True    # the controller's spelling
    assert helpers.settings(HPC_SHARING="0", HPC_SCOPE="a1b2c3d4e5").sharing is False


def test_a_resumable_step_never_shares_a_node(capsys):
    """A host's slot manager does not pass the signal on (#1609)."""
    assert helpers.settings(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", HPC_RESUMABLE_LEAD="10").sharing is False
    assert "it is resumable" in capsys.readouterr().out


def test_the_job_name_carries_the_scope_only_with_sharing_on():
    wf, step = "0b7f6c1e-1111-2222-3333-444455556666", "model-training"
    assert helpers.settings().job_site_name == "kaos-%s-%s" % (wf, step)
    assert helpers.settings(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5").job_site_name == \
        "kaos-a1b2c3d4e5-%s-%s" % (wf, step)


# ---------------------------------------------------------------- dataset

def test_dataset_coordinates_and_the_upstream_pin():
    assert helpers.settings().dataset_coords() == ("dataset-default", "main")
    assert helpers.settings(DATASET="dataset-cars@v3").dataset_coords() == ("dataset-cars", "v3")
    assert helpers.settings(DATASET=None).dataset_coords() == ("", "")
    with pytest.raises(SystemExit):
        helpers.settings(DATASET="cars@latest").dataset_coords()
    summary = json.dumps({"dataset_commit": "c0ffee12"})
    assert helpers.settings(STEP_COMMAND=json.dumps(["x", "--info", summary])).pinned_commit() == "c0ffee12"
    assert helpers.settings().pinned_commit() == ""


# ---------------------------------------------------------------- Slurm readings

def _error(code, body: bytes):
    return urllib.error.HTTPError("https://slurm-api.lxp.lu/slurm/v0.0.44/job/submit", code,
                                  "INTERNAL ERROR", {}, io.BytesIO(body))


def test_the_refusal_names_slurms_reasons():
    body = {"errors": [{"description": "Invalid qos specification", "error_number": 2066}],
            "warnings": [], "job_id": None}
    out = slurm.submit_refusal(_error(500, json.dumps(body).encode()))
    assert out.startswith("HTTP 500 ") and "Invalid qos specification" in out and "2066" in out
    assert "warnings" not in out   # empty fields are left out
    assert slurm.submit_refusal(_error(502, b"<html>bad gateway</html>")) == "HTTP 502 (no reason in the body)"


def test_the_token_is_read_fresh_on_every_request(tmp_path, monkeypatch):
    f = tmp_path / "token"
    f.write_text("one\n")
    s = slurm.Slurm(helpers.settings(), token_file=str(f))
    assert s.tok() == "one"
    f.write_text("two\n")
    assert s.tok() == "two", "the mounted Secret rotates; an env var would not"
    f.unlink()
    monkeypatch.setenv("SLURM_TOKEN", " env-tok ")
    assert s.tok() == "env-tok"
    assert s.hdrs()["X-SLURM-USER-NAME"] == "u1"


def test_a_hosts_comment_is_read_as_state():
    share, comment_of = slurm.share_state, slurm.comment_of
    assert share("kaos-commit=c0ffee12") == {}
    assert share("err231:pull failed|") == {}
    assert share("kaos-commit=c0ffee12 kaos-image=0123456789abcdef kaos-state=taken host=5417591 slot=2 attempt=1") == \
        {"kaos-state": "taken", "host": "5417591", "slot": "2", "attempt": "1"}
    assert share("kaos-state=done rc=0 host=5417591") == {"kaos-state": "done", "rc": "0", "host": "5417591"}
    assert comment_of({"comment": {"job": "kaos-state=done rc=0", "administrator": ""}}) == "kaos-state=done rc=0"
    assert comment_of({"comment": "kaos-state=taken"}) == "kaos-state=taken"
    assert comment_of({}) == ""
    for c in ("kaos-commit=c0ffee12 kaos-state=taken host=1 slot=0 attempt=1",
              "kaos-commit=c0ffee12 kaos-state=done rc=0"):
        assert LEASE_RE.findall(c) == ["c0ffee12"], "every host write keeps the dataset lease"


def test_site_times_are_utc():
    assert slurm.site_time({"time": {"start": {"set": True, "number": 1762300800}}}, "start") == "2025-11-05T00:00:00Z"
    assert slurm.site_time({"time": {"end": 0}}, "end") is None


class _Acc:
    def __init__(self, records):
        self.records = records

    def accounting(self, jid):
        rec = self.records[jid]
        if isinstance(rec, Exception):
            raise rec
        return {"jobs": [{"state": {"current": rec}}]}["jobs"][0]


def test_a_host_is_gone_only_when_accounting_says_it_ended():
    acc = _Acc({"1": ["RUNNING"], "2": ["COMPLETING"], "3": ["NODE_FAIL"], "4": ["CANCELLED"],
                "5": ["COMPLETED"], "6": [], "7": OSError("REST down"), "8": ["PENDING"]})
    gone = lambda h: slurm.host_gone(acc, h)   # noqa: E731
    assert not gone("1") and not gone("2") and not gone("8"), "a live host is never given up on"
    assert gone("3") and gone("4") and gone("5"), "a host that ended without handing the job back is gone"
    assert not gone("6") and not gone("7"), "an unreadable or empty record is not a death"


# ---------------------------------------------------------------- the wallet's RS256 without PyJWT

@pytest.mark.skipif(shutil.which("openssl") is None, reason="needs the openssl CLI to verify")
@pytest.mark.parametrize("traditional", [True, False])
def test_rs256_verifies_with_openssl_for_pkcs1_and_pkcs8_keys(tmp_path, traditional):
    run = lambda *a: subprocess.run(a, check=True, capture_output=True)   # noqa: E731
    run("openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(tmp_path / "k8"))
    key = tmp_path / "k8"
    if traditional:
        run("openssl", "rsa", "-in", str(key), "-traditional", "-out", str(tmp_path / "k1"))
        key = tmp_path / "k1"
    pem = key.read_text()
    assert pem.startswith("-----BEGIN RSA PRIVATE KEY" if traditional else "-----BEGIN PRIVATE KEY")
    run("openssl", "rsa", "-in", str(key), "-pubout", "-out", str(tmp_path / "pub"))
    tok = wallet.jwt_rs256({"iss": "u", "aud": "https://z"}, pem, "kid1")
    head, body, sig = tok.split(".")
    assert json.loads(base64.urlsafe_b64decode(head + "==")) == {"alg": "RS256", "typ": "JWT", "kid": "kid1"}
    (tmp_path / "msg").write_text(head + "." + body)
    (tmp_path / "sig").write_bytes(base64.urlsafe_b64decode(sig + "=="))
    r = subprocess.run(["openssl", "dgst", "-sha256", "-verify", str(tmp_path / "pub"), "-signature",
                        str(tmp_path / "sig"), str(tmp_path / "msg")], capture_output=True, text=True)
    assert "Verified OK" in r.stdout, r.stdout + r.stderr


def test_the_wallet_is_minted_from_the_machine_key_with_the_project_audience(tmp_path, monkeypatch):
    if shutil.which("openssl") is None:
        pytest.skip("needs openssl to make a key")
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out",
                    str(tmp_path / "k")], check=True, capture_output=True)
    keyfile = tmp_path / "ZITADEL_MACHINE_KEY"
    keyfile.write_text(json.dumps({"userId": "u-1", "keyId": "k-1", "key": (tmp_path / "k").read_text()}))
    seen = {}

    def urlopen(req, timeout=0):
        seen["url"], seen["data"] = req.full_url, dict(x.split("=", 1) for x in req.data.decode().split("&"))
        return io.BytesIO(json.dumps({"access_token": "wallet.jwt"}).encode())
    monkeypatch.setattr(wallet.urllib.request, "urlopen", urlopen)
    env = {"ZITADEL_DOMAIN": "zitadel.example", "ZITADEL_MLFLOW_PROJECT_ID": "123"}
    assert wallet.mint_wallet(env, keyfile=str(keyfile)) == "wallet.jwt"
    assert seen["url"] == "https://zitadel.example/oauth/v2/token"
    assert seen["data"]["grant_type"].endswith("jwt-bearer") and "project%3Aid%3A123%3Aaud" in seen["data"]["scope"]
    claims = json.loads(base64.urlsafe_b64decode(seen["data"]["assertion"].split(".")[1] + "=="))
    assert claims["iss"] == claims["sub"] == "u-1" and claims["aud"] == "https://zitadel.example"


def test_without_a_machine_key_or_domain_the_job_runs_without_a_wallet(tmp_path, capsys):
    assert wallet.mint_wallet({"ZITADEL_DOMAIN": "z"}, keyfile=str(tmp_path / "absent")) == ""
    assert "data-plane env omitted" in capsys.readouterr().out
    assert wallet.mint_wallet({}, keyfile=str(tmp_path / "absent")) == ""


def test_a_broken_key_degrades_to_no_wallet(tmp_path, capsys):
    f = tmp_path / "key"
    f.write_text(json.dumps({"userId": "u", "keyId": "k", "key": "not a pem"}))
    assert wallet.mint_wallet({"ZITADEL_DOMAIN": "z"}, keyfile=str(f)) == ""
    assert "mint failed" in capsys.readouterr().out


def test_wallet_expiry_is_read_from_the_jwt():
    exp = base64.urlsafe_b64encode(json.dumps({"exp": 1762300800}).encode()).decode().rstrip("=")
    assert wallet.wallet_exp("h.%s.s" % exp) == 1762300800.0
    assert wallet.wallet_exp("garbage") == 0.0


# ---------------------------------------------------------------- the entry point

def test_the_entry_point_is_python_dash_m_with_the_runner_dir_on_the_path(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HPC_", "KAOS_"))}
    env["PYTHONPATH"] = str(PKG.parent)
    r = subprocess.run(["python3", "-m", "kaos_hpc_runner"], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 2 and "HPC_JOB_NAMESPACE / HPC_JOB_NAME missing" in r.stdout


def test_the_package_is_stdlib_only():
    import ast
    import sys
    allowed = set(sys.stdlib_module_names) | {"kaos_hpc_runner"}
    for f in PKG.glob("*.py"):
        for node in ast.walk(ast.parse(f.read_text())):
            mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                    else [node.module] if isinstance(node, ast.ImportFrom) and node.level == 0 else [])
            for m in mods:
                assert m.split(".")[0] in allowed, "%s imports %s" % (f.name, m)


def test_nothing_is_installed_at_run_time():
    for f in PKG.glob("*.py"):
        text = f.read_text()
        assert "'pip'" not in text and '"pip"' not in text and "apk add" not in text, f.name


def test_main_py_is_the_entrypoint_the_job_runs(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HPC_", "KAOS_", "PYTHONPATH"))}
    r = subprocess.run(["python3", "-u", str(PKG.parent / "main.py")], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 2 and "HPC_JOB_NAMESPACE" in r.stdout
