"""PRD kaos#1911 AC-14: a job reads and writes the data store as its APP.

A. the runner gets a token of the app's workflow service account and exchanges it for the app's key pair;
B. the step gets the key through a file (0600, replaced before it ends), never through the job's environment, script or log;
C. the tracker's token comes from the project's second machine user, with a one-line fallback;
D. an app without an identity is refused before anything is staged, with a reason that names the app.
"""
import io
import json
import os
import stat
import urllib.error

import helpers
import pytest
from test_runner import FakeSlurm, acc, rig  # noqa: F401  (rig is a fixture)

from kaos_hpc_runner import identity, jobscript, payloads, wallet
from kaos_hpc_runner import runner as runmod

OWN = "https://lakefs.example"


def http_error(code):
    return urllib.error.HTTPError("x", code, "no", {}, io.BytesIO(b"{}"))


# ---------------------------------------------------------------- A. the token and the exchange

class Kube:
    """The cluster's API as the runner reaches it: one TokenRequest."""

    def __init__(self, answer=None, error=None):
        self.calls, self.answer, self.error = [], answer, error

    def __call__(self, method, path, body=None, ctype="application/json", timeout=30):
        self.calls.append((method, path, body))
        if self.error:
            raise http_error(self.error)
        return self.answer if self.answer is not None else {"status": {"token": "sa.jwt"}}


def test_the_token_is_asked_for_the_apps_workflow_service_account_for_the_data_store_only():
    kube = Kube()
    assert identity.request_token(kube, "ml-proj", "yolo") == "sa.jwt"
    (method, path, body), = kube.calls
    assert (method, path) == ("POST", "/api/v1/namespaces/ml-proj/serviceaccounts/yolo-workflow/token")
    assert body["spec"]["audiences"] == ["kaos-lake"] and body["kind"] == "TokenRequest"


@pytest.mark.parametrize("code, said", [(403, "may not be used by this run"), (404, "does not exist")])
def test_an_app_whose_service_account_cannot_be_used_is_refused_naming_the_app(code, said):
    with pytest.raises(identity.AppIdentityUnavailable) as e:
        identity.request_token(Kube(error=code), "ml-proj", "yolo")
    assert "the app yolo" in str(e.value) and "yolo-workflow" in str(e.value) and said in str(e.value)
    assert "Nothing was staged" in str(e.value)


def test_a_cluster_that_gives_no_token_is_a_refusal_too():
    with pytest.raises(identity.AppIdentityUnavailable, match="the app yolo"):
        identity.request_token(Kube(answer={"status": {}}), "ml-proj", "yolo")


def exchange_with(monkeypatch, answer=None, code=None):
    seen = []

    def opener(req):
        seen.append(req)
        if code:
            raise http_error(code)
        return io.BytesIO(json.dumps(answer).encode() if not isinstance(answer, bytes) else answer)
    monkeypatch.setattr(identity, "_open", opener)
    return seen


def test_the_token_is_exchanged_for_the_apps_key_pair(monkeypatch):
    seen = exchange_with(monkeypatch, {"access_key_id": "KAOSXA", "secret_access_key": "s3", "expiration": "2030-01-01T00:00:00Z"})
    key = identity.exchange(OWN, "sa.jwt", "yolo")
    req, = seen
    assert req.full_url == OWN + "/kaos/v1/credentials" and req.get_method() == "POST"
    assert req.get_header("Authorization") == "Bearer sa.jwt"
    assert (key.key_id, key.secret, key.expires) == ("KAOSXA", "s3", 1893456000.0)
    assert key.record() == {"access_key_id": "KAOSXA", "secret_access_key": "s3", "expiration": "2030-01-01T00:00:00Z"}
    assert key.auth() == "Basic S0FPU1hBOnMz"


@pytest.mark.parametrize("code", [401, 403, 404, 405, 501])
def test_a_data_store_that_does_not_know_the_app_refuses_the_run_naming_the_app(monkeypatch, code):
    exchange_with(monkeypatch, code=code)
    with pytest.raises(identity.AppIdentityUnavailable) as e:
        identity.exchange(OWN, "sa.jwt", "yolo")
    assert "the app yolo" in str(e.value) and "HTTP %d" % code in str(e.value) and "Nothing was staged" in str(e.value)


@pytest.mark.parametrize("answer", [b"not json", {"access_key_id": "x"}, {"access_key_id": "", "secret_access_key": ""}, []])
def test_an_answer_that_is_no_key_is_a_refusal(monkeypatch, answer):
    exchange_with(monkeypatch, answer=answer)
    with pytest.raises(identity.AppIdentityUnavailable, match="the app yolo"):
        identity.exchange(OWN, "sa.jwt", "yolo")


def test_a_failing_data_store_is_a_transient_error_not_a_refusal(monkeypatch):
    exchange_with(monkeypatch, code=503)
    with pytest.raises(urllib.error.HTTPError):
        identity.exchange(OWN, "sa.jwt", "yolo")


def test_a_key_pair_is_never_sent_across_a_redirect():
    assert identity._NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere") is None


def test_a_job_that_names_no_app_has_no_identity():
    cfg = helpers.settings(KAOS_APP=None)
    with pytest.raises(identity.AppIdentityUnavailable, match="names no app"):
        identity.app_key(Kube(), cfg)


# ---------------------------------------------------------------- D. refused before anything is staged

@pytest.mark.parametrize("fail", [
    [identity.AppIdentityUnavailable("the app yolo has no identity in the data store (HTTP 403)")],
    [OSError("connection refused")] * 4,
], ids=["the app is not known", "the data store never answers"])
def test_an_app_without_an_identity_is_refused_before_anything_is_staged(rig, capsys, fail):
    staged = []
    rig.monkeypatch.setattr(runmod.staging, "stage_dataset", lambda *a, **kw: staged.append("dataset") or "")
    rig.monkeypatch.setattr(runmod.imagemod, "stage_image", lambda *a, **kw: staged.append("image") or "")
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner._keys = helpers.FakeKeys(fail=fail)
    with pytest.raises(SystemExit) as e:
        rig.runner.run()
    assert e.value.code == 0, "reported: the pod ends cleanly"
    assert staged == [] and slurm.submitted == [] and slurm.cancelled == []
    run = rig.kube.run
    assert (run["state"], run["exitCode"], run["terminal"]) == ("FAILED", 246, True) and not run.get("siteJobID")
    assert "yolo" in run["message"]
    assert "ERROR: AppIdentityUnavailable:" in capsys.readouterr().out


def test_a_data_store_that_blips_is_asked_again_before_the_run_is_refused(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm)
    rig.runner._keys = helpers.FakeKeys(fail=[OSError("blip"), OSError("blip")])
    assert rig.runner.run() == 0
    assert len(slurm.submitted) == 1


def test_a_resubmit_of_an_app_without_an_identity_cancels_nothing(rig):
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0)]})
    rig.make(slurm)
    rig.runner.jid = 5001
    rig.runner._keys = helpers.FakeKeys(fail=[identity.AppIdentityUnavailable("the app yolo has no identity")])
    with pytest.raises(SystemExit):
        rig.runner.resubmit("pull token expired")
    assert slurm.cancelled == [] and slurm.submitted == []
    assert rig.kube.run["exitCode"] == 246


# ---------------------------------------------------------------- A, B. no bearer for the data store anywhere

def test_the_job_holds_no_bearer_for_the_data_store_in_its_environment_script_or_log(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    assert rig.runner.run() == 0
    sub = slurm.submitted[0]
    everything = json.dumps(sub)
    assert "LAKEFS_BEARER_TOKEN" not in everything
    assert helpers.APP_KEY_SECRET not in everything and helpers.APP_KEY_ID not in everything, "the key is not in the submission"
    assert "LAKEFS_KEY_FILE" in sub["script"] and "wallet.jwt" in sub["script"], "the tracker's token is the only credential"
    assert "LAKEFS_KEY_FILE" not in "\n".join(sub["job"]["environment"])


def test_no_variable_the_step_or_a_payload_reads_holds_a_data_store_bearer():
    assert "LAKEFS_BEARER_TOKEN" not in jobscript.STEP_ENV + jobscript.FILE_ENV
    text = "\n".join(jobscript.batch_lines("-", "/scr"))
    assert "LAKEFS_BEARER_TOKEN" not in text
    for program in (payloads.HANDBACK, payloads.STAGEIN):
        assert "LAKEFS_BEARER_TOKEN" not in program and "LAKEFS_KEY_FILE" in program


def test_the_script_waits_for_the_key_binds_it_read_only_and_ends_246_without_it():
    lines = jobscript.batch_lines("-", "/scr")
    text = "\n".join(lines)
    assert "KAOS_KEY_DIR=$SCR/kaos-tmp/key-$KAOS_JOB_ID" in text and "export LAKEFS_KEY_FILE=$KAOS_KEY_DIR/lakefs-key.json" in text
    assert "fail 246" in text and "-B $KAOS_KEY_DIR:/kaos/key:ro" in text
    assert "APPTAINERENV_LAKEFS_KEY_FILE=/kaos/key/lakefs-key.json" in text
    assert text.index("fail 246") < text.index("BIND=\"\"; KAOS_IN_ROOT"), "nothing runs before the key is there"
    assert "rm -rf \"$KAOS_KEY_DIR\"" in text, "the key folder goes with the job"


@pytest.mark.parametrize("program", ["HANDBACK", "STAGEIN"])
def test_a_payload_without_a_readable_key_file_fails_and_says_so(tmp_path, program):
    import subprocess
    import sys
    env = dict(os.environ, LAKEFS_ENDPOINT="http://127.0.0.1:9", LAKEFS_KEY_FILE=str(tmp_path / "gone.json"),
               KAOS_OUTPUTS_REPO="o", WF_UID="w", STEP_NAME="s", KAOS_JOB_ID="1", KAOS_WORK=str(tmp_path),
               DATASET_REPO="ds", DATASET_COMMIT="c" * 12, DATASET_CACHE=str(tmp_path / "c"), DATASET_DIRFILE=str(tmp_path / "d"))
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "kaos-result.json").write_text("{}")
    p = subprocess.run([sys.executable, "-c", getattr(payloads, program)], env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode != 0 and "key file" in (p.stdout + p.stderr), p.stdout + p.stderr


# ---------------------------------------------------------------- B. the key file is placed and renewed on the site

def test_the_key_is_placed_once_the_job_is_known_and_renewed_every_25_minutes(rig):
    slurm = FakeSlurm({"5001": [acc("RUNNING")]})
    r = rig.make(slurm)
    r.keys.expires = 5000.0 + 3600
    clock = [5000.0]
    r.runner.clock = lambda: clock[0]
    r.runner.prepare_handback()
    r.runner.jid = "5001"
    r.runner.keep_key()
    assert [k["secret_access_key"] for k in r.stager.keys] == [helpers.APP_KEY_SECRET + "1"], "the key obtained a moment ago"
    r.runner.keep_key()
    clock[0] += 24 * 60
    r.runner.keep_key()
    assert len(r.stager.keys) == 1, "not before 25 minutes"
    clock[0] += 2 * 60
    r.keys.expires = clock[0] + 3600
    r.runner.keep_key()
    assert [k["secret_access_key"] for k in r.stager.keys] == [helpers.APP_KEY_SECRET + "1", helpers.APP_KEY_SECRET + "2"]
    assert set(r.stager.keys[1]) == {"access_key_id", "secret_access_key", "expiration"}
    clock[0] += 26 * 60
    r.keys.expires = clock[0] + 3600
    r.runner.keep_key()
    assert len(r.stager.keys) == 3, "again after another 25 minutes"


def test_a_job_that_waits_in_the_queue_gets_a_fresh_key_for_its_new_site_job(rig):
    r = rig.make(FakeSlurm({}))
    r.runner.prepare_handback()
    r.runner.jid = "5001"
    r.runner.keep_key()
    r.runner.jid = "5002"                   # replaced: the new site job has no key file yet
    r.runner.keep_key()
    assert len(r.stager.keys) == 2


def test_a_key_that_cannot_be_placed_is_reported_asked_again_and_never_stops_the_job(rig, capsys):
    class Failing:
        keys = []

        def have_key(self):
            return False

        def __call__(self, words, feed=None, quiet=False):
            if words[0] == "key":
                return 75, []
            return 0, [{"event": "log", "offset": 0, "data": ""}]
    slurm = FakeSlurm({})
    r = rig.make(slurm, stager=Failing())
    r.runner.prepare_handback()
    r.runner.jid = "5001"
    r.runner.keep_key()
    assert "could not be renewed on the site" in r.kube.run["message"], "reported in the job's status"
    assert "not placed for site job 5001" in capsys.readouterr().out
    assert r.runner.key_for is None, "asked again at the next poll"
    r.runner.stager = type("Ok", (), {"__call__": lambda self, words, feed=None, quiet=False: (0, [{"event": "key"}])})()
    r.runner.keep_key()
    assert r.runner.key_for == "5001" and not r.kube.run.get("message"), "the message clears when a key lands"


def test_a_key_that_ends_soon_is_replaced_for_the_runners_own_calls():
    clock = [1000.0]
    keys = helpers.FakeKeys(expires=1000.0 + 3600)
    r = runmod.Runner(helpers.settings(), slurm=object(), stager=object(), reporter=object(), logs=object(), keys=keys,
                      mint=lambda: "w", clock=lambda: clock[0])
    first = r.current_key()
    assert r.current_key() is first
    clock[0] += 3500
    keys.expires = clock[0] + 3600
    assert r.current_key() is not first and keys.calls == 2


# the staging tool's `key` verb is placed by the site's tool (kubecore-operator, images/meluxina-tokenmint/stager): its file mode and
# its atomic replace are checked there; this is what a tool is held to by the runner.
def test_the_key_record_the_tool_is_given_is_exactly_the_file_the_payloads_read(tmp_path):
    key = helpers.app_key()
    path = helpers.write_key_file(tmp_path / "k", key)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600 and stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode) == 0o700
    assert json.load(open(path)) == key.record()


# ---------------------------------------------------------------- C. the tracker's identity

def test_the_tracker_token_is_minted_from_the_second_machine_user_when_it_exists(tmp_path, monkeypatch, capsys):
    hpc, used = tmp_path / "hpc-key", []
    hpc.write_text("{}")
    monkeypatch.setattr(wallet, "mint_wallet", lambda env=None, keyfile=None: used.append(keyfile) or "tracker.jwt")
    assert wallet.mint_tracker({"ZITADEL_HPC_KEY_FILE": str(hpc)}) == "tracker.jwt"
    assert used == [str(hpc)] and "mlflow-hpc is absent" not in capsys.readouterr().out


def test_without_the_second_machine_user_the_old_tracker_token_is_used_and_the_log_says_so_once(tmp_path, monkeypatch, capsys):
    used = []
    monkeypatch.setattr(wallet, "mint_wallet", lambda env=None, keyfile=None: used.append(keyfile) or "old.jwt")
    monkeypatch.setattr(wallet, "_FALLBACK_SAID", False)
    env = {"ZITADEL_HPC_KEY_FILE": str(tmp_path / "absent"), "ZITADEL_MACHINE_KEY_FILE": str(tmp_path / "old-key")}
    assert wallet.mint_tracker(env) == "old.jwt" and wallet.mint_tracker(env) == "old.jwt"
    assert used == [str(tmp_path / "old-key")] * 2
    out = capsys.readouterr().out
    assert out.count("Secret mlflow-hpc is absent") == 1, "said once, not at every mint"


def test_the_tracker_token_is_the_only_token_in_the_job(rig):
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
    assert rig.runner.run() == 0
    script = slurm.submitted[0]["script"]
    assert "export MLFLOW_TRACKING_TOKEN=wallet.jwt" in script and "LAKEFS_BEARER_TOKEN" not in script
