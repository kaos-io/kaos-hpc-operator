"""What the runner writes to its HPCArgoWorkflowJob: the status subresource, merge-patched."""
import json
import urllib.error

import helpers
import pytest

from kaos_hpc_runner import reporter as rep


def _reporter(kube=None, **env):
    cfg = helpers.settings(**env)
    kube = kube or helpers.FakeKube(cfg)
    return rep.Reporter(cfg, kube, sleep=lambda s: None), kube


def test_it_reports_to_its_own_job_in_its_own_namespace_with_a_merge_patch():
    r, kube = _reporter(HPC_JOB_NAMESPACE="ml-other", HPC_JOB_NAME="hpc-abc")
    kube.path = "/apis/hpc.kaos.io/v1alpha1/namespaces/ml-other/hpcargoworkflowjobs/hpc-abc"
    kube.job = {"metadata": {}, "status": {}}
    r.report(siteJobID="5425070", state="PENDING")
    method, path, body, ctype = kube.calls[-1]
    assert (method, path, ctype) == ("PATCH", kube.path + "/status", "application/merge-patch+json")
    assert set(body["status"]["run"]) == set(rep.RUN_FIELDS)
    assert "outputs" not in body["status"], "no outputs unless given"


def test_every_report_writes_the_whole_run_so_nothing_of_an_earlier_report_survives():
    r, kube = _reporter()
    r.report(siteJobID="5425070", state="FAILED", exitCode=85, terminal=True,
             checkpointRef="lakefs://outputs-yolo/" + "ab" * 32 + "/hpc-checkpoints/w/s/attempt-1/")
    r.attempt = 2
    r.report(siteJobID="5425099", state="PENDING")
    assert kube.run == {"attempt": 2, "siteJobID": "5425099", "state": "PENDING"}


def test_the_log_offset_rides_every_report_and_is_cleared_for_a_new_site_job():
    r, kube = _reporter()
    r.report(siteJobID="1", state="RUNNING")
    assert "logOffset" not in kube.run
    assert r.write_log_offset(4096) is True
    assert kube.run["logOffset"] == 4096 and kube.run["attempt"] == 1
    r.report(siteJobID="1", state="RUNNING", startTime="2026-11-06T10:00:00Z")
    assert kube.run["logOffset"] == 4096 and kube.run["startTime"] == "2026-11-06T10:00:00Z"
    r.log_offset = None                              # a replaced site job starts its log over
    r.report(siteJobID="2", state="PENDING")
    assert "logOffset" not in kube.run and "startTime" not in kube.run


def test_outputs_ride_in_the_same_patch_as_the_terminal_report():
    r, kube = _reporter()
    outs = {"training-result": json.dumps({"map50": 0.71})}
    r.report(must=True, outputs=outs, siteJobID="1", state="COMPLETED", exitCode=0, terminal=True)
    (patch,) = kube.patches()
    assert patch[2]["status"]["outputs"] == outs and patch[2]["status"]["run"]["terminal"] is True
    assert kube.status["outputs"] == outs


def test_outputs_above_64_kib_in_total_are_an_error_and_nothing_is_written():
    r, kube = _reporter()
    half = {"a": "x" * (32 * 1024), "b": "y" * (32 * 1024)}
    r.report(must=True, outputs=half, siteJobID="1", state="COMPLETED", terminal=True)   # exactly 64 KiB: fine
    kube.calls.clear()
    over = {"a": "x" * (32 * 1024), "b": "y" * (32 * 1024 + 1)}
    with pytest.raises(rep.OutputsTooLarge, match="65537 bytes"):
        r.report(must=True, outputs=over, siteJobID="1", state="COMPLETED", terminal=True)
    assert kube.calls == []
    assert rep.check_outputs({"é": "é" * 100})   # bytes, not characters, are counted
    with pytest.raises(rep.OutputsTooLarge):
        rep.check_outputs({"a": "é" * (32 * 1024 + 1)})


def test_the_end_of_an_attempt_is_reported_until_it_lands():
    r, kube = _reporter()
    kube.fail = [500, 503, OSError("timeout")]
    r.report(must=True, siteJobID="1", state="COMPLETED", exitCode=0, terminal=True)
    assert kube.run["terminal"] is True


def test_an_end_that_cannot_be_reported_fails_the_runner_so_the_job_retries(capsys):
    r, kube = _reporter()
    kube.fail = [500] * 60
    with pytest.raises(SystemExit) as e:
        r.report(must=True, siteJobID="1", state="COMPLETED", terminal=True)
    assert e.value.code == 1 and "could not be reported" in capsys.readouterr().out


def test_a_state_report_that_fails_does_not_stop_the_runner():
    r, kube = _reporter()
    kube.fail = [500]
    assert r.report(siteJobID="1", state="RUNNING") is False


def test_the_pending_reason_is_written_and_cleared():
    r, kube = _reporter()
    r.pending_reason("AssocGrpGRESMinutes")
    assert kube.status["pendingReason"] == "AssocGrpGRESMinutes"
    r.pending_reason(None)
    assert "pendingReason" not in kube.status
    kube.fail = [500]
    r.pending_reason("Priority")      # best effort: no exception


def test_the_job_is_read_through_api_blips_and_a_missing_one_is_none():
    r, kube = _reporter(kube=helpers.FakeKube(status={"phase": "Admitted"}))
    kube.fail = [500, OSError("reset")]
    assert r.get_job()["status"] == {"phase": "Admitted"}
    r, kube = _reporter(kube=helpers.FakeKube(exists=False))
    assert r.get_job() is None


def test_without_permission_the_runner_says_which_binding_is_missing(capsys):
    r, kube = _reporter()
    kube.fail = [403]
    with pytest.raises(SystemExit):
        r.get_job()
    assert "hpcargoworkflowjob-runner-role" in capsys.readouterr().out


def test_the_checkpoint_ref_round_trips():
    from kaos_hpc_runner.settings import resume_of
    ckpt = ("ab" * 32, "hpc-checkpoints/wf/model-training/attempt-2/")
    assert resume_of(rep.checkpoint_uri("outputs-yolo", ckpt)) == ckpt
    for bad in (None, "", "lakefs://r/not-a-commit/p/", "s3://x/" + "ab" * 32 + "/p/"):
        assert resume_of(bad) is None


def test_the_kube_client_rereads_its_projected_token(tmp_path, monkeypatch):
    (tmp_path / "token").write_text("one")
    (tmp_path / "ca.crt").write_text("")
    seen = []

    class R:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def read(self):
            return b"{}"
    monkeypatch.setattr(rep.ssl, "create_default_context", lambda cafile: None)
    monkeypatch.setattr(rep.urllib.request, "urlopen",
                        lambda req, context=None, timeout=0: seen.append(req.get_header("Authorization")) or R())
    kube = rep.Kube(sa=str(tmp_path), api="https://k")
    kube("GET", "/x")
    (tmp_path / "token").write_text("two")
    kube("GET", "/x")
    assert seen == ["Bearer one", "Bearer two"]
