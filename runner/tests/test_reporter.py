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
             checkpointRef="scratch://kaos-ckpt/w/s/000001.tar#" + "ab" * 64)
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


def test_the_result_rides_in_the_same_patch_as_the_terminal_report():
    r, kube = _reporter()
    wire = helpers.fixture("result-model.wire")
    r.report(must=True, publish=True, result=wire, siteJobID="1", state="COMPLETED", exitCode=0, terminal=True)
    (patch,) = kube.patches()
    assert patch[2]["status"]["result"] == wire and patch[2]["status"]["run"]["terminal"] is True
    assert kube.status["result"] == wire


def test_a_step_that_wrote_no_result_publishes_none_and_removes_what_was_there():
    r, kube = _reporter()
    kube.job["status"]["result"] = "left by something else"
    r.report(must=True, publish=True, result=None, siteJobID="1", state="COMPLETED", exitCode=0, terminal=True)
    assert kube.patches()[0][2]["status"]["result"] is None and "result" not in kube.status


def test_no_other_report_touches_the_result():
    r, kube = _reporter()
    for fields in ({"state": "RUNNING"}, {"state": "FAILED", "exitCode": 1, "terminal": True}):
        r.report(must=True, siteJobID="1", **fields)
    assert all("result" not in p[2]["status"] for p in kube.patches())
    assert not hasattr(rep, "check_outputs"), "status.outputs is gone: one result, checked by result.check"


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
    ckpt = {"sha": "ab" * 32, "rel": "kaos-ckpt/wf/model-training/000002.tar"}
    assert resume_of(rep.checkpoint_uri(ckpt)) == (ckpt["sha"], ckpt["rel"])
    for bad in (None, "", "lakefs://r/" + "ab" * 32 + "/p/", "s3://x/" + "ab" * 32 + "/p/"):
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


def test_the_gpu_readings_ride_every_report_and_are_cleared_for_a_new_site_job():
    r, kube = _reporter()
    r.gpu_util = [{"gpu": "0", "percent": 50, "maxPercent": 80}]
    r.report(siteJobID="1", state="RUNNING")
    assert kube.run["gpuUtilization"] == r.gpu_util
    r.gpu_util = None
    r.report(siteJobID="2", state="PENDING")
    assert "gpuUtilization" not in kube.run


def test_queue_replacements_keep_the_newest_and_count_all_and_a_restarted_runner_carries_on():
    r, kube = _reporter()
    for n in range(rep.QUEUE_REPLACED_KEPT + 3):
        r.note_replacement(str(n + 1), old=str(n), reason="TokenRefresh", queued=60 * n)
    r.report(siteJobID="8", state="PENDING")
    kept = kube.run["queueReplaced"]
    assert [x["newSiteJobID"] for x in kept] == ["4", "5", "6", "7", "8"] and kube.run["queueReplacedCount"] == 8
    r2, kube2 = _reporter()
    r2.restore_replacements(kube.run)
    r2.note_replacement("9", old="8", reason="HostLost", queued=5)
    r2.report(siteJobID="9", state="PENDING")
    assert kube2.run["queueReplacedCount"] == 9 and len(kube2.run["queueReplaced"]) == rep.QUEUE_REPLACED_KEPT
    assert kube2.run["queueReplaced"][-1]["reason"] == "HostLost"


def test_the_staging_record_rides_every_report_and_a_restarted_runner_keeps_the_first_copy():
    info = {"repo": "ds", "commit": "c2" * 6, "kind": "first", "files": 2, "bytes": 14, "seconds": 9}
    r, kube = _reporter()
    r.note_staging(info)
    r.report(siteJobID="1", state="PENDING")
    r.report(siteJobID="1", state="RUNNING")
    assert kube.run["staged"] == [info]
    # The runner restarts in the same attempt: its own staging finds the copy resident ("cached") and must not
    # replace what the first one reported; another commit does replace it.
    r2, kube2 = _reporter()
    r2.restore_staging(kube.run)
    r2.note_staging(dict(info, kind="cached", files=0, bytes=0, seconds=0))
    r2.report(siteJobID="1", state="RUNNING")
    assert kube2.run["staged"] == [info]
    # A second input is a second record, in the order staged.
    other = dict(info, repo="labels", commit="d3" * 6, kind="delta")
    r2.note_staging(other)
    r2.report(siteJobID="1", state="RUNNING")
    assert kube2.run["staged"] == [info, other]


def test_no_staging_record_is_sent_when_the_job_stages_in_job():
    r, kube = _reporter()
    r.note_staging({})
    r.report(siteJobID="1", state="PENDING")
    assert "staged" not in kube.run
