"""HPC job logs (D-65, F-38, AC-70): the stager's read-only `log` subcommand, JSON lines on stdout, the offset
in status.run.logOffset."""
import base64
import json
import time

import helpers

from kaos_hpc_runner import logs as logsmod
from kaos_hpc_runner import reporter as rep


class LogStager:
    """The login-node stager's `log --job <id> --offset <n> [--max <bytes>]`, over in-memory job outputs."""

    def __init__(self, files=None, rc=0):
        self.files, self.calls, self.rc = files or {}, [], rc

    def have_key(self):
        return False            # no staging key: the runner stages nothing and only streams the log

    def __call__(self, words, feed=None, quiet=False):
        assert words[0] == "log" and quiet, "read-only, and its data is printed by the streamer, not echoed"
        w = dict(zip(words[1::2], words[2::2]))
        self.calls.append((w["--job"], int(w["--offset"])))
        if self.rc:
            return self.rc, []
        data = self.files.get(w["--job"], b"")[int(w["--offset"]):][:int(w["--max"])]
        return 0, [{"event": "log", "offset": int(w["--offset"]) + len(data),
                    "data": base64.b64encode(data).decode()}]


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def _streamer(stager, job="5425070", offset=0, **kw):
    cfg = helpers.settings(KAOS_ATTEMPT="2")
    kube = helpers.FakeKube(cfg, status={"run": {"attempt": 2, "siteJobID": job}})
    r = rep.Reporter(cfg, kube, sleep=lambda s: None)
    out = []
    s = logsmod.LogStreamer(cfg, stager, r, out=out.append, clock=kw.pop("clock", Clock()), **kw)
    s.attach(job, offset)
    return s, out, kube


def _msgs(out):
    return [json.loads(l)["msg"] for l in out]


def test_each_complete_line_is_one_json_object_with_the_job_step_workflow_and_attempt():
    s, out, _ = _streamer(LogStager({"5425070": b"epoch 1\nepoch 2\n"}))
    assert s.poll() is True
    assert [json.loads(l) for l in out] == [
        {"hpc_job": "5425070", "step": "model-training", "workflow": "yolo-pipeline-x7k2p", "attempt": 2, "msg": "epoch 1"},
        {"hpc_job": "5425070", "step": "model-training", "workflow": "yolo-pipeline-x7k2p", "attempt": 2, "msg": "epoch 2"}]
    assert list(json.loads(out[0])) == ["hpc_job", "step", "workflow", "attempt", "msg"]


def test_a_half_written_line_waits_for_its_end_and_is_never_split():
    files = {"5425070": b"epoch 1\nepo"}
    s, out, _ = _streamer(LogStager(files))
    s.poll()
    assert _msgs(out) == ["epoch 1"] and s.committed() == len(b"epoch 1\n")
    files["5425070"] += b"ch 2\r\nlast"
    s.poll()
    assert _msgs(out) == ["epoch 1", "epoch 2"], "CRLF is stripped, the unfinished line still waits"
    s.finish()
    assert _msgs(out) == ["epoch 1", "epoch 2", "last"], "once the job ended the last line is printed too"


def test_metric_lines_carry_their_parsed_json_and_a_bad_one_is_plain_text():
    sample = {"t": 1762300800, "gpus": [{"util": 90, "mem_used_mib": 10, "mem_total_mib": 80}], "load": [1, 2, 3]}
    files = {"5425070": ("KAOS_METRIC %s\nKAOS_METRIC {broken\nKAOS_METRICS no\n" % json.dumps(sample)).encode()}
    s, out, _ = _streamer(LogStager(files))
    s.poll()
    a, b, c = (json.loads(l) for l in out)
    assert a["metric"] is True and a["m"] == sample and a["msg"].startswith("KAOS_METRIC {")
    assert "metric" not in b and "m" not in b and b["msg"] == "KAOS_METRIC {broken"
    assert "metric" not in c


def test_unicode_and_invalid_bytes_do_not_stop_the_stream():
    s, out, _ = _streamer(LogStager({"5425070": "Ünïcode ✓\n".encode() + b"\xff\xfe bad\n"}))
    s.poll()
    assert _msgs(out)[0] == "Ünïcode ✓" and "bad" in _msgs(out)[1]


def test_it_catches_up_in_as_many_reads_as_it_takes(monkeypatch):
    monkeypatch.setattr(logsmod, "MAX_BYTES", 8)
    body = b"".join(b"line %d\n" % i for i in range(20))
    stager = LogStager({"5425070": body})
    s, out, _ = _streamer(stager)
    s.poll()
    assert _msgs(out) == ["line %d" % i for i in range(20)]
    assert s.offset == len(body)


def test_the_offset_is_written_at_most_every_30_seconds():
    clock = Clock()
    files = {"5425070": b"a\n"}
    s, out, kube = _streamer(LogStager(files), clock=clock)
    clock.t = 10
    s.poll()
    assert "logOffset" not in kube.run, "10 s after the start: too early"
    files["5425070"] += b"b\n"
    clock.t = 31
    s.poll()
    assert kube.run["logOffset"] == 4 and kube.run["attempt"] == 2
    files["5425070"] += b"c\n"
    clock.t = 40
    s.poll()
    assert kube.run["logOffset"] == 4, "9 s after the last write"
    s.persist(force=True)                  # the runner stops (SIGTERM) or the job ended
    assert kube.run["logOffset"] == 6
    n = len(kube.patches())
    s.persist(force=True)
    assert len(kube.patches()) == n, "an unchanged offset is not written again"


def test_a_restarted_runner_resumes_without_a_duplicate_line_ac70():
    files = {"5425070": b"".join(b"step %d\n" % i for i in range(10)) + b"partial"}
    first_stager = LogStager(files)
    s1, out1, kube = _streamer(first_stager)
    s1.poll()
    files["5425070"] += b" line\nstep 11\n"
    s1.persist(force=True)                  # what on_term does
    saved = kube.run["logOffset"]
    assert saved == len(b"".join(b"step %d\n" % i for i in range(10))), "up to the last complete line"
    # the runner pod is replaced: a new runner reads the offset from the job's status
    s2, out2, _ = _streamer(LogStager(files), offset=saved)
    s2.finish()
    assert _msgs(out1) + _msgs(out2) == ["step %d" % i for i in range(10)] + ["partial line", "step 11"]


def test_a_new_site_job_starts_its_log_over():
    files = {"1": b"old job\n", "2": b"new job\n"}
    stager = LogStager(files)
    s, out, kube = _streamer(stager, job="1")
    s.poll()
    s.finish()
    s.attach("2", 0)
    assert kube_offset_cleared(s)
    s.poll()
    assert _msgs(out) == ["old job", "new job"] and stager.calls[-1] == ("2", 0)
    assert [json.loads(l)["hpc_job"] for l in out] == ["1", "2"]


def kube_offset_cleared(s):
    return s.reporter.log_offset is None and s.committed() == 0


def test_a_stager_without_log_disables_streaming_with_one_warning(capsys):
    stager = LogStager(rc=2)               # an unknown subcommand is a usage error
    s, out, kube = _streamer(stager)
    s.poll()
    s.poll()
    s.finish()
    assert out == [] and s.disabled and len(stager.calls) == 1
    assert capsys.readouterr().out.count("has no `log` subcommand") == 1
    assert "logOffset" not in kube.run


def test_a_transient_stager_failure_is_retried_not_disabled():
    stager = LogStager({"5425070": b"x\n"}, rc=255)      # ssh dropped
    s, out, _ = _streamer(stager)
    s.poll()
    assert not s.disabled and out == []
    stager.rc = 0
    s.poll()
    assert _msgs(out) == ["x"]


def test_a_stager_that_raises_does_not_take_the_runner_down(capsys):
    def boom(*a, **k):
        raise OSError("ssh not found")
    s, out, _ = _streamer(boom)
    assert s.poll() is False
    assert "stager call failed" in capsys.readouterr().out


def test_the_background_thread_polls_only_while_the_job_runs():
    files = {"5425070": b"hello\n"}
    stager = LogStager(files)
    s, out, _ = _streamer(stager, poll_s=0.01)
    s.start()
    try:
        time.sleep(0.15)
        assert stager.calls == [] and out == [], "queued: nothing to read yet"
        s.active.set()
        for _ in range(100):
            if out:
                break
            time.sleep(0.02)
        assert _msgs(out) == ["hello"]
        s.active.clear()
        n = len(stager.calls)
        time.sleep(0.15)
        assert len(stager.calls) == n
    finally:
        s.close()


def test_the_tail_keeps_the_last_step_lines_and_leaves_the_metric_lines_out():
    """D-72: an Idle report carries what the step last said, not the sampler's lines."""
    body = b"".join(b"line %d\n" % i for i in range(30)) + b'KAOS_METRIC {"t":1,"gpus":[]}\nwaiting for rank 1\n'
    s, _, _ = _streamer(LogStager({"5425070": body}))
    s.poll()
    lines = s.tail_text().split("\n")
    assert lines[-1] == "waiting for rank 1" and "KAOS_METRIC" not in s.tail_text()
    assert len(lines) == logsmod.TAIL_LINES and lines[0] == "line 11"


def test_the_tail_is_bounded():
    s, _, _ = _streamer(LogStager({"5425070": (b"x" * 500 + b"\n") * 10}))
    s.poll()
    assert len(s.tail_text()) == logsmod.TAIL_MAX_CHARS


def _sample(*utils):
    gpus = ",".join('{"util":%s,"mem_used_mib":1,"mem_total_mib":2}' % u for u in utils)
    return ('KAOS_METRIC {"t":1,"gpus":[%s],"load":[0,0,0],"mem_available_kb":1}\n' % gpus).encode()


def test_gpu_samples_keep_the_last_and_the_highest_value_per_gpu():
    """AC-69: the runner reports last and max per GPU in status.run.gpuUtilization."""
    body = _sample(10, 0) + _sample(90, 5) + _sample(40, "null")
    s, _, _ = _streamer(LogStager({"5425070": body}))
    s.poll()
    assert s.reporter.gpu_util == [{"gpu": "0", "percent": 40, "maxPercent": 90},
                                   {"gpu": "1", "percent": 5, "maxPercent": 5}]


def test_a_sample_without_a_gpu_or_with_a_broken_line_reports_nothing():
    body = b'KAOS_METRIC {"t":1,"gpus":[],"load":[0,0,0]}\nKAOS_METRIC {broken\n'
    s, _, _ = _streamer(LogStager({"5425070": body}))
    s.poll()
    assert s.reporter.gpu_util is None


def test_the_gpu_readings_are_written_with_the_offset_and_only_when_they_changed():
    clock = Clock()
    s, _, kube = _streamer(LogStager({"5425070": _sample(70)}), clock=clock)
    clock.t = 31
    s.poll()
    assert kube.run["gpuUtilization"] == [{"gpu": "0", "percent": 70, "maxPercent": 70}]
    n = len(kube.patches())
    clock.t = 100
    s.persist()
    assert len(kube.patches()) == n, "nothing changed: no write"


def test_attaching_a_new_site_job_starts_the_readings_over():
    s, _, _ = _streamer(LogStager({"5425070": _sample(70)}))
    s.poll()
    s.attach("5425071", 0)
    assert s.reporter.gpu_util is None
