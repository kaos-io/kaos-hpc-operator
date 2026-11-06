"""PRD-HPC-1461 D-72 / F-46 / AC-78: the idle watchdog (payload/idlewatch.py) and its wiring in the batch script."""
import importlib.util
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kaos_hpc_runner import jobscript, payloads  # noqa: E402

_spec = importlib.util.spec_from_file_location("idlewatch", ROOT / "kaos_hpc_runner" / "payload" / "idlewatch.py")
iw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(iw)

STEP = 30   # seconds between samples


class Series:
    """A probe / log watch that returns the next value of a series on each call."""

    def __init__(self, values):
        self.values = list(values)

    def util(self):
        return self.values.pop(0)

    def grew(self):
        return self.values.pop(0)


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def drive(util_series, grew_series, window_min=20):
    """Run the watchdog over a sampling series (30 s apart); the index of the sample that ends the step, or None."""
    clock = FakeClock()
    wd = iw.Watchdog(os.getpid(), Series(util_series), Series(grew_series), "/dev/null", window_min, clock=clock)
    for i in range(len(util_series)):
        clock.t += STEP
        if wd.tick():
            return i
    return None


# ---- the rule (sampling series)

def test_should_end_the_step_after_20_idle_minutes_when_gpu_and_log_are_both_quiet():
    n = 20 * 60 // STEP + 5
    got = drive([0] * n, [False] * n)
    # the first idle sample opens the window, 40 samples later it is 20 minutes: ended within 21 min of the hang
    assert got == 40 and (got + 1) * STEP <= 21 * 60


def test_should_not_end_a_step_whose_log_keeps_growing_with_the_gpu_idle():
    n = 90
    assert drive([0] * n, [True] * n) is None, "a slow CPU data-loading phase that logs is not a hang"


def test_should_not_end_a_step_with_a_busy_gpu_and_a_silent_log():
    n = 90
    assert drive([80] * n, [False] * n) is None


def test_log_growth_resets_the_window():
    n = 100
    grew = [False] * n
    grew[30] = True                                   # one line at minute 15
    assert drive([0] * n, grew) == 71                 # a new full window opens after the growth at sample 30


def test_a_sample_that_cannot_be_taken_resets_the_window_and_never_ends_the_step():
    n = 60
    util = [0] * n
    util[39] = None                                   # nvidia-smi hiccup just before the window fills
    got = drive(util, [False] * n)
    assert got is None or got > 40


def test_gpu_use_of_5_percent_or_more_resets_the_window():
    n = 100
    util = [0] * n
    util[30] = 5                                      # exactly 5 % is busy: the bound is below 5
    got = drive(util, [False] * n)
    assert got is not None and got > 40


def test_a_longer_window_is_honoured():
    n = 130
    assert drive([0] * n, [False] * n, window_min=60) == 120


# ---- the log signal

def test_the_samplers_own_metric_lines_are_not_log_growth(tmp_path):
    f = tmp_path / "slurm-1.out"
    f.write_text("start\n")
    lw = iw.LogWatch(str(f))
    with open(f, "a") as o:
        o.write('KAOS_METRIC {"t":1,"gpus":[]}\n')
    assert lw.grew() is False
    with open(f, "a") as o:
        o.write("step: epoch 1\n")
    assert lw.grew() is True
    assert lw.grew() is False                         # nothing new since


def test_an_unfinished_line_is_growth_only_when_it_changes(tmp_path):
    f = tmp_path / "slurm-1.out"
    f.write_text("")
    lw = iw.LogWatch(str(f))
    with open(f, "a") as o:
        o.write("progress 10%")                       # a progress bar without newline
    assert lw.grew() is True
    assert lw.grew() is False                         # same bytes: no growth
    with open(f, "a") as o:
        o.write("\rprogress 20%")
    assert lw.grew() is True
    with open(f, "a") as o:
        o.write('\nKAOS_METRIC {"t":')                # half a metric line is not growth
    assert lw.grew() is True                          # the finished progress line was new
    assert lw.grew() is False


def test_a_missing_log_file_is_no_growth(tmp_path):
    assert iw.LogWatch(str(tmp_path / "none.out")).grew() is False


# ---- the CPU tier

def test_cpu_tier_uses_the_cpu_use_of_the_steps_processes():
    ticks = iter([{1: 0}, {1: 100}, {1: 100, 2: 100}, {1: 100, 2: 100}])   # CLK_TCK 100: 100 ticks = 1 s of CPU
    clock = FakeClock()
    p = iw.CpuProbe(1, ticks=lambda pgid: next(ticks), clock=clock)
    p.hz = 100
    assert p.util() is None                           # needs two samples
    clock.t += 10
    assert round(p.util()) == 10                      # 1 CPU-second in 10 s
    clock.t += 10
    assert round(p.util()) == 10                      # a new process counts its ticks
    clock.t += 10
    assert p.util() == 0


def test_cpu_tier_ends_a_silent_step_below_5_percent_and_spares_a_computing_one():
    n = 50
    assert drive([None] + [1.0] * (n - 1), [False] * n) is not None       # 1 % of a core, silent: idle
    assert drive([None] + [60.0] * (n - 1), [False] * n) is None          # computing: busy


def test_step_ticks_finds_the_step_by_group_and_descendants():
    p = subprocess.Popen(["sh", "-c", "sleep 30 & wait"], start_new_session=True)   # own group, a child
    try:
        time.sleep(0.5)
        got = iw.step_ticks(p.pid)
        assert p.pid in got and len(got) >= 2
        assert os.getpid() not in got
    finally:
        os.killpg(p.pid, signal.SIGKILL)
        p.wait()


# ---- which GPUs are the step's (per-slot attribution)

SMI = "0, GPU-aaaa1111, 0\n1, GPU-bbbb2222, 93\n2, GPU-cccc3333, 0\n3, GPU-dddd4444, 0\n"


def util_of(env):
    return iw.gpu_util(iw.pick_gpus(iw.parse_gpu_csv(SMI), iw.gpu_tokens(env)))


def test_a_slot_is_judged_by_its_own_gpu_only():
    assert util_of({"CUDA_VISIBLE_DEVICES": "0"}) == 0, "slot 0 is idle although GPU 1 is busy"
    assert util_of({"CUDA_VISIBLE_DEVICES": "1"}) == 93, "slot 1 is busy"
    assert util_of({"CUDA_VISIBLE_DEVICES": "2,3"}) == 0
    assert util_of({"CUDA_VISIBLE_DEVICES": "0,1"}) == 93, "the highest of the step's GPUs"


def test_gpus_can_be_named_by_uuid_or_by_nvidia_visible_devices():
    assert util_of({"CUDA_VISIBLE_DEVICES": "GPU-bbbb"}) == 93
    assert util_of({"NVIDIA_VISIBLE_DEVICES": "2"}) == 0
    assert util_of({"CUDA_VISIBLE_DEVICES": "3", "NVIDIA_VISIBLE_DEVICES": "1"}) == 0, "CUDA wins"


def test_an_unnamed_set_means_every_gpu_so_a_busy_neighbour_keeps_the_step_alive():
    for env in ({}, {"CUDA_VISIBLE_DEVICES": ""}, {"NVIDIA_VISIBLE_DEVICES": "all"}, {"CUDA_VISIBLE_DEVICES": "7"}):
        assert util_of(env) == 93, env


def test_an_unreadable_gpu_util_is_unknown_not_idle():
    assert iw.gpu_util(iw.parse_gpu_csv("0, GPU-a, [N/A]\n")) is None
    assert iw.gpu_util([]) is None


# ---- ending the step, per slot

def _spawn():
    return subprocess.Popen(["sleep", "60"], start_new_session=True)


def test_an_idle_slot_ends_only_its_own_step_while_the_other_slot_keeps_running(tmp_path, monkeypatch):
    monkeypatch.setattr(iw, "SAMPLE_S", 0.05)
    monkeypatch.setattr(iw, "GRACE_S", 0.2)
    idle_step, busy_step = _spawn(), _spawn()
    gpus = {"0": 0, "1": 90}                          # the shared node: slot 0 idle, slot 1 busy
    smi = lambda: [(i, "GPU-" + i, u) for i, u in gpus.items()]   # noqa: E731
    dogs = []
    for step, gpu, name in ((idle_step, "0", "a"), (busy_step, "1", "b")):
        log = tmp_path / ("slurm-%s.out" % name)
        log.write_text("hello\n")
        wd = iw.Watchdog(step.pid, iw.GpuProbe([gpu], query=smi), iw.LogWatch(str(log)),
                         str(tmp_path / ("mark-" + name)), 20)
        wd.rule.window_s = 0.3
        dogs.append(wd)
    threads = [threading.Thread(target=d.run) for d in dogs]
    for t in threads:
        t.start()
    try:
        assert idle_step.wait(timeout=10) == -signal.SIGTERM, "the idle step got TERM"
        time.sleep(0.5)
        threads[0].join(5)
        assert (tmp_path / "mark-a").exists() and not (tmp_path / "mark-b").exists()
        assert busy_step.poll() is None, "the busy slot is untouched"
    finally:
        for s in (idle_step, busy_step):
            if s.poll() is None:
                os.killpg(s.pid, signal.SIGKILL)
        busy_step.wait()
        threads[1].join(5)


def test_a_step_that_ignores_term_is_killed_after_the_grace(tmp_path, monkeypatch):
    monkeypatch.setattr(iw, "SAMPLE_S", 0.05)
    monkeypatch.setattr(iw, "GRACE_S", 0.5)
    step = subprocess.Popen(["sh", "-c", "trap '' TERM; sleep 60 & wait"], start_new_session=True)
    time.sleep(0.3)
    wd = iw.Watchdog(step.pid, Series([0] * 200), Series([False] * 200), str(tmp_path / "m"), 20)
    wd.rule.window_s = 0.0
    t0 = time.time()
    wd.run()
    try:
        assert step.wait(timeout=10) == -signal.SIGKILL and time.time() - t0 >= 0.5
    finally:
        if step.poll() is None:
            os.killpg(step.pid, signal.SIGKILL)


# ---- the batch script

def test_the_batch_script_starts_the_watchdog_beside_the_step_and_reads_its_marker():
    lines = jobscript.batch_lines("-", "/scratch")
    text = "\n".join(lines)
    assert "IDLEWATCH_B64" in text and "KAOS_IDLE_TIMEOUT_MIN:-20" in text
    start = next(i for i, l in enumerate(lines) if l.startswith('if [ -n "$IDLEWATCH_B64" ]'))
    assert start > lines.index("STEP_PID=$!"), "after the step exists"
    marker = 'if [ -f "$IDLE_MARK" ]; then rm -f "$IDLE_MARK"; rc=236; echo "idle watchdog: the step ended as Idle (exit 236)"; fi'
    assert lines.index(marker) < lines.index("if [ $rc -eq 85 ]; then"), "Idle wins over a checkpoint exit"


def _run_block(tmp_path, step_cmd, **extra_env):
    """The step part of the batch script (fake apptainer and nvidia-smi); (exit code, job output)."""
    lines = jobscript.batch_lines("-", str(tmp_path))
    fail = next(l for l in lines if l.startswith("fail(){"))
    sampler = next(l for l in lines if l.startswith("kaos_sample(){"))
    start = lines.index('[ -n "$STEP_CMD" ] || STEP_CMD="nvidia-smi -L || nproc"')
    script = "\n".join([fail, sampler] + lines[start:])
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (bindir / "apptainer").write_text('#!/bin/bash\nwhile [ "$1" != "$SIF" ]; do shift; done; shift; exec "$@"\n')
    (bindir / "apptainer").chmod(0o755)
    (bindir / "nvidia-smi").write_text('#!/bin/sh\ncase "$*" in *index,uuid*) echo "0, GPU-a, 0";; *) echo "0, 1024, 81920";; esac\n')
    (bindir / "nvidia-smi").chmod(0o755)
    (tmp_path / "kaos-tmp").mkdir(exist_ok=True)
    env = dict(os.environ, SIF="img.sif", STEP_CMD=step_cmd, NV="", PWD_OPT="", BIND="", SCR=str(tmp_path),
               KAOS_JOB_ID="7", KAOS_WORK=str(tmp_path / "work"), HPC_GPUS="1", KAOS_IDLE_TIMEOUT_MIN="20",
               IDLEWATCH_B64=payloads.b64(payloads.IDLEWATCH), KAOS_IDLE_SAMPLE_S="0.2", KAOS_IDLE_GRACE_S="1",
               KAOS_IDLE_WINDOW_S="1.5", PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    env.update(extra_env)
    with open(tmp_path / "slurm-7.out", "w") as out:
        p = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, stdout=out, stderr=subprocess.STDOUT, timeout=60)
    return p.returncode, (tmp_path / "slurm-7.out").read_text()


def test_a_hung_step_ends_with_exit_236_and_leaves_no_marker(tmp_path):
    t0 = time.time()
    rc, out = _run_block(tmp_path, "echo started; sleep 120")
    assert rc == 236, out
    assert "idle watchdog: the step ended as Idle (exit 236)" in out and "reason Idle" in out
    assert time.time() - t0 < 30
    assert not list((tmp_path / "kaos-tmp").glob("idle-*"))


def test_a_step_that_keeps_logging_is_not_ended(tmp_path):
    rc, out = _run_block(tmp_path, "for i in 1 2 3 4 5 6 7 8; do echo tick $i; sleep 0.4; done; exit 3")
    assert rc == 3, out
    assert "reason Idle" not in out


def test_idle_timeout_0_starts_no_watchdog(tmp_path):
    rc, out = _run_block(tmp_path, "sleep 4; exit 5", KAOS_IDLE_TIMEOUT_MIN="0")
    assert rc == 5, out
    assert "idle watchdog" not in out


def test_without_the_payload_the_job_runs_as_before(tmp_path):
    rc, out = _run_block(tmp_path, "sleep 2; exit 0", IDLEWATCH_B64="")
    assert rc == 0, out
