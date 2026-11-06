"""Phase-1 additions to the batch script: the KAOS_METRIC sampler (D-65) and the staged-tar deletion (D-62)."""
import json
import os
import subprocess
from pathlib import Path

from kaos_hpc_runner import jobscript

LINES = jobscript.batch_lines("-", "/scratch")


def _sampler_line():
    return next(l for l in LINES if l.startswith("kaos_sample(){"))


def _bin(tmp_path, nvidia=None):
    bindir = tmp_path / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    if nvidia is not None:
        (bindir / "nvidia-smi").write_text("#!/bin/sh\n" + nvidia)
        (bindir / "nvidia-smi").chmod(0o755)
    return bindir


def _sample_once(tmp_path, nvidia):
    bindir = _bin(tmp_path, nvidia)
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    script = _sampler_line() + "\nkaos_sample & P=$!; sleep 1; kill $P\nwait $P 2>/dev/null\nexit 0"
    p = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    metric = [l for l in p.stdout.splitlines() if l.startswith("KAOS_METRIC ")]
    assert len(metric) == 1, p.stdout + p.stderr
    return json.loads(metric[0][len("KAOS_METRIC "):])


def test_the_sampler_reports_gpu_load_and_memory_as_json(tmp_path):
    m = _sample_once(tmp_path, 'printf "37, 1024, 81920\\n 12, 5, 81920\\n"\n')
    assert m["gpus"] == [{"util": 37, "mem_used_mib": 1024, "mem_total_mib": 81920},
                         {"util": 12, "mem_used_mib": 5, "mem_total_mib": 81920}]
    assert len(m["load"]) == 3 and m["mem_available_kb"] > 0 and m["t"] > 1.7e9


def test_a_value_nvidia_smi_cannot_report_is_null(tmp_path):
    m = _sample_once(tmp_path, 'printf "[N/A], 5, 81920\\n"\n')
    assert m["gpus"] == [{"util": None, "mem_used_mib": 5, "mem_total_mib": 81920}]


def test_without_nvidia_smi_or_with_a_failing_one_the_sampler_still_samples(tmp_path):
    assert _sample_once(tmp_path / "a", None)["gpus"] == []     # a CPU class
    assert _sample_once(tmp_path / "b", "echo 'NVIDIA-SMI has failed' >&2; exit 9\n")["gpus"] == []


def test_the_sampler_starts_after_the_step_and_stops_with_it(tmp_path):
    start = LINES.index('[ -n "$STEP_CMD" ] || STEP_CMD="nvidia-smi -L || nproc"')
    end = LINES.index("trap - USR1", start)
    block = LINES[start:end + 1]
    assert block.index("kaos_sample &") > block.index("STEP_PID=$!")
    assert block.index("kill $SAMPLER_PID 2>/dev/null") > block.index(
        "while kill -0 $STEP_PID 2>/dev/null; do wait $STEP_PID; rc=$?; done")
    bindir = _bin(tmp_path)
    (bindir / "apptainer").write_text('#!/bin/bash\nwhile [ "$1" != "$SIF" ]; do shift; done; shift; exec "$@"\n')
    (bindir / "apptainer").chmod(0o755)
    script = "\n".join([_sampler_line()] + block + ['wait $SAMPLER_PID 2>/dev/null; echo "sampler rc=$?"',
                                                    'echo "step rc=$rc"', "exit $rc"])
    env = dict(os.environ, SIF="img.sif", STEP_CMD="echo hello; sleep 1; exit 3", NV="", PWD_OPT="", BIND="",
               PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    p = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert p.returncode == 3, "the sampler never changes the step's exit code: " + p.stdout + p.stderr
    assert "hello" in p.stdout and "KAOS_METRIC " in p.stdout
    assert "sampler rc=0" in p.stdout, "stopped at the end of the step, its sleep with it"


def test_the_staged_tar_is_deleted_only_after_a_successful_build(tmp_path):
    start = LINES.index('if [ ! -f "$SIF" ]; then')
    block = LINES[start:LINES.index("fi", start) + 1]

    def run(build_ok):
        scr = tmp_path / ("ok" if build_ok else "bad")
        (scr / "sif-cache").mkdir(parents=True)
        tar = scr / "k.tar"
        tar.write_bytes(b"tar")
        bindir = scr / "bin"
        bindir.mkdir()
        (bindir / "apptainer").write_text(
            '#!/bin/bash\nif [ "$1" = build ]; then [ "%d" = 1 ] || exit 1; fi\n'
            'if [ "$1" = pull ]; then exit 1; fi\nprintf sif > "$2"\n' % build_ok)
        (bindir / "apptainer").chmod(0o755)
        env = dict(os.environ, SCR=str(scr), SIF=str(scr / "sif-cache" / "abc.sif"), IMAGE_REF="r/x:t",
                   SLURM_JOB_ID="1", IMAGE_STAGED=str(tar), PATH=str(bindir) + os.pathsep + os.environ["PATH"])
        script = "\n".join(['fail(){ exit "$1"; }'] + block)
        subprocess.run(["bash", "-c", script], env=env, stdout=subprocess.PIPE)
        return tar

    assert not run(True).exists(), "D-62: the SIF has the same key, so the tar is never needed again"
    assert run(False).exists(), "a failed build keeps the tar for the next try"
