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


# ---------------------------------------------------------------- AC-65: image lifecycle, the job's half

def _image_block():
    """From the label function to the first cleanup line: SIF label, build/pull, tar deletion."""
    a = next(i for i, l in enumerate(LINES) if l.startswith("kaos_sif_label(){"))
    b = next(i for i, l in enumerate(LINES) if l.startswith("find $SCR/sif-cache"))
    return LINES[a:a + 1] + LINES[LINES.index('if [ ! -f "$SIF" ]; then'):b]


def _run_image_block(tmp_path, name, *, sif_cached=False, tar=True, build_ok=True, pull_ok=True,
                     image_ref="reg.example/proj/app:v1@sha256:" + "a" * 64, step="train"):
    scr = tmp_path / name
    (scr / "sif-cache").mkdir(parents=True)
    sif = scr / "sif-cache" / "k.sif"
    if sif_cached:
        sif.write_bytes(b"old sif")
    staged = scr / "images" / "k.tar"
    staged.parent.mkdir()
    staged.write_bytes(b"tar")
    bindir = scr / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text(
        '#!/bin/bash\necho "$@" >> "$SCR/calls"\n'
        'if [ "$1" = build ]; then [ "%d" = 1 ] || exit 1; fi\n'
        'if [ "$1" = pull ]; then [ "%d" = 1 ] || exit 1; fi\nprintf sif > "$2"\n' % (build_ok, pull_ok))
    (bindir / "apptainer").chmod(0o755)
    env = dict(os.environ, SCR=str(scr), SIF=str(sif), IMAGE_REF=image_ref, STEP_NAME=step, SLURM_JOB_ID="1",
               PATH=str(bindir) + os.pathsep + os.environ["PATH"])
    if tar:
        env["IMAGE_STAGED"] = str(staged)
    script = "\n".join(['fail(){ exit "$1"; }'] + _image_block())
    subprocess.run(["bash", "-c", script], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    calls = (scr / "calls").read_text() if (scr / "calls").exists() else ""
    return sif, staged, calls


def test_a_built_sif_is_labelled_with_its_image_repository_and_step(tmp_path):
    sif, staged, calls = _run_image_block(tmp_path, "built")
    assert (sif.parent / "k.sif.lineage").read_text() == "reg.example/proj/app\ttrain\n"
    assert not staged.exists()
    assert not list(sif.parent.glob("*tmp*")), "the label is renamed into place, no temp file is left"


def test_the_label_drops_a_tag_but_keeps_a_registry_port(tmp_path):
    sif, _, _ = _run_image_block(tmp_path, "port", image_ref="localhost:5000/proj/app@sha256:" + "b" * 64)
    assert (sif.parent / "k.sif.lineage").read_text() == "localhost:5000/proj/app\ttrain\n"
    sif, _, _ = _run_image_block(tmp_path, "tag", image_ref="localhost:5000/proj/app:v7")
    assert (sif.parent / "k.sif.lineage").read_text() == "localhost:5000/proj/app\ttrain\n"


def test_a_pulled_sif_is_labelled_too(tmp_path):
    sif, _, calls = _run_image_block(tmp_path, "pulled", tar=False)
    assert calls.startswith("pull ") and (sif.parent / "k.sif.lineage").exists()


def test_the_staged_tar_goes_even_when_the_sif_was_already_cached(tmp_path):
    """Live 2026-11-10: a job that found the SIF built never reached the old deletion, so every re-staged tar stayed."""
    sif, staged, calls = _run_image_block(tmp_path, "cached", sif_cached=True)
    assert calls == "", "no build, no pull"
    assert not staged.exists() and sif.read_bytes() == b"old sif"
    assert not (sif.parent / "k.sif.lineage").exists(), "an existing SIF keeps its label (or the lack of one)"


def test_with_no_sif_after_a_failed_build_and_pull_the_tar_stays_for_the_next_try(tmp_path):
    sif, staged, _ = _run_image_block(tmp_path, "failed", build_ok=False, pull_ok=False)
    assert staged.exists() and not sif.exists() and not (sif.parent / "k.sif.lineage").exists()


def test_many_builds_leave_no_tar_behind_and_every_new_sif_a_label(tmp_path):
    """AC-65 at scale, the job's half: 120 submits of 12 images (each digest staged again by every submit that
    finds no SIF, as the runner does), then every one of them again with the SIF cached. No tar is left on
    scratch afterwards and every SIF built has its label. (The stager's half, the bounded size of the SIF cache
    across 400 builds, is test_stager.py::ImageGcTest in the stager's repository: the stager is not here.)"""
    scr = tmp_path / "scr"
    (scr / "sif-cache").mkdir(parents=True)
    (scr / "images").mkdir()
    bindir = scr / "bin"
    bindir.mkdir()
    (bindir / "apptainer").write_text('#!/bin/bash\nif [ "$1" = build ] || [ "$1" = pull ]; then printf sif > "$2"; fi\n')
    (bindir / "apptainer").chmod(0o755)
    block = "\n".join(['fail(){ exit "$1"; }'] + _image_block())
    for rnd in range(10):
        for n in range(12):
            key = "%016x" % (n + 1)
            tar = scr / "images" / (key + ".tar")
            tar.write_bytes(b"t" * 1000)
            env = dict(os.environ, SCR=str(scr), SIF=str(scr / "sif-cache" / (key + ".sif")), SLURM_JOB_ID=str(rnd),
                       IMAGE_REF="reg.example/proj/app%d@sha256:%s" % (n % 4, "%064x" % n), STEP_NAME="train",
                       IMAGE_STAGED=str(tar), PATH=str(bindir) + os.pathsep + os.environ["PATH"])
            subprocess.run(["bash", "-c", block], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    assert list((scr / "images").iterdir()) == [], "no tar survives a job that found or built the SIF"
    sifs = sorted(p.name for p in (scr / "sif-cache").glob("*.sif"))
    assert len(sifs) == 12 and all((scr / "sif-cache" / (s + ".lineage")).exists() for s in sifs)
