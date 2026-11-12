"""The step contract on the site (docs/step-contract.md; PRD kaos#1911 F-04): the result's check, the job's
hand-back program, and the whole job script run in bash against fakes, alone on its node and as a step on a
shared node: the step receives its context unchanged, finds each input as a folder named after it, and its
result comes back or the job fails.

The site job is built by the runner's own code (Runner.site_job), so what is run here is what is submitted.
"""
import base64
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import helpers
import pytest
from test_jobscript import FakeLakeFS

from kaos_hpc_runner import payloads, result
from kaos_hpc_runner import runner as runmod

REPO_ROOT = Path(__file__).resolve().parents[2]
WF, STEP = helpers.BASE_ENV["WF_UID"], helpers.BASE_ENV["STEP_NAME"]
BRANCH = "hpc-%s-%s" % (WF, STEP)


@pytest.fixture
def lakefs():
    srv = FakeLakeFS()
    yield srv
    srv.httpd.shutdown()


def reference_wire():
    """The contract's reference writer (hack/contract-checks/wire.py)."""
    spec = importlib.util.spec_from_file_location("contract_wire", str(REPO_ROOT / "hack" / "contract-checks" / "wire.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------- result.check

@pytest.mark.parametrize("name", ["result-ok", "result-model"])
def test_a_fixture_result_is_published_as_its_wire_form(name):
    wire = helpers.fixture(name + ".wire")
    assert result.check(helpers.fixture(name + ".json").encode()) == wire, "from any JSON spelling"
    assert result.check(wire.encode()) == wire, "and the wire form is a fixed point"
    assert result.cost(wire) <= result.RESULT_CAP


def test_the_runner_writes_the_wire_form_the_contract_defines():
    ref = reference_wire()
    for f in sorted(helpers.FIXTURES.glob("*.json")):
        if f.name == "hpc-job.json":
            continue
        value = json.loads(f.read_text())
        assert result.wire(value) == ref.dumps(value) == f.with_suffix(".wire").read_text(), f.name
        assert result.cost(result.wire(value)) == ref.cost(ref.dumps(value)), f.name
    assert (result.RESULT_CAP, json.loads(result.ABSENT)) == (ref.RESULT_CAP, ref.ABSENT)


def test_the_wire_form_escapes_what_a_hop_could_change_and_keeps_numbers_as_written():
    raw = b'{"version":1,"status":"ok","values":{"k{}":"{{=1+1}} a&b <c> \\u00e9 \\" \\\\","lr":1e-05,"r":1.0,"n":12345678901234567890,"z":-0.0},"reads":[],"writes":[]}'
    text = result.check(raw)
    assert text == ('{"version":1,"status":"ok","values":{"k\\u007b\\u007d":"\\u007b\\u007b=1+1\\u007d\\u007d a\\u0026b '
                    '\\u003cc\\u003e \\u00e9 \\" \\\\","lr":1e-05,"r":1.0,"n":12345678901234567890,"z":-0.0},"reads":[],"writes":[]}')
    assert json.loads(text) == json.loads(raw.decode())
    assert "{{" not in text and all(ord(c) < 128 for c in text)
    # The cost is the length as a JSON string (as Go's encoder writes it): quotes and backslashes count twice.
    assert result.cost(text) == len(json.dumps(text)) - 2 == len(text) + text.count('"') + text.count("\\")


def test_the_fields_come_out_in_the_contracts_order_whatever_order_the_step_wrote():
    """Wire form, rule 6. A step (or a JSON library that sorts keys) may write them in any order."""
    want = json.loads(helpers.fixture("result-model.json"))
    scrambled = json.dumps(want, sort_keys=True, indent=1).encode()
    assert list(json.loads(scrambled)) != list(want)
    assert result.check(scrambled) == helpers.fixture("result-model.wire")
    extra = dict(want, later="a field of a later contract")
    assert list(json.loads(result.check(json.dumps(extra, sort_keys=True).encode()))) == list(want) + ["later"], \
        "a field this runner does not know is kept, after the ones it knows"


def test_a_result_at_the_cap_passes_and_one_byte_over_does_not():
    def with_blob(n):
        return json.dumps({"version": 1, "status": "ok", "values": {"blob": "x" * n}, "reads": [], "writes": []},
                          separators=(",", ":")).encode()
    room = result.RESULT_CAP - result.cost(with_blob(0).decode())
    assert result.cost(result.check(with_blob(room))) == result.RESULT_CAP
    with pytest.raises(result.InvalidResult, match="costs 8193 bytes, more than the 8192"):
        result.check(with_blob(room + 1))


# ---------------------------------------------------------------- payload/handback.py

def _handback(tmp_path, lakefs, write=None, **extra):
    out = tmp_path / "work" / "output"
    out.mkdir(parents=True, exist_ok=True)
    if write is not None:
        (out / "kaos-result.json").write_bytes(write)
    env = dict(os.environ, LAKEFS_ENDPOINT=lakefs.url, LAKEFS_BEARER_TOKEN="wallet", KAOS_OUTPUTS_REPO="outputs-yolo",
               WF_UID=WF, STEP_NAME=STEP, KAOS_JOB_ID="5001", KAOS_WORK=str(tmp_path / "work"), KAOS_HANDBACK_RETRY_SLEEP="0")
    env.update(extra)
    env = {k: v for k, v in env.items() if v is not None}
    return subprocess.run([sys.executable, "-c", payloads.HANDBACK], env=env, capture_output=True, text=True)


def _handed(lakefs, jid="5001"):
    prefix = "hpc-outputs/%s/%s/%s/" % (WF, STEP, jid)
    return {p[len(prefix):]: d for (b, p), d in lakefs.objects.items() if b == BRANCH and p.startswith(prefix)}


def test_the_result_file_is_handed_back_unread_under_the_site_jobs_id(tmp_path, lakefs):
    raw = b'\xff not even text {'      # the job does not judge the file: the runner does
    p = _handback(tmp_path, lakefs, write=raw)
    assert p.returncode == 0, p.stdout + p.stderr
    assert _handed(lakefs) == {"kaos-result.json": raw} and BRANCH in lakefs.branches
    assert "result hand-back: kaos-result.json, %d bytes" % len(raw) in p.stdout


def test_a_step_that_wrote_no_file_hands_back_the_record_that_there_is_none(tmp_path, lakefs):
    p = _handback(tmp_path, lakefs)
    assert p.returncode == 0 and list(_handed(lakefs)) == ["kaos-result.absent"]
    assert "the step wrote no result" in p.stdout


def test_a_requeued_job_replaces_what_its_earlier_run_handed_back(tmp_path, lakefs):
    assert _handback(tmp_path, lakefs, write=b'{"old":1}').returncode == 0
    (tmp_path / "work" / "output" / "kaos-result.json").unlink()          # the second run wrote none
    assert _handback(tmp_path, lakefs).returncode == 0
    assert list(_handed(lakefs)) == ["kaos-result.absent"], "the earlier run's result is gone"
    assert _handback(tmp_path, lakefs, write=b'{"new":1}').returncode == 0
    assert _handed(lakefs) == {"kaos-result.json": b'{"new":1}'}


@pytest.mark.parametrize("case, said", [
    ("upload refused", "lakeFS did not take the step's result"),
    ("a folder", "is not a regular file"),
    ("a link", "is not a regular file"),
    ("unreadable", "cannot be read"),
    ("far too large", "a result may cost 8 KiB"),
    ("no token", "the job has no LAKEFS_BEARER_TOKEN"),
    ("no repository", "the job has no KAOS_OUTPUTS_REPO"),
])
def test_a_hand_back_that_fails_ends_non_zero_and_says_why(tmp_path, lakefs, case, said):
    out = tmp_path / "work" / "output"
    out.mkdir(parents=True)
    f, write, extra = out / "kaos-result.json", None, {}
    if case == "upload refused":
        lakefs.fail_uploads, write = 99, b"{}"
    elif case == "a folder":
        f.mkdir()
    elif case == "a link":
        (tmp_path / "elsewhere").write_text("{}")
        f.symlink_to(tmp_path / "elsewhere")
    elif case == "unreadable":
        f.write_bytes(b"{}")
        f.chmod(0)
        if os.access(str(f), os.R_OK):
            pytest.skip("running as a user that reads every file")
    elif case == "far too large":
        write = b"x" * (64 * 1024 + 1)
    elif case == "no token":
        extra, write = {"LAKEFS_BEARER_TOKEN": None}, b"{}"
    elif case == "no repository":
        extra, write = {"KAOS_OUTPUTS_REPO": None}, b"{}"
    p = _handback(tmp_path, lakefs, write=write, **extra)
    assert p.returncode != 0 and ("result hand-back: " in p.stdout and said in p.stdout), p.stdout + p.stderr
    assert _handed(lakefs) == {}, "nothing stands in for the result"


def test_an_upload_that_blips_is_tried_again(tmp_path, lakefs):
    lakefs.fail_uploads = 2
    assert _handback(tmp_path, lakefs, write=b"{}").returncode == 0 and _handed(lakefs) == {"kaos-result.json": b"{}"}


# ---------------------------------------------------------------- the whole job script, run

# A stand-in for Apptainer. `exec` builds the step's view of the file system under $FAKE_ROOT from its -B
# binds (a link per bind), passes on APPTAINERENV_ variables without the prefix as Apptainer does (paths under
# the fake root), and runs the command. pull/build leave a file.
APPTAINER = r'''#!/bin/bash
case "$1" in
  pull|build) : > "$2"; exit 0 ;;
  exec) shift ;;
  *) exit 0 ;;
esac
ROOT=$FAKE_ROOT; rm -rf "$ROOT"; mkdir -p "$ROOT"
while [ $# -gt 0 ] && [ "${1%.sif}" = "$1" ]; do
  if [ "$1" = "-B" ]; then
    shift; src=${1%%:*}; rest=${1#*:}; dst=${rest%%:*}
    if [ -L "$ROOT$(dirname "$dst")" ]; then
      # A bind onto a folder of an earlier bind: this stand-in works on a copy of that outer folder.
      outer=$(readlink "$ROOT$(dirname "$dst")"); rm "$ROOT$(dirname "$dst")"; cp -r "$outer" "$ROOT$(dirname "$dst")"
      NESTED="$NESTED $(dirname "$dst")"
    fi
    case " $NESTED " in *" $(dirname "$dst") "*)
      # Apptainer needs the folder it binds onto to exist in the outer bind.
      rmdir "$ROOT$dst" || { echo "fake apptainer: no empty mount point $dst in the outer bind" >&2; exit 97; } ;;
    esac
    mkdir -p "$ROOT$(dirname "$dst")"; ln -s "$src" "$ROOT$dst"
  fi
  shift
done
shift
for v in $(compgen -v APPTAINERENV_); do
  val=${!v}; case "$val" in /*) val=$ROOT$val ;; esac
  export "${v#APPTAINERENV_}=$val"
done
exec "$@"
'''

# The step: what kaos-lib does with what it is given: it reads KAOS_STEP_CONTEXT, finds each input under
# $KAOS_STAGED_ROOT by the record <input>.kaos-ready beside the folder <input>/ (repository, commit, owner absent
# or null, "full": true), and writes /work/output/kaos-result.json. It reports every file it sees in each folder.
STEP_PY = r'''
import hashlib, json, os, pathlib, sys
ctx = os.environ.get("KAOS_STEP_CONTEXT", "")
root = os.environ.get("KAOS_STAGED_ROOT")
found = {}
if root and pathlib.Path(root).is_dir():
    for record in sorted(pathlib.Path(root).glob("*.kaos-ready")):
        staged = record.with_name(record.name[:-len(".kaos-ready")])
        try:
            info = json.loads(record.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(info, dict) and info.get("full") is True and staged.is_dir():
            files = sorted(str(f.relative_to(staged)) for f in staged.rglob("*") if f.is_file())
            found[staged.name] = {"repo": info.get("repo"), "commit": info.get("commit"), "owner": info.get("owner") or None,
                                  "files": files}
    found["(root)"] = sorted(p.name for p in pathlib.Path(root).iterdir())
saw = {"bytes": len(ctx.encode()), "sha256": hashlib.sha256(ctx.encode()).hexdigest(), "inputs": found,
       "old": [k for k in ("KUBECORE_STAGED_DIR", "KUBECORE_DATASET_DIR", "KAOS_LIB_LOCK", "DATASET_STAGED") if k in os.environ]}
pathlib.Path(os.environ["SAW"]).write_text(json.dumps(saw))
mode = os.environ.get("STEP_DOES", "result")
out = pathlib.Path(os.environ["FAKE_ROOT"]) / "work" / "output" / "kaos-result.json"
if mode in ("result", "result-then-fail"):
    out.write_text(os.environ["STEP_RESULT"])
if mode == "folder":
    out.mkdir()
sys.exit(3 if mode == "result-then-fail" else 0)
'''

LABELS, EMB = "2" * 64, "3" * 64
TWO_INPUTS = [{"name": "labels", "repo": "dataset-labels", "ref": "v3", "commit": LABELS},
              {"name": "embeddings", "repo": "dataset-embeddings", "commit": EMB}]


class Site:
    """One site job, built by the runner and run on a stand-in node."""

    def __init__(self, tmp_path, lakefs, shared, context, inputs=(), does="result", monkeypatch=None, drop=()):
        self.tmp, self.lakefs = tmp_path, lakefs
        self.scratch, self.home, bindir = tmp_path / "scratch", tmp_path / "home", tmp_path / "bin"
        for d in (self.scratch, self.home, bindir):
            d.mkdir()
        (bindir / "apptainer").write_text(APPTAINER)
        for tool in ("squeue", "scontrol"):
            (bindir / tool).write_text("#!/bin/bash\nexit 0\n")
        for tool in ("apptainer", "squeue", "scontrol"):
            (bindir / tool).chmod(0o755)
        (tmp_path / "step.py").write_text(STEP_PY)
        over = dict(HPC_SCRATCH=str(self.scratch), HPC_HOME=str(self.home), KAOS_STEP_CONTEXT=context,
                    HPC_INPUTS=json.dumps(list(inputs)), LAKEFS_EXTERNAL_URL=lakefs.url,
                    STEP_COMMAND=json.dumps([sys.executable, str(tmp_path / "step.py")]))
        if shared:
            over.update(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5")
        self.cfg = helpers.settings(**over)
        self.cfg.check_job()
        monkeypatch.setattr(runmod.wallet, "fetch_workdir", lambda image, token: "")
        self.runner = runmod.Runner(self.cfg, slurm=object(), stager=object(), reporter=object(), logs=object(),
                                    mint=lambda: "wallet")
        self.shared, self.does, self.drop = shared, does, drop
        self.saw = tmp_path / "saw.json"

    def resident(self, inp, files, marker=True):
        """A copy the site's staging tool made from a login node, with the record it writes. The dataset's files
        are under data/, the record and the lease note beside it, and the path the runner is given is data/."""
        d = self.scratch / "kaos" / "p1" / "datasets" / inp["repo"] / inp["commit"]
        (d / "data").mkdir(parents=True, exist_ok=True)
        for rel, body in files.items():
            (d / "data" / rel).parent.mkdir(parents=True, exist_ok=True)
            (d / "data" / rel).write_bytes(body)
        if marker:
            (d / ".kaos-ready").write_text(json.dumps({"repo": inp["repo"], "commit": inp["commit"], "ref": inp.get("ref"),
                                                       "prefix": "", "full": True, "layout": "data", "base": None,
                                                       "files": len(files)}))
        self.runner.staged[inp["name"]] = str(d / "data")
        return d

    def run(self, jid="5001", result_text=None):
        batch, env, commits, limit = self.runner.site_job("", "")
        self.batch, self.env = batch, env
        node = dict(e.split("=", 1) for e in env)
        node.update(PATH=str(self.tmp / "bin") + os.pathsep + os.environ["PATH"], SLURM_JOB_ID=jid, HOME=str(self.home),
                    FAKE_ROOT=str(self.tmp / "rootfs"), SAW=str(self.saw), STEP_DOES=self.does,
                    STEP_RESULT=result_text or helpers.fixture("result-model.wire"), KAOS_HANDBACK_RETRY_SLEEP="0",
                    KAOS_COMMENT_RETRY_SLEEP="0")
        if self.shared:
            node["KAOS_IN_STEP"] = "1"        # on a shared node the host runs each step's script as a job step
        script = self.tmp / "job.sh"
        script.write_text(batch)
        if self.drop:                         # a job script handed something less than the runner built
            script.write_text("\n".join(l for l in batch.split("\n") if not l.startswith(tuple("export %s=" % d for d in self.drop))))
            for d in self.drop:
                node.pop(d, None)
        p = subprocess.run(["bash", str(script)], cwd=str(self.home), env=node, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=120)
        self.out = p.stdout.decode(errors="replace")
        return p.returncode

    def step_saw(self):
        return json.loads(self.saw.read_text())


MODES = pytest.mark.parametrize("shared", [False, True], ids=["alone", "shared-node"])


@MODES
@pytest.mark.parametrize("name", ["context-escapes", "context-one-upstream", "context-skipped-upstream"])
def test_the_step_receives_its_context_unchanged(tmp_path, lakefs, monkeypatch, shared, name):
    """Quotes, a backslash, braces and \\u escapes: neither the Slurm environment nor the export line of the
    job script may alter one byte."""
    wire = helpers.fixture(name + ".wire")
    site = Site(tmp_path, lakefs, shared, wire, monkeypatch=monkeypatch)
    assert site.run() == 0, site.out
    saw = site.step_saw()
    assert (saw["bytes"], saw["sha256"]) == (len(wire), hashlib.sha256(wire.encode()).hexdigest())
    in_env = any(e.startswith("KAOS_STEP_CONTEXT=") for e in site.env)
    assert in_env != shared, "alone: in the Slurm environment; shared node: in the script, never in the environment"
    assert ("export KAOS_STEP_CONTEXT=" in site.batch) == shared
    assert saw["old"] == [], "nothing of the contract of before reaches the step"


@MODES
def test_each_input_is_one_folder_named_after_it_under_one_root_whichever_way_it_was_staged(tmp_path, lakefs, monkeypatch, shared):
    """labels was staged from a login node; embeddings was not (the kept in-job fallback downloads it). The step
    finds both the same way."""
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), inputs=TWO_INPUTS, monkeypatch=monkeypatch)
    site.resident(TWO_INPUTS[0], {"train/a.csv": b"a", "meta.json": b"{}"})
    lakefs.commits[EMB] = {"part-0.npy": b"0000", "deep/part-1.npy": b"1111", ".kaos/schema.json": b"{}"}
    assert site.run() == 0, site.out
    saw = site.step_saw()["inputs"]
    assert saw.pop("(root)") == ["embeddings", "embeddings.kaos-ready", "labels", "labels.kaos-ready"], \
        "one folder per input and its record beside it, nothing else"
    assert {k: (v["repo"], v["commit"], v["owner"]) for k, v in saw.items()} == {
        "labels": ("dataset-labels", LABELS, None), "embeddings": ("dataset-embeddings", EMB, None)}
    for name, files in (("labels", ["meta.json", "train/a.csv"]),
                        ("embeddings", [".kaos/schema.json", "deep/part-1.npy", "part-0.npy"])):
        assert saw[name]["files"] == files, "the whole tree of the commit, and nothing of the platform's"
    assert not (site.scratch / "kaos-tmp" / "in-5001").exists(), "the job's own folder of inputs goes with the step"
    assert "input labels: resident copy" in site.out and "stage-in: downloaded 3 of 3" in site.out
    assert "KAOS_STAGED_ROOT=/kaos/inputs" in site.batch and "KUBECORE_STAGED_DIR" not in site.batch
    # Both commits are leased, so the staging tool evicts neither while the job queues or runs.
    leases = next(e for e in site.env + site.batch.split("\n") if "KAOS_LEASES=" in e)
    assert "kaos-commit=" + LABELS in leases and "kaos-commit=" + EMB in leases


@MODES
def test_a_staged_copy_without_its_record_is_not_served_the_job_downloads_the_commit(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-no-upstream.wire"), inputs=TWO_INPUTS[:1], monkeypatch=monkeypatch)
    half = site.resident(TWO_INPUTS[0], {"train/a.csv": b"a"}, marker=False)      # a copy that was never completed
    lakefs.commits[LABELS] = {"train/a.csv": b"a", "train/b.csv": b"b"}
    assert site.run() == 0, site.out
    assert site.step_saw()["inputs"]["labels"]["files"] == ["train/a.csv", "train/b.csv"]
    assert "not ready - downloading in-job" in site.out and not (half / "data" / "train" / "b.csv").exists()


@MODES
def test_an_input_that_cannot_be_staged_ends_the_job_232_before_the_step(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-no-upstream.wire"), inputs=TWO_INPUTS[:1], monkeypatch=monkeypatch)
    lakefs.commits[LABELS] = {"train/a.csv": b"aaaa"}
    lakefs.truncate = True                      # the download comes one byte short: the copy is not the commit
    assert site.run() == 232, site.out
    assert "downloaded 3 bytes" in site.out
    assert not site.saw.exists(), "the step never starts without its data"


@MODES
def test_a_result_is_handed_back_and_the_runner_publishes_it(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), monkeypatch=monkeypatch)
    assert site.run(jid="5001") == 0, site.out
    assert _handed(lakefs) == {"kaos-result.json": helpers.fixture("result-model.wire").encode()}
    assert result.check(_handed(lakefs)["kaos-result.json"]) == helpers.fixture("result-model.wire")
    assert not (site.scratch / "kaos-work" / "5001").exists(), "the step's scratch goes once its result is handed back"


@MODES
def test_a_step_that_ends_well_without_a_result_hands_back_the_record_of_none(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), does="nothing", monkeypatch=monkeypatch)
    assert site.run() == 0, site.out
    assert list(_handed(lakefs)) == ["kaos-result.absent"]


@MODES
def test_a_result_left_by_an_earlier_run_under_the_same_job_id_is_never_handed_back(tmp_path, lakefs, monkeypatch, shared):
    """A requeued job runs again with the same site job id and the same work folder."""
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), does="nothing", monkeypatch=monkeypatch)
    left = site.scratch / "kaos-work" / "5001" / "output"
    left.mkdir(parents=True)
    (left / "kaos-result.json").write_text(helpers.fixture("result-ok.wire"))
    assert site.run(jid="5001") == 0, site.out
    assert list(_handed(lakefs)) == ["kaos-result.absent"], "this run wrote no result: the earlier run's is not its"


# PRD kaos#1911 X-2, the first two silent paths. Before: the job script printed "stage-out failed (rc=0)" and
# exited 0 (jobscript.py), and ran the upload only when STEP_OUTPUTS, STAGEOUT_B64 and LAKEFS_BEARER_TOKEN were
# all set, exiting 0 without it; the upload skipped a file it could not find (payload/stageout.py). The job
# was COMPLETED and the next step read "{}".
@MODES
@pytest.mark.parametrize("case, said", [
    ("lakeFS refuses the upload", "result hand-back: lakeFS did not take the step's result"),
    ("the result file is a folder", "result hand-back: the result file"),
    ("the job has no token", "result hand-back: the job has no LAKEFS_BEARER_TOKEN"),
    ("the job has no hand-back program", "result hand-back: the job has no hand-back program"),
])
def test_a_hand_back_that_fails_fails_the_job_243_and_the_log_says_why(tmp_path, lakefs, monkeypatch, shared, case, said):
    does, drop = "result", ()
    if case == "lakeFS refuses the upload":
        lakefs.fail_uploads = 99
    elif case == "the result file is a folder":
        does = "folder"      # the step itself makes it (the script clears a result FILE left behind before the step)
    elif case == "the job has no token":
        drop = ("LAKEFS_BEARER_TOKEN",)
    else:
        drop = ("HANDBACK_B64",)
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), does=does, drop=drop, monkeypatch=monkeypatch)
    assert site.run() == 243, site.out
    assert said in site.out
    assert _handed(lakefs) == {}, "nothing was handed back in its place"
    assert (site.scratch / "kaos-work" / "5001").exists(), "a failed job keeps its work folder"


@MODES
def test_a_step_that_fails_hands_nothing_back_whatever_it_wrote(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), does="result-then-fail", monkeypatch=monkeypatch)
    assert site.run() == 3, site.out
    assert _handed(lakefs) == {} and "result hand-back" not in site.out


# Both caches keep the dataset's files under <copy>/data/ and their own markers (.kaos-ready and .kaos-used in the
# staging tool's, .kaos-complete and .kaos-parts/ in the in-job one) beside it: the job binds data/.
@MODES
def test_the_step_sees_only_the_datasets_files_in_an_input_folder(tmp_path, lakefs, monkeypatch, shared):
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), inputs=TWO_INPUTS, monkeypatch=monkeypatch)
    site.resident(TWO_INPUTS[0], {"train/a.csv": b"a"})
    lakefs.commits[EMB] = {"part-0.npy": b"0000"}
    assert site.run() == 0, site.out
    saw = site.step_saw()["inputs"]
    assert (saw["labels"]["files"], saw["embeddings"]["files"]) == (["train/a.csv"], ["part-0.npy"])


@MODES
def test_a_copy_of_the_older_layout_is_served_by_neither_path(tmp_path, lakefs, monkeypatch, shared):
    """Copies from before the data/ layout have their files directly in the folder. The login-node copy is handed
    to the job by the path an older staging tool reported (the folder itself, its markers inside); the in-job
    copy is complete by an empty marker. The job serves neither: the step sees the commit's files, no others."""
    site = Site(tmp_path, lakefs, shared, helpers.fixture("context-one-upstream.wire"), inputs=TWO_INPUTS, monkeypatch=monkeypatch)
    old = site.scratch / "kaos" / "p1" / "datasets" / "dataset-labels" / LABELS
    old.mkdir(parents=True)
    (old / "stale-a.csv").write_bytes(b"stale")
    (old / ".kaos-ready").write_text(json.dumps({"repo": "dataset-labels", "commit": LABELS, "full": True}))
    site.runner.staged["labels"] = str(old)                 # what an older staging tool reported as the path
    in_job = site.scratch / "data-cache" / "dataset-embeddings" / EMB
    in_job.mkdir(parents=True)
    (in_job / "stale-b.npy").write_bytes(b"stale")
    (in_job / ".kaos-complete").write_text("")
    lakefs.commits[LABELS] = {"train/a.csv": b"a"}
    lakefs.commits[EMB] = {"part-0.npy": b"0000"}
    assert site.run() == 0, site.out
    saw = site.step_saw()["inputs"]
    assert (saw["labels"]["files"], saw["embeddings"]["files"]) == (["train/a.csv"], ["part-0.npy"])
    assert "input labels: resident copy" not in site.out and "not ready - downloading in-job" in site.out


def test_the_payloads_run_on_the_nodes_python():
    """The node's system python3 is 3.6: no f-strings with =, no walrus, no capture_output in the payloads."""
    for name in ("HANDBACK", "STAGEIN"):
        src = getattr(payloads, name)
        compile(src, name, "exec")
        assert ":=" not in src and "capture_output" not in src and base64.b64decode(payloads.b64(src)).decode() == src


def test_no_dollar_is_ever_written_in_a_result():
    """Kubernetes expands `$(NAME)` and `$$` in an env value (docs/step-contract.md, section 6, rule 7)."""
    raw = b'{"version":1,"status":"ok","values":{"$(HOME)":"$$ $(X) $"},"reads":[],"writes":[]}'
    text = result.check(raw)
    assert "$" not in text
    assert json.loads(text) == json.loads(raw.decode())
