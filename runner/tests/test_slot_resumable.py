"""AC-73 (reverses PRD-HPC-1461 D-44's gate): resumable steps share a node. The slot manager sends USR1 to each
resumable step itself, its lead before the end of its own limit or of the host job, whichever comes first
(a guest is a step of the host's job, so Slurm's warning signal never reaches it). Real slot manager and real
step processes against the fake Slurm of test_slot_manager.py; a lead's unit and the margin are shortened
(patch) so the tests run in seconds.
"""
import json
import time

from test_slot_manager import SCOPE, Node, _job

# Declared leads are minutes; here one "minute" of lead is 4 s, and a step must end 1 s before its host (not 300 s).
SHORT = {"LEAD_UNIT_S = 60": "LEAD_UNIT_S = 4", "MARGIN_S = 300": "MARGIN_S = 1"}

# A step that checkpoints when asked: USR1 stamps the time and exits 85, as kaos-lib's handler does.
SAVES = ("export KAOS_RESUMABLE_LEAD_MIN=%d\nexport KAOS_STEP_LIMIT_MIN=%d\n"
         "trap 'date +%%s.%%N > $OUT/%s.sig; echo saved > $OUT/%s.ckpt; exit 85' USR1\n"
         "date +%%s.%%N > $OUT/%s.start\nwhile :; do sleep 0.2; done\n")


def _saver(name, lead, limit_min=1):
    return SAVES % (lead, limit_min, name, name, name)


def _stamp(node, name, kind):
    return float((node.out / ("%s.%s" % (name, kind))).read_text())


def test_each_resumable_step_is_signalled_at_its_own_lead_and_checkpoints(tmp_path):
    """Two resumable guests and a resumable host, one-minute limits: leads of 2, 1 and 3 give signals at
    limit - lead, in that order, each step checkpoints and exits 85, and the host's job is not harmed."""
    guests = {"101": _job("kaos-%s-wf1-train" % SCOPE, _saver("g1", 2), comment="kaos-commit=bbbbbbb2"),
              "102": _job("kaos-%s-wf2-train" % SCOPE, _saver("g2", 1), comment="kaos-commit=bbbbbbb3")}
    own = _saver("host", 3)
    node = Node(tmp_path, guests, own_body=own, patch=SHORT,
                extra_env={"KAOS_RESUMABLE_LEAD_MIN": "3", "KAOS_STEP_LIMIT_MIN": "1"})
    rc, log = node.finish(timeout=150)
    for name, lead_s in (("g1", 8), ("g2", 4), ("host", 12)):
        # srun --time is whole minutes: the step's limit is 60 s, its signal is the lead before that
        assert abs(_stamp(node, name, "sig") - _stamp(node, name, "start") - (60 - lead_s)) < 4, (name, log)
        assert (node.out / (name + ".ckpt")).read_text() == "saved\n"
    assert _stamp(node, "host", "sig") < _stamp(node, "g1", "sig") < _stamp(node, "g2", "sig"), "leads 3, 2, 1: the longest lead first"
    jobs = node.jobs()
    assert jobs["101"]["comment"] == "kaos-commit=bbbbbbb2 kaos-state=done rc=85 host=100", log
    assert jobs["102"]["comment"] == "kaos-commit=bbbbbbb3 kaos-state=done rc=85 host=100", log
    assert rc == 85 and jobs["100"]["comment"] == "kaos-commit=aaaaaaa1 kaos-state=done rc=85", log
    assert "reason=TIMEOUT" not in json.dumps(jobs), "saved before the limit: not a timeout"
    srun = [a for c in json.loads(node.state.read_text())["calls"] if c[0] == "srun" for a in c]
    assert "--time=1" in srun and not any(a.startswith("--signal") for a in srun), \
        "the slot manager's own timer is the guarantee: srun is not asked to signal (a second USR1 would hit a step that already saves)"


def test_a_host_job_that_ends_first_signals_every_resumable_guest_in_time(tmp_path):
    """The host's job ends in 30 s; its guests' limits (10 min) are far beyond it. A resumable guest is admitted
    when its lead fits and is signalled at the host's end minus its own lead; both end before the host does."""
    guests = {"101": _job("kaos-%s-wf1-train" % SCOPE, _saver("g1", 2, 10), comment="kaos-commit=bbbbbbb2"),
              "102": _job("kaos-%s-wf2-train" % SCOPE, _saver("g2", 4, 10), comment="kaos-commit=bbbbbbb3")}
    t0 = time.time()
    node = Node(tmp_path, guests, own_body=_saver("host", 3, 10), host_secs=30, patch=SHORT,
                extra_env={"KAOS_RESUMABLE_LEAD_MIN": "3", "KAOS_STEP_LIMIT_MIN": "10"})
    rc, log = node.finish(timeout=60)
    host_end = t0 + 30
    for name, lead_s in (("g1", 8), ("g2", 16), ("host", 12)):
        sig = _stamp(node, name, "sig")
        assert sig <= host_end - lead_s + 3, (name, sig - host_end, log)
        assert sig >= host_end - lead_s - 3, "not earlier than its lead before the host's end (%s)" % name
        assert (node.out / (name + ".ckpt")).exists()
    jobs = node.jobs()
    assert jobs["101"]["comment"].endswith("kaos-state=done rc=85 host=100") and jobs["102"]["comment"].endswith("rc=85 host=100"), log


def test_a_step_that_is_not_resumable_is_never_signalled(tmp_path):
    """A guest without a lead gets nothing from the slot manager, however near the host's end."""
    quiet = ("export KAOS_STEP_LIMIT_MIN=1\ntrap 'echo got > $OUT/g3.sig; exit 85' USR1\n"
             "sleep 6\nexit 0\n")
    guests = {"101": _job("kaos-%s-wf3-train" % SCOPE, quiet, comment="kaos-commit=bbbbbbb4", limit="10:00")}
    node = Node(tmp_path, guests, own_body="sleep 8\nexit 0\n", host_secs=120, patch=SHORT)
    rc, log = node.finish(timeout=60)
    assert not (node.out / "g3.sig").exists(), log
    assert node.jobs()["101"]["comment"] == "kaos-commit=bbbbbbb4 kaos-state=done rc=0 host=100"


def test_a_resumable_guest_is_taken_when_its_lead_fits_and_a_plain_one_is_not(tmp_path):
    """Admission (AC-73): the host has 30 min left; a 180-minute guest does not fit, unless it is resumable and its
    lead plus the margin does. A 30-minute lead does not fit 30 minutes."""
    def guest(n, lead):
        body = "export KAOS_STEP_LIMIT_MIN=180\n" + ("export KAOS_RESUMABLE_LEAD_MIN=%d\n" % lead if lead else "") + "exit 0\n"
        return "10%d" % n, _job("kaos-%s-wf%d-train" % (SCOPE, n), body, comment="kaos-commit=bbbbbbb%d" % n, limit="3:00:00")
    guests = dict([guest(1, 10), guest(2, 0), guest(3, 30)])
    node = Node(tmp_path, guests, own_body="sleep 4\nexit 0\n", host_secs=1800, extra_env={"KAOS_IDLE_S": "2"})
    rc, log = node.finish()
    jobs = node.jobs()
    assert jobs["101"]["comment"] == "kaos-commit=bbbbbbb1 kaos-state=done rc=0 host=100", log
    for j in ("102", "103"):
        assert jobs[j]["state"] == "PENDING" and "kaos-state" not in jobs[j]["comment"], j


def test_a_guest_that_saved_as_its_host_goes_away_is_reported_not_requeued(tmp_path):
    """The host is cancelled and Slurm TERMs the steps first: a step that checkpoints on TERM and exits 85 has
    saved; its result (and the copy named in its comment) is kept, never thrown back to the queue."""
    import os
    import signal
    body = "export KAOS_STEP_LIMIT_MIN=10\ntrap 'exit 85' TERM\nwhile :; do sleep 0.2; done\n"
    guests = {"101": _job("kaos-%s-wf1-train" % SCOPE, body, comment="kaos-commit=bbbbbbb2")}
    node = Node(tmp_path, guests, own_body="sleep 60\n")
    assert node.wait_for(lambda j: "kaos-state=taken" in j["101"]["comment"] and j["101"]["held"])
    end = time.time() + 20
    while len(json.loads(node.state.read_text()).get("steps", {})) < 2 and time.time() < end:
        time.sleep(0.1)
    time.sleep(0.5)     # the step's trap is set
    data = json.loads(node.state.read_text())
    data["jobs"]["100"]["state"] = "CANCELLED"
    node.state.write_text(json.dumps(data))
    for step in data["steps"].values():
        os.killpg(step["pid"], signal.SIGTERM)
    node.finish()
    assert node.jobs()["101"]["comment"] == "kaos-commit=bbbbbbb2 kaos-state=done rc=85 host=100"


def test_a_host_whose_own_step_saved_takes_nothing_new_and_leaves_with_its_guests(tmp_path):
    """AC-73: the continuation of a step that saved is reported when its host job ends, so the job is not held open
    for more work: no new guest, and no idle wait once the last guest is gone (KAOS_IDLE_S would be 100 s)."""
    guests = {"101": _job("kaos-%s-wf1-train" % SCOPE, "export KAOS_STEP_LIMIT_MIN=10\nexit 0\n", comment="kaos-commit=bbbbbbb2")}
    t0 = time.time()
    node = Node(tmp_path, guests, own_body="exit 85\n", extra_env={"KAOS_IDLE_S": "100"})
    rc, log = node.finish(timeout=60)
    assert rc == 85 and time.time() - t0 < 30, log
    j = node.jobs()["101"]
    assert j["state"] == "PENDING" and "kaos-state" not in j["comment"], "left in the queue for another host or its own node"
