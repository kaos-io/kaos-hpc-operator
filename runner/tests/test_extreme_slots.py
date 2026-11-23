"""Extreme cases of the shared-node slot manager (docs/extreme-cases.md, section 4): a short-lived host (the
30-minute test lane), a guest whose token ends before its step could, a host that is killed without a chance
to hand its guests back. Real slot manager process against the fake Slurm of test_slot_manager.py.
"""
import json

from test_slot_manager import SCOPE, Node, _job


def _guest(n, step_min, limit="2:00:00"):
    body = "export KAOS_STEP_LIMIT_MIN=%d\n" % step_min
    return "10%d" % n, _job("kaos-%s-wf%d-train" % (SCOPE, n), body + "exit 0\n",
                            comment="kaos-commit=bbbbbbb%d" % n, limit=limit)


def _holds(node, job):
    return [c for c in json.loads(node.state.read_text())["calls"] if c[:2] == ["scontrol", "hold"] and c[2] == job]


def test_a_30_minute_test_lane_host_takes_only_the_guests_that_end_before_it_does(tmp_path):
    """The host ends in 30 min; a guest must end 5 min before that (MARGIN_S). 15 min fits; 26, 60 and 180 do not
    and are skipped for good without ever being held."""
    guests = dict([_guest(1, 15), _guest(2, 26), _guest(3, 60), _guest(4, 180)])
    node = Node(tmp_path, guests, own_body="sleep 4\nexit 0\n", host_secs=1800, extra_env={"KAOS_IDLE_S": "2"})
    rc, log = node.finish()
    assert rc == 0, log
    jobs = node.jobs()
    assert jobs["101"]["comment"] == "kaos-commit=bbbbbbb1 kaos-state=done rc=0 host=100", log
    for j in ("102", "103", "104"):
        assert jobs[j]["state"] == "PENDING" and not jobs[j]["held"], j
        assert _holds(node, j) == [], "%s does not fit the host's remaining time: never held" % j
        assert "kaos-state" not in jobs[j]["comment"]


def test_a_host_killed_hard_leaves_its_guest_held_for_its_runner_to_find(tmp_path):
    """SIGKILL (node failure, OOM-killer on the manager): no TERM handler runs, nothing is handed back. The guest
    stays held and marked taken; the guest's own runner detects the dead host (slurm.host_gone) and starts the
    job over (test_core / test_runner)."""
    guests = dict([_guest(1, 10)])
    guests["101"]["script"] = guests["101"]["script"].replace("exit 0", "sleep 60")
    node = Node(tmp_path, guests, own_body="sleep 60\n")
    assert node.wait_for(lambda j: "kaos-state=taken" in j["101"]["comment"] and j["101"]["held"])
    node.proc.kill()
    node.finish()
    j = node.jobs()["101"]
    assert j["state"] == "PENDING" and j["held"] and "kaos-state=taken host=100" in j["comment"]


def test_a_term_while_a_guest_is_being_taken_still_hands_it_back(tmp_path):
    """F-7: the TERM arrives between writing `taken` and registering the guest. It is held until the guest is
    registered, so the guest goes back to the queue instead of staying held forever. Repeated to catch the window."""
    import signal as sig
    for i in range(6):
        d = tmp_path / str(i)
        d.mkdir()
        guests = dict([_guest(1, 10)])
        guests["101"]["script"] = guests["101"]["script"].replace("exit 0", "sleep 60")
        node = Node(d, guests, own_body="sleep 60\n")
        # the comment is written first: TERM as soon as it shows, the worst moment
        assert node.wait_for(lambda j: "kaos-state=taken" in j["101"]["comment"])
        node.proc.send_signal(sig.SIGTERM)
        node.finish()
        j = node.jobs()["101"]
        assert j["state"] == "PENDING" and not j["held"] and "requeued" in j["comment"], (i, j)
