"""PRD-HPC-1461 D-70: the controller plans a shareable step as a host or a guest; the runner carries the plan out.

A guest is submitted deferred (BeginTime far ahead) so no free node can start it on its own, and its host takes
it. While it waits it reads the plan again; when the controller planned it again as a host the runner replaces
the deferred job by one that starts at once in the lane the plan names.
"""
import time

from kaos_hpc_runner import settings as smod
from test_runner import NAME, FakeSlurm, acc, rig  # noqa: F401  (rig is a fixture)

SHARING = dict(HPC_SHARING="on", HPC_SCOPE="a1b2c3d4e5", SLURM_TIME_LIMIT="20")


def begin(sub):
    return sub["job"]["begin_time"]["number"] - int(time.time())


def test_should_submit_a_guest_deferred_so_it_never_starts_on_a_node_of_its_own(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    sub = slurm.submitted[0]
    assert abs(begin(sub) - smod.GUEST_HOLD_S) < 30
    assert "reservation" not in sub["job"] and sub["job"]["qos"] == "default", "a guest holds no lane"
    assert "export KAOS_SLOTS_B64=" in sub["script"], "it is still a host-capable job if it ever starts"


def test_should_offer_a_host_for_the_usual_window_and_a_step_without_a_plan_too(rig):  # noqa: F811
    for role in ("host", None):
        slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
        rig.make(slurm, HPC_SHARE_ROLE=role, **SHARING)
        rig.runner.run()
        assert abs(begin(slurm.submitted[0]) - smod.OFFER_S) < 30


def test_should_submit_the_deferred_guest_as_a_host_when_the_plan_says_host(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0), acc("PENDING", start=0)], "5002": [acc("COMPLETED")]})
    plan = {"timeLimitMinutes": 20, "qos": "dev", "reservation": "gpudev",
            "share": {"role": "host", "jobLimitMinutes": 40}}
    rig.make(slurm, status={"placement": plan}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == ["5001"] and len(slurm.submitted) == 2
    first, second = slurm.submitted
    assert abs(begin(first) - smod.GUEST_HOLD_S) < 30
    assert abs(begin(second) - smod.OFFER_S) < 30, "now a host: the usual offer window, not the deferral"
    assert second["job"]["qos"] == "dev" and second["job"]["reservation"] == "gpudev"
    assert second["job"]["time_limit"] == 40, "the host's limit is the plan's"
    assert rig.runner.jid == 5002


def test_should_keep_waiting_while_the_plan_still_says_guest(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0), acc("PENDING", start=0), acc("COMPLETED")]})
    plan = {"timeLimitMinutes": 20, "share": {"role": "guest", "host": "ns/h1"}}
    rig.make(slurm, status={"placement": plan}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1


def test_should_not_replace_a_guest_a_host_already_took(rig):  # noqa: F811
    taken = acc("PENDING", start=0, comment="kaos-state=taken host=7001 slot=1 attempt=1")
    done = acc("CANCELLED", comment="kaos-state=done rc=0 host=7001 start=1762300800")
    slurm = FakeSlurm({"5001": [taken, done]}, live={"5001": {"state_reason": "JobHeldUser",
                                                              "comment": "kaos-state=taken host=7001 slot=1 attempt=1"}})
    plan = {"timeLimitMinutes": 20, "share": {"role": "host"}}
    rig.make(slurm, status={"placement": plan}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1, "taken: the host runs its step, nothing is replaced"


def test_should_keep_waiting_when_the_plan_cannot_be_read(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0), acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARE_ROLE="guest", **SHARING)
    rig.reporter.placement = lambda: None
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1


def test_should_never_read_the_plan_for_a_step_that_is_not_a_guest(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("PENDING", start=0), acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARE_ROLE="host", **SHARING)
    rig.reporter.placement = lambda: (_ for _ in ()).throw(AssertionError("read the plan"))
    rig.runner.run()
    assert len(slurm.submitted) == 1


def test_should_ignore_the_role_when_sharing_is_off(rig):  # noqa: F811
    slurm = FakeSlurm({"5001": [acc("COMPLETED")]})
    rig.make(slurm, HPC_SHARE_ROLE="guest")
    assert rig.cfg.share_role == ""
    rig.runner.run()
    assert "begin_time" not in slurm.submitted[0]["job"]


class SeqLive(FakeSlurm):
    """slurmctld answers each question in turn (the last answer repeats); an exception is the site not answering."""

    def __init__(self, jobs, answers):
        super().__init__(jobs)
        self.answers = list(answers)

    def live(self, jid):
        a = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(a, Exception):
            raise a
        return a


HOST_PLAN = {"timeLimitMinutes": 20, "qos": "dev", "share": {"role": "host", "jobLimitMinutes": 40}}
WAITS = {"job_state": ["PENDING"], "state_reason": "BeginTime"}
TAKEN = {"job_state": ["PENDING"], "state_reason": "JobHeldUser", "comment": "kaos-state=taken host=7001 slot=1 attempt=1"}
ENDED = acc("CANCELLED", comment="kaos-state=done rc=0 host=7001 start=1762300800")


def test_should_not_replace_a_guest_a_host_took_after_the_poll_read_it(rig):  # noqa: F811
    # The poll's reading says "waits"; the host takes the job before the replacement asks the site again.
    slurm = SeqLive({"5001": [acc("PENDING", start=0), acc("PENDING", start=0), ENDED]}, [WAITS, TAKEN])
    rig.make(slurm, status={"placement": HOST_PLAN}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1, "its step runs on the host's node: one run, one result"
    assert rig.runner.cfg.share_role == "guest" and rig.runner.cfg.qos != "dev", "the plan it was submitted with stays"
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["hostSiteJobID"] == "7001"
    assert not rig.kube.run.get("queueReplaced")


def test_should_not_replace_a_guest_whose_step_ended_on_the_host_while_its_job_still_waits(rig):  # noqa: F811
    # Live 2026-11-16: the step had saved its model, the job was still PENDING, and the job was replaced.
    over = dict(TAKEN, comment="kaos-state=done rc=0 host=7001 start=1762300800")
    slurm = SeqLive({"5001": [acc("PENDING", start=0), acc("PENDING", start=0), ENDED]}, [over])
    rig.make(slurm, status={"placement": HOST_PLAN}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1
    assert rig.kube.run["state"] == "COMPLETED" and rig.kube.run["exitCode"] == 0


def test_should_not_replace_a_guest_when_the_site_does_not_say_it_still_waits(rig):  # noqa: F811
    slurm = SeqLive({"5001": [acc("PENDING", start=0), acc("PENDING", start=0), acc("COMPLETED")]},
                    [WAITS, OSError("timed out")])
    rig.make(slurm, status={"placement": HOST_PLAN}, HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    assert slurm.cancelled == [] and len(slurm.submitted) == 1, "no answer is not a yes"
    assert rig.runner.cfg.share_role == "guest"


def test_should_report_the_host_of_a_taken_guest_while_its_step_runs(rig):  # noqa: F811
    # The accounting record does not show the host's write while the job waits; the job itself does. Without
    # this report the controller counts the guest as not taken and plans it again after its take wait.
    slurm = SeqLive({"5001": [acc("PENDING", start=0), acc("PENDING", start=0), ENDED]}, [TAKEN])
    rig.make(slurm, status={"placement": {"timeLimitMinutes": 20, "share": {"role": "guest", "host": "ns/h1"}}},
             HPC_SHARE_ROLE="guest", **SHARING)
    rig.runner.run()
    runs = [c[2]["status"]["run"] for c in rig.kube.patches() if "run" in c[2]["status"]]
    waiting = [r for r in runs if r.get("state") == "PENDING" and r.get("hostSiteJobID")]
    assert waiting and (waiting[0]["hostSiteJobID"], waiting[0]["slot"]) == ("7001", 1)
