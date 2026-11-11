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
