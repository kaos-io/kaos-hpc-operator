"""How an attempt of a step ends and what the platform does about it: one table, read by the pipeline engine for a step on
the cluster and by the HPC controller (internal/resume), which embeds a copy of the same file.

  machine lost   the machine or the pod was taken away (spot, eviction, node failure, preemption)  -> run again
  limit          the attempt reached its time limit, or the step saved and asked to be continued   -> run again
  own error      the step's own failure, out of memory, the idle watchdog, a missing result         -> never again

`endings.json` is the source. The controller's copy must be equal (internal/resume TestEndingsTableIsTheEngines);
tests/test_endings_parity.py feeds the same cases to both. The budget is one constant for every executor.
"""
import json
from pathlib import Path

TABLE = json.loads(Path(__file__).with_name("endings.json").read_text())

# At most this many continuations from saved progress, and restarts from zero, per step.
MAX_CONTINUATIONS = TABLE["budget"]["continuations"]
MAX_RESTARTS = TABLE["budget"]["restarts"]

# How the kubelet words a step pod lost with its node. Argo marks such a pod Failed (a pod with a status message
# is never an Error), so OnError alone never retries a spot preemption or a node-pressure eviction.
POD_LOSS_MESSAGES = "|".join(TABLE["machine_lost"]["pod_messages"])
# A step that ends well without writing its result is an Error of the executor ("no such file ...
# kaos-result.json"). It is deterministic: the same step ends the same way again, and a re-run would
# spend the GPU twice. It is not retried; a node lost with the step is.
NO_RESULT_MESSAGE = TABLE["own_error"]["no_result_message"]
POD_LOSS_RETRY = ("(lastRetry.status == 'Error' or lastRetry.message matches '.*(%s).*')"
                  " and not (lastRetry.message contains '%s')" % (POD_LOSS_MESSAGES, NO_RESULT_MESSAGE))
# The pool's idle watchdog ends a step by setting its pod Failed with a message that starts with this prefix
# (kaos-hpc-operator internal/idlewatch: MessagePrefix). An idle end is the step's own outcome, never a lost node:
# whatever else the message says, a message that starts with the prefix is not run again.
IDLE_END_PREFIX = TABLE["own_error"]["idle_message_prefix"]
# A step that reached its time limit (the kubelet's and the workflow engine's words for it) or asked to be continued
# (exit code 85, kaos_lib.checkpoint exit_for_restart) is run again from its saved progress.
CONTINUE_EXIT_CODE = str(TABLE["limit"]["exit_code"])
DEADLINE_MESSAGES = "|".join(TABLE["limit"]["pod_messages"])
