"""Why a platform task of a run failed, kept on the run.

A task's log is gone when its pod is (minutes to hours after the run). The line a task fails with is also written
where Kubernetes keeps a container's last message, so the run shows it as the task's message for as long as the run
is listed.
"""
import os
import sys

MESSAGE_FILE_ENV = "KAOS_TERMINATION_LOG"
MESSAGE_FILE = "/dev/termination-log"
MESSAGE_MAX = 3500        # Kubernetes keeps 4096 bytes of it


def failed(task, message):
    """Print `<task> FAILED: <message>` and keep it as the container's last message. Never raises."""
    line = f"{task} FAILED: {message}"
    print(line, file=sys.stderr, flush=True)
    try:
        with open(os.environ.get(MESSAGE_FILE_ENV) or MESSAGE_FILE, "w", encoding="utf-8") as f:
            f.write(line.encode("utf-8")[:MESSAGE_MAX].decode("utf-8", "ignore"))
    except OSError:
        pass                # not in a pod (a local run, a test): the printed line is all there is
