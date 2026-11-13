"""PRD kaos#1911: two platform projects as two DIFFERENT Unix users on one shared scratch (hack/local-lab/two_users.py).

Needs real distinct uids, so it runs only where they can be had: as root inside a container (the validate container
normally is), or on a workstation with podman or docker and the python:3.12-slim image. Anywhere else it is skipped with
the reason, and the same-uid fake-site tests (test_per_project_root.py) are what runs. The staging tool (STAGER, the
kubecore-operator checkout) is required: it is what connects each project first."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "hack" / "local-lab" / "two_users.py"


def _in_container():
    return os.geteuid() == 0 and (Path("/.dockerenv").exists() or Path("/run/.containerenv").exists())


def _command():
    if not os.environ.get("STAGER"):
        pytest.skip("the run needs the staging tool (STAGER=<kubecore-operator>/images/meluxina-tokenmint/stager/stager.py): each project is connected by its `prepare` verb first")
    if _in_container():
        return ["python3", str(SCRIPT)]
    ctr = os.environ.get("CONTAINER") or shutil.which("podman") or shutil.which("docker")
    if not ctr:
        pytest.skip("two different Unix users need root in a container, or podman/docker: neither is here")
    image = os.environ.get("TWO_USERS_IMAGE", "docker.io/library/python:3.12-slim")
    have = subprocess.run([ctr, "image", "exists", image] if "podman" in ctr else [ctr, "image", "inspect", image],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not have:
        pytest.skip("the image %s is not present locally (the test never pulls)" % image)
    return [str(REPO / "hack" / "local-lab" / "two-users.sh")]


def test_two_projects_with_two_unix_users_share_one_scratch_without_touching_each_other():
    cmd = _command()
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, timeout=600)
    assert r.returncode == 0 and r.stdout.rstrip().endswith("ALL OK"), r.stdout[-3000:]
    assert "FAIL" not in r.stdout
