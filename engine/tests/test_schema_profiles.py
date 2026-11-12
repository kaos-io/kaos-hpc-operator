"""The structured-config schemas of the two profiles (kubecore/derive_tree.py, engine/README.md "Schema profiles")."""

import json
import os
import subprocess
import sys

import pytest

from paths import ENGINE


def data_fields(profile):
    code = ("import json, dataclasses; from kubecore import derive_tree as d; "
            "print(json.dumps([f.name for f in dataclasses.fields(d.DataConfig)]))")
    env = dict(os.environ, KUBECORE_SCHEMA_PROFILE=profile, PYTHONPATH=str(ENGINE))
    done = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr[-1500:]
    return json.loads(done.stdout)


@pytest.mark.parametrize("field", ["source", "path_override"])
def test_the_yolo_data_config_has_no_field_that_no_step_reads(field):
    assert field not in data_fields("yolo")


def test_the_yolo_data_config_keeps_what_the_steps_read():
    assert data_fields("yolo") == ["sample_size", "seed"]
