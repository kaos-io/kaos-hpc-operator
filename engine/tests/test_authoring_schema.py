"""authoring-schema.json describes step() for the linter and must track the code."""

import inspect
import json
import subprocess
import sys

from paths import ENGINE, TOOLS

SCHEMA = json.loads((ENGINE / "kaos_pipeline_engine" / "authoring-schema.json").read_text())


def test_should_match_the_generator_output():
    res = subprocess.run([sys.executable, str(TOOLS / "build_authoring_schema.py"), "--check"], capture_output=True)
    assert res.returncode == 0, "authoring-schema.json is stale: python engine/tools/build_authoring_schema.py"


def test_should_list_every_step_option_in_signature_order_with_its_default():
    from kubecore import authoring
    sig = {n: p for n, p in inspect.signature(authoring.step).parameters.items()
           if p.kind is not inspect.Parameter.VAR_KEYWORD}
    assert [p["name"] for p in SCHEMA["params"]] == list(sig)
    assert list(sig) == ["name", "kind", "reads", "inputs", "needs", "produces", "gpu", "when", "walltime",
                         "resumable", "checkpoint_every", "idle_timeout", "matrix", "matrix_allow_failed"], \
        "the keywords of the step contract"
    for entry in SCHEMA["params"]:
        param = sig[entry["name"]]
        assert entry["required"] == (param.default is inspect.Parameter.empty or entry["name"] == "kind")
        assert entry["doc"] and entry["type"]


def test_should_document_the_options_new_in_the_engine_over_a_copy_vendored_before_2026_11_05():
    names = {p["name"] for p in SCHEMA["params"]}
    assert {"checkpoint_every", "idle_timeout"} <= names


def test_should_publish_patterns_that_agree_with_the_validators():
    import re

    from kubecore import authoring
    by_name = {p["name"]: p for p in SCHEMA["params"]}
    assert re.match(by_name["walltime"]["pattern"], "90m") and not re.match(by_name["walltime"]["pattern"], "1d")
    assert by_name["kind"]["enum"] == ["ingest", "transform", "train", "evaluate", "publish", "control"]
    assert by_name["resumable"]["true_means"] == authoring.DEFAULT_RESUMABLE_LEAD


def test_should_tell_the_linter_what_was_removed_and_what_to_write_instead():
    assert set(SCHEMA["removed"]) == {"outputs", "hpc_time_limit", "disk"}
    assert SCHEMA["removed"]["disk"] == "disk= is gone. The step gets the disk of the machine class it runs on."
    assert not {"outputs", "hpc_time_limit", "disk"} & {p["name"] for p in SCHEMA["params"]}
    assert "migration_guide" not in SCHEMA
    assert SCHEMA["old_app_marker"] == "Create it again from the template."
