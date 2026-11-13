"""The run form's contract (docs/run-form.schema.json, PRD 1533/F-21 and 1533/F-22).

The schema is OSR's, copied byte for byte and never edited here; this pins it and the engine's constants to it.
"""

import hashlib
import json
import re

from jsonschema import Draft202012Validator

from kubecore import step_contract as sc
from paths import ENGINE

SCHEMA_PATH = ENGINE.parent / "docs" / "run-form.schema.json"
SCHEMA_SHA256 = "c2a0f870e2f82f37a75c126c9c7087b1c36886973aa30a12da0281ab978f3098"


def _schema():
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_schema_sha256_is_pinned():
    assert hashlib.sha256(SCHEMA_PATH.read_bytes()).hexdigest() == SCHEMA_SHA256


def test_schema_is_a_valid_draft_2020_12_schema():
    Draft202012Validator.check_schema(_schema())


def test_stamp_constants_equal_the_schema():
    stamp = _schema()["properties"]["stamp"]["properties"]
    assert sc.STAMP_BY == stamp["by"]["const"]
    assert sc.ENGINE_VERSION_RE == stamp["engine"]["pattern"]
    assert sc.COMMIT_RE == stamp["commit"]["pattern"]
    assert re.match(sc.ENGINE_VERSION_RE, "0.1.0-dev.g8722848")
    assert not re.match(sc.ENGINE_VERSION_RE, "0.1.0+dev")


def test_caps_are_consistent_with_the_contract():
    assert sc.RUN_FORM_CAP == 131072          # OSR maxRunFormBytes
    assert sc.RUN_FORM_WARN == 98304 < sc.RUN_FORM_CAP
    assert sc.RAW_FORM_CAP == 128 * 1024
    assert sc.FORM_OVERRIDES_CAP == 64 * 1024


def test_annotation_and_key_names():
    assert sc.RUN_FORM_ANNOTATION == "ui.kubecore.io/run-form"
    assert sc.RAW_FORM_ANNOTATION == "ui.kubecore.io/raw-form"
    assert sc.ENGINE_VERSION_ANNOTATION == "platform.kubecore.io/engine-version"
    assert sc.ENGINE_RUN_LABEL == "platform.kubecore.io/engine"
    for key in (sc.SOURCE_COMMIT_KEY, sc.PARAMS_DIGEST_KEY, sc.PIPELINE_VERSION_KEY):
        assert key.startswith("platform.kubecore.io/")
