"""The run's data lock (PRD-HPC-1461 D-81): the compose step resolves the data inputs a step reads, once, and
every step and the HPC follower receive the result. Nothing is skipped; a laptop resolves nothing, explicitly."""

import json
import subprocess
import sys
import types

import pytest
import yaml

from paths import APP as TEST_APP
from paths import ENGINE

sys.path.insert(0, str(ENGINE))
from kubecore import compose  # noqa: E402

COMMIT = "c2" * 32


class FakeLock:
    def __init__(self):
        self.reads = []

    def current(self):
        return types.SimpleNamespace(to_json=lambda: json.dumps({"version": 1, "reads": self.reads}))


class FakeDatasets:
    """kaos-lib's datasets module as compose uses it: DatasetRef.parse and resolve (which records the read)."""

    def __init__(self, lock, error=None):
        self.lock, self.error, self.resolved = lock, error, []
        self.DatasetRef = types.SimpleNamespace(parse=self.parse)

    def parse(self, text):
        name, _, version = text.partition("@")
        if not version:
            raise ValueError(f"{text!r} is not <dataset>@<version>")
        owner, _, repo = name.rpartition("/")
        return types.SimpleNamespace(repo=repo, version=version, owner=owner or None)

    def resolve(self, ref):
        if self.error:
            raise self.error
        self.resolved.append(ref)
        self.lock.reads.append({"repo": ref.repo, "owner": ref.owner, "ref": ref.version, "commit": COMMIT})
        return COMMIT


def test_a_dataset_a_step_reads_is_resolved_once_into_the_lock():
    lock = FakeLock()
    ds = FakeDatasets(lock)
    text = compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset", "train"], datasets=ds, lock=lock)
    assert json.loads(text) == {"version": 1, "reads": [
        {"repo": "dataset-a", "owner": None, "ref": "latest", "commit": COMMIT}]}
    assert len(ds.resolved) == 1


def test_a_dataset_shared_by_another_project_keeps_its_owner():
    lock = FakeLock()
    text = compose.data_lock({"dataset": "other/dataset-a@v2"}, ["dataset"], datasets=FakeDatasets(lock), lock=lock)
    assert json.loads(text)["reads"][0]["owner"] == "other"


def test_a_dataset_no_step_reads_is_never_resolved_and_cannot_fail_the_run():
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("lakeFS is down"))
    assert compose.data_lock({"dataset": "dataset-a@latest"}, ["train"], datasets=ds, lock=lock) == compose.EMPTY_LOCK
    assert compose.data_lock({}, ["dataset"], datasets=ds, lock=lock) == compose.EMPTY_LOCK
    assert compose.data_lock({"dataset": ""}, ["dataset"], datasets=ds, lock=lock) == compose.EMPTY_LOCK


@pytest.mark.parametrize("error", [RuntimeError("dataset-a has no version v9. Available: v1, v2"),
                                   ConnectionError("lakeFS unreachable"), PermissionError("no credentials")])
def test_a_reference_that_does_not_resolve_ends_the_run_naming_the_form_field(error):
    lock = FakeLock()
    with pytest.raises(compose.ComposeError) as exc:
        compose.data_lock({"dataset": "dataset-a@v9"}, ["dataset"], datasets=FakeDatasets(lock, error=error), lock=lock)
    assert str(exc.value).startswith("dataset: cannot use 'dataset-a@v9'") and str(error) in str(exc.value)


def test_a_value_that_is_not_a_reference_is_refused():
    lock = FakeLock()
    with pytest.raises(compose.ComposeError, match="dataset: cannot use 'dataset-a'"):
        compose.data_lock({"dataset": "dataset-a"}, ["dataset"], datasets=FakeDatasets(lock), lock=lock)
    with pytest.raises(compose.ComposeError, match="dataset: expected <dataset>@<version>"):
        compose.data_lock({"dataset": {"repo": "x"}}, ["dataset"], datasets=FakeDatasets(lock), lock=lock)


def test_offline_resolves_nothing_and_writes_an_empty_lock():
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("must not be called"))
    assert compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset"], offline=True, datasets=ds, lock=lock) \
        == compose.EMPTY_LOCK
    assert json.loads(compose.EMPTY_LOCK) == {"version": 1, "reads": []}


def test_without_kaos_lib_the_compose_step_fails_instead_of_skipping(monkeypatch):
    monkeypatch.setitem(sys.modules, "kaos_lib", None)   # import kaos_lib -> ImportError
    with pytest.raises(compose.ComposeError, match="kaos-lib is not installed"):
        compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset"])


# ------------------------------------------------------------------ the rendered pipeline carries the lock

def _render():
    code = (f"import sys, runpy; sys.path.insert(0, {str(ENGINE)!r}); sys.path.insert(1, {str(TEST_APP)!r}); "
            f"ns = runpy.run_path({str(TEST_APP / 'pipeline.py')!r}, run_name='render'); print(ns['p'].wt.to_yaml())")
    env = {"KUBECORE_CONFIG_DIR": str(TEST_APP / "config"), "KUBECORE_SCHEMA_PROFILE": "hera", "PATH": "/usr/bin:/bin"}
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=TEST_APP)
    assert done.returncode == 0, done.stderr[-2000:]
    return yaml.safe_load(done.stdout)


def test_every_step_gets_the_lock_and_the_compose_step_writes_it():
    wft = _render()
    templates = {t["name"]: t for t in wft["spec"]["templates"]}
    comp = templates["compose-and-validate"]
    assert comp["container"]["command"] == ["python", "-I", "-m", "kaos_engine.compose"]
    args = comp["container"]["args"]
    assert args[args.index("--lock-output") + 1] == "/work/data-lock.json"
    assert "--offline" not in args, "a rendered pipeline never composes offline"
    outs = {o["name"]: o["valueFrom"]["path"] for o in comp["outputs"]["parameters"]}
    assert outs == {"params": "/work/params.yaml", "data-lock": "/work/data-lock.json"}
    steps = [t for n, t in templates.items() if "container" in t and n != "compose-and-validate"]
    assert steps
    for step in steps:
        assert "data-lock" in [p["name"] for p in step["inputs"]["parameters"]], step["name"]
        env = {e["name"]: e["value"] for e in step["container"]["env"]}
        assert env["KAOS_LIB_LOCK"] == "{{inputs.parameters.data-lock}}", step["name"]
    dag = next(t for t in wft["spec"]["templates"] if "dag" in t)
    for task in dag["dag"]["tasks"]:
        if task["name"] == "compose-and-validate":
            continue
        got = {p["name"]: p["value"] for p in task["arguments"]["parameters"]}
        assert got["data-lock"] == "{{tasks.compose-and-validate.outputs.parameters.data-lock}}", task["name"]


def test_every_step_carries_its_name_and_kind_as_labels():
    wft = _render()
    for tpl in wft["spec"]["templates"]:
        if "container" not in tpl:
            continue
        labels = tpl["metadata"]["labels"]
        assert labels["platform.kubecore.io/step"] == tpl["name"]
        assert labels["platform.kubecore.io/step-kind"] == ("control" if tpl["name"] == "compose-and-validate"
                                                            else "unspecified")


# ------------------------------------------------------------------ the entry point the compose image runs

def test_the_compose_entry_point_runs_the_engines_compose_isolated_and_writes_params_and_lock(tmp_path):
    out, lock = tmp_path / "params.yaml", tmp_path / "data-lock.json"
    env = {"KAOS_ENGINE_DIR": str(ENGINE), "KUBECORE_CONFIG_DIR": str(TEST_APP / "config"),
           "KUBECORE_SCHEMA_PROFILE": "hera", "PATH": "/usr/bin:/bin"}
    code = (f"import sys; sys.path.insert(0, {str(ENGINE)!r}); from kaos_engine import compose; "
            "sys.exit(compose.main(sys.argv[1:]))")
    done = subprocess.run([sys.executable, "-I", "-c", code, "--require=experiment", "--output", str(out),
                           "--lock-output", str(lock)], capture_output=True, text=True, env=env, cwd=tmp_path)
    assert done.returncode == 0, done.stderr[-2000:]
    assert "experiment" in yaml.safe_load(out.read_text())
    assert json.loads(lock.read_text()) == {"version": 1, "reads": []}, "no step reads the dataset here"
    assert "kaos-engine: version=" in done.stderr


# ------------------------------------------------------------------ a step without a built image never starts a run

GOOD = "zot.example:5000/proj/app:train-0a1b2c3"


@pytest.mark.parametrize("ref", [GOOD, "registry.example/p/a/b:v1", "r.example/p/a@sha256:" + "a" * 64])
def test_real_image_references_pass(ref):
    compose.check_step_images(json.dumps({"train": ref}))
    compose.check_step_images(None)
    compose.check_step_images("")


@pytest.mark.parametrize("ref", [":probe-learn-latest", "", "probe-learn-latest", "/proj/app:tag", "zot.example/proj/app",
                                 "zot.example/:tag", "zot.example/proj/app:", "{{workflow.parameters.image-x}}", None, 7])
def test_a_reference_without_registry_repository_or_tag_stops_the_run_naming_the_step(ref):
    with pytest.raises(compose.ComposeError) as exc:
        compose.check_step_images(json.dumps({"train": GOOD, "probe-learn": ref}))
    assert "no image has been built yet for: probe-learn" in str(exc.value) and "train (" not in str(exc.value)


@pytest.mark.parametrize("raw", ["{", "[]", '"x"'])
def test_an_unreadable_image_list_stops_the_run_instead_of_being_skipped(raw):
    with pytest.raises(compose.ComposeError, match="cannot read the steps' images"):
        compose.check_step_images(raw)


def test_the_compose_entry_point_refuses_the_run_before_composing(tmp_path):
    out = tmp_path / "params.yaml"
    env = {"KAOS_ENGINE_DIR": str(ENGINE), "KUBECORE_CONFIG_DIR": str(TEST_APP / "config"),
           "KUBECORE_SCHEMA_PROFILE": "hera", "PATH": "/usr/bin:/bin",
           "KAOS_STEP_IMAGES": json.dumps({"probe-learn": ":probe-learn-latest"})}
    code = (f"import sys; sys.path.insert(0, {str(ENGINE)!r}); from kaos_engine import compose; "
            "sys.exit(compose.main(sys.argv[1:]))")
    done = subprocess.run([sys.executable, "-I", "-c", code, "--output", str(out), "--lock-output",
                           str(tmp_path / "lock.json")], capture_output=True, text=True, env=env, cwd=tmp_path)
    assert done.returncode == 1 and "no image has been built yet for: probe-learn" in done.stderr
    assert not out.exists() and not (tmp_path / "lock.json").exists(), "a refused run publishes nothing"


def test_the_enhancer_hands_the_compose_step_every_other_steps_image():
    from kubecore import enhance
    steps = [{"name": "compose-and-validate", "container": {"image": "{{workflow.parameters.image-compose-and-validate}}"}},
             {"name": "train", "container": {"image": "{{workflow.parameters.image-train}}"}},
             {"name": "util", "container": {"image": "busybox:1.36"}}]      # a verbatim image annotation: no parameter
    enhance.enhance_image_guard(steps)
    env = {e["name"]: e["value"] for e in steps[0]["container"]["env"]}
    assert json.loads(env["KAOS_STEP_IMAGES"]) == {"train": "{{workflow.parameters.image-train}}"}
    enhance.enhance_image_guard(steps)
    assert len(steps[0]["container"]["env"]) == 1, "idempotent"
    assert "env" not in steps[1]["container"]
