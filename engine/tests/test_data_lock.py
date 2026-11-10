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

    def record(self, repo, ref, commit, owner=None):
        self.reads.append({"repo": repo, "owner": owner, "ref": ref, "commit": commit})


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

    def resolve(self, ref, client=None):
        if self.error:
            raise self.error
        self.resolved.append(ref)
        self.lock.reads.append({"repo": ref.repo, "owner": ref.owner, "ref": ref.version, "commit": COMMIT})
        return COMMIT


def test_a_dataset_a_step_reads_is_resolved_once_into_the_lock_and_the_pin():
    lock = FakeLock()
    ds = FakeDatasets(lock)
    text, pin = compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset", "train"], datasets=ds, lock=lock)
    assert json.loads(text) == {"version": 1, "reads": [
        {"repo": "dataset-a", "owner": None, "ref": "latest", "commit": COMMIT}]}
    assert json.loads(pin) == {"repo": "dataset-a", "version": "latest", "commit": COMMIT, "owner": ""}
    assert len(ds.resolved) == 1


def test_a_dataset_shared_by_another_project_keeps_its_owner():
    lock = FakeLock()
    text, pin = compose.data_lock({"dataset": "other/dataset-a@v2"}, ["dataset"], datasets=FakeDatasets(lock), lock=lock)
    assert json.loads(text)["reads"][0]["owner"] == "other" and json.loads(pin)["owner"] == "other"


def test_a_reference_that_names_the_own_project_is_locked_as_written_too():
    """kaos-lib records `me/dataset-a@v1` of project `me` without an owner. A step that parses the same text
    asks the lock WITH the owner, so the lock answers both ways; the pin is the canonical one (no owner)."""
    lock = FakeLock()

    class OwnProject(FakeDatasets):
        def resolve(self, ref, client=None):
            self.lock.reads.append({"repo": ref.repo, "owner": None, "ref": ref.version, "commit": COMMIT})
            return COMMIT

    text, pin = compose.data_lock({"dataset": "me/dataset-a@v1"}, ["dataset"], datasets=OwnProject(lock), lock=lock)
    owners = sorted(str(r["owner"]) for r in json.loads(text)["reads"])
    assert owners == ["None", "me"] and {r["commit"] for r in json.loads(text)["reads"]} == {COMMIT}
    assert json.loads(pin)["owner"] == ""


def test_a_dataset_no_step_reads_is_never_resolved_and_cannot_fail_the_run():
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("lakeFS is down"))
    for resolved, required in (({"dataset": "dataset-a@latest"}, ["train"]), ({}, ["dataset"]),
                               ({"dataset": ""}, ["dataset"]), ({"dataset": "  "}, ["dataset"]),
                               ({"dataset": None}, ["dataset"])):
        assert compose.data_lock(resolved, required, datasets=ds, lock=lock) == (compose.EMPTY_LOCK, "")


@pytest.mark.parametrize("value", [{"name": "dataset-a", "version": "latest"}, ["dataset-a@v1"], 123, True])
def test_a_dataset_section_that_is_not_a_reference_string_is_left_alone(value, capsys):
    """An app may keep its own settings in a `dataset` section (a mapping, a list): that is not a reference the
    platform can pin, and it composed before the pin existed, so it still does. It is said in the log."""
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("must not be called"))
    assert compose.data_lock({"dataset": value}, ["dataset"], datasets=ds, lock=lock) == (compose.EMPTY_LOCK, "")
    assert "nothing is pinned for this run" in capsys.readouterr().err


@pytest.mark.parametrize("error", [RuntimeError("dataset-a has no version v9. Available: v1, v2"),
                                   ConnectionError("lakeFS unreachable"), PermissionError("no credentials")])
def test_a_reference_that_does_not_resolve_ends_the_run_naming_the_form_field(error):
    lock = FakeLock()
    with pytest.raises(compose.ComposeError) as exc:
        compose.data_lock({"dataset": "dataset-a@v9"}, ["dataset"], datasets=FakeDatasets(lock, error=error), lock=lock)
    assert str(exc.value).startswith("dataset: cannot use 'dataset-a@v9'") and str(error) in str(exc.value)


@pytest.mark.parametrize("value", ["models-v2@v1", "outputs-app@main", "my-repo@latest", "other/models@v1"])
def test_only_a_dataset_repository_can_be_the_runs_dataset(value):
    """A platform Dataset is a repository named dataset-<name>. The rule is checked here, once, so a run never
    works on a cluster class and then fails on an HPC class."""
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("must not be resolved"))
    with pytest.raises(compose.ComposeError, match="is not a dataset: a dataset is a repository named dataset-"):
        compose.data_lock({"dataset": value}, ["dataset"], datasets=ds, lock=lock)
    assert lock.reads == []


def test_a_malformed_reference_is_refused():
    lock = FakeLock()
    with pytest.raises(compose.ComposeError, match="dataset: cannot use 'dataset-a'"):
        compose.data_lock({"dataset": "dataset-a"}, ["dataset"], datasets=FakeDatasets(lock), lock=lock)


@pytest.mark.parametrize("answer", [None, "", "abc123", "C2" * 32, 7])
def test_an_answer_that_is_not_a_commit_id_ends_the_run_instead_of_reaching_the_steps(answer):
    """A data store that answers without a commit must not produce a lock every step then rejects."""
    lock = FakeLock()

    class Odd(FakeDatasets):
        def resolve(self, ref, client=None):
            self.lock.reads.append({"repo": ref.repo, "owner": None, "ref": ref.version, "commit": answer})
            return answer

    with pytest.raises(compose.ComposeError, match="where a commit id was expected"):
        compose.data_lock({"dataset": "dataset-a@v1"}, ["dataset"], datasets=Odd(lock), lock=lock)


def test_a_resolve_that_records_nothing_ends_the_run():
    lock = FakeLock()

    class Silent(FakeDatasets):
        def resolve(self, ref, client=None):
            return COMMIT

    with pytest.raises(compose.ComposeError, match="recorded no read"):
        compose.data_lock({"dataset": "dataset-a@v1"}, ["dataset"], datasets=Silent(lock), lock=lock)


def test_offline_resolves_nothing_and_writes_an_empty_lock():
    lock = FakeLock()
    ds = FakeDatasets(lock, error=RuntimeError("must not be called"))
    assert compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset"], offline=True, datasets=ds, lock=lock) \
        == (compose.EMPTY_LOCK, "")
    assert json.loads(compose.EMPTY_LOCK) == {"version": 1, "reads": []}


def test_without_kaos_lib_the_compose_step_fails_instead_of_skipping(monkeypatch):
    monkeypatch.setitem(sys.modules, "kaos_lib", None)   # import kaos_lib -> ImportError
    with pytest.raises(compose.ComposeError, match="kaos-lib is not installed"):
        compose.data_lock({"dataset": "dataset-a@latest"}, ["dataset"])


def test_a_lock_in_the_compose_pods_own_environment_is_never_replayed(monkeypatch):
    """kaos-lib replays $KAOS_LIB_LOCK: with one set, a resolve returns ITS commit without asking the data
    store. The compose step decides the commit, so it drops the variable and kaos-lib's cached copy first."""
    calls = []
    fake_lock = types.SimpleNamespace(reset=lambda: calls.append("reset"))
    fake_lakefs = types.SimpleNamespace(LakeFS=types.SimpleNamespace(
        connect=lambda: types.SimpleNamespace(timeout=300)))
    pkg = types.ModuleType("kaos_lib")
    pkg.datasets, pkg.lock = types.SimpleNamespace(), fake_lock
    monkeypatch.setitem(sys.modules, "kaos_lib", pkg)
    monkeypatch.setitem(sys.modules, "kaos_lib.datasets", pkg.datasets)
    monkeypatch.setitem(sys.modules, "kaos_lib.lock", fake_lock)
    monkeypatch.setitem(sys.modules, "kaos_lib.lakefs", fake_lakefs)
    monkeypatch.setenv("KAOS_LIB_LOCK", '{"version":1,"reads":[]}')
    _, _, client = compose._kaos_lib()
    assert "KAOS_LIB_LOCK" not in compose.os.environ and calls == ["reset"]
    assert client.timeout == compose.LAKEFS_TIMEOUT_S, "a dead data store ends the run in minutes, not half an hour"


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
    assert args[args.index("--pin-output") + 1] == "/work/dataset-pin.json"
    assert "--offline" not in args, "a rendered pipeline never composes offline"
    outs = {o["name"]: o["valueFrom"]["path"] for o in comp["outputs"]["parameters"]}
    assert outs == {"params": "/work/params.yaml", "data-lock": "/work/data-lock.json",
                    "dataset-pin": "/work/dataset-pin.json"}
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
    out, lock, pin = tmp_path / "params.yaml", tmp_path / "data-lock.json", tmp_path / "dataset-pin.json"
    env = {"KAOS_ENGINE_DIR": str(ENGINE), "KUBECORE_CONFIG_DIR": str(TEST_APP / "config"),
           "KUBECORE_SCHEMA_PROFILE": "hera", "PATH": "/usr/bin:/bin"}
    code = (f"import sys; sys.path.insert(0, {str(ENGINE)!r}); from kaos_engine import compose; "
            "sys.exit(compose.main(sys.argv[1:]))")
    done = subprocess.run([sys.executable, "-I", "-c", code, "--require=experiment", "--output", str(out),
                           "--lock-output", str(lock), "--pin-output", str(pin)],
                          capture_output=True, text=True, env=env, cwd=tmp_path)
    assert done.returncode == 0, done.stderr[-2000:]
    assert "experiment" in yaml.safe_load(out.read_text())
    assert json.loads(lock.read_text()) == {"version": 1, "reads": []}, "no step reads the dataset here"
    assert pin.read_text() == "", "and the run has no dataset pin"
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


# ------------------------------------------------------------------ names the platform keeps for itself

@pytest.mark.parametrize("name", ["x-hpc", "hpc-hpc", "compose-and-validate", "train-hpc"])
def test_a_step_may_not_take_the_name_of_an_hpc_variant_or_of_the_compose_step(name):
    from kubecore import authoring
    with pytest.raises(authoring.AuthoringError, match="is reserved"):
        with authoring.pipeline("p"):
            authoring.step(name)


@pytest.mark.parametrize("output", ["params", "data-lock", "dataset-pin"])
def test_a_step_output_may_not_take_a_name_the_platform_passes_to_every_step(output):
    from kubecore import authoring
    with pytest.raises(authoring.AuthoringError, match="reserved"):
        with authoring.pipeline("p"):
            authoring.step("one", outputs=[output])


def test_a_step_called_hpc_is_fine():
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        authoring.step("hpc")
    assert "hpc" in [s.name for s in p.steps]


def test_the_engine_entry_point_says_so_when_the_engine_has_no_compose(tmp_path):
    broken = tmp_path / "engine"
    (broken / "kubecore").mkdir(parents=True)
    (broken / "kubecore" / "__init__.py").write_text("")
    code = (f"import sys; sys.path.insert(0, {str(ENGINE)!r}); from kaos_engine import compose; "
            "sys.exit(compose.main(sys.argv[1:]))")
    done = subprocess.run([sys.executable, "-I", "-c", code, "--output", str(tmp_path / "p.yaml")],
                          capture_output=True, text=True, env={"KAOS_ENGINE_DIR": str(broken), "PATH": "/usr/bin:/bin"})
    assert done.returncode == 4 and "has no kubecore/compose.py" in done.stderr and "Traceback" not in done.stderr


def test_the_engine_image_carries_what_kaos_lib_needs_so_the_compose_image_adds_only_kaos_lib():
    """The compose image installs kaos-lib with --no-deps: every library kaos-lib imports must already be in
    the engine's pinned, scanned set, or the compose step fails at import (or cannot read a shared dataset)."""
    pinned = {line.split("==")[0].strip().lower() for line in (ENGINE / "requirements.txt").read_text().splitlines()
              if "==" in line and not line.startswith("#")}
    assert {"requests", "pyyaml", "packaging", "pyjwt", "cryptography"} <= pinned
