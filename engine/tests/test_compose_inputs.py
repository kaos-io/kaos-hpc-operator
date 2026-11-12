"""What the compose step does when a run starts (PRD kaos#1911 F-03, CON-04, D-06): it pins every declared
input to a commit, writes each step's context and the run's ledger, and refuses, before any other step
starts, a run that cannot work. Nothing is skipped; a laptop resolves nothing, explicitly."""

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
from kubecore.step_contract import OLD_APP_NOTICE, cost, wire  # noqa: E402

COMMITS = {"dataset-a": "a1" * 32, "dataset-b": "b2" * 32}


class FakeDatasets:
    """kaos-lib's datasets module as compose uses it: DatasetRef.parse, source and resolve."""

    def __init__(self, error=None, own_project="me", answer=None):
        self.error, self.own_project, self.answer, self.resolved = error, own_project, answer, []
        self.DatasetRef = types.SimpleNamespace(parse=self.parse)

    def parse(self, text):
        name, _, version = text.partition("@")
        if not version:
            raise ValueError(f"{text!r} is not <dataset>@<version>")
        owner, _, repo = name.rpartition("/")
        return types.SimpleNamespace(repo=repo, version=version, owner=owner or None)

    def source(self, ref, client=None):
        if ref.owner == self.own_project:
            ref = types.SimpleNamespace(repo=ref.repo, version=ref.version, owner=None)
        return ref, client

    def resolve(self, ref, client=None):
        if self.error:
            raise self.error
        self.resolved.append(f"{ref.repo}@{ref.version}")
        return COMMITS[ref.repo] if self.answer is None else self.answer[0]


# What the steps of these tests name in reads=.
READS = ["train", "experiment", "blob", "k", "lr"]


def steps(*specs):
    """[(name, inputs, how many steps it needs, produces)] -> the step table authoring writes."""
    return [{"name": n, "kind": "train", "reads": READS, "inputs": list(i),
             "needs": [f"up{k}" for k in range(needs)], "produces": list(p)}
            for n, i, needs, p in specs]


TWO_INPUTS = steps(("prepare", ["raw"], 0, []), ("train", ["raw", "labels"], 1, []))


# ---------------------------------------------------------------- every declared input is pinned

def test_every_declared_input_is_resolved_once_to_a_commit():
    """AC-04: a step with two declared inputs gets both, each at its commit; an input two steps share is
    resolved once, so both read the same commit."""
    ds = FakeDatasets()
    reads = compose.pin_inputs({"raw": "dataset-a@latest", "labels": "dataset-b@v3", "other": "dataset-a@v1"},
                               TWO_INPUTS, datasets=ds)
    assert reads == {
        "raw": {"name": "raw", "repo": "dataset-a", "owner": None, "ref": "latest", "commit": COMMITS["dataset-a"]},
        "labels": {"name": "labels", "repo": "dataset-b", "owner": None, "ref": "v3", "commit": COMMITS["dataset-b"]}}
    assert ds.resolved == ["dataset-a@latest", "dataset-b@v3"], "once each, and nothing no step declares"


def test_a_reference_without_a_version_is_latest():
    """D-06: `latest` is not a version, it is the head of the dataset's main when the run starts."""
    ds = FakeDatasets()
    reads = compose.pin_inputs({"raw": "dataset-a"}, steps(("prepare", ["raw"], 0, [])), datasets=ds)
    assert reads["raw"]["ref"] == "latest" and reads["raw"]["commit"] == COMMITS["dataset-a"]


def test_a_dataset_shared_by_another_project_keeps_its_owner_and_the_own_project_is_dropped():
    one = steps(("prepare", ["raw"], 0, []))
    assert compose.pin_inputs({"raw": "other/dataset-a@v2"}, one, datasets=FakeDatasets())["raw"]["owner"] == "other"
    assert compose.pin_inputs({"raw": "me/dataset-a@v2"}, one, datasets=FakeDatasets())["raw"]["owner"] is None


def test_a_config_value_no_step_declares_is_never_resolved_and_cannot_fail_the_run():
    ds = FakeDatasets(error=RuntimeError("lakeFS is down"))
    assert compose.pin_inputs({"dataset": "dataset-a@latest"}, steps(("train", [], 0, [])), datasets=ds) == {}


@pytest.mark.parametrize("value, why", [("", "empty"), ("  ", "empty"), (None, "empty"),
                                        ({"name": "dataset-a"}, "not a dataset reference"), (7, "not a dataset reference")])
def test_a_declared_input_without_a_reference_ends_the_run_naming_the_input(value, why):
    ds = FakeDatasets(error=RuntimeError("must not be called"))
    with pytest.raises(compose.ComposeError) as exc:
        compose.pin_inputs({"raw": value}, TWO_INPUTS, datasets=ds)
    assert str(exc.value) == (f"input 'raw' (step prepare, train): the config value 'raw' is {why}. Pick a dataset "
                              "and a version for 'raw' on the form, for example dataset-default@latest")


@pytest.mark.parametrize("error", [RuntimeError("dataset-a has no version v9. Available: v1, v2"),
                                   ConnectionError("lakeFS unreachable"), PermissionError("no credentials")])
def test_a_missing_version_or_an_unreadable_input_ends_the_run_naming_the_input(error):
    """CON-04: fail closed, before GPU."""
    with pytest.raises(compose.ComposeError) as exc:
        compose.pin_inputs({"raw": "dataset-a@v9"}, steps(("prepare", ["raw"], 0, [])),
                           datasets=FakeDatasets(error=error))
    assert str(exc.value) == f"input 'raw' (step prepare): cannot use 'dataset-a@v9': {error}"


@pytest.mark.parametrize("value", ["models-v2@v1", "outputs-app@main", "my-repo@latest", "other/models@v1"])
def test_only_a_dataset_repository_can_be_an_input(value):
    ds = FakeDatasets(error=RuntimeError("must not be resolved"))
    with pytest.raises(compose.ComposeError, match="is not a dataset: a dataset is a repository named dataset-"):
        compose.pin_inputs({"raw": value}, steps(("prepare", ["raw"], 0, [])), datasets=ds)


@pytest.mark.parametrize("answer", [None, "", "abc123", "C2" * 32, 7])
def test_an_answer_that_is_not_a_commit_id_ends_the_run_instead_of_reaching_the_steps(answer):
    with pytest.raises(compose.ComposeError, match="where a commit id was expected"):
        compose.pin_inputs({"raw": "dataset-a@v1"}, steps(("prepare", ["raw"], 0, [])),
                           datasets=FakeDatasets(answer=[answer]))


def test_without_kaos_lib_the_compose_step_fails_instead_of_skipping(monkeypatch):
    monkeypatch.setitem(sys.modules, "kaos_lib", None)   # import kaos_lib -> ImportError
    with pytest.raises(compose.ComposeError, match="kaos-lib is not installed"):
        compose.pin_inputs({"raw": "dataset-a@latest"}, steps(("prepare", ["raw"], 0, [])))


def test_compose_connects_with_a_short_timeout(monkeypatch):
    fake_lakefs = types.SimpleNamespace(LakeFS=types.SimpleNamespace(
        connect=lambda: types.SimpleNamespace(timeout=300)))
    pkg = types.ModuleType("kaos_lib")
    pkg.datasets = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "kaos_lib", pkg)
    monkeypatch.setitem(sys.modules, "kaos_lib.datasets", pkg.datasets)
    monkeypatch.setitem(sys.modules, "kaos_lib.lakefs", fake_lakefs)
    _, client = compose._kaos_lib()
    assert client.timeout == compose.LAKEFS_TIMEOUT_S, "a dead data store ends the run in minutes, not half an hour"


# ---------------------------------------------------------------- each step's context

RESOLVED = {"raw": "dataset-a@latest", "labels": "dataset-b@v3", "train": {"epochs": 3},
            "experiment": {"name": "baseline"}}


def contexts(table, resolved=RESOLVED, plan=None):
    reads = compose.pin_inputs(resolved, table, datasets=FakeDatasets())
    return {name: json.loads(text) for name, text in
            compose.step_contexts(table, "run-1", "demo", "vision", resolved, reads, plan).items()}


def test_a_step_context_holds_its_own_inputs_and_the_sections_it_reads():
    got = contexts(TWO_INPUTS)
    assert list(got["train"]) == ["version", "run", "app", "project", "step", "kind", "experiment", "params",
                                  "ledger", "produces"], "the contract's fields, in order, without upstream"
    assert [r["name"] for r in got["prepare"]["ledger"]["reads"]] == ["raw"]
    assert [r["name"] for r in got["train"]["ledger"]["reads"]] == ["raw", "labels"]
    assert got["train"]["ledger"]["version"] == 2 and got["train"]["ledger"]["writes"] == []
    assert got["train"]["run"] == "run-1" and got["train"]["app"] == "demo" and got["train"]["project"] == "vision"
    assert got["train"]["experiment"] == "baseline"
    assert got["train"]["params"] == {"train": {"epochs": 3}, "experiment": {"name": "baseline"}}, \
        "the sections the step names in reads=, in that order; an input's own config value is not one of them"


def test_a_step_gets_only_the_config_sections_it_reads():
    table = [{"name": "prepare", "kind": "transform", "reads": [], "inputs": ["raw"], "needs": [], "produces": []},
             {"name": "train", "kind": "train", "reads": ["train"], "inputs": [], "needs": ["prepare"], "produces": []}]
    got = contexts(table)
    assert got["prepare"]["params"] == {} and got["train"]["params"] == {"train": {"epochs": 3}}
    assert got["prepare"]["experiment"] == "baseline", "the experiment does not depend on what the step reads"
    assert compose.step_params({"reads": ["gone", "train"]}, RESOLVED) == {"train": {"epochs": 3}}


def test_the_experiment_is_the_configs_experiment_name_else_the_app():
    assert compose.experiment_of({"experiment": {"name": " exp-1 "}}, "demo") == "exp-1"
    for resolved in ({}, {"experiment": "text"}, {"experiment": {"name": ""}}, {"experiment": {"name": None}}):
        assert compose.experiment_of(resolved, "demo") == "demo"


def test_what_a_step_produces_names_its_repository():
    table = steps(("train", [], 0, [{"name": "model", "type": "model"}, {"name": "report", "type": "artifact"},
                                    {"name": "embeddings", "type": "dataset"}]))
    assert contexts(table)["train"]["produces"] == [
        {"name": "model", "type": "model", "repo": "outputs-demo"},
        {"name": "report", "type": "artifact", "repo": "outputs-demo"},
        {"name": "embeddings", "type": "dataset", "repo": "dataset-demo-embeddings"}]


def test_a_run_promotes_a_dataset_only_when_it_asks():
    """D-06: plan.promote.<dataset> of the run request; absent means the step seals a version and latest stays."""
    table = steps(("prepare", [], 0, [{"name": "embeddings", "type": "dataset"}, {"name": "model", "type": "model"}]))
    asked = contexts(table, plan={"promote": {"embeddings": True}})["prepare"]["produces"]
    assert asked[0] == {"name": "embeddings", "type": "dataset", "repo": "dataset-demo-embeddings", "promote": True}
    assert "promote" not in asked[1]
    for plan in (None, {}, {"promote": {"embeddings": False}}):
        assert "promote" not in contexts(table, plan=plan)["prepare"]["produces"][0]
    with pytest.raises(compose.ComposeError) as exc:
        contexts(table, plan={"promote": {"embedings": True}})
    assert str(exc.value) == ("plan.promote.embedings: no step of this pipeline produces the dataset 'embedings' "
                              "(it produces: embeddings)")


# ---------------------------------------------------------------- the run's own keys in the ADVANCED override

def test_plan_is_taken_out_of_the_override_before_it_reaches_the_apps_config():
    advanced, plan = compose.split_plan("train:\n  epochs: 5\nplan:\n  promote:\n    embeddings: true\n")
    assert advanced == {"train": {"epochs": 5}} and plan == {"promote": {"embeddings": True}}
    assert compose.split_plan("") == (None, {}) and compose.split_plan("train: {epochs: 5}")[1] == {}
    for text, want in (("true", True), ("True", True), ("TRUE", True), ("false", False), ("False", False),
                       ("'true'", True), ("'FALSE'", False)):
        assert compose.split_plan(f"plan: {{promote: {{embeddings: {text}}}}}")[1] == {"promote": {"embeddings": want}}


@pytest.mark.parametrize("advanced, message", [
    ("pipeline-version: abc", "'pipeline-version' is reserved for the platform and is not available yet; remove it "
                              "from the ADVANCED override"),
    ("plan: {target: train}", "'plan.target' is reserved for the platform and is not available yet; this version "
                              "reads plan.promote.<dataset> only"),
    ("plan: {use: {model: abc}}", "'plan.use' is reserved for the platform and is not available yet; this version "
                                  "reads plan.promote.<dataset> only"),
    ("plan: yes", "'plan' must be a mapping, for example plan: {promote: {<dataset>: true}}"),
    ("plan: {promote: {embeddings: 1}}", "'plan.promote' must map a produced dataset to true or false, for "
                                         "example plan: {promote: {<dataset>: true}}"),
    ("plan: {promote: {embeddings: maybe}}", "'plan.promote' must map a produced dataset to true or false, for "
                                             "example plan: {promote: {<dataset>: true}}"),
])
def test_a_run_key_this_version_does_not_act_on_fails_the_run_instead_of_being_ignored(advanced, message):
    with pytest.raises(compose.ComposeError) as exc:
        compose.split_plan(advanced)
    assert str(exc.value) == message


@pytest.mark.parametrize("key", ["plan", "pipeline-version"])
def test_a_config_tree_that_defines_a_reserved_key_fails_compose_naming_the_key(key, monkeypatch):
    """T-09. The render refuses it first (tests/test_authoring_contract.py); compose refuses it again for a
    config that reached a run another way."""
    from omegaconf import OmegaConf
    monkeypatch.setattr(compose.derive_tree, "compose_tree", lambda overrides=None: OmegaConf.create({key: "mine"}))
    with pytest.raises(compose.derive_tree.DeriveError) as exc:
        compose.compose_config([])
    assert str(exc.value) == (f"the config key '{key}' is reserved for the platform (reserved: plan, "
                              f"pipeline-version); rename '{key}' in your config/ tree")


def test_a_config_tree_with_a_platform_section_of_its_own_fails_compose_as_an_old_app(monkeypatch):
    from omegaconf import OmegaConf
    monkeypatch.setattr(compose.derive_tree, "compose_tree", lambda overrides=None: OmegaConf.create({"platform": {"a": 1}}))
    with pytest.raises(compose.derive_tree.DeriveError) as exc:
        compose.compose_config([])
    assert str(exc.value) == ("the config tree defines a top-level `platform` section, which the platform no longer "
                              "uses. " + OLD_APP_NOTICE)


# ---------------------------------------------------------------- config never reads the environment

SECRET = "LAKEFS_SECRET_KEY"


def tree_with(monkeypatch, value, section="train"):
    from omegaconf import OmegaConf
    monkeypatch.setattr(compose.derive_tree, "compose_tree",
                        lambda overrides=None: OmegaConf.create({section: {"note": value}, "experiment": {"name": "e"}}))


@pytest.mark.parametrize("value, resolver", [("${oc.env:LAKEFS_SECRET_KEY,none}", "oc.env"),
                                             ("prefix-${oc.env:LAKEFS_SECRET_KEY}", "oc.env"),
                                             ("${ oc.env : LAKEFS_SECRET_KEY }", "oc.env"),
                                             ("${now:%Y}", "now"), ("${oc.decode:'1'}", "oc.decode")])
def test_a_resolver_in_the_config_is_refused_naming_the_key_before_anything_is_resolved(monkeypatch, value, resolver):
    monkeypatch.setenv(SECRET, "hunter2")
    tree_with(monkeypatch, value)
    with pytest.raises(compose.derive_tree.DeriveError) as exc:
        compose.compose_config([], None, ["train"])
    assert str(exc.value) == (f"config value 'train.note' uses the resolver `${{{resolver}:...}}`. Config is not a place "
                              "for secrets or platform values: a resolver reads the environment or the files of the "
                              "machine that composes the run, so none is allowed. Write the value in config/, or "
                              "choose it on the form.")
    assert "hunter2" not in str(exc.value)


def test_a_resolver_in_the_advanced_override_or_a_form_value_is_refused_too(monkeypatch):
    monkeypatch.setenv(SECRET, "hunter2")
    tree_with(monkeypatch, "plain")
    advanced, _ = compose.split_plan("train: {note: '${oc.env:LAKEFS_SECRET_KEY}'}")
    with pytest.raises(compose.derive_tree.DeriveError, match="config value 'train.note' uses the resolver"):
        compose.compose_config([], advanced, ["train"])
    tree_with(monkeypatch, "${oc.env:LAKEFS_SECRET_KEY}", section="probe")
    with pytest.raises(compose.derive_tree.DeriveError, match="config value 'probe.note'"):
        compose.compose_config([])


def test_a_reference_to_another_config_value_is_not_a_resolver(monkeypatch):
    from omegaconf import OmegaConf
    monkeypatch.setattr(compose.derive_tree, "compose_tree", lambda overrides=None: OmegaConf.create(
        {"train": {"lr": 0.1, "again": "${train.lr}", "relative": "${.lr}", "literal": "\\${not.a.reference}"}}))
    out = compose.compose_config([], None, ["train"])
    assert out["train"]["again"] == 0.1 and out["train"]["relative"] == 0.1


def test_even_a_value_that_gets_through_cannot_see_the_environment(monkeypatch):
    """Belt and braces: the resolver check is bypassed here, so the resolve itself must find nothing. The
    environment is back afterwards: the run's inputs are pinned with the credentials."""
    from omegaconf import OmegaConf
    monkeypatch.setenv(SECRET, "hunter2")
    monkeypatch.setattr(compose.derive_tree, "refuse_resolvers", lambda *a, **k: None)
    tree_with(monkeypatch, "${oc.env:LAKEFS_SECRET_KEY,none}")
    out = compose.compose_config([], None, ["train"])
    assert out["train"]["note"] == "none" and "hunter2" not in json.dumps(out)
    assert compose.os.environ[SECRET] == "hunter2"
    tree_with(monkeypatch, "${oc.env:LAKEFS_SECRET_KEY}")
    with pytest.raises(Exception, match="Environment variable 'LAKEFS_SECRET_KEY' not found"):   # noqa: B017  no default and no variable: an error, never the value
        compose.compose_config([], None, ["train"])
    assert compose.os.environ[SECRET] == "hunter2", "the environment is restored when the resolve fails too"
    assert OmegaConf is not None


def test_the_render_refuses_a_resolver_before_it_resolves_anything(tmp_path, monkeypatch):
    from helpers import copy_app, run_render
    repo = copy_app(tmp_path / "app")
    with (repo / "config" / "experiment.yaml").open("a") as f:
        f.write("  token: ${oc.env:LAKEFS_SECRET_KEY,none}\n")
    done = run_render(repo, extra_env={SECRET: "hunter2"})
    assert done.returncode == 1 and not (repo / "out").exists()
    assert "config value 'experiment.token' uses the resolver `${oc.env:...}`. Config is not a place for secrets" in done.stderr
    assert "hunter2" not in done.stderr


# ---------------------------------------------------------------- the size cap

def test_a_context_over_its_cap_ends_the_run_naming_the_step_and_the_limit():
    """The cap is counted by cost and leaves 8 KiB for each needed step's result."""
    table = steps(("prepare", [], 0, []), ("train", [], 1, []))
    big = {"blob": "x" * 91000}
    with pytest.raises(compose.ComposeError) as exc:
        compose.step_contexts(table, "run-1", "demo", "vision", big, {})
    message = str(exc.value)
    assert message.startswith("step 'train': its context costs ") and "; the limit is 90112 (98304 less 8192 for each " \
        "of the 1 steps it needs)" in message
    assert "The config sections it reads (blob) are " in message
    assert message.endswith("Move large values out of the config into a dataset or an artifact, or read fewer sections")
    alone = compose.step_contexts(table[:1], "run-1", "demo", "vision", big, {})
    assert cost(alone["prepare"]) <= 98304, "the same config fits a step that needs none"


def test_the_cap_is_exact_and_counts_the_upstream_keys():
    """One byte under the limit composes, one over does not; the names of the needed steps are in the count."""
    from kubecore.step_contract import base_cost
    table = steps(("train", [], 3, []))

    def at(fill):
        return compose.step_contexts(table, "run-1", "demo", "vision", {"blob": "x" * fill}, {})["train"]
    room = 98304 - 3 * 8192 - base_cost(at(0), table[0]["needs"])
    assert base_cost(at(room), table[0]["needs"]) == 73728
    with pytest.raises(compose.ComposeError, match="its context costs 73729 bytes; the limit is 73728"):
        at(room + 1)


def test_the_cap_counts_what_the_value_costs_not_its_bytes():
    """Quotes cost two bytes where the workflow engine carries the context: many short strings fill the cap sooner."""
    table = steps(("prepare", [], 0, []))
    quotes = {"k": [""] * 24000}          # about 72 KiB of text, about 120 KiB by cost
    assert len(wire(quotes)) < 98304 < cost(wire(quotes))
    with pytest.raises(compose.ComposeError, match="its context costs"):
        compose.step_contexts(table, "run-1", "demo", "vision", quotes, {})


def test_a_config_value_json_cannot_hold_ends_the_run():
    with pytest.raises(compose.ComposeError, match="the resolved config cannot be handed to the steps"):
        compose.step_contexts(steps(("prepare", [], 0, [])), "run-1", "demo", "vision", {"lr": float("inf")}, {})


# ---------------------------------------------------------------- the entry point the compose image runs

def _compose(tmp_path, *args, env=None):
    base = {"KAOS_ENGINE_DIR": str(ENGINE), "KUBECORE_CONFIG_DIR": str(TEST_APP / "config"),
            "KUBECORE_SCHEMA_PROFILE": "hera", "PATH": "/usr/bin:/bin"}
    code = (f"import sys; sys.path.insert(0, {str(ENGINE)!r}); from kaos_engine import compose; "
            "sys.exit(compose.main(sys.argv[1:]))")
    return subprocess.run([sys.executable, "-I", "-c", code, *args], capture_output=True, text=True,
                          env={**base, **(env or {})}, cwd=tmp_path)


STEP_TABLE = wire([{"name": "hello-world", "kind": "transform", "reads": ["experiment"], "inputs": [], "needs": [],
                    "produces": [{"name": "report", "type": "artifact"}]}])


def test_the_compose_entry_point_writes_params_each_context_and_the_ledger(tmp_path):
    out, ledger, contexts_dir = tmp_path / "params.yaml", tmp_path / "ledger.json", tmp_path / "context"
    done = _compose(tmp_path, "--require=experiment", "--output", str(out), "--run", "run-1", "--app", "demo",
                    "--project", "vision", "--steps", STEP_TABLE, "--context-dir", str(contexts_dir),
                    "--ledger-output", str(ledger), "--advanced", "experiment: {name: exp-2}")
    assert done.returncode == 0, done.stderr[-2000:]
    assert "experiment" in yaml.safe_load(out.read_text())
    assert ledger.read_text() == '{"version":2,"reads":[],"writes":[]}', "no step declares an input here"
    text = (contexts_dir / "hello-world.json").read_text()
    context = json.loads(text)
    assert text == wire(context), "wire form, as written"
    assert context["step"] == "hello-world" and context["kind"] == "transform" and context["experiment"] == "exp-2"
    assert context["produces"] == [{"name": "report", "type": "artifact", "repo": "outputs-demo"}]
    assert "upstream" not in context and list(context["params"]) == ["experiment"]
    assert "kaos-engine: version=" in done.stderr


def test_the_compose_entry_point_needs_the_runs_identity_to_write_contexts(tmp_path):
    done = _compose(tmp_path, "--output", str(tmp_path / "p.yaml"), "--steps", STEP_TABLE)
    assert done.returncode == 2 and "--steps needs --run, --app, --project, --context-dir" in done.stderr


def test_an_input_that_cannot_be_pinned_stops_the_run_and_publishes_nothing(tmp_path):
    """The engine's own test environment has no kaos-lib: the run ends, as it would without credentials."""
    table = wire([{"name": "hello-world", "kind": "transform", "reads": [], "inputs": ["dataset"], "needs": [], "produces": []}])
    done = _compose(tmp_path, "--output", str(tmp_path / "p.yaml"), "--run", "r", "--app", "a", "--project", "p",
                    "--steps", table, "--context-dir", str(tmp_path / "context"),
                    "--ledger-output", str(tmp_path / "ledger.json"))
    assert done.returncode == 1 and "compose-and-validate FAILED: cannot resolve the run's inputs" in done.stderr
    assert not (tmp_path / "p.yaml").exists() and not (tmp_path / "context").exists()
    assert not (tmp_path / "ledger.json").exists()


# ---------------------------------------------------------------- a step without a built image never starts a run

GOOD = "zot.example:5000/proj/app:train-0a1b2c3"


@pytest.mark.parametrize("ref", [GOOD, "registry.example/p/a/b:v1", "r.example/p/a@sha256:" + "a" * 64,
                                 "r.example/p/a:v1@sha256:" + "a" * 64, "localhost:5000/x:y", "10.0.0.1:5000/x/y:1",
                                 "r.example/a_b/c.d:V1.2-rc_3", "r.example/p/a:" + "t" * 128, GOOD + "\n"])
def test_real_image_references_pass(ref):
    compose.check_step_images(json.dumps({"train": ref}))
    compose.check_step_images(None)
    compose.check_step_images("")


@pytest.mark.parametrize("ref", [":probe-learn-latest", "", "probe-learn-latest", "/proj/app:tag", "zot.example/proj/app",
                                 "zot.example/:tag", "zot.example/proj/app:", "{{workflow.parameters.image-x}}", None, 7,
                                 "r.example/p/a:" + "t" * 129, "r.example/p/a:v1\nextra"])
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
    done = _compose(tmp_path, "--output", str(out),
                    env={"KAOS_STEP_IMAGES": json.dumps({"probe-learn": ":probe-learn-latest"})})
    assert done.returncode == 1 and "no image has been built yet for: probe-learn" in done.stderr
    assert not out.exists(), "a refused run publishes nothing"


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


def test_the_engine_entry_point_says_so_when_the_engine_has_no_compose(tmp_path):
    broken = tmp_path / "engine"
    (broken / "kubecore").mkdir(parents=True)
    (broken / "kubecore" / "__init__.py").write_text("")
    done = _compose(tmp_path, "--output", str(tmp_path / "p.yaml"), env={"KAOS_ENGINE_DIR": str(broken)})
    assert done.returncode == 4 and "has no kubecore/compose.py" in done.stderr and "Traceback" not in done.stderr


def test_the_engine_image_carries_what_kaos_lib_needs_so_the_compose_image_adds_only_kaos_lib():
    """The compose image installs kaos-lib with --no-deps: every library kaos-lib imports must already be in
    the engine's pinned, scanned set, or the compose step fails at import (or cannot read a shared dataset)."""
    pinned = {line.split("==")[0].strip().lower() for line in (ENGINE / "requirements.txt").read_text().splitlines()
              if "==" in line and not line.startswith("#")}
    assert {"requests", "pyyaml", "packaging", "pyjwt", "cryptography"} <= pinned


# ---------------------------------------------------------------- the form's free text, checked before any step

def test_the_override_is_refused_above_16_kib():
    ok = "train: {note: " + "x" * 16000 + "}"
    assert compose.split_plan(ok)[0] is not None
    with pytest.raises(compose.ComposeError) as exc:
        compose.split_plan("train: {note: " + "x" * 16400 + "}")
    assert str(exc.value) == "the override is too large; put the values in the app's config"


@pytest.mark.parametrize("value, why", [
    ("dataset-a@" + "v" * 256, "the version is 256 characters, at most 255"),
    ("dataset-" + "a" * 56 + "@v1", "the repository id 'dataset-" + "a" * 56 + "' is 64 characters, at most 63"),
    ("other/dataset-" + "a" * 56 + "@v1", "the repository id 'dataset-" + "a" * 56 + "' is 64 characters, at most 63"),
])
def test_a_dataset_reference_over_the_limits_ends_the_run_naming_the_input(value, why):
    ds = FakeDatasets(error=RuntimeError("must not be asked"))
    with pytest.raises(compose.ComposeError) as exc:
        compose.pin_inputs({"raw": value}, steps(("prepare", ["raw"], 0, [])), datasets=ds)
    assert str(exc.value) == f"input 'raw' (step prepare): the reference is not valid: {why}"
    assert compose.pin_inputs({"raw": "dataset-a@" + "v" * 255}, steps(("prepare", ["raw"], 0, [])),
                              datasets=FakeDatasets())["raw"]["ref"] == "v" * 255


# ---------------------------------------------------------------- a produced dataset is named per app

def test_two_apps_that_declare_the_same_dataset_name_get_two_repositories():
    table = steps(("prepare", [], 0, [{"name": "clean", "type": "dataset"}]))
    repos = {}
    for app in ("hera1", "hera2"):
        reads = {}
        text = compose.step_contexts(table, "run-1", app, "vision", RESOLVED, reads, {"promote": {"clean": True}})["prepare"]
        repos[app] = json.loads(text)["produces"][0]
    assert repos == {"hera1": {"name": "clean", "type": "dataset", "repo": "dataset-hera1-clean", "promote": True},
                     "hera2": {"name": "clean", "type": "dataset", "repo": "dataset-hera2-clean", "promote": True}}


def test_the_repository_is_named_in_one_place_and_models_keep_the_outputs_repository():
    from kubecore import step_contract as c
    assert c.dataset_repo("hera1", "clean") == c.produced_repo("hera1", "dataset", "clean") == "dataset-hera1-clean"
    assert c.produced_repo("hera1", "model", "m") == c.produced_repo("hera1", "artifact", "a") == "outputs-hera1"


def test_a_name_the_lake_cannot_hold_ends_the_run_naming_both_names_and_the_limit():
    table = steps(("prepare", [], 0, [{"name": "n" * 40, "type": "dataset"}]))
    ok_app = "a" * 7                                                   # 7 + 1 + 40 = 48
    compose.step_contexts(table, "r", ok_app, "p", RESOLVED, {})
    with pytest.raises(compose.ComposeError) as exc:
        compose.step_contexts(table, "r", "a" * 8, "p", RESOLVED, {})  # 49
    assert str(exc.value) == (f"step 'prepare': dataset('{'n' * 40}') in app '{'a' * 8}' is the repository "
                              f"'dataset-{'a' * 8}-{'n' * 40}', and the lake allows 3 to 48 characters after 'dataset-': "
                              f"'{'a' * 8}-{'n' * 40}' has 49. Shorten the dataset name ('{'n' * 40}') or the app's name "
                              f"('{'a' * 8}').")
