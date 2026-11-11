"""The authoring keywords of the step contract, and every refusal with its message (PRD kaos#1911 D-15, AC-01).

A pipeline that cannot run fails when it is rendered, naming the step and what to write instead.
"""

import textwrap

import pytest
import yaml

from helpers import copy_app, run_render


def render(build):
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        build(authoring)
    return yaml.safe_load(p.wt.to_yaml())


def refused(build):
    from kubecore import authoring, derive_tree
    with pytest.raises((authoring.AuthoringError, derive_tree.DeriveError)) as exc:
        render(build)
    return str(exc.value)


# ---------------------------------------------------------------- kind=

def test_a_step_without_a_kind_fails_the_render_naming_the_kinds_and_the_guide():
    from kubecore import authoring
    message = refused(lambda a: a.step("train"))
    assert message == ("step 'train' has no kind=. Every step declares what it does: kind= is one of ingest, "
                       "transform, train, evaluate, publish, control (for example step('train', kind='transform', "
                       f"...)). See {authoring.MIGRATION_GUIDE_URL}")


def test_an_unknown_kind_fails_the_render_naming_the_kinds():
    message = refused(lambda a: a.step("train", kind="training"))
    assert message == ("step 'train': kind='training' is not a step kind. Use one of ingest, transform, train, "
                       "evaluate, publish, control.")


@pytest.mark.parametrize("kind", ["ingest", "transform", "train", "evaluate", "publish", "control"])
def test_every_pod_carries_its_real_kind(kind):
    wft = render(lambda a: a.step("one", kind=kind))
    labels = next(t for t in wft["spec"]["templates"] if t["name"] == "one")["metadata"]["labels"]
    assert labels == {"platform.kubecore.io/step": "one", "platform.kubecore.io/step-kind": kind}
    compose = next(t for t in wft["spec"]["templates"] if t["name"] == "compose-and-validate")
    assert compose["metadata"]["labels"]["platform.kubecore.io/step-kind"] == "control"


def test_the_keywords_after_the_name_are_given_by_name():
    """The second position meant reads= before the contract: a positional value must not land in kind=."""
    from kubecore import authoring
    with pytest.raises(TypeError):
        with authoring.pipeline("p"):
            authoring.step("one", ["experiment"])
    with pytest.raises(TypeError, match="unexpected keyword argument 'walltime_minutes'"):
        with authoring.pipeline("p"):
            authoring.step("one", kind="train", walltime_minutes=5)


# ---------------------------------------------------------------- removed keywords (AC-01)

def test_outputs_fails_the_render_with_the_migration_message():
    from kubecore import authoring
    message = refused(lambda a: a.step("prepare", kind="transform", outputs=["dataset-info"]))
    assert message == (
        "step 'prepare': outputs= was removed. A step hands its values on with kaos_lib.step.result(...) and the "
        "steps that need it read them with kaos_lib.step.upstream('prepare'); what the step writes is declared "
        f"with produces=[model(...), artifact(...), dataset(...)]. See {authoring.MIGRATION_GUIDE_URL}")


def test_hpc_time_limit_fails_the_render_with_the_migration_message():
    from kubecore import authoring
    message = refused(lambda a: a.step("train", kind="train", hpc_time_limit="12h"))
    assert message == ("step 'train': hpc_time_limit= was removed. Write walltime= with the same value "
                       f"(walltime='12h'). See {authoring.MIGRATION_GUIDE_URL}")


def test_an_app_on_the_old_contract_fails_the_render_and_writes_nothing(tmp_path):
    """AC-01, as CI runs it: exit 1, the migration message on stderr, no rendered template."""
    repo = copy_app(tmp_path / "app")
    (repo / "pipeline.py").write_text(textwrap.dedent("""
        from pathlib import Path
        from kubecore.authoring import pipeline, step
        with pipeline("old") as p:
            load = step("dataset-loading", reads=["dataset"], outputs=["dataset-info"])
            step("hello-world", reads=["experiment", "dataset"], needs=[load], hpc_time_limit="2h")
        p.write(Path(__file__).parent / "out" / "raw-workflow-template.yaml")
    """))
    done = run_render(repo)
    assert done.returncode == 1
    assert "outputs= was removed" in done.stderr and "/guides/migrate-to-the-step-contract" in done.stderr
    assert not (repo / "out").exists()


def test_walltime_takes_the_values_hpc_time_limit_took():
    wft = render(lambda a: a.step("train", kind="train", walltime="12h"))
    step = next(t for t in wft["spec"]["templates"] if t["name"] == "train")
    assert step["metadata"]["annotations"] == {"platform.kubecore.io/hpc": "12h"}
    assert "walltime='1d' must be minutes or hours" in refused(lambda a: a.step("train", kind="train", walltime="1d"))


# ---------------------------------------------------------------- produces=

def test_produces_takes_models_artifacts_and_datasets():
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        authoring.step("train", kind="train", produces=[authoring.model("model"), authoring.artifact("checkpoints"),
                                                        authoring.dataset("embeddings")])
    assert [(e.name, e.type) for e in p.steps[0].produces] == [
        ("model", "model"), ("checkpoints", "artifact"), ("embeddings", "dataset")]


def test_a_dataset_stored_as_a_variant_of_its_source_is_refused_until_it_ships():
    message = refused(lambda a: a.step("clean", kind="transform", produces=[a.dataset("clean", of="raw")]))
    assert message == ("dataset('clean', of='raw'): of= is reserved for a dataset stored as a variant of its source, "
                       "which is not available yet. Write dataset('clean'): the step then writes its own dataset.")


def test_produces_holds_only_what_the_three_helpers_make():
    message = refused(lambda a: a.step("train", kind="train", produces=["model"]))
    assert message.startswith("step 'train': produces= holds 'model'. Declare what the step writes with model('name')")
    assert "more than once" in refused(
        lambda a: a.step("train", kind="train", produces=[a.model("m"), a.artifact("m")]))


@pytest.mark.parametrize("make, name", [("model", "kaos-model"), ("artifact", "kaos-result"), ("dataset", "kaos-x")])
def test_a_produced_name_may_not_start_with_the_platforms_prefix(make, name):
    message = refused(lambda a: a.step("one", kind="train", produces=[getattr(a, make)(name)]))
    assert message == (f"{make}('{name}'): names that start with 'kaos-' are reserved for the platform. "
                       "Choose another name.")


@pytest.mark.parametrize("make, name", [("dataset", "Raw"), ("dataset", "a" * 56), ("dataset", "raw.clean"),
                                        ("model", "a/b"), ("artifact", ""), ("model", 7)])
def test_a_produced_name_must_be_storable(make, name):
    assert "the name is invalid" in refused(lambda a: a.step("one", kind="train", produces=[getattr(a, make)(name)]))


def test_a_dataset_has_one_producer_in_the_pipeline():
    """CON-02: a second step that writes the same dataset is refused."""
    def build(a):
        a.step("one", kind="transform", produces=[a.dataset("embeddings")])
        a.step("two", kind="transform", produces=[a.dataset("embeddings")])
    assert refused(build) == ("step 'two': dataset('embeddings') is already produced by step 'one'. A dataset has "
                              "one writer: exactly one step of one pipeline produces it.")


# ---------------------------------------------------------------- inputs= and needs=

def test_an_input_names_a_top_level_config_value():
    wft = render(lambda a: a.step("load", kind="ingest", inputs=["dataset"]))
    assert wft["kind"] == "WorkflowTemplate"
    assert refused(lambda a: a.step("load", kind="ingest", inputs=["raw"])).startswith(
        "step 'load' declares the input 'raw', but the config tree has no top-level value 'raw' "
        "(top-level keys: experiment, dataset). Add `raw: <dataset>@<version>` to config/")
    assert refused(lambda a: a.step("load", kind="ingest", inputs=["experiment"])).startswith(
        "step 'load' declares the input 'experiment', but the config value 'experiment' is a section.")
    assert "inputs= holds 'data.raw'" in refused(lambda a: a.step("load", kind="ingest", inputs=["data.raw"]))
    assert "inputs= names dataset more than once" in refused(
        lambda a: a.step("load", kind="ingest", inputs=["dataset", "dataset"]))


def test_an_input_and_something_a_needed_step_produces_may_not_share_a_name():
    def build(a):
        prepare = a.step("prepare", kind="transform", produces=[a.dataset("dataset")])
        a.step("train", kind="train", inputs=["dataset"], needs=[prepare])
    assert refused(build) == (
        "step 'train': dataset is both an input (inputs=) and something the step it needs, 'prepare', produces. "
        "A step reads both by name, so the two must differ: rename the config value or the produced thing.")


def test_two_needed_steps_may_not_produce_the_same_name():
    def build(a):
        one = a.step("one", kind="train", produces=[a.model("model")])
        two = a.step("two", kind="train", produces=[a.artifact("model")])
        a.step("three", kind="evaluate", needs=[one, two])
    assert refused(build) == (
        "step 'three': the steps it needs, 'one' and 'two', both produce 'model'. A step reads what the steps it "
        "needs wrote by name, so each name can come from one of them only: rename one.")

    def fine(a):   # the same name from steps this one does not both need
        one = a.step("one", kind="train", produces=[a.model("model")])
        a.step("two", kind="train", produces=[a.artifact("model")])
        a.step("three", kind="evaluate", needs=[one])
    assert render(fine)


def test_a_step_may_need_at_most_ten_steps():
    """Each needed step's result takes 8 KiB of the context; ten leave the 16 KiB the rest needs."""
    def build(count):
        def inner(a):
            a.step("last", kind="evaluate", needs=[a.step(f"s{i}", kind="transform") for i in range(count)])
        return inner
    assert render(build(10))
    assert refused(build(11)).startswith(
        "step 'last' needs 11 steps; a step may need at most 10. Each needed step's result takes room in this "
        "step's context, and 11 would leave under 16384 bytes for the rest.")


def test_a_control_step_takes_no_data():
    for build in (lambda a: a.step("gate", kind="control", inputs=["dataset"]),
                  lambda a: a.step("gate", kind="control", produces=[a.artifact("report")])):
        assert refused(build).startswith(
            "step 'gate': a control step steers the run and handles no data, so it takes no inputs= and no produces=.")
    assert render(lambda a: a.step("gate", kind="control", reads=["experiment"]))


# ---------------------------------------------------------------- reserved names (T-09)

@pytest.mark.parametrize("name", ["x-hpc", "hpc-hpc", "compose-and-validate", "train-hpc", "main", "hpc-follower",
                                  "kaos-step", "kaos-result"])
def test_a_step_may_not_take_a_name_the_platform_keeps(name):
    message = refused(lambda a: a.step(name, kind="train"))
    assert message.startswith(f"step name '{name}' is reserved") and "'kaos-'" in message


def test_a_step_called_hpc_is_fine():
    assert render(lambda a: a.step("hpc", kind="train"))


@pytest.mark.parametrize("key", ["plan", "pipeline-version"])
def test_a_config_key_the_platform_reserves_fails_the_render_naming_the_key(key, tmp_path):
    repo = copy_app(tmp_path / "app")
    with (repo / "config" / "experiment.yaml").open("a") as f:
        f.write(f"{key}: mine\n")
    done = run_render(repo)
    assert done.returncode == 1
    assert (f"the config key '{key}' is reserved for the platform (reserved: plan, pipeline-version); "
            f"rename '{key}' in your config/ tree") in done.stderr


def test_a_config_leaf_whose_form_field_would_be_a_reserved_name_fails_the_render(tmp_path):
    repo = copy_app(tmp_path / "app")
    with (repo / "config" / "experiment.yaml").open("a") as f:
        f.write("pipeline:\n  version: 3\n")
    done = run_render(repo)
    assert done.returncode == 1
    assert ("config leaf pipeline.version becomes the form field 'pipeline-version', a name the platform reserves "
            "(plan, pipeline-version); rename the config key") in done.stderr


# ---------------------------------------------------------------- when=

def test_a_step_that_needs_a_conditional_step_waits_for_it_to_end_either_way():
    def build(a):
        export = a.step("export", kind="transform", when="{{workflow.parameters.experiment-name}} == yes")
        a.step("evaluate", kind="evaluate", needs=[export])
    wft = render(build)
    tasks = {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}
    assert tasks["evaluate"]["depends"] == ("compose-and-validate && "
                                            "(export.Succeeded || export.Skipped || export.Omitted)")
    argument = tasks["evaluate"]["arguments"]["parameters"][0]
    assert argument["name"] == "kaos-context"
    assert ('sprig.default("{\\"version\\":1,\\"status\\":\\"absent\\"}", '
            'tasks["export"].outputs.parameters["kaos-result"])') in argument["value"]


# ---------------------------------------------------------------- names and counts

@pytest.mark.parametrize("name", ["Train", "train_model", "a" * 41, "-train", "train-", "tr.ain", ""])
def test_a_step_name_is_lowercase_letters_digits_and_hyphens_at_most_40(name):
    assert f"step name {name!r} is invalid" in refused(lambda a: a.step(name, kind="train"))
    assert render(lambda a: a.step("a" * 40, kind="train"))


@pytest.mark.parametrize("make", ["model", "artifact", "dataset"])
@pytest.mark.parametrize("name", ["Best", "best_model", "a" * 41, "v1.0"])
def test_a_produced_name_follows_the_same_rule(make, name):
    message = refused(lambda a: a.step("one", kind="train", produces=[getattr(a, make)(name)]))
    assert message.startswith(f"{make}({name!r}): the name is invalid — use lowercase letters, digits and hyphens, "
                              "at most 40 characters")


@pytest.mark.parametrize("name", ["train_data", "Raw", "a" * 41])
def test_an_input_name_follows_the_same_rule(name):
    message = refused(lambda a: a.step("one", kind="train", inputs=[name]))
    assert f"inputs= holds {name!r}" in message and message.endswith("at most 40 characters. Rename the config value.")


def test_an_input_name_may_not_start_with_the_platforms_prefix():
    assert refused(lambda a: a.step("one", kind="train", inputs=["kaos-data"])) == (
        "step 'one': the input 'kaos-data' starts with 'kaos-', which is reserved for the platform. "
        "Rename the config value.")


def test_a_step_may_read_at_most_sixteen_datasets_by_name(tmp_path, monkeypatch):
    """Its inputs= and the datasets the steps it needs produce, together."""
    config = copy_app(tmp_path / "app") / "config"
    with (config / "dataset.yaml").open("a") as f:
        f.writelines(f"in{i}: dataset-default@latest\n" for i in range(16))
    from kubecore import derive_tree
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", config)
    names = [f"in{i}" for i in range(16)]

    def build(inputs, extra):
        def inner(a):
            up = a.step("up", kind="transform", produces=[a.dataset(f"made{i}") for i in range(extra)] + [a.model("m")])
            a.step("last", kind="train", inputs=inputs, needs=[up])
        return inner
    assert render(build(names, 0)) and render(build(names[:10], 6))
    assert refused(build(names, 1)) == (
        "step 'last' can read 17 datasets by name (16 in inputs= and 1 produced by the steps it needs); a step may "
        "have at most 16. Split the step, or join datasets it reads.")
