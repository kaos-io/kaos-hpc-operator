"""The authoring keywords of the step contract, and every refusal with its message (PRD kaos#1911 D-15, AC-01).

A pipeline that cannot run fails when it is rendered, naming the step and what to write instead.
"""

import textwrap

import pytest
import yaml

from helpers import copy_app, run_render, valid
from kubecore.step_contract import OLD_APP_MARKER, OLD_APP_NOTICE


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

def test_a_step_without_a_kind_is_an_app_from_before_the_contract():
    message = refused(lambda a: a.step("train"))
    assert message == ("step 'train' has no kind=, which every step declares: one of ingest, transform, train, "
                       "evaluate, publish, control. " + OLD_APP_NOTICE)


def test_a_pipeline_where_every_step_lacks_a_kind_is_an_app_from_before_the_contract():
    message = refused(lambda a: (a.step("one"), a.step("two"), a.step("three")))
    assert message == ("step 'one' has no kind=, which every step declares: one of ingest, transform, train, "
                       "evaluate, publish, control. " + OLD_APP_NOTICE)


def test_one_step_without_a_kind_among_steps_that_have_one_is_not_an_old_app():
    message = refused(lambda a: (a.step("one", kind="evaluate"), a.step("two"), a.step("three", kind="evaluate")))
    assert message == ("step 'two' has no kind=, which every step declares: one of ingest, transform, train, "
                       "evaluate, publish, control.")
    assert OLD_APP_MARKER not in message


def test_an_unknown_kind_fails_the_render_naming_the_kinds():
    message = refused(lambda a: a.step("train", kind="training"))
    assert message == ("step 'train': kind='training' is not a step kind. Use one of ingest, transform, train, "
                       "evaluate, publish, control.")


@pytest.mark.parametrize("kind", ["ingest", "transform", "train", "evaluate", "publish", "control"])
def test_every_pod_carries_its_real_kind(kind):
    wft = render(lambda a: a.step("one", kind=kind, **valid(a, kind)))
    labels = next(t for t in wft["spec"]["templates"] if t["name"] == "one")["metadata"]["labels"]
    labels.pop("platform.kubecore.io/produces-dataset", None)    # the ingest step produces a dataset
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
            authoring.step("one", kind="evaluate", walltime_minutes=5)


# ---------------------------------------------------------------- removed keywords (AC-01)

def test_outputs_says_what_to_write_instead():
    message = refused(lambda a: a.step("prepare", kind="evaluate", outputs=["dataset-info"]))
    assert message == ("step 'prepare': outputs= is gone. Declare what the step writes with produces=[...] and "
                       "hand it on with step.result().")


def test_hpc_time_limit_says_what_to_write_instead():
    message = refused(lambda a: a.step("train", kind="evaluate", hpc_time_limit="12h"))
    assert message == "step 'train': hpc_time_limit= is gone. Use walltime=, for example walltime=\"12h\"."


def test_disk_says_a_step_gets_the_disk_of_its_class():
    message = refused(lambda a: a.step("train", kind="evaluate", disk="20Gi"))
    assert message == "step 'train': disk= is gone. The step gets the disk of the machine class it runs on."


def test_any_other_unknown_keyword_lists_the_keywords_that_exist():
    with pytest.raises(TypeError) as exc:
        render(lambda a: a.step("one", kind="evaluate", gpus=1))
    assert str(exc.value) == ("step() got an unexpected keyword argument 'gpus'. The keywords are: name, kind, reads, "
                              "inputs, needs, produces, gpu, when, walltime, resumable, checkpoint_every, "
                              "idle_timeout.")


def test_an_app_on_the_old_contract_fails_the_render_and_writes_nothing(tmp_path):
    """AC-01, as CI runs it: exit 1, the old-app message on stderr, no rendered template."""
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
    assert "step 'dataset-loading': outputs= is gone." in done.stderr
    assert not (repo / "out").exists()


def test_walltime_takes_the_values_hpc_time_limit_took():
    wft = render(lambda a: a.step("train", kind="evaluate", walltime="12h"))
    step = next(t for t in wft["spec"]["templates"] if t["name"] == "train")
    assert step["metadata"]["annotations"] == {"platform.kubecore.io/hpc": "12h"}
    assert "walltime='1d' must be minutes or hours" in refused(lambda a: a.step("train", kind="evaluate", walltime="1d"))


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
    wft = render(lambda a: a.step("load", kind="ingest", inputs=["dataset"], **valid(a, "ingest")))
    assert wft["kind"] == "WorkflowTemplate"
    assert refused(lambda a: a.step("load", kind="ingest", inputs=["raw"], **valid(a, "ingest"))).startswith(
        "step 'load' declares the input 'raw', but the config tree has no top-level value 'raw' "
        "(top-level keys: experiment, dataset). Add `raw: <dataset>@<version>` to config/")
    assert refused(lambda a: a.step("load", kind="ingest", inputs=["experiment"], **valid(a, "ingest"))).startswith(
        "step 'load' declares the input 'experiment', but the config value 'experiment' is a section.")
    assert "inputs= holds 'data.raw'" in refused(
        lambda a: a.step("load", kind="ingest", inputs=["data.raw"], **valid(a, "ingest")))
    assert "inputs= names dataset more than once" in refused(
        lambda a: a.step("load", kind="ingest", inputs=["dataset", "dataset"], **valid(a, "ingest")))


def test_an_input_and_something_a_needed_step_produces_may_not_share_a_name():
    def build(a):
        prepare = a.step("prepare", kind="transform", produces=[a.dataset("dataset")])
        a.step("train", kind="evaluate", inputs=["dataset"], needs=[prepare])
    assert refused(build) == (
        "step 'train': dataset is both an input (inputs=) and something the step it needs, 'prepare', produces. "
        "A step reads both by name, so the two must differ: rename the config value or the produced thing.")


def test_two_needed_steps_may_not_produce_the_same_name():
    def build(a):
        one = a.step("one", kind="train", produces=[a.model("model")])
        two = a.step("two", kind="transform", produces=[a.artifact("model")])
        a.step("three", kind="evaluate", needs=[one, two])
    assert refused(build) == (
        "step 'three': the steps it needs, 'one' and 'two', both produce 'model'. A step reads what the steps it "
        "needs wrote by name, so each name can come from one of them only: rename one.")

    def fine(a):   # the same name from steps this one does not both need
        one = a.step("one", kind="train", produces=[a.model("model")])
        a.step("two", kind="transform", produces=[a.artifact("model")])
        a.step("three", kind="evaluate", needs=[one])
    assert render(fine)


def test_a_step_may_need_at_most_ten_steps():
    """Each needed step's result takes 8 KiB of the context; ten leave the 16 KiB the rest needs."""
    def build(count):
        def inner(a):
            a.step("last", kind="evaluate", needs=[a.step(f"s{i}", kind="evaluate") for i in range(count)])
        return inner
    assert render(build(10))
    assert refused(build(11)).startswith(
        "step 'last' needs 11 steps; a step may need at most 10. Each needed step's result takes room in this "
        "step's context, and 11 would leave too little for the rest.")


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
    message = refused(lambda a: a.step(name, kind="evaluate"))
    assert message.startswith(f"step name '{name}' is reserved") and "'kaos-'" in message


def test_a_step_called_hpc_is_fine():
    assert render(lambda a: a.step("hpc", kind="evaluate"))


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
        export = a.step("export", kind="evaluate", when="experiment-name == yes")
        a.step("evaluate", kind="evaluate", needs=[export])
    wft = render(build)
    tasks = {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}
    assert tasks["evaluate"]["depends"] == ("compose-and-validate && "
                                            "(export.Succeeded || export.Skipped || export.Omitted)")
    argument = tasks["evaluate"]["arguments"]["parameters"][0]
    assert argument["name"] == "kaos-context"
    assert ('sprig.default("{\\"version\\":1,\\"status\\":\\"absent\\"}", '
            'tasks["export"].outputs.parameters["kaos-result"])') in argument["value"]


def _when_of(when, **more):
    wft = render(lambda a: a.step("export", kind="evaluate", when=when, **more))
    return {t["name"]: t for t in next(t for t in wft["spec"]["templates"] if "dag" in t)["dag"]["tasks"]}["export"]


@pytest.mark.parametrize("written, rendered", [
    ("experiment-name != off", "{{=workflow.parameters['experiment-name'] != 'off'}}"),
    ("experiment-name == true", "{{=workflow.parameters['experiment-name'] == 'true'}}"),
    ("experiment-name == 3", "{{=workflow.parameters['experiment-name'] == '3'}}"),
    ("experiment-name == \"two words\"", "{{=workflow.parameters['experiment-name'] == 'two words'}}"),
    ("experiment-name == 'two words'", "{{=workflow.parameters['experiment-name'] == 'two words'}}"),
    ("experiment-name=='x'", "{{=workflow.parameters['experiment-name'] == 'x'}}"),
    ("experiment-name == \"it's\"", "{{=workflow.parameters['experiment-name'] == 'it\\'s'}}"),
    ("experiment-name == \"a and b\" or dataset != x",
     "{{=workflow.parameters['experiment-name'] == 'a and b' || workflow.parameters['dataset'] != 'x'}}"),
    ("experiment-name == qat or experiment-name == both",
     "{{=workflow.parameters['experiment-name'] == 'qat' || workflow.parameters['experiment-name'] == 'both'}}"),
    ("experiment-description == true and experiment-name != small",
     "{{=workflow.parameters['experiment-description'] == 'true' && workflow.parameters['experiment-name'] != 'small'}}"),
    ("dataset == a or dataset == b and experiment-name == c",
     "{{=workflow.parameters['dataset'] == 'a' || workflow.parameters['dataset'] == 'b' "
     "&& workflow.parameters['experiment-name'] == 'c'}}"),
])
def test_when_is_a_run_form_field_compared_with_a_value(written, rendered):
    assert _when_of(written)["when"] == rendered


def test_a_step_without_when_has_no_condition():
    assert "when" not in _when_of(None)


def test_and_binds_tighter_than_or_in_the_rendered_condition():
    """The workflow engine's expression language has the same precedence, so the order is kept as written."""
    import re
    c = _when_of("dataset == a or dataset == b and experiment-name == c")["when"]
    assert re.search(r"'a' \|\| .* == 'b' && ", c)


def test_the_hpc_variant_keeps_the_steps_own_condition():
    from kubecore import meluxina
    assert meluxina._when_expr(_when_of("experiment-name != off")["when"]) == (
        "workflow.parameters['experiment-name'] != 'off'")


def _example():
    return 'for example when="mode != off"'


@pytest.mark.parametrize("written", [
    "experiment-name = off", "experiment-name > 3", "experiment-name", "experiment-name ==", "== off",
    "", "   ", "(experiment-name == a)", "experiment-name == a and (dataset == b)", "len(dataset) == 3",
    "experiment-name == a && dataset == b", "experiment-name == a, dataset == b", "experiment-name == two words",
    "exp$ment == a", "experiment name == a", "experiment-name == a AND dataset == b", "experiment-name == a and",
    "experiment-name == 'open",
])
def test_a_when_that_is_not_a_comparison_is_refused_with_an_example(written):
    assert refused(lambda a: a.step("export", kind="evaluate", when=written)) == (
        f"step 'export': when={written!r} is not a run-form field compared with a value, "
        f'for example when="mode != off". Join comparisons with and / or; no parentheses.')


@pytest.mark.parametrize("written", [
    "{{workflow.parameters.experiment-name}} == yes", "experiment-name == {{x}}", "{{=a == b}}",
    "experiment-name == 'a}}'",
])
def test_the_workflow_systems_own_syntax_in_when_is_refused(written):
    assert refused(lambda a: a.step("export", kind="evaluate", when=written)) == (
        "step 'export': when= is written with the name of a run-form field, for example when=\"mode != off\". "
        "Write the field's name instead of {{workflow.parameters.<field>}}.")


def test_a_when_that_is_not_text_is_refused():
    assert "is not a run-form field compared with a value" in refused(
        lambda a: a.step("export", kind="evaluate", when=True))


def test_a_when_naming_a_field_the_run_form_does_not_have_is_refused_with_the_fields_it_has():
    message = refused(lambda a: a.step("export", kind="evaluate", when="quantization-mode != off"))
    assert message.startswith("step 'export': when='quantization-mode != off' names 'quantization-mode', "
                              "which is not a field of the run form. The fields are: ")
    assert message.endswith("The fields are: experiment-name, experiment-description, dataset.")


# ---------------------------------------------------------------- names and counts

@pytest.mark.parametrize("name", ["Train", "train_model", "a" * 41, "-train", "train-", "tr.ain", ""])
def test_a_step_name_is_lowercase_letters_digits_and_hyphens_at_most_40(name):
    assert f"step name {name!r} is invalid" in refused(lambda a: a.step(name, kind="evaluate"))
    assert render(lambda a: a.step("a" * 40, kind="evaluate"))


@pytest.mark.parametrize("make", ["model", "artifact", "dataset"])
@pytest.mark.parametrize("name", ["Best", "best_model", "a" * 41, "v1.0"])
def test_a_produced_name_follows_the_same_rule(make, name):
    message = refused(lambda a: a.step("one", kind="train", produces=[getattr(a, make)(name)]))
    assert message.startswith(f"{make}({name!r}): the name is invalid — use lowercase letters, digits and hyphens, "
                              + ("3 to 40 characters" if make == "dataset" else "at most 40 characters"))


@pytest.mark.parametrize("make", ["model", "artifact", "dataset"])
def test_a_produced_name_of_40_characters_is_accepted_and_41_is_not(make):
    assert render(lambda a: a.step("one", kind="transform", produces=[getattr(a, make)("a" * 40)]))
    assert "the name is invalid" in refused(
        lambda a: a.step("one", kind="transform", produces=[getattr(a, make)("a" * 41)]))


def test_no_name_the_engine_accepts_makes_a_job_the_definition_refuses():
    """The job definition (api/v1alpha1/hpcargoworkflowjob_types.go): an input's name is at most 40 characters,
    its repo matches ^dataset-[a-z0-9][a-z0-9-]{0,54}$ within 63, and the job's app is at most 55."""
    import re

    from kubecore.step_contract import MAX_APP_NAME, MAX_NAME, dataset_name_problem, dataset_repo, produced_repo
    repo_pattern = re.compile(r"^dataset-[a-z0-9][a-z0-9-]{0,54}$")
    assert MAX_NAME == 40
    for app_len in range(1, MAX_APP_NAME + 1):
        app = "a" * app_len
        for name_len in range(3, MAX_NAME + 1):
            name = "b" * name_len
            if dataset_name_problem(app, "one", name):
                continue   # refused at render, no job is made
            repo = dataset_repo(app, name)
            assert len(repo) <= 63 and repo_pattern.match(repo), (app_len, name_len)
            assert produced_repo(app, "dataset", name) == repo
        assert len(produced_repo(app, "model", "m")) <= 63   # outputs-<app>, held by the app's 55 characters


@pytest.mark.parametrize("name", ["train_data", "Raw", "a" * 41])
def test_an_input_name_follows_the_same_rule(name):
    message = refused(lambda a: a.step("one", kind="evaluate", inputs=[name]))
    assert f"inputs= holds {name!r}" in message and message.endswith("at most 40 characters. Rename the config value.")


def test_an_input_name_may_not_start_with_the_platforms_prefix():
    assert refused(lambda a: a.step("one", kind="evaluate", inputs=["kaos-data"])) == (
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
            a.step("last", kind="evaluate", inputs=inputs, needs=[up])
        return inner
    assert render(build(names, 0)) and render(build(names[:10], 6))
    assert refused(build(names, 1)) == (
        "step 'last' can read 17 datasets by name (16 in inputs= and 1 produced by the steps it needs); a step may "
        "have at most 16. Split the step, or join datasets it reads.")


# ---------------------------------------------------------------- the platform config group is gone

def test_an_app_whose_config_still_names_the_platform_group_fails_the_render_with_what_to_delete(tmp_path):
    repo = copy_app(tmp_path / "app")
    with (repo / "config" / "config.yaml").open("a") as f:
        f.write("  - platform: live\n")
    done = run_render(repo)
    assert done.returncode == 1 and not (repo / "out").exists()
    assert ("config/config.yaml lists `- platform: live` in its defaults, and the platform no longer adds a "
            "`platform` section to an app's config. " + OLD_APP_NOTICE) in done.stderr


def test_the_same_app_fails_compose_with_the_same_message(tmp_path, monkeypatch):
    from kubecore import compose, derive_tree
    config = copy_app(tmp_path / "app") / "config"
    with (config / "config.yaml").open("a") as f:
        f.write("  - platform: live\n")
    monkeypatch.setattr(derive_tree, "CONFIG_DIR", config)
    with pytest.raises(derive_tree.DeriveError, match="lists `- platform: live` in its defaults"):
        compose.compose_config([])


def test_a_rendered_pipeline_carries_no_platform_value():
    wft = render(lambda a: a.step("one", kind="evaluate", reads=["experiment"]))
    assert "platform." not in yaml.safe_dump(wft).replace("platform.kubecore.io", "")


# ---------------------------------------------------------------- walltime= and the dataset name

@pytest.mark.parametrize("kind", ["ingest", "publish", "control"])
def test_walltime_on_a_step_that_never_runs_on_hpc_is_accepted(kind):
    wft = render(lambda a: a.step("one", kind=kind, walltime="2h", **valid(a, kind)))
    step = next(t for t in wft["spec"]["templates"] if t["name"] == "one")
    assert step["metadata"]["annotations"]["platform.kubecore.io/hpc"] == "2h"


@pytest.mark.parametrize("kind", ["transform", "train", "evaluate"])
def test_walltime_on_any_step_that_may_run_on_hpc_is_accepted_with_or_without_a_gpu(kind):
    for gpu in (False, True):
        wft = render(lambda a, kind=kind, gpu=gpu: a.step("one", kind=kind, gpu=gpu, walltime="2h", **valid(a, kind)))
        step = next(t for t in wft["spec"]["templates"] if t["name"] == "one")
        assert step["metadata"]["annotations"]["platform.kubecore.io/hpc"] == "2h"


def test_a_produced_name_may_be_written_through_a_module_attribute():
    """produces=[au.model("m")] is the same call as model("m"): the entries are what counts."""
    from kubecore import authoring as au
    with au.pipeline("p") as p:
        au.step("one", kind="train", produces=[au.model("m"), au.dataset("clean")])
    assert [(e.name, e.type) for e in p.steps[0].produces] == [("m", "model"), ("clean", "dataset")]


@pytest.mark.parametrize("name", ["a", "ab"])
def test_a_dataset_name_is_three_to_forty_characters(name):
    message = refused(lambda a: a.step("one", kind="transform", produces=[a.dataset(name)]))
    assert message.startswith(f"dataset({name!r}): the name is invalid — use lowercase letters, digits and hyphens, "
                              "3 to 40 characters")
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        authoring.step("one", kind="transform", produces=[authoring.dataset("abc"), authoring.dataset("a" * 40)])
    assert len(p.steps[0].produces) == 2
    assert [e.name for e in render_produced("model", "ab")] == ["ab"], "a model or artifact may have a short name"


def render_produced(make, name):
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        authoring.step("one", kind="train", produces=[getattr(authoring, make)(name)])
    return p.steps[0].produces


# ---------------------------------------------------------------- review findings

def test_a_step_may_not_name_the_same_needed_step_twice():
    def build(a):
        up = a.step("up", kind="evaluate")
        a.step("last", kind="evaluate", needs=[up, up])
    assert refused(build) == "step 'last': needs= names up more than once."


@pytest.mark.parametrize("value", ["0", "0m", "0h", "00"])
def test_walltime_of_zero_is_refused_at_render_for_every_step(value):
    for kind in ("train", "transform", "evaluate", "ingest"):
        message = refused(lambda a, kind=kind: a.step("one", kind=kind, walltime=value, **valid(a, kind)))
        assert message == (f"step 'one': walltime={value!r} must be minutes or hours as a string like '90m', '12h' or "
                           "'720', at least one minute (the step's time limit, wherever it runs; the step is "
                           "ended when it elapses).")
    assert render(lambda a: a.step("one", kind="evaluate", walltime="1m"))


@pytest.mark.parametrize("value", [90, "90s", "1d", "", "-5", None.__class__])
def test_a_walltime_of_the_wrong_shape_is_refused_at_render(value):
    assert "must be minutes or hours as a string" in refused(lambda a: a.step("one", kind="evaluate", walltime=value))


def test_no_message_a_client_reads_names_the_workflow_engine():
    """The messages of authoring and the schema a linter reads are for the people who write apps."""
    import json

    from kubecore import authoring
    from paths import ENGINE
    schema = (ENGINE / "kaos_pipeline_engine" / "authoring-schema.json").read_text()
    assert "Argo" not in schema and "Workflow" not in json.loads(schema)["doc"]
    for build in (lambda a: a.step("Bad Name", kind="evaluate"), lambda a: a.step("one"), lambda a: a.step("one", kind="x"),
                  lambda a: a.step("one", kind="evaluate", outputs=["x"]), lambda a: a.step("main", kind="evaluate")):
        assert "Argo" not in refused(build)
    assert not hasattr(authoring, "MIGRATION_GUIDE_URL")


def test_the_schema_says_kind_is_required():
    import json

    from paths import ENGINE
    params = {p["name"]: p for p in json.loads((ENGINE / "kaos_pipeline_engine" / "authoring-schema.json").read_text())["params"]}
    assert params["kind"]["required"] is True and params["name"]["required"] is True
    assert not any(p["required"] for n, p in params.items() if n not in ("name", "kind"))


def test_an_app_may_not_define_a_top_level_platform_section_of_its_own(tmp_path):
    repo = copy_app(tmp_path / "app")
    with (repo / "config" / "experiment.yaml").open("a") as f:
        f.write("platform:\n  region: x\n")
    done = run_render(repo)
    assert done.returncode == 1
    assert ("the config tree defines a top-level `platform` section, which the platform no longer uses. "
            + OLD_APP_NOTICE) in done.stderr


def test_the_old_app_marker_is_one_sentence_the_notice_ends_with():
    from kubecore import step_contract
    assert OLD_APP_MARKER == "Create it again from the template."
    assert OLD_APP_NOTICE == "This app was made before the current pipelines. " + OLD_APP_MARKER
    assert not hasattr(step_contract, "MIGRATION_GUIDE_URL")


def test_an_honest_mistake_in_a_new_pipeline_does_not_carry_the_old_app_marker():
    for build in (lambda a: a.step("one", kind="training"), lambda a: a.step("Bad Name", kind="evaluate"),
                  lambda a: a.step("a" * 41, kind="evaluate"), lambda a: a.step("kaos-x", kind="evaluate")):
        assert OLD_APP_MARKER not in refused(build)


# ---------------------------------------------------------------- what each kind requires (docs/step-contract.md)

KIND_REFUSALS = [
    ("ingest", {}, "step 'x': an ingest step brings data in, so it produces a dataset. "
                   "Add produces=[dataset(\"name\")]."),
    ("ingest", {"models": 1}, "step 'x': an ingest step brings data in, so it produces a dataset. "
                              "Add produces=[dataset(\"name\")]."),
    ("transform", {}, "step 'x': a transform step turns data into something. Add produces=[...] with a dataset, "
                      "a model or an artifact."),
    ("train", {}, "step 'x': a train step produces a model. Add produces=[model(\"name\")], or give the step the "
                  "kind of the work it does (ingest, transform, evaluate, publish, control)."),
    ("train", {"artifacts": 1}, "step 'x': a train step produces a model. Add produces=[model(\"name\")], or give "
                                "the step the kind of the work it does (ingest, transform, evaluate, publish, "
                                "control)."),
    ("evaluate", {"models": 1}, "step 'x': an evaluate step writes reports only, so it may not produce a dataset "
                                "or a model. Use kind=\"transform\" for a step that makes a dataset, kind=\"train\" "
                                "for one that makes a model."),
    ("evaluate", {"datasets": 1}, "step 'x': an evaluate step writes reports only, so it may not produce a "
                                  "dataset or a model. Use kind=\"transform\" for a step that makes a dataset, "
                                  "kind=\"train\" for one that makes a model."),
    ("publish", {"artifacts": 1}, "step 'x': a publish step delivers a result and produces nothing here. Remove "
                                  "produces=, or give the step the kind of the work it does."),
]


def _produced(a, spec):
    return ([a.model("m") for _ in range(spec.get("models", 0))]
            + [a.artifact("a") for _ in range(spec.get("artifacts", 0))]
            + [a.dataset("data") for _ in range(spec.get("datasets", 0))])


@pytest.mark.parametrize("kind, spec, sentence", KIND_REFUSALS)
def test_a_step_that_breaks_the_contract_of_its_kind_is_refused_with_what_to_do(kind, spec, sentence):
    assert refused(lambda a: a.step("x", kind=kind, produces=_produced(a, spec))) == sentence


def test_a_control_step_keeps_its_own_sentence():
    assert refused(lambda a: a.step("x", kind="control", produces=[a.artifact("a")])) == (
        "step 'x': a control step steers the run and handles no data, so it takes no inputs= and no produces=. "
        "Give the step the kind of the work it does (ingest, transform, train, evaluate, publish).")


@pytest.mark.parametrize("kind, spec", [
    ("ingest", {"datasets": 1}), ("transform", {"datasets": 1}), ("transform", {"models": 1}),
    ("transform", {"artifacts": 1}), ("train", {"models": 1}), ("train", {"models": 1, "artifacts": 1}),
    ("evaluate", {}), ("evaluate", {"artifacts": 1}), ("publish", {}), ("control", {})])
def test_the_minimal_valid_step_of_each_kind_is_accepted(kind, spec):
    wft = render(lambda a: a.step("x", kind=kind, produces=_produced(a, spec)))
    labels = next(t for t in wft["spec"]["templates"] if t["name"] == "x")["metadata"]["labels"]
    assert labels["platform.kubecore.io/step-kind"] == kind
