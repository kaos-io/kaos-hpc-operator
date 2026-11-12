"""The run's final task: `latest` of every dataset to promote moves once, all or none (kubecore/promote.py)."""

import copy
import json
from pathlib import Path

import pytest
import yaml

from kubecore import enhance, promote
from kubecore.promote import PROMOTED, PromoteError

RUN = "app-run-1"


class LakeError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


class FakeLake:
    """Just what the task asks of the lake: commits with parents and metadata, the head of main, a guarded merge."""

    def __init__(self):
        self.commits, self.heads, self.merges, self.fail = {}, {}, [], set()

    def repo(self, repo, head="h0"):
        self.heads[repo] = head
        self.commits.setdefault(head, {"parents": [], "metadata": {}})

    def seal(self, repo, name, step, on=None, run=RUN):
        commit = f"seal-{repo}"
        self.commits[commit] = {"parents": [on or self.heads[repo]], "message": f"{name}: x",
                                "metadata": {"kaos.run": run, "kaos.step": step}}
        return commit

    def commit_info(self, repo, commit):
        return self.commits.get(commit, {"parents": [], "metadata": {}})

    def branch_commit(self, repo, branch):
        assert branch == "main"
        return self.heads[repo]

    def merge(self, repo, source, destination, message, metadata=None, tolerate_empty=False, expected_head=None):
        if repo in self.fail:
            raise LakeError(500)
        if expected_head and expected_head != self.heads[repo]:
            raise LakeError(412)
        merged = f"merge-{repo}"
        self.commits[merged] = {"parents": [self.heads[repo], source], "metadata": metadata}
        self.heads[repo] = merged
        self.merges.append((repo, source, expected_head, metadata))
        return merged


def entry(lake, name, step, promote_="true", sealed=True, on=None):
    repo = f"dataset-app-{name}"
    if repo not in lake.heads:
        lake.repo(repo)
    writes = [{"name": name, "type": "dataset", "repo": repo, "commit": lake.seal(repo, name, step, on)}] if sealed else []
    return {"step": step, "name": name, "promote": promote_, "writes": writes}


def run(plan, lake):
    lines = []
    return promote.promote(plan, RUN, lake, say=lines.append), lines


def test_every_dataset_to_promote_is_merged_on_the_head_its_version_started_from():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features")]
    done, lines = run(plan, lake)
    assert done == ["clean", "feats"]
    assert [m[:3] for m in lake.merges] == [("dataset-app-clean", "seal-dataset-app-clean", "h0"),
                                            ("dataset-app-feats", "seal-dataset-app-feats", "h0")]
    assert all(m[3][PROMOTED] == m[1] for m in lake.merges), "the merge keeps kaos.promoted=<sealed commit>"
    assert lake.heads == {"dataset-app-clean": "merge-dataset-app-clean", "dataset-app-feats": "merge-dataset-app-feats"}


def test_when_one_expected_head_moved_nothing_is_promoted_and_the_message_names_it():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features")]
    lake.heads["dataset-app-feats"] = "someone-elses"
    with pytest.raises(PromoteError) as exc:
        run(plan, lake)
    assert lake.merges == [], "all heads are checked before the first merge"
    message = str(exc.value)
    assert message.startswith("nothing was promoted. dataset 'feats' (dataset-app-feats): its latest was h0 when "
                              "step 'features' sealed seal-dataset, and is someone-else now: another run "
                              "promoted it.")
    assert "'clean'" not in message
    assert "stays kept" in message


def test_every_dataset_that_moved_is_named_not_only_the_first():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features")]
    lake.heads.update({"dataset-app-clean": "x1", "dataset-app-feats": "x2"})
    with pytest.raises(PromoteError, match=r"dataset 'clean'.*dataset 'feats'"):
        run(plan, lake)


def test_a_dataset_the_form_did_not_tick_is_left_sealed():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare", promote_="false"), entry(lake, "feats", "features")]
    done, lines = run(plan, lake)
    assert done == ["feats"]
    assert any("dataset 'clean': not promoted (promote-clean is not ticked)" in line for line in lines)


def test_a_dataset_whose_producer_was_skipped_is_not_promoted_and_the_log_says_so():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features", sealed=False)]
    done, lines = run(plan, lake)
    assert done == ["clean"]
    assert "dataset 'feats': not promoted: step 'features' wrote no version of it (it was skipped or did not run)." in lines


def test_a_step_that_wrote_what_main_already_holds_has_nothing_to_promote():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare")]
    plan[0]["writes"][0]["commit"] = "h0"          # the folder was the head of main: no new commit, no new version
    done, lines = run(plan, lake)
    assert done == [] and lake.merges == []
    assert "there is no new version" in lines[0]


def test_running_the_task_again_leaves_a_promoted_dataset_alone_and_merges_the_rest():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features")]
    lake.fail = {"dataset-app-feats"}
    with pytest.raises(PromoteError) as exc:
        run(plan, lake)
    message = str(exc.value)
    assert "dataset 'feats' (dataset-app-feats) could not be promoted: HTTP 500. Already promoted: clean. " \
           "Not promoted: feats." in message
    lake.fail = set()
    done, lines = run(plan, lake)
    assert done == ["feats"]
    assert "dataset 'clean': already promoted by an earlier try of this task" in lines[0]
    assert len(lake.merges) == 2


def test_a_merge_the_lake_refuses_after_the_check_names_what_is_done_and_what_is_not():
    lake = FakeLake()
    plan = [entry(lake, "clean", "prepare"), entry(lake, "feats", "features")]
    original = lake.merge

    def racing(repo, *args, **kwargs):
        if repo == "dataset-app-feats":
            lake.heads[repo] = "another-run"
        return original(repo, *args, **kwargs)
    lake.merge = racing
    with pytest.raises(PromoteError) as exc:
        run(plan, lake)
    assert "HTTP 412. Already promoted: clean. Not promoted: feats." in str(exc.value)


def test_a_plan_that_is_not_json_or_has_the_wrong_shape_is_refused():
    for text in ("", "nope", "{}", '[{"step": "a"}]'):
        with pytest.raises(PromoteError, match="KAOS_PROMOTE_PLAN"):
            promote.read_plan(text)


def test_main_reports_a_failure_on_stderr_and_exits_one(monkeypatch, capsys):
    monkeypatch.setenv("KAOS_RUN", RUN)
    monkeypatch.setenv("KAOS_PROMOTE_PLAN", "nope")
    assert promote.main() == 1
    assert "kaos-promote FAILED: KAOS_PROMOTE_PLAN is not JSON" in capsys.readouterr().err


def test_main_with_nothing_to_promote_needs_no_lake(monkeypatch, capsys):
    monkeypatch.setenv("KAOS_RUN", RUN)
    monkeypatch.setenv("KAOS_PROMOTE_PLAN", json.dumps([{"step": "p", "name": "clean", "promote": "false", "writes": []}]))
    assert promote.main() == 0
    assert "0 dataset(s) promoted" in capsys.readouterr().out


# ------------------------------------------------------------------------------------- the task in the template

CTX = yaml.safe_load((Path(__file__).resolve().parents[1] / "kubecore" / "local-dev" / "pipeline-context.yaml").read_text())


def enhanced(build, hpc=False):
    from kubecore import authoring
    ctx = copy.deepcopy(CTX)
    ctx["hpc"]["enabled"] = hpc
    if hpc:
        ctx["hpc"].update(controller="serve", followerImage="registry.example/kaos-hpc-operator:v1.2.3")
    with authoring.pipeline("p") as p:
        build(authoring)
    return enhance.enhance(yaml.safe_load(p.wt.to_yaml()), ctx)


def two_datasets(a):
    ingest = a.step("ingest", kind="ingest", produces=[a.dataset("clean")])
    a.step("features", kind="transform", needs=[ingest], produces=[a.dataset("feats")])
    a.step("evaluate", kind="evaluate", needs=[ingest])


def parts(wft):
    templates = {t["name"]: t for t in wft["spec"]["templates"]}
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    return templates, tasks


def test_a_pipeline_that_produces_datasets_ends_with_the_final_task_after_every_step():
    templates, tasks = parts(enhanced(two_datasets))
    task = tasks["kaos-promote"]
    assert task["depends"] == ("compose-and-validate && (ingest.Succeeded || ingest.Skipped) && "
                               "(features.Succeeded || features.Skipped) && (evaluate.Succeeded || evaluate.Skipped)")
    plan = next(p["value"] for p in task["arguments"]["parameters"] if p["name"] == "kaos-promote-plan")
    for name in ("clean", "feats"):
        assert f'workflow.parameters["promote-{name}"]' in plan
    assert plan.startswith("{{=toJSON([") and plan.endswith("])}}")


def test_every_task_the_final_task_reads_is_in_its_depends():
    """The workflow engine shows a task's outputs only to a task that names it in `depends`: also through a pair."""
    import re
    _, tasks = parts(enhanced(two_datasets, hpc=True))
    task = tasks["kaos-promote"]
    plan = next(p["value"] for p in task["arguments"]["parameters"] if p["name"] == "kaos-promote-plan")
    read = set(re.findall(r'tasks\["([^"]+)"\]', plan))
    named = set(re.findall(r"[a-z0-9][a-z0-9-]*(?=\.[A-Z]|\b)", task["depends"]))
    assert read == {"ingest", "features"} and read <= named, (read, named)


def test_the_final_task_runs_the_engines_promote_with_the_identity_of_the_compose_step():
    templates, _ = parts(enhanced(two_datasets))
    tpl, compose = templates["kaos-promote"], templates["compose-and-validate"]
    assert tpl["container"]["command"] == ["python", "-I", "-m", "kaos_engine.promote"] and tpl["container"]["args"] == []
    assert tpl["container"]["image"] == compose["container"]["image"]
    names = {e["name"] for e in tpl["container"]["env"]}
    assert {"LAKEFS_ENDPOINT", "LAKEFS_ACCESS_KEY", "LAKEFS_SECRET_KEY", "KAOS_RUN", "KAOS_PROMOTE_PLAN"} <= names
    assert tpl.get("serviceAccountName") is None, "the workflow's one account, as every step has"
    assert tpl["activeDeadlineSeconds"] == compose["activeDeadlineSeconds"]
    assert "outputs" not in tpl
    assert tpl["metadata"]["labels"]["platform.kubecore.io/step-kind"] == "control"


def test_a_pipeline_without_a_dataset_producer_has_no_final_task():
    templates, tasks = parts(enhanced(lambda a: a.step("evaluate", kind="evaluate")))
    assert "kaos-promote" not in templates and "kaos-promote" not in tasks


def test_a_step_that_may_run_on_hpc_is_waited_for_through_its_pair():
    _, tasks = parts(enhanced(two_datasets, hpc=True))
    depends = tasks["kaos-promote"]["depends"]
    assert "(evaluate.Succeeded || evaluate-hpc.Succeeded) || (evaluate.Skipped && evaluate-hpc.Skipped)" in depends
    assert "kaos-promote-hpc" not in tasks, "the final task never runs on an HPC class"


def test_a_pipeline_cannot_carry_a_template_of_that_name():
    from kubecore import authoring
    with authoring.pipeline("p") as p:
        authoring.step("ingest", kind="ingest", produces=[authoring.dataset("clean")])
    wft = yaml.safe_load(p.wt.to_yaml())
    wft["spec"]["templates"].append({"name": "kaos-promote", "container": {"image": "x"}})
    with pytest.raises(enhance.EnhanceError, match="uses a name the platform reserves.*kaos-promote"):
        enhance.enhance(wft, copy.deepcopy(CTX))


def test_a_step_cannot_be_named_kaos_promote():
    from kubecore import authoring
    with pytest.raises(authoring.AuthoringError, match="reserved"):
        with authoring.pipeline("p"):
            authoring.step("kaos-promote", kind="evaluate")
