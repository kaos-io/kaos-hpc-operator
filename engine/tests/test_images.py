"""Where a step's image comes from: the image record, read by the template, never a parameter of the run form."""

import copy
import json
from pathlib import Path

import pytest
import yaml

from kubecore import enhance
from kubecore.step_contract import IMAGE_INPUT, image_of

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


def pipeline(a):
    ingest = a.step("ingest", kind="ingest", produces=[a.dataset("clean")])
    a.step("train", kind="train", needs=[ingest], produces=[a.model("model")])
    a.step("evaluate", kind="evaluate", needs=[ingest])


def parts(wft):
    templates = {t["name"]: t for t in wft["spec"]["templates"]}
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    return templates, tasks


def params(template):
    return {p["name"]: p for p in template.get("inputs", {}).get("parameters", [])}


def test_no_image_is_an_argument_of_the_workflow():
    for hpc in (False, True):
        wft = enhanced(pipeline, hpc)
        names = [p["name"] for p in wft["spec"]["arguments"]["parameters"]]
        assert not [n for n in names if "image" in n], names
        assert not [p for p in wft["spec"]["arguments"]["parameters"] if "valueFrom" in p], "nothing is read from a record there"


def test_a_step_template_takes_its_image_from_its_task_not_from_a_parameter_or_a_record():
    templates, tasks = parts(enhanced(pipeline))
    for name in ("ingest", "train", "evaluate"):
        assert templates[name]["container"]["image"] == IMAGE_INPUT
        assert params(templates[name])["image"] == {"name": "image"}, "no default, no valueFrom: only the task gives it"
        argument = next(p for p in tasks[name]["arguments"]["parameters"] if p["name"] == "image")
        assert argument["value"] == image_of(name)
    assert image_of("train") == ('{{=fromJSON(tasks["compose-and-validate"].outputs.parameters["kaos-images"])["train"]}}')


def test_compose_reads_the_record_once_for_every_step_and_for_itself():
    templates, _ = parts(enhanced(pipeline))
    compose = templates["compose-and-validate"]
    got = params(compose)
    assert compose["container"]["image"] == IMAGE_INPUT
    assert got["image"]["valueFrom"]["configMapKeyRef"] == {"name": f"{CTX['app']}-pipeline-images",
                                                            "key": "compose-and-validate"}
    for step in ("ingest", "train", "evaluate"):
        assert got[f"image-{step}"]["valueFrom"]["configMapKeyRef"]["key"] == step
    env = {e["name"]: e["value"] for e in compose["container"]["env"] if "value" in e}
    want = {s: "{{inputs.parameters.image-" + s + "}}" for s in ("evaluate", "ingest", "train")}
    assert json.loads(env["KAOS_STEP_IMAGES"]) == want
    args = compose["container"]["args"]
    assert args[args.index("--images-output") + 1] == "/work/kaos-images.json"
    assert {"name": "kaos-images", "valueFrom": {"path": "/work/kaos-images.json"}} in compose["outputs"]["parameters"]


def test_a_step_with_the_source_annotation_reads_that_key_of_the_record():
    from kubecore import authoring

    def build(a):
        s = a.step("train", kind="train", produces=[a.model("model")])
        assert s
    ctx = copy.deepcopy(CTX)
    ctx["hpc"]["enabled"] = False
    with authoring.pipeline("p") as p:
        build(authoring)
    wft = yaml.safe_load(p.wt.to_yaml())
    for t in wft["spec"]["templates"]:
        if t["name"] == "train":
            t.setdefault("metadata", {}).setdefault("annotations", {})["platform.kubecore.io/source"] = "other-key"
    got = params(parts(enhance.enhance(wft, ctx))[0]["compose-and-validate"])
    assert got["image-train"]["valueFrom"]["configMapKeyRef"]["key"] == "other-key"


def test_a_verbatim_image_annotation_is_refused():
    """A step runs the image CI built. The annotation that named any image (the app's identity and secrets would run it)
    is no longer part of what a definition may carry (T-20, T-15)."""
    from kubecore import authoring
    ctx = copy.deepcopy(CTX)
    ctx["hpc"]["enabled"] = False
    with authoring.pipeline("p") as p:
        authoring.step("train", kind="train", produces=[authoring.model("model")])
    wft = yaml.safe_load(p.wt.to_yaml())
    for t in wft["spec"]["templates"]:
        if t["name"] == "train":
            t.setdefault("metadata", {}).setdefault("annotations", {})["platform.kubecore.io/image"] = "busybox:1.36"
    with pytest.raises(enhance.EnhanceError, match="step 'train' carries 'metadata.annotations.platform.kubecore.io/image'"):
        enhance.enhance(wft, ctx)


def test_the_hpc_variant_gets_the_same_string_as_the_step():
    _, tasks = parts(enhanced(pipeline, hpc=True))
    for name in ("train", "evaluate"):
        twin = next(p["value"] for p in tasks[f"{name}-hpc"]["arguments"]["parameters"] if p["name"] == "image")
        step = next(p["value"] for p in tasks[name]["arguments"]["parameters"] if p["name"] == "image")
        assert twin == step == image_of(name)


def test_the_final_task_reads_the_compose_image_and_none_of_the_steps():
    templates, tasks = parts(enhanced(pipeline))
    got = params(templates["kaos-promote"])
    assert got["image"]["valueFrom"]["configMapKeyRef"]["key"] == "compose-and-validate"
    assert not [n for n in got if n.startswith("image-")]
    assert templates["kaos-promote"]["container"]["image"] == IMAGE_INPUT
    given = [p for p in tasks["kaos-promote"]["arguments"]["parameters"] if p["name"] == "image"]
    assert not given, "its input is read from the record"


def test_every_task_that_reads_an_image_from_compose_depends_on_compose():
    _, tasks = parts(enhanced(pipeline, hpc=True))
    for name, task in tasks.items():
        if any(p["name"] == "image" for p in (task.get("arguments") or {}).get("parameters", [])):
            assert "compose-and-validate" in task["depends"], name
