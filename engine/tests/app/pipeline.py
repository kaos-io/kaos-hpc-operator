"""Your pipeline: the DAG. Structure and nothing else.

Each step declares its name, its kind (kind=), the config sections it reads
(reads=), the datasets it takes in (inputs=), whether it needs a GPU (gpu=True),
what it depends on (needs=), what it writes (produces=), and an optional
condition (when=). Every tunable parameter lives in the config/ tree and reaches
the submit form automatically — there is no parameter wiring here.

This fixture has two example steps: dataset-loading takes the run's dataset (the
`dataset` config value, <dataset>@<version>, pinned to one commit when the run
starts) and hello-world needs its result and writes a report.
"""

from pathlib import Path

from kubecore.authoring import artifact, dataset, pipeline, step  # platform-owned, do not edit

HERE = Path(__file__).parent

# The name you pass here is for local readability only — the platform RENAMES
# the released WorkflowTemplate to "{your-app}-pipeline" at CI render time, so
# each app gets its own uniquely-named WFT (no collisions across apps in a
# shared namespace). You don't need to change it per app.
with pipeline("ml-pipeline") as p:
    load = step("dataset-loading", kind="ingest", inputs=["dataset"], produces=[dataset("loaded")])
    hello = step("hello-world", kind="transform", reads=["experiment"], inputs=["dataset"], needs=[load],
                 produces=[artifact("report")])

if __name__ == "__main__":
    p.write(HERE / "out" / "raw-workflow-template.yaml")
