"""Generate authoring-schema.json: the options of kubecore.authoring.step() for the pipeline linter.

    python engine/tools/build_authoring_schema.py            # rewrite the file
    python engine/tools/build_authoring_schema.py --check    # exit 1 if the committed file is stale

Names and defaults come from the live signature and the patterns from the live validators; the type
and the one-line doc of each option are written here, and the generator refuses a signature with an
option this table does not describe (so a new step() option cannot ship undocumented).
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from pathlib import Path

ENGINE = Path(__file__).resolve().parent.parent
OUT = ENGINE / "kaos_pipeline_engine" / "authoring-schema.json"
sys.path.insert(0, str(ENGINE))


def _described(authoring) -> dict[str, dict]:
    """The hand-written part: {option: {type, doc, ...live constraints}}."""
    return {
        "name": {"type": "string", "pattern": authoring._STEP_NAME_RE.pattern,
                 "doc": "DNS-label step name: the Argo template, the steps/<dir> folder and the image-<step> parameter."},
        "reads": {"type": "array", "items": "string",
                  "doc": "Config sections (top-level keys of the config/ tree) the step reads; checked at render."},
        "gpu": {"type": "boolean", "doc": "The step needs one GPU."},
        "needs": {"type": "array", "items": "step",
                  "doc": "Steps this one depends on; their declared outputs become inputs of this step."},
        "outputs": {"type": "array", "items": "string",
                    "doc": "Names of small result files the step writes to /work/output/<name>.json."},
        "when": {"type": "string", "doc": "An Argo `when` expression; the step is skipped when it is false."},
        "disk": {"type": "string", "pattern": authoring._DISK_QUANTITY_RE.pattern,
                 "doc": "Ephemeral-storage request as a Kubernetes quantity, e.g. '20Gi'."},
        "hpc_time_limit": {"type": "string", "pattern": authoring._HPC_TIME_LIMIT_RE.pattern,
                           "doc": "Slurm wall-clock limit of the step when it runs on an HPC class: '90m', '12h' or "
                                  "'720' (minutes)."},
        "resumable": {"type": ["boolean", "string"], "pattern": authoring._HPC_TIME_LIMIT_RE.pattern,
                      "doc": "True or a lead time ('20m', '1h'): how long before the HPC wall time "
                             "the step is told to save its checkpoint; False opts out.",
                      "true_means": authoring.DEFAULT_RESUMABLE_LEAD},
        "checkpoint_every": {"type": ["boolean", "integer", "string"], "pattern": authoring._CHECKPOINT_EVERY_RE.pattern,
                             "doc": "How often the platform copies the step's checkpoint folder: minutes ('20m', '1h', 20); "
                                    "False = no copies; unset = the platform default."},
        "idle_timeout": {"type": ["boolean", "integer", "string"], "pattern": authoring._CHECKPOINT_EVERY_RE.pattern,
                         "doc": "How long a step may show no GPU use and no new log line before it is ended as Idle: "
                                "minutes ('40m', '1h', 40); 0 or False = never; unset = the platform rule."},
    }


def build() -> dict:
    from kubecore import authoring

    described = _described(authoring)
    params = []
    for name, p in inspect.signature(authoring.step).parameters.items():
        if name not in described:
            raise SystemExit(f"step() option {name!r} has no entry in build_authoring_schema._described")
        default = None if p.default is inspect.Parameter.empty else p.default
        params.append({"name": name, "required": p.default is inspect.Parameter.empty, "default": default,
                       **described[name]})
    extra = set(described) - {p["name"] for p in params}
    if extra:
        raise SystemExit(f"described options not in step(): {sorted(extra)}")
    return {
        "schema": 1,
        "function": "kubecore.authoring.step",
        "doc": inspect.getdoc(authoring).split("\n\n")[0],
        "params": params,
        "annotations": {
            "hpc_time_limit": "platform.kubecore.io/hpc",
            "resumable": authoring.HPC_RESUMABLE_ANNOTATION,
            "checkpoint_every": authoring.CHECKPOINT_EVERY_ANNOTATION,
            "idle_timeout": authoring.IDLE_TIMEOUT_ANNOTATION,
        },
        "constants": {"compose_step": authoring.COMPOSE_STEP, "params_path": authoring.PARAMS_PATH},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = json.dumps(build(), indent=1) + "\n"
    if args.check:
        sys.exit(0 if OUT.exists() and OUT.read_text() == text else 1)
    OUT.write_text(text)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
