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
        "name": {"type": "string", "pattern": authoring._STEP_NAME_RE.pattern, "maxLength": authoring.MAX_NAME,
                 "doc": "DNS-label step name: the step's folder under steps/ and part of its image name."},
        "kind": {"type": "string", "enum": list(authoring.KINDS),
                 "doc": "Required. What the step does; the platform labels the step's pod and HPC job with it. "
                        "ingest, publish and control steps never run on an HPC class."},
        "reads": {"type": "array", "items": "string",
                  "doc": "Config sections (top-level keys of the config/ tree) the step reads; checked at render. "
                         "They are the step's params: it gets these sections and no other."},
        "inputs": {"type": "array", "items": "string", "pattern": authoring._STEP_NAME_RE.pattern,
                   "maxLength": authoring.MAX_NAME,
                   "doc": "Names of the top-level config values that hold a dataset reference. Each is pinned to "
                          "one commit when the run starts; step code names the input, never a version."},
        "needs": {"type": "array", "items": "step",
                  "doc": "Steps this one runs after; each one's result reaches this step (kaos_lib.step.upstream)."},
        "produces": {"type": "array", "items": "produced",
                     "doc": "What the step writes: model('name'), artifact('name') or dataset('name')."},
        "gpu": {"type": "boolean", "doc": "The step needs one GPU."},
        "when": {"type": "string",
                 "doc": "A condition in plain words: a run-form field or a value of a needed step compared with a "
                        "text: 'mode != off', 'evaluate.passed == true', joined with and / or; the step is "
                        "skipped when it is false."},
        "walltime": {"type": "string", "pattern": authoring._WALLTIME_RE.pattern,
                     "doc": "The default time limit of the step, wherever it runs: '90m', '12h' or '720' (minutes). "
                            "The person who starts the run sets the limit on the run form (<step>-walltime); "
                            "4h when not given."},
        "resumable": {"type": ["boolean", "string"], "pattern": authoring._WALLTIME_RE.pattern,
                      "doc": "True or a lead time ('20m', '1h'): how long before its time limit "
                             "the step is told to save its checkpoint; False opts out.",
                      "true_means": authoring.DEFAULT_RESUMABLE_LEAD},
        "checkpoint_every": {"type": ["boolean", "integer", "string"], "pattern": authoring._CHECKPOINT_EVERY_RE.pattern,
                             "doc": "The default for how often the platform copies the step's checkpoint folder: "
                                    "minutes ('20m', '1h', 20); False = no copies; unset = the platform default "
                                    "(a resumable step has the field too). The person who starts the run "
                                    "can change it on the run form (<step>-checkpoint-every)."},
        "idle_timeout": {"type": ["boolean", "integer", "string"], "pattern": authoring._CHECKPOINT_EVERY_RE.pattern,
                         "doc": "How long a step may show no GPU use and no new log line before it is ended as Idle: "
                                "minutes ('40m', '1h', 40); 0 or False = never; unset = the platform rule."},
    }


def build() -> dict:
    from kubecore import authoring
    from kubecore.step_contract import OLD_APP_MARKER

    described = _described(authoring)
    params = []
    for name, p in inspect.signature(authoring.step).parameters.items():
        if p.kind is inspect.Parameter.VAR_KEYWORD:   # only there to answer a removed keyword with its message
            continue
        if name not in described:
            raise SystemExit(f"step() option {name!r} has no entry in build_authoring_schema._described")
        default = None if p.default is inspect.Parameter.empty else p.default
        required = p.default is inspect.Parameter.empty or name == "kind"   # kind= is keyword-only and has no valid default
        params.append({"name": name, "required": required, "default": default,
                       **described[name]})
    extra = set(described) - {p["name"] for p in params}
    if extra:
        raise SystemExit(f"described options not in step(): {sorted(extra)}")
    return {
        "schema": 1,
        "function": "kubecore.authoring.step",
        "doc": inspect.getdoc(authoring).split("\n\n")[0],
        "params": params,
        # Keywords the step contract removed, with the reason the render gives (it fails on them).
        "removed": dict(authoring._REMOVED_KEYWORDS),
        "produced": {"model": "a trained model, stored with the run", "artifact": "any other files of the run",
                     "dataset": "a new version of a dataset this step alone writes"},
        "old_app_marker": OLD_APP_MARKER,
        "annotations": {
            "walltime": authoring.WALLTIME_ANNOTATION,
            "resumable": authoring.HPC_RESUMABLE_ANNOTATION,
            "checkpoint_every": authoring.CHECKPOINT_EVERY_ANNOTATION,
            "idle_timeout": authoring.IDLE_TIMEOUT_ANNOTATION,
        },
        "constants": {"compose_step": authoring.COMPOSE_STEP},
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
