"""Seeded, PLATFORM-OWNED render-time helpers. DO NOT EDIT.

Gives pipeline.py the authoring surface: steps declare name, kind, the config
sections they read, the datasets they take in, compute, dependencies, and what
they produce — and nothing else.
Under the hood this emits plain Hera Containers and a standard Argo DAG (the
vanilla-Hera property: thin helpers, standard YAML out), with the
compose-and-validate step as task 1:

- the compose step's args are ALL derived override tokens
  (`train.epochs={{workflow.parameters.train-epochs}}`), the ADVANCED
  override, and the union of declared reads= sections to validate;
- every other step runs `python -m app.entry` with no arguments and receives
  one value, KAOS_STEP_CONTEXT (docs/step-contract.md): its base is written
  by compose, the results of the steps it needs are joined in by the DAG task
  argument — downstream steps NEVER see override tokens;
- every step hands back one result, the output `kaos-result`;
- `gpu=True` -> `Resources(gpus=1)`, which is exactly what the enhancer
  detects for GPU scheduling;
- there is no `disk=`: a step gets the disk of the machine its class runs on;
- `needs=[other]` wires dependencies AND puts the needed step's result in
  this step's context; a needed step that is conditional (`when=`) gets a
  skip-tolerant depends expression automatically, and its result is then
  the sentinel `{"version":1,"status":"absent"}`.

This is a render-time helper, not a runtime SDK.
"""

import json
import re

from hera.workflows import DAG, Container, Env, Parameter, Resources, WorkflowTemplate
from hera.workflows.models import ValueFrom

from kubecore import derive_tree
from kubecore.step_contract import (
    CONTEXT_DIR,
    CONTEXT_ENV,
    CONTEXT_OUTPUT_PREFIX,
    CONTEXT_PARAM,
    DATASET_PREFIX,
    CLUSTER_ONLY_KINDS,
    KINDS,
    MAX_INPUTS,
    MAX_NAME,
    MAX_NEEDS,
    DATASET_PRODUCER_LABEL,
    DATASET_PRODUCERS_STAY_ON_CLUSTER,
    OLD_APP_NOTICE,
    MIN_DATASET_NAME,
    LEDGER_PARAM,
    LEDGER_PATH,
    RESERVED_PREFIX,
    RESULT_PARAM,
    RESULT_PATH,
    SENTINEL,
    context_expression,
    wire,
)

IMAGE = "platform-managed"  # sentinel; the platform always rewrites images

# Step names become Argo template names + the steps/<dir> folder + the
# image-<step> parameter, so they must be DNS-label-safe (lowercase alphanumeric
# and hyphens, no leading/trailing hyphen). Validating here turns a silent bad
# render (or a cryptic Hera NodeNameConflict) into a clear authoring error.
_STEP_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")   # also an input's name and a produced name


class AuthoringError(Exception):
    pass
COMPOSE_STEP = "compose-and-validate"
# Template names the platform renders itself: the DAG, and the HPC follower.
RESERVED_STEP_NAMES = (COMPOSE_STEP, "main", "hpc-follower")
# A step's HPC variant is the task `<step>-hpc` (kubecore/meluxina.py); a step may not take such a name.
RESERVED_STEP_SUFFIX = "-hpc"
STEP_LABEL = "platform.kubecore.io/step"
STEP_KIND_LABEL = "platform.kubecore.io/step-kind"
# The compose step runs `python -m kubecore.compose` inside its own image;
# every other step runs `python -m app.entry` (the app's console-script).
COMPOSE_COMMAND = ["python", "-I", "-m", "kaos_engine.compose"]
STEP_COMMAND = ["python", "-m", "app.entry"]

_current = None


# Kubernetes resource quantity ("10Gi", "512Mi", "2G"); anchored so a typo
# ("10 Gi", "10GB") fails the render instead of rendering an invalid manifest.
_WALLTIME_RE = re.compile(r"^[0-9]+[mh]?$")
# Keywords the step contract removed, with what to write instead.
_REMOVED_KEYWORDS = {
    "outputs": "outputs= is no longer part of step()",
    "hpc_time_limit": "hpc_time_limit= is no longer part of step()",
    "disk": "disk is no longer declared; a step gets the disk of the machine its class runs on",
}


# step(resumable=True) gets the stop signal this long before its HPC wall time.
DEFAULT_RESUMABLE_LEAD = "10m"
HPC_RESUMABLE_ANNOTATION = "platform.kubecore.io/hpc-resumable"
# step(walltime=...): the run limit on an HPC class (read by kubecore/meluxina.py).
WALLTIME_ANNOTATION = "platform.kubecore.io/hpc"
RESUMABLE_OFF = "off"   # the annotation value of resumable=False
CHECKPOINT_EVERY_ANNOTATION = "platform.kubecore.io/checkpoint-every"
_CHECKPOINT_EVERY_RE = re.compile(r"^([0-9]+)([mh]?)$")


def _checkpoint_every_minutes(name, value):
    """step(checkpoint_every=...) -> annotation value: None keeps the platform default (GPU steps copy their
    checkpoint folder every 10 min), False -> "off", '20m' / '1h' / 20 -> minutes as text."""
    if value is None:
        return None
    if value is False:
        return "off"
    match = _CHECKPOINT_EVERY_RE.match(str(value).strip()) if value is not True else None
    if not match or int(match.group(1)) == 0:
        raise AuthoringError(
            f"step {name!r}: checkpoint_every={value!r} must be False (no copies) or minutes like '20m', '1h' "
            f"or 20: how often the platform copies the step's checkpoint folder (KAOS_CHECKPOINT_DIR)."
        )
    minutes = int(match.group(1))
    return str(minutes * 60 if match.group(2) == "h" else minutes)
IDLE_TIMEOUT_ANNOTATION = "platform.kubecore.io/idle-timeout"


def _idle_timeout_minutes(name, value):
    """step(idle_timeout=...) -> annotation value (whole minutes, "0" = off): None keeps the platform rule
    (a GPU step with GPU use below 5 % and no new log line for 20 minutes is ended as Idle), 0 or False turns
    it off, '40m' / '1h' / 40 raise the window. Read by the pool's idle watchdog."""
    if value is None:
        return None
    if value is False or (not isinstance(value, bool) and str(value).strip() == "0"):
        return "0"
    match = _CHECKPOINT_EVERY_RE.match(str(value).strip()) if value is not True else None
    if not match or int(match.group(1)) == 0:
        raise AuthoringError(
            f"step {name!r}: idle_timeout={value!r} must be 0 (never end the step as idle) or minutes like "
            f"'40m', '1h' or 40: how long the step may show no GPU use and no new log lines."
        )
    minutes = int(match.group(1))
    return str(minutes * 60 if match.group(2) == "h" else minutes)


class Produced:
    """One entry of produces=: a model, an artifact or a dataset the step writes."""

    def __init__(self, name, type):
        self.name = name
        self.type = type


def _well_formed(name) -> bool:
    """A step, input or produced name: lowercase letters, digits and hyphens, at most 40 characters."""
    return isinstance(name, str) and len(name) <= MAX_NAME and bool(_STEP_NAME_RE.match(name))


def _produced(type, name):
    if not _well_formed(name) or (type == "dataset" and len(name) < MIN_DATASET_NAME):
        raise AuthoringError(
            f"{type}({name!r}): the name is invalid — use lowercase letters, digits and hyphens, "
            f"{f'{MIN_DATASET_NAME} to ' if type == 'dataset' else 'at most '}{MAX_NAME} characters, for example "
            f"{type}('{'embeddings' if type == 'dataset' else type}'). "
            + (f"The dataset is stored as {DATASET_PREFIX}<app>-<name>, where <app> is the app's name." if type == "dataset" else
               "It is the folder the files are stored under.")
        )
    if name.startswith(RESERVED_PREFIX):
        raise AuthoringError(
            f"{type}({name!r}): names that start with '{RESERVED_PREFIX}' are reserved for the platform. "
            f"Choose another name."
        )
    return Produced(name, type)


def model(name) -> Produced:
    """produces=[model("name")]: a trained model, stored with the run."""
    return _produced("model", name)


def artifact(name) -> Produced:
    """produces=[artifact("name")]: any other files of the run (a report, checkpoints)."""
    return _produced("artifact", name)


def dataset(name, **options) -> Produced:
    """produces=[dataset("name")]: a new version of the app's dataset `name` (the repository
    `dataset-<app>-<name>`), written by this step and by no other."""
    if "of" in options:
        raise AuthoringError(
            f"dataset({name!r}, of={options['of']!r}): of= is reserved for a dataset stored as a variant of "
            f"its source, which is not available yet. Write dataset({name!r}): the step then writes its own "
            f"dataset."
        )
    if options:
        raise TypeError(f"dataset() got an unexpected keyword argument {next(iter(options))!r}")
    return _produced("dataset", name)


class Step:
    def __init__(self, name, kind, reads, inputs, needs, produces, gpu, when, walltime, resumable,
                 checkpoint_every, idle_timeout):
        self.name = name
        self.kind = kind
        self.reads = reads
        self.inputs = inputs
        self.needs = needs
        self.produces = produces
        self.gpu = gpu
        self.when = when
        self.walltime = walltime
        self.resumable = resumable
        self.checkpoint_every = checkpoint_every
        self.idle_timeout = idle_timeout


def _check_kind(name, kind):
    # A step with no kind= is reported when the pipeline is assembled (pipeline._check_kinds): whether the app is
    # one from before the contract depends on the other steps.
    if kind is not None and kind not in KINDS:
        raise AuthoringError(
            f"step {name!r}: kind={kind!r} is not a step kind. Use one of {', '.join(KINDS)}."
        )


def _check_data(name, kind, inputs, needs, produces):
    """The names a step reads and writes: each well formed, each once, and no name with two meanings."""
    for entry in inputs:
        if not _well_formed(entry):
            raise AuthoringError(
                f"step {name!r}: inputs= holds {entry!r}. An input is the name of a top-level config value "
                f"that holds a dataset reference (for example inputs=['dataset'], with `dataset: "
                f"dataset-default@latest` in config/): lowercase letters, digits and hyphens, at most "
                f"{MAX_NAME} characters. Rename the config value."
            )
        if entry.startswith(RESERVED_PREFIX):
            raise AuthoringError(
                f"step {name!r}: the input {entry!r} starts with '{RESERVED_PREFIX}', which is reserved for "
                f"the platform. Rename the config value."
            )
    twice = sorted({entry for entry in inputs if inputs.count(entry) > 1})
    if twice:
        raise AuthoringError(f"step {name!r}: inputs= names {', '.join(twice)} more than once.")
    for entry in produces:
        if not isinstance(entry, Produced):
            raise AuthoringError(
                f"step {name!r}: produces= holds {entry!r}. Declare what the step writes with model('name'), "
                f"artifact('name') or dataset('name') (from kubecore.authoring import model, artifact, dataset)."
            )
    names = [entry.name for entry in produces]
    twice = sorted({n for n in names if names.count(n) > 1})
    if twice:
        raise AuthoringError(f"step {name!r}: produces= names {', '.join(twice)} more than once.")
    if kind == "control" and (inputs or produces):
        raise AuthoringError(
            f"step {name!r}: a control step steers the run and handles no data, so it takes no inputs= and "
            f"no produces=. Give the step the kind of the work it does ({', '.join(k for k in KINDS if k != 'control')})."
        )
    needed = [need.name for need in needs]
    again = sorted({n for n in needed if needed.count(n) > 1})
    if again:
        raise AuthoringError(f"step {name!r}: needs= names {', '.join(again)} more than once.")
    if len(needs) > MAX_NEEDS:
        raise AuthoringError(
            f"step {name!r} needs {len(needs)} steps; a step may need at most {MAX_NEEDS}. Each needed step's "
            f"result takes room in this step's context, and {len(needs)} would leave too little for the rest. "
            f"Join some of the steps it needs, or put a step between."
        )
    # input(name) reads a declared input from the ledger and anything else from the result of a needed
    # step: one name may have one meaning only, or the step could not say which one it reads.
    upstream = {}
    for need in needs:
        for entry in need.produces:
            if entry.name in upstream:
                raise AuthoringError(
                    f"step {name!r}: the steps it needs, {upstream[entry.name]!r} and {need.name!r}, both "
                    f"produce {entry.name!r}. A step reads what the steps it needs wrote by name, so each name "
                    f"can come from one of them only: rename one."
                )
            upstream[entry.name] = need.name
    clash = sorted(set(inputs) & set(upstream))
    if clash:
        raise AuthoringError(
            f"step {name!r}: {', '.join(clash)} is both an input (inputs=) and something the step it needs, "
            f"{upstream[clash[0]]!r}, produces. A step reads both by name, so the two must differ: rename "
            f"the config value or the produced {'thing' if len(clash) == 1 else 'things'}."
        )
    readable = len(inputs) + sum(1 for need in needs for entry in need.produces if entry.type == "dataset")
    if readable > MAX_INPUTS:
        raise AuthoringError(
            f"step {name!r} can read {readable} datasets by name ({len(inputs)} in inputs= and "
            f"{readable - len(inputs)} produced by the steps it needs); a step may have at most {MAX_INPUTS}. "
            f"Split the step, or join datasets it reads."
        )
    for entry in produces:
        if entry.type != "dataset":
            continue
        other = next((s.name for s in _current.steps for e in s.produces
                      if e.type == "dataset" and e.name == entry.name), None)
        if other:
            raise AuthoringError(
                f"step {name!r}: dataset({entry.name!r}) is already produced by step {other!r}. A dataset "
                f"has one writer: exactly one step of one pipeline produces it."
            )


def step(name, *, kind=None, reads=None, inputs=None, needs=None, produces=None, gpu=False,
         when=None, walltime=None, resumable=None, checkpoint_every=None, idle_timeout=None, **removed) -> Step:
    if _current is None:
        raise RuntimeError("step() must be called inside `with pipeline(...):`")
    for keyword in removed:
        if keyword not in _REMOVED_KEYWORDS:
            raise TypeError(f"step() got an unexpected keyword argument {keyword!r}")
        raise AuthoringError(
            f"step {name!r}: {_REMOVED_KEYWORDS[keyword]}. {OLD_APP_NOTICE}"
        )
    if not _well_formed(name):
        raise AuthoringError(
            f"step name {name!r} is invalid — use a DNS-label name: lowercase "
            f"letters, digits and hyphens, no leading/trailing hyphen, at most {MAX_NAME} characters "
            f"(e.g. 'model-evaluation'). The name is the step's folder under steps/ and part of its image name."
        )
    if name.endswith(RESERVED_STEP_SUFFIX) or name in RESERVED_STEP_NAMES or name.startswith(RESERVED_PREFIX):
        raise AuthoringError(
            f"step name {name!r} is reserved — the platform names a step's HPC variant '<step>{RESERVED_STEP_SUFFIX}', "
            f"keeps {', '.join(RESERVED_STEP_NAMES)} for itself, and every name that starts with "
            f"'{RESERVED_PREFIX}'. Choose another name."
        )
    _check_kind(name, kind)
    if walltime is not None and kind in CLUSTER_ONLY_KINDS:
        raise AuthoringError(
            f"step {name!r}: walltime= is the run limit on an HPC class, and a {kind} step never runs on one. "
            f"Remove walltime= from the step."
        )
    reads, inputs, needs, produces = list(reads or []), list(inputs or []), list(needs or []), list(produces or [])
    _check_data(name, kind, inputs, needs, produces)
    if any(s.name == name for s in _current.steps):
        raise AuthoringError(
            f"duplicate step name {name!r} — every step must be uniquely named "
            f"(it is the step's identity in the DAG and its image)."
        )
    if walltime is not None and not (
        isinstance(walltime, str) and _WALLTIME_RE.match(walltime) and int(walltime.rstrip("mh")) != 0
    ):
        raise AuthoringError(
            f"step {name!r}: walltime={walltime!r} must be minutes or hours as a "
            f"string like '90m', '12h' or '720', at least one minute (the run limit when this step "
            f"runs on an HPC class; the step is ended when it elapses)."
        )
    if resumable is True:
        resumable = DEFAULT_RESUMABLE_LEAD
    elif resumable is False:
        resumable = RESUMABLE_OFF        # resumable=False: never continued from a checkpoint (opt-out)
    elif resumable is None:
        resumable = None
    elif not (isinstance(resumable, str) and _WALLTIME_RE.match(resumable) and int(resumable.rstrip("mh")) != 0):
        raise AuthoringError(
            f"step {name!r}: resumable={resumable!r} must be True or a lead time like '20m' or '1h': how long "
            f"before its HPC wall time the step is told to save its checkpoint (kaos_lib.checkpoint)."
        )
    # Any step may declare these, GPU or not: every step can be placed on an HPC class, and a step on a spot
    # machine is interrupted too. A resumable step gets its checkpoint copies wherever it runs (resilience.py).
    checkpoint_every = _checkpoint_every_minutes(name, checkpoint_every)
    idle_timeout = _idle_timeout_minutes(name, idle_timeout)
    s = Step(name, kind, reads, inputs, needs, produces, gpu, when, walltime, resumable, checkpoint_every,
             idle_timeout)
    _current.steps.append(s)
    return s


class pipeline:
    def __init__(self, name: str):
        self.name = name
        self.steps = []
        self.wt = None

    def __enter__(self):
        global _current
        _current = self
        return self

    def __exit__(self, exc_type, exc, tb):
        global _current
        _current = None
        if exc_type is None:
            self._check_kinds()
            self._build()
        return False

    def _check_kinds(self) -> None:
        """A step with no kind=. An app from before the contract has no kind on any step, so only then is it told
        to create the app again; in a pipeline where other steps have a kind, the step simply forgot it."""
        kindless = [s.name for s in self.steps if s.kind is None]
        if kindless:
            notice = "" if len(kindless) < len(self.steps) else f" {OLD_APP_NOTICE}"
            raise AuthoringError(
                f"step {kindless[0]!r} has no kind=, which every step declares: one of {', '.join(KINDS)}.{notice}"
            )

    def _step_table(self) -> str:
        """What compose needs to write each step's context: its kind, the config sections it
        reads (its params), its inputs, the steps it needs (each one's result takes room in the context)
        and what it produces."""
        return wire([
            {"name": s.name, "kind": s.kind, "reads": s.reads, "inputs": s.inputs, "needs": [n.name for n in s.needs],
             "produces": [{"name": e.name, "type": e.type} for e in s.produces]}
            for s in self.steps
        ])

    def _build(self) -> None:
        form = derive_tree.derive()
        derive_tree.validate_reads(
            {s.name: s.reads for s in self.steps}, form.sections
        )  # render gate: reads= sections must exist in the tree
        derive_tree.validate_inputs({s.name: s.inputs for s in self.steps}, form.tree)
        required = sorted({sec for s in self.steps for sec in s.reads})

        with WorkflowTemplate(
            name=self.name, entrypoint="main", arguments=form.parameters
        ) as wt:
            compose = Container(
                name=COMPOSE_STEP, image=IMAGE,
                command=COMPOSE_COMMAND,
                args=form.tokens
                # render-time defaults manifest: lets the compose step
                # elide untouched leaves so group swaps take effect
                # (the WFT stays a self-contained release artifact)
                + ["--render-defaults", json.dumps(form.render_defaults, separators=(",", ":"))]
                + ["--advanced", "{{workflow.parameters.config}}"]
                + [f"--require={sec}" for sec in required]
                + ["--run", "{{workflow.name}}", "--steps", self._step_table(),
                   "--context-dir", CONTEXT_DIR, "--ledger-output", LEDGER_PATH],
                labels={STEP_LABEL: COMPOSE_STEP, STEP_KIND_LABEL: "control"},
                outputs=[Parameter(name=LEDGER_PARAM, value_from=ValueFrom(path=LEDGER_PATH))]
                + [Parameter(name=CONTEXT_OUTPUT_PREFIX + s.name,
                             value_from=ValueFrom(path=f"{CONTEXT_DIR}/{s.name}.json")) for s in self.steps],
            )
            containers = {}
            for s in self.steps:
                containers[s.name] = Container(
                    name=s.name, image=IMAGE,
                    command=STEP_COMMAND,
                    env=[Env(name=CONTEXT_ENV, value="{{inputs.parameters.%s}}" % CONTEXT_PARAM)],
                    labels={STEP_LABEL: s.name, STEP_KIND_LABEL: s.kind,
                            **({DATASET_PRODUCER_LABEL: "true"}
                               if DATASET_PRODUCERS_STAY_ON_CLUSTER and any(e.type == "dataset" for e in s.produces)
                               else {})},
                    resources=Resources(gpus=1) if s.gpu else None,
                    # Read by kubecore/meluxina.py: the run limit when
                    # this step runs on an HPC class (developer-set; the
                    # enhancer defaults it otherwise).
                    # and, for step(resumable=...), how long before it the step
                    # is told to save (kaos_lib.checkpoint).
                    annotations={k: v for k, v in (
                        (WALLTIME_ANNOTATION, s.walltime),
                        (HPC_RESUMABLE_ANNOTATION, s.resumable),
                        (CHECKPOINT_EVERY_ANNOTATION, s.checkpoint_every),
                        (IDLE_TIMEOUT_ANNOTATION, s.idle_timeout)) if v}
                    or None,
                    inputs=[Parameter(name=CONTEXT_PARAM)],
                    # No default: a step that ends well without writing its result fails in the workflow engine,
                    # loudly. A step that did not run is the sentinel through the DAG argument, not through here.
                    outputs=[Parameter(name=RESULT_PARAM, value_from=ValueFrom(path=RESULT_PATH))],
                )

            with DAG(name="main"):
                compose()
                for s in self.steps:
                    depends = " && ".join(
                        [COMPOSE_STEP]
                        + [
                            f"({n.name}.Succeeded || {n.name}.Skipped || {n.name}.Omitted)"
                            if n.when
                            else n.name
                            for n in s.needs
                        ]
                    )
                    containers[s.name](
                        arguments={CONTEXT_PARAM: context_expression(COMPOSE_STEP, s.name, [n.name for n in s.needs])},
                        when=s.when, depends=depends,
                    )
        self.wt = wt

    def write(self, path) -> None:
        path.parent.mkdir(exist_ok=True)
        path.write_text(self.wt.to_yaml())
        print(f"wrote {path}")
