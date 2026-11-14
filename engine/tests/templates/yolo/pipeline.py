"""Your ML pipeline: the steps, what each one reads and writes, and their order. Nothing else.

  config-validation  checks the settings before any machine is paid for.
  prepare-data       reads the run's dataset and writes it as the dataset `prepared`: every image, or
                     `data.sample_size` images per split (`data.seed` picks them), for a quick test.
  model-training     trains on exactly the version `prepare-data` wrote and saves the weights as the
                     model `checkpoints`. A step lost with its machine continues from its last checkpoint.
  qat-finetune       (quantization-mode qat) fine-tunes the model with quantization-aware training.
  model-quantization (quantization-mode ptq or qat) makes the INT8 TFLite model.
  model-registration registers the model in the model registry.

Every setting lives in config/ and reaches the run form by itself; the version of the dataset is chosen
on the run form, never here. Conditions refer to form fields by name (config path, dots to hyphens):
quantization.mode is quantization-mode. Add a step:
https://storage.googleapis.com/kaos-dev-511509-kaos-lib/docs/first-step.html
"""

from pathlib import Path

from kubecore.authoring import dataset, model, pipeline, step  # provided by the platform

HERE = Path(__file__).parent

with pipeline("ml-pipeline") as p:
    # A gate: it decides whether the run goes on and handles no data.
    validate = step("config-validation", kind="control", reads=["experiment", "model", "quantization", "registration"])
    prepare = step("prepare-data", kind="transform", needs=[validate], reads=["data"], inputs=["dataset"],
                   produces=[dataset("prepared")])
    # walltime: the default time limit of the step, here 12h. The person who starts a run sets the limit on the run
    # form (`model-training-walltime`); this value is what the run form starts with. A step lost with its machine is run again and
    # continues from the checkpoints Ultralytics writes. Today a limit above 11 h 30 is refused on an HPC (Slurm) class
    # unless the step declares resumable=.
    train = step("model-training", kind="train", gpu=True, needs=[prepare], walltime="12h",
                 reads=["experiment", "model", "train", "image_processing", "logging"],
                 produces=[model("checkpoints")])
    qat = step("qat-finetune", kind="train", gpu=True, needs=[prepare, train],
               reads=["train", "quantization"],
               when="quantization-mode == qat",
               produces=[model("qat")])
    quant = step("model-quantization", kind="transform", needs=[prepare, train, qat],
                 reads=["quantization"],
                 when="quantization-mode != none",
                 produces=[model("quantization")])
    register = step("model-registration", kind="publish", needs=[prepare, train, quant],
                    reads=["data", "registration"])

if __name__ == "__main__":
    p.write(HERE / "out" / "raw-workflow-template.yaml")
