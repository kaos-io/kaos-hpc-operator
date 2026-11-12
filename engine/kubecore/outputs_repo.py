"""Where a pipeline's outputs go: the app's own lakeFS outputs repository.

Platform-side naming only (the render-time engine reads the pipeline-context and
must stay stdlib + PyYAML). Steps write what they produce with kaos-lib
(`kaos_lib.step.output`), which uses the same names: `outputs-<app>`, on `main`,
under `<name>/<experiment>/<run>/`, one commit per producing step per run.

A step learns the repository from its context (`produces[].repo`).
"""

OUTPUTS_REPO_PREFIX = "outputs-"
OUTPUTS_REF = "main"  # the branch results land on (merged from a per-run branch)


def outputs_repo(ctx: dict) -> str:
    """The app's outputs repository name: `outputs-<app>`."""
    app = ctx.get("app")
    if not app:
        raise ValueError("context is missing 'app' - cannot derive the outputs repository")
    return f"{OUTPUTS_REPO_PREFIX}{app}"
