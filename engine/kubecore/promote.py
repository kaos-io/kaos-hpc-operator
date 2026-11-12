"""kaos-promote: the final task of a run. It moves `latest` of the datasets the run form asked to promote, all or none.

A step only seals its version of a dataset (kaos_lib.step.result()); it never moves `latest`. This task runs once,
after every step of the run has succeeded or was skipped, and does two things in this order:

1. CHECK every dataset to promote: the version a step sealed was started on the head of the dataset's `main` (the
   parent of the sealed commit). If any dataset's `main` has another head now, another run promoted first: nothing is
   promoted and the task fails, naming every dataset that moved.
2. MERGE every dataset's sealed version into its `main`, each on the condition that `main` is still at the head
   checked (the lake refuses a merge otherwise).

What a failure leaves behind. A failed CHECK leaves everything as it was: every sealed version stays kept (tagged)
in its dataset. A MERGE that fails after another merge succeeded (a second run promoted in the instant between the
check and the merge, or the lake went away) leaves the datasets already merged at the new version and the others at
the old one; the task fails and says which is which. Running the task again recovers a crash or an outage: a dataset
whose `main` already holds this run's merge (the merge commit carries `kaos.promoted=<sealed commit>`) is recognised
and left alone, the others are checked and merged. After another run moved a dataset in the meantime, the task cannot
recover that dataset: the run's sealed version stays kept, and the next run starts from the new `latest`.

A producer that was skipped (a `when=` or `step.skip()`) wrote no version; its dataset is not promoted, and the log
says so. The task takes its input from the environment variable KAOS_PROMOTE_PLAN: a JSON list of
{"step", "name", "promote", "writes"} for every step that produces a dataset (see enhance.add_promote_task).
"""

import json
import os
import sys

PLAN_ENV = "KAOS_PROMOTE_PLAN"
PROMOTED = "kaos.promoted"     # on the merge commit: the full id of the sealed version it promoted (kaos_lib.writer)
MAIN = "main"
HEAD_MOVED = 412               # kaos-lake: the merge's expected head is not the branch's head any more


class PromoteError(Exception):
    pass


def read_plan(text):
    """The plan from KAOS_PROMOTE_PLAN: [{step, name, promote (the form's text), writes}]."""
    try:
        plan = json.loads(text or "")
    except ValueError as exc:
        raise PromoteError(f"{PLAN_ENV} is not JSON ({exc}); the platform built the run wrongly: report it with the "
                           f"run's name") from None
    if not isinstance(plan, list) or not all(isinstance(e, dict) and {"step", "name", "promote", "writes"} <= set(e)
                                             and isinstance(e["writes"], list) for e in plan):
        raise PromoteError(f"{PLAN_ENV} is not a list of {{step, name, promote, writes}}; the platform built the run "
                           f"wrongly: report it with the run's name")
    return plan


def _sealed(entry):
    """The write of the dataset `name` in the producing step's result, or None (the step did not run)."""
    return next((w for w in entry["writes"] if isinstance(w, dict) and w.get("name") == entry["name"]
                 and w.get("type") == "dataset"), None)


def promote(plan, run, client, say=print):
    """Check every dataset to promote, then merge them; returns the names promoted. PromoteError on any refusal."""
    todo, problems = [], []
    for entry in plan:
        name, step = entry["name"], entry["step"]
        if str(entry["promote"]).strip().lower() != "true":
            say(f"dataset '{name}': not promoted (promote-{name} is not ticked). Its version stays sealed.")
            continue
        write = _sealed(entry)
        if write is None:
            say(f"dataset '{name}': not promoted: step '{step}' wrote no version of it (it was skipped or did not run).")
            continue
        repo, commit = write["repo"], write["commit"]
        info = client.commit_info(repo, commit)
        meta = info.get("metadata") or {}
        if meta.get("kaos.run") != run or meta.get("kaos.step") != step:
            say(f"dataset '{name}': not promoted: step '{step}' wrote the same content as {repo} held ({commit[:12]}); "
                f"there is no new version.")
            continue
        head = client.branch_commit(repo, MAIN)
        merged = (client.commit_info(repo, head).get("metadata") or {}).get(PROMOTED)
        if merged == commit:
            say(f"dataset '{name}': already promoted by an earlier try of this task ({repo} is at {head[:12]}).")
            continue
        expected = (info.get("parents") or [""])[0]
        if head != expected:
            problems.append(f"dataset '{name}' ({repo}): its latest was {expected[:12]} when step '{step}' sealed "
                            f"{commit[:12]}, and is {head[:12]} now: another run promoted it.")
            continue
        todo.append((name, step, repo, commit, expected, info))
    if problems:
        raise PromoteError("nothing was promoted. " + " ".join(problems) + " The sealed version of every dataset of "
                           "this run stays kept. Run again: the new run starts from the new latest.")
    done = []
    for name, step, repo, commit, expected, info in todo:
        try:
            merged = client.merge(repo, commit, MAIN, info.get("message") or f"{name}: {run}",
                                  {**(info.get("metadata") or {}), PROMOTED: commit}, tolerate_empty=True,
                                  expected_head=expected)
        except Exception as exc:  # noqa: BLE001 - whatever stops a merge is reported with what is already done
            left = [item[0] for item in todo[len(done) + 1:]]
            raise PromoteError(
                f"dataset '{name}' ({repo}) could not be promoted: {exc}. Already promoted: "
                f"{', '.join(done) or 'none'}. Not promoted: {', '.join([name] + left)}. The sealed versions stay "
                f"kept. Run this task again to finish: a dataset already promoted is left alone.") from None
        done.append(name)
        say(f"dataset '{name}': {repo} is now at {merged[:12]} (latest), the version step '{step}' sealed "
            f"({commit[:12]}).")
    return done


def main(argv=None):
    """Exit codes: 0 done (also when nothing was to promote), 1 refused or failed."""
    run = os.environ.get("KAOS_RUN", "")
    try:
        plan = read_plan(os.environ.get(PLAN_ENV))
        if not run:
            raise PromoteError("KAOS_RUN is not set; the platform built the run wrongly")
        if any(str(e["promote"]).strip().lower() == "true" for e in plan):
            from kaos_lib.lakefs import LakeFS
            client = LakeFS.connect()
        else:
            client = None
        done = promote(plan, run, client)
    except PromoteError as exc:
        print(f"kaos-promote FAILED: {exc}", file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - the lake or the network: nothing has been merged yet, or promote() said what was
        print(f"kaos-promote FAILED: {exc}. Run the task again.", file=sys.stderr, flush=True)
        return 1
    print(f"kaos-promote: {len(done)} dataset(s) promoted.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
