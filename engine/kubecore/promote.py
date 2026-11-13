"""kaos-promote: the final task of a run. It moves `latest` of the datasets the run form asked to promote, all or none.

A step only seals its version of a dataset (kaos_lib.step.result()); it never moves `latest`. This task runs once,
after every step of the run has succeeded or was skipped, and does two things in this order:

1. CHECK every dataset to promote: the version a step sealed was started on the head of the dataset's `main` (the
   parent of the sealed commit). If any dataset's `main` has another head now, another run promoted first: nothing more
   is promoted and the task fails, naming every dataset that moved.
2. MERGE every dataset's sealed version into its `main`, each on the condition that `main` is still at the head
   checked (the lake refuses a merge otherwise).

What a failure leaves behind. A failed CHECK merges nothing more (what an earlier try of this run merged stays). A MERGE
that fails after another merge succeeded (another run promoted in the instant between the check and the merge, or the
data store did not answer) leaves the datasets already merged at the new version and the others at the old one. The task
says the whole truth of it: what this run promoted (recognised by `kaos.promoted=<sealed commit>` on a merge commit
anywhere in main's history since the head the version was sealed on), what was refused and why, what was not tried, and
whether trying again can help: after an outage yes, a dataset already promoted is left alone; after another run moved a
dataset no, and the message says "start a new run to promote <names>". Every sealed version stays kept (tagged).

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


HISTORY_LIMIT = 200          # merges of one dataset looked through for this run's own (by first parents)


def _promoted_by_this_run(client, repo, head, expected, commit):
    """Whether `commit` was merged into `repo`'s main by this run: a merge commit with `kaos.promoted=<commit>` on the
    way from the head back to the head the version was sealed on (`expected`). Not only at the head: another run may
    have moved the dataset again since."""
    seen = head
    for _ in range(HISTORY_LIMIT):
        if seen == expected or not seen:
            return False
        info = client.commit_info(repo, seen)
        if (info.get("metadata") or {}).get(PROMOTED) == commit:
            return True
        seen = (info.get("parents") or [""])[0]
    return False


def _status(exc):
    return getattr(exc, "status", None)


def _failure(promoted, moved, untried, failed=None):
    """The one message of a refused or failed task: what this run promoted, what it did not and why, and whether trying
    again can help. `moved` is a list of (name, text) of datasets another run moved before this run's check; `failed` is
    (name, repo, why, overtaken) of the merge that was refused; `untried` the names after it (or after the check)."""
    said = ["Promoted by this run: %s." % (", ".join(promoted) if promoted else "nothing")]
    if moved:
        said.append("Not promoted, because another run promoted the dataset first: "
                    + "; ".join(text for _, text in moved) + ".")
    overtaken = bool(moved) or bool(failed and failed[3])
    if failed:
        said.append(f"Dataset '{failed[0]}' ({failed[1]}) could not be promoted: {failed[2]}.")
    if untried:
        said.append("Not tried, so that the datasets of this run stay together: %s." % ", ".join(untried))
    said.append("Every version this run made stays kept and can be read by its exact version.")
    if overtaken:
        names = [n for n, _ in moved] + ([failed[0]] if failed and failed[3] else []) + list(untried)
        said.append("Another run moved a dataset, so trying this task again cannot finish it: start a new run to promote "
                    + ", ".join(names) + ".")
    else:
        said.append("The data store did not answer as needed: try again. A dataset this run already promoted is "
                    "recognised and left alone.")
    return " ".join(said)


def promote(plan, run, client, say=print):
    """Check every dataset to promote, then merge them; returns the names this run promoted. PromoteError says the whole
    truth of an outcome that is not a full success."""
    todo, moved, done = [], [], []
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
        expected = (info.get("parents") or [""])[0]
        if _promoted_by_this_run(client, repo, head, expected, commit):
            done.append(name)
            say(f"dataset '{name}': already promoted by this run ({repo} is at {head[:12]}).")
            continue
        if head != expected:
            moved.append((name, f"dataset '{name}' ({repo}): its latest was {expected[:12]} when step '{step}' sealed "
                                f"{commit[:12]}, and is {head[:12]} now"))
            continue
        todo.append((name, step, repo, commit, expected, info))
    if moved:
        raise PromoteError(_failure(done, moved, [item[0] for item in todo]))
    for i, (name, step, repo, commit, expected, info) in enumerate(todo):
        try:
            merged = client.merge(repo, commit, MAIN, info.get("message") or f"{name}: {run}",
                                  {**(info.get("metadata") or {}), PROMOTED: commit}, tolerate_empty=True,
                                  expected_head=expected)
        except Exception as exc:  # noqa: BLE001 - whatever stops a merge is reported with what is already done
            overtaken = _status(exc) == HEAD_MOVED
            why = "another run promoted the dataset in the instant before this merge" if overtaken else str(exc)
            raise PromoteError(_failure(done, [], [item[0] for item in todo[i + 1:]], (name, repo, why, overtaken))) from None
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
