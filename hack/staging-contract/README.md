# The runner and the site's staging tool together

The runner (this repository, `runner/`) and the site's staging tool (`stager.py`, shipped by the platform
repository in `images/meluxina-tokenmint/stager/`) ship separately. Each repository tests its own side; these two
scripts run both sides together on a fake site. Nothing here is imported by a test: run them by hand after a
change to dataset staging on either side, against a checkout of the other.

No cluster, no HPC site, no network: a fake `ssh` runs the staging tool in forced-command mode, a local HTTP
server stands in for lakeFS, and the staging tool's own test file provides the fake `lfs` and `squeue`.

## site.py: the cases of the staging contract, and names that are not copied

```
python3 hack/staging-contract/site.py <runner dir> <stager.py> <dir of test_stager.py> <scenario>
```

Python 3.12+ (the runner's). With `STAGER=<checkout of the platform repository>/images/meluxina-tokenmint/stager`:

| Scenario | Runner dir | stager.py | Proves |
|---|---|---|---|
| `any-shape` | `runner` | `$STAGER/stager.py` | A dataset with no `data.yaml`, `images/` or `labels/` is complete on the site, under `<copy>/data/` with the tool's own files beside it, and the path handed to the job is that `data/` folder; the next version is a delta; a whole copy is a cache hit |
| `old-partial` | `runner` | `$STAGER/stager.py` | A copy made when only the YOLO folders were staged is never served and never a base; one a job leases or that was used in the last day stays; the same commit is completed in the folder the job reads |
| `old-layout` | `runner` | `$STAGER/stager.py` | A copy whose marker says `full` but whose files are directly in the commit folder (no `data/`) is never served and never a base; staging the same commit completes it into `data/` |
| `marker-like-names` | `runner` | `$STAGER/stager.py` | A dataset with root names `.kaos-ready`, `.kaos-used`, `.kaos-plan`, a folder `sub/.kaos-ready` and a file `data` is staged under `data/` and the copy is whole |
| `reserved-name` | `runner` | `$STAGER/stager.py` | A dataset with a file name ending in `.kaos-part` stops the run before anything is staged, with and without the staging key |
| `unsafe-name` | `runner` | `$STAGER/stager.py` | A dataset with an object path that leaves its folder (`a/../../../escape.txt`) stops the run before anything is staged or written, with and without the staging key |
| `uncopyable` | `runner` | `$STAGER/stager.py` | A file that is also a folder, and a file name too long for the site, stop the run before anything is staged, with and without the staging key |
| `folder-marker` | `runner` | `$STAGER/stager.py` | Zero-byte folder markers (`docs/`) are skipped and the dataset is complete |
| `file-folder-delta` | `runner` | `$STAGER/stager.py` | A version in which a folder became a file and a file became a folder is staged as a delta |
| `tool-absent` | `runner` | a path that does not exist | The runner asks for 20 minutes (of a fake clock), then stops with the "not installed" sentence |
| `new-runner-old-tool` | `runner` | a `stager.py` from before the contract | The runner stops with `StagingMismatch`; nothing is staged |
| `old-runner-new-tool` | a `runner/` from before the contract | `$STAGER/stager.py` | The staging tool refuses the runner; nothing is staged (that runner then stages in-job, as it always did) |

The third argument is always `$STAGER` (the fake `lfs` and `squeue` come from its `test_stager.py`). Each check
prints `PASS:` or `FAIL:`; the script exits 1 on the first failure.

`site.py` also runs against the older runner of `old-runner-new-tool`, which has another `stage_dataset` signature; the
script picks the call by it.

The two "old" sides are the last commits before the staging contract, fetched without touching any working tree:

```
git -C <platform repository> show 7dcde84e:images/meluxina-tokenmint/stager/stager.py > /tmp/old-stager.py
mkdir /tmp/old-runner && git archive fd730c0 runner | tar -x -C /tmp/old-runner     # then use /tmp/old-runner/runner
```

When the contract number changes again, use the commits just before that change.

## names.py: one table of names, three implementations

```
python3 hack/staging-contract/names.py runner $STAGER
```

`name_rule` decides which object names are copied. The dataset's files live under `data/` in every copy, so only a file
name ending in `.kaos-part` (the staging tool's half-downloaded files, named beside their target) is reserved. It is the same code in `runner/kaos_hpc_runner/staging.py`,
`runner/kaos_hpc_runner/payload/stagein.py` and the staging tool's `stager.py`. `names.json` (here, and the same
file next to the staging tool) is the table each repository's tests feed to its own copies; this script feeds it
to all three, and checks that the two copies of the table and the three functions are the same.

## race.py: a copy being made against another job's cleanup

```
python3 hack/staging-contract/race.py runner
```

An in-job copy of the older layout (`.complete`, files directly in the folder), last used 20 days ago, is moved aside
and made again under `data/` by the real stage-in while the real cleanup lines of the job script run again and again.
The folder being filled must not be taken from under the stage-in and the copy must be marked only when it is the
whole commit (`docs/extreme-cases.md` 3.18).

## race-hit.py: a cache hit against a cleanup that moves the folder at the worst moment

```
python3 hack/staging-contract/race-hit.py runner
```

A whole copy (files under `data/`) with a 20-day-old marker is hit by the real stage-in; right after it renews the marker, the cleanup's
move of the folder is done (the second case: another stage-in has also begun filling the folder again). The
stage-in must not fail, must not call it a cache hit, and must end with a whole copy.

## stagein36.py: the in-job stage-in under Python 3.6

The in-job stage-in (`runner/kaos_hpc_runner/payload/stagein.py`) runs under the compute node's system Python
(3.6, standard library only), which the runner's own tests (Python 3.12+) do not exercise:

```
docker run --rm --network none \
  -v $PWD/hack/staging-contract:/h:ro -v $PWD/runner/kaos_hpc_runner/payload:/p:ro \
  python:3.6-slim python /h/stagein36.py /p/stagein.py
```

It never serves a copy of the older layout (marker `.complete`, or an empty `.kaos-complete`): the copy is moved aside
and made again under `data/`. It serves a whole copy as a cache hit, takes a dataset file named like a platform file
(`.kaos-commit`) for data, refuses a name ending in `.kaos-part` or an object path that leaves the cache folder before
any download, and does not write through a folder of the copy that is a symbolic link to elsewhere.
