# The runner and the site's staging tool together

The runner (this repository, `runner/`) and the site's staging tool (`stager.py`, shipped by the platform
repository in `images/meluxina-tokenmint/stager/`) ship separately. Each repository tests its own side; these two
scripts run both sides together on a fake site. Nothing here is imported by a test: run them by hand after a
change to dataset staging on either side, against a checkout of the other.

No cluster, no HPC site, no network: a fake `ssh` runs the staging tool in forced-command mode, a local HTTP
server stands in for lakeFS, and the staging tool's own test file provides the fake `lfs` and `squeue`.

## site.py: the four cases of the staging contract, and names that are not copied

```
python3 hack/staging-contract/site.py <runner dir> <stager.py> <dir of test_stager.py> <scenario>
```

Python 3.12+ (the runner's). With `STAGER=<checkout of the platform repository>/images/meluxina-tokenmint/stager`:

| Scenario | Runner dir | stager.py | Proves |
|---|---|---|---|
| `any-shape` | `runner` | `$STAGER/stager.py` | A dataset with no `data.yaml`, `images/` or `labels/` is complete on the site; the next version is a delta; a whole copy is a cache hit |
| `old-partial` | `runner` | `$STAGER/stager.py` | A copy made when only the YOLO folders were staged is never served and never a base; a job that still reads it keeps its files; it goes when the lease ends |
| `reserved-name` | `runner` | `$STAGER/stager.py` | A dataset with a root name under `.kaos-` stops the run before anything is staged, with and without the staging key |
| `unsafe-name` | `runner` | `$STAGER/stager.py` | A dataset with an object path that leaves its folder (`a/../../../escape.txt`) stops the run before anything is staged or written, with and without the staging key |
| `new-runner-old-tool` | `runner` | a `stager.py` from before the contract | The runner stops with `StagingMismatch`; nothing is staged |
| `old-runner-new-tool` | a `runner/` from before the contract | `$STAGER/stager.py` | The staging tool refuses the runner; nothing is staged (that runner then stages in-job, as it always did) |

The third argument is always `$STAGER` (the fake `lfs` and `squeue` come from its `test_stager.py`). Each check
prints `PASS:` or `FAIL:`; the script exits 1 on the first failure.

The two "old" sides are the last commits before the staging contract, fetched without touching any working tree:

```
git -C <platform repository> show 7dcde84e:images/meluxina-tokenmint/stager/stager.py > /tmp/old-stager.py
mkdir /tmp/old-runner && git archive fd730c0 runner | tar -x -C /tmp/old-runner     # then use /tmp/old-runner/runner
```

When the contract number changes again, use the commits just before that change.

## stagein36.py: the in-job stage-in under Python 3.6

The in-job stage-in (`runner/kaos_hpc_runner/payload/stagein.py`) runs under the compute node's system Python
(3.6, standard library only), which the runner's own tests (Python 3.12+) do not exercise:

```
docker run --rm --network none \
  -v $PWD/hack/staging-contract:/h:ro -v $PWD/runner/kaos_hpc_runner/payload:/p:ro \
  python:3.6-slim python /h/stagein36.py /p/stagein.py
```

It completes a copy left with only the old `.complete` marker, serves a whole copy as a cache hit, refuses a
dataset with a reserved root name or with an object path that leaves the cache folder before any download, and
does not write through a folder of the copy that is a symbolic link to elsewhere.
