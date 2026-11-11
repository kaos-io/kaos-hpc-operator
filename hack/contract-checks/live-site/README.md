# Live-site probe for check 4 (not run)

`context_probe.py` closes what check 4 cannot prove on the fake site: that a 96 KiB `KAOS_STEP_CONTEXT`
reaches a job on a real Slurm site unchanged.

**It has not been run. It spends site time, which is the client's money: run it only with the owner's
word.** It is not part of `run-all.sh`, sends nothing without `--submit`, and refuses `--submit` without
`--owner-approved`.

## What it does

It submits **one** job, one node, with a time limit of 5 minutes (it needs seconds). The job carries the
same 98304-byte value twice, the two ways the runner sends a step's inputs, and writes what it finds
into its own job comment as `<word>=<bytes>:<sha256>`:

| Word | What it proves |
|---|---|
| `env` | Slurm hands a 96 KiB environment string to the job unchanged (a step alone on its node), and the site's `slurmrestd` and `slurmctld` take the request. |
| `script` | The value as an export line in the job script arrives unchanged (a step for a shared node), and the site takes a script of that size. |
| `reread` | `scontrol write batch_script` returns that script whole: what the host of a shared node reads from a waiting job. |
| `container` | Only with `--image`: Apptainer passes the variable into the container. |

The probe reads the comment over the REST API and prints PASS or FAIL per word. It holds no other
credential and touches nothing else on the site.

Not covered even by this: a real host taking a real guest on a shared node. `reread` covers the one
step of that which depends on the size of the value.

## How to run it

On a machine that reaches the site's Slurm REST API, with the project's site identity:

```bash
export HPC_ENDPOINT=https://<the site's slurm api>   HPC_REST_VERSION=v0.0.44
export HPC_USER=<user> HPC_ACCOUNT=<account> HPC_HOME=<home on the site>
export SLURM_TOKEN_FILE=<file with the site token>      # or SLURM_TOKEN

# 1. Sends nothing: the request's sizes and the sha256 every word must show.
python3 hack/contract-checks/live-site/context_probe.py --partition <test-lane partition>

# 2. The one job. Name who allowed it.
python3 hack/contract-checks/live-site/context_probe.py --partition <test-lane partition> --qos <qos> \
  --image <a SIF on the site, or docker://alpine:3.20> \
  --submit --owner-approved "<who>, <date>"
```

Use a partition and QoS the project has budget on: a job without budget waits forever, and the probe
gives up after an hour without cancelling it (`scancel` it then).

Expected output of step 1 (checked locally):

```
request: 198339 bytes; job script: 99487 bytes; largest environment string: 98322 bytes
expected for every word: 98304:50b506212c7ad09b640670501c0f748464538b19a1706d2ee6424894072645cd
nothing was sent (no --submit)
```

The job script itself was run locally under bash with a stand-in for `scontrol`: `env`, `script` and
`reread` all report the expected value, and a value changed by one character is reported as different.
