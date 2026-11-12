# Live-site probe for check 4 (not run)

`context_probe.py` closes what check 4 cannot prove on the fake site: that a 96 KiB `KAOS_STEP_CONTEXT`
reaches a job on a real Slurm site unchanged.

**It has not been run. It spends site time, which is the client's money: run it only with the owner's
word.** It is not part of `run-all.sh`, sends nothing without `--submit`, and refuses `--submit` without
`--owner-approved`.

## What it does

It submits **one** job, one node, with a time limit of 5 minutes (it needs seconds). The job carries the
same 98240-byte value twice, the two ways the runner sends a step's inputs, and writes what it finds
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

The value is built with the reference wire writer (`hack/contract-checks/wire.py`), so it is true wire form and
costs exactly 96 KiB (98240 bytes: each quote and backslash counts twice against the cap). It carries one string
with a dollar sign, a backtick, quotes, a backslash and braces, written as the wire form writes them, so the
comparison also proves the escaped form survives.

Expected output of step 1 with none of `HPC_HOME`, `HPC_USER`, `HPC_ACCOUNT` set (checked locally; the request
size depends on their lengths, the other two numbers and the sha256 do not):

```
request: 198213 bytes; job script: 99423 bytes; largest environment string: 98258 bytes
expected for every word: 98240:1e55abd174f77faafe771b7f37f5d6c4723220aeda70acffdba7e9c2af29f55d
nothing was sent (no --submit)
```

The job script itself was run locally under bash with a stand-in for `scontrol`: `env`, `script` and
`reread` all report the expected value, and a value changed by one character is reported as different.
