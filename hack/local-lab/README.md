# Local lab

Everything a pipeline needs, on one workstation, with no cloud and no cost. Two commands:

```
hack/local-lab/up.sh      # bring the lab up; safe to run twice
hack/local-lab/down.sh    # tear it down (cluster, lake container and volume, local state)
```

`hack/local-lab/smoke.sh` proves the lab: it runs a two-step workflow whose first step writes files to the
lake and commits them and whose second step reads them back, and exits non-zero unless the checksums match.

Set `LAB_NAME` to run a second lab beside the first (default `prd1911`; the cluster is `kind-<LAB_NAME>`).
Needs docker, kind, kubectl, helm, jq, openssl and curl. About 2 GiB of memory with the lab up.

| Part | What it stands in for |
|---|---|
| kind cluster (one node) | the workload cluster of a project |
| Argo Workflows (chart 1.0.14, app v4.0.5, controller only) | the workflow engine on the dev hub; the matching `argo` CLI is downloaded to `.bin/` (git-ignored) |
| kaos-lake container on local storage | the project's data lake; reachable from pods as `lake.lab.svc:8000`, from the host at the address `up.sh` prints. One declared app user (`kaos-app-lab`), outputs repository `outputs-lab`, dataset repository `dataset-lab` with three small files. Credentials in `.state/<LAB_NAME>/lake-app.env` and in the Secret `lake-app` of namespace `lab` |
| fake Slurm site (`test/fakeslurmd`) | an HPC site: `https://fake-slurm.fake-slurm.svc`, token `lab-token`, CA in `.state/<LAB_NAME>/pki/ca.crt`; jobs are simulated, not executed |
| probe step image `kaos-lab-probe:<LAB_NAME>` | a pipeline step: prints its context, lists input folders with sha256 checksums, writes a result file; also carries the `lake` helper (put, commit, get, sync) |

Lake image: `LAKE_IMAGE` if set, else the local `kaos-lake:prd1911` if present, else the published dev image,
else (with a loud warning) upstream `treeverse/lakefs:1.86.0`, which has no declared users, so the app uses the admin key.

Workflows run in namespace `lab` with service account `lab`:

```
hack/local-lab/.bin/argo --context kind-prd1911 -n lab submit my-workflow.yaml --wait --log
```
