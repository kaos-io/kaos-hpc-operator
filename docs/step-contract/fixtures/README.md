# Step contract fixtures

One JSON file per case of [the step contract](../../step-contract.md). Load them from tests; do not copy
them. Each `<name>.json` has its [wire form](../../step-contract.md#6-wire-form) beside it as
`<name>.wire`: the exact bytes that travel, with no newline at the end.

All fixtures describe one run, `hera-demo-x7k2p`, of the app `hera-demo` in the project `vision`:
`prepare` reads the dataset `raw` and produces the dataset `embeddings`; `train` reads `labels` and
`embeddings` and produces `model`; `export` is conditional and was skipped; `evaluate` needs `train` and
`export`.

Each context's `params` holds only the config sections its step names in `reads=`: here one section, named
after the step. Dataset references in config name the repository (`dataset-raw-images@latest`); they are
not in these fixtures, because an input is read through `ledger`, not through `params`.

| File | What it is |
|---|---|
| `context-no-upstream.json` | The context of `prepare`: one input, one produced dataset, no upstream. |
| `context-one-upstream.json` | The context of `train`: one upstream (`prepare`, whose result is `result-ok.json`). |
| `context-skipped-upstream.json` | The context of `evaluate`: `train` ran (`result-model.json`), `export` was skipped and is the sentinel. |
| `context-escapes.json` | A context whose strings hold `{{...}}`, `&`, `<`, `>`, a quote, a backslash and non-ASCII characters, and whose numbers a float would change. Its `.wire` file shows every escape. |
| `result-ok.json` | The result of `prepare`: one read, one write (a dataset). |
| `result-model.json` | The result of `train`: two reads (one of a dataset produced in the run), one write (a model). |
| `result-skipped.json` | The result of a step that ended itself as skipped with `step.skip("reason")`: no values, no reads, no writes. |
| `result-absent.json` | The sentinel: what stands in for a result that is missing. |
| `ledger-two-reads.json` | A ledger with two reads, as compose writes it when the run starts. |
| `ledger-run-union.json` | The ledger of the whole run: the union of compose's reads and every step's result. |
| `hpc-job.json` | An `HPCArgoWorkflowJob` for `train` with the contract's fields: `spec.app`, `spec.inputs[]`, `spec.context` (equal to `context-one-upstream.wire`) and `status.result` (equal to `result-model.wire`). |

Every fixture is far below the size caps (a context may cost 96 KiB, a result 8 KiB; section 7 of the
contract). The cases at the caps are built by the checks, not stored here.

To change a fixture: edit the `.json` file, then

```bash
python3 hack/contract-checks/wire.py write-fixtures   # rewrites every .wire file and hpc-job.json
python3 hack/contract-checks/wire.py check-fixtures   # the same check hack/contract-checks/0-fixtures.sh runs
```
