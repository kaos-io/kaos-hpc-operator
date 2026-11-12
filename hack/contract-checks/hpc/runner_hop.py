#!/usr/bin/env python3
"""Check 4, the runner's part: from the runner's environment to the step's environment in the job.

Started by hops_test.go with the environment of the runner Job's container, as the API server stored
it, plus KAOS_STEP_CONTEXT. With the runner's own modules it builds the site job twice, the way
Runner.submit does: alone (the inputs in the Slurm environment) and for a shared node (the inputs as
export lines in the job script). Each is submitted to the fake Slurm site with the runner's client and
then run with bash and a stand-in for Apptainer. The step is a program that records what it finds in
KAOS_STEP_CONTEXT.

Prints one JSON object: for each mode the site job's name, the request and script sizes, and the byte
count and sha256 the step saw.
"""
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "runner"))

from kaos_hpc_runner import jobscript, payloads  # noqa: E402
from kaos_hpc_runner.settings import Settings  # noqa: E402
from kaos_hpc_runner.sharing import inputs_in_script  # noqa: E402
from kaos_hpc_runner.slurm import Slurm  # noqa: E402

STEP = '''import hashlib, json, os, sys
v = os.environ.get("KAOS_STEP_CONTEXT", "").encode()
json.dump({"bytes": len(v), "sha256": hashlib.sha256(v).hexdigest()}, open(sys.argv[1], "w"))
'''

# `apptainer pull|build SIF ...` leaves a file; `apptainer exec [opts] SIF cmd...` runs cmd; the rest is a no-op.
APPTAINER = '''#!/bin/bash
case "$1" in
  pull|build) : > "$2" ;;
  exec) shift; while [ $# -gt 0 ] && [ "${1%.sif}" = "$1" ]; do shift; done; shift; exec "$@" ;;
esac
'''


def job_env(cfg, context, step_cmd, shared):
    """The site job's environment as Runner.submit builds it (a job without a data-plane token), plus the context."""
    ident = cfg.identity()
    env = ['PATH=/usr/bin:/bin:/usr/local/bin', 'HOME=' + ident['home'], 'USER=' + ident['user'],
           'IMAGE_REF=' + cfg.image, 'REG_TOKEN=', 'HPC_GPUS=' + str(cfg.hpc_class().get('gpus') or 0),
           'STEP_CMD=' + step_cmd, 'STEP_WORKDIR=', 'WF_UID=' + cfg.wf_uid, 'STEP_NAME=' + cfg.step,
           'STEP_OUTPUTS=', 'DATASET_STAGED=', 'IMAGE_STAGED=', 'KAOS_ATTEMPT=%d' % cfg.attempt,
           'KAOS_IDLE_TIMEOUT_MIN=%d' % cfg.idle_timeout, 'IDLEWATCH_B64=' + payloads.b64(payloads.IDLEWATCH),
           'CKPT_B64=' + payloads.b64(payloads.CKPT), 'KAOS_CKPT_BUDGET_PCT=%d' % cfg.ckpt_budget_pct,
           'KAOS_PROJID=1', 'STAGEIN_B64=' + payloads.b64(payloads.STAGEIN),
           'STAGEOUT_B64=' + payloads.b64(payloads.STAGEOUT),
           'KAOS_STEP_CONTEXT=' + context]
    if shared:
        env += ['KAOS_SLOTS_B64=' + payloads.b64(payloads.SLOTS), 'KAOS_SCOPE=' + cfg.scope,
                'KAOS_SCR=' + ident['scratch'], 'KAOS_STEP_LIMIT_MIN=%d' % cfg.time_limit]
    return env


def one(cfg, context, mode, work):
    shared = mode == 'shared'
    scratch = work / mode / 'scratch'
    bindir = work / mode / 'bin'
    for d in (scratch, bindir):
        d.mkdir(parents=True)
    out = work / mode / 'step-saw.json'
    step = work / mode / 'step.py'
    step.write_text(STEP)
    (bindir / 'apptainer').write_text(APPTAINER)
    for tool in ('squeue', 'scontrol'):
        (bindir / tool).write_text('#!/bin/bash\nexit 0\n')
    for tool in ('apptainer', 'squeue', 'scontrol'):
        (bindir / tool).chmod(0o755)

    env = job_env(cfg, context, 'python3 %s %s' % (step, out), shared)
    batch = '\n'.join(jobscript.batch_lines(cfg.scope if shared else '-', str(scratch)))
    if shared:
        batch, env = inputs_in_script(batch, env)
    name = cfg.job_site_name + '-' + mode + ('-control' if os.environ.get('CONTRACT_CHECK_DAMAGE') else '')
    body = {'job': {'name': name, 'partition': cfg.hpc_class()['partition'],
                    'account': cfg.identity()['account'], 'qos': cfg.hpc_class().get('qos') or 'default',
                    'time_limit': cfg.time_limit, 'current_working_directory': cfg.identity()['home'],
                    'environment': env, 'tasks': 1, 'nodes': '1', 'comment': 'kaos-ckpt-lease=x/y'},
            'script': batch}
    reply = Slurm(cfg).submit(body)

    # The node: Slurm starts the script with the submitted environment (and its own variables).
    node_env = dict(e.split('=', 1) for e in env)
    node_env.update(PATH=str(bindir) + os.pathsep + os.environ['PATH'], SLURM_JOB_ID=str(reply['job_id']),
                    HOME=str(work / mode))
    if shared:
        # On a shared node the host runs each step's script as a job step and marks it so.
        node_env['KAOS_IN_STEP'] = '1'
    script = work / mode / 'job.sh'
    script.write_text(batch)
    run = subprocess.run(['bash', str(script)], cwd=str(work / mode), env=node_env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    saw = json.loads(out.read_text()) if out.exists() else {}
    return {'job': name, 'site_job_id': reply['job_id'], 'request_bytes': len(json.dumps(body)),
            'script_bytes': len(batch), 'largest_env_string': max(len(e) for e in env),
            'script_exit': run.returncode, 'step_saw': saw,
            'script_output_tail': run.stdout.decode(errors='replace')[-600:] if not saw else ''}


def main():
    context = os.environ['KAOS_STEP_CONTEXT']
    cfg = Settings(dict(os.environ))
    # The control run of the check: one character of what the job is given differs from what the runner
    # received. The step must then report another sha256, or the comparison proves nothing.
    handed = context.replace('contract-4', 'contract-X', 1) if os.environ.get('CONTRACT_CHECK_DAMAGE') else context
    with tempfile.TemporaryDirectory() as tmp:
        result = {mode: one(cfg, handed, mode, pathlib.Path(tmp)) for mode in ('alone', 'shared')}
    result['runner_saw'] = {'bytes': len(context.encode()), 'sha256': hashlib.sha256(context.encode()).hexdigest()}
    json.dump(result, sys.stdout)


if __name__ == '__main__':
    main()
