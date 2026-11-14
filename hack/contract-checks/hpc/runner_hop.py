#!/usr/bin/env python3
"""Check 4, the runner's part: from the runner's environment to the step's environment in the job.

Started by hops_test.go with the environment of the runner Job's container, as the API server stored
it, plus KAOS_STEP_CONTEXT. With the runner's own code (Runner.site_job, what Runner.submit sends) it
builds the site job twice: alone (the inputs in the Slurm environment) and for a shared node (the inputs as
export lines in the job script). Each is submitted to the fake Slurm site with the runner's client and
then run with bash and a stand-in for Apptainer. The step is a program that records what it finds in
KAOS_STEP_CONTEXT.

Prints one JSON object: for each mode the site job's name, the request and script sizes, and the byte
count and sha256 the step saw.
"""
import hashlib
import http.server
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "runner"))

from kaos_hpc_runner import identity, wallet  # noqa: E402
from kaos_hpc_runner.runner import Runner  # noqa: E402
from kaos_hpc_runner.settings import CONTEXT_ENV, Settings  # noqa: E402
from kaos_hpc_runner.slurm import Slurm  # noqa: E402

wallet.fetch_workdir = lambda image, token: ''   # no registry in this check: the step starts where the script does

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


def one(context, mode, work, lakefs):
    shared = mode == 'shared'
    # The site project's scratch holds the shared folder kaos/ (3770); the job's root is the project's own folder in it.
    site_scratch = work / mode / 'scratch'
    scratch = site_scratch / 'kaos' / 'proj'
    bindir = work / mode / 'bin'
    for d in (scratch.parent, bindir):
        d.mkdir(parents=True)
    scratch.parent.chmod(0o3770)
    scratch.mkdir()
    scratch.chmod(0o700)    # the project's root, as the connection's set-up leaves it
    out = work / mode / 'step-saw.json'
    step = work / mode / 'step.py'
    step.write_text(STEP)
    (bindir / 'apptainer').write_text(APPTAINER)
    for tool in ('squeue', 'scontrol'):
        (bindir / tool).write_text('#!/bin/bash\nexit 0\n')
    for tool in ('apptainer', 'squeue', 'scontrol'):
        (bindir / tool).chmod(0o755)

    # The site job exactly as the runner submits it (Runner.site_job): alone, the context rides in the Slurm
    # environment; for a shared node, in the script. The step's command is this check's recorder.
    env_in = dict(os.environ, HPC_SCRATCH=str(scratch), STEP_COMMAND=json.dumps(['python3', str(step), str(out)]),
                  LAKEFS_EXTERNAL_URL=lakefs, HPC_SHARING='on' if shared else '0', HPC_SCOPE='a1b2c3d4e5' if shared else '')
    env_in[CONTEXT_ENV] = context
    cfg = Settings(env_in)
    cfg.check_job()
    runner = Runner(cfg, slurm=object(), stager=object(), reporter=object(), logs=object(), mint=lambda: 'check-wallet', keys=lambda: identity.Key('KAOSXA', 'secret', 1e12, '2099-01-01T00:00:00Z'))
    runner.prepare_handback()
    batch, env, _, _ = runner.site_job('', '')
    name = cfg.job_site_name + '-' + mode + ('-control' if os.environ.get('CONTRACT_CHECK_DAMAGE') else '')
    body = {'job': {'name': name, 'partition': cfg.hpc_class()['partition'],
                    'account': cfg.identity()['account'], 'qos': cfg.hpc_class().get('qos') or 'default',
                    'time_limit': cfg.time_limit, 'current_working_directory': cfg.identity()['home'],
                    'environment': env, 'tasks': 1, 'nodes': '1', 'comment': 'kaos-ckpt-lease=x/y'},
            'script': batch}
    reply = Slurm(cfg).submit(body)

    # The site's staging tool places the app's key in the job's own folder before the step starts.
    key_dir = scratch / 'kaos-tmp' / ('key-%s' % reply['job_id'])
    key_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    (key_dir / 'lakefs-key.json').write_text(json.dumps(runner.key.record()))
    (key_dir / 'lakefs-key.json').chmod(0o600)

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


class TakesAnything(http.server.BaseHTTPRequestHandler):
    """Stands in for lakeFS: the job hands the step's (missing) result back when the step ends, and this check
    is about the context, not about that hand-back (runner/tests/test_step_contract.py is)."""

    def log_message(self, *a):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length') or 0))
        self.send_response(201)
        self.send_header('Content-Length', '0')
        self.end_headers()

    do_DELETE = do_POST


def main():
    context = os.environ['KAOS_STEP_CONTEXT']
    # The control run of the check: one character of what the job is given differs from what the runner
    # received. The step must then report another sha256, or the comparison proves nothing.
    handed = context.replace('contract-4', 'contract-X', 1) if os.environ.get('CONTRACT_CHECK_DAMAGE') else context
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), TakesAnything)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory() as tmp:
        result = {mode: one(handed, mode, pathlib.Path(tmp), 'http://127.0.0.1:%d' % srv.server_address[1])
                  for mode in ('alone', 'shared')}
    result['runner_saw'] = {'bytes': len(context.encode()), 'sha256': hashlib.sha256(context.encode()).hexdigest()}
    json.dump(result, sys.stdout)


if __name__ == '__main__':
    main()
