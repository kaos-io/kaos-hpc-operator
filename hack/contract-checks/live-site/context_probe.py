#!/usr/bin/env python3
"""The live-site half of check 4 of the step contract: ONE job on a real Slurm site that reports
whether a 96 KiB KAOS_STEP_CONTEXT reaches a job unchanged. NOT part of run-all.sh. It spends site time:
run it only with the owner's word (README.md here).

The job carries the same value twice, the two ways the runner sends a step's inputs:
  env     in the Slurm environment (a step alone on its node)
  script  as an export line in the job script (a step for a shared node)
and reports, as words in its own job comment, the byte count and sha256 it finds:
  env=        the variable as Slurm handed it to the job
  script=     the variable set by the export line
  reread=     the export line of the script as `scontrol write batch_script` returns it (what a host of
              a shared node reads from a waiting job)
  container=  the variable inside the container (only with --image; Apptainer must pass it on)

Without --submit it sends nothing: it prints the request's sizes and the sha256 to expect.

Environment: HPC_ENDPOINT, HPC_REST_VERSION (default v0.0.44), HPC_USER, HPC_ACCOUNT, HPC_HOME and
SLURM_TOKEN (or SLURM_TOKEN_FILE).
"""
import argparse
import hashlib
import json
import os
import shlex
import sys
import time
import urllib.request

CAP = 96 * 1024

# Each line reports one way the value arrived. `kaos_word NAME` reads the value from stdin.
JOB = r'''#!/bin/bash -l
# kaos-contract-probe
set +e
kaos_word(){ local f; f=$(mktemp); cat > "$f"; printf '%s=%s:%s' "$1" "$(wc -c < "$f" | tr -d ' ')" "$(sha256sum < "$f" | cut -c1-64)"; rm -f "$f"; }
W="$(printf %s "$KAOS_STEP_CONTEXT" | kaos_word env)"
W="$W $(printf %s "$KAOS_STEP_CONTEXT_SCRIPT" | kaos_word script)"
if scontrol write batch_script "$SLURM_JOB_ID" "/tmp/kaos-probe-$SLURM_JOB_ID.sh" >/dev/null 2>&1; then
  W="$W $(bash -c "$(grep '^export KAOS_STEP_CONTEXT_SCRIPT=' "/tmp/kaos-probe-$SLURM_JOB_ID.sh"); printf %s \"\$KAOS_STEP_CONTEXT_SCRIPT\"" | kaos_word reread)"
  rm -f "/tmp/kaos-probe-$SLURM_JOB_ID.sh"
else
  W="$W reread=unreadable"
fi
if [ -n "$PROBE_IMAGE" ]; then
  for f in /usr/share/lmod/lmod/init/bash /etc/profile.d/lmod.sh; do [ -r "$f" ] && source "$f" && break; done
  module load Apptainer 2>/dev/null || module load apptainer 2>/dev/null
  if command -v apptainer >/dev/null; then
    W="$W $(apptainer exec "$PROBE_IMAGE" /bin/sh -c 'printf %s "$KAOS_STEP_CONTEXT"' | kaos_word container)"
  else
    W="$W container=no-apptainer"
  fi
fi
scontrol update JobId="$SLURM_JOB_ID" "Comment=$W"
echo "$W"
exit 0
'''


def context(size=CAP):
    """A step context in wire form of exactly `size` bytes, with what a shell could change in a string."""
    head = ('{"version":1,"run":"contract-live","app":"hera-demo","project":"vision","step":"train","kind":"train",'
            '"experiment":"it\'s $HOME `x` ; done \\\\n \\"q\\" \\u007b\\u007bworkflow.name\\u007d\\u007d","params":{"blob":"')
    tail = '"},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{"prepare":{"version":1,"status":"absent"}}}'
    fill, links, h = size - len(head) - len(tail), [], "contract-live"
    while 64 * len(links) < fill:
        h = hashlib.sha256(h.encode()).hexdigest()
        links.append(h)
    text = head + "".join(links)[:fill] + tail
    assert len(text) == size and json.loads(text)["version"] == 1
    return text


def request_body(value, args, env):
    lines = JOB.split("\n")
    script = "\n".join(lines[:3] + ["export KAOS_STEP_CONTEXT_SCRIPT=" + shlex.quote(value)] + lines[3:])
    job_env = ["PATH=/usr/bin:/bin:/usr/local/bin", "HOME=" + env["HPC_HOME"], "USER=" + env["HPC_USER"],
               "KAOS_STEP_CONTEXT=" + value, "PROBE_IMAGE=" + (args.image or "")]
    return {"job": {"name": "kaos-contract-probe", "partition": args.partition, "account": env["HPC_ACCOUNT"],
                    "qos": args.qos, "time_limit": args.minutes, "current_working_directory": env["HPC_HOME"],
                    "environment": job_env, "tasks": 1, "nodes": "1"},
            "script": script}


def call(env, method, path, body=None):
    token = env.get("SLURM_TOKEN") or open(env["SLURM_TOKEN_FILE"]).read().strip()
    req = urllib.request.Request(
        "%s/slurm/%s%s" % (env["HPC_ENDPOINT"].rstrip("/"), env.get("HPC_REST_VERSION") or "v0.0.44", path),
        data=json.dumps(body).encode() if body is not None else None, method=method,
        headers={"X-SLURM-USER-NAME": env["HPC_USER"], "X-SLURM-USER-TOKEN": token, "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))


def main(argv=None, env=None):
    env = dict(os.environ if env is None else env)
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--partition", required=True, help="the partition of the site's test lane")
    p.add_argument("--qos", default="default")
    p.add_argument("--minutes", type=int, default=5, help="the job's time limit (default 5)")
    p.add_argument("--image", default="", help="a SIF file on the site, or docker://..., to also check inside a container")
    p.add_argument("--submit", action="store_true", help="really submit the job; without it nothing is sent")
    p.add_argument("--owner-approved", default="", help="who allowed spending site time, and when; required with --submit")
    args = p.parse_args(argv)
    value = context()
    want = "%d:%s" % (len(value), hashlib.sha256(value.encode()).hexdigest())
    if args.submit and not args.owner_approved.strip():
        print("refusing to submit: --owner-approved '<who, when>' is required; site time is the client's money", file=sys.stderr)
        return 2
    needed = ("HPC_HOME", "HPC_USER", "HPC_ACCOUNT") + (("HPC_ENDPOINT",) if args.submit else ())
    missing = [n for n in needed if not env.get(n)]
    if args.submit and missing:
        print("missing environment: %s" % ", ".join(missing), file=sys.stderr)
        return 2
    for name in missing:   # a dry run only measures: the names stand in for the values
        env[name] = "<%s>" % name
    body = request_body(value, args, env)
    print("request: %d bytes; job script: %d bytes; largest environment string: %d bytes"
          % (len(json.dumps(body)), len(body["script"]), max(len(e) for e in body["job"]["environment"])))
    print("expected for every word: %s" % want)
    if not args.submit:
        print("nothing was sent (no --submit)")
        return 0
    print("submitting one job (time limit %d min, partition %s); approved by: %s" % (args.minutes, args.partition, args.owner_approved))
    job_id = call(env, "POST", "/job/submit", body)["job_id"]
    print("site job %s" % job_id)
    deadline = time.time() + 3600
    while time.time() < deadline:
        job = (call(env, "GET", "/job/%s" % job_id).get("jobs") or [{}])[0]
        state = job.get("job_state")
        state = state[0] if isinstance(state, list) else state
        comment = job.get("comment") or ""
        comment = comment.get("job", "") if isinstance(comment, dict) else comment
        if "env=" in comment or state in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"):
            break
        time.sleep(30)
    else:
        print("FAIL  the job did not report within an hour (job %s)" % job_id)
        return 1
    print("job %s is %s; it reports: %s" % (job_id, state, comment))
    ok = True
    for word in ("env", "script", "reread") + (("container",) if args.image else ()):
        got = dict(w.split("=", 1) for w in comment.split() if "=" in w).get(word)
        same = got == want
        ok = ok and same
        print("%s  %s: %s" % ("PASS" if same else "FAIL", word, got))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
