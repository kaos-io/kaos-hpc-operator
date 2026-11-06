"""PRD-1609 node sharing: where a host can read a waiting job, and what it must not see."""

import shlex

# What stays in the Slurm environment when the inputs move into the script: the login shell builds
# PATH from it, so it is never re-exported over the profile.
SLURM_ENV_KEEP = ('PATH', 'HOME', 'USER')


def inputs_in_script(batch, env):
    """D-02: a host can read a waiting job's script, never its environment (probe 2026-11-03), so
    with sharing on the step's inputs ride in the script as export lines after its header, and the
    Slurm environment keeps only SLURM_ENV_KEEP."""
    keep, exports = [], []
    for e in env:
        k, v = e.split('=', 1)
        if k in SLURM_ENV_KEEP:
            keep.append(e)
        else:
            exports.append('export %s=%s' % (k, shlex.quote(v)))
    lines = batch.split('\n')
    return '\n'.join(lines[:3] + exports + lines[3:]), keep
