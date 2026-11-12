"""Live lessons of the old in-pod waiter, each pinned by a controller test (PRD-HPC-1461 AC-11).

Every test is named after the lesson and carries the live job (or date) that taught it. The table that maps
all lessons to their tests is docs/live-lessons.md. These tests run the REAL batch script lines
(jobscript.batch_lines) in bash against fake `apptainer`/`module`/`scontrol` binaries, so a change to the
script that drops a lesson fails here.
"""
import io
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from kaos_hpc_runner import jobscript, wallet

LEASE_RE = re.compile(r'kaos-commit=([0-9a-f]{6,64})')   # the stager's pattern, applied to `squeue -o %k`
BASH = shutil.which('bash')


def _lines():
    return jobscript.batch_lines('-', '/scratch')


def _span(first_prefix, last_prefix):
    """The script lines from the first starting with first_prefix to the first starting with last_prefix."""
    lines = _lines()
    a = next(i for i, l in enumerate(lines) if l.startswith(first_prefix))
    b = next(i for i, l in enumerate(lines) if i >= a and l.startswith(last_prefix))
    return lines[a:b + 1]


def _fake(bindir, name, body):
    bindir.mkdir(parents=True, exist_ok=True)
    p = bindir / name
    p.write_text('#!/bin/bash\n' + body)
    p.chmod(0o755)


# ------------------------------------------------ OCI WORKDIR -> apptainer --pwd (live job 5148071)

class _Registry:
    """A fake registry behind urllib: serves manifest(s) and the image config, records the requests."""

    def __init__(self, workdir, multi_arch=False):
        cfg = {'config': {'WorkingDir': workdir} if workdir is not None else {}}
        manifest = {'config': {'digest': 'sha256:cfg'}}
        self.docs = {'/manifests/v1': manifest, '/blobs/sha256:cfg': cfg}
        if multi_arch:
            self.docs['/manifests/v1'] = {'manifests': [
                {'digest': 'sha256:arm', 'platform': {'architecture': 'arm64', 'os': 'linux'}},
                {'digest': 'sha256:amd', 'platform': {'architecture': 'amd64', 'os': 'linux'}}]}
            self.docs['/manifests/sha256:amd'] = manifest
            self.docs['/manifests/sha256:arm'] = {'config': {'digest': 'sha256:wrong'}}
            self.docs['/blobs/sha256:wrong'] = {'config': {'WorkingDir': '/wrong-arch'}}
        self.requests = []

    def urlopen(self, req, timeout=None):
        self.requests.append(req)
        path = req.full_url.split('/v2/app', 1)[1]
        return io.BytesIO(json.dumps(self.docs[path]).encode())


@pytest.fixture
def registry(monkeypatch):
    def make(workdir, **kw):
        reg = _Registry(workdir, **kw)
        monkeypatch.setattr(wallet.urllib.request, 'urlopen', reg.urlopen)
        return reg
    return make


def test_fetch_workdir_reads_the_images_workdir_from_the_registry_config__live_job_5148071(registry):
    reg = registry('/opt/app')
    assert wallet.fetch_workdir('zot.example/app:v1', '') == '/opt/app'
    assert [r.full_url for r in reg.requests] == ['https://zot.example/v2/app/manifests/v1',
                                                  'https://zot.example/v2/app/blobs/sha256:cfg']


def test_fetch_workdir_picks_the_linux_amd64_manifest_of_a_multi_arch_index__live_job_5148071(registry):
    registry('/opt/app', multi_arch=True)
    assert wallet.fetch_workdir('zot.example/app:v1', '') == '/opt/app'


def test_fetch_workdir_is_empty_for_an_image_without_a_workdir__live_job_5148071(registry):
    registry(None)
    assert wallet.fetch_workdir('zot.example/app:v1', '') == ''


def test_fetch_workdir_never_blocks_a_submit_when_the_registry_fails__live_job_5148071(monkeypatch):
    def boom(req, timeout=None):
        raise OSError('registry down')
    monkeypatch.setattr(wallet.urllib.request, 'urlopen', boom)
    assert wallet.fetch_workdir('zot.example/app:v1', 'tok') == ''


def test_fetch_workdir_presents_the_token_only_to_gar_hosts__live_job_5140432(registry):
    reg = registry('/w')
    wallet.fetch_workdir('zot.example/app:v1', 'tok')
    assert all(r.get_header('Authorization') is None for r in reg.requests)
    reg = registry('/w')
    wallet.fetch_workdir('europe-docker.pkg.dev/app:v1', 'tok')
    assert reg.requests and all(r.get_header('Authorization') == 'Bearer tok' for r in reg.requests)


def _exec_script(tmp_path, workdir, gpus, extra_env=None, probe_ok=True):
    """The batch script's PWD_OPT/NV lines and the `apptainer exec`, run against a recording fake apptainer."""
    block = _span('PWD_OPT=""', 'apptainer exec') + ['wait']
    # The fence probe is `apptainer exec --nv --nvccli img.sif true`; the step's own exec records args and env.
    _fake(tmp_path / 'bin', 'apptainer',
          '[ "$*" = "exec --nv --nvccli img.sif true" ] && exit %d\n' % (0 if probe_ok else 1) +
          'echo "$@" > "$OUT"; env | grep "^APPTAINERENV_" | sort >> "$OUT.env"\n'
          'env | grep -E "^(CUDA_VISIBLE_DEVICES|NVIDIA_VISIBLE_DEVICES|KUBECORE_SLOT_GPUS)=" | sort > "$OUT.host"\n')
    env = dict(os.environ, PATH=str(tmp_path / 'bin') + os.pathsep + os.environ['PATH'], OUT=str(tmp_path / 'args'),
               SIF='img.sif', BIND='', STEP_CMD='true', STEP_WORKDIR=workdir, **(extra_env or {}))
    env.pop('HPC_GPUS', None)
    if gpus is not None:
        env['HPC_GPUS'] = str(gpus)
    subprocess.run([BASH, '-c', '\n'.join(block)], env=env, check=True, cwd=tmp_path)
    return (tmp_path / 'args').read_text().split()


def test_the_step_starts_in_the_images_workdir_with_pwd__live_job_5148071(tmp_path):
    args = _exec_script(tmp_path, '/opt/app', gpus=0)
    assert args[args.index('--pwd') + 1] == '/opt/app'
    assert args.index('--pwd') < args.index('img.sif'), 'an option after the image is the step command'


def test_an_image_without_a_workdir_gets_no_pwd__live_job_5148071(tmp_path):
    assert '--pwd' not in _exec_script(tmp_path, '', gpus=0)


def test_the_runner_puts_the_looked_up_workdir_into_the_job_environment__live_job_5148071():
    src = (Path(__file__).resolve().parents[1] / 'kaos_hpc_runner' / 'runner.py').read_text()
    assert "'STEP_WORKDIR=' + wallet.fetch_workdir(cfg.image, reg)" in src


# ------------------------------------------------ --nv only on GPU classes (cpu/largemem partitions)

@pytest.mark.parametrize('gpus, want_nv', [(1, True), (4, True), (0, False), (None, False)])
def test_nv_is_passed_on_gpu_classes_only__cpu_and_largemem_partitions(tmp_path, gpus, want_nv):
    args = _exec_script(tmp_path, '', gpus=gpus)
    assert ('--nv' in args) is want_nv
    if want_nv:
        assert args.index('--nv') < args.index('img.sif')


# ------------------------------------------------ AC-57 / D-29: a shared-node step sees only its own GPUs

def _fenced(tmp_path, cuda, shared='1', cli=True, probe_ok=True):
    if cli:
        _fake(tmp_path / 'bin', 'nvidia-container-cli', 'exit 0\n')
    env = {'KUBECORE_SLOT_SHARED': shared, 'CUDA_VISIBLE_DEVICES': cuda}
    args = _exec_script(tmp_path, '', gpus=4, extra_env=env, probe_ok=probe_ok)
    envf = tmp_path / 'args.env'
    return args, (envf.read_text().split() if envf.exists() else [])


def test_a_shared_step_gets_only_its_gpu_bound_and_renumbered_when_nvccli_works(tmp_path):
    args, env = _fenced(tmp_path, '2')
    assert args[:3] == ['exec', '--nv', '--nvccli']
    assert env == ['APPTAINERENV_CUDA_VISIBLE_DEVICES=0', 'APPTAINERENV_NVIDIA_VISIBLE_DEVICES=2']


def test_a_two_gpu_shared_step_sees_cuda_0_and_1(tmp_path):
    args, env = _fenced(tmp_path, '1,3')
    assert '--nvccli' in args
    assert env == ['APPTAINERENV_CUDA_VISIBLE_DEVICES=0,1', 'APPTAINERENV_NVIDIA_VISIBLE_DEVICES=1,3']


def test_a_step_falls_back_to_plain_nv_when_the_nvccli_probe_fails(tmp_path):
    args, env = _fenced(tmp_path, '2', probe_ok=False)
    assert '--nv' in args and '--nvccli' not in args
    assert env == [], 'no renumbered devices without the fence: the slot manager watchdog is the guard'


def test_a_step_falls_back_to_plain_nv_without_nvidia_container_cli(tmp_path):
    args, env = _fenced(tmp_path, '2', cli=False)
    assert '--nv' in args and '--nvccli' not in args and env == []


def test_a_job_that_owns_its_node_is_not_fenced(tmp_path):
    args, env = _fenced(tmp_path, '0', shared='0')
    assert '--nv' in args and '--nvccli' not in args and env == []


# ------------------------------------------------ Lmod: module load, exit 210 without apptainer

def _lmod_script(tmp_path, apptainer_loadable):
    block = _span('module load', 'command -v apptainer')
    bindir = tmp_path / 'bin'
    _fake(bindir, 'apptainer', 'exit 0\n')
    (tmp_path / 'empty').mkdir(exist_ok=True)
    pre = ('fail(){ echo "$1" > failed; exit "$1"; }\n'
           'module(){ echo "$*" >> modlog; if [ "$2" = apptainer ] && [ -n "$LOADABLE" ];'
           ' then export PATH="$BIN:$PATH"; return 0; fi; return 1; }\n')
    env = dict(os.environ, PATH=str(tmp_path / 'empty'), BIN=str(bindir), LOADABLE='1' if apptainer_loadable else '')
    rc = subprocess.run([BASH, '-c', pre + '\n'.join(block)], env=env, cwd=tmp_path).returncode
    log = (tmp_path / 'modlog').read_text().splitlines() if (tmp_path / 'modlog').exists() else []
    return rc, log


def test_the_script_loads_the_apptainer_module_under_either_name(tmp_path):
    rc, log = _lmod_script(tmp_path, apptainer_loadable=True)
    assert rc == 0 and log == ['load Apptainer', 'load apptainer']


def test_the_script_fails_210_when_lmod_cannot_provide_apptainer(tmp_path):
    rc, log = _lmod_script(tmp_path, apptainer_loadable=False)
    assert rc == 210 and (tmp_path / 'failed').read_text().strip() == '210'
    assert log == ['load Apptainer', 'load apptainer']


def test_the_batch_script_restores_lmod_before_the_module_load():
    lines = _lines()
    assert lines[0] == '#!/bin/bash -l'
    init = next(i for i, l in enumerate(lines) if l.startswith('for f in /usr/share/lmod'))
    load = next(i for i, l in enumerate(lines) if l.startswith('module load'))
    assert init < load


# ------------------------------------------------ GCP token for GAR hosts only (live job 5140432)

def _pull(tmp_path, ref, token):
    lines = _lines()
    start = lines.index('if [ ! -f "$SIF" ]; then')
    block = lines[start:lines.index('fi', start) + 1]
    scr = tmp_path / 'scratch'
    (scr / 'sif-cache').mkdir(parents=True)
    _fake(tmp_path / 'bin', 'apptainer',
          'echo "user=$APPTAINER_DOCKER_USERNAME pass=$APPTAINER_DOCKER_PASSWORD" > "$SCR/cred"\nprintf sif > "$2"\n')
    env = {k: v for k, v in os.environ.items() if not k.startswith('APPTAINER')}
    env.update(PATH=str(tmp_path / 'bin') + os.pathsep + os.environ['PATH'], SCR=str(scr),
               SIF=str(scr / 'sif-cache' / 'k.sif'), IMAGE_REF=ref, SLURM_JOB_ID='1', REG_TOKEN=token)
    script = 'fail(){ exit "$1"; }\n' + '\n'.join(block)
    assert subprocess.run([BASH, '-c', script], env=env).returncode == 0
    return (scr / 'cred').read_text().strip()


@pytest.mark.parametrize('ref, token, want', [
    ('europe-docker.pkg.dev/proj/repo/app@sha256:ab', 'tok', 'user=oauth2accesstoken pass=tok'),
    ('us-docker.pkg.dev/proj/repo/app:v1', 'tok', 'user=oauth2accesstoken pass=tok'),
    ('zot.example/proj/app:v0.0.0', 'tok', 'user= pass='),            # Zot: a token turns anonymous OK into 401
    ('docker.pkg.dev.evil.example/app:v1', 'tok', 'user= pass='),     # only a real GAR host name
    ('europe-docker.pkg.dev/proj/repo/app:v1', '', 'user= pass='),    # no token: nothing to present
])
def test_the_gcp_token_goes_to_gar_hosts_only__live_job_5140432(tmp_path, ref, token, want):
    assert _pull(tmp_path, ref, token) == want


# ------------------------------------------------ in-job stage-in failure is exit 232, never silent

def _inputs_block():
    """The job script from its inputs loop to the line after it (the staged root of the step)."""
    lines = _lines()
    return lines[lines.index('BIND=""; KAOS_IN_ROOT=$SCR/kaos-tmp/in-$KAOS_JOB_ID; rm -rf "$KAOS_IN_ROOT"'):lines.index('done') + 2]


def _stagein(tmp_path, stagein_py, staged_ready=False):
    import base64
    staged = tmp_path / 'staged'
    staged.mkdir()
    if staged_ready:
        (staged / '.kaos-ready').write_text('{}')
    env = dict(os.environ, SCR=str(tmp_path / 'scr'), KAOS_JOB_ID='9', LAKEFS_BEARER_TOKEN='w', LAKEFS_ENDPOINT='http://x',
               KAOS_INPUTS='raw|dataset-r|c0ffee12|%s' % staged,
               STAGEIN_B64=base64.b64encode(stagein_py.encode()).decode())
    (tmp_path / 'scr' / 'kaos-tmp').mkdir(parents=True)
    script = 'fail(){ echo "$1" > failed; exit "$1"; }\n' + '\n'.join(_inputs_block()) + '\necho "BIND=$BIND"\n'
    p = subprocess.run([BASH, '-c', script], env=env, cwd=tmp_path, capture_output=True, text=True)
    return p, staged


def test_a_failed_in_job_stage_in_ends_the_job_232_instead_of_running_without_data(tmp_path):
    p, _ = _stagein(tmp_path, 'import sys; sys.exit(3)')
    assert p.returncode == 232 and (tmp_path / 'failed').read_text().strip() == '232'
    assert 'BIND=' not in p.stdout, 'the step must never start after a failed stage-in'
    assert 'downloading in-job' in p.stdout, 'the not-ready staged copy is said to be replaced by the download'


def test_a_successful_in_job_stage_in_binds_the_downloaded_copy(tmp_path):
    copy = tmp_path / 'data' / 'dataset-r' / 'c0ffee12'
    ok = ('import os, pathlib; d = pathlib.Path(%r); d.mkdir(parents=True); (d / ".kaos-complete").touch(); '
          'assert os.environ["DATASET_REPO"] == "dataset-r" and os.environ["DATASET_COMMIT"] == "c0ffee12"; '
          'open(os.environ["DATASET_DIRFILE"], "w").write(str(d))' % str(copy))
    p, _ = _stagein(tmp_path, ok)
    root = tmp_path / 'scr' / 'kaos-tmp' / 'in-9'
    assert p.returncode == 0 and 'BIND=-B %s:/kaos/inputs:ro -B %s:/kaos/inputs/raw:ro' % (root, copy) in p.stdout, p.stdout + p.stderr
    # The record the step finds the input by is BESIDE the folder the copy is bound onto, never inside it.
    assert json.loads((root / 'raw.kaos-ready').read_text()) == {'repo': 'dataset-r', 'commit': 'c0ffee12', 'full': True}
    assert (root / 'raw').is_dir() and list((root / 'raw').iterdir()) == []


def test_an_in_job_stage_in_that_leaves_no_complete_copy_ends_the_job_232(tmp_path):
    """The stage-in program ended well but its folder has no record that it is whole: never bound as it is."""
    p, _ = _stagein(tmp_path, 'import os; open(os.environ["DATASET_DIRFILE"], "w").write("%s")' % (tmp_path / 'nowhere'))
    assert p.returncode == 232 and 'BIND=' not in p.stdout
    assert 'is not complete' in p.stdout
    assert not (tmp_path / 'scr' / 'kaos-tmp' / 'in-9' / 'raw.kaos-ready').exists(), 'no record for a copy that is not whole'


def test_a_ready_staged_copy_skips_the_in_job_download_and_is_marked_used(tmp_path):
    p, staged = _stagein(tmp_path, 'import sys; sys.exit(3)', staged_ready=True)
    assert p.returncode == 0 and '-B %s:/kaos/inputs/raw:ro' % staged in p.stdout
    assert (staged / '.kaos-used').exists()
    assert (tmp_path / 'scr' / 'kaos-tmp' / 'in-9' / 'raw.kaos-ready').exists()


# ------------------------------------------------ dataset-commit lease vs the 24 h eviction rule

def test_a_queued_job_names_its_dataset_commits_where_the_stagers_lease_pattern_finds_them():
    """The stager evicts copies unused for 24 h EXCEPT those a queued/running job names in `squeue -o %k`
    (PRD-1541 D-06). Each lease word must therefore be a lowercase hex commit the stager's LEASE_RE finds,
    next to the image and checkpoint leases that must not hide it: one word per input."""
    src = (Path(__file__).resolve().parents[1] / 'kaos_hpc_runner' / 'runner.py').read_text()
    assert "commits = ['kaos-commit=' + i['commit'] for i in cfg.inputs]" in src and "lease = list(commits)" in src
    comment = 'kaos-commit=c0ffee12 kaos-commit=deadbeef kaos-image=0123456789abcdef kaos-ckpt-lease=wf1/train'
    assert LEASE_RE.findall(comment) == ['c0ffee12', 'deadbeef']


def test_a_resident_copy_is_marked_used_by_the_job_that_starts_on_it_so_the_24h_clock_restarts(tmp_path):
    """The stager evicts copies unused for 24 h; the job that starts on a ready copy refreshes its marker."""
    staged = tmp_path / 'staged'
    staged.mkdir()
    (staged / '.kaos-ready').write_text('')
    marker = staged / '.kaos-used'
    marker.write_text('')
    old = time.time() - 30 * 3600
    os.utime(marker, (old, old))
    (tmp_path / 'kaos-tmp').mkdir()
    env = dict(os.environ, SCR=str(tmp_path), KAOS_JOB_ID='9', KAOS_INPUTS='raw|dataset-r|c0ffee12|%s' % staged)
    subprocess.run([BASH, '-c', 'fail(){ exit "$1"; }\n' + '\n'.join(_inputs_block())], env=env, check=True)
    assert time.time() - marker.stat().st_mtime < 60


# ------------------------------------------------ AC-37 (live job 5439721): clean GPU list, slot GPUs when alone

def _host_env(tmp_path, gpus, extra_env):
    _exec_script(tmp_path, '', gpus=gpus, extra_env=extra_env)
    return (tmp_path / 'args.host').read_text().splitlines()


def test_a_step_alone_sees_cuda_without_trailing_blanks_and_slot_gpus_from_its_class__live_job_5439721(tmp_path):
    host = _host_env(tmp_path, 4, {'CUDA_VISIBLE_DEVICES': '0,1,2,3     '})
    assert host == ['CUDA_VISIBLE_DEVICES=0,1,2,3', 'KUBECORE_SLOT_GPUS=4']


def test_padding_is_removed_from_nvidia_visible_devices_too__live_job_5439721(tmp_path):
    host = _host_env(tmp_path, 4, {'CUDA_VISIBLE_DEVICES': ' 0, 1,2,3\t', 'NVIDIA_VISIBLE_DEVICES': '0,1,2,3  '})
    assert 'CUDA_VISIBLE_DEVICES=0,1,2,3' in host and 'NVIDIA_VISIBLE_DEVICES=0,1,2,3' in host


def test_a_shared_slot_keeps_the_slot_managers_slot_gpus_and_gets_a_clean_list__live_job_5439721(tmp_path):
    host = _host_env(tmp_path, 4, {'CUDA_VISIBLE_DEVICES': '2   ', 'KUBECORE_SLOT_GPUS': '1', 'KUBECORE_SLOT_SHARED': '1'})
    assert host == ['CUDA_VISIBLE_DEVICES=2', 'KUBECORE_SLOT_GPUS=1']


def test_a_padded_list_reaches_the_container_clean_on_the_fenced_path_too__live_job_5439721(tmp_path):
    args, env = _fenced(tmp_path, '1,3   ')
    assert '--nvccli' in args
    assert env == ['APPTAINERENV_CUDA_VISIBLE_DEVICES=0,1', 'APPTAINERENV_NVIDIA_VISIBLE_DEVICES=1,3']


def test_a_cpu_class_gets_no_slot_gpus_and_no_cuda_variable__live_job_5439721(tmp_path):
    assert _host_env(tmp_path, 0, {}) == []
