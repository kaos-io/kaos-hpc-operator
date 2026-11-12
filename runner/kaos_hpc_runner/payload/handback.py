# Hands the step's result back (docs/step-contract.md, section 8). The step wrote one file,
# /work/output/kaos-result.json ($KAOS_WORK/output on the node), or none. Its bytes go, unread, to lakeFS at
# hpc-outputs/{wf-uid}/{step}/{site job id}/kaos-result.json on a hand-off branch of the app's outputs
# repository; the runner fetches them, checks them and puts them in the job's status, and deletes the branch only after
# that report was accepted (a runner that dies in between leaves it for the next runner of the attempt).
# When the step wrote no file, an object kaos-result.absent says so: the runner must be able to tell "no
# result" from "the result did not arrive". The site job id is in the path, so a result of an earlier attempt
# or of a job this one replaced can never be taken for this one's.
#
# Any failure ends this program with a line "result hand-back: <cause>" and a non-zero exit; the job script
# then ends the job 243 (ResultNotHandedBack). Nothing here reports success on failure (PRD kaos#1911 CON-04).
# Runs under the node's system python3 (3.6, stdlib only).
import json, os, stat, sys, time, urllib.error, urllib.parse, urllib.request

MAX_BYTES = 64 * 1024   # a result may cost 8 KiB; a file far above that is not handed back at all


def die(why):
    print('result hand-back: ' + why, flush=True)
    sys.exit(1)


missing = [k for k in ('LAKEFS_ENDPOINT', 'LAKEFS_BEARER_TOKEN', 'KAOS_OUTPUTS_REPO', 'WF_UID', 'STEP_NAME', 'KAOS_JOB_ID',
                       'KAOS_WORK') if not os.environ.get(k)]
if missing:
    die('the job has no %s, so the step\'s result cannot be handed back' % ', '.join(missing))
base = os.environ['LAKEFS_ENDPOINT'].rstrip('/') + '/api/v1/repositories/' + urllib.parse.quote(os.environ['KAOS_OUTPUTS_REPO'], safe='')
H = {'Authorization': 'Bearer ' + os.environ['LAKEFS_BEARER_TOKEN']}
branch = 'hpc-%s-%s' % (os.environ['WF_UID'], os.environ['STEP_NAME'])
prefix = 'hpc-outputs/%s/%s/%s/' % (os.environ['WF_UID'], os.environ['STEP_NAME'], os.environ['KAOS_JOB_ID'])
path = os.path.join(os.environ['KAOS_WORK'], 'output', 'kaos-result.json')

name, other, data = 'kaos-result.absent', 'kaos-result.json', b'absent\n'
if os.path.lexists(path):
    try:
        st = os.lstat(path)
        if not stat.S_ISREG(st.st_mode):
            die('the result file %s is not a regular file' % path)
        if st.st_size > MAX_BYTES:
            die('the result file is %d bytes; a result may cost 8 KiB. Metrics and anything large belong in MLflow '
                'or in an artifact, not in the result' % st.st_size)
        with open(path, 'rb') as f:
            data = f.read()
    except OSError as e:
        die('the result file %s cannot be read: %s' % (path, e))
    name, other = other, name


def call(req, ok=()):
    # Three tries: the node's network may blip at the end of a long job.
    for n in range(3):
        try:
            return urllib.request.urlopen(req, timeout=120).read()
        except urllib.error.HTTPError as e:
            if e.code in ok:
                return b''
            err = 'HTTP %d %s' % (e.code, (e.read() if e.fp else b'')[:200].decode('utf-8', 'replace'))
            if e.code < 500 and e.code != 429:
                break
        except Exception as e:
            err = str(e)
        if n < 2:
            time.sleep(float(os.environ.get('KAOS_HANDBACK_RETRY_SLEEP') or 5))
    die('lakeFS did not take the step\'s result (%s %s): %s' % (req.get_method(), req.full_url.split('?')[0], err))


call(urllib.request.Request(
    base + '/branches', method='POST',
    data=json.dumps({'name': branch, 'source': 'main'}).encode(),
    headers=dict(H, **{'Content-Type': 'application/json'})), ok=(409,))   # 409: the branch is there already
body = (b'--B\r\nContent-Disposition: form-data; name="content"; filename="f"\r\n'
        b'Content-Type: application/octet-stream\r\n\r\n' + data + b'\r\n--B--\r\n')
call(urllib.request.Request(
    base + '/branches/%s/objects?%s' % (branch, urllib.parse.urlencode({'path': prefix + name})), data=body, method='POST',
    headers=dict(H, **{'Content-Type': 'multipart/form-data; boundary=B'})))
# A requeued job runs again under the same site job id: what its earlier run left must not stand beside this.
call(urllib.request.Request(
    base + '/branches/%s/objects?%s' % (branch, urllib.parse.urlencode({'path': prefix + other})), method='DELETE',
    headers=H), ok=(404,))
print('result hand-back: %s' % ('the step wrote no result' if name.endswith('.absent')
                                 else 'kaos-result.json, %d bytes' % len(data)), flush=True)
