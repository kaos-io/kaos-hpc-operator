"""PRD kaos#1911 F-04 / AC-05: the in-job stage-in (the fallback when nothing was staged from a login node)
downloads the whole tree of the pinned commit, whatever its shape, and completes a copy an older stage-in left
with only data.yaml, images/ and labels/ instead of serving it. The real payload runs against a fake lakeFS."""
import json
import subprocess
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from kaos_hpc_runner import payloads

COMMIT = "c0ffee" * 8
YOLO = {"data.yaml": b"names: [x]", "images/a.jpg": b"a" * 100, "labels/a.txt": b"0 0.5"}
REST = {"README.md": b"# notes", "calibration/cam0.json": b'{"fx": 1}'}
NOT_YOLO = {"scenes/0001/points.ply": b"p" * 300, "scenes/0001/poses.json": b"{}",
            "calibration/cam0.json": b'{"fx": 1}', "README.md": b"# a 3D dataset", "splits.csv": b"0001,train\n"}


class _LakeFS:
    """objects/ls and objects of one commit of one repository; counts the downloads."""

    def __init__(self, objects):
        self.objects, self.downloads = objects, []
        fake = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                url = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(url.query)
                if url.path.endswith("/refs/%s/objects/ls" % COMMIT):
                    body = json.dumps({"results": [{"path": p, "path_type": "object", "size_bytes": len(b)}
                                                   for p, b in sorted(fake.objects.items())],
                                       "pagination": {"has_more": False}}).encode()
                elif url.path.endswith("/refs/%s/objects" % COMMIT) and q["path"][0] in fake.objects:
                    fake.downloads.append(q["path"][0])
                    body = fake.objects[q["path"][0]]
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]


@pytest.fixture
def lakefs():
    made = []

    def make(objects):
        made.append(_LakeFS(objects))
        return made[-1]
    yield make
    for fake in made:
        fake.httpd.shutdown()
        fake.httpd.server_close()


def _stage_in(tmp_path, fake):
    env = {"LAKEFS_ENDPOINT": fake.url, "LAKEFS_BEARER_TOKEN": "w", "DATASET_REPO": "ds", "DATASET_COMMIT": COMMIT,
           "DATASET_CACHE": str(tmp_path / "data-cache"), "DATASET_DIRFILE": str(tmp_path / "dirfile"),
           "SLURM_JOB_ID": "9"}
    p = subprocess.run([sys.executable, "-"], input=payloads.STAGEIN, env=env, capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    assert (tmp_path / "dirfile").read_text() == str(dest)
    return dest, p.stdout


def _tree(dest):
    return {str(f.relative_to(dest)): f.read_bytes() for f in dest.rglob("*")
            if f.is_file() and f.name not in (".complete", ".kaos-commit")}


def test_a_tree_without_data_yaml_images_or_labels_is_downloaded_whole(tmp_path, lakefs):
    fake = lakefs(NOT_YOLO)
    dest, _ = _stage_in(tmp_path, fake)
    assert _tree(dest) == NOT_YOLO
    assert (dest / ".complete").read_text().strip() == "full" and (dest / ".kaos-commit").read_text() == COMMIT


def test_an_old_partial_copy_is_completed_not_served(tmp_path, lakefs):
    """The copy an older stage-in left: the YOLO folders and an empty .complete marker."""
    dest = tmp_path / "data-cache" / "ds" / COMMIT
    for rel, body in YOLO.items():
        (dest / rel).parent.mkdir(parents=True, exist_ok=True)
        (dest / rel).write_bytes(body)
    (dest / ".complete").write_text("")
    fake = lakefs(dict(YOLO, **REST))
    dest, out = _stage_in(tmp_path, fake)
    assert "cache hit" not in out, "a copy of the YOLO folders only is not the dataset"
    assert _tree(dest) == dict(YOLO, **REST)
    assert sorted(fake.downloads) == sorted(REST), "what the old copy holds is not downloaded again"
    assert (dest / ".complete").read_text().strip() == "full"


def test_a_whole_copy_is_a_cache_hit_and_downloads_nothing(tmp_path, lakefs):
    fake = lakefs(NOT_YOLO)
    _stage_in(tmp_path, fake)
    fake.downloads.clear()
    dest, out = _stage_in(tmp_path, fake)
    assert "cache hit" in out and fake.downloads == []
    assert (dest / ".complete").read_text().strip() == "full", "marking the last use keeps the marker's word"
