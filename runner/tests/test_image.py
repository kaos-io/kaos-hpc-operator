"""Image pre-staging: a MeluXina job that queues longer than the registry token's hour must not fail its image
pull (live 2026-11-05, exit 231). Covered: the stdlib registry client against a real local HTTP registry (token
flow, manifest list -> linux/amd64, blob redirect without the registry's Authorization), the streamed tar and
its trailer, the stager exchange (skip when staged, space refusal, fallbacks that never block the submit).
"""
import base64
import hashlib
import http.server
import io
import json
import os
import tarfile
import threading

import helpers
from kaos_hpc_runner import image, staging



GZ = "application/vnd.oci.image.layer.v1.tar+gzip"


def sha(b):
    return "sha256:" + hashlib.sha256(b).hexdigest()


class FakeRegistry:
    """A registry on 127.0.0.1: bearer token flow (Basic oauth2accesstoken:<tok> -> token),
    an index with an attestation, an arm64 and an amd64 image, blobs served by redirect."""

    def __init__(self, token_required=True):
        self.layers = [os.urandom(1500), os.urandom(700_000) + b"x", b"\x1f\x8b" + os.urandom(37)]
        self.config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        self.manifest = json.dumps({
            "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": sha(self.config), "size": len(self.config)},
            "layers": [{"mediaType": GZ, "digest": sha(b), "size": len(b)} for b in self.layers]}).encode()
        arm = json.dumps({"schemaVersion": 2, "config": {"digest": sha(b"arm"), "size": 3},
                          "layers": [{"digest": sha(b"arm"), "size": 3}]}).encode()
        self.arm = arm
        self.index = json.dumps({"schemaVersion": 2, "manifests": [
            {"digest": sha(b"att"), "platform": {"os": "unknown", "architecture": "unknown"}},
            {"digest": sha(arm), "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": sha(self.manifest), "platform": {"os": "linux", "architecture": "amd64"}}]}).encode()
        self.blobs = {sha(b): b for b in self.layers + [self.config]}
        self.manifests = {sha(self.manifest): self.manifest, sha(arm): arm, "v1": self.index,
                          sha(self.index): self.index}
        self.token_required, self.log, self.leaked = token_required, [], []
        reg = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body=b"", headers=None):
                self.send_response(code)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                reg.log.append((self.path, self.headers.get("Authorization")))
                if self.path.startswith("/token"):
                    want = "Basic " + base64.b64encode(b"oauth2accesstoken:gcp-tok").decode()
                    if self.headers.get("Authorization") != want:
                        return self._send(401)
                    return self._send(200, json.dumps({"token": "reg-bearer"}).encode())
                if self.path.startswith("/store/"):      # blob store behind a redirect
                    if self.headers.get("Authorization"):
                        reg.leaked.append(self.path)
                    return self._send(200, reg.blobs["sha256:" + self.path.split("/")[-1]])
                if reg.token_required and self.headers.get("Authorization") != "Bearer reg-bearer":
                    return self._send(401, headers={"WWW-Authenticate": 'Bearer realm="http://%s/token",'
                                                    'service="reg",scope="repository:p/app:pull"' % reg.host})
                kind, _, ref = self.path.split("/v2/p/app/")[1].partition("/")
                if kind == "manifests" and ref in reg.manifests:
                    return self._send(200, reg.manifests[ref])
                if kind == "blobs" and ref in reg.blobs:
                    return self._send(307, headers={"Location": "http://localhost:%d/store/%s" % (reg.httpd.server_port, ref.split(":")[1])})
                self._send(404)
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.host = "127.0.0.1:%d" % self.httpd.server_port
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _read_tar(chunks):
    data = b"".join(chunks)
    assert data[-65:-1].decode() == hashlib.sha256(data[:-65]).hexdigest(), "trailer = sha256 of the tar"
    return data[:-65], tarfile.open(fileobj=io.BytesIO(data[:-65]))


def _token_host(monkeypatch):
    # The test registry stands in for a GAR host: it gets the GCP token.
    monkeypatch.setattr(image, "registry_token_host", lambda host: True)



def test_streams_a_multi_layer_image_from_the_amd64_manifest_with_the_token(monkeypatch):
    reg = FakeRegistry()
    try:
        _token_host(monkeypatch)
        registry = image.Registry(reg.host, "gcp-tok")
        host, repo, ref = image.parse_image("%s/p/app@%s" % (reg.host, sha(reg.manifest)))
        assert (host, repo, ref) == (reg.host, "p/app", sha(reg.manifest))
        # a tag resolves through the index to the linux/amd64 image, not arm64 or the attestation
        m = image.image_manifest(registry, "p/app", "v1")
        assert [d["digest"] for d in m["layers"]] == [sha(b) for b in reg.layers]
        entries, total = image.image_plan(registry, "p/app", m)
        raw, tar = _read_tar(image.image_feed(registry, "p/app", entries, 1e18))
        assert len(raw) + 65 == total + 65 and total == len(raw), "declared size = streamed size"
        names = tar.getnames()
        assert names[0] == "manifest.json" and len(names) == 5
        manifest = json.load(tar.extractfile("manifest.json"))[0]
        assert manifest["Config"] == sha(reg.config)[7:] + ".json"
        for name, blob in zip(manifest["Layers"], reg.layers):
            assert name == sha(blob)[7:] + ".tar.gz"
            assert tar.extractfile(name).read() == blob, "layers are copied as they are"
        # auth: bearer from the token endpoint with Basic oauth2accesstoken:<gcp token>
        assert any(p.startswith("/token") for p, _ in reg.log)
        assert all(a == "Bearer reg-bearer" for p, a in reg.log if "/v2/" in p and a)
        assert reg.leaked == [], "the registry's Authorization never reaches the blob store"
    finally:
        reg.close()


def test_a_corrupt_layer_is_not_completed(monkeypatch):
    reg = FakeRegistry()
    try:
        _token_host(monkeypatch)
        registry = image.Registry(reg.host, "gcp-tok")
        m = image.image_manifest(registry, "p/app", sha(reg.manifest))
        entries, _ = image.image_plan(registry, "p/app", m)
        reg.blobs[sha(reg.layers[1])] = b"y" * len(reg.layers[1])        # bytes no longer match the digest
        sent = []
        try:
            for c in image.image_feed(registry, "p/app", entries, 1e18):
                sent.append(c)
        except ValueError as e:
            assert "mismatch" in str(e)
        else:
            raise AssertionError("a layer that does not match its digest must abort the stream")
        assert not sent or len(sent[-1]) != 65, "no trailer after a bad layer"
    finally:
        reg.close()


def test_the_anonymous_registry_gets_no_token():
    reg = FakeRegistry(token_required=False)
    try:
        registry = image.Registry(reg.host, "gcp-tok")        # not a GAR host: token withheld
        assert image.image_manifest(registry, "p/app", sha(reg.manifest))["layers"]
        assert all(a is None for _, a in reg.log)
    finally:
        reg.close()


# ---------------------------------------------------------------- the stager exchange

class FakeStager:
    def __init__(self, status=None, image_rc=0, status_rc=0, key=True):
        self.status, self.image_rc, self.status_rc, self.calls, self.received = status, image_rc, status_rc, [], b""
        self.key = key

    def have_key(self):
        return self.key

    def __call__(self, words, feed=None, quiet=False):
        self.calls.append(words)
        if words[0] == "image-status":
            ev = [dict(self.status, event="image_status")] if self.status is not None else []
            return self.status_rc, ev
        self.received = b"".join(feed)
        return self.image_rc, ([{"event": "done", "path": "/scr/kaos/p/images/k.tar"}] if self.image_rc == 0 else [])


def _stage(monkeypatch, stager, reg):
    _token_host(monkeypatch)
    monkeypatch.setattr(image, "ssh_present", lambda: True)
    return "%s/p/app@%s" % (reg.host, sha(reg.manifest))



def test_stages_the_tar_under_the_jobs_cache_key(monkeypatch):
    reg = FakeRegistry()
    try:
        stager = FakeStager(status={"ready": False})
        img = _stage(monkeypatch, stager, reg)
        assert image.stage_image(img, "gcp-tok", stager) == "/scr/kaos/p/images/k.tar"
        key = hashlib.sha256(img.encode()).hexdigest()[:16]           # the job's: sha256sum | cut -c1-16
        assert stager.calls[0] == ["image-status", "--key", key]
        words = stager.calls[1]
        assert words[:3] == ["image", "--key", key] and words[3] == "--size"
        assert words[4] == len(stager.received) - 65
        assert tarfile.open(fileobj=io.BytesIO(stager.received[:-65])).getnames()[0] == "manifest.json"
    finally:
        reg.close()


def test_an_image_already_staged_is_not_fetched_again(monkeypatch):
    reg = FakeRegistry()
    try:
        stager = FakeStager(status={"ready": True, "path": "/scr/kaos/p/images/k.tar"})
        img = _stage(monkeypatch, stager, reg)
        assert image.stage_image(img, "gcp-tok", stager) == "/scr/kaos/p/images/k.tar"
        assert [w[0] for w in stager.calls] == ["image-status"] and reg.log == []
    finally:
        reg.close()


def test_without_the_key_an_old_stager_or_no_space_the_job_pulls(monkeypatch):
    reg = FakeRegistry()
    try:
        s = FakeStager(status={"ready": False}, key=False)
        img = _stage(monkeypatch, s, reg)
        assert image.stage_image(img, "gcp-tok", s) == "" and reg.log == []
        old = FakeStager(status=None, status_rc=2)                     # unknown subcommand: no image event
        assert image.stage_image(img, "gcp-tok", old) == "" and reg.log == []
        full = FakeStager(status={"ready": False}, image_rc=76)         # NoSpace
        assert image.stage_image(img, "gcp-tok", full) == ""
        assert [w[0] for w in full.calls] == ["image-status", "image"], "no pointless retry on no space"
    finally:
        reg.close()


def test_a_registry_failure_never_raises_and_a_dropped_stream_retries_once(monkeypatch):
    reg = FakeRegistry()
    try:
        stager = FakeStager(status={"ready": False}, image_rc=75)
        img = _stage(monkeypatch, stager, reg)
        assert image.stage_image(img, "gcp-tok", stager) == ""
        assert [w[0] for w in stager.calls] == ["image-status", "image", "image"]
        bad = "%s/p/missing@%s" % (reg.host, sha(reg.manifest))
        assert image.stage_image(bad, "gcp-tok", stager) == "", "a 404 from the registry is a fallback, not an error"
        assert image.stage_image("docker-hub-name", "t", stager) == ""
    finally:
        reg.close()


def test_the_lease_key_is_the_job_scripts_cache_key():
    key = hashlib.sha256(b"r/x:t").hexdigest()[:16]
    assert image.image_key("r/x:t") == key and len(key) == 16


def test_the_stager_call_passes_tar_bytes_through_ssh_unchanged(monkeypatch, tmp_path):
    out = tmp_path / "received"
    ssh = tmp_path / "bin" / "ssh"
    ssh.parent.mkdir()
    ssh.write_text("#!/bin/sh\ncat > %s\necho '{\"event\": \"done\", \"path\": \"/p\"}'\n" % out)
    ssh.chmod(0o755)
    monkeypatch.setenv("PATH", str(ssh.parent) + os.pathsep + os.environ["PATH"])
    stager = staging.Stager(helpers.settings(), staging_dir=str(tmp_path))
    payload = bytes(range(256)) * 5000
    rc, ev = stager(["image", "--key", "k", "--size", len(payload)],
                    iter([payload[:1000], payload[1000:], b"%s\n" % b"ab"]))
    assert rc == 0 and ev[-1]["event"] == "done"
    assert out.read_bytes() == payload + b"ab\n"
