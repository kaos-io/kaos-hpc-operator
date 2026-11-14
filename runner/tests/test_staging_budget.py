"""PRD kaos#1911 F-40 / AC-26, kaos#2212 part 3: the time staging may go on grows with the size of the copy; a token
is minted again for every attempt; a dataset too large for the job to download is never left to the job; a copy that
is making progress says how far it is. A fake clock stands in for time."""
import base64
import io
import json
import urllib.request

import helpers
import pytest
from kaos_hpc_runner import staging
from test_staging import COMMIT, _LakeFS, _Stager
from test_staging import _twin as _base_twin

GB, TB = 10 ** 9, 10 ** 12
H = 3600


class Clock:
    def __init__(self, monkeypatch):
        self.now, self.slept, self.mp = 1_000_000.0, 0, monkeypatch
        CLOCKS.append(self)
        self.install()

    def install(self):
        self.mp.setattr(staging.time, "time", lambda: self.now)
        self.mp.setattr(staging.time, "sleep", self.sleep)

    def sleep(self, s):
        self.now += s
        self.slept += s


def _twin(monkeypatch, lakefs, stager, **env):
    """test_staging's twin, which stubs sleep out: the fake clock is put back after it."""
    cfg = _base_twin(monkeypatch, lakefs, stager, **env)
    CLOCKS[-1].install()
    return cfg


CLOCKS = []


def _jwt(exp):
    seg = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return "h.%s.s" % seg


def _big(monkeypatch, size, rcs, **kw):
    stager = _Stager({"ready": False, "base": None}, stage_rcs=rcs)
    cfg = _twin(monkeypatch, _LakeFS({"data.bin": size}), stager, **kw)
    return cfg, stager


# ---------------------------------------------------------------- the budget, one function

@pytest.mark.parametrize("nbytes, hours", [
    (None, 12),                  # the size could not be read
    (0, 4), (1 * GB, 4), (100 * GB, 4),
    (720 * GB, 4),               # 2 x 720 GB / 50 MB/s = exactly 8 h ... see the next row
    (2.5 * TB, 2 * 2.5 * TB / 50e6 / H),
    (10 * TB, 72),               # capped
])
def test_the_budget_is_at_least_four_hours_else_twice_the_time_at_the_floor_rate_capped_at_72(nbytes, hours):
    want = 8 if nbytes == 720 * GB else hours
    assert staging.stage_budget(nbytes) == pytest.approx(want * H, abs=1)


def test_the_clients_2_5_tb_dataset_may_be_staged_for_about_28_hours():
    assert 27 * H < staging.stage_budget(2.5 * TB) < 29 * H


# ---------------------------------------------------------------- the retries follow the budget

def test_a_large_dataset_is_asked_again_for_longer_than_four_hours_and_then_refused_not_downloaded_in_the_job(
        monkeypatch, capsys):
    clock = Clock(monkeypatch)
    cfg, stager = _big(monkeypatch, int(2.5 * TB), [75] * 500)
    with pytest.raises(staging.DatasetNotStaged) as e:
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key)
    assert 27 * H < clock.slept < 29 * H, "kept asking for the budget of 2.5 TB (~27.8 h), not the old 4 h"
    msg = str(e.value)
    assert "dataset-poulaki" in msg and "2.5 TB" in msg and "could not stage it in time" in msg
    assert "GPU time" in msg and "Nothing was submitted" in msg
    out = capsys.readouterr().out
    assert out.count("staging may go on for") == 1, "the size and the budget are said once"
    assert "is 2.5 TB; staging may go on for 27.8 hours" in out


def test_a_small_dataset_keeps_the_four_hours_and_falls_back_to_the_job_as_before(monkeypatch, capsys):
    clock = Clock(monkeypatch)
    cfg, stager = _big(monkeypatch, 10 * GB, [75] * 500)
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key) == ""
    assert 4 * H <= clock.slept <= 4 * H + 600
    assert "NOT STAGED - the job will download" in capsys.readouterr().out


def test_a_dataset_just_above_the_fallback_size_is_refused_and_one_just_below_is_not(monkeypatch):
    for size, refused in ((51 * GB, True), (49 * GB, False)):
        Clock(monkeypatch)
        cfg, stager = _big(monkeypatch, size, [75] * 500)
        if refused:
            with pytest.raises(staging.DatasetNotStaged):
                staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key)
        else:
            assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key) == ""


def test_an_unknown_size_gets_twelve_hours_and_then_the_job_stages_as_before(monkeypatch, capsys):
    clock = Clock(monkeypatch)
    stager = _Stager({"ready": False, "base": None}, stage_rcs=[75] * 500)

    def no_listing(req, timeout=0):
        raise OSError("data store unreachable")
    cfg = _twin(monkeypatch, no_listing, stager)
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key) == ""
    assert 12 * H <= clock.slept <= 12 * H + 600
    out = capsys.readouterr().out
    assert out.count("could not be read; staging may go on for 12.0 hours") == 1


def test_a_dataset_that_cannot_be_staged_because_there_is_no_staging_key_is_refused_when_large(monkeypatch):
    Clock(monkeypatch)
    stager = _Stager({"ready": False}, key=False)
    cfg = _twin(monkeypatch, _LakeFS({"a.bin": 200 * GB}), stager)
    with pytest.raises(staging.DatasetNotStaged) as e:
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key)
    assert "200.0 GB" in str(e.value) and "staging tool cannot be used" in str(e.value)
    cfg = _twin(monkeypatch, _LakeFS({"a.bin": 5 * GB}), stager)
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key) == ""


def test_the_size_of_a_delta_never_hides_a_large_dataset_from_the_refusal(monkeypatch):
    """The copy is a small delta, but the job's own download would be the whole commit."""
    Clock(monkeypatch)
    lakefs = _LakeFS({"big.bin": 300 * GB}, diff=[{"type": "added", "path": "x", "size_bytes": 5}])
    stager = _Stager({"ready": False, "base": "c1" * 32, "base_files": 1, "base_prefix": ""}, stage_rcs=[75] * 500)
    cfg = _twin(monkeypatch, lakefs, stager)
    with pytest.raises(staging.DatasetNotStaged) as e:
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key)
    assert "300.0 GB" in str(e.value)


def test_a_shared_dataset_is_not_affected_by_the_fallback_size_rule():
    assert not issubclass(staging.DatasetNotStaged, staging.SharedDatasetUnavailable)


# ---------------------------------------------------------------- the token

def test_every_attempt_asks_for_the_apps_key_again_and_sends_that_one(monkeypatch):
    Clock(monkeypatch)
    cfg, stager = _big(monkeypatch, 1 * GB, [75, 75, 0])
    keys = helpers.FakeKeys()
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, keys).endswith(COMMIT)
    auths = [r[0]["authorization"] for w, r in stager.calls if w[0] == "stage"]
    assert len(auths) == 3 and len(set(auths)) == 3, "never the key of an earlier attempt"
    assert auths[-1] == helpers.FakeKeys.auth_of(keys.calls)


def test_a_failed_mint_keeps_the_old_token_only_while_it_has_an_hour_left(monkeypatch):
    clock = Clock(monkeypatch)
    now = clock.now
    assert staging.renewed_wallet(lambda: "new", "old") == "new"
    assert staging.renewed_wallet(lambda: "", _jwt(now + 5 * H)) == _jwt(now + 5 * H)
    assert staging.renewed_wallet(lambda: "", _jwt(now + 30 * 60)) == "", "close to its end: not reused"
    assert staging.renewed_wallet(lambda: "", "not-a-jwt") == "not-a-jwt", "an end that is unknown is not a reason"


def test_an_attempt_whose_key_cannot_be_obtained_is_a_failed_attempt_that_is_asked_again(monkeypatch, capsys):
    Clock(monkeypatch)
    cfg, stager = _big(monkeypatch, 1 * GB, [75, 0])
    keys = helpers.FakeKeys(fail=[None, None, OSError("blip"), OSError("blip")])
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, keys).endswith(COMMIT)
    assert capsys.readouterr().out.count("failed: blip") == 2


# ---------------------------------------------------------------- progress

class _Progress(_Stager):
    """A stager whose stage call reports progress events at the given clock times, as the real one does every 5 s."""

    def __init__(self, clock, events, **kw):
        super().__init__({"ready": False, "base": None}, **kw)
        self.clock, self.events = clock, events

    def __call__(self, words, feed=None, quiet=False):
        if words[0] == "stage":
            for t, files, nbytes in self.events:
                self.clock.now = 1_000_000.0 + t
                self.progress({"event": "progress", "phase": "fetch", "files": files, "bytes": nbytes, "skipped": 0})
            self.progress({"event": "progress", "phase": "link", "files": 1})
        return super().__call__(words, feed, quiet)


def test_a_copy_that_makes_progress_says_how_far_it_is_every_ten_minutes_and_clears_it_at_the_end(monkeypatch, capsys):
    clock = Clock(monkeypatch)
    stager = _Progress(clock, [(0, 1, 5 * GB), (300, 2, 10 * GB), (600, 3, 20 * GB), (900, 4, 30 * GB)], stage_rcs=[0])
    cfg = _twin(monkeypatch, _LakeFS({"a": 500 * GB, "b": 500 * GB, "c": 1, "d": 2}), stager)
    notes = []
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key, note=notes.append).endswith(COMMIT)
    assert len(notes) == 3 and notes[-1] is None, "the first line at once, the next after ten minutes, then cleared"
    assert notes[0].startswith("staging dataset dataset-poulaki: 1 of 4 files, 5.0 GB of 1.0 TB fetched")
    assert "staging since 1970-01-12 13:46 UTC" in notes[0]
    assert "3 of 4 files, 20.0 GB" in notes[1]
    assert stager.progress is None
    assert capsys.readouterr().out.count("stage: staging dataset") == 2


def test_no_note_is_made_when_nothing_was_copied(monkeypatch):
    Clock(monkeypatch)
    stager = _Stager({"ready": True, "path": "/p"})
    cfg = _twin(monkeypatch, _LakeFS({"a": 1}), stager)
    notes = []
    staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key, note=notes.append)
    assert notes == []


# ---------------------------------------------------------------- 72 hours in total for all inputs of a step

def test_two_large_datasets_share_one_72_hour_clock_and_the_second_gets_what_is_left(monkeypatch):
    clock = Clock(monkeypatch)
    start = clock.now
    deadline = start + staging.STAGE_MAX_BUDGET_S
    cfg, stager = _big(monkeypatch, 5 * TB, [75] * 2000)     # 5 TB: a budget of 55.6 h of its own
    with pytest.raises(staging.DatasetNotStaged):
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key, deadline=deadline)
    spent = clock.now - start
    assert 55 * H < spent < 56 * H + 600, "the first dataset used its own size-based budget"
    with pytest.raises(staging.DatasetNotStaged):
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key, deadline=deadline)
    assert clock.now - start <= 72 * H + 600, "the second stopped at the clock of the whole step, not 55.6 h later"
    assert clock.now - start > 71 * H, "and it used what was left"


def test_a_dataset_with_a_small_budget_is_not_stretched_by_the_step_clock(monkeypatch):
    clock = Clock(monkeypatch)
    cfg, stager = _big(monkeypatch, 10 * GB, [75] * 500)
    assert staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key,
                                 deadline=clock.now + 72 * H) == ""
    assert clock.slept <= 4 * H + 600


def test_a_refusal_raised_mid_copy_clears_the_progress_note(monkeypatch):
    Clock(monkeypatch)
    stager = _Stager({"ready": False, "base": None})
    cfg = _twin(monkeypatch, _LakeFS({"a": 1}), stager)

    def refused_mid_copy(stg, repo, ref, commit, source, wallet, info, key, plan):
        plan.update(files=4, bytes=10 * GB)
        stg.progress({"event": "progress", "phase": "fetch", "files": 1, "bytes": GB, "skipped": 0})
        raise staging.InsufficientQuota("no room")
    monkeypatch.setattr(staging, "stage_once", refused_mid_copy)
    notes = []
    with pytest.raises(staging.InsufficientQuota):
        staging.stage_dataset(cfg, cfg.inputs[0], stager, helpers.app_key, note=notes.append)
    assert len(notes) == 2 and notes[0].startswith("staging dataset") and notes[1] is None
    assert stager.progress is None


def test_the_three_72_hour_allowances_are_one_number():
    """The runner's budget, the runner Job's deadline (Go) and the follower's deadline (engine) are changed together."""
    import pathlib
    import re
    root = pathlib.Path(__file__).resolve().parents[2]
    engine = re.search(r"^STAGING_ALLOWANCE_MINUTES\s*=\s*(\d+)\s*\*\s*(\d+)\s*$",
                       (root / "engine/kubecore/meluxina.py").read_text(), re.M)
    runnerjob = re.search(r"^\s*stagingAllowanceMinutes\s*=\s*(\d+)\s*\*\s*(\d+)\s*$",
                          (root / "internal/runnerjob/runnerjob.go").read_text(), re.M)
    assert engine and runnerjob
    want = staging.STAGE_MAX_BUDGET_S // 60
    assert int(engine[1]) * int(engine[2]) == want
    assert int(runnerjob[1]) * int(runnerjob[2]) == want
