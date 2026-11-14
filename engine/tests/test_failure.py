"""A platform task's failure line is kept as the container's last message, so the run shows it after the pod's log
is gone (PRD kaos#1911, AC-24)."""
from kubecore import failure, promote


def test_the_failure_line_is_printed_and_kept(tmp_path, monkeypatch, capsys):
    kept = tmp_path / "termination-log"
    monkeypatch.setenv(failure.MESSAGE_FILE_ENV, str(kept))
    failure.failed("kaos-promote", "another run promoted the dataset first")
    line = "kaos-promote FAILED: another run promoted the dataset first"
    assert line in capsys.readouterr().err and kept.read_text() == line


def test_a_long_line_is_cut_to_what_kubernetes_keeps(tmp_path, monkeypatch):
    kept = tmp_path / "termination-log"
    monkeypatch.setenv(failure.MESSAGE_FILE_ENV, str(kept))
    failure.failed("compose-and-validate", "é" * 5000)
    assert len(kept.read_bytes()) <= failure.MESSAGE_MAX and kept.read_text().startswith("compose-and-validate FAILED: é")


def test_no_place_to_keep_it_is_not_an_error(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(failure.MESSAGE_FILE_ENV, str(tmp_path / "missing" / "termination-log"))
    failure.failed("kaos-gather", "no item succeeded")
    assert "kaos-gather FAILED: no item succeeded" in capsys.readouterr().err


def test_a_refused_promotion_keeps_its_reason(tmp_path, monkeypatch, capsys):
    kept = tmp_path / "termination-log"
    monkeypatch.setenv(failure.MESSAGE_FILE_ENV, str(kept))
    monkeypatch.setenv(promote.PLAN_ENV, "not json")
    monkeypatch.setenv("KAOS_RUN", "run-1")
    assert promote.main() == 1
    assert kept.read_text().startswith("kaos-promote FAILED: KAOS_PROMOTE_PLAN is not JSON")
