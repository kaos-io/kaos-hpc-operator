"""kaos-gather-<step>: what the items of a matrix step handed back, joined for the steps that need it."""

import json
from pathlib import Path

import pytest

from kubecore import gather
from kubecore.step_contract import MATRIX_ENTRY_CAP, SENTINEL, wire

FIXTURES = Path(__file__).resolve().parents[2] / "docs" / "step-contract" / "fixtures"
EXPECTED = json.loads((FIXTURES / "result-matrix.json").read_text())
MODEL = json.loads((FIXTURES / "result-model.json").read_text())
PLAN = [{"index": e["index"], "of": 4, "text": wire({"index": e["index"], "of": 4, "values": e["values"]})}
        for e in EXPECTED["items"]]


def came_back(order, results):
    """What the workflow system hands over: results and the item each came from, in the order the items finished."""
    return [json.dumps(results[i]) for i in order], [str(i) for i in order]


RESULTS = {0: MODEL, 1: SENTINEL, 2: {"version": 1, "status": "skipped", "reason": "the loss did not move in the first epoch"},
           3: MODEL}


def test_the_items_are_put_in_index_order_with_each_status_as_the_fixture_shows():
    results, items = came_back([3, 1, 0, 2], RESULTS)
    document = gather.gather("train", "Succeeded", results, items, PLAN, allow_failed=True)
    assert document == EXPECTED
    assert wire(document) == (FIXTURES / "result-matrix.wire").read_text()


def test_a_result_the_workflow_system_gives_back_as_an_object_with_sorted_names_is_put_in_contract_order():
    sorted_model = dict(sorted(MODEL.items()))
    results, items = came_back([0], {0: sorted_model})
    document = gather.gather("train", "Succeeded", [json.loads(r) for r in results], items, PLAN[:1])
    assert list(document["items"][0]["result"]) == ["version", "status", "values", "reads", "writes"]


def test_an_item_that_never_came_back_is_failed():
    results, items = came_back([0], RESULTS)
    document = gather.gather("train", "Succeeded", results, items, PLAN, allow_failed=True)
    assert [i["status"] for i in document["items"]] == ["ok", "failed", "failed", "failed"]


def test_no_item_succeeded_fails_the_task_even_when_failed_items_are_allowed():
    results, items = came_back([0, 1], {0: SENTINEL, 1: SENTINEL})
    with pytest.raises(gather.GatherError, match="no item of step 'train' succeeded .4 failed, 0 skipped.*items' logs"):
        gather.gather("train", "Succeeded", results, items, PLAN, allow_failed=True)


def test_by_default_one_failed_item_fails_the_step_and_the_message_names_it_and_its_values():
    results, items = came_back([3, 1, 0, 2], RESULTS)
    with pytest.raises(gather.GatherError) as exc:
        gather.gather("train", "Succeeded", results, items, PLAN)
    message = str(exc.value)
    assert message.startswith("1 of the 4 items of step 'train' failed: index 1 (lr=0.01, batch=32).")
    assert "nothing after it runs and nothing is promoted" in message and "matrix_allow_failed=True on step 'train'" in message


def test_a_skipped_item_is_not_a_failure():
    results, items = came_back([0, 2], {0: MODEL, 2: RESULTS[2]})
    plan = [PLAN[0], PLAN[2]]
    document = gather.gather("train", "Succeeded", results, items, plan)
    assert [i["status"] for i in document["items"]] == ["ok", "skipped"]


def test_allowed_failed_items_are_said_in_the_log(capsys):
    results, items = came_back([0, 1, 2, 3], RESULTS)
    said = []
    gather.gather("train", "Succeeded", results, items, PLAN, allow_failed=True, say=said.append)
    assert said == ["kaos-gather: 1 of the 4 items of step 'train' failed (index 1 (lr=0.01, batch=32)); "
                    "matrix_allow_failed=True lets the step after it run with the others."]


def test_the_values_arrive_with_the_number_and_the_text_they_were_written_with():
    values = {"lr": 1.0, "n": 9007199254740992, "tag": "a b"}
    plan = [{"index": 0, "of": 1, "text": wire({"index": 0, "of": 1, "values": values})}]
    document = gather.gather("t", "Succeeded", [json.dumps(MODEL)], ["0"], plan)
    assert wire(document["items"][0]["values"]) == '{"lr":1.0,"n":9007199254740992,"tag":"a b"}'
    assert isinstance(document["items"][0]["values"]["lr"], float)


def test_a_matrix_step_that_was_skipped_is_the_sentinel():
    for status in ("Skipped", "Omitted"):
        assert gather.gather("train", status, [], [], PLAN) == SENTINEL


def test_the_items_share_the_entry_cap():
    big = dict(MODEL, values={"blob": "x" * 8000})
    both = {0: big, 1: big, 2: big, 3: big, 4: big, 5: big, 6: big, 7: big}
    plan = [{"index": i, "of": 8, "text": wire({"index": i, "of": 8, "values": {"a": i}})} for i in range(8)]
    results, items = [json.dumps(both[i]) for i in range(8)], [str(i) for i in range(8)]
    with pytest.raises(gather.GatherError, match=f"more than the {MATRIX_ENTRY_CAP} they share"):
        gather.gather("train", "Succeeded", results, items, plan)


def test_main_writes_the_entry_as_the_tasks_result(tmp_path, monkeypatch, capsys):
    results, items = came_back([1, 0, 3, 2], RESULTS)
    target = tmp_path / "out" / "kaos-result.json"
    monkeypatch.setattr(gather, "RESULT_FILE", str(target))
    for name, value in (("KAOS_GATHER_STEP", "train"), ("KAOS_GATHER_STATUS", "Succeeded"),
                        ("KAOS_GATHER_ALLOW_FAILED", "true"),
                        ("KAOS_GATHER_RESULTS", json.dumps(results)), ("KAOS_GATHER_ITEMS", json.dumps(items)),
                        ("KAOS_GATHER_PLAN", json.dumps(PLAN))):
        monkeypatch.setenv(name, value)
    assert gather.main() == 0
    assert target.read_text() == (FIXTURES / "result-matrix.wire").read_text()
    monkeypatch.setenv("KAOS_GATHER_RESULTS", json.dumps(came_back([0], {0: SENTINEL})[0]))
    monkeypatch.setenv("KAOS_GATHER_ITEMS", json.dumps(came_back([0], {0: SENTINEL})[1]))
    assert gather.main() == 1 and "kaos-gather FAILED: no item of step 'train' succeeded" in capsys.readouterr().err
