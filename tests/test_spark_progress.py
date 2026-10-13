from __future__ import annotations

from typing import Any

import pytest

from streamcase import CapturedBatch
from streamcase._spark_progress import _capture_action_progress, _report_key


def _report(run_id: str, batch_id: int, timestamp: str) -> dict[str, object]:
    return {"runId": run_id, "batchId": batch_id, "timestamp": timestamp}


def test_capture_only_matches_new_callback_backed_reports() -> None:
    old = _report("run-1", 7, "earlier")
    fresh = _report("run-1", 8, "now")
    other_run = _report("run-2", 9, "now")
    no_callback = _report("run-1", 10, "now")

    captured = _capture_action_progress(
        3,
        [CapturedBatch(7, []), CapturedBatch(8, []), CapturedBatch(9, [])],
        [old],
        [old, fresh, other_run, no_callback],
        "run-1",
    )

    assert len(captured) == 1
    assert (captured[0].action_index, captured[0].trigger_index, captured[0].batch_id) == (
        3,
        1,
        8,
    )
    assert captured[0].run_id == "run-1"
    assert captured[0].num_input_rows is None
    fresh["batchId"] = 999
    assert captured[0].batch_id == 8


def test_missing_report_leaves_progress_empty() -> None:
    assert _capture_action_progress(0, [CapturedBatch(0, [])], [], [], "run-1") == ()


def test_new_run_can_reuse_batch_id_without_old_run_match() -> None:
    captured = _capture_action_progress(
        2,
        [CapturedBatch(0, [])],
        [_report("run-1", 0, "old")],
        [_report("run-1", 0, "old"), _report("run-2", 0, "new")],
        "run-2",
    )
    assert [(item.action_index, item.batch_id, item.run_id) for item in captured] == [
        (2, 0, "run-2")
    ]


def test_repeated_callback_batch_id_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="repeated callback batch ID 0"):
        _capture_action_progress(
            0,
            [CapturedBatch(0, []), CapturedBatch(0, [])],
            [],
            [_report("run-1", 0, "now")],
            "run-1",
        )


def test_multiple_fresh_reports_for_one_callback_are_rejected() -> None:
    with pytest.raises(RuntimeError, match=r"multiple progress reports.*batch ID 0"):
        _capture_action_progress(
            1,
            [CapturedBatch(0, [])],
            [],
            [_report("run-1", 0, "first"), _report("run-1", 0, "second")],
            "run-1",
        )


def test_unique_positive_input_report_disambiguates_no_data_update() -> None:
    data = _report("run-1", 0, "data")
    data["numInputRows"] = 2
    idle = _report("run-1", 0, "idle")
    idle["numInputRows"] = 0

    captured = _capture_action_progress(0, [CapturedBatch(0, [])], [], [data, idle], "run-1")

    assert [(item.action_index, item.batch_id) for item in captured] == [(0, 0)]
    assert captured[0].num_input_rows == 2


@pytest.mark.parametrize("counts", [(1, 2), (True, 0)])
def test_non_unique_or_invalid_input_counts_remain_ambiguous(counts: tuple[object, object]) -> None:
    first = _report("run-1", 0, "first")
    first["numInputRows"] = counts[0]
    second = _report("run-1", 0, "second")
    second["numInputRows"] = counts[1]

    with pytest.raises(RuntimeError, match=r"multiple progress reports.*batch ID 0"):
        _capture_action_progress(0, [CapturedBatch(0, [])], [], [first, second], "run-1")


@pytest.mark.parametrize("invalid", [None, "", "  "])
def test_report_key_rejects_missing_run_id(invalid: object) -> None:
    report = _report("run-1", 0, "now")
    report["runId"] = invalid
    with pytest.raises(ValueError, match="runId must be present"):
        _report_key(report)


@pytest.mark.parametrize("invalid", [True, "0"])
def test_report_key_rejects_invalid_batch_id(invalid: object) -> None:
    report = _report("run-1", 0, "now")
    report["batchId"] = invalid
    with pytest.raises(TypeError, match="batchId must be an int"):
        _report_key(report)


@pytest.mark.parametrize("invalid", [None, "", 1])
def test_report_key_rejects_invalid_timestamp(invalid: Any) -> None:
    report = _report("run-1", 0, "now")
    report["timestamp"] = invalid
    with pytest.raises(TypeError, match="timestamp must be a non-empty str"):
        _report_key(report)
