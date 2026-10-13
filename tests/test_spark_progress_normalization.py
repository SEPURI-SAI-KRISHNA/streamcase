from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streamcase import CapturedBatch
from streamcase._spark_progress import _capture_action_progress
from streamcase.progress import CapturedProgress


def _capture(report: dict[str, object]) -> CapturedProgress:
    complete_report = {"runId": "run-1", "batchId": 7, "timestamp": "now", **report}
    return _capture_action_progress(2, [CapturedBatch(7, [])], [], [complete_report], "run-1")[0]


def test_spark_progress_metrics_are_copied_into_immutable_models() -> None:
    first_operator: dict[str, object] = {
        "operatorName": "stateStoreSave",
        "numRowsTotal": 0,
        "numRowsUpdated": 2,
        "numRowsRemoved": 1,
        "numRowsDroppedByWatermark": 0,
        "memoryUsedBytes": 4096,
        "customMetrics": {"lookup": 99},
    }
    second_operator: dict[str, object] = {"operatorName": "stateStoreRestore"}
    operators = [first_operator, second_operator]
    event_time = {"watermark": "2026-10-13T12:30:00.000Z", "max": "ignored"}
    report: dict[str, object] = {
        "numInputRows": 0,
        "eventTime": event_time,
        "stateOperators": operators,
        "sources": [{"numInputRows": 100}],
    }

    captured = _capture(report)

    assert captured.action_index == 2
    assert captured.trigger_index == 0
    assert captured.num_input_rows == 0
    assert captured.watermark == datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)
    assert captured.state_operators is not None
    assert len(captured.state_operators) == 2
    assert captured.state_operators[0].operator_name == "stateStoreSave"
    assert captured.state_operators[0].num_rows_total == 0
    assert captured.state_operators[0].num_rows_updated == 2
    assert captured.state_operators[0].num_rows_removed == 1
    assert captured.state_operators[0].num_rows_dropped_by_watermark == 0
    assert captured.state_operators[0].memory_used_bytes == 4096
    assert captured.state_operators[1].operator_name == "stateStoreRestore"
    assert captured.state_operators[1].num_rows_total is None
    event_time["watermark"] = "2000-01-01T00:00:00Z"
    first_operator["numRowsTotal"] = 999
    operators.clear()
    assert captured.watermark == datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)
    assert len(captured.state_operators) == 2
    assert captured.state_operators[0].num_rows_total == 0


@pytest.mark.parametrize(
    ("report", "expected_operators"),
    [
        ({}, None),
        ({"num_input_rows": 4, "state_operators": []}, None),
        ({"numInputRows": None, "eventTime": None, "stateOperators": None}, None),
        ({"eventTime": {}, "stateOperators": []}, ()),
        ({"eventTime": {"watermark": None}, "stateOperators": []}, ()),
    ],
)
def test_missing_fields_and_unrecognized_aliases_remain_missing(
    report: dict[str, object], expected_operators: object
) -> None:
    captured = _capture(report)
    assert captured.num_input_rows is None
    assert captured.watermark is None
    assert captured.state_operators == expected_operators


def test_offset_watermark_is_normalized_to_utc() -> None:
    captured = _capture({"eventTime": {"watermark": "2026-10-13T18:00:00+05:30"}})
    assert captured.watermark == datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("report", "error", "message"),
    [
        ({"numInputRows": True}, TypeError, "numInputRows must be an int"),
        ({"numInputRows": "1"}, TypeError, "numInputRows must be an int"),
        ({"numInputRows": -1}, ValueError, "numInputRows must be non-negative"),
        ({"eventTime": []}, TypeError, "eventTime must be a mapping"),
        ({"eventTime": {"watermark": 1}}, TypeError, "watermark must be an ISO8601 string"),
        ({"eventTime": {"watermark": "bad"}}, ValueError, "watermark must be ISO8601"),
        (
            {"eventTime": {"watermark": "2026-10-13T12:30:00"}},
            ValueError,
            "watermark must include a timezone",
        ),
        ({"stateOperators": {}}, TypeError, "stateOperators must be a sequence"),
        ({"stateOperators": [None]}, TypeError, r"stateOperators\[0\] must be a mapping"),
        (
            {"stateOperators": [{"operatorName": 1}]},
            TypeError,
            r"stateOperators\[0\].operatorName must be a str",
        ),
        (
            {"stateOperators": [{"numRowsTotal": -1}]},
            ValueError,
            "numRowsTotal must be non-negative",
        ),
        (
            {"stateOperators": [{"numRowsUpdated": True}]},
            TypeError,
            "numRowsUpdated must be an int",
        ),
        (
            {"stateOperators": [{"numRowsRemoved": "1"}]},
            TypeError,
            "numRowsRemoved must be an int",
        ),
        (
            {"stateOperators": [{"numRowsDroppedByWatermark": -1}]},
            ValueError,
            "numRowsDroppedByWatermark must be non-negative",
        ),
        (
            {"stateOperators": [{"memoryUsedBytes": False}]},
            TypeError,
            "memoryUsedBytes must be an int",
        ),
    ],
)
def test_malformed_spark_metrics_are_rejected(
    report: dict[str, object], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        _capture(report)
