from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from streamcase import CapturedProgress, CapturedStateOperator


def test_state_operator_preserves_missing_values_and_numeric_zero() -> None:
    missing = CapturedStateOperator()
    zero = CapturedStateOperator(
        operator_name="aggregate",
        num_rows_total=0,
        num_rows_updated=0,
        num_rows_removed=0,
        num_rows_dropped_by_watermark=0,
        memory_used_bytes=0,
    )

    assert missing.operator_name is None
    assert missing.num_rows_total is None
    assert missing.num_rows_updated is None
    assert missing.num_rows_removed is None
    assert missing.num_rows_dropped_by_watermark is None
    assert missing.memory_used_bytes is None
    assert zero != missing
    assert zero == CapturedStateOperator("aggregate", 0, 0, 0, 0, 0)

    with pytest.raises(FrozenInstanceError):
        zero.num_rows_total = 1  # type: ignore[misc]


@pytest.mark.parametrize("operator_name", [1, True, []])
def test_state_operator_rejects_non_string_names(operator_name: object) -> None:
    with pytest.raises(TypeError, match="operator_name must be a str or None"):
        CapturedStateOperator(operator_name=operator_name)  # type: ignore[arg-type]


@pytest.mark.parametrize("operator_name", ["", "  "])
def test_state_operator_rejects_blank_names(operator_name: str) -> None:
    with pytest.raises(ValueError, match="operator_name must not be empty"):
        CapturedStateOperator(operator_name=operator_name)


@pytest.mark.parametrize(
    "metric",
    [
        "num_rows_total",
        "num_rows_updated",
        "num_rows_removed",
        "num_rows_dropped_by_watermark",
        "memory_used_bytes",
    ],
)
@pytest.mark.parametrize("invalid", [True, 1.5, -1])
def test_state_operator_rejects_invalid_metrics(metric: str, invalid: object) -> None:
    error_type = ValueError if invalid == -1 else TypeError
    with pytest.raises(error_type, match=metric):
        CapturedStateOperator(**{metric: invalid})  # type: ignore[arg-type]


def test_progress_preserves_absence_and_snapshots_operator_iterable() -> None:
    operator = CapturedStateOperator("aggregate", num_rows_total=0)
    supplied = [operator]
    progress = CapturedProgress(0, 0, 5, "run-1", state_operators=supplied)
    supplied.clear()

    assert progress.state_operators == (operator,)
    assert progress.num_input_rows is None
    assert progress.watermark is None
    assert CapturedProgress(0, 0, 5, "run-1").state_operators is None
    assert CapturedProgress(0, 0, 5, "run-1", state_operators=[]).state_operators == ()
    assert progress != CapturedProgress(0, 0, 5, "run-1", state_operators=[])


def test_progress_normalizes_aware_watermark_and_keeps_zero() -> None:
    local_zone = timezone(timedelta(hours=5, minutes=30))
    observed = datetime(2026, 10, 13, 12, 30, tzinfo=local_zone)
    progress = CapturedProgress(2, 1, 9, "run-2", num_input_rows=0, watermark=observed)

    assert progress.watermark == datetime(2026, 10, 13, 7, 0, tzinfo=timezone.utc)
    assert progress.watermark is not None
    assert progress.watermark.tzinfo is timezone.utc
    assert progress.num_input_rows == 0
    assert progress == CapturedProgress(
        2, 1, 9, "run-2", num_input_rows=0, watermark=progress.watermark
    )
    with pytest.raises(FrozenInstanceError):
        progress.num_input_rows = 1  # type: ignore[misc]


@pytest.mark.parametrize("field_name", ["action_index", "trigger_index", "batch_id"])
@pytest.mark.parametrize("invalid", [True, "1", -1])
def test_progress_rejects_invalid_identifiers(field_name: str, invalid: object) -> None:
    values: dict[str, object] = {
        "action_index": 0,
        "trigger_index": 0,
        "batch_id": 0,
        "run_id": "run-1",
    }
    values[field_name] = invalid
    error_type = ValueError if invalid == -1 else TypeError
    with pytest.raises(error_type, match=field_name):
        CapturedProgress(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("run_id", [True, 1])
def test_progress_rejects_non_string_run_id(run_id: object) -> None:
    with pytest.raises(TypeError, match="run_id must be a str"):
        CapturedProgress(0, 0, 0, run_id)  # type: ignore[arg-type]


@pytest.mark.parametrize("run_id", ["", "  "])
def test_progress_rejects_blank_run_id(run_id: str) -> None:
    with pytest.raises(ValueError, match="run_id must not be empty"):
        CapturedProgress(0, 0, 0, run_id)


@pytest.mark.parametrize("invalid", [True, -1])
def test_progress_rejects_invalid_input_row_count(invalid: object) -> None:
    error_type = ValueError if invalid == -1 else TypeError
    with pytest.raises(error_type, match="num_input_rows"):
        CapturedProgress(0, 0, 0, "run-1", num_input_rows=invalid)  # type: ignore[arg-type]


def test_progress_rejects_non_datetime_watermark() -> None:
    with pytest.raises(TypeError, match="watermark must be a datetime or None"):
        CapturedProgress(0, 0, 0, "run-1", watermark="2026-10-13")  # type: ignore[arg-type]


def test_progress_rejects_naive_watermark() -> None:
    with pytest.raises(ValueError, match="watermark must be timezone-aware"):
        CapturedProgress(0, 0, 0, "run-1", watermark=datetime(2026, 10, 13))


def test_progress_rejects_non_model_operator_with_index() -> None:
    with pytest.raises(TypeError, match=r"index 1.*got dict"):
        CapturedProgress(
            0,
            0,
            0,
            "run-1",
            state_operators=[CapturedStateOperator(), {}],  # type: ignore[list-item]
        )
