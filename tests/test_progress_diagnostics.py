from __future__ import annotations

import traceback
from collections.abc import Callable
from datetime import datetime, timezone

import pytest

from streamcase import (
    CapturedProgress,
    CapturedStateOperator,
    ScenarioResult,
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
)


@pytest.mark.parametrize(
    ("assertion", "subject", "expected", "actual"),
    [
        ("input", "input-row count", "2", "1"),
        ("watermark", "watermark", "None", "2026-10-13T00:00:00+00:00"),
        ("state", "state metric num_rows_total", "2", "1"),
    ],
)
def test_progress_assertions_share_ordered_diagnostic_shape(
    assertion: str, subject: str, expected: str, actual: str
) -> None:
    progress = CapturedProgress(
        2,
        0,
        7,
        "run-1",
        num_input_rows=1,
        watermark=datetime(2026, 10, 13, tzinfo=timezone.utc),
        state_operators=[CapturedStateOperator("stateStoreSave", num_rows_total=1)],
    )
    result = ScenarioResult([], progress=[progress])
    calls: dict[str, Callable[[], None]] = {
        "input": lambda: assert_input_row_count(result, 2, action_index=2, trigger_index=0),
        "watermark": lambda: assert_watermark(result, None, action_index=2, trigger_index=0),
        "state": lambda: assert_state_metric(
            result, "num_rows_total", 2, action_index=2, trigger_index=0, operator_index=0
        ),
    }

    with pytest.raises(AssertionError) as error_info:
        calls[assertion]()

    lines = str(error_info.value).splitlines()
    assert len(lines) == 4
    assert lines[0] == f"Progress assertion failed: {subject}."
    assert lines[1].startswith("Context: action 2, trigger 0, Spark batch 7, run 'run-1'")
    assert lines[2] == f"Expected: {expected}."
    assert lines[3] == f"Actual: {actual}."
    if assertion == "state":
        assert "operator 0 ('stateStoreSave')" in lines[1]


def test_missing_report_uses_same_shape_and_keeps_requested_context() -> None:
    with pytest.raises(AssertionError) as error_info:
        assert_state_metric(
            ScenarioResult([]),
            "num_rows_total",
            0,
            action_index=2,
            trigger_index=1,
            operator_index=3,
        )

    assert str(error_info.value).splitlines() == [
        "Progress assertion failed: progress report selection.",
        "Context: action 2, trigger 1, operator 3.",
        "Expected: exactly one progress report.",
        "Actual: No progress report was captured.",
    ]


def test_long_run_and_operator_names_are_bounded_without_raw_payloads() -> None:
    progress = CapturedProgress(
        2,
        0,
        7,
        "r" * 5000,
        state_operators=[CapturedStateOperator("o" * 5000, num_rows_total=1)],
    )

    with pytest.raises(AssertionError) as error_info:
        assert_state_metric(
            ScenarioResult([], progress=[progress]),
            "num_rows_total",
            2,
            action_index=2,
            trigger_index=0,
            operator_index=0,
        )

    lines = str(error_info.value).splitlines()
    assert len(lines) == 4
    assert len(lines[1]) < 300
    assert lines[1].count("more chars omitted") == 2
    assert "r" * 5000 not in lines[1]
    assert "o" * 5000 not in lines[1]


def test_assertion_traceback_keeps_public_call_and_error_context() -> None:
    result = ScenarioResult([], progress=[CapturedProgress(2, 0, 7, "run-1", num_input_rows=1)])

    with pytest.raises(AssertionError) as error_info:
        assert_input_row_count(result, 2, action_index=2, trigger_index=0)

    rendered = "".join(traceback.format_exception(error_info.value))
    assert "test_assertion_traceback_keeps_public_call_and_error_context" in rendered
    assert "assert_input_row_count" in rendered
    assert "Context: action 2, trigger 0, Spark batch 7" in rendered


def test_control_characters_in_identifiers_remain_on_one_line() -> None:
    progress = CapturedProgress(2, 0, 7, "run\nnext", num_input_rows=1)

    with pytest.raises(AssertionError) as error_info:
        assert_input_row_count(
            ScenarioResult([], progress=[progress]), 2, action_index=2, trigger_index=0
        )

    lines = str(error_info.value).splitlines()
    assert len(lines) == 4
    assert r"run\nnext" in lines[1]
