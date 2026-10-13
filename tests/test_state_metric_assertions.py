from __future__ import annotations

from typing import cast

import pytest

from streamcase import (
    CapturedProgress,
    CapturedStateOperator,
    ScenarioResult,
    assert_state_metric,
)


def _result(*progress: CapturedProgress) -> ScenarioResult:
    return ScenarioResult([], progress=progress)


@pytest.mark.parametrize(
    ("metric", "expected"),
    [
        ("num_rows_total", 3),
        ("num_rows_updated", 2),
        ("num_rows_removed", 0),
        ("num_rows_dropped_by_watermark", 1),
        ("memory_used_bytes", 4096),
    ],
)
def test_all_documented_metrics_match_exactly(metric: str, expected: int) -> None:
    operator = CapturedStateOperator(
        operator_name="stateStoreSave",
        num_rows_total=3,
        num_rows_updated=2,
        num_rows_removed=0,
        num_rows_dropped_by_watermark=1,
        memory_used_bytes=4096,
    )
    result = _result(CapturedProgress(2, 0, 7, "run-1", state_operators=[operator]))

    assert_state_metric(result, metric, expected, action_index=2, trigger_index=0, operator_index=0)


def test_operator_index_disambiguates_repeated_names_and_triggers() -> None:
    result = _result(
        CapturedProgress(
            2,
            0,
            7,
            "run-1",
            state_operators=[CapturedStateOperator("same", num_rows_total=1)],
        ),
        CapturedProgress(
            2,
            1,
            8,
            "run-1",
            state_operators=[
                CapturedStateOperator("same", num_rows_total=2),
                CapturedStateOperator("same", num_rows_total=3),
            ],
        ),
    )

    assert_state_metric(
        result, "num_rows_total", 3, action_index=2, trigger_index=1, operator_index=1
    )


def test_mismatch_reports_metric_expected_actual_and_operator_context() -> None:
    result = _result(
        CapturedProgress(
            2,
            0,
            7,
            "run-1",
            state_operators=[CapturedStateOperator("stateStoreSave", num_rows_total=3)],
        )
    )

    with pytest.raises(AssertionError) as error_info:
        assert_state_metric(
            result, "num_rows_total", 4, action_index=2, trigger_index=0, operator_index=0
        )

    message = str(error_info.value)
    assert "State metric num_rows_total differs" in message
    assert "action 2, trigger 0, Spark batch 7, run 'run-1'" in message
    assert "operator 0 ('stateStoreSave')" in message
    assert "Expected: 4. Actual: 3." in message


def test_unreported_operator_collection_fails_distinctly_from_empty() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1"))

    with pytest.raises(AssertionError) as error_info:
        assert_state_metric(
            result, "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=0
        )

    assert "State operators unavailable" in str(error_info.value)
    assert "Spark did not report them" in str(error_info.value)


def test_reported_empty_operator_collection_fails_as_out_of_range() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1", state_operators=[]))

    with pytest.raises(AssertionError, match=r"0 operator\(s\) reported"):
        assert_state_metric(
            result, "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=0
        )


def test_operator_index_out_of_range_reports_available_count() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1", state_operators=[CapturedStateOperator()]))

    with pytest.raises(AssertionError, match=r"1 operator\(s\) reported"):
        assert_state_metric(
            result, "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=1
        )


def test_missing_metric_fails_distinctly_from_zero_without_operator_name() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1", state_operators=[CapturedStateOperator()]))

    with pytest.raises(AssertionError) as error_info:
        assert_state_metric(
            result, "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=0
        )

    message = str(error_info.value)
    assert "State metric num_rows_total unavailable" in message
    assert "operator 0" in message
    assert "Actual: 0" not in message


def test_missing_progress_report_fails_before_operator_selection() -> None:
    with pytest.raises(AssertionError, match="No progress report was captured for action 2"):
        assert_state_metric(
            _result(), "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=0
        )


def test_duplicate_progress_identity_is_rejected() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1"), CapturedProgress(2, 0, 8, "run-2"))

    with pytest.raises(AssertionError, match="Multiple progress reports match action 2"):
        assert_state_metric(
            result, "num_rows_total", 0, action_index=2, trigger_index=0, operator_index=0
        )


@pytest.mark.parametrize("invalid", [None, 1])
def test_metric_name_must_be_string(invalid: object) -> None:
    with pytest.raises(TypeError, match="metric must be a str"):
        assert_state_metric(
            _result(), cast(str, invalid), 0, action_index=0, trigger_index=0, operator_index=0
        )


def test_unsupported_metric_name_is_rejected() -> None:
    with pytest.raises(ValueError, match="metric must be one of") as error_info:
        assert_state_metric(
            _result(), "vendorCustomMetric", 0, action_index=0, trigger_index=0, operator_index=0
        )

    assert "num_rows_total" in str(error_info.value)
    assert "memory_used_bytes" in str(error_info.value)


@pytest.mark.parametrize("invalid", [True, 1.5, "1"])
def test_expected_value_must_be_int(invalid: object) -> None:
    with pytest.raises(TypeError, match="expected must be an int"):
        assert_state_metric(
            _result(),
            "num_rows_total",
            cast(int, invalid),
            action_index=0,
            trigger_index=0,
            operator_index=0,
        )


def test_expected_value_must_be_non_negative() -> None:
    with pytest.raises(ValueError, match="expected must be non-negative"):
        assert_state_metric(
            _result(), "num_rows_total", -1, action_index=0, trigger_index=0, operator_index=0
        )


@pytest.mark.parametrize("invalid", [True, 1.5, "0"])
def test_operator_index_must_be_int(invalid: object) -> None:
    with pytest.raises(TypeError, match="operator_index must be an int"):
        assert_state_metric(
            _result(),
            "num_rows_total",
            0,
            action_index=0,
            trigger_index=0,
            operator_index=cast(int, invalid),
        )


def test_operator_index_must_be_non_negative() -> None:
    with pytest.raises(ValueError, match="operator_index must be non-negative"):
        assert_state_metric(
            _result(), "num_rows_total", 0, action_index=0, trigger_index=0, operator_index=-1
        )


def test_invalid_result_is_rejected() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult"):
        assert_state_metric(
            cast(ScenarioResult, []),
            "num_rows_total",
            0,
            action_index=0,
            trigger_index=0,
            operator_index=0,
        )


def test_indexes_are_required_keywords() -> None:
    with pytest.raises(TypeError, match="required keyword-only arguments"):
        assert_state_metric(_result(), "num_rows_total", 0)  # type: ignore[call-arg]
