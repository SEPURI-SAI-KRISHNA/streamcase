from __future__ import annotations

from typing import cast

import pytest

from streamcase import CapturedBatch, CapturedProgress, ScenarioResult, assert_input_row_count


def _result(*progress: CapturedProgress) -> ScenarioResult:
    return ScenarioResult([CapturedBatch(9, [{"value": 1}, {"value": 2}])], progress=progress)


def test_input_row_count_selects_exact_action_and_trigger() -> None:
    result = _result(
        CapturedProgress(0, 0, 4, "first", num_input_rows=0),
        CapturedProgress(2, 0, 7, "second", num_input_rows=3),
        CapturedProgress(2, 1, 8, "second", num_input_rows=1),
    )

    assert_input_row_count(result, 0, action_index=0, trigger_index=0)
    assert_input_row_count(result, 3, action_index=2, trigger_index=0)
    assert_input_row_count(result, 1, action_index=2, trigger_index=1)


def test_input_row_count_mismatch_reports_selected_batch_and_values() -> None:
    result = _result(CapturedProgress(2, 1, 8, "run-2", num_input_rows=3))

    with pytest.raises(AssertionError) as error_info:
        assert_input_row_count(result, 4, action_index=2, trigger_index=1)

    message = str(error_info.value)
    assert "Input-row count differs" in message
    assert "action 2, trigger 1" in message
    assert "Spark batch 8" in message
    assert "run 'run-2'" in message
    assert "Expected: 4. Actual: 3." in message


def test_missing_input_count_fails_distinctly_from_reported_zero() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1"))

    with pytest.raises(AssertionError) as error_info:
        assert_input_row_count(result, 0, action_index=2, trigger_index=0)

    message = str(error_info.value)
    assert "unavailable" in message
    assert "action 2, trigger 0" in message
    assert "Spark batch 7" in message
    assert "run 'run-1'" in message
    assert "Actual: 0" not in message


def test_input_count_is_not_inferred_from_output_rows() -> None:
    with pytest.raises(AssertionError, match="No progress report was captured"):
        assert_input_row_count(_result(), 2, action_index=0, trigger_index=0)


def test_missing_selected_trigger_reports_action_and_trigger() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1", num_input_rows=1))

    with pytest.raises(AssertionError, match="action 2, trigger 1"):
        assert_input_row_count(result, 1, action_index=2, trigger_index=1)


def test_duplicate_progress_identity_is_ambiguous() -> None:
    result = _result(
        CapturedProgress(2, 0, 7, "run-1", num_input_rows=1),
        CapturedProgress(2, 0, 8, "run-2", num_input_rows=1),
    )

    with pytest.raises(AssertionError, match="Multiple progress reports match action 2, trigger 0"):
        assert_input_row_count(result, 1, action_index=2, trigger_index=0)


@pytest.mark.parametrize("expected_count", [True, 1.5, "1"])
def test_input_row_count_rejects_noninteger_expected_count(expected_count: object) -> None:
    with pytest.raises(TypeError, match="expected_count must be an int"):
        assert_input_row_count(
            _result(), cast(int, expected_count), action_index=0, trigger_index=0
        )


def test_input_row_count_rejects_negative_expected_count() -> None:
    with pytest.raises(ValueError, match="expected_count must be non-negative"):
        assert_input_row_count(_result(), -1, action_index=0, trigger_index=0)


@pytest.mark.parametrize("index_name", ["action_index", "trigger_index"])
@pytest.mark.parametrize("invalid", [True, 1.5, "0"])
def test_input_row_count_rejects_noninteger_indexes(index_name: str, invalid: object) -> None:
    indexes = {"action_index": 0, "trigger_index": 0, index_name: invalid}
    with pytest.raises(TypeError, match=rf"{index_name} must be an int"):
        assert_input_row_count(_result(), 0, **indexes)  # type: ignore[arg-type]


@pytest.mark.parametrize("index_name", ["action_index", "trigger_index"])
def test_input_row_count_rejects_negative_indexes(index_name: str) -> None:
    indexes = {"action_index": 0, "trigger_index": 0, index_name: -1}
    with pytest.raises(ValueError, match=rf"{index_name} must be non-negative"):
        assert_input_row_count(_result(), 0, **indexes)


def test_input_row_count_requires_result_model() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult; got list"):
        assert_input_row_count([], 0, action_index=0, trigger_index=0)  # type: ignore[arg-type]


def test_input_row_count_requires_explicit_keyword_indexes() -> None:
    with pytest.raises(TypeError, match="required keyword-only arguments"):
        assert_input_row_count(_result(), 0)  # type: ignore[call-arg]
