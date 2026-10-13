from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import cast

import pytest

from streamcase import CapturedProgress, ScenarioResult, assert_watermark


def _result(*progress: CapturedProgress) -> ScenarioResult:
    return ScenarioResult([], progress=progress)


def test_watermark_compares_aware_datetimes_as_instants() -> None:
    observed = datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)
    expected = datetime(2026, 10, 13, 18, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    result = _result(
        CapturedProgress(0, 0, 4, "run-1", watermark=None),
        CapturedProgress(2, 1, 8, "run-2", watermark=observed),
    )

    assert_watermark(result, expected, action_index=2, trigger_index=1)
    assert_watermark(result, None, action_index=0, trigger_index=0)


def test_expected_none_does_not_match_observed_epoch_watermark() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    result = _result(CapturedProgress(2, 0, 7, "run-1", watermark=epoch))

    with pytest.raises(AssertionError) as error_info:
        assert_watermark(result, None, action_index=2, trigger_index=0)

    message = str(error_info.value)
    assert "Watermark differs" in message
    assert "action 2, trigger 0, Spark batch 7, run 'run-1'" in message
    assert "Expected: None. Actual: 1970-01-01T00:00:00+00:00." in message


def test_expected_watermark_does_not_match_missing_observation() -> None:
    result = _result(CapturedProgress(2, 0, 7, "run-1"))
    expected = datetime(2026, 10, 13, tzinfo=timezone.utc)

    with pytest.raises(AssertionError) as error_info:
        assert_watermark(result, expected, action_index=2, trigger_index=0)

    assert "Expected: 2026-10-13T00:00:00+00:00. Actual: None." in str(error_info.value)


def test_different_observed_and_expected_watermarks_fail() -> None:
    actual = datetime(2026, 10, 13, 12, tzinfo=timezone.utc)
    expected = datetime(2026, 10, 13, 13, tzinfo=timezone.utc)
    result = _result(CapturedProgress(0, 0, 4, "run-1", watermark=actual))

    with pytest.raises(AssertionError) as error_info:
        assert_watermark(result, expected, action_index=0, trigger_index=0)

    message = str(error_info.value)
    assert "Expected: 2026-10-13T13:00:00+00:00." in message
    assert "Actual: 2026-10-13T12:00:00+00:00." in message


def test_expected_none_still_requires_selected_progress_report() -> None:
    with pytest.raises(AssertionError, match="No progress report was captured for action 2"):
        assert_watermark(_result(), None, action_index=2, trigger_index=0)


def test_duplicate_progress_identity_is_rejected() -> None:
    result = _result(
        CapturedProgress(2, 0, 7, "run-1"),
        CapturedProgress(2, 0, 8, "run-2"),
    )

    with pytest.raises(AssertionError, match="Multiple progress reports match action 2"):
        assert_watermark(result, None, action_index=2, trigger_index=0)


@pytest.mark.parametrize("invalid", ["2026-10-13", date(2026, 10, 13), 0])
def test_watermark_rejects_non_datetime_expectation(invalid: object) -> None:
    with pytest.raises(TypeError, match="expected must be a datetime or None"):
        assert_watermark(_result(), cast(datetime, invalid), action_index=0, trigger_index=0)


def test_watermark_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="expected watermark must be timezone-aware"):
        assert_watermark(_result(), datetime(2026, 10, 13), action_index=0, trigger_index=0)


def test_watermark_rejects_non_result() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult"):
        assert_watermark([], None, action_index=0, trigger_index=0)  # type: ignore[arg-type]


@pytest.mark.parametrize("index_name", ["action_index", "trigger_index"])
def test_watermark_rejects_invalid_indexes(index_name: str) -> None:
    invalid_indexes = {"action_index": 0, "trigger_index": 0, index_name: True}
    with pytest.raises(TypeError, match=rf"{index_name} must be an int"):
        assert_watermark(_result(), None, **invalid_indexes)


def test_watermark_requires_explicit_keyword_indexes() -> None:
    with pytest.raises(TypeError, match="required keyword-only arguments"):
        assert_watermark(_result(), None)  # type: ignore[call-arg]
