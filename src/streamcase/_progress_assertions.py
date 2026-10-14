"""Assertions over immutable, backend-independent progress snapshots."""

from __future__ import annotations

from datetime import datetime, timezone

from streamcase._diagnostics import _bounded_progress_identifier, _format_progress_failure
from streamcase.progress import CapturedProgress
from streamcase.results import ScenarioResult

_STATE_METRICS = frozenset(
    {
        "num_rows_total",
        "num_rows_updated",
        "num_rows_removed",
        "num_rows_dropped_by_watermark",
        "memory_used_bytes",
    }
)


def _non_negative_index(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int; got {type(value).__name__}.")
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")
    return value


def _select_progress(
    result: ScenarioResult,
    action_index: int,
    trigger_index: int,
    operator_index: int | None = None,
) -> CapturedProgress:
    """Select exactly one snapshot by its logical action and trigger indexes."""
    if not isinstance(result, ScenarioResult):
        raise TypeError(f"result must be a ScenarioResult; got {type(result).__name__}.")
    action_index = _non_negative_index(action_index, "action_index")
    trigger_index = _non_negative_index(trigger_index, "trigger_index")
    context = f"action {action_index}, trigger {trigger_index}"
    if operator_index is not None:
        context += f", operator {operator_index}"
    matches = [
        progress
        for progress in result.progress
        if progress.action_index == action_index and progress.trigger_index == trigger_index
    ]
    if not matches:
        raise AssertionError(
            _format_progress_failure(
                "progress report selection",
                context,
                "exactly one progress report",
                "No progress report was captured",
            )
        )
    if len(matches) > 1:
        raise AssertionError(
            _format_progress_failure(
                "progress report selection",
                context,
                "exactly one progress report",
                f"Multiple progress reports match ({len(matches)} matched)",
            )
        )
    return matches[0]


def _progress_context(
    progress: CapturedProgress,
    operator_index: int | None = None,
    operator_name: str | None = None,
) -> str:
    context = (
        f"action {progress.action_index}, trigger {progress.trigger_index}, "
        f"Spark batch {progress.batch_id}, run {_bounded_progress_identifier(progress.run_id)}"
    )
    if operator_index is not None:
        context += f", operator {operator_index}"
        if operator_name is not None:
            context += f" ({_bounded_progress_identifier(operator_name)})"
    return context


def assert_input_row_count(
    result: ScenarioResult,
    expected_count: int,
    *,
    action_index: int,
    trigger_index: int,
) -> None:
    """Assert Spark's reported input rows for one captured trigger."""
    if isinstance(expected_count, bool) or not isinstance(expected_count, int):
        raise TypeError(f"expected_count must be an int; got {type(expected_count).__name__}.")
    if expected_count < 0:
        raise ValueError("expected_count must be non-negative.")

    progress = _select_progress(result, action_index, trigger_index)
    context = _progress_context(progress)
    if progress.num_input_rows is None:
        raise AssertionError(
            _format_progress_failure(
                "input-row count",
                context,
                str(expected_count),
                "unavailable (Spark did not report numInputRows)",
            )
        )
    if progress.num_input_rows != expected_count:
        raise AssertionError(
            _format_progress_failure(
                "input-row count", context, str(expected_count), str(progress.num_input_rows)
            )
        )


def assert_watermark(
    result: ScenarioResult,
    expected: datetime | None,
    *,
    action_index: int,
    trigger_index: int,
) -> None:
    """Assert the observed watermark, or its absence, for one captured trigger."""
    if expected is not None:
        if not isinstance(expected, datetime):
            raise TypeError(f"expected must be a datetime or None; got {type(expected).__name__}.")
        if expected.tzinfo is None or expected.utcoffset() is None:
            raise ValueError("expected watermark must be timezone-aware.")
        expected = expected.astimezone(timezone.utc)

    progress = _select_progress(result, action_index, trigger_index)
    if progress.watermark != expected:
        expected_text = "None" if expected is None else expected.isoformat()
        actual_text = "None" if progress.watermark is None else progress.watermark.isoformat()
        raise AssertionError(
            _format_progress_failure(
                "watermark", _progress_context(progress), expected_text, actual_text
            )
        )


def assert_state_metric(
    result: ScenarioResult,
    metric: str,
    expected: int,
    *,
    action_index: int,
    trigger_index: int,
    operator_index: int,
) -> None:
    """Assert one documented metric on one state operator in a captured trigger."""
    if not isinstance(metric, str):
        raise TypeError(f"metric must be a str; got {type(metric).__name__}.")
    if metric not in _STATE_METRICS:
        allowed = ", ".join(sorted(_STATE_METRICS))
        raise ValueError(f"metric must be one of: {allowed}.")
    if isinstance(expected, bool) or not isinstance(expected, int):
        raise TypeError(f"expected must be an int; got {type(expected).__name__}.")
    if expected < 0:
        raise ValueError("expected must be non-negative.")
    operator_index = _non_negative_index(operator_index, "operator_index")

    progress = _select_progress(result, action_index, trigger_index, operator_index)
    context = _progress_context(progress, operator_index)
    operators = progress.state_operators
    if operators is None:
        raise AssertionError(
            _format_progress_failure(
                f"state metric {metric}",
                context,
                str(expected),
                "unavailable (Spark did not report state operators)",
            )
        )
    if operator_index >= len(operators):
        unavailable = (
            f"operator index {operator_index} out of range ({len(operators)} operator(s) reported)"
        )
        raise AssertionError(
            _format_progress_failure(
                f"state metric {metric}",
                context,
                str(expected),
                unavailable,
            )
        )

    operator = operators[operator_index]
    context = _progress_context(progress, operator_index, operator.operator_name)
    values = {
        "num_rows_total": operator.num_rows_total,
        "num_rows_updated": operator.num_rows_updated,
        "num_rows_removed": operator.num_rows_removed,
        "num_rows_dropped_by_watermark": operator.num_rows_dropped_by_watermark,
        "memory_used_bytes": operator.memory_used_bytes,
    }
    actual = values[metric]
    if actual is None:
        raise AssertionError(
            _format_progress_failure(
                f"state metric {metric}",
                context,
                str(expected),
                f"unavailable (Spark did not report {metric})",
            )
        )
    if actual != expected:
        raise AssertionError(
            _format_progress_failure(f"state metric {metric}", context, str(expected), str(actual))
        )
