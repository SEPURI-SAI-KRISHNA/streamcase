"""Assertions over immutable, backend-independent progress snapshots."""

from __future__ import annotations

from streamcase.progress import CapturedProgress
from streamcase.results import ScenarioResult


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
) -> CapturedProgress:
    """Select exactly one snapshot by its logical action and trigger indexes."""
    if not isinstance(result, ScenarioResult):
        raise TypeError(f"result must be a ScenarioResult; got {type(result).__name__}.")
    action_index = _non_negative_index(action_index, "action_index")
    trigger_index = _non_negative_index(trigger_index, "trigger_index")
    matches = [
        progress
        for progress in result.progress
        if progress.action_index == action_index and progress.trigger_index == trigger_index
    ]
    if not matches:
        raise AssertionError(
            f"No progress report was captured for action {action_index}, trigger {trigger_index}."
        )
    if len(matches) > 1:
        raise AssertionError(
            f"Multiple progress reports match action {action_index}, trigger {trigger_index}."
        )
    return matches[0]


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
    context = (
        f"action {action_index}, trigger {trigger_index}, "
        f"Spark batch {progress.batch_id}, run {progress.run_id!r}"
    )
    if progress.num_input_rows is None:
        raise AssertionError(f"Input-row count unavailable for {context}: Spark did not report it.")
    if progress.num_input_rows != expected_count:
        raise AssertionError(
            f"Input-row count differs for {context}. "
            f"Expected: {expected_count}. Actual: {progress.num_input_rows}."
        )
