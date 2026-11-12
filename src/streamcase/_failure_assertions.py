"""Assertions over immutable expected-query-failure observations."""

from __future__ import annotations

from typing import Literal

from streamcase.failures import FailureObservation
from streamcase.results import ScenarioResult

_MAX_MESSAGE_CHARS = 1_024
_MAX_CONDITION_CHARS = 256
_MAX_DISPLAY_CHARS = 80


def _display(value: str) -> str:
    """Keep expected and actual free-form fields on one short diagnostic line."""
    rendered = repr(value)
    if len(rendered) <= _MAX_DISPLAY_CHARS:
        return rendered
    omitted = len(rendered) - _MAX_DISPLAY_CHARS
    return f"{rendered[:_MAX_DISPLAY_CHARS]}... ({omitted} more chars omitted)"


def _mismatch(subject: str, expected: str, actual: str) -> AssertionError:
    return AssertionError(
        "\n".join(
            (
                f"Failure assertion failed: {subject}.",
                f"Expected: {expected}.",
                f"Actual: {actual}.",
            )
        )
    )


def _validate_expected_text(value: str | None, name: str, max_chars: int) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str or None; got {type(value).__name__}.")
    if not value.strip():
        raise ValueError(f"{name} must not be blank.")
    if len(value) > max_chars:
        raise ValueError(f"{name} must contain at most {max_chars} characters.")


def assert_failure(
    result: ScenarioResult,
    *,
    action_index: int,
    kind: Literal["query_terminated"] = "query_terminated",
    condition: str | None = None,
    message_contains: str | None = None,
) -> None:
    """Assert a matched query failure's action, kind, and optional fields.

    ``message_contains`` checks the bounded diagnostic preview, not a Spark
    exception or stack trace. Prefer a caller-controlled marker to Spark prose.
    """
    if not isinstance(result, ScenarioResult):
        raise TypeError(f"result must be a ScenarioResult; got {type(result).__name__}.")
    if isinstance(action_index, bool) or not isinstance(action_index, int):
        raise TypeError(f"action_index must be an int; got {type(action_index).__name__}.")
    if action_index < 0:
        raise ValueError("action_index must be non-negative.")
    if not isinstance(kind, str):
        raise TypeError(f"kind must be a str; got {type(kind).__name__}.")
    if kind != "query_terminated":
        raise ValueError("kind must be 'query_terminated'.")
    _validate_expected_text(condition, "condition", _MAX_CONDITION_CHARS)
    _validate_expected_text(message_contains, "message_contains", _MAX_MESSAGE_CHARS)

    observation: FailureObservation | None = result.failure
    if observation is None:
        raise _mismatch("failure observation", f"action {action_index}, kind {kind}", "none")
    if observation.action_index != action_index:
        raise _mismatch("action index", str(action_index), str(observation.action_index))
    if observation.kind != kind:
        raise _mismatch("failure kind", kind, _display(observation.kind))
    if condition is not None and observation.condition != condition:
        actual_condition = (
            "unavailable" if observation.condition is None else _display(observation.condition)
        )
        raise _mismatch("condition", _display(condition), actual_condition)
    if message_contains is not None and message_contains not in observation.message:
        actual = (
            f"not found in {len(observation.message)}-character preview "
            f"(truncated={observation.truncated})"
        )
        raise _mismatch("message marker", _display(message_contains), actual)
