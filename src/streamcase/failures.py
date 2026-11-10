"""Backend-independent observations for expected streaming-query failures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

_MAX_FAILURE_MESSAGE_CHARS = 1_024
_MAX_FAILURE_CONDITION_CHARS = 256


def _validate_action_index(action_index: int) -> None:
    if isinstance(action_index, bool) or not isinstance(action_index, int):
        raise TypeError(f"Failure action index must be an int; got {type(action_index).__name__}.")
    if action_index < 0:
        raise ValueError("Failure action index must be non-negative.")


@dataclass(frozen=True, slots=True, init=False)
class FailureObservation:
    """Immutable, bounded text snapshot of one terminated query.

    ``message`` may contain user data and is truncated, not redacted. It is
    excluded from repr to avoid accidentally printing it with a result.
    """

    action_index: int
    kind: Literal["query_terminated"]
    condition: str | None
    message: str = field(repr=False)
    truncated: bool

    def __init__(
        self,
        action_index: int,
        *,
        message: str,
        condition: str | None = None,
    ) -> None:
        """Normalize a plain-text preview without retaining exception objects."""
        _validate_action_index(action_index)
        if not isinstance(message, str):
            raise TypeError(f"Failure message must be a string; got {type(message).__name__}.")
        if condition is not None:
            if not isinstance(condition, str):
                raise TypeError(
                    f"Failure condition must be a string or None; got {type(condition).__name__}."
                )
            if not condition.strip():
                raise ValueError("Failure condition must not be blank.")
            if len(condition) > _MAX_FAILURE_CONDITION_CHARS:
                raise ValueError(
                    "Failure condition must contain at most "
                    f"{_MAX_FAILURE_CONDITION_CHARS} characters."
                )

        normalized_message = message.replace("\r\n", "\n").replace("\r", "\n")
        object.__setattr__(self, "action_index", action_index)
        object.__setattr__(self, "kind", "query_terminated")
        object.__setattr__(self, "condition", condition)
        object.__setattr__(self, "message", normalized_message[:_MAX_FAILURE_MESSAGE_CHARS])
        object.__setattr__(
            self,
            "truncated",
            len(normalized_message) > _MAX_FAILURE_MESSAGE_CHARS,
        )


class ExpectedFailureNotObservedError(AssertionError):
    """A declared terminal query failure did not occur at its action."""

    def __init__(self, action_index: int) -> None:
        _validate_action_index(action_index)
        self.action_index = action_index
        super().__init__(
            f"Expected query failure at scenario action index {action_index} was not observed."
        )
