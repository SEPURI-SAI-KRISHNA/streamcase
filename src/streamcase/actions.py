"""Scenario actions for describing streaming test inputs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True, init=False)
class Batch:
    """One non-empty logical batch of input rows.

    Row mappings are copied when the batch is created and exposed as read-only
    mappings. Every row key must be a string. Values inside a row are not
    recursively copied or frozen.
    """

    rows: tuple[Mapping[str, object], ...]

    def __init__(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Create a batch from rows while preserving their insertion order."""
        snapshots = tuple(MappingProxyType(dict(row)) for row in rows)
        if not snapshots:
            raise ValueError("Batch must contain at least one row.")

        for row_index, row in enumerate(snapshots):
            for key in row:
                if not isinstance(key, str):
                    message = (
                        f"Batch row at index {row_index} contains a non-string key "
                        f"of type {type(key).__name__}."
                    )
                    raise TypeError(message)

        object.__setattr__(self, "rows", snapshots)


@dataclass(frozen=True, slots=True)
class Restart:
    """A checkpoint-preserving streaming-query restart boundary."""


@dataclass(frozen=True, slots=True, init=False)
class ExpectFailure:
    """A terminal batch whose query failure must match explicit selectors.

    The declaration is backend-independent. Runner execution is added in a
    separate phase; constructing this action does not start Spark.
    """

    batch: Batch
    message_contains: str | None
    condition: str | None

    def __init__(
        self,
        batch: Batch,
        *,
        message_contains: str | None = None,
        condition: str | None = None,
    ) -> None:
        if not isinstance(batch, Batch):
            raise TypeError(f"Expected-failure input must be a Batch; got {type(batch).__name__}.")

        for name, value in (("message_contains", message_contains), ("condition", condition)):
            if value is not None and not isinstance(value, str):
                raise TypeError(f"Expected-failure {name} must be a string or None.")
            if isinstance(value, str) and not value.strip():
                raise ValueError(f"Expected-failure {name} must not be blank.")

        if message_contains is None and condition is None:
            raise ValueError("Expected failure requires message_contains or condition.")
        if condition == "STREAM_FAILED" and message_contains is None:
            raise ValueError("STREAM_FAILED requires a distinctive message_contains marker.")

        object.__setattr__(self, "batch", batch)
        object.__setattr__(self, "message_contains", message_contains)
        object.__setattr__(self, "condition", condition)


def batch(*rows: Mapping[str, object]) -> Batch:
    """Create a :class:`Batch` from one or more input rows."""
    return Batch(rows)


def restart() -> Restart:
    """Create a checkpoint-preserving restart action."""
    return Restart()


def expect_failure(
    action: Batch,
    *,
    message_contains: str | None = None,
    condition: str | None = None,
) -> ExpectFailure:
    """Declare one terminal input batch expected to terminate the query."""
    return ExpectFailure(action, message_contains=message_contains, condition=condition)
