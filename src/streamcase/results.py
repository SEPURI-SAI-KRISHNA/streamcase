"""Immutable backend-independent captured result models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from types import MappingProxyType


def _freeze_mapping(value: Mapping[object, object], path: str) -> Mapping[str, object]:
    frozen: dict[str, object] = {}
    for key, nested_value in value.items():
        if not isinstance(key, str):
            message = f"Captured mapping at {path} must use string keys; got {type(key).__name__}."
            raise TypeError(message)
        frozen[key] = _freeze_value(nested_value, f"{path}.{key}")
    return MappingProxyType(frozen)


def _freeze_value(value: object, path: str) -> object:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str, bytes, Decimal, date)):
        return value
    if isinstance(value, Mapping):
        return _freeze_mapping(value, path)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_value(nested_value, f"{path}[{index}]")
            for index, nested_value in enumerate(value)
        )

    message = f"Captured value at {path} has unsupported type {type(value).__name__}."
    raise TypeError(message)


def _freeze_row(row: object, row_index: int) -> Mapping[str, object]:
    if not isinstance(row, Mapping):
        message = f"Captured row at index {row_index} must be a mapping; got {type(row).__name__}."
        raise TypeError(message)
    return _freeze_mapping(row, f"row[{row_index}]")


@dataclass(frozen=True, slots=True, init=False)
class CapturedBatch:
    """One immutable output batch captured independently of a streaming backend."""

    batch_id: int
    rows: tuple[Mapping[str, object], ...]

    def __init__(self, batch_id: int, rows: Iterable[Mapping[str, object]]) -> None:
        """Snapshot an output batch and recursively freeze its rows."""
        if isinstance(batch_id, bool) or not isinstance(batch_id, int):
            message = f"Captured batch ID must be an int; got {type(batch_id).__name__}."
            raise TypeError(message)
        if batch_id < 0:
            raise ValueError("Captured batch ID must be non-negative.")

        snapshots = tuple(_freeze_row(row, index) for index, row in enumerate(rows))
        object.__setattr__(self, "batch_id", batch_id)
        object.__setattr__(self, "rows", snapshots)


@dataclass(frozen=True, slots=True, init=False)
class ScenarioResult:
    """An immutable ordered snapshot of captured output batches."""

    batches: tuple[CapturedBatch, ...]
    _rows: tuple[Mapping[str, object], ...] = field(repr=False, compare=False)

    def __init__(self, batches: Iterable[CapturedBatch]) -> None:
        """Snapshot captured batches and expose their rows in execution order."""
        snapshot = tuple(batches)
        for index, captured_batch in enumerate(snapshot):
            if not isinstance(captured_batch, CapturedBatch):
                message = (
                    f"Scenario result batch at index {index} must be a CapturedBatch; "
                    f"got {type(captured_batch).__name__}."
                )
                raise TypeError(message)

        rows = tuple(row for captured_batch in snapshot for row in captured_batch.rows)
        object.__setattr__(self, "batches", snapshot)
        object.__setattr__(self, "_rows", rows)

    @property
    def rows(self) -> tuple[Mapping[str, object], ...]:
        """Return captured rows flattened in batch and row order."""
        return self._rows
