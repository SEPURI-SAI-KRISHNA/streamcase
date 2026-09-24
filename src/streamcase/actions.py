"""Scenario actions for describing streaming test inputs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True, init=False)
class Batch:
    """One non-empty logical batch of input rows.

    Row mappings are copied when the batch is created and exposed as read-only
    mappings. Values inside a row are not recursively copied or frozen.
    """

    rows: tuple[Mapping[str, object], ...]

    def __init__(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Create a batch from rows while preserving their insertion order."""
        snapshots = tuple(MappingProxyType(dict(row)) for row in rows)
        if not snapshots:
            raise ValueError("Batch must contain at least one row.")

        object.__setattr__(self, "rows", snapshots)


def batch(*rows: Mapping[str, object]) -> Batch:
    """Create a :class:`Batch` from one or more input rows."""
    return Batch(rows)
