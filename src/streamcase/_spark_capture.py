"""Private driver-side capture for Spark ``foreachBatch`` callbacks."""

from __future__ import annotations

from _thread import LockType
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

from streamcase.results import CapturedBatch

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(slots=True)
class _BatchCapture:
    """Collect complete immutable output batches in callback order."""

    _batches: list[CapturedBatch] = field(default_factory=list, init=False, repr=False)
    _lock: LockType = field(default_factory=Lock, init=False, repr=False)

    def callback(self, dataframe: DataFrame, batch_id: int) -> None:
        """Collect and freeze one Spark output micro-batch on the driver."""
        with self._lock:
            rows = (row.asDict(recursive=True) for row in dataframe.collect())
            captured = CapturedBatch(batch_id, rows)
            self._batches.append(captured)

    def snapshot(self) -> tuple[CapturedBatch, ...]:
        """Return an immutable snapshot after any active callback completes."""
        with self._lock:
            return tuple(self._batches)
