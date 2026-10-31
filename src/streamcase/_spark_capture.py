"""Private driver-side capture for Spark ``foreachBatch`` callbacks."""

from __future__ import annotations

from _thread import LockType
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

from streamcase.limits import ScenarioLimitError, _validate_positive_limit
from streamcase.results import CapturedBatch

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(slots=True)
class _BatchCapture:
    """Collect complete immutable output batches in callback order."""

    max_output_rows: int | None = None
    _batches: list[CapturedBatch] = field(default_factory=list, init=False, repr=False)
    _lock: LockType = field(default_factory=Lock, init=False, repr=False)
    _captured_rows: int = field(default=0, init=False, repr=False)
    _action_index: int = field(default=0, init=False, repr=False)
    _limit_error: ScenarioLimitError | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.max_output_rows = _validate_positive_limit("max_output_rows", self.max_output_rows)

    def set_action_index(self, index: int) -> None:
        """Identify the action whose query callback is about to run."""
        with self._lock:
            self._action_index = index

    def limit_error(self) -> ScenarioLimitError | None:
        """Return a callback overflow even if Spark wraps its exception."""
        with self._lock:
            return self._limit_error

    def callback(self, dataframe: DataFrame, batch_id: int) -> None:
        """Collect and freeze one Spark output micro-batch on the driver."""
        with self._lock:
            if self.max_output_rows is None:
                collected = dataframe.collect()
            else:
                remaining = self.max_output_rows - self._captured_rows
                collected = dataframe.limit(remaining + 1).collect()
                if len(collected) > remaining:
                    error = ScenarioLimitError(
                        action_index=self._action_index,
                        resource="output_rows",
                        limit=self.max_output_rows,
                        observed_at_least=self._captured_rows + len(collected),
                        batch_id=batch_id,
                    )
                    self._limit_error = error
                    raise error
            rows = (row.asDict(recursive=True) for row in collected)
            captured = CapturedBatch(batch_id, rows)
            self._batches.append(captured)
            self._captured_rows += len(collected)

    def snapshot(self) -> tuple[CapturedBatch, ...]:
        """Return an immutable snapshot after any active callback completes."""
        with self._lock:
            return tuple(self._batches)
