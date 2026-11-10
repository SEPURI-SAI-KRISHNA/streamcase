"""Private atomic input-file publication for scenario batches."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from streamcase._directories import RunDirectories, _require_direct_child
from streamcase._serialization import _iter_batch_json_lines
from streamcase.actions import Batch
from streamcase.limits import ScenarioLimitError, _validate_positive_limit

_BATCH_INDEX_WIDTH = 20


def _batch_file_name(index: int) -> str:
    return f"batch-{index:0{_BATCH_INDEX_WIDTH}d}.json"


@dataclass(slots=True)
class AtomicBatchWriter:
    """Publish encoded batches sequentially into one isolated input directory."""

    directories: RunDirectories
    max_input_bytes_per_batch: int | None = 8_388_608
    max_input_bytes_per_run: int | None = 67_108_864
    _next_index: int = field(default=0, init=False, repr=False)
    _published_bytes: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.max_input_bytes_per_batch = _validate_positive_limit(
            "max_input_bytes_per_batch", self.max_input_bytes_per_batch
        )
        self.max_input_bytes_per_run = _validate_positive_limit(
            "max_input_bytes_per_run", self.max_input_bytes_per_run
        )

    def publish(self, action: Batch, *, action_index: int = 0) -> Path:
        """Atomically publish one batch and return its final input path."""
        file_name = _batch_file_name(self._next_index)
        destination = self.directories.input_dir / file_name
        temporary = self.directories.input_dir / f".{file_name}.tmp"

        _require_direct_child(destination, self.directories.input_dir, "batch input file")
        _require_direct_child(temporary, self.directories.input_dir, "temporary batch file")

        if destination.exists():
            raise FileExistsError(f"Batch input destination already exists: {destination}")
        if temporary.exists():
            raise FileExistsError(f"Temporary batch input file already exists: {temporary}")

        temporary_created = False
        batch_bytes = 0
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                temporary_created = True
                for row_index, line in _iter_batch_json_lines(action):
                    try:
                        line_bytes = len(line.encode("utf-8"))
                    except UnicodeEncodeError as error:
                        message = (
                            f"Batch row at index {row_index} could not be encoded as UTF-8: {error}"
                        )
                        raise ValueError(message) from error

                    next_batch_bytes = batch_bytes + line_bytes
                    next_run_bytes = self._published_bytes + next_batch_bytes
                    if (
                        self.max_input_bytes_per_batch is not None
                        and next_batch_bytes > self.max_input_bytes_per_batch
                    ):
                        raise ScenarioLimitError(
                            action_index=action_index,
                            resource="input_bytes_per_batch",
                            limit=self.max_input_bytes_per_batch,
                            observed_at_least=next_batch_bytes,
                            row_index=row_index,
                        )
                    if (
                        self.max_input_bytes_per_run is not None
                        and next_run_bytes > self.max_input_bytes_per_run
                    ):
                        raise ScenarioLimitError(
                            action_index=action_index,
                            resource="input_bytes_per_run",
                            limit=self.max_input_bytes_per_run,
                            observed_at_least=next_run_bytes,
                            row_index=row_index,
                        )
                    handle.write(line)
                    batch_bytes = next_batch_bytes
                handle.flush()
                os.fsync(handle.fileno())

            if destination.exists():
                raise FileExistsError(f"Batch input destination already exists: {destination}")
            temporary.rename(destination)
        except BaseException:
            if temporary_created:
                temporary.unlink(missing_ok=True)
            raise

        self._next_index += 1
        self._published_bytes += batch_bytes
        return destination
