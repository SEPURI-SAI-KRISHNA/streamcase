"""Private atomic input-file publication for scenario batches."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from streamcase._directories import RunDirectories, _require_direct_child
from streamcase._serialization import _encode_batch_json_lines
from streamcase.actions import Batch

_BATCH_INDEX_WIDTH = 20


def _batch_file_name(index: int) -> str:
    return f"batch-{index:0{_BATCH_INDEX_WIDTH}d}.json"


@dataclass(slots=True)
class AtomicBatchWriter:
    """Publish encoded batches sequentially into one isolated input directory."""

    directories: RunDirectories
    _next_index: int = field(default=0, init=False, repr=False)

    def publish(self, action: Batch) -> Path:
        """Atomically publish one batch and return its final input path."""
        encoded = _encode_batch_json_lines(action)
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
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                temporary_created = True
                handle.write(encoded)
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
        return destination
