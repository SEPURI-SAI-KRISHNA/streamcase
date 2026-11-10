"""Internal serialization helpers for scenario actions."""

from __future__ import annotations

import json
from collections.abc import Iterator

from streamcase.actions import Batch


def _iter_batch_json_lines(action: Batch) -> Iterator[tuple[int, str]]:
    """Encode each row independently, preserving JSON Lines row context."""
    for row_index, row in enumerate(action.rows):
        try:
            encoded = json.dumps(
                dict(row),
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        except TypeError as error:
            message = f"Batch row at index {row_index} could not be encoded as JSON: {error}"
            raise TypeError(message) from error
        except ValueError as error:
            message = f"Batch row at index {row_index} could not be encoded as JSON: {error}"
            raise ValueError(message) from error

        yield row_index, encoded + "\n"


def _encode_batch_json_lines(action: Batch) -> str:
    """Encode a batch as deterministic, newline-terminated JSON Lines text."""
    return "".join(line for _, line in _iter_batch_json_lines(action))
