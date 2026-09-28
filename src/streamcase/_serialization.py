"""Internal serialization helpers for scenario actions."""

from __future__ import annotations

import json

from streamcase.actions import Batch


def _encode_batch_json_lines(action: Batch) -> str:
    """Encode a batch as deterministic, newline-terminated JSON Lines text."""
    lines: list[str] = []
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

        lines.append(encoded)

    return "\n".join(lines) + "\n"
