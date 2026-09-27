"""Internal serialization helpers for scenario actions."""

from __future__ import annotations

import json

from streamcase.actions import Batch


def _encode_batch_json_lines(action: Batch) -> str:
    """Encode a batch as deterministic, newline-terminated JSON Lines text."""
    lines = (
        json.dumps(
            dict(row),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for row in action.rows
    )
    return "\n".join(lines) + "\n"
