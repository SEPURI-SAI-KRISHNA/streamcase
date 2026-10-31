"""Public errors for bounded scenario execution."""

from __future__ import annotations

from typing import Literal

LimitResource = Literal["output_rows", "input_bytes_per_batch", "input_bytes_per_run"]


class ScenarioLimitError(RuntimeError):
    """A scenario exceeded a configured capture or input allowance."""

    def __init__(
        self,
        *,
        action_index: int,
        resource: LimitResource,
        limit: int,
        observed_at_least: int,
        batch_id: int | None = None,
    ) -> None:
        self.action_index = action_index
        self.resource = resource
        self.limit = limit
        self.observed_at_least = observed_at_least
        self.batch_id = batch_id
        context = f"Batch action at index {action_index}"
        if batch_id is not None:
            context += f" (Spark batch {batch_id})"
        super().__init__(
            f"{context} exceeded {resource} limit {limit}; observed at least {observed_at_least}."
        )


def _validate_positive_limit(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be a positive integer or None.")
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer or None.")
    return value
