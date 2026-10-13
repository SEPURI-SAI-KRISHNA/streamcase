"""Immutable, backend-independent streaming progress snapshots."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone


def _non_negative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int; got {type(value).__name__}.")
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")
    return value


def _optional_non_negative_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, name)


@dataclass(frozen=True, slots=True, init=False)
class CapturedStateOperator:
    """Optional metrics for one state operator in one captured trigger."""

    operator_name: str | None
    num_rows_total: int | None
    num_rows_updated: int | None
    num_rows_removed: int | None
    num_rows_dropped_by_watermark: int | None
    memory_used_bytes: int | None

    def __init__(
        self,
        operator_name: str | None = None,
        num_rows_total: int | None = None,
        num_rows_updated: int | None = None,
        num_rows_removed: int | None = None,
        num_rows_dropped_by_watermark: int | None = None,
        memory_used_bytes: int | None = None,
    ) -> None:
        """Validate and freeze the documented subset of state-operator metrics."""
        if operator_name is not None:
            if not isinstance(operator_name, str):
                raise TypeError(
                    f"operator_name must be a str or None; got {type(operator_name).__name__}."
                )
            if not operator_name.strip():
                raise ValueError("operator_name must not be empty or whitespace.")

        object.__setattr__(self, "operator_name", operator_name)
        object.__setattr__(
            self, "num_rows_total", _optional_non_negative_int(num_rows_total, "num_rows_total")
        )
        object.__setattr__(
            self,
            "num_rows_updated",
            _optional_non_negative_int(num_rows_updated, "num_rows_updated"),
        )
        object.__setattr__(
            self,
            "num_rows_removed",
            _optional_non_negative_int(num_rows_removed, "num_rows_removed"),
        )
        object.__setattr__(
            self,
            "num_rows_dropped_by_watermark",
            _optional_non_negative_int(
                num_rows_dropped_by_watermark, "num_rows_dropped_by_watermark"
            ),
        )
        object.__setattr__(
            self,
            "memory_used_bytes",
            _optional_non_negative_int(memory_used_bytes, "memory_used_bytes"),
        )


@dataclass(frozen=True, slots=True, init=False)
class CapturedProgress:
    """One immutable progress report associated with a scenario batch action."""

    action_index: int
    trigger_index: int
    batch_id: int
    run_id: str
    num_input_rows: int | None
    watermark: datetime | None
    state_operators: tuple[CapturedStateOperator, ...] | None

    def __init__(
        self,
        action_index: int,
        trigger_index: int,
        batch_id: int,
        run_id: str,
        num_input_rows: int | None = None,
        watermark: datetime | None = None,
        state_operators: Iterable[CapturedStateOperator] | None = None,
    ) -> None:
        """Validate identifiers and snapshot optional progress values."""
        validated_action_index = _non_negative_int(action_index, "action_index")
        validated_trigger_index = _non_negative_int(trigger_index, "trigger_index")
        validated_batch_id = _non_negative_int(batch_id, "batch_id")

        if not isinstance(run_id, str):
            raise TypeError(f"run_id must be a str; got {type(run_id).__name__}.")
        if not run_id.strip():
            raise ValueError("run_id must not be empty or whitespace.")

        if watermark is not None:
            if not isinstance(watermark, datetime):
                raise TypeError(
                    f"watermark must be a datetime or None; got {type(watermark).__name__}."
                )
            if watermark.tzinfo is None or watermark.utcoffset() is None:
                raise ValueError("watermark must be timezone-aware.")
            watermark = watermark.astimezone(timezone.utc)

        if state_operators is None:
            operators = None
        else:
            operators = tuple(state_operators)
            for index, operator in enumerate(operators):
                if not isinstance(operator, CapturedStateOperator):
                    raise TypeError(
                        "state_operators entry at index "
                        f"{index} must be a CapturedStateOperator; got {type(operator).__name__}."
                    )

        object.__setattr__(self, "action_index", validated_action_index)
        object.__setattr__(self, "trigger_index", validated_trigger_index)
        object.__setattr__(self, "batch_id", validated_batch_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(
            self, "num_input_rows", _optional_non_negative_int(num_input_rows, "num_input_rows")
        )
        object.__setattr__(self, "watermark", watermark)
        object.__setattr__(self, "state_operators", operators)
