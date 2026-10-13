"""Associate callback-backed batches with newly observed Spark progress."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime

from streamcase.progress import CapturedProgress, CapturedStateOperator
from streamcase.results import CapturedBatch


def _report_key(report: Mapping[str, object]) -> tuple[str, int, str]:
    """Identify one progress update without retaining its Spark-owned payload."""
    run_id = report["runId"]
    batch_id = report["batchId"]
    timestamp = report["timestamp"]
    if run_id is None or not str(run_id).strip():
        raise ValueError("Spark progress runId must be present.")
    if isinstance(batch_id, bool) or not isinstance(batch_id, int):
        raise TypeError("Spark progress batchId must be an int.")
    if not isinstance(timestamp, str) or not timestamp:
        raise TypeError("Spark progress timestamp must be a non-empty str.")
    return str(run_id), batch_id, timestamp


def _optional_count(report: Mapping[str, object], field: str) -> int | None:
    """Preserve field absence while rejecting malformed reported counts."""
    value = report.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Spark progress {field} must be an int or None.")
    if value < 0:
        raise ValueError(f"Spark progress {field} must be non-negative.")
    return value


def _watermark(report: Mapping[str, object]) -> datetime | None:
    event_time = report.get("eventTime")
    if event_time is None:
        return None
    if not isinstance(event_time, Mapping):
        raise TypeError("Spark progress eventTime must be a mapping or None.")
    value = event_time.get("watermark")
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("Spark progress eventTime.watermark must be an ISO8601 string.")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError("Spark progress eventTime.watermark must be ISO8601.") from error
    if parsed.utcoffset() is None:
        raise ValueError("Spark progress eventTime.watermark must include a timezone.")
    return parsed


def _state_operators(report: Mapping[str, object]) -> tuple[CapturedStateOperator, ...] | None:
    raw_operators = report.get("stateOperators")
    if raw_operators is None:
        return None
    if not isinstance(raw_operators, (list, tuple)):
        raise TypeError("Spark progress stateOperators must be a sequence or None.")

    operators: list[CapturedStateOperator] = []
    for index, raw_operator in enumerate(raw_operators):
        if not isinstance(raw_operator, Mapping):
            raise TypeError(f"Spark progress stateOperators[{index}] must be a mapping.")
        name = raw_operator.get("operatorName")
        if name is not None and not isinstance(name, str):
            raise TypeError(f"Spark progress stateOperators[{index}].operatorName must be a str.")
        operators.append(
            CapturedStateOperator(
                operator_name=name,
                num_rows_total=_optional_count(raw_operator, "numRowsTotal"),
                num_rows_updated=_optional_count(raw_operator, "numRowsUpdated"),
                num_rows_removed=_optional_count(raw_operator, "numRowsRemoved"),
                num_rows_dropped_by_watermark=_optional_count(
                    raw_operator, "numRowsDroppedByWatermark"
                ),
                memory_used_bytes=_optional_count(raw_operator, "memoryUsedBytes"),
            )
        )
    return tuple(operators)


def _normalize_progress(
    action_index: int,
    trigger_index: int,
    batch_id: int,
    run_id: str,
    report: Mapping[str, object],
) -> CapturedProgress:
    """Copy only approved Spark fields into a backend-independent snapshot."""
    return CapturedProgress(
        action_index=action_index,
        trigger_index=trigger_index,
        batch_id=batch_id,
        run_id=run_id,
        num_input_rows=_optional_count(report, "numInputRows"),
        watermark=_watermark(report),
        state_operators=_state_operators(report),
    )


def _capture_action_progress(
    action_index: int,
    callback_batches: Iterable[CapturedBatch],
    before: Iterable[Mapping[str, object]],
    after: Iterable[Mapping[str, object]],
    run_id: str,
) -> tuple[CapturedProgress, ...]:
    """Snapshot unambiguous new reports for this action's callback batches."""
    old_keys = {_report_key(report) for report in before}
    new_reports = [
        (key, report)
        for report in after
        if (key := _report_key(report)) not in old_keys and key[0] == run_id
    ]
    captured: list[CapturedProgress] = []
    seen_batch_ids: set[int] = set()
    for trigger_index, batch in enumerate(callback_batches):
        if batch.batch_id in seen_batch_ids:
            raise RuntimeError(
                f"Batch action at index {action_index} has repeated callback batch ID "
                f"{batch.batch_id}; progress matching is ambiguous."
            )
        seen_batch_ids.add(batch.batch_id)
        matches = [report for key, report in new_reports if key[1] == batch.batch_id]
        if len(matches) > 1:
            # Spark can publish a no-data update with the same batch ID. The
            # scenario Batch has input rows, so a single positive-input report
            # identifies its data trigger without assigning the idle update.
            data_matches = [
                report
                for report in matches
                if isinstance((count := report.get("numInputRows")), int)
                and not isinstance(count, bool)
                and count > 0
            ]
            if len(data_matches) != 1:
                raise RuntimeError(
                    f"Batch action at index {action_index} has multiple progress reports "
                    f"for callback batch ID {batch.batch_id}."
                )
            matches = data_matches
        if matches:
            captured.append(
                _normalize_progress(
                    action_index,
                    trigger_index,
                    batch.batch_id,
                    run_id,
                    matches[0],
                )
            )
    return tuple(captured)
