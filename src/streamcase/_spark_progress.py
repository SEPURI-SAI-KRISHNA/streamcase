"""Associate callback-backed batches with newly observed Spark progress."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from streamcase.progress import CapturedProgress
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


def _capture_action_progress(
    action_index: int,
    callback_batches: Iterable[CapturedBatch],
    before: Iterable[Mapping[str, object]],
    after: Iterable[Mapping[str, object]],
    run_id: str,
) -> tuple[CapturedProgress, ...]:
    """Snapshot unambiguous new reports for this action's callback batches."""
    old_keys = {_report_key(report) for report in before}
    new_keys = [
        key for report in after if (key := _report_key(report)) not in old_keys and key[0] == run_id
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
        matches = [key for key in new_keys if key[1] == batch.batch_id]
        if len(matches) > 1:
            raise RuntimeError(
                f"Batch action at index {action_index} has multiple progress reports "
                f"for callback batch ID {batch.batch_id}."
            )
        if matches:
            captured.append(
                CapturedProgress(
                    action_index=action_index,
                    trigger_index=trigger_index,
                    batch_id=batch.batch_id,
                    run_id=run_id,
                )
            )
    return tuple(captured)
