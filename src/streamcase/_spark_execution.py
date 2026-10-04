"""Private execution of batch-only Spark streaming scenarios."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from streamcase._cleanup import _cleanup_on_exit
from streamcase._directories import RunDirectories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_lifecycle import _QueryLifecycle
from streamcase.actions import Batch
from streamcase.results import ScenarioResult
from streamcase.scenario import Scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _process_batch_action(
    index: int,
    action: Batch,
    input_writer: AtomicBatchWriter,
    lifecycle: _QueryLifecycle,
) -> None:
    try:
        query = lifecycle.require_active()
        input_writer.publish(action)
        query.processAllAvailable()
        lifecycle.require_active()
    except Exception as error:
        raise RuntimeError(f"Batch action at index {index} failed: {error}") from error


def _require_batch_only(scenario: Scenario) -> None:
    for index, action in enumerate(scenario.actions):
        if not isinstance(action, Batch):
            raise ValueError(f"Scenario action at index {index} is not a Batch.")


def _execute_batches(
    stream: DataFrame,
    scenario: Scenario,
    directories: RunDirectories,
    capture: _BatchCapture,
    *,
    output_mode: str = "append",
    query_options: Mapping[str, str] | None = None,
) -> ScenarioResult:
    """Process each input batch and return an immutable output snapshot."""
    _require_batch_only(scenario)
    lifecycle = _QueryLifecycle(
        directories,
        capture,
        output_mode=output_mode,
        query_options=query_options,
    )
    lifecycle.start(stream, action_index=0)

    with _cleanup_on_exit("streaming query", lifecycle.stop):
        input_writer = AtomicBatchWriter(directories)
        for index, action in enumerate(scenario.actions):
            _process_batch_action(index, cast(Batch, action), input_writer, lifecycle)
    return ScenarioResult(capture.snapshot())
