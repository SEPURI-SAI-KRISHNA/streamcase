"""Private execution of batch-only Spark streaming scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from streamcase._directories import RunDirectories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_capture import _BatchCapture
from streamcase.actions import Batch
from streamcase.scenario import Scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery


def _process_batch_action(
    index: int,
    action: Batch,
    input_writer: AtomicBatchWriter,
    query: StreamingQuery,
) -> None:
    try:
        if not query.isActive:
            raise RuntimeError("The streaming query terminated.")
        input_writer.publish(action)
        query.processAllAvailable()
        if not query.isActive:
            raise RuntimeError("The streaming query terminated.")
    except Exception as error:
        raise RuntimeError(f"Batch action at index {index} failed: {error}") from error


def _execute_batches(
    stream: DataFrame,
    scenario: Scenario,
    directories: RunDirectories,
    capture: _BatchCapture,
) -> None:
    """Publish and process each input batch before advancing to the next one."""
    for index, action in enumerate(scenario.actions):
        if not isinstance(action, Batch):
            raise ValueError(f"Scenario action at index {index} is not a Batch.")

    writer = (
        stream.writeStream.foreachBatch(capture.callback)
        .outputMode("append")
        .option("checkpointLocation", str(directories.checkpoint_dir))
        .queryName(directories.root.name)
    )
    try:
        query = writer.start()
    except Exception as error:
        raise RuntimeError("Could not start the query before Batch action at index 0.") from error

    input_writer = AtomicBatchWriter(directories)
    try:
        for index, action in enumerate(scenario.actions):
            _process_batch_action(index, cast(Batch, action), input_writer, query)
    finally:
        query.stop()
