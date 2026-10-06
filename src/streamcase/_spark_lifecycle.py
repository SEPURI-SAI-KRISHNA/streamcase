"""Private ownership and transitions for one Spark streaming query at a time."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from streamcase._cleanup import _cleanup_on_exit
from streamcase._directories import RunDirectories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_query_options import _prepare_query_configuration

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery


def _stop_named_query(stream: DataFrame, name: str) -> None:
    for active_query in stream.sparkSession.streams.active:
        if active_query.name == name:
            active_query.stop()


class _QueryLifecycle:
    """Keep runner-owned query transitions and writer settings in one place."""

    def __init__(
        self,
        directories: RunDirectories,
        capture: _BatchCapture,
        *,
        output_mode: str = "append",
        query_options: Mapping[str, str] | None = None,
    ) -> None:
        self._directories = directories
        self._capture = capture
        self._output_mode, self._query_options = _prepare_query_configuration(
            output_mode, query_options
        )
        self._query: StreamingQuery | None = None
        self._pending_start_stream: DataFrame | None = None

    def require_active(self) -> StreamingQuery:
        """Return the owned active query, rejecting missing or terminated state."""
        if self._query is None:
            raise RuntimeError("No streaming query is owned by this run.")
        if not self._query.isActive:
            raise RuntimeError("The streaming query terminated.")
        return self._query

    def start(self, stream: DataFrame, *, action_index: int) -> None:
        """Start a query only when the previous one was successfully stopped."""
        if self._query is not None or self._pending_start_stream is not None:
            raise RuntimeError("Stop the owned streaming query before starting another.")

        writer = stream.writeStream
        for name, value in self._query_options.items():
            writer = writer.option(name, value)
        writer = (
            writer.foreachBatch(self._capture.callback)
            .outputMode(self._output_mode)
            .option("checkpointLocation", str(self._directories.checkpoint_dir))
            .queryName(self._directories.root.name)
        )
        try:
            self._query = writer.start()
        except Exception as error:
            self._pending_start_stream = stream
            with _cleanup_on_exit("streaming query", self.stop):
                raise RuntimeError(
                    f"Could not start the query before Batch action at index {action_index}."
                ) from error

    def stop(self) -> None:
        """Stop once; repeat calls are safe after success or before first start."""
        if self._query is not None:
            self._query.stop()
            self._query = None
        if self._pending_start_stream is not None:
            _stop_named_query(self._pending_start_stream, self._directories.root.name)
            self._pending_start_stream = None
