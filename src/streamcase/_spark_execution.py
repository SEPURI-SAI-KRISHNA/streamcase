"""Private execution of Spark streaming scenario actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from streamcase._cleanup import _cleanup_on_exit, _record_cleanup_failure
from streamcase._directories import RunDirectories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_lifecycle import _QueryLifecycle
from streamcase.actions import Batch, Restart
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


def _process_restart_action(
    index: int,
    rebuild_stream: Callable[[], DataFrame],
    lifecycle: _QueryLifecycle,
) -> None:
    try:
        lifecycle.require_active()
        lifecycle.stop()
    except Exception as error:
        raise _restart_failure(index, "stopping the active query", error) from error

    try:
        replacement_stream = rebuild_stream()
    except Exception as error:
        raise _restart_failure(index, "rebuilding the stream", error) from error

    try:
        lifecycle.start(replacement_stream, action_index=index + 1)
        lifecycle.require_active()
    except Exception as error:
        raise _restart_failure(index, "starting the replacement query", error) from error


def _restart_failure(index: int, transition: str, error: Exception) -> RuntimeError:
    failure = RuntimeError(f"Restart action at index {index} failed while {transition}: {error}")
    for resource, cleanup_error in getattr(error, "_streamcase_cleanup_failures", ()):
        _record_cleanup_failure(failure, resource, cleanup_error)
    return failure


def _execute_batches(
    stream: DataFrame,
    scenario: Scenario,
    directories: RunDirectories,
    capture: _BatchCapture,
    *,
    output_mode: str = "append",
    query_options: Mapping[str, str] | None = None,
    rebuild_stream: Callable[[], DataFrame] | None = None,
) -> ScenarioResult:
    """Execute each action and return an immutable output snapshot."""
    if rebuild_stream is None and any(isinstance(action, Restart) for action in scenario.actions):
        raise ValueError("Restart actions require a stream rebuild callback.")

    lifecycle = _QueryLifecycle(
        directories,
        capture,
        output_mode=output_mode,
        query_options=query_options,
    )
    with _cleanup_on_exit("streaming query", lifecycle.stop):
        lifecycle.start(stream, action_index=0)
        input_writer = AtomicBatchWriter(directories)
        for index, action in enumerate(scenario.actions):
            if isinstance(action, Batch):
                _process_batch_action(index, action, input_writer, lifecycle)
            else:
                assert rebuild_stream is not None
                _process_restart_action(index, rebuild_stream, lifecycle)
    return ScenarioResult(capture.snapshot())
