"""Private execution of Spark streaming scenario actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from streamcase._cleanup import _cleanup_on_exit, _record_cleanup_failure
from streamcase._directories import RunDirectories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_lifecycle import _QueryLifecycle
from streamcase._spark_progress import _capture_action_progress
from streamcase.actions import Batch, ExpectFailure, Restart
from streamcase.failures import ExpectedFailureNotObservedError, FailureObservation
from streamcase.limits import ScenarioLimitError
from streamcase.progress import CapturedProgress
from streamcase.results import ScenarioResult
from streamcase.scenario import Scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _process_batch_action(
    index: int,
    action: Batch,
    input_writer: AtomicBatchWriter,
    lifecycle: _QueryLifecycle,
    capture: _BatchCapture,
) -> tuple[CapturedProgress, ...]:
    capture.set_action_index(index)
    try:
        query = lifecycle.require_active()
        progress_before = tuple(query.recentProgress)
        captured_before = len(capture.snapshot())
        input_writer.publish(action, action_index=index)
        query.processAllAvailable()
        lifecycle.require_active()
        return _capture_action_progress(
            index,
            capture.snapshot()[captured_before:],
            progress_before,
            query.recentProgress,
            str(query.runId),
        )
    except Exception as error:
        if isinstance(error, ScenarioLimitError):
            raise
        limit_error = capture.limit_error()
        if limit_error is not None:
            raise limit_error from error
        raise RuntimeError(f"Batch action at index {index} failed: {error}") from error


def _query_condition(error: Exception) -> str | None:
    """Read only the top-level Spark condition, without traversing causes."""
    reader = getattr(error, "getCondition", None)
    if reader is None:
        reader = getattr(error, "getErrorClass", None)
    if reader is None:
        return None
    if not callable(reader):
        raise TypeError("Query failure condition accessor is not callable.")
    condition = reader()
    if condition is not None and not isinstance(condition, str):
        raise TypeError("Query failure condition must be a string or None.")
    return condition


def _process_expected_failure_action(
    index: int,
    action: ExpectFailure,
    input_writer: AtomicBatchWriter,
    lifecycle: _QueryLifecycle,
    capture: _BatchCapture,
) -> FailureObservation:
    """Accept only a selected failure of this runner's query at this action."""
    capture.set_action_index(index)
    try:
        query = lifecycle.require_active()
        input_writer.publish(action.batch, action_index=index)
    except Exception as error:
        if isinstance(error, ScenarioLimitError):
            raise
        raise RuntimeError(
            f"ExpectFailure action at index {index} failed before query processing: {error}"
        ) from error

    processing_error: Exception | None = None
    try:
        query.processAllAvailable()
    except Exception as error:
        processing_error = error

    limit_error = capture.limit_error()
    if limit_error is not None:
        if processing_error is not None:
            raise limit_error from processing_error
        raise limit_error

    callback_failure = capture.callback_failure()
    if callback_failure is not None:
        failed_index, phase, callback_error = callback_failure
        if failed_index != index or phase != "evaluation":
            raise RuntimeError(
                f"ExpectFailure action at index {index} failed inside Streamcase capture."
            ) from callback_error

    try:
        active = query.isActive
        query_error = None if active else query.exception()
    except Exception as error:
        raise RuntimeError(
            f"Could not inspect the owned query after ExpectFailure action at index {index}."
        ) from error

    if active:
        if processing_error is not None:
            raise RuntimeError(
                f"ExpectFailure action at index {index} failed while its query stayed active."
            ) from processing_error
        raise ExpectedFailureNotObservedError(index)
    if query_error is None:
        if processing_error is not None:
            raise RuntimeError(
                f"ExpectFailure action at index {index} failed without a query exception."
            ) from processing_error
        raise RuntimeError(
            f"ExpectFailure action at index {index} stopped without a query exception."
        )
    if not isinstance(query_error, Exception):
        raise TypeError("Owned query returned a non-exception failure object.")

    try:
        message = str(query_error)
        condition = _query_condition(query_error)
    except Exception as error:
        raise RuntimeError(
            f"Could not read the owned query failure at ExpectFailure action index {index}."
        ) from error

    message_matches = action.message_contains is None or action.message_contains in message
    condition_matches = action.condition is None or action.condition == condition
    if not message_matches or not condition_matches:
        cause = processing_error if processing_error is not None else query_error
        raise RuntimeError(
            f"Query failure at ExpectFailure action index {index} did not match its selectors."
        ) from cause

    return FailureObservation(index, message=message, condition=condition)


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
    max_input_bytes_per_batch: int | None = 8_388_608,
    max_input_bytes_per_run: int | None = 67_108_864,
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
        capture.set_action_index(0)
        lifecycle.start(stream, action_index=0)
        input_writer = AtomicBatchWriter(
            directories,
            max_input_bytes_per_batch=max_input_bytes_per_batch,
            max_input_bytes_per_run=max_input_bytes_per_run,
        )
        progress: list[CapturedProgress] = []
        failure: FailureObservation | None = None
        for index, action in enumerate(scenario.actions):
            if isinstance(action, Batch):
                progress.extend(
                    _process_batch_action(index, action, input_writer, lifecycle, capture)
                )
            elif isinstance(action, Restart):
                assert rebuild_stream is not None
                capture.set_action_index(index + 1)
                _process_restart_action(index, rebuild_stream, lifecycle)
            else:
                failure = _process_expected_failure_action(
                    index, action, input_writer, lifecycle, capture
                )
    return ScenarioResult(capture.snapshot(), progress=progress, failure=failure)
