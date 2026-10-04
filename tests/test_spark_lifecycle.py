from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest

from streamcase._directories import RunDirectories, create_run_directories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_lifecycle import _QueryLifecycle


@pytest.fixture
def run_directories(tmp_path: Path) -> Iterator[RunDirectories]:
    directories = create_run_directories(base_dir=tmp_path)
    try:
        yield directories
    finally:
        directories.cleanup()


def _fake_stream(query: Mock) -> tuple[Mock, Mock]:
    writer = Mock()
    writer.foreachBatch.return_value = writer
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.queryName.return_value = writer
    writer.start.return_value = query
    stream = Mock(writeStream=writer)
    stream.sparkSession.streams.active = []
    return stream, writer


def test_stop_is_safe_before_start_and_repeated_after_success(
    run_directories: RunDirectories,
) -> None:
    query = Mock(isActive=True)
    stream, writer = _fake_stream(query)
    lifecycle = _QueryLifecycle(run_directories, _BatchCapture())

    lifecycle.stop()
    with pytest.raises(RuntimeError, match="No streaming query is owned"):
        lifecycle.require_active()

    lifecycle.start(cast(Any, stream), action_index=0)
    assert lifecycle.require_active() is query
    lifecycle.stop()
    lifecycle.stop()

    query.stop.assert_called_once_with()
    writer.start.assert_called_once_with()
    stream.sparkSession.stop.assert_not_called()
    with pytest.raises(RuntimeError, match="No streaming query is owned"):
        lifecycle.require_active()


def test_start_requires_a_successful_stop_before_another_start(
    run_directories: RunDirectories,
) -> None:
    first_query = Mock(isActive=True)
    first_stream, _first_writer = _fake_stream(first_query)
    second_stream, second_writer = _fake_stream(Mock(isActive=True))
    lifecycle = _QueryLifecycle(run_directories, _BatchCapture())

    lifecycle.start(cast(Any, first_stream), action_index=0)
    with pytest.raises(RuntimeError, match="Stop the owned streaming query"):
        lifecycle.start(cast(Any, second_stream), action_index=2)

    second_writer.start.assert_not_called()
    first_query.stop.assert_not_called()
    lifecycle.stop()


def test_stopped_controller_reuses_checkpoint_and_writer_configuration(
    run_directories: RunDirectories,
) -> None:
    capture = _BatchCapture()
    options = {"customOption": "original"}
    first_query = Mock(isActive=True)
    first_stream, first_writer = _fake_stream(first_query)
    second_query = Mock(isActive=True)
    second_stream, second_writer = _fake_stream(second_query)
    lifecycle = _QueryLifecycle(
        run_directories,
        capture,
        output_mode="update",
        query_options=options,
    )
    options["customOption"] = "changed"

    lifecycle.start(cast(Any, first_stream), action_index=0)
    lifecycle.stop()
    lifecycle.start(cast(Any, second_stream), action_index=2)

    for writer in (first_writer, second_writer):
        writer.foreachBatch.assert_called_once_with(capture.callback)
        writer.outputMode.assert_called_once_with("update")
        writer.option.assert_any_call("customOption", "original")
        writer.option.assert_any_call("checkpointLocation", str(run_directories.checkpoint_dir))
        writer.queryName.assert_called_once_with(run_directories.root.name)
        writer.start.assert_called_once_with()
    assert lifecycle.require_active() is second_query
    assert first_query.stop.call_count == 1
    lifecycle.stop()
    assert second_query.stop.call_count == 1


def test_terminated_query_is_not_reported_as_active_but_is_stopped(
    run_directories: RunDirectories,
) -> None:
    query = Mock(isActive=False)
    stream, _writer = _fake_stream(query)
    lifecycle = _QueryLifecycle(run_directories, _BatchCapture())
    lifecycle.start(cast(Any, stream), action_index=0)

    with pytest.raises(RuntimeError, match="streaming query terminated"):
        lifecycle.require_active()

    lifecycle.stop()
    query.stop.assert_called_once_with()


def test_failed_stop_keeps_ownership_until_a_retry_succeeds(
    run_directories: RunDirectories,
) -> None:
    query = Mock(isActive=True)
    query.stop.side_effect = [OSError("injected stop failure"), None]
    stream, _writer = _fake_stream(query)
    other_stream, other_writer = _fake_stream(Mock(isActive=True))
    lifecycle = _QueryLifecycle(run_directories, _BatchCapture())
    lifecycle.start(cast(Any, stream), action_index=0)

    with pytest.raises(OSError, match="injected stop failure"):
        lifecycle.stop()
    with pytest.raises(RuntimeError, match="Stop the owned streaming query"):
        lifecycle.start(cast(Any, other_stream), action_index=2)
    other_writer.start.assert_not_called()

    lifecycle.stop()
    assert query.stop.call_count == 2
    lifecycle.start(cast(Any, other_stream), action_index=2)
    other_writer.start.assert_called_once_with()
    lifecycle.stop()


def test_failed_later_start_only_stops_the_query_named_for_this_run(
    run_directories: RunDirectories,
) -> None:
    owned_query = Mock(isActive=True)
    owned_query.name = run_directories.root.name
    caller_query = Mock(isActive=True)
    caller_query.name = "caller-query"
    stream, writer = _fake_stream(owned_query)
    stream.sparkSession.streams.active = [caller_query, owned_query]
    writer.start.side_effect = OSError("injected restart start failure")
    lifecycle = _QueryLifecycle(run_directories, _BatchCapture())

    with pytest.raises(RuntimeError, match="before Batch action at index 2") as error_info:
        lifecycle.start(cast(Any, stream), action_index=2)

    assert isinstance(error_info.value.__cause__, OSError)
    owned_query.stop.assert_called_once_with()
    caller_query.stop.assert_not_called()
    stream.sparkSession.stop.assert_not_called()
