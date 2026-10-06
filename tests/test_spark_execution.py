from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from streamcase import (
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
    batch,
    restart,
    scenario,
)
from streamcase._directories import RunDirectories, create_run_directories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_execution import _execute_batches


class _FakeRow:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values

    def asDict(self, recursive: bool = False) -> dict[str, object]:  # noqa: N802
        assert recursive
        return dict(self.values)


class _FakeDataFrame:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self.rows = rows

    def collect(self) -> list[_FakeRow]:
        return self.rows


class _FakeQuery:
    def __init__(
        self,
        directories: RunDirectories,
        processed: list[tuple[str, str]] | None = None,
    ) -> None:
        self.directories = directories
        self.processed: list[tuple[str, str]] = [] if processed is None else processed
        self.process_error: Exception | None = None
        self.terminate_during_processing = False
        self.isActive = True
        self.stop_calls = 0
        self.stop_error: Exception | None = None
        self.stop_errors: list[Exception] = []
        self.name: str | None = None
        self.callback: Any = None
        self.drop_output = False

    def processAllAvailable(self) -> None:  # noqa: N802 - mirrors the PySpark API
        if self.process_error is not None:
            raise self.process_error
        visible = sorted(self.directories.input_dir.glob("batch-*.json"))
        assert len(visible) == len(self.processed) + 1
        current = visible[-1]
        self.processed.append((current.name, current.read_text(encoding="utf-8")))
        if self.callback is not None:
            rows = (
                []
                if self.drop_output
                else [
                    _FakeRow(json.loads(line))
                    for line in current.read_text(encoding="utf-8").splitlines()
                ]
            )
            self.callback(cast(Any, _FakeDataFrame(rows)), len(self.processed) - 1)
        if self.terminate_during_processing:
            self.isActive = False

    def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_errors:
            raise self.stop_errors.pop(0)
        if self.stop_error is not None:
            raise self.stop_error
        self.isActive = False


class _FakeStreamWriter:
    def __init__(self, query: _FakeQuery) -> None:
        self.query = query
        self.callback: object | None = None
        self.output_mode: str | None = None
        self.options: dict[str, str] = {}
        self.query_name: str | None = None
        self.start_error: Exception | None = None
        self.start_calls = 0

    def foreachBatch(self, callback: object) -> _FakeStreamWriter:  # noqa: N802
        self.callback = callback
        self.query.callback = callback
        return self

    def outputMode(self, mode: str) -> _FakeStreamWriter:  # noqa: N802
        self.output_mode = mode
        return self

    def option(self, name: str, value: str) -> _FakeStreamWriter:
        self.options[name] = value
        return self

    def queryName(self, name: str) -> _FakeStreamWriter:  # noqa: N802
        self.query_name = name
        self.query.name = name
        return self

    def start(self) -> _FakeQuery:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error
        return self.query


class _FakeStream:
    def __init__(self, query: _FakeQuery) -> None:
        self.writer = _FakeStreamWriter(query)
        self.writer_accesses = 0
        self.sparkSession = SimpleNamespace(streams=SimpleNamespace(active=[]))

    @property
    def writeStream(self) -> _FakeStreamWriter:  # noqa: N802 - mirrors the PySpark API
        self.writer_accesses += 1
        return self.writer


@pytest.fixture
def run_directories(tmp_path: Path) -> Iterator[RunDirectories]:
    directories = create_run_directories(base_dir=tmp_path)
    try:
        yield directories
    finally:
        directories.cleanup()


def test_each_batch_is_published_then_processed_in_scenario_order(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    capture = _BatchCapture()

    result = _execute_batches(
        cast(Any, stream),
        scenario(batch({"id": 1}), batch({"id": 2})),
        run_directories,
        capture,
    )

    assert query.processed == [
        ("batch-00000000000000000000.json", '{"id":1}\n'),
        ("batch-00000000000000000001.json", '{"id":2}\n'),
    ]
    assert stream.writer_accesses == 1
    assert stream.writer.callback == capture.callback
    assert stream.writer.output_mode == "append"
    assert stream.writer.options == {"checkpointLocation": str(run_directories.checkpoint_dir)}
    assert stream.writer.query_name == run_directories.root.name
    assert stream.writer.start_calls == 1
    assert query.stop_calls == 1
    assert isinstance(result, ScenarioResult)
    assert_batch_count(result, 2)
    assert_rows_equal(result, [{"id": 1}, {"id": 2}])
    assert_unique_keys(result, "id")

    capture.callback(cast(Any, _FakeDataFrame([_FakeRow({"id": 3})])), 2)
    assert_batch_count(result, 2)
    assert_rows_equal(result, [{"id": 1}, {"id": 2}])


@pytest.mark.parametrize("mode", ["complete", "update"])
def test_approved_output_mode_and_query_options_reach_the_writer(
    run_directories: RunDirectories,
    mode: str,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    caller_options = {"customOption": "enabled"}

    _execute_batches(
        cast(Any, stream),
        scenario(batch({"id": 1})),
        run_directories,
        _BatchCapture(),
        output_mode=mode,
        query_options=caller_options,
    )

    assert stream.writer.output_mode == mode
    assert stream.writer.options == {
        "customOption": "enabled",
        "checkpointLocation": str(run_directories.checkpoint_dir),
    }
    assert stream.writer.query_name == run_directories.root.name
    assert caller_options == {"customOption": "enabled"}
    assert query.stop_calls == 1


def test_invalid_query_configuration_does_not_access_the_writer(
    run_directories: RunDirectories,
) -> None:
    stream = _FakeStream(_FakeQuery(run_directories))

    with pytest.raises(ValueError, match="Runner-owned query options"):
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
            query_options={"CHECKPOINTLOCATION": "caller-path"},
        )

    assert stream.writer_accesses == 0


def test_empty_output_callback_remains_a_distinct_result_batch(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    query.drop_output = True
    stream = _FakeStream(query)

    result = _execute_batches(
        cast(Any, stream),
        scenario(batch({"id": 1})),
        run_directories,
        _BatchCapture(),
    )

    assert_batch_count(result, 1)
    assert result.batches[0].batch_id == 0
    assert_rows_equal(result, [])
    assert query.stop_calls == 1


def test_result_snapshot_is_taken_after_query_shutdown(
    run_directories: RunDirectories,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    capture = _BatchCapture()
    original_snapshot = _BatchCapture.snapshot

    def snapshot_after_stop(self: _BatchCapture) -> tuple[Any, ...]:
        assert query.stop_calls == 1
        assert not query.isActive
        return original_snapshot(self)

    monkeypatch.setattr(_BatchCapture, "snapshot", snapshot_after_stop)

    result = _execute_batches(
        cast(Any, stream),
        scenario(batch({"id": 1})),
        run_directories,
        capture,
    )

    assert_batch_count(result, 1)


def test_restart_requires_rebuild_callback_before_query_start_or_publication(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)

    with pytest.raises(ValueError, match="Restart actions require a stream rebuild callback"):
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
        )

    assert stream.writer_accesses == 0
    assert list(run_directories.input_dir.iterdir()) == []


def test_restart_rebuilds_stream_and_reuses_checkpoint_and_capture(
    run_directories: RunDirectories,
) -> None:
    first_query = _FakeQuery(run_directories)
    replacement_query = _FakeQuery(run_directories, first_query.processed)
    first_stream = _FakeStream(first_query)
    replacement_stream = _FakeStream(replacement_query)
    capture = _BatchCapture()
    rebuild_calls = 0

    def rebuild_stream() -> Any:
        nonlocal rebuild_calls
        rebuild_calls += 1
        assert first_query.stop_calls == 1
        assert not first_query.isActive
        return replacement_stream

    result = _execute_batches(
        cast(Any, first_stream),
        scenario(batch({"id": 1}), restart(), batch({"id": 2})),
        run_directories,
        capture,
        output_mode="update",
        query_options={"customOption": "enabled"},
        rebuild_stream=rebuild_stream,
    )

    assert rebuild_calls == 1
    assert first_query is not replacement_query
    assert first_query.stop_calls == replacement_query.stop_calls == 1
    assert first_query.processed == [
        ("batch-00000000000000000000.json", '{"id":1}\n'),
        ("batch-00000000000000000001.json", '{"id":2}\n'),
    ]
    for stream in (first_stream, replacement_stream):
        assert stream.writer.start_calls == 1
        assert stream.writer.callback == capture.callback
        assert stream.writer.output_mode == "update"
        assert stream.writer.options == {
            "customOption": "enabled",
            "checkpointLocation": str(run_directories.checkpoint_dir),
        }
        assert stream.writer.query_name == run_directories.root.name
    assert [captured.batch_id for captured in result.batches] == [0, 1]
    assert_rows_equal(result, [{"id": 1}, {"id": 2}])


def test_restart_build_failure_has_action_context_and_stops_old_query(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    build_error = OSError("injected stream rebuild failure")

    def fail_rebuild() -> Any:
        raise build_error

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while rebuilding the stream"
    ) as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
            rebuild_stream=fail_rebuild,
        )

    assert error_info.value.__cause__ is build_error
    assert query.stop_calls == 1
    assert len(query.processed) == 1


def test_restart_stop_failure_is_retried_without_starting_a_replacement(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    query.stop_errors = [OSError("injected first stop failure")]
    stream = _FakeStream(query)
    rebuild_calls = 0

    def rebuild_stream() -> Any:
        nonlocal rebuild_calls
        rebuild_calls += 1
        return stream

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while stopping the active query"
    ) as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
            rebuild_stream=rebuild_stream,
        )

    assert isinstance(error_info.value.__cause__, OSError)
    assert query.stop_calls == 2
    assert not query.isActive
    assert rebuild_calls == 0
    assert stream.writer.start_calls == 1
    assert len(query.processed) == 1


def test_restart_query_start_failure_has_action_context(
    run_directories: RunDirectories,
) -> None:
    first_query = _FakeQuery(run_directories)
    replacement_query = _FakeQuery(run_directories, first_query.processed)
    first_stream = _FakeStream(first_query)
    replacement_stream = _FakeStream(replacement_query)
    start_error = OSError("injected replacement start failure")
    replacement_stream.writer.start_error = start_error

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while starting the replacement query"
    ) as error_info:
        _execute_batches(
            cast(Any, first_stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
            rebuild_stream=lambda: cast(Any, replacement_stream),
        )

    assert isinstance(error_info.value.__cause__, RuntimeError)
    assert error_info.value.__cause__.__cause__ is start_error
    assert first_query.stop_calls == 1
    assert replacement_stream.writer.start_calls == 1
    assert len(first_query.processed) == 1


def test_restart_start_cleanup_retries_only_the_run_owned_registered_query(
    run_directories: RunDirectories,
) -> None:
    first_query = _FakeQuery(run_directories)
    replacement_query = _FakeQuery(run_directories, first_query.processed)
    cleanup_error = OSError("injected first cleanup failure")
    replacement_query.stop_errors = [cleanup_error]
    caller_query = _FakeQuery(run_directories)
    caller_query.name = "caller-owned-query"
    first_stream = _FakeStream(first_query)
    replacement_stream = _FakeStream(replacement_query)
    replacement_stream.sparkSession.streams.active = [caller_query, replacement_query]
    start_error = OSError("injected replacement start failure")
    replacement_stream.writer.start_error = start_error

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while starting the replacement query"
    ) as error_info:
        _execute_batches(
            cast(Any, first_stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
            rebuild_stream=lambda: cast(Any, replacement_stream),
        )

    primary = error_info.value
    assert isinstance(primary.__cause__, RuntimeError)
    assert primary.__cause__.__cause__ is start_error
    assert primary.__dict__["_streamcase_cleanup_failures"] == (("streaming query", cleanup_error),)
    assert first_query.stop_calls == 1
    assert replacement_query.stop_calls == 2
    assert not replacement_query.isActive
    assert caller_query.stop_calls == 0


def test_inactive_replacement_query_is_cleaned_up_with_restart_context(
    run_directories: RunDirectories,
) -> None:
    first_query = _FakeQuery(run_directories)
    replacement_query = _FakeQuery(run_directories, first_query.processed)
    replacement_query.isActive = False
    first_stream = _FakeStream(first_query)
    replacement_stream = _FakeStream(replacement_query)

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while starting the replacement query"
    ) as error_info:
        _execute_batches(
            cast(Any, first_stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
            rebuild_stream=lambda: cast(Any, replacement_stream),
        )

    assert "streaming query terminated" in str(error_info.value)
    assert first_query.stop_calls == 1
    assert replacement_query.stop_calls == 1
    assert not replacement_query.isActive


def test_query_start_failure_identifies_first_action(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    stream.writer.start_error = OSError("injected query start failure")

    with pytest.raises(RuntimeError, match="before Batch action at index 0") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert isinstance(error_info.value.__cause__, OSError)
    assert query.stop_calls == 0
    assert list(run_directories.input_dir.iterdir()) == []


def test_start_failure_stops_only_the_query_owned_by_this_run(
    run_directories: RunDirectories,
) -> None:
    own_query = _FakeQuery(run_directories)
    caller_query = _FakeQuery(run_directories)
    caller_query.name = "caller-owned-query"
    stream = _FakeStream(own_query)
    stream.sparkSession.streams.active = [caller_query, own_query]
    stream.writer.start_error = OSError("injected failure after query registration")

    with pytest.raises(RuntimeError, match="before Batch action at index 0") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert isinstance(error_info.value.__cause__, OSError)
    assert own_query.stop_calls == 1
    assert caller_query.stop_calls == 0


def test_initial_start_failure_retries_a_registered_query_cleanup(
    run_directories: RunDirectories,
) -> None:
    own_query = _FakeQuery(run_directories)
    cleanup_error = OSError("injected first cleanup failure")
    own_query.stop_errors = [cleanup_error]
    caller_query = _FakeQuery(run_directories)
    caller_query.name = "caller-owned-query"
    stream = _FakeStream(own_query)
    stream.sparkSession.streams.active = [caller_query, own_query]
    start_error = OSError("injected start failure after registration")
    stream.writer.start_error = start_error

    with pytest.raises(RuntimeError, match="before Batch action at index 0") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert error_info.value.__cause__ is start_error
    assert error_info.value.__dict__["_streamcase_cleanup_failures"] == (
        ("streaming query", cleanup_error),
    )
    assert own_query.stop_calls == 2
    assert not own_query.isActive
    assert caller_query.stop_calls == 0
    assert list(run_directories.input_dir.iterdir()) == []


def test_processing_and_query_stop_failures_keep_both_errors(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    query.process_error = OSError("injected processing failure")
    query.stop_error = OSError("injected stop failure")

    with pytest.raises(RuntimeError, match="Batch action at index 0 failed") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert error_info.value.__cause__ is query.process_error
    assert error_info.value.__dict__["_streamcase_cleanup_failures"] == (
        ("streaming query", query.stop_error),
    )


def test_publication_failure_preserves_action_index_and_cause(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)

    with pytest.raises(RuntimeError, match="Batch action at index 1 failed") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), batch({"unsupported": object()})),
            run_directories,
            _BatchCapture(),
        )

    assert isinstance(error_info.value.__cause__, TypeError)
    assert len(query.processed) == 1
    assert query.stop_calls == 1


def test_processing_failure_preserves_action_index_and_cause(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    query.process_error = OSError("injected processing failure")

    with pytest.raises(RuntimeError, match="Batch action at index 0 failed") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert isinstance(error_info.value.__cause__, OSError)
    assert query.stop_calls == 1


def test_query_termination_after_processing_is_reported_with_action_index(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)
    query.terminate_during_processing = True

    with pytest.raises(RuntimeError, match="Batch action at index 0 failed") as error_info:
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
        )

    assert "streaming query terminated" in str(error_info.value)
    assert len(query.processed) == 1
    assert query.stop_calls == 1


def test_query_termination_before_publication_does_not_write_input(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    query.isActive = False
    stream = _FakeStream(query)

    with pytest.raises(RuntimeError, match="Batch action at index 0 failed"):
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1})),
            run_directories,
            _BatchCapture(),
        )

    assert list(run_directories.input_dir.iterdir()) == []
    assert query.stop_calls == 1
