from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from streamcase import batch, restart, scenario
from streamcase._directories import RunDirectories, create_run_directories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_execution import _execute_batches


class _FakeQuery:
    def __init__(self, directories: RunDirectories) -> None:
        self.directories = directories
        self.processed: list[tuple[str, str]] = []
        self.process_error: Exception | None = None
        self.terminate_during_processing = False
        self.isActive = True
        self.stop_calls = 0

    def processAllAvailable(self) -> None:  # noqa: N802 - mirrors the PySpark API
        if self.process_error is not None:
            raise self.process_error
        visible = sorted(self.directories.input_dir.glob("batch-*.json"))
        assert len(visible) == len(self.processed) + 1
        current = visible[-1]
        self.processed.append((current.name, current.read_text(encoding="utf-8")))
        if self.terminate_during_processing:
            self.isActive = False

    def stop(self) -> None:
        self.stop_calls += 1
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
        return self

    def outputMode(self, mode: str) -> _FakeStreamWriter:  # noqa: N802
        self.output_mode = mode
        return self

    def option(self, name: str, value: str) -> _FakeStreamWriter:
        self.options[name] = value
        return self

    def queryName(self, name: str) -> _FakeStreamWriter:  # noqa: N802
        self.query_name = name
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

    _execute_batches(
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


def test_restart_is_rejected_before_query_start_or_publication(
    run_directories: RunDirectories,
) -> None:
    query = _FakeQuery(run_directories)
    stream = _FakeStream(query)

    with pytest.raises(ValueError, match="action at index 1 is not a Batch"):
        _execute_batches(
            cast(Any, stream),
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            run_directories,
            _BatchCapture(),
        )

    assert stream.writer_accesses == 0
    assert list(run_directories.input_dir.iterdir()) == []


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
