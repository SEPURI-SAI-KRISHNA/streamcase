from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest

from streamcase import (
    ScenarioLimitError,
    assert_batch_count,
    assert_rows_equal,
    batch,
    expect_failure,
    restart,
    scenario,
)
from streamcase._directories import RunDirectories
from streamcase._spark_managed import _run_managed_batches


def test_managed_run_rejects_unimplemented_expected_failure_before_side_effects(
    tmp_path: Path,
) -> None:
    build_stream = Mock()
    declared = scenario(expect_failure(batch({"id": 1}), message_contains="bad-input"))

    with pytest.raises(NotImplementedError, match="issue #175"):
        _run_managed_batches(declared, build_stream, base_dir=tmp_path)

    build_stream.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def _fake_stream() -> tuple[Mock, Mock]:
    query = Mock(isActive=True, recentProgress=[], runId="fake-run")
    writer = Mock()
    writer.foreachBatch.return_value = writer
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.queryName.return_value = writer
    writer.start.return_value = query
    stream = Mock(writeStream=writer)
    stream.sparkSession.streams.active = []
    return stream, query


def test_managed_run_removes_generated_root_before_return(tmp_path: Path) -> None:
    roots: list[Path] = []
    stream, query = _fake_stream()

    def build_stream(directories: RunDirectories) -> Any:
        roots.append(directories.root)
        return stream

    result = _run_managed_batches(
        scenario(batch({"id": 1})),
        build_stream,
        base_dir=tmp_path,
    )

    assert_batch_count(result, 0)
    query.stop.assert_called_once_with()
    assert len(roots) == 1
    assert not roots[0].exists()
    assert tmp_path.is_dir()

    with pytest.raises(AssertionError):
        assert_rows_equal(result, [{"id": 1}])
    assert not roots[0].exists()


def test_managed_run_cleans_directory_when_stream_builder_fails(tmp_path: Path) -> None:
    def build_stream(directories: RunDirectories) -> Any:
        raise AssertionError("transform failed")

    with pytest.raises(AssertionError, match="transform failed"):
        _run_managed_batches(scenario(batch({"id": 1})), build_stream, base_dir=tmp_path)

    assert list(tmp_path.glob("streamcase-run-*")) == []


def test_managed_run_cleans_directory_when_query_start_fails(tmp_path: Path) -> None:
    stream, query = _fake_stream()
    stream.writeStream.start.side_effect = OSError("injected start failure")

    with pytest.raises(RuntimeError, match="before Batch action at index 0"):
        _run_managed_batches(
            scenario(batch({"id": 1})),
            cast(Any, lambda directories: stream),
            base_dir=tmp_path,
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []
    query.stop.assert_not_called()


def test_managed_run_retains_generated_root_when_requested(tmp_path: Path) -> None:
    roots: list[Path] = []
    stream, query = _fake_stream()

    def build_stream(directories: RunDirectories) -> Any:
        roots.append(directories.root)
        return stream

    _run_managed_batches(
        scenario(batch({"id": 1})),
        build_stream,
        base_dir=tmp_path,
        retain_artifacts=True,
    )

    query.stop.assert_called_once_with()
    assert len(roots) == 1
    assert roots[0].is_dir()
    assert roots[0].parent == tmp_path.resolve()


def test_retained_artifacts_survive_a_stream_build_failure(tmp_path: Path) -> None:
    roots: list[Path] = []

    def fail_build(directories: RunDirectories) -> Any:
        roots.append(directories.root)
        raise AssertionError("injected build failure")

    with pytest.raises(AssertionError, match="injected build failure"):
        _run_managed_batches(
            scenario(batch({"id": 1})),
            fail_build,
            base_dir=tmp_path,
            retain_artifacts=True,
        )

    assert len(roots) == 1
    assert roots[0].is_dir()


def test_directory_cleanup_failure_does_not_replace_build_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_error = OSError("injected directory cleanup failure")

    def fail_cleanup(_directories: RunDirectories) -> None:
        raise cleanup_error

    def fail_build(_directories: RunDirectories) -> Any:
        raise AssertionError("injected build failure")

    monkeypatch.setattr(RunDirectories, "cleanup", fail_cleanup)

    with pytest.raises(AssertionError, match="injected build failure") as error_info:
        _run_managed_batches(scenario(batch({"id": 1})), fail_build, base_dir=tmp_path)

    assert error_info.value.__dict__["_streamcase_cleanup_failures"] == (
        ("run directory", cleanup_error),
    )


def test_restart_rebuilds_stream_with_the_same_managed_directories(tmp_path: Path) -> None:
    first_stream, first_query = _fake_stream()
    second_stream, second_query = _fake_stream()
    streams = [first_stream, second_stream]
    roots: list[Path] = []
    checkpoints: list[Path] = []

    def build_stream(directories: RunDirectories) -> Any:
        roots.append(directories.root)
        checkpoints.append(directories.checkpoint_dir)
        return streams.pop(0)

    result = _run_managed_batches(
        scenario(batch({"id": 1}), restart(), batch({"id": 2})),
        build_stream,
        base_dir=tmp_path,
    )

    assert_batch_count(result, 0)
    assert len(roots) == 2
    assert roots[0] == roots[1]
    assert checkpoints == [roots[0] / "checkpoint"] * 2
    assert not roots[0].exists()
    first_query.stop.assert_called_once_with()
    second_query.stop.assert_called_once_with()
    for stream in (first_stream, second_stream):
        stream.writeStream.option.assert_any_call("checkpointLocation", str(checkpoints[0]))


def test_restart_rebuild_failure_cleans_managed_root_without_stopping_session(
    tmp_path: Path,
) -> None:
    stream, query = _fake_stream()
    roots: list[Path] = []
    build_error = OSError("injected replacement stream failure")

    def build_stream(directories: RunDirectories) -> Any:
        roots.append(directories.root)
        if len(roots) == 2:
            raise build_error
        return stream

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while rebuilding the stream"
    ) as error_info:
        _run_managed_batches(
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            build_stream,
            base_dir=tmp_path,
        )

    assert error_info.value.__cause__ is build_error
    assert len(roots) == 2
    assert roots[0] == roots[1]
    assert not roots[0].exists()
    query.stop.assert_called_once_with()
    stream.sparkSession.stop.assert_not_called()


@pytest.mark.parametrize(
    ("output_mode", "query_options", "message"),
    [
        ("overwrite", None, "output mode must be"),
        ("append", {"QUERYNAME": "caller-name"}, "Runner-owned query options"),
    ],
)
def test_invalid_query_configuration_fails_before_directory_or_stream_creation(
    tmp_path: Path,
    output_mode: str,
    query_options: dict[str, str] | None,
    message: str,
) -> None:
    def unexpected_build(_directories: RunDirectories) -> Any:
        pytest.fail("stream builder was called")

    with pytest.raises(ValueError, match=message):
        _run_managed_batches(
            scenario(batch({"id": 1})),
            unexpected_build,
            base_dir=tmp_path,
            output_mode=output_mode,
            query_options=query_options,
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []


@pytest.mark.parametrize(
    ("limit_name", "invalid", "error_type"),
    [
        ("max_input_bytes_per_batch", True, TypeError),
        ("max_input_bytes_per_batch", 0, ValueError),
        ("max_input_bytes_per_run", False, TypeError),
        ("max_input_bytes_per_run", -1, ValueError),
    ],
)
def test_invalid_input_limit_fails_before_directory_or_stream_creation(
    tmp_path: Path,
    limit_name: str,
    invalid: object,
    error_type: type[Exception],
) -> None:
    def unexpected_build(_directories: RunDirectories) -> Any:
        pytest.fail("stream builder was called")

    with pytest.raises(error_type, match=f"{limit_name} must be a positive integer or None"):
        _run_managed_batches(
            scenario(batch({"id": 1})),
            unexpected_build,
            base_dir=tmp_path,
            **{limit_name: cast(Any, invalid)},
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []


def test_limit_error_stays_primary_when_query_and_directory_cleanup_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream, owned_query = _fake_stream()
    caller_query = Mock(isActive=True)
    stream.sparkSession.streams.active = [caller_query, owned_query]
    stop_error = OSError("injected owned-query stop failure")
    directory_error = OSError("injected run-directory cleanup failure")
    owned_query.stop.side_effect = stop_error
    roots: list[RunDirectories] = []

    def build_stream(directories: RunDirectories) -> Any:
        roots.append(directories)
        return stream

    def fail_cleanup(_directories: RunDirectories) -> None:
        raise directory_error

    try:
        with monkeypatch.context() as patch:
            patch.setattr(RunDirectories, "cleanup", fail_cleanup)
            with pytest.raises(ScenarioLimitError, match="Batch action at index 0") as error_info:
                _run_managed_batches(
                    scenario(batch({"id": 1})),
                    build_stream,
                    base_dir=tmp_path,
                    max_input_bytes_per_batch=8,
                )

        failure = error_info.value
        assert (failure.resource, failure.row_index) == ("input_bytes_per_batch", 0)
        assert failure.__dict__["_streamcase_cleanup_failures"] == (
            ("streaming query", stop_error),
            ("run directory", directory_error),
        )
        notes = tuple(getattr(failure, "__notes__", ())) + tuple(
            str(item) for item in failure.args[1:]
        )
        assert any("streaming query" in note for note in notes)
        assert any("run directory" in note for note in notes)
        owned_query.stop.assert_called_once_with()
        caller_query.stop.assert_not_called()
        stream.sparkSession.stop.assert_not_called()
    finally:
        for directories in roots:
            directories.cleanup()
