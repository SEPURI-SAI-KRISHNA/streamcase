"""Real-Spark ownership and isolation checks for the opt-in pytest fixture."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from streamcase import (
    ScenarioLimitError,
    ScenarioResult,
    assert_rows_equal,
    batch,
    restart,
    scenario,
)
from streamcase._directories import RunDirectories

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery

pytest_plugins = ("streamcase.pytest_plugin",)
pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def caller_resources(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[SparkSession, StreamingQuery]]:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-pytest-isolation-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    caller_dir = tmp_path_factory.mktemp("caller-owned-stream")
    source_dir = caller_dir / "input"
    source_dir.mkdir()
    try:
        query = (
            spark.readStream.schema("id LONG")
            .json(str(source_dir))
            .writeStream.format("memory")
            .queryName("streamcase_caller_owned")
            .option("checkpointLocation", str(caller_dir / "checkpoint"))
            .start()
        )
        try:
            yield spark, query
            # Module teardown runs after all function-scoped runner fixtures.
            assert query.isActive
            assert spark.range(1).count() == 1
        finally:
            query.stop()
    finally:
        spark.stop()


@pytest.fixture(scope="module")
def observed_test_paths() -> set[Path]:
    return set()


def _active_run_ids(spark: SparkSession) -> set[str]:
    return {query.runId for query in spark.streams.active}


@pytest.mark.parametrize("value", [1, 2])
def test_each_case_gets_a_distinct_parent_and_cleans_successful_run(
    value: int,
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    observed_test_paths: set[Path],
    tmp_path: Path,
) -> None:
    spark, caller_query = caller_resources
    assert tmp_path not in observed_test_paths
    observed_test_paths.add(tmp_path)

    active_before = _active_run_ids(spark)
    assert caller_query.runId in active_before

    result = streamcase_runner(
        spark,
        scenario(batch({"id": value})),
        schema="id LONG",
        transform=lambda source: source.selectExpr("id"),
    )

    assert_rows_equal(result, [{"id": value}])
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert caller_query.isActive


def test_repeated_calls_keep_separate_retained_run_roots(
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
) -> None:
    spark, caller_query = caller_resources
    active_before = _active_run_ids(spark)

    for value in (3, 4):
        result = streamcase_runner(
            spark,
            scenario(batch({"id": value})),
            schema="id LONG",
            transform=lambda source: source.selectExpr("id"),
            retain_artifacts=True,
        )
        assert_rows_equal(result, [{"id": value}])
        assert _active_run_ids(spark) == active_before

    roots = sorted(tmp_path.glob("streamcase-run-*"))
    assert len(roots) == 2
    assert roots[0] != roots[1]
    assert all((root / "input").is_dir() for root in roots)
    assert all((root / "checkpoint").is_dir() for root in roots)
    assert caller_query.isActive


def test_failed_calls_clean_runner_resources_but_keep_caller_resources(
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
) -> None:
    spark, caller_query = caller_resources
    active_before = _active_run_ids(spark)
    caller_file = tmp_path / "caller-owned.txt"
    caller_file.write_text("preserve me", encoding="utf-8")

    with pytest.raises(TypeError, match="must return a DataFrame"):
        streamcase_runner(
            spark,
            scenario(batch({"id": 1})),
            schema="id LONG",
            transform=lambda _source: None,
        )
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before

    with pytest.raises(RuntimeError, match="Batch action at index 1 failed"):
        streamcase_runner(
            spark,
            scenario(batch({"id": 1}), batch({"id": object()})),
            schema="id LONG",
            transform=lambda source: source.selectExpr("id"),
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert caller_query.isActive
    assert caller_file.read_text(encoding="utf-8") == "preserve me"
    assert spark.range(1).count() == 1


@pytest.mark.parametrize(
    ("failure_kind", "retain_artifacts", "expected_resource", "expected_files"),
    [
        ("input_before_processing", True, "input_bytes_per_batch", 0),
        ("output_after_first_batch", True, "output_rows", 2),
        ("output_after_first_batch", False, "output_rows", 2),
        ("input_after_restart", True, "input_bytes_per_run", 1),
    ],
)
def test_limit_failures_keep_caller_query_and_isolate_artifacts(
    failure_kind: str,
    retain_artifacts: bool,
    expected_resource: str,
    expected_files: int,
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
) -> None:
    spark, caller_query = caller_resources
    active_before = _active_run_ids(spark)
    assert caller_query.runId in active_before

    if failure_kind == "input_before_processing":
        steps = scenario(batch({"id": 1}))
        options = {"max_input_bytes_per_batch": 8}
        expected_action_index = 0
    elif failure_kind == "output_after_first_batch":
        steps = scenario(batch({"id": 1}), batch({"id": 2}))
        options = {"max_output_rows": 1}
        expected_action_index = 1
    else:
        steps = scenario(batch({"id": 1}), restart(), batch({"id": 2}))
        options = {"max_input_bytes_per_run": 9}
        expected_action_index = 2

    with pytest.raises(ScenarioLimitError) as error_info:
        streamcase_runner(
            spark,
            steps,
            schema="id LONG",
            transform=lambda source: source.selectExpr("id"),
            retain_artifacts=retain_artifacts,
            **options,
        )

    failure = error_info.value
    assert (failure.resource, failure.action_index) == (
        expected_resource,
        expected_action_index,
    )
    roots = list(tmp_path.glob("streamcase-run-*"))
    if retain_artifacts:
        assert len(roots) == 1
        input_files = sorted((roots[0] / "input").iterdir())
        assert len(input_files) == expected_files
        assert all(
            path.name.startswith("batch-") and path.suffix == ".json" for path in input_files
        )
        assert [path.read_bytes() for path in input_files] == [
            b'{"id":1}\n',
            b'{"id":2}\n',
        ][:expected_files]
    else:
        assert roots == []
    assert _active_run_ids(spark) == active_before
    assert caller_query.isActive
    assert spark.range(1).count() == 1


def test_invalid_input_limit_has_no_query_or_directory_side_effects(
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
) -> None:
    spark, caller_query = caller_resources
    active_before = _active_run_ids(spark)

    with pytest.raises(ValueError, match="max_input_bytes_per_batch"):
        streamcase_runner(
            spark,
            scenario(batch({"id": 1})),
            schema="id LONG",
            transform=lambda source: source,
            max_input_bytes_per_batch=0,
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert caller_query.isActive


def test_limit_error_remains_primary_if_directory_cleanup_fails(
    caller_resources: tuple[SparkSession, StreamingQuery],
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, caller_query = caller_resources
    active_before = _active_run_ids(spark)
    cleanup_error = OSError("injected run-directory cleanup failure")
    captured_directories: list[RunDirectories] = []

    def fail_cleanup(directories: RunDirectories) -> None:
        captured_directories.append(directories)
        raise cleanup_error

    try:
        with monkeypatch.context() as patch:
            patch.setattr(RunDirectories, "cleanup", fail_cleanup)
            with pytest.raises(ScenarioLimitError) as error_info:
                streamcase_runner(
                    spark,
                    scenario(batch({"id": 1}), batch({"id": 2})),
                    schema="id LONG",
                    transform=lambda source: source.selectExpr("id"),
                    max_output_rows=1,
                )

        failure = error_info.value
        assert (failure.resource, failure.action_index) == ("output_rows", 1)
        assert failure.__dict__["_streamcase_cleanup_failures"] == (
            ("run directory", cleanup_error),
        )
        assert len(captured_directories) == 1
        assert captured_directories[0].root.parent == tmp_path.resolve()
        assert captured_directories[0].root.is_dir()
        assert _active_run_ids(spark) == active_before
        assert caller_query.isActive
        assert spark.range(1).count() == 1
    finally:
        for directories in captured_directories:
            directories.cleanup()
