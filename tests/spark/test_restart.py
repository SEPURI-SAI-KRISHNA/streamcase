"""Real-Spark coverage for checkpoint-preserving restart scenarios."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from streamcase import assert_batch_count, assert_rows_equal, assert_unique_keys, batch, restart
from streamcase import scenario as make_scenario
from streamcase._spark_lifecycle import _QueryLifecycle

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

pytestmark = pytest.mark.spark


@dataclass(frozen=True)
class _ObservedStart:
    action_index: int
    query_id: str
    run_id: str
    checkpoint_dir: Path
    query: StreamingQuery


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-restart-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _record_starts(
    monkeypatch: pytest.MonkeyPatch,
    observed: list[_ObservedStart],
    *,
    fail_after_replacement_start: Exception | None = None,
) -> None:
    original_start = _QueryLifecycle.start

    def record_start(self: _QueryLifecycle, stream: DataFrame, *, action_index: int) -> None:
        original_start(self, stream, action_index=action_index)
        if observed:
            assert not observed[-1].query.isActive
        query = self.require_active()
        observed.append(
            _ObservedStart(
                action_index=action_index,
                query_id=query.id,
                run_id=query.runId,
                checkpoint_dir=self._directories.checkpoint_dir,
                query=query,
            )
        )
        if action_index > 0 and fail_after_replacement_start is not None:
            raise fail_after_replacement_start

    monkeypatch.setattr(_QueryLifecycle, "start", record_start)


def _active_run_ids(spark: SparkSession) -> set[str]:
    return {query.runId for query in spark.streams.active}


def test_restart_reuses_checkpoint_with_a_new_query_execution(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from streamcase.spark import run_scenario

    observed: list[_ObservedStart] = []
    _record_starts(monkeypatch, observed)
    active_before = _active_run_ids(spark)
    transform_calls = 0

    def transform(source: DataFrame) -> DataFrame:
        nonlocal transform_calls
        transform_calls += 1
        return source.selectExpr("id", "id * 10 AS scaled")

    result = run_scenario(
        spark,
        make_scenario(batch({"id": 1}), restart(), batch({"id": 2})),
        schema="id LONG",
        transform=transform,
        base_dir=tmp_path,
        retain_artifacts=True,
    )

    assert transform_calls == 2
    assert [item.action_index for item in observed] == [0, 2]
    assert len({item.query_id for item in observed}) == 1
    assert len({item.run_id for item in observed}) == 2
    assert len({item.checkpoint_dir for item in observed}) == 1
    assert all(not item.query.isActive for item in observed)
    assert_batch_count(result, 2)
    assert [item.batch_id for item in result.batches] == [0, 1]
    assert_rows_equal(result, [{"id": 1, "scaled": 10}, {"id": 2, "scaled": 20}])
    assert_unique_keys(result, "id")

    root = observed[0].checkpoint_dir.parent
    assert list(tmp_path.glob("streamcase-run-*")) == [root]
    assert observed[0].checkpoint_dir.is_dir()
    input_files = sorted((root / "input").glob("batch-*.json"))
    assert [path.name for path in input_files] == [
        "batch-00000000000000000000.json",
        "batch-00000000000000000001.json",
    ]
    assert [path.read_text(encoding="utf-8") for path in input_files] == [
        '{"id":1}\n',
        '{"id":2}\n',
    ]
    assert _active_run_ids(spark) == active_before
    assert spark.range(1).count() == 1


def test_restart_rebuild_failure_stops_query_and_removes_run_root(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from streamcase.spark import run_scenario

    observed: list[_ObservedStart] = []
    _record_starts(monkeypatch, observed)
    active_before = _active_run_ids(spark)
    failure = ValueError("injected replacement transformation failure")
    transform_calls = 0

    def transform(source: DataFrame) -> DataFrame:
        nonlocal transform_calls
        transform_calls += 1
        if transform_calls == 2:
            raise failure
        return source

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while rebuilding the stream"
    ) as error_info:
        run_scenario(
            spark,
            make_scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            schema="id LONG",
            transform=transform,
            base_dir=tmp_path,
        )

    assert error_info.value.__cause__ is failure
    assert transform_calls == 2
    assert len(observed) == 1
    assert not observed[0].query.isActive
    assert not observed[0].checkpoint_dir.exists()
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert spark.range(1).count() == 1


def test_restart_stop_failure_is_cleaned_up_without_starting_replacement(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from streamcase.spark import run_scenario

    observed: list[_ObservedStart] = []
    _record_starts(monkeypatch, observed)
    active_before = _active_run_ids(spark)
    original_stop = _QueryLifecycle.stop
    failure = OSError("injected restart stop failure")
    stop_calls = 0

    def fail_first_stop(self: _QueryLifecycle) -> None:
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise failure
        original_stop(self)

    monkeypatch.setattr(_QueryLifecycle, "stop", fail_first_stop)

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while stopping the active query"
    ) as error_info:
        run_scenario(
            spark,
            make_scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            schema="id LONG",
            transform=lambda frame: frame,
            base_dir=tmp_path,
        )

    assert error_info.value.__cause__ is failure
    assert stop_calls == 2
    assert len(observed) == 1
    assert not observed[0].query.isActive
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert spark.range(1).count() == 1


def test_restart_start_failure_after_registration_stops_replacement(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from streamcase.spark import run_scenario

    observed: list[_ObservedStart] = []
    failure = OSError("injected failure after replacement start")
    _record_starts(monkeypatch, observed, fail_after_replacement_start=failure)
    active_before = _active_run_ids(spark)

    with pytest.raises(
        RuntimeError, match="Restart action at index 1 failed while starting the replacement query"
    ) as error_info:
        run_scenario(
            spark,
            make_scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            schema="id LONG",
            transform=lambda frame: frame,
            base_dir=tmp_path,
        )

    assert error_info.value.__cause__ is failure
    assert [item.action_index for item in observed] == [0, 2]
    assert len({item.query_id for item in observed}) == 1
    assert len({item.run_id for item in observed}) == 2
    assert all(not item.query.isActive for item in observed)
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
    assert spark.range(1).count() == 1
