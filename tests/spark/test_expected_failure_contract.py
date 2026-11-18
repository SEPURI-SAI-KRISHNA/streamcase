"""Real-Spark evidence for expected-failure ownership and partial results."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from streamcase import (
    ScenarioLimitError,
    assert_failure,
    assert_rows_equal,
    batch,
    expect_failure,
    restart,
    scenario,
)
from streamcase._spark_lifecycle import _QueryLifecycle

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

pytestmark = pytest.mark.spark

_MARKER = "STREAMCASE_CONTRACT_BAD_RECORD"


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-expected-failure-contract")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _reject_bad(source: DataFrame) -> DataFrame:
    return source.selectExpr(
        f"CASE WHEN value = 'bad' THEN raise_error('{_MARKER}') ELSE value END AS value"
    )


def _active_run_ids(spark: SparkSession) -> set[str]:
    return {query.runId for query in spark.streams.active}


def _assert_no_runner_leaks(spark: SparkSession, base_dir: Path, before: set[str]) -> None:
    assert _active_run_ids(spark) == before
    assert list(base_dir.glob("streamcase-run-*")) == []
    assert spark.range(1).count() == 1


def test_first_action_failure_has_no_completed_captures(
    spark: SparkSession, tmp_path: Path
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    result = run_scenario(
        spark,
        scenario(expect_failure(batch({"value": "bad"}), message_contains=_MARKER)),
        schema="value STRING",
        transform=_reject_bad,
        base_dir=tmp_path,
    )

    assert result.batches == ()
    assert result.progress == ()
    assert_failure(result, action_index=0, message_contains=_MARKER)
    _assert_no_runner_leaks(spark, tmp_path, before)


def test_failure_after_completed_batch_preserves_only_prior_rows_and_progress(
    spark: SparkSession, tmp_path: Path
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    result = run_scenario(
        spark,
        scenario(
            batch({"value": "good"}),
            expect_failure(batch({"value": "bad"}), message_contains=_MARKER),
        ),
        schema="value STRING",
        transform=_reject_bad,
        base_dir=tmp_path,
    )

    assert_rows_equal(result, [{"value": "good"}])
    assert [item.batch_id for item in result.batches] == [0]
    assert [(item.action_index, item.trigger_index) for item in result.progress] == [(0, 0)]
    assert_failure(result, action_index=1, message_contains=_MARKER)
    _assert_no_runner_leaks(spark, tmp_path, before)


def test_partial_results_retain_order_across_restart_and_preserve_other_query(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from streamcase.spark import run_scenario

    caller_input = tmp_path / "caller-input"
    caller_input.mkdir()
    caller_file = tmp_path / "caller-owned.txt"
    caller_file.write_text("preserve me", encoding="utf-8")
    unrelated = (
        spark.readStream.schema("value STRING")
        .json(str(caller_input))
        .writeStream.foreachBatch(lambda _frame, _batch_id: None)
        .queryName("caller-owned-query")
        .option("checkpointLocation", str(tmp_path / "caller-checkpoint"))
        .start()
    )
    before = _active_run_ids(spark)
    owned: list[StreamingQuery] = []
    original_start = _QueryLifecycle.start

    def record_start(self: _QueryLifecycle, stream: DataFrame, *, action_index: int) -> None:
        original_start(self, stream, action_index=action_index)
        owned.append(self.require_active())

    monkeypatch.setattr(_QueryLifecycle, "start", record_start)
    try:
        result = run_scenario(
            spark,
            scenario(
                batch({"value": "one"}),
                batch({"value": "two"}),
                restart(),
                batch({"value": "three"}),
                expect_failure(batch({"value": "bad"}), message_contains=_MARKER),
            ),
            schema="value STRING",
            transform=_reject_bad,
            base_dir=tmp_path,
            retain_artifacts=True,
        )

        assert_rows_equal(result, [{"value": "one"}, {"value": "two"}, {"value": "three"}])
        assert [row["value"] for row in result.rows] == ["one", "two", "three"]
        assert [item.batch_id for item in result.batches] == [0, 1, 2]
        progress_locations = [
            (item.action_index, item.trigger_index, item.batch_id) for item in result.progress
        ]
        assert progress_locations == [
            (0, 0, 0),
            (1, 0, 1),
            (3, 0, 2),
        ]
        assert_failure(result, action_index=4, message_contains=_MARKER)
        assert len(owned) == 2
        assert len({query.id for query in owned}) == 1
        assert len({query.runId for query in owned}) == 2
        assert all(not query.isActive for query in owned)
        assert [item.run_id for item in result.progress] == [
            owned[0].runId,
            owned[0].runId,
            owned[1].runId,
        ]

        retained_roots = list(tmp_path.glob("streamcase-run-*"))
        assert len(retained_roots) == 1
        root = retained_roots[0]
        assert (root / "checkpoint").is_dir()
        input_files = sorted((root / "input").glob("batch-*.json"))
        assert [path.read_text(encoding="utf-8") for path in input_files] == [
            '{"value":"one"}\n',
            '{"value":"two"}\n',
            '{"value":"three"}\n',
            '{"value":"bad"}\n',
        ]
        assert unrelated.isActive
        assert _active_run_ids(spark) == before
        assert caller_file.read_text(encoding="utf-8") == "preserve me"
        assert spark.range(1).count() == 1
    finally:
        unrelated.stop()


def test_pre_query_transform_failure_cannot_satisfy_expected_query_failure(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    original_start = _QueryLifecycle.start
    started: list[bool] = []

    def record_start(self: _QueryLifecycle, stream: DataFrame, *, action_index: int) -> None:
        started.append(True)
        original_start(self, stream, action_index=action_index)

    def invalid_transform(_source: DataFrame) -> DataFrame:
        raise AssertionError(_MARKER)

    monkeypatch.setattr(_QueryLifecycle, "start", record_start)
    with pytest.raises(AssertionError, match=_MARKER):
        run_scenario(
            spark,
            scenario(expect_failure(batch({"value": "bad"}), message_contains=_MARKER)),
            schema="value STRING",
            transform=invalid_transform,
            base_dir=tmp_path,
        )

    assert started == []
    _assert_no_runner_leaks(spark, tmp_path, before)


def test_earlier_action_query_failure_cannot_satisfy_later_declaration(
    spark: SparkSession, tmp_path: Path
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    with pytest.raises(RuntimeError, match="Batch action at index 0 failed"):
        run_scenario(
            spark,
            scenario(
                batch({"value": "bad"}),
                expect_failure(batch({"value": "bad"}), message_contains=_MARKER),
            ),
            schema="value STRING",
            transform=_reject_bad,
            base_dir=tmp_path,
        )

    _assert_no_runner_leaks(spark, tmp_path, before)


def test_terminal_input_limit_is_not_an_expected_query_failure(
    spark: SparkSession, tmp_path: Path
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    with pytest.raises(ScenarioLimitError, match="input_bytes_per_batch"):
        run_scenario(
            spark,
            scenario(expect_failure(batch({"value": "bad"}), message_contains=_MARKER)),
            schema="value STRING",
            transform=_reject_bad,
            base_dir=tmp_path,
            max_input_bytes_per_batch=1,
        )

    _assert_no_runner_leaks(spark, tmp_path, before)


def test_cleanup_failure_after_matched_query_failure_still_fails_and_cleans(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from streamcase.spark import run_scenario

    before = _active_run_ids(spark)
    owned: list[StreamingQuery] = []
    original_start = _QueryLifecycle.start
    original_stop = _QueryLifecycle.stop

    def record_start(self: _QueryLifecycle, stream: DataFrame, *, action_index: int) -> None:
        original_start(self, stream, action_index=action_index)
        owned.append(self.require_active())

    def stop_then_fail(self: _QueryLifecycle) -> None:
        original_stop(self)
        raise OSError("injected post-stop cleanup failure")

    monkeypatch.setattr(_QueryLifecycle, "start", record_start)
    monkeypatch.setattr(_QueryLifecycle, "stop", stop_then_fail)
    with pytest.raises(OSError, match="injected post-stop cleanup failure"):
        run_scenario(
            spark,
            scenario(expect_failure(batch({"value": "bad"}), message_contains=_MARKER)),
            schema="value STRING",
            transform=_reject_bad,
            base_dir=tmp_path,
        )

    assert len(owned) == 1
    assert not owned[0].isActive
    _assert_no_runner_leaks(spark, tmp_path, before)
