"""Real-Spark progress assertions without timing-based synchronization."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from streamcase import (
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
    batch,
    scenario,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-progress-assertions-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _active_run_ids(spark: SparkSession) -> set[str]:
    return {query.runId for query in spark.streams.active}


def test_stateless_progress_counts_and_absent_metrics(spark: SparkSession, tmp_path: Path) -> None:
    from streamcase.spark import run_scenario

    active_before = _active_run_ids(spark)
    result = run_scenario(
        spark,
        scenario(batch({"id": 1}, {"id": 2}), batch({"id": 3})),
        schema="id LONG",
        transform=lambda source: source.selectExpr("id"),
        base_dir=tmp_path,
    )

    # The file source processes one published file per action. No event-time or
    # stateful operator is in this query plan, so those report fields are absent.
    assert [(item.action_index, item.trigger_index) for item in result.progress] == [(0, 0), (1, 0)]
    assert_input_row_count(result, 2, action_index=0, trigger_index=0)
    assert_input_row_count(result, 1, action_index=1, trigger_index=0)
    assert_watermark(result, None, action_index=0, trigger_index=0)
    assert result.progress[0].state_operators == ()

    with pytest.raises(AssertionError) as count_error:
        assert_input_row_count(result, 3, action_index=0, trigger_index=0)
    assert "Context: action 0, trigger 0, Spark batch" in str(count_error.value)
    assert "Expected: 3.\nActual: 2." in str(count_error.value)

    with pytest.raises(AssertionError) as operator_error:
        assert_state_metric(
            result, "num_rows_total", 0, action_index=0, trigger_index=0, operator_index=0
        )
    assert "operator index 0 out of range (0 operator(s) reported)" in str(operator_error.value)

    with pytest.raises(AssertionError) as watermark_error:
        assert_watermark(
            result,
            datetime(2026, 10, 13, tzinfo=timezone.utc),
            action_index=0,
            trigger_index=0,
        )
    assert "Actual: None." in str(watermark_error.value)
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before


def test_fixed_event_time_exposes_watermark_and_state_metrics(
    spark: SparkSession, tmp_path: Path
) -> None:
    from streamcase.spark import run_scenario

    active_before = _active_run_ids(spark)

    def aggregate(source: DataFrame) -> DataFrame:
        # Fixed milliseconds make the input deterministic; processAllAvailable
        # is the only synchronization barrier. No wall-clock sleep is needed.
        return (
            source.selectExpr("timestamp_millis(event_ms) AS event_time")
            .withWatermark("event_time", "10 minutes")
            .groupBy("event_time")
            .count()
        )

    result = run_scenario(
        spark,
        scenario(batch({"event_ms": 1700000000000}), batch({"event_ms": 1700000300000})),
        schema="event_ms LONG",
        transform=aggregate,
        output_mode="complete",
        base_dir=tmp_path,
    )

    assert [(item.action_index, item.trigger_index) for item in result.progress] == [(0, 0), (1, 0)]
    assert_input_row_count(result, 1, action_index=0, trigger_index=0)
    assert_input_row_count(result, 1, action_index=1, trigger_index=0)
    assert all(item.state_operators for item in result.progress)
    for action_index, captured in enumerate(result.progress):
        assert captured.state_operators is not None
        count = captured.state_operators[0].num_rows_total
        assert count is not None
        assert_state_metric(
            result,
            "num_rows_total",
            count,
            action_index=action_index,
            trigger_index=0,
            operator_index=0,
        )

    first_operators = result.progress[0].state_operators
    assert first_operators is not None
    first_count = first_operators[0].num_rows_total
    assert first_count is not None
    with pytest.raises(AssertionError) as metric_error:
        assert_state_metric(
            result,
            "num_rows_total",
            first_count + 1,
            action_index=0,
            trigger_index=0,
            operator_index=0,
        )
    assert f"Expected: {first_count + 1}.\nActual: {first_count}." in str(metric_error.value)

    # Spark reports its observed watermark; do not assume a particular trigger
    # advances it or assert a wall-clock-dependent value.
    observed = [(index, item.watermark) for index, item in enumerate(result.progress)]
    reported = [(index, value) for index, value in observed if value is not None]
    assert reported, "Spark did not report a watermark for the fixed-event-time query."
    action_index, watermark = reported[0]
    assert watermark is not None
    assert watermark.tzinfo == timezone.utc
    assert_watermark(result, watermark, action_index=action_index, trigger_index=0)

    with pytest.raises(AssertionError) as watermark_error:
        assert_watermark(
            result,
            watermark + timedelta(seconds=1),
            action_index=action_index,
            trigger_index=0,
        )
    assert "Progress assertion failed: watermark." in str(watermark_error.value)
    assert f"Context: action {action_index}, trigger 0, Spark batch" in str(watermark_error.value)
    assert "Expected:" in str(watermark_error.value)
    assert "Actual:" in str(watermark_error.value)
    assert list(tmp_path.glob("streamcase-run-*")) == []
    assert _active_run_ids(spark) == active_before
