from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import assert_batch_count, assert_rows_equal, batch, restart, scenario

pytestmark = pytest.mark.spark


def test_public_runner_uses_a_caller_owned_session_and_cleans_up(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-public-runner-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        with pytest.raises(TypeError, match="must return a DataFrame"):
            run_scenario(
                spark,
                scenario(batch({"id": 1})),
                schema="id LONG",
                transform=lambda _frame: None,
                base_dir=tmp_path,
            )
        assert list(tmp_path.glob("streamcase-run-*")) == []

        result = run_scenario(
            spark,
            scenario(batch({"id": 1})),
            schema="id LONG",
            transform=lambda frame: frame.selectExpr("id * 2 AS doubled"),
            base_dir=tmp_path,
        )

        assert_batch_count(result, 1)
        assert_rows_equal(result, [{"doubled": 2}])
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert spark.range(1).count() == 1
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
    finally:
        spark.stop()


def test_public_runner_rebuilds_after_restart_without_replaying_input(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-restart-smoke-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    transform_calls = 0

    def transform(source: DataFrame) -> DataFrame:
        nonlocal transform_calls
        transform_calls += 1
        return source.selectExpr("id * 2 AS doubled")

    try:
        result = run_scenario(
            spark,
            scenario(batch({"id": 1}), restart(), batch({"id": 2})),
            schema="id LONG",
            transform=transform,
            base_dir=tmp_path,
        )

        assert transform_calls == 2
        assert_batch_count(result, 2)
        assert_rows_equal(result, [{"doubled": 2}, {"doubled": 4}])
        assert [captured.batch_id for captured in result.batches] == [0, 1]
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
