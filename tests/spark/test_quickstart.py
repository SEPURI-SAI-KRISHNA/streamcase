from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import assert_batch_count, assert_rows_equal, batch, scenario

pytestmark = pytest.mark.spark


def test_two_batch_public_runner_quickstart(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    def transform(source: DataFrame) -> DataFrame:
        return source.filter("category = 'keep'").selectExpr("id", "id * 10 AS scaled")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-two-batch-quickstart-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    caller_file = tmp_path / "caller-owned.txt"
    caller_file.write_text("keep me", encoding="utf-8")

    try:
        active_before = {query.id for query in spark.streams.active}
        result = run_scenario(
            spark,
            scenario(
                batch({"id": 1, "category": "keep"}, {"id": 2, "category": "drop"}),
                batch({"id": 3, "category": "keep"}),
            ),
            schema="id LONG, category STRING",
            transform=transform,
            base_dir=tmp_path,
        )

        assert_batch_count(result, 2)
        assert_rows_equal(result, [{"id": 1, "scaled": 10}, {"id": 3, "scaled": 30}])
        assert [captured.batch_id for captured in result.batches] == [0, 1]
        assert result.batches[0].rows == ({"id": 1, "scaled": 10},)
        assert result.batches[1].rows == ({"id": 3, "scaled": 30},)
        assert caller_file.read_text(encoding="utf-8") == "keep me"
        assert sorted(path.name for path in tmp_path.iterdir()) == ["caller-owned.txt"]
        assert {query.id for query in spark.streams.active} == active_before
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
