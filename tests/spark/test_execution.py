from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import (
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
    batch,
    scenario,
)
from streamcase._directories import create_run_directories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_execution import _execute_batches
from streamcase._spark_source import _build_json_stream

pytestmark = pytest.mark.spark


def test_batch_actions_produce_distinct_spark_micro_batches(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-batch-execution-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    directories = create_run_directories(base_dir=tmp_path)
    capture = _BatchCapture()

    try:
        stream = _build_json_stream(spark, directories, schema="id LONG")
        result = _execute_batches(
            stream,
            scenario(batch({"id": 1}), batch({"id": 2})),
            directories,
            capture,
        )

        assert isinstance(result, ScenarioResult)
        assert [item.batch_id for item in result.batches] == [0, 1]
        assert_batch_count(result, 2)
        assert_rows_equal(result, [{"id": 1}, {"id": 2}])
        assert_unique_keys(result, "id")
        assert all(
            type(value).__module__ == "builtins" for row in result.rows for value in row.values()
        )
        assert sorted(path.name for path in directories.input_dir.iterdir()) == [
            "batch-00000000000000000000.json",
            "batch-00000000000000000001.json",
        ]
    finally:
        directories.cleanup()
        spark.stop()
