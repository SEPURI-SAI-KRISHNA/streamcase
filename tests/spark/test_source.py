from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import batch
from streamcase._directories import create_run_directories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_source import _build_json_stream

pytestmark = pytest.mark.spark


def test_json_source_is_streaming_and_limits_each_trigger_to_one_file(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-source-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    directories = create_run_directories(base_dir=tmp_path)
    query = None
    captured_ids: list[tuple[int, ...]] = []

    def capture_ids(dataframe: DataFrame, _batch_id: int) -> None:
        captured_ids.append(tuple(row["id"] for row in dataframe.orderBy("id").collect()))

    try:
        stream = _build_json_stream(spark, directories, schema="id LONG")
        assert stream.isStreaming

        writer = AtomicBatchWriter(directories)
        writer.publish(batch({"id": 1}))
        writer.publish(batch({"id": 2}))

        query = (
            stream.writeStream.foreachBatch(capture_ids)
            .option("checkpointLocation", str(directories.checkpoint_dir))
            .start()
        )
        query.processAllAvailable()

        assert len(captured_ids) == 2
        assert all(len(batch_ids) <= 1 for batch_ids in captured_ids)
        assert sorted(row_id for batch_ids in captured_ids for row_id in batch_ids) == [1, 2]
    finally:
        if query is not None:
            query.stop()
        directories.cleanup()
        spark.stop()
