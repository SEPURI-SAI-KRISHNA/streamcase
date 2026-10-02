from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from streamcase import batch
from streamcase._directories import create_run_directories
from streamcase._input_files import AtomicBatchWriter
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_source import _build_json_stream

pytestmark = pytest.mark.spark


def _contains_pyspark_value(value: object) -> bool:
    if type(value).__module__.startswith("pyspark"):
        return True
    if isinstance(value, Mapping):
        return any(
            _contains_pyspark_value(key) or _contains_pyspark_value(nested)
            for key, nested in value.items()
        )
    if isinstance(value, tuple):
        return any(_contains_pyspark_value(nested) for nested in value)
    return False


def test_foreach_batch_capture_freezes_nested_values_and_keeps_empty_batches(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-capture-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    directories = create_run_directories(base_dir=tmp_path)
    capture = _BatchCapture()
    query = None

    schema = """
        keep BOOLEAN,
        id LONG,
        name STRING,
        score DOUBLE,
        amount DECIMAL(10, 2),
        created_on DATE,
        tags ARRAY<STRING>,
        details STRUCT<active: BOOLEAN, count: LONG>,
        attributes MAP<STRING, STRING>
    """

    try:
        stream = _build_json_stream(spark, directories, schema=schema).where("keep").drop("keep")
        query = (
            stream.writeStream.foreachBatch(capture.callback)
            .option("checkpointLocation", str(directories.checkpoint_dir))
            .start()
        )

        writer = AtomicBatchWriter(directories)
        writer.publish(
            batch(
                {
                    "keep": True,
                    "id": 1,
                    "name": "created",
                    "score": 1.5,
                    "amount": 10.5,
                    "created_on": "2026-10-02",
                    "tags": ["new", "priority"],
                    "details": {"active": True, "count": 2},
                    "attributes": {"region": "eu"},
                }
            )
        )
        query.processAllAvailable()

        writer.publish(batch({"keep": False, "id": 2}))
        query.processAllAvailable()

        captured = capture.snapshot()
        assert [captured_batch.batch_id for captured_batch in captured] == [0, 1]
        assert captured[0].rows == (
            {
                "id": 1,
                "name": "created",
                "score": 1.5,
                "amount": Decimal("10.50"),
                "created_on": date(2026, 10, 2),
                "tags": ("new", "priority"),
                "details": {"active": True, "count": 2},
                "attributes": {"region": "eu"},
            },
        )
        assert captured[1].rows == ()
        assert not any(
            _contains_pyspark_value(row)
            for captured_batch in captured
            for row in captured_batch.rows
        )
    finally:
        if query is not None:
            query.stop()
        directories.cleanup()
        spark.stop()
