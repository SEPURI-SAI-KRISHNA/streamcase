from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_supported_pyspark_starts_in_local_mode() -> None:
    pytest.importorskip("pyspark")
    from pyspark import __version__ as pyspark_version
    from pyspark.sql import SparkSession

    assert pyspark_version.startswith("4.2.")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        assert spark.version.startswith("4.2.")
        rows = spark.range(3).selectExpr("id * 2 AS value").collect()
        assert [row["value"] for row in rows] == [0, 2, 4]
    finally:
        spark.stop()
