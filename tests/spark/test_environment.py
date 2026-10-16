from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.spark


def test_supported_pyspark_starts_in_local_mode() -> None:
    pytest.importorskip("pyspark")
    from pyspark import __version__ as pyspark_version
    from pyspark.sql import SparkSession

    expected_minor = os.environ.get("STREAMCASE_TEST_SPARK_MINOR", "4.2")
    assert expected_minor in {"3.5", "4.2"}
    assert pyspark_version.startswith(f"{expected_minor}.")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        assert spark.version.startswith(f"{expected_minor}.")
        rows = spark.range(3).selectExpr("id * 2 AS value").collect()
        assert [row["value"] for row in rows] == [0, 2, 4]
    finally:
        spark.stop()
