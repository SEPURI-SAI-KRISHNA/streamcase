from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import batch, scenario
from streamcase._directories import RunDirectories
from streamcase._spark_managed import _run_managed_batches
from streamcase._spark_source import _build_json_stream

pytestmark = pytest.mark.spark


def test_failed_spark_runs_release_queries_and_run_directories(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-cleanup-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    caller_file = tmp_path / "caller-owned.txt"
    caller_file.write_text("preserve me", encoding="utf-8")

    def fail_during_build(_directories: RunDirectories) -> DataFrame:
        raise AssertionError("injected transformation failure")

    def build_json_stream(directories: RunDirectories) -> DataFrame:
        return _build_json_stream(spark, directories, schema="id LONG")

    try:
        with pytest.raises(AssertionError, match="injected transformation failure"):
            _run_managed_batches(
                scenario(batch({"id": 1})),
                fail_during_build,
                base_dir=tmp_path,
            )
        assert list(tmp_path.glob("streamcase-run-*")) == []

        with pytest.raises(RuntimeError, match="Batch action at index 1 failed"):
            _run_managed_batches(
                scenario(batch({"id": 1}), batch({"unsupported": object()})),
                build_json_stream,
                base_dir=tmp_path,
            )

        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
        assert caller_file.read_text(encoding="utf-8") == "preserve me"
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
