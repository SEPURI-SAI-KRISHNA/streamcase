from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import ScenarioLimitError, assert_rows_equal, batch, restart, scenario
from streamcase._serialization import _encode_batch_json_lines

pytestmark = pytest.mark.spark


def test_public_input_budget_counts_nested_utf8_bytes_and_cleans_failed_run(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-input-budget-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    action = batch({"city": "東京", "details": {"label": "é"}})
    payload_size = len(_encode_batch_json_lines(action).encode("utf-8"))

    try:
        with pytest.raises(ValueError, match="max_input_bytes_per_batch"):
            run_scenario(
                spark,
                scenario(action),
                schema="city STRING, details STRUCT<label: STRING>",
                transform=lambda source: source,
                max_input_bytes_per_batch=0,
                base_dir=tmp_path,
            )
        assert list(tmp_path.glob("streamcase-run-*")) == []

        result = run_scenario(
            spark,
            scenario(action),
            schema="city STRING, details STRUCT<label: STRING>",
            transform=lambda source: source.selectExpr("city", "details.label AS label"),
            max_input_bytes_per_batch=payload_size,
            max_input_bytes_per_run=payload_size,
            base_dir=tmp_path,
        )
        assert_rows_equal(result, [{"city": "東京", "label": "é"}])

        with pytest.raises(ScenarioLimitError, match="input row 0") as error_info:
            run_scenario(
                spark,
                scenario(action),
                schema="city STRING, details STRUCT<label: STRING>",
                transform=lambda source: source,
                max_input_bytes_per_batch=payload_size - 1,
                base_dir=tmp_path,
            )

        error = error_info.value
        assert (error.resource, error.limit, error.observed_at_least) == (
            "input_bytes_per_batch",
            payload_size - 1,
            payload_size,
        )
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert spark.range(1).count() == 1
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
    finally:
        spark.stop()


def test_public_run_byte_budget_survives_checkpoint_restart(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-input-run-budget-restart-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    steps = scenario(batch({"id": 1}), restart(), batch({"id": 2}))

    try:
        with pytest.raises(ScenarioLimitError, match="Batch action at index 2") as error_info:
            run_scenario(
                spark,
                steps,
                schema="id LONG",
                transform=lambda source: source,
                max_input_bytes_per_run=9,
                base_dir=tmp_path,
            )

        assert error_info.value.resource == "input_bytes_per_run"
        assert error_info.value.observed_at_least == 18
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
