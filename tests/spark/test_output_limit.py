from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import ScenarioLimitError, assert_rows_equal, batch, restart, scenario

pytestmark = pytest.mark.spark


def test_public_output_limit_accepts_boundary_and_rejects_later_batch(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-output-limit-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    def transform(source: DataFrame) -> DataFrame:
        return source.where("keep").selectExpr("id")

    try:
        accepted = run_scenario(
            spark,
            scenario(
                batch({"id": 1, "keep": True}),
                batch({"id": 2, "keep": False}),
                batch({"id": 3, "keep": True}),
            ),
            schema="id LONG, keep BOOLEAN",
            transform=transform,
            max_output_rows=2,
            base_dir=tmp_path,
        )

        assert [len(item.rows) for item in accepted.batches] == [1, 0, 1]
        assert_rows_equal(accepted, [{"id": 1}, {"id": 3}])

        with pytest.raises(ScenarioLimitError, match="Batch action at index 1") as error_info:
            run_scenario(
                spark,
                scenario(batch({"id": 1, "keep": True}), batch({"id": 2, "keep": True})),
                schema="id LONG, keep BOOLEAN",
                transform=transform,
                max_output_rows=1,
                base_dir=tmp_path,
            )

        error = error_info.value
        assert (error.resource, error.limit, error.observed_at_least) == ("output_rows", 1, 2)
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert spark.range(1).count() == 1
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
    finally:
        spark.stop()


def test_output_limit_accumulates_across_restart_and_invalid_value_has_no_side_effects(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-output-limit-restart-integration-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    steps = scenario(batch({"id": 1}), restart(), batch({"id": 2}))

    try:
        with pytest.raises(ValueError, match="max_output_rows must be a positive integer"):
            run_scenario(
                spark,
                steps,
                schema="id LONG",
                transform=lambda source: source,
                max_output_rows=0,
                base_dir=tmp_path,
            )
        assert list(tmp_path.glob("streamcase-run-*")) == []

        with pytest.raises(ScenarioLimitError, match="Batch action at index 2") as error_info:
            run_scenario(
                spark,
                steps,
                schema="id LONG",
                transform=lambda source: source,
                max_output_rows=1,
                base_dir=tmp_path,
            )

        assert error_info.value.observed_at_least == 2
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
