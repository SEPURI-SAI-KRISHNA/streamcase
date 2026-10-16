from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from streamcase import ScenarioResult, assert_rows_equal, batch, scenario

pytest_plugins = ("streamcase.pytest_plugin",)
pytestmark = pytest.mark.spark


def test_fixture_runs_scenario_with_caller_owned_session(
    streamcase_runner: Callable[..., ScenarioResult], tmp_path: Path
) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-pytest-plugin-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        result = streamcase_runner(
            spark,
            scenario(batch({"id": 1})),
            schema="id LONG",
            transform=lambda source: source.selectExpr("id"),
        )

        assert_rows_equal(result, [{"id": 1}])
        assert not any(tmp_path.iterdir())
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
