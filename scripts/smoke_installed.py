"""Smoke-test an installed Streamcase distribution outside the source tree."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import sys
from pathlib import Path

import streamcase
from streamcase import (
    CapturedBatch,
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
    batch,
    scenario,
)


def check_installation() -> None:
    """Require the package to come from this isolated environment."""
    package_path = Path(streamcase.__file__).resolve()
    environment_path = Path(sys.prefix).resolve()
    if not package_path.is_relative_to(environment_path):
        raise AssertionError(
            f"Imported Streamcase from {package_path}, outside environment {environment_path}"
        )

    installed_version = importlib.metadata.version("streamcase")
    if streamcase.__version__ != installed_version:
        raise AssertionError(
            f"Package version {streamcase.__version__!r} does not match "
            f"distribution version {installed_version!r}"
        )


def check_core() -> None:
    """Exercise public backend-independent APIs without importing PySpark."""
    if importlib.util.find_spec("pyspark") is not None:
        raise AssertionError("Core smoke environment unexpectedly contains PySpark")

    example = scenario(batch({"id": 1}))
    if len(example.actions) != 1:
        raise AssertionError("Scenario did not contain its batch action")

    result = ScenarioResult([CapturedBatch(0, [{"id": 1}])])
    assert_batch_count(result, 1)
    assert_rows_equal(result, [{"id": 1}])
    assert_unique_keys(result, "id")
    if "pyspark" in sys.modules:
        raise AssertionError("Importing core APIs loaded PySpark")


def check_spark() -> None:
    """Run a small two-batch example with the optional Spark dependency."""
    import pyspark
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    if not pyspark.__version__.startswith("4.2."):
        raise AssertionError(f"Expected PySpark 4.2.x, found {pyspark.__version__}")

    def transform(source: DataFrame) -> DataFrame:
        return source.filter("category = 'keep'").selectExpr("id")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-distribution-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        result = run_scenario(
            spark,
            scenario(
                batch({"id": 1, "category": "keep"}),
                batch({"id": 2, "category": "keep"}),
            ),
            schema="id LONG, category STRING",
            transform=transform,
        )
        assert_batch_count(result, 2)
        assert_rows_equal(result, [{"id": 1}, {"id": 2}])
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    """Run core or Spark checks from an isolated installation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("core", "spark"))
    arguments = parser.parse_args(argv)

    check_installation()
    if arguments.mode == "core":
        check_core()
    else:
        check_spark()

    print(
        f"Installed Streamcase {streamcase.__version__} {arguments.mode} smoke passed "
        f"on Python {sys.version_info.major}.{sys.version_info.minor}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
