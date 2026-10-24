"""Smoke-test an installed Streamcase distribution outside the source tree."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import os
import sys
from collections.abc import Callable
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
    """Exercise public APIs without optional pytest or PySpark dependencies."""
    if importlib.util.find_spec("pytest") is not None:
        raise AssertionError("Core smoke environment unexpectedly contains pytest")
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

    expected_minor = os.environ.get("STREAMCASE_TEST_SPARK_MINOR", "4.2")
    if expected_minor not in {"3.5", "4.2"}:
        raise AssertionError(f"Unexpected Spark test minor: {expected_minor!r}")
    if not pyspark.__version__.startswith(f"{expected_minor}."):
        raise AssertionError(f"Expected PySpark {expected_minor}.x, found {pyspark.__version__}")

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


def test_installed_pytest_fixture(
    streamcase_runner: Callable[..., ScenarioResult], tmp_path: Path
) -> None:
    """Run an explicitly loaded pytest fixture from a clean distribution install."""
    check_installation()

    package_metadata = importlib.metadata.metadata("streamcase")
    extras = package_metadata.get_all("Provides-Extra") or []
    requirements = package_metadata.get_all("Requires-Dist") or []
    if "pytest" not in extras or not any(
        requirement.lower().startswith("pytest")
        and "extra == 'pytest'" in requirement.replace('"', "'")
        for requirement in requirements
    ):
        raise AssertionError("Installed distribution does not declare the pytest extra")

    import pyspark
    from pyspark.sql import SparkSession

    expected_minor = os.environ.get("STREAMCASE_TEST_SPARK_MINOR", "4.2")
    if expected_minor not in {"3.5", "4.2"}:
        raise AssertionError(f"Unexpected Spark test minor: {expected_minor!r}")
    if not pyspark.__version__.startswith(f"{expected_minor}."):
        raise AssertionError(f"Expected PySpark {expected_minor}.x, found {pyspark.__version__}")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-installed-pytest-smoke")
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
        if any(tmp_path.iterdir()):
            raise AssertionError("Pytest fixture left runner-owned files behind")
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
