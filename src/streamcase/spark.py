"""Public Apache Spark Structured Streaming runner."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Literal

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType
except ModuleNotFoundError as error:
    if (error.name or "").partition(".")[0] != "pyspark":
        raise
    raise ImportError(
        'PySpark is required for streamcase.spark; install "streamcase[spark]".'
    ) from error

from streamcase._directories import RunDirectories
from streamcase._spark_managed import _run_managed_batches
from streamcase._spark_query_options import _prepare_query_configuration
from streamcase._spark_source import _build_json_stream, _copy_source_options
from streamcase.results import ScenarioResult
from streamcase.scenario import Scenario

__all__ = ["run_scenario"]


def _validate_output_frame(output: object, spark: SparkSession) -> DataFrame:
    if not isinstance(output, DataFrame):
        raise TypeError("Spark transformation must return a DataFrame.")
    if not output.isStreaming:
        raise ValueError("Spark transformation must return a streaming DataFrame.")
    if output.sparkSession is not spark:
        raise ValueError("Spark transformation output must belong to the supplied session.")

    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in output.columns:
        normalized = name.casefold()
        if normalized in seen:
            duplicates.add(name)
        seen.add(normalized)
    if duplicates:
        rendered = ", ".join(repr(name) for name in sorted(duplicates, key=str.casefold))
        raise ValueError(f"Spark transformation output has duplicate column names: {rendered}.")
    return output


def run_scenario(
    spark: SparkSession,
    scenario: Scenario,
    *,
    schema: StructType | str,
    transform: Callable[[DataFrame], DataFrame],
    output_mode: Literal["append", "complete", "update"] = "append",
    source_options: Mapping[str, str] | None = None,
    query_options: Mapping[str, str] | None = None,
    base_dir: str | os.PathLike[str] | None = None,
    retain_artifacts: bool = False,
) -> ScenarioResult:
    """Run a scenario using a caller-owned Spark session."""
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a PySpark SparkSession.")
    if not isinstance(scenario, Scenario):
        raise TypeError("scenario must be a Streamcase Scenario.")
    if not isinstance(schema, (StructType, str)):
        raise TypeError("schema must be a Spark StructType or DDL string.")
    if isinstance(schema, str) and not schema.strip():
        raise ValueError("The Spark input schema must not be empty.")
    if not callable(transform):
        raise TypeError("transform must be callable.")

    approved_source_options = _copy_source_options(source_options)
    approved_mode, approved_query_options = _prepare_query_configuration(output_mode, query_options)

    def build_stream(directories: RunDirectories) -> DataFrame:
        source = _build_json_stream(
            spark,
            directories,
            schema=schema,
            source_options=approved_source_options,
        )
        return _validate_output_frame(transform(source), spark)

    return _run_managed_batches(
        scenario,
        build_stream,
        base_dir=base_dir,
        retain_artifacts=retain_artifacts,
        output_mode=approved_mode,
        query_options=approved_query_options,
    )
