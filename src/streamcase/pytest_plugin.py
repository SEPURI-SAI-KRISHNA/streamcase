"""Opt-in pytest fixture for the public Spark scenario runner."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pytest

from streamcase.results import ScenarioResult
from streamcase.scenario import Scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


@pytest.fixture
def streamcase_runner(tmp_path: Path) -> Callable[..., ScenarioResult]:
    """Bind each scenario run to this test's temporary directory."""

    def run(
        spark: SparkSession,
        scenario: Scenario,
        *,
        schema: StructType | str,
        transform: Callable[[DataFrame], DataFrame],
        output_mode: Literal["append", "complete", "update"] = "append",
        source_options: Mapping[str, str] | None = None,
        query_options: Mapping[str, str] | None = None,
        retain_artifacts: bool = False,
        max_output_rows: int | None = 10_000,
    ) -> ScenarioResult:
        from streamcase.spark import run_scenario

        return run_scenario(
            spark,
            scenario,
            schema=schema,
            transform=transform,
            output_mode=output_mode,
            source_options=source_options,
            query_options=query_options,
            base_dir=tmp_path,
            retain_artifacts=retain_artifacts,
            max_output_rows=max_output_rows,
        )

    return run
