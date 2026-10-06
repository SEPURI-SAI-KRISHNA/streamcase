"""Private ownership boundary for one Spark scenario run."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from streamcase._cleanup import _cleanup_on_exit
from streamcase._directories import RunDirectories, create_run_directories
from streamcase._spark_capture import _BatchCapture
from streamcase._spark_execution import _execute_batches
from streamcase._spark_query_options import _prepare_query_configuration
from streamcase.results import ScenarioResult
from streamcase.scenario import Scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _run_managed_batches(
    scenario: Scenario,
    build_stream: Callable[[RunDirectories], DataFrame],
    *,
    base_dir: str | os.PathLike[str] | None = None,
    retain_artifacts: bool = False,
    output_mode: str = "append",
    query_options: Mapping[str, str] | None = None,
) -> ScenarioResult:
    """Own run directories while a caller-owned session builds the stream."""
    approved_mode, approved_options = _prepare_query_configuration(output_mode, query_options)
    directories = create_run_directories(base_dir=base_dir, retain_artifacts=retain_artifacts)
    with _cleanup_on_exit("run directory", directories.cleanup):
        stream = build_stream(directories)
        return _execute_batches(
            stream,
            scenario,
            directories,
            _BatchCapture(),
            output_mode=approved_mode,
            query_options=approved_options,
            rebuild_stream=lambda: build_stream(directories),
        )
