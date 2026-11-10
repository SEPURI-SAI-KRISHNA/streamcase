"""Deterministic testing for Apache Spark Structured Streaming."""

from streamcase._progress_assertions import (
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
)
from streamcase._version import __version__
from streamcase.actions import Batch, ExpectFailure, Restart, batch, expect_failure, restart
from streamcase.assertions import assert_batch_count, assert_rows_equal, assert_unique_keys
from streamcase.failures import ExpectedFailureNotObservedError, FailureObservation
from streamcase.limits import ScenarioLimitError
from streamcase.progress import CapturedProgress, CapturedStateOperator
from streamcase.results import CapturedBatch, ScenarioResult
from streamcase.scenario import Action, Scenario, scenario

__all__ = [
    "Action",
    "Batch",
    "CapturedBatch",
    "CapturedProgress",
    "CapturedStateOperator",
    "ExpectFailure",
    "ExpectedFailureNotObservedError",
    "FailureObservation",
    "Restart",
    "Scenario",
    "ScenarioLimitError",
    "ScenarioResult",
    "__version__",
    "assert_batch_count",
    "assert_input_row_count",
    "assert_rows_equal",
    "assert_state_metric",
    "assert_unique_keys",
    "assert_watermark",
    "batch",
    "expect_failure",
    "restart",
    "scenario",
]
