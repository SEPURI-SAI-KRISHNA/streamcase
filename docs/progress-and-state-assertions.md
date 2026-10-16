# Progress and state assertions

Streamcase captures a small, immutable subset of Spark Structured Streaming
progress for each unambiguously matched, callback-backed trigger. The models
and assertions can also be used without PySpark. This guide describes the
current source on `main`; the published `0.1.0a2` prerelease predates these
progress APIs. Use an editable checkout to try them until a later release.

For the Spark runner, use one of the [tested Python 3.11 and Java 17 Spark
combinations](spark-compatibility.md). Progress assertions import no PySpark;
only `streamcase.spark` needs the optional `spark` or `spark35` extra.

## Core-only example

This example runs with the core package and no Spark session. It shows exact
input-row, watermark, and state-metric expectations, including a reported
zero that must not be confused with a missing metric.

```python
from datetime import datetime, timezone

from streamcase import (
    CapturedProgress,
    CapturedStateOperator,
    ScenarioResult,
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
)

expected_watermark = datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)
result = ScenarioResult(
    [],
    progress=[
        CapturedProgress(
            action_index=0,
            trigger_index=0,
            batch_id=7,
            run_id="example-run",
            num_input_rows=2,
            watermark=expected_watermark,
            state_operators=[
                CapturedStateOperator(
                    operator_name="stateStoreSave",
                    num_rows_total=1,
                    num_rows_updated=0,
                )
            ],
        )
    ],
)

assert_input_row_count(result, 2, action_index=0, trigger_index=0)
assert_watermark(result, expected_watermark, action_index=0, trigger_index=0)
assert_state_metric(
    result,
    "num_rows_updated",
    0,
    action_index=0,
    trigger_index=0,
    operator_index=0,
)
```

Successful assertions return `None`. A missing report or mismatched value
raises `AssertionError`; invalid expected values or indexes raise `TypeError`
or `ValueError`. The three assertions require `action_index` and
`trigger_index` as keywords. State metrics also require `operator_index`.

## Spark example (fixed event time)

Install the checkout with `python -m pip install -e ".[spark]"` for Spark 4.2
or `python -m pip install -e ".[spark35]"` for Spark 3.5.9+, and provide a
Java 17 JDK. This example uses fixed event-time input and Spark's
`processAllAvailable()` barrier, not sleeps or a wall-clock deadline.

```python
from pyspark.sql import DataFrame, SparkSession

from streamcase import (
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
    batch,
    scenario,
)
from streamcase.spark import run_scenario


def aggregate(source: DataFrame) -> DataFrame:
    return (
        source.selectExpr("timestamp_millis(event_ms) AS event_time")
        .withWatermark("event_time", "10 minutes")
        .groupBy("event_time")
        .count()
    )


spark = SparkSession.builder.master("local[2]").appName("streamcase-progress-guide").getOrCreate()
try:
    result = run_scenario(
        spark,
        scenario(batch({"event_ms": 1700000000000}), batch({"event_ms": 1700000300000})),
        schema="event_ms LONG",
        transform=aggregate,
        output_mode="complete",
    )

    assert_input_row_count(result, 1, action_index=0, trigger_index=0)
    assert_input_row_count(result, 1, action_index=1, trigger_index=0)

    first_operators = result.progress[0].state_operators
    assert first_operators is not None
    observed_rows_total = first_operators[0].num_rows_total
    assert observed_rows_total is not None
    assert_state_metric(
        result,
        "num_rows_total",
        observed_rows_total,
        action_index=0,
        trigger_index=0,
        operator_index=0,
    )

    reported = next((item for item in result.progress if item.watermark is not None), None)
    assert reported is not None
    assert reported.watermark is not None
    assert_watermark(
        result,
        reported.watermark,
        action_index=reported.action_index,
        trigger_index=reported.trigger_index,
    )
finally:
    spark.stop()
```

The state-metric and watermark calls above demonstrate selection using values
Spark actually reported. For a meaningful application test, replace those
observed values with your pipeline's expected values. Do not assume a
watermark advances on every logical `Batch`; Spark's event-time progress and
no-data triggers are engine-controlled. The
[Spark integration tests](../tests/spark/test_spark_progress_assertions.py)
exercise this fixed-event-time pattern and assertion failures.

## Identity and restart boundaries

`result.progress` is an immutable tuple in observation order, separate from
`result.batches` and `result.rows`. Each `CapturedProgress` has:

- `action_index`: zero-based position in the scenario's actions;
- `trigger_index`: zero-based position among callback-backed triggers for that
  action;
- `batch_id`: Spark's micro-batch identifier, not the action index;
- `run_id`: the query execution identifier, which changes after a restart.

For `scenario(batch(...), restart(), batch(...))`, the two batch actions are at
indexes `0` and `2`; index `1` is the `Restart` action and has no progress
entry. After a restart, Spark can continue batch IDs from the checkpoint while
the run ID changes. Select by `(action_index, trigger_index)`, never by tuple
position, batch ID alone, or an implicit "latest" report. A missing progress
report leaves a gap rather than renumbering later triggers. See the
[restart guide](restart-scenarios.md) for a runnable checkpoint example.

## Availability is not zero

| Captured field | Reported value | Unavailable value |
| --- | --- | --- |
| `num_input_rows` | A non-negative input count, including `0` | `None`; `assert_input_row_count(..., 0)` fails |
| `watermark` | A timezone-aware UTC `datetime` | `None`; `assert_watermark(..., None)` passes only if the selected report exists |
| `state_operators` | A tuple in report order; `()` means Spark reported no operators | `None` means the collection was not reported |
| An operator metric | A non-negative count, including `0` | `None`; `assert_state_metric(..., 0)` fails |

The five supported state metrics are `num_rows_total`, `num_rows_updated`,
`num_rows_removed`, `num_rows_dropped_by_watermark`, and `memory_used_bytes`.
Select an operator by its zero-based index within one report. An operator name
is useful context, but names can repeat or change with the query plan; indexes
are not guaranteed stable across plan changes or restarts. A stateless query
normally reports `state_operators=()` and has no operator index to assert.

Input-row counts refer to source rows processed by Spark, not rows emitted by
`foreachBatch`. Watermarks compare instants after UTC normalization; expected
datetimes must be timezone-aware. An expected `None` checks the absence of a
watermark **in an existing report**, not the absence of a report itself.

## Failures and limits

Progress assertion failures have four lines: subject, action/trigger and
available Spark context, expected value, and actual or unavailable value. For
example, a reported count of `1` when `2` was expected looks like:

```text
Progress assertion failed: input-row count.
Context: action 0, trigger 0, Spark batch 7, run 'example-run'.
Expected: 2.
Actual: 1.
```

The runner only snapshots new reports it can unambiguously associate with
`foreachBatch` callbacks. It does not promise one progress entry per scenario
action, capture idle-only/no-data updates, expose raw Spark payloads, or infer
missing fields from output rows or checkpoints. Spark's `recentProgress`
history is bounded, and ambiguous matches fail explicitly. These assertions
are testing tools, not production monitoring or watermark-control helpers.
For the design rationale and field-level contract, see
[ADR 0003](design/0003-progress-state-api.md).
