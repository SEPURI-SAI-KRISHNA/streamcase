# ADR 0003: Progress and state assertion API

- Status: Accepted
- Decision scope: Phase 5
- Related issue: #100

## Context

`ScenarioResult` currently captures output from `foreachBatch`, but not Spark's
per-trigger progress. Phase 5 needs input-row, watermark, and state-operator
assertions without leaking a live query or requiring PySpark for core tests.

Spark's `processAllAvailable()` is a testing barrier for available source data,
not a promise of one progress update per Streamcase `Batch`. `lastProgress` may
be absent, and `recentProgress` is bounded by a Spark session setting. Progress
can include no-data triggers; Spark may reuse a batch ID when no data is
processed. A restart changes the query run ID. Therefore action index, Spark
batch ID, and progress-list position are different concepts. See the
[Spark query API](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html),
[progress model](https://spark.apache.org/docs/4.2.0/api/python/_modules/pyspark/sql/streaming/listener.html),
and [recent-progress API](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.recentProgress.html).

## Decision

Phase 5 adds two immutable, backend-independent public models, an optional
collection on `ScenarioResult`, and three free assertions:

```python
from streamcase import (
    CapturedProgress,
    CapturedStateOperator,
    assert_input_row_count,
    assert_state_metric,
    assert_watermark,
)

result = ScenarioResult(batches, progress=())
```

The existing `ScenarioResult(batches)` constructor and `run_scenario(...)`
signature remain valid. `result.progress` is an immutable tuple in observation
order, independent of `result.batches`. Core models and assertions import no
PySpark; only the optional Spark runner reads query progress.

## Snapshot contract

```python
CapturedProgress(
    action_index: int,
    trigger_index: int,
    batch_id: int,
    run_id: str,
    num_input_rows: int | None = None,
    watermark: datetime | None = None,
    state_operators: Iterable[CapturedStateOperator] | None = None,
)

CapturedStateOperator(
    operator_name: str | None = None,
    num_rows_total: int | None = None,
    num_rows_updated: int | None = None,
    num_rows_removed: int | None = None,
    num_rows_dropped_by_watermark: int | None = None,
    memory_used_bytes: int | None = None,
)
```

- `action_index` is the zero-based position in `Scenario.actions` of the
  `Batch` whose barrier observed the trigger. A `Restart` has no progress entry.
- `trigger_index` is zero-based within that action's **callback-backed**
  triggers. It follows callback order; a missing progress report leaves a gap
  rather than renumbering later triggers.
- `batch_id` is Spark's micro-batch identifier, also used by the corresponding
  `CapturedBatch`; it is not the scenario action index. `run_id` distinguishes
  query runs across checkpoint-preserving restarts. Neither is assumed unique
  alone across every possible Spark progress event.
- `num_input_rows` is Spark's aggregate input-row count for that trigger, not
  the number of output rows. Zero is a real observation; `None` means the
  documented field was unavailable.
- `watermark` is the observed `eventTime.watermark`, normalized to an aware UTC
  `datetime`. `None` means no watermark was reported. It does not mean the
  watermark is at the epoch or has advanced.
- `state_operators` is a tuple in Spark report order when the field is present.
  An empty tuple means Spark reported no state operators; `None` means the
  collection was unavailable. Each metric is independently optional, and zero
  is distinct from `None`. `operator_name` is for inspection and diagnostics,
  not a stable selector.

Constructors reject negative identifiers and counts, booleans masquerading as
integers, naive datetimes, invalid operator values, and mutable or non-model
members. The snapshot uses value equality and retains no Spark Row, DataFrame,
query, JVM object, or mutable metric mapping. Unsupported or malformed fields
are not silently converted to zero.

## Capture ownership and limits

After each `Batch` action's `processAllAvailable()` returns, the Spark runner
compares newly captured `foreachBatch` callbacks with that run's progress
reports. It snapshots only reports that can be unambiguously matched to those
callbacks by run and Spark batch identity, before another action or restart
changes the query. It never reuses `lastProgress` for a different action and
never invents a report for an unmatched callback. A missing report leaves no
`CapturedProgress` entry; an assertion targeting it fails as unavailable.
If no-data and data reports reuse a batch ID, a unique report with positive
`numInputRows` identifies the data trigger. Otherwise ambiguous matches fail
capture explicitly instead of assigning metrics to the wrong action.

This first API intentionally excludes background/no-data progress with no
matching `foreachBatch` callback. In particular, it does not guarantee capture
of every trigger or watermark advancement caused solely by idle triggers.
Spark's bounded `recentProgress` history may also omit older reports; the
runner must not represent such omissions as zero. Issue #102 must prove the
matching behavior in the supported Spark lane before this contract ships.

## Assertion selection and outcomes

Assertions select one progress snapshot by `(action_index, trigger_index)`.
Both indexes are required keyword arguments; there is no implicit "latest"
selection. A `Restart` index, missing report, or duplicate identity is an
assertion failure with action/trigger context. Invalid index types or negative
indexes are API errors (`TypeError` or `ValueError`). Successful assertions
return `None`; mismatches raise `AssertionError`.

```python
assert_input_row_count(result, 3, action_index=0, trigger_index=0)
assert_watermark(result, expected_utc_time, action_index=0, trigger_index=0)
assert_watermark(result, None, action_index=0, trigger_index=0)
assert_state_metric(
    result,
    "num_rows_total",
    2,
    action_index=0,
    trigger_index=0,
    operator_index=0,
)
```

`assert_input_row_count` compares a non-negative integer exactly and fails
distinctly when `num_input_rows is None`.

`assert_watermark` compares instants exactly after normalizing an aware
`datetime` to UTC. An expected `None` asserts that the selected report lacks a
watermark; it does **not** pass when the report itself is missing. It neither
controls Spark's clock nor assumes the watermark advances every trigger.

`assert_state_metric` selects the zero-based `operator_index` within the
snapshot's state-operator tuple. It accepts only the five metric names listed
in `CapturedStateOperator`, with a non-negative integer expected value. Missing
operator collection, out-of-range operator, and unavailable metric fail
distinctly from a numeric mismatch. Operator indexes are local to one report;
tests must not assume an index or name stays stable after a plan change or
restart. All failures show the selected action, trigger, Spark batch, run, and
operator context where available. Issue #107 standardizes wording and bounds
payload size.

## Version policy

The initial normalization target is the currently supported Spark 4.2 line.
Only Spark-documented fields above become public; vendor-specific custom
metrics, timing fields, source offsets, and raw progress payloads do not.
Unknown extra fields are ignored. Absent documented fields remain `None`, even
if another Spark version usually supplies them. Any supported-version change
requires focused compatibility tests and an issue-linked review; the separate
Spark 3.5 assessment (#110) does not imply 3.5 support today.

## Alternatives considered

- Returning `lastProgress` once per action: rejected because it can be absent
  or refer to a later no-data trigger, and cannot represent multiple triggers.
- Exposing raw Spark progress dictionaries: rejected because fields vary and
  can contain backend-owned objects or undocumented metrics.
- Selecting state operators by name: rejected because names may repeat or
  change with a query plan. Explicit index is inspectable and unambiguous
  within one report.
- Inferring input counts or state from output rows/checkpoints: rejected because
  output modes and stateful operations do not preserve that relationship.

## Consequences

- Issues #101-#103 implement the models, capture, and normalization separately.
- Issues #104-#106 add assertions against the fixed names and semantics above.
- Users can construct snapshots without Spark and inspect missing values
  honestly, but cannot yet assert no-data-only progress or every engine metric.
