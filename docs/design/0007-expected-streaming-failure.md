# ADR 0007: Explicit expected streaming failures

- Status: Accepted; runner execution implemented in source for planned 0.3.0a1
- Decision scope: Phase 8
- Related issue: #172

## Context

A negative streaming test needs to prove that a particular input makes the
*query* fail. Today `run_scenario` raises on any failed action, so a test can
only catch an exception outside the runner. That loses the distinction between
the intended query failure and unrelated input publication, capture, limit,
restart, setup, or cleanup failures. It also hides safely completed output and
progress from earlier actions.

The runner already owns one query at a time, including across checkpoint-
preserving `Restart` actions. The caller owns the Spark session. The public
result models and assertions do not import PySpark. These boundaries remain in
force. In particular, the Phase 7 [bounded-capture policy](0005-bounded-capture-policy.md)
must not be bypassed by a negative test.

Spark exposes `StreamingQuery.exception()` as the exception that terminated a
query, or `None` if there is no such failure, in its
[3.5 API](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.exception.html).
The runner must inspect **its owned query**, not infer query termination merely
from an exception thrown by `processAllAvailable()`. Spark's error condition
APIs differ by version: [3.5 documents `getErrorClass()`](https://spark.apache.org/docs/3.5.9/api/python/reference/api/pyspark.errors.PySparkException.getErrorClass.html),
while [4.2 documents `getCondition()`](https://spark.apache.org/docs/4.2.0/api/python/reference/api/pyspark.errors.PySparkException.getCondition.html).
Neither arbitrary exception wording nor an inner cause's condition is a
cross-version Streamcase guarantee.

## Decision

Introduce a terminal `ExpectFailure` scenario action that **wraps one `Batch`**.
The wrapped batch is published and processed exactly once at the wrapper's
zero-based action index. It is not an extra execution step. A scenario may have
at most one `ExpectFailure`, and it must be the final action. A `Restart` may
precede it, but may not follow it. As with ordinary scenarios, the first action
cannot be `Restart`; placement and selector validation happen before Spark is
started.

The declaration requires at least one non-empty, case-sensitive selector:
`message_contains` or `condition`. If both are provided, **both** must match.
There is no catch-all selector, regular expression, exception-type predicate,
or user callback. `message_contains` is an exact substring of the query's
observed exception text; the caller must choose a distinctive marker it
controls rather than rely on Spark's changing prose. `condition` matches only
the top-level structured condition of the query exception when Spark exposes
one. A missing condition never matches. A generic wrapper condition such as
`STREAM_FAILED` is not sufficient evidence of a specific inner error and may
only be used when paired with a distinctive `message_contains` marker. Other
condition-only selectors are valid only when Spark supplies that condition on
the owned query exception. Selectors are
snapshotted as strings and never applied to runner-generated error messages.

Illustrative source-checkout usage (not available in published 0.2.0a1):

```python
from streamcase import batch, expect_failure, restart, scenario
from streamcase.spark import run_scenario

case = scenario(
    batch({"value": "good"}),
    restart(),
    expect_failure(
        batch({"value": "bad"}),
        message_contains="INVALID_RECORD_MARKER",
    ),
)
result = run_scenario(spark, case, schema="value STRING", transform=reject_bad_records)
assert result.failure is not None
assert result.failure.action_index == 2
```

`reject_bad_records` is an illustrative caller-owned transformation that
causes the query to fail with the stated marker on the second input. The code
above describes the source API, not an example runnable against the current
published release. Issue #178 will provide a complete, CI-executed example.

## Outcome and result shape

An ordinary successful scenario still returns `ScenarioResult` with
`failure=None`. An expected failure that actually matches returns the same
backend-independent result type with an immutable `FailureObservation` in
`result.failure`:

```python
FailureObservation(
    action_index: int,
    kind: Literal["query_terminated"],
    condition: str | None,
    message: str,
    truncated: bool,
)
```

The action index is the `ExpectFailure` wrapper's index, including any prior
`Restart`. `kind` describes the observed event, not the underlying root cause.
`condition` is the top-level query condition when available, otherwise `None`.
It is observational metadata, not a promise that the same condition will be
emitted by every Spark version. `message` is a plain-text diagnostic snapshot
capped at 1,024 Unicode code points after CRLF and CR normalization to LF;
`truncated` states whether text was omitted. The top-level `condition`, when
present, is capped at 256 characters and is rejected rather than truncated if
oversized. `message` is excluded from automatic object `repr`, but remains
directly accessible. The model accepts plain text only and does not traverse
nested exception causes. The runner will decide how to extract the top-level
query text; it must not copy arbitrary traceback objects into the result. The
object contains no raw exception, traceback, DataFrame, query, JVM handle, or
mutable Spark collection. Exception text can still contain sensitive user
data; tests should use synthetic inputs and must not treat this field as a
redaction mechanism.

`ScenarioResult.batches`, `.rows`, and `.progress` retain their existing
meaning. On a matched failure, they contain only **fully completed** captured
callbacks and progress records. Completed callbacks from the failing action
may appear in `batches` if Spark ran more than one trigger before termination;
no partially collected callback is exposed. Progress for a failed action is
not promised, because its normal synchronization did not complete. Progress
from earlier completed actions remains in observation order. No assertion is
allowed to mistake these partial captures for a successfully completed
scenario: `result.failure` makes the terminal outcome explicit.

## Matching and error precedence

The runner accepts an expected failure only if all of these hold:

1. The `ExpectFailure` batch was successfully encoded and atomically published.
2. During that action, the owned query actually terminated with a non-`None`
   `query.exception()`; an exception from `processAllAvailable()` alone does not
   establish this.
3. No Streamcase input, capture-*normalization*, limit,
   progress-normalization, lifecycle, or cleanup error is the cause of
   termination or otherwise occurred during the run. A Spark failure while
   evaluating the caller's output DataFrame inside `foreachBatch.collect()`
   **may** be eligible: that is where the transformation can actually execute.
   Private capture state must distinguish that Spark evaluation phase from
   Streamcase's own row conversion, `CapturedBatch` construction, and limit
   checks, including when Spark wraps their exceptions. An unknown callback
   phase is ineligible rather than presumed to be a pipeline failure.
4. Every declared selector matches the observed query exception.
5. Query and run-directory cleanup both succeed under the existing retention
   policy before a passing result is returned.

If the declared failure never occurs, the runner raises a distinct
`ExpectedFailureNotObservedError` with the action index; it does not return a
successful `ScenarioResult`. If the query fails at an earlier action, at a
different phase, or with nonmatching selectors, the existing action-context
failure remains primary, with its original cause. If the expected query failure
matches but cleanup fails, cleanup failure is surfaced; the negative test must
not pass. A `ScenarioLimitError` is never an expected pipeline failure. A
failure while constructing or validating `transform`, rebuilding after
`Restart`, publishing input, normalizing collected output, or stopping the
query is never matched merely because its text resembles the selector. A
failure during output evaluation still needs query-termination evidence and
the explicit selector; failure text alone is insufficient.

Only the runner-owned query is inspected or stopped. The caller's session and
unrelated queries survive. `retain_artifacts=True` keeps the generated run
directory under the existing caller-owned base; it does not weaken cleanup of
the query. Checkpoint contents are never edited. As in the existing runner,
there is no per-action deadline: an action whose synchronization blocks may
continue to block rather than be declared a missing expected failure.

## Implementation and verification boundaries

- #173 adds and validates the declaration in the core scenario model.
- #174 adds immutable failure observation and the explicit missing-failure
  error without importing PySpark.
- #175 integrates query-specific matching into the runner, with private
  callback-error provenance and cleanup precedence.
- #176 adds backend-independent assertions over action index and observed
  classification, without treating Spark prose as a stable taxonomy.
- #177 exercises successful, missing, wrong-action, wrong-selector, partial,
  restart, limit, and cleanup cases on Spark 3.5 and 4.2.
- #178 publishes a runnable pytest example and user-facing limitations.

The exact selector and observation contract must be tested against both Spark
CI lines before the feature is included in 0.3.0a1. If either line cannot
reliably distinguish query failure from runner callback failure, the release
must narrow or defer the feature; it must not silently accept all exceptions.

## Alternatives considered

- **A runner-wide `expected_exception` option:** rejected because a scenario
  may have several batches and restarts; the intended action would be unclear.
- **A separate marker action after `Batch`:** rejected because the batch can
  fail before the runner reaches the marker, making interpretation fragile.
- **A raw `pytest.raises` around `run_scenario`:** remains possible but cannot
  distinguish runner failures or return a bounded partial result.
- **Match any `StreamingQueryException`:** rejected because wrapper exceptions
  also report callback and limit failures, and a wrong pipeline failure could
  make the test pass.
- **Expose the raw Spark exception in results:** rejected because it ties core
  results to live/backend-specific objects and unbounded diagnostic text.
