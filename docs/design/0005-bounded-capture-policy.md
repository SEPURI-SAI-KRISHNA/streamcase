# ADR 0005: Bounded scenario input and output capture

- Status: Accepted for implementation in 0.3.0a1; not available in 0.2.0a1
- Decision scope: Phase 7
- Related issue: #166

## Context

The Spark runner is intended for finite tests, but its current `foreachBatch`
callback calls `DataFrame.collect()` without a row limit. It retains every
captured batch until the run ends. Input publication first encodes an entire
`Batch` as one JSON Lines string. A mistaken transformation, large row, or
long scenario can therefore overwhelm the Python driver or fill the run's
input directory before the test can report a useful failure.

Spark's `processAllAvailable()` is a testing synchronization method, not a
deadline: [Spark notes](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.processAllAvailable.html)
that it may block indefinitely when data keeps arriving. This decision
limits captured rows and published input bytes; it does **not** solve execution
deadlines. Issues #170 and #171 assess deadlines separately.

The [existing runner ownership decision](0002-spark-runner-api.md) still
applies. Streamcase owns the query, input files, checkpoint, and run directory;
the caller owns the Spark session. The opt-in pytest fixture forwards the
public runner's supported options rather than imposing different limits.

## Decision

Add three keyword-only parameters to `streamcase.spark.run_scenario` and
forward them unchanged from `streamcase.pytest_plugin.streamcase_runner`:

```python
max_output_rows: int | None = 10_000
max_input_bytes_per_batch: int | None = 8_388_608   # 8 MiB
max_input_bytes_per_run: int | None = 67_108_864    # 64 MiB
```

Each limit is inclusive. A value must be a positive, non-boolean integer;
`None` explicitly disables that particular limit. Zero, negative values,
booleans, floats, strings, and other objects are rejected before creating a
run directory or starting a query: `TypeError` for a wrong type and
`ValueError` for a non-positive integer. The fixture must expose the same
names, defaults, validation, and errors. It must not maintain a second limit
counter or change session ownership.

These finite defaults are an intentional alpha-stage behavior change: a test
that previously collected more than 10,000 output rows or published more
than the input allowances will now fail. Small finite tests are Streamcase's
primary use case, and silently collecting unlimited rows is an unsafe default.
Callers with a justified larger test can choose a larger positive value or
explicitly pass `None`; the latter restores the old unbounded behavior for
that dimension and must be documented as a driver/disk exhaustion risk. No
environment variable or hidden global setting changes these defaults.

### Output rows

`max_output_rows` counts rows in all `CapturedBatch` objects for one
`run_scenario` invocation, including callbacks after `Restart` and rows
repeated by `complete` or `update` output mode. It is **not** a per-callback
limit, and it does not count input rows or progress records. Zero-row
callbacks remain distinct batches and use no row allowance. Exactly the
limit is accepted; evidence of one more row fails the run. Callback batch
IDs and successful capture ordering retain their existing meaning.

The callback must never call unrestricted `collect()` while this limit is
enabled. With `remaining = limit - captured_rows`, it may collect at most
`remaining + 1` rows from the batch DataFrame, using Spark's documented
[`DataFrame.limit`](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html)
operation. The extra row is only an overflow witness. Do not count the entire
DataFrame first or materialize all rows and then slice them. Row order within
a DataFrame is not promised by Spark; the sample is only used to decide
whether the cap was exceeded, not to return a truncated result. On overflow,
do not append a partial `CapturedBatch` or return a partial `ScenarioResult`.
Previously completed batches may remain in private capture state only until
runner cleanup completes.

With `max_output_rows=None`, the callback retains the current full-capture
behavior. This is an explicit opt-out, not a process-wide memory guarantee.

### Input bytes

`max_input_bytes_per_batch` applies to each `Batch` action's exact encoded
UTF-8 JSON Lines payload, including one LF byte after every serialized row.
`max_input_bytes_per_run` applies to the sum of those payload sizes **successfully
published** during one invocation. A `Restart` does not reset the total.
Neither limit counts the hidden temporary file twice, checkpoint files, or
Spark's own storage. Exactly the limit is accepted; the first byte over it
fails the affected action.

Encoding must check the byte budgets incrementally as rows are serialized.
It may write only to a hidden temporary file until the complete batch fits
both budgets; it must never publish a final file with a truncated payload.
An overflow removes that temporary file, leaves the next input-file index and
published-byte total unchanged, and does not call `processAllAvailable()` for
the rejected action. A successful same-directory atomic rename commits both
the file and its byte count. Existing collision checks, UTF-8/LF JSON Lines
format, deterministic encoding, and row-index serialization diagnostics remain
in force. Disabling one input limit does not disable the other.

## Failure contract

A new core exception, `ScenarioLimitError(RuntimeError)`, is exported from
`streamcase` without importing PySpark. It identifies `action_index`,
`resource` (`"output_rows"`, `"input_bytes_per_batch"`, or
`"input_bytes_per_run"`), `limit`, and `observed_at_least`. For input bytes,
the observed value is the exact cumulative encoded count at the first
offending row. For output rows it is a lower bound established by the
overflow witness, not a claim about the DataFrame's total row count. The
message includes the zero-based action index, limit name, configured value,
and observed lower bound. A callback may also include Spark's batch ID when
available; callers must not depend on one being present for input failures.

The user-facing failure must retain this exception and its context even when
Spark wraps a Python callback error during `processAllAvailable()`. The runner
may record the overflow in private callback state and re-raise the typed
error, chaining Spark's wrapper as the cause. It must not reinterpret an
unrelated Spark failure as a limit breach. A `ScenarioLimitError` is never an
expected *pipeline* failure under Phase 8's future expected-failure API.
Other action failures keep their existing action-index context and causes.

On any limit breach, Streamcase attempts to stop only its active query and
then applies the existing run-directory cleanup or artifact-retention rule.
It never stops the caller-owned session or unrelated queries. If cleanup
also fails, the limit breach remains primary and the cleanup failure is
attached under the existing cleanup contract. `retain_artifacts=True` may
preserve already published inputs and checkpoints, but not a rejected
partially written input file.

## Limits of this policy

These settings bound **retained output row count** and **published JSON input
bytes**, not process-wide memory, Spark state, checkpoint size, execution
time, or total rows Spark computes. A single output row may contain a huge
string or nested value; Python/Spark conversion can use extra memory before
the row count is checked. `Batch` currently snapshots all input mappings
eagerly, and serializing one input row can allocate a large string before its
encoded size is known. Spark itself may retain state or shuffle data far
beyond these allowances. The callback row cap does not cancel a slow Spark
job. Documentation must describe the controls as guardrails for small tests,
not a memory-safety or timeout guarantee.

## Verification and follow-up

- Issue #167 implements output capture, including exact-boundary,
  cross-callback, restart, zero-row, and callback-wrapped-error tests.
- Issue #168 implements input budgets and atomic failure behavior, including
  multibyte UTF-8, newline accounting, per-run accumulation, and rollback
  after a failed publication.
- Issue #169 proves query cleanup, caller-session survival, directory
  isolation, and pytest forwarding with both supported Spark CI lanes.
- Issues #170 and #171 separately determine whether safe per-action deadlines
  are possible; this ADR makes no deadline promise.

The 0.3.0a1 release notes and runner guide must explain the new defaults,
explicit opt-out, and migration from 0.2.0a1. This ADR is a design decision,
not documentation that these parameters already exist in a published build.

## Alternatives considered

- **Keep unlimited defaults and offer opt-in caps:** rejected because the
  normal test path would retain the current driver-exhaustion footgun.
- **Cap each callback but not the run:** rejected because many individually
  small callbacks, including those after restarts, could still accumulate
  unlimited captured rows.
- **Cap only each input batch:** rejected because many accepted files could
  still fill the isolated run directory; the aggregate input budget closes
  that gap for Streamcase's published files.
- **Truncate results or skip oversized inputs:** rejected because a passing
  assertion against incomplete data would be misleading.
- **Use `count()` before `collect()`:** rejected because it adds a full extra
  Spark action and can re-evaluate a stateful `foreachBatch` DataFrame.
