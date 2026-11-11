# Spark runner contract

The public scenario runner lives in `streamcase.spark`. Install a supported Spark extra
and provide a Java 17 runtime before using it; see the
[compatibility policy](spark-compatibility.md). Importing the core `streamcase`
package does not require PySpark. Importing `streamcase.spark` without an extra
raises an installation hint.

## Quick start

Use Python 3.11 and a Java 17 JDK on `PATH` or in `JAVA_HOME`. Install either
`python -m pip install "streamcase[spark]==0.2.0a1"` for PySpark 4.2 or
`python -m pip install "streamcase[spark35]==0.2.0a1"` for PySpark 3.5.9 or
later 3.5 patches. Both extras are in the published prerelease. The example
runs in Spark local mode; no cluster or timing sleeps are needed.

```python
from pyspark.sql import DataFrame, SparkSession

from streamcase import assert_batch_count, assert_rows_equal, batch, scenario
from streamcase.spark import run_scenario


def transform(source: DataFrame) -> DataFrame:
    return source.filter("category = 'keep'").selectExpr("id", "id * 10 AS scaled")


spark = SparkSession.builder.master("local[2]").appName("streamcase-quickstart").getOrCreate()
try:
    result = run_scenario(
        spark,
        scenario(
            batch({"id": 1, "category": "keep"}, {"id": 2, "category": "drop"}),
            batch({"id": 3, "category": "keep"}),
        ),
        schema="id LONG, category STRING",
        transform=transform,
    )

    assert_batch_count(result, 2)
    assert_rows_equal(result, [{"id": 1, "scaled": 10}, {"id": 3, "scaled": 30}])
    assert [captured.batch_id for captured in result.batches] == [0, 1]
finally:
    spark.stop()
```

Each `Batch` publishes one input file and produces one captured output batch in
this example. Streamcase stops its query and removes its generated input and
checkpoint files before `run_scenario` returns. The `SparkSession` remains
caller-owned, so the `finally` block stops it explicitly. The Spark CI lane
executes the same two-batch workflow and checks query and directory cleanup.

The caller creates and eventually stops `spark`. `run_scenario` never creates or
stops a session and does not touch unrelated streaming queries. It creates a
unique input/checkpoint directory and owns only the queries it starts.

## Parameters

`spark` must be a caller-owned PySpark `SparkSession`; `scenario` must be a
Streamcase `Scenario` containing `Batch` actions and optional `Restart`
boundaries. At each restart, Streamcase stops its query, rebuilds the source
and transformation, and starts a replacement using the same checkpoint and
query settings. The caller's `transform` callable may therefore run more than
once per scenario. See the [checkpoint restart guide](restart-scenarios.md) for
a runnable example, guarantees, and limits.
Failures identify the restart action and transition; Streamcase attempts query
and directory cleanup without replacing the original exception.

The source checkout executes a terminal `ExpectFailure` declaration from the
core scenario model. Its wrapped `Batch` is published once at that action's
zero-based index. A passing negative test requires the owned query to terminate
with an exception at that action, every explicit selector to match, and query
and directory cleanup to succeed. The result then contains a bounded
`failure` observation plus only fully completed captured batches and prior
progress; progress for the failed action is not promised. A missing expected
failure raises `ExpectedFailureNotObservedError`. Input publication,
capture-normalization, limit, restart, and cleanup errors remain test failures,
even if their text resembles a marker. This feature is absent from published
0.2.0a1. See the
[declaration guide](scenario-model.md#expected-failure-declaration-source-checkout-only).

`schema` is a non-empty Spark DDL string or `StructType`. `transform` receives
the streaming JSON input DataFrame and must return a streaming DataFrame from
the same session. The output cannot have duplicate column names, including
names that differ only by case.

`output_mode` defaults to `"append"` and also accepts `"complete"` and
`"update"`. Optional `source_options` and `query_options` must not override
runner-owned source discovery, callback/sink, checkpoint, query name, trigger,
or output-mode settings. Option mappings are copied and not mutated.

`base_dir` may point to an existing caller-owned directory. Streamcase creates
one unique child under it. Generated artifacts are normally removed after
success or failure; `retain_artifacts=True` requires an explicit `base_dir`
and preserves the generated child for inspection. The caller's base directory
is never removed.

The return value is an immutable `ScenarioResult` with no live Spark objects.
Use the [result assertions](results-and-assertions.md) for output and the
[progress guide](progress-and-state-assertions.md) for input-row, watermark,
and state-operator observations.

## Bounded scenario capture in source checkouts

The current source checkout adds `max_output_rows` to `run_scenario(...)`.
It defaults to `10_000` and caps the total number of output rows captured
across all callbacks and `Restart` actions in one run. The limit is inclusive:
exactly 10,000 rows succeed, while evidence of one more raises the public
`ScenarioLimitError` with action index, Spark batch ID, limit, and an observed
lower bound. No partial `ScenarioResult` is returned. Zero-row callbacks
remain visible and do not consume the allowance. A limit failure still
attempts the runner-owned query and directory cleanup; it never stops the
caller's Spark session.

Pass a larger positive integer for a deliberately larger test, or `None` to
disable this row limit. Booleans, non-integers, zero, and negative values are
rejected before the query or run directory is created. Disabling the limit
restores full driver-side collection, which can exhaust memory. Even with a
limit, one very large or deeply nested row, Spark state, and the checkpoint
can use substantial memory or disk. This is a retained-row guardrail, **not**
a process-wide memory bound or execution deadline. Input-byte limits are
described below and in [ADR 0005](design/0005-bounded-capture-policy.md).

The current source checkout also adds `max_input_bytes_per_batch=8_388_608`
(8 MiB) and `max_input_bytes_per_run=67_108_864` (64 MiB). They count the
exact UTF-8 JSON Lines bytes, including one LF after each input row. The
per-batch limit resets for every `Batch` action; the run total counts only
successfully published files and persists across `Restart`. Both limits are
inclusive. An oversize action raises `ScenarioLimitError` with its action and
input-row indexes, the exceeded budget, and the observed byte count. The
rejected action never publishes a final file or invokes Spark processing;
the hidden temporary file is removed, and the next input filename remains
available for a retry. The runner then follows its usual query and directory
cleanup rules.

Limit failures stop only the query started for that scenario. The caller's
Spark session and unrelated active queries remain caller-owned. Generated run
directories are removed by default; `retain_artifacts=True` preserves the
run's earlier published files and checkpoints for inspection, but never a
rejected partial input file. Stopping a query does not promise immediate
reclamation of all Python/JVM memory or Spark-managed state.

Each byte limit can be raised independently or explicitly disabled with
`None`. Invalid values fail before run-directory creation. These limits do
not bound the memory needed to construct a `Batch`, serialize one very large
row, run Spark transformations, or store checkpoints. Disabling both input
limits restores unrestricted file publication and can exhaust disk space.
See [ADR 0005](design/0005-bounded-capture-policy.md) for the accounting and
failure contract.

These three limit options are **not in the published 0.2.0a1 package**
installed in the quick start above. They will first be available in a later
prerelease after the bounded-runner work passes the supported Spark CI lanes.

## Execution-time limitation

Neither the published 0.2.0a1 runner nor the current source checkout provides
a per-action `timeout` or `deadline` parameter. The capture and input-byte
allowances above bound retained rows and published input files, **not** how
long Spark takes to process an action. `processAllAvailable()` is a blocking
testing barrier; it can remain blocked when data keeps arriving or a callback
does not finish. Streamcase cannot promise to return by a wall-clock deadline
or to finish query cleanup by one. Its normal cleanup runs when control
returns or an exception is raised; an action that never returns prevents that
path from running.

The [cross-line deadline assessment](design/0006-spark-deadline-feasibility.md)
records why a Python timer or `awaitTermination(timeout)` is not safe action
cancellation. Its guarded Spark 3.5.9 and 4.2.1 probes showed a timer firing
while `processAllAvailable()` remained blocked. A separate test process with
an external watchdog may bound the *test process's* lifetime, but forcefully
ending it does not provide graceful Spark-query cleanup or preserve a
caller-owned session. Streamcase does not ship that as a runner feature.
