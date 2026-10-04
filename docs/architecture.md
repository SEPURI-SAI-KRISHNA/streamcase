# Architecture

## Design goals

Streamcase aims to make small streaming tests deterministic, readable, isolated,
and diagnosable. The backend-independent
[scenario model](scenario-model.md), result layer, and batch-only Spark runner
are implemented. Restart execution remains planned. Streamcase does not emulate
Spark; tests run through Spark's public Structured Streaming interfaces.

## Execution model

```text
test actions
    |
    v
one JSON file per Batch ---> Spark file stream ---> user pipeline
                                                  |
                                                  v
checkpoint <--- stop/start action           foreachBatch capture
                                                  |
                                                  v
                                      rows + result assertions
```

The batch-only `run_scenario()` call creates an isolated directory containing
its input and checkpoint data. `maxFilesPerTrigger=1` preserves logical batch
boundaries. After writing a file atomically, Streamcase calls
`processAllAvailable()`, which Spark documents as a testing-oriented
synchronization method.

A planned `Restart` action will stop the active query and recreate the source,
transformation, and query with the same checkpoint. The batch-only runner
records rows in the Python driver through `foreachBatch` and normalizes them
into backend-independent result objects.

The accepted [Spark runner API and ownership decision](design/0002-spark-runner-api.md)
defines a caller-owned `SparkSession`, the optional `streamcase.spark`
namespace, deterministic source/query configuration, and runner-owned cleanup.
The implemented [public Spark runner contract](spark-runner.md) covers the
batch-only phase.

## Boundaries

- The runner controls test input, synchronization, checkpoint location, and sink.
- The caller controls schema, transformations, output mode, and non-reserved
  source/query options.
- The result object contains no live Spark or JVM objects.
- Checkpoints are opaque to Streamcase and are never edited.

## Result and assertion model

The accepted [result and assertion API decision](design/0001-result-assertion-api.md)
defines immutable captured batches, scenario results, duplicate-aware unordered
row comparison, batch-count assertions, and unique-key assertions. The
[results and assertions guide](results-and-assertions.md) documents the
implemented contract, which does not import PySpark.

## Compatibility

Spark version differences are isolated in the optional runner implementation.
The public action and result objects do not import PySpark at runtime. PySpark is
an optional dependency so assertion-only consumers and documentation tooling stay
lightweight.

The [Spark compatibility policy](spark-compatibility.md) defines the initial
tested version line and installation extra.

## Run directory layout

Each runner invocation creates a unique `streamcase-run-*` root with exactly
three initial child directories:

```text
streamcase-run-<unique>/
  input/
  checkpoint/
  temporary/
```

When the caller supplies `base_dir`, that directory must already exist and must
be a directory. Streamcase creates a new direct child beneath it, never reuses an
existing run root, never modifies unrelated contents, and never removes the
caller-owned base. Without `base_dir`, the unique run root is created in the
platform temporary location.

Generated paths are resolved and checked against their intended parent before
use. Normal cleanup removes only the generated run root and is idempotent.
Artifact retention is accepted only with an explicit caller-owned base; retained
run roots remain available after cleanup. The layout uses `pathlib` and platform
temporary-directory APIs rather than assuming a path separator.

Batch file publication, checkpoint contents, Spark query lifecycle, and
distributed filesystems are outside the directory layout's responsibility.

## Atomic batch publication

One private writer instance publishes a scenario's `Batch` actions sequentially.
It uses the deterministic JSON Lines encoder and assigns zero-based, monotonic
names such as `batch-00000000000000000000.json`.

The complete batch is encoded before filesystem changes. The writer then creates
a hidden temporary file beside the destination in `input/` using exclusive file
creation, writes UTF-8 with LF line endings, flushes and synchronizes the file,
and performs a same-directory rename to the visible name. Spark therefore never
sees a final path containing partial content. The separate `temporary/` directory
remains available for other runner-owned working files.

An existing temporary or destination path is a collision and fails without
overwriting it. Failed writes or renames remove the writer's temporary file and
do not advance the monotonic index. The writer is private and sequential;
parallel publication within one run is not supported.

## Spark JSON input source

The private Spark source builder requires an explicit schema before it accesses
the caller-owned session. It reads JSON from the isolated `input/` directory with
`maxFilesPerTrigger=1`, single-line records, and fail-fast malformed-input
handling. The returned DataFrame is streaming, but constructing it does not start
a query.

Caller source options are copied and may configure non-conflicting JSON decoding
details such as date, timestamp, and locale formats. Option names are checked
case-insensitively. Streamcase rejects options that control paths, file discovery,
schema inference, record boundaries, malformed-input behavior, or files per
trigger before accessing Spark. This keeps directory ownership and logical batch
boundaries under the runner's control without mutating caller mappings.

## Driver-side batch capture

One private capture object owns the `foreachBatch` callback state for a runner
invocation. Each callback collects its intentionally small output DataFrame on
the driver, recursively converts Spark rows into ordinary Python mappings and
sequences, and constructs an immutable `CapturedBatch` with Spark's batch
identifier. No DataFrame, Spark `Row`, JVM handle, or mutable callback collection
is retained.

The capture lock covers collection, conversion, and publication. A runner
snapshot therefore waits for an in-flight callback and sees either the complete
captured batch or no batch from that callback. Complete batches remain distinct
in callback order, including callbacks whose output DataFrame contains zero
rows. Query configuration and lifecycle remain separate responsibilities.

## Batch execution

The private batch executor starts one streaming query with the run's checkpoint
and driver capture callback. It validates that every scenario action is a
`Batch` before starting the query. For each action in order, it atomically
publishes one input file and calls Spark's `processAllAvailable()` before
proceeding. There are no time-based sleeps between actions.

The executor defaults to append output mode and accepts the approved complete
and update modes. Optional query-writer settings are copied and checked before
run directories are created. Option names are case-insensitive for collision
checks; checkpoint location, query name, sink/callback, trigger, output mode,
and other writer-owned controls cannot be overridden. Approved options are
applied before the runner sets its own callback, output mode, checkpoint, and
query name. The caller's mapping is not mutated.

After the final synchronization and query stop succeed, the executor snapshots
the capture state into `ScenarioResult`. The returned object contains only the
approved immutable batch identifiers and rows, works with the existing Phase 2
assertions, and cannot change when later callbacks append to the capture object.
An output callback with zero rows remains a distinct captured batch.

If publication, processing, or query activity fails, the raised error names
the zero-based action index and retains the original failure as its cause. The
executor stops its query after success or failure. A private managed-run
boundary removes its generated directory after query shutdown, including when
stream construction, execution, or a callback fails. Retention keeps that run
root only when the caller supplied a base directory. Neither layer stops the
caller-owned Spark session or touches unrelated queries.

If work and cleanup both fail, the original exception remains primary. Cleanup
failures are attached to it and included in the traceback on supported Python
versions; a Python 3.10 fallback includes their context in the original error
message. A cleanup failure after otherwise successful work is surfaced.
`Restart` execution remains follow-up work.

## Future extensions

Kafka sources, Delta sinks, watermark-control helpers, and PyFlink support require
separate design issues. They should not add backend-specific behavior to the core
result assertions.
