# Architecture

## Design goals

Streamcase aims to make small streaming tests deterministic, readable, isolated,
and diagnosable. The backend-independent
[scenario model](scenario-model.md) is implemented; the runner and result layers
are being delivered separately. The result layer is implemented and the runner
below remains proposed. Streamcase will not emulate Spark. Tests will execute
through Spark's public Structured Streaming interfaces.

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

The planned `run_scenario()` call will create an isolated directory containing
its input and checkpoint data. `maxFilesPerTrigger=1` will preserve logical batch
boundaries. After writing a file atomically, Streamcase will call
`processAllAvailable()`, which Spark documents as a testing-oriented
synchronization method.

A planned `Restart` action will stop the active query and recreate the source,
transformation, and query with the same checkpoint. The proposed runner will
record rows in the Python driver through `foreachBatch` and normalize them into
the backend-independent result objects.

The accepted [Spark runner API and ownership decision](design/0002-spark-runner-api.md)
defines a caller-owned `SparkSession`, the optional `streamcase.spark`
namespace, deterministic source/query configuration, and runner-owned cleanup.

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

## Future extensions

Kafka sources, Delta sinks, watermark-control helpers, and PyFlink support require
separate design issues. They should not add backend-specific behavior to the core
result assertions.
