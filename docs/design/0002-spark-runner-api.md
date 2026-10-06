# ADR 0002: Spark runner API and ownership

- Status: Accepted
- Decision scope: Phase 3 and Phase 4
- Related issue: #48

## Context

Streamcase needs to execute a `Scenario` against Apache Spark Structured
Streaming while preserving deterministic logical batch boundaries, isolating
checkpoints and input files, capturing output into backend-independent result
objects, and cleaning up after failures.

The runner must also coexist with test suites that already configure a
`SparkSession`. Creating or stopping hidden global sessions would make ownership
ambiguous, interfere with pytest fixture scopes, and risk terminating queries
that Streamcase did not start.

PySpark remains an optional dependency. Core scenario, result, and assertion
users must be able to import `streamcase` without installing or initializing
Spark.

## Decision

Spark integration will live in the optional `streamcase.spark` namespace and
expose one initial public function:

```python
from streamcase.spark import run_scenario

result = run_scenario(
    spark,
    scenario,
    schema=input_schema,
    transform=build_pipeline,
)
```

`run_scenario` receives a caller-owned session and returns the existing
backend-independent `ScenarioResult`.

## Public signature

The Phase 3 compatibility target is:

```python
def run_scenario(
    spark: SparkSession,
    scenario: Scenario,
    *,
    schema: StructType | str,
    transform: Callable[[DataFrame], DataFrame],
    output_mode: Literal["append", "complete", "update"] = "append",
    source_options: Mapping[str, str] | None = None,
    query_options: Mapping[str, str] | None = None,
    base_dir: str | PathLike[str] | None = None,
    retain_artifacts: bool = False,
) -> ScenarioResult: ...
```

`schema` and `transform` are keyword-only because reversing them or relying on
schema inference would create failures that are difficult to diagnose.

The initial implementation may be split into private configuration and lifecycle
objects. Those objects are not public compatibility surfaces.

## Session ownership

The caller creates, configures, and eventually stops `SparkSession`.
Streamcase:

- never calls `SparkSession.builder.getOrCreate()`;
- never stops the supplied session or its Spark context;
- does not mutate session-wide configuration;
- does not stop or reconfigure pre-existing queries;
- validates that transformation output belongs to the supplied session.

The same caller-owned session may be reused for multiple sequential scenario
runs. Each call receives independent input, checkpoint, query, callback, and
capture state. Concurrent calls must not share those resources, but explicit
thread-safety guarantees are deferred until there is evidence for parallel use.

## Resource ownership

| Resource | Owner | Cleanup rule |
| --- | --- | --- |
| `SparkSession` and Spark context | Caller | Streamcase never stops them |
| Pre-existing streaming queries | Caller | Streamcase never changes them |
| Query started by `run_scenario` | Streamcase | Stop on success and every failure path |
| Per-run input and checkpoint paths | Streamcase | Remove unless artifact retention is enabled |
| Caller-provided base directory | Caller | Never remove the base directory itself |
| Returned `ScenarioResult` | Caller | Contains no live Spark or JVM objects |

Cleanup failures must not erase the primary execution failure. If execution
succeeds but cleanup fails, cleanup failure is surfaced. The exact internal
exception-composition mechanism must support Python 3.10 and is not public API.

## Directory and artifact policy

Every call creates a unique child directory containing at least:

```text
run root/
  input/
  checkpoint/
  temporary/
```

When `base_dir` is omitted, Streamcase creates and owns a temporary base.
When supplied, `base_dir` is caller-owned and Streamcase creates a unique child
beneath it. Resolved generated paths must remain beneath their intended root.

`retain_artifacts=False` removes runner-owned paths on both success and failure.
`retain_artifacts=True` is allowed only with an explicit `base_dir`, so retained
artifacts have a discoverable caller-owned location. Streamcase never edits
checkpoint contents.

The directory-layout issue defines exact child names and collision behavior
without expanding this public signature.

## Schema and source contract

The caller supplies an explicit `StructType` or Spark DDL schema string. Schema
inference is not supported because it depends on file arrival and can change
test behavior.

The runner builds a JSON file stream from its private input directory. It owns
all options affecting location, file discovery, logical batch boundaries, and
malformed-input handling. In particular, the runner guarantees one input file
per trigger, single-line JSON records, and fail-fast parsing.

`source_options` is reserved for non-conflicting JSON decoding options such as
date and timestamp formats. Option names are compared case-insensitively.
Conflicts are rejected before directories or queries are created. Options that
alter paths, discovery order, recursion, cleanup, multiline behavior, schema
inference, parsing mode, or files per trigger are runner-owned.

Each scenario `Batch` is encoded using the existing deterministic JSON Lines
contract, written to a temporary file, and atomically published into the input
directory with a monotonic file name.

## Transformation contract

`transform` receives the streaming input `DataFrame` and returns a streaming
output `DataFrame` associated with the supplied session.

The callable must:

- be deterministic for the same input schema and session configuration;
- construct transformations without starting or awaiting a query;
- avoid owning the checkpoint or sink;
- be safe to invoke again when a future `Restart` recreates the query.

Streamcase rejects a non-DataFrame result, a batch DataFrame, a DataFrame from a
different session, or output with duplicate column names before processing
scenario actions. Captured nested Spark rows are converted recursively to
ordinary Python mappings and sequences before construction of `CapturedBatch`.

## Query and capture contract

Streamcase owns:

- the `foreachBatch` callback;
- checkpoint location;
- generated query name;
- trigger configuration;
- query start, synchronization, and stop operations.

`output_mode` accepts Spark's `append`, `complete`, and `update` modes.
`query_options` may contain only non-conflicting writer options. Checkpoint,
query name, sink, callback, trigger, and output-mode configuration are reserved
and rejected case-insensitively when supplied through option mappings.

For each `Batch` action, the runner:

1. atomically publishes one JSON Lines input file;
2. calls `processAllAvailable()` on its active query;
3. waits for callback capture to complete before the next action.

No time-based sleep is part of synchronization.

The callback collects intentionally small test output on the driver and creates
one immutable `CapturedBatch` per callback invocation, including callbacks with
zero rows. Spark's epoch identifier becomes `CapturedBatch.batch_id`. Callback
and row order are retained for inspection; row-equality assertions remain
order-independent.

After successful execution, the runner stops its query and returns a
`ScenarioResult` containing no live Spark objects.

## Restart staging

The first Phase 3 implementation supported batch-only scenarios and rejected
`Restart` before creating directories or starting a query. Phase 4 removed
that temporary restriction. A restart stops the active query and recreates the
source, transformation, callback, and query with the same checkpoint and
caller-owned session. The transformation callable may therefore be invoked
more than once during one public API call.

## Validation and failures

Configuration and complete scenario support are validated before side effects.
Once execution starts, failures include scenario action context while preserving
the original exception as the cause.

The runner guarantees a cleanup attempt for:

- transformation failures;
- query-start failures;
- atomic input publication failures;
- `processAllAvailable()` failures;
- callback collection or result-normalization failures;
- assertion or caller interruption inside runner-managed callbacks;
- Phase 4 stop and restart failures.

Streamcase does not catch or convert failures merely to hide Spark exception
details.

## Optional dependency boundary

`import streamcase` continues to work without PySpark. Importing
`streamcase.spark` without the optional dependency produces an actionable
installation error for the Spark extra.

The exact PySpark, Python, and Java support ranges belong to the separate
dependency-policy issue. No supported Spark version is implied by this ADR
alone.

## Alternatives considered

### Create and stop a SparkSession internally

Rejected because Spark sessions and contexts are commonly fixture-owned,
expensive, and process-global in ways that make hidden ownership unsafe.

### Public SparkRunner class

Rejected for the initial API because a stateful public object exposes lifecycle
and reuse questions before they are needed. Private collaborators can still
separate directory, capture, and query responsibilities internally.

### Accept a ready streaming DataFrame

Rejected because checkpoint-preserving restarts must recreate the source and
transformation. Accepting only a finished DataFrame would hide how that graph can
be rebuilt.

### Infer schema from arriving JSON files

Rejected because streaming file sources require predictable schemas and
inference would couple behavior to file timing and contents.

### Let callers configure trigger and checkpoint options

Rejected because those settings define Streamcase's synchronization and restart
guarantees. Conflicting ownership would make deterministic behavior impossible.

### Use the memory sink

Rejected because `foreachBatch` preserves callback batch identifiers, supports
all selected output modes, and allows immediate conversion into the existing
backend-independent result models.

## Consequences

- Issue #49 can define the optional dependency without changing core imports.
- Issues #51 through #58 have explicit ownership and public API boundaries.
- Tests and pytest fixtures must supply a session; fixture ownership remains
  unambiguous.
- The runner controls a deliberately narrow source and query configuration
  surface in exchange for deterministic execution.
- New sources, production sinks, session factories, concurrency guarantees, and
  public runner classes require separate design issues.
