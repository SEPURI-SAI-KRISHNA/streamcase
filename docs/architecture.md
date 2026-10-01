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

## Future extensions

Kafka sources, Delta sinks, watermark-control helpers, and PyFlink support require
separate design issues. They should not add backend-specific behavior to the core
result assertions.
