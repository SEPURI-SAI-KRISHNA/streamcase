# Architecture

## Design goals

Streamcase aims to make small streaming tests deterministic, readable, isolated,
and diagnosable. This document describes the proposed architecture; only the
package scaffold currently exists. Streamcase will not emulate Spark. Tests will
execute through Spark's public Structured Streaming interfaces.

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
                                      rows + progress + assertions
```

The planned `run()` call will receive an isolated directory containing its input
and checkpoint data. `maxFilesPerTrigger=1` will preserve logical batch
boundaries. After writing a file atomically, Streamcase will call
`processAllAvailable()`, which Spark documents as a testing-oriented
synchronization method.

A planned `Restart` action will stop the active query and start the same
transformed streaming DataFrame with the same checkpoint. The proposed runner
will record rows in the Python driver through `foreachBatch` and deep-copy
progress into ordinary dictionaries.

## Boundaries

- The runner controls test input, synchronization, checkpoint location, and sink.
- The caller controls schema, transformations, output mode, and non-reserved
  source/query options.
- The result object contains no live Spark or JVM objects.
- Checkpoints are opaque to Streamcase and are never edited.

## Compatibility

Spark version differences are isolated in progress normalization and the runner.
The public action and result objects do not import PySpark at runtime. PySpark is
an optional dependency so assertion-only consumers and documentation tooling stay
lightweight.

## Future extensions

Kafka sources, Delta sinks, watermark-control helpers, and PyFlink support require
separate design issues. They should not add backend-specific behavior to the core
result assertions.
