# Checkpoint restart scenarios

Use `Restart` to test that a Structured Streaming pipeline continues after its
query is stopped and started with the same checkpoint. Streamcase runs this
sequence against Spark; it does not simulate a restart or edit checkpoint data.

This guide uses the [supported Python, Java, and PySpark combination](spark-compatibility.md).
From a checkout, install the Spark extra with `python -m pip install -e ".[spark]"`.

## Minimal example

```python
from pyspark.sql import DataFrame, SparkSession

from streamcase import assert_batch_count, assert_rows_equal, batch, restart, scenario
from streamcase.spark import run_scenario


def transform(source: DataFrame) -> DataFrame:
    return source.selectExpr("id", "id * 10 AS scaled")


spark = SparkSession.builder.master("local[2]").appName("streamcase-restart-guide").getOrCreate()
try:
    result = run_scenario(
        spark,
        scenario(
            batch({"id": 1}),
            restart(),
            batch({"id": 2}),
        ),
        schema="id LONG",
        transform=transform,
    )

    assert_batch_count(result, 2)
    assert_rows_equal(result, [{"id": 1, "scaled": 10}, {"id": 2, "scaled": 20}])
    assert [item.batch_id for item in result.batches] == [0, 1]
finally:
    spark.stop()
```

The [Spark integration lane](../.github/workflows/spark.yml) executes this
exact code block. No timing sleeps or private Streamcase APIs are needed.

## What the restart guarantees

Each `Batch` publishes one complete JSON Lines file. Streamcase waits for
`processAllAvailable()` to finish before moving to the next action, so the
first batch is processed before the restart begins. At `Restart`, it stops the
owned query, rebuilds the source and calls `transform` again, then starts a
replacement with the same checkpoint, query settings, and output capture.
Later batches continue with new input-file names; Streamcase does not republish
earlier `Batch` actions. The result retains captured output from both sides of
the boundary.

Spark's [query `id`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.id.html)
persists across a checkpoint restart, while its
[`runId`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.runId.html)
changes for the replacement execution. Streamcase's integration tests verify
both along with the same checkpoint path and the example's uninterrupted batch
identifiers.

## Ownership and limits

The caller owns the `SparkSession`; the `finally` block above stops it.
Streamcase owns only the queries and unique input/checkpoint directory created
for this run. On success or failure it attempts to stop its active query and
normally removes that generated directory. It does not stop unrelated queries or remove
a caller-supplied `base_dir`. If you need files for debugging, pass an existing
`base_dir` and `retain_artifacts=True`; the generated child directory is then
left for you to inspect and remove later.

`Restart` must appear between `Batch` actions; it cannot be first, last, or
adjacent to another restart. Use the same input schema, transformation behavior,
output mode, and compatible query options across a restart. Streamcase calls
the same `transform` callable again but does not support changing a query plan,
migrating checkpoint contents, or repairing a corrupt checkpoint. It is a
small, local-mode testing utility, not a production failover orchestrator.

Failures identify the zero-based restart action index and whether stopping,
rebuilding, or starting failed. The original exception remains available as
the cause. Cleanup failures are reported without replacing it. If Spark itself
persistently refuses to stop a query, Streamcase can report that failure but
cannot force the JVM to shut it down.

See the [scenario rules](scenario-model.md), [runner contract](spark-runner.md),
and [result assertions](results-and-assertions.md) for related API details.
