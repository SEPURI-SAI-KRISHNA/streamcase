# Spark runner contract

The public scenario runner lives in `streamcase.spark`. Install the Spark extra
and provide a Java 17 runtime before using it; see the
[compatibility policy](spark-compatibility.md). Importing the core `streamcase`
package does not require PySpark. Importing `streamcase.spark` without the extra
raises an installation hint.

## Quick start

From a checkout, use Python 3.10-3.13, a Java 17 JDK on `PATH` or in
`JAVA_HOME`, and the supported PySpark 4.2 line. Install the optional dependency
with `python -m pip install -e ".[spark]"`. The example runs in Spark local
mode; no cluster or timing sleeps are needed.

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
unique input/checkpoint directory and owns only the query it starts.

## Parameters

`spark` must be a caller-owned PySpark `SparkSession`; `scenario` must be a
Streamcase `Scenario` containing `Batch` actions and optional `Restart`
boundaries. At each restart, Streamcase stops its query, rebuilds the source
and transformation, and starts a replacement using the same checkpoint and
query settings. The caller's `transform` callable may therefore run more than
once per scenario. A more detailed restart example is tracked separately.

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
Use the [result assertions](results-and-assertions.md) to inspect it.
