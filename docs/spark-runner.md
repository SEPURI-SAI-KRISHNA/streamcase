# Spark runner contract

The public batch-only runner lives in `streamcase.spark`. Install the Spark extra
and provide a Java 17 runtime before using it; see the
[compatibility policy](spark-compatibility.md). Importing the core `streamcase`
package does not require PySpark. Importing `streamcase.spark` without the extra
raises an installation hint.

```python
from streamcase.spark import run_scenario

result = run_scenario(
    spark,
    test_scenario,
    schema="id LONG",
    transform=build_pipeline,
)
```

The caller creates and eventually stops `spark`. `run_scenario` never creates or
stops a session and does not touch unrelated streaming queries. It creates a
unique input/checkpoint directory and owns only the query it starts.

## Parameters

`spark` must be a caller-owned PySpark `SparkSession`; `test_scenario` must be a
Streamcase `Scenario` containing only `Batch` actions for now. A `Restart`
action fails validation before directories are created.

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
Use the [result assertions](results-and-assertions.md) to inspect it. A full
two-batch quick start is planned for issue #58.
