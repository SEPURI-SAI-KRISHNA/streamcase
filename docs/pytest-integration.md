# Pytest integration

Streamcase's optional pytest plugin provides a function-scoped
`streamcase_runner` fixture. It returns a callable for the public Spark runner,
with each test's `tmp_path` used as the parent for generated input and
checkpoint files. You opt in explicitly; installing Streamcase does not
register the plugin or start Spark automatically.

The plugin, `pytest` extra, and `spark35` extra are included in the published
[0.2.0a1 prerelease](releases/0.2.0a1.md). They are absent from the older
`0.1.0a2` prerelease.

## Install and prerequisites

The tested local-mode combinations are Python 3.11 with Java 17 and either
PySpark 4.2.x or PySpark 3.5.9 or later 3.5 patches. Install a Java 17 JDK
separately and make it available to Spark. Choose exactly one Spark extra:

```shell
python -m pip install "streamcase[pytest,spark]==0.2.0a1"
```

Or, for Spark 3.5:

```shell
python -m pip install "streamcase[pytest,spark35]==0.2.0a1"
```

See the [Spark compatibility policy](spark-compatibility.md) for the full
tested matrix. The `pytest` extra installs pytest but not PySpark; the Spark
extra installs the selected PySpark line. Do not combine `spark` and `spark35`.

## Root conftest.py

Put this in the test project's **root** `conftest.py`. The session fixture is
owned by the test suite, not Streamcase. If your suite already supplies a
Spark session fixture, keep it and add only the `pytest_plugins` assignment.
The import inside `spark_session` is intentionally lazy, so collecting tests
that never request Spark does not require PySpark.

```python
import pytest

pytest_plugins = ("streamcase.pytest_plugin",)


@pytest.fixture
def spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-pytest-guide")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()
```

Do not put `pytest_plugins` in a non-root `conftest.py`: pytest plugin loading
has suite-wide effects. Alternatively, load the plugin for one invocation with
`python -m pytest -p streamcase.pytest_plugin`. Neither route defines or
requests a Spark session fixture on its own.

## Test file

Save this as `test_streaming.py` beside the root `conftest.py`, then run
`python -m pytest -q test_streaming.py`:

```python
from streamcase import assert_rows_equal, batch, scenario


def test_one_batch(spark_session, streamcase_runner):
    result = streamcase_runner(
        spark_session,
        scenario(batch({"id": 1})),
        schema="id LONG",
        transform=lambda source: source.selectExpr("id"),
    )
    assert_rows_equal(result, [{"id": 1}])
```

`spark_session` comes from the caller's fixture above; `streamcase_runner`
comes from the explicitly loaded plugin. A test that does not request either
fixture neither creates a session nor invokes the Spark runner. The example
uses only public Streamcase and PySpark APIs, with no sleeps or private hooks.

## Ownership and cleanup

The caller owns the `SparkSession` and any pre-existing queries. Streamcase
never creates, stops, or reconfigures them. Each `streamcase_runner(...)` call
uses the public `run_scenario` implementation: it creates its own query and
unique run root under that test's `tmp_path`, then stops its query and removes
its run root on success or failure. The plugin adds no teardown finalizer that
could stop a caller-owned query. Different tests receive distinct temporary
parents; repeated calls within one test receive distinct run roots.

The callable forwards `schema`, `transform`, `output_mode`, `source_options`,
`query_options`, and `retain_artifacts` with the public runner's defaults and
errors. With `retain_artifacts=True`, generated run roots remain under
pytest's `tmp_path` for inspection during that test; pytest's retention policy
still applies. The fixture does not expose `base_dir`. For a durable or custom
directory, call [`run_scenario`](spark-runner.md) directly.

Core-only users can import `streamcase` and use scenario/result/assertion APIs
without installing PySpark or registering the plugin. Registering the plugin
requires pytest but does not import PySpark until the runner callable is
invoked. For the design and ownership rationale, see
[ADR 0004](design/0004-pytest-integration.md).
