# Expected streaming failures (source checkout)

Use an explicit expected-failure action to test that a particular input batch
terminates the query, without treating an unrelated runner or cleanup error as
a passing negative test. This API is in the current source checkout for the
planned 0.3.0a1 prerelease. It is **not** in the published 0.2.0a1 package.

## Install and prerequisites

Use Python 3.11, a separately installed Java 17 JDK, and one tested PySpark
line. From a checkout of this repository, install either:

```shell
python -m pip install -e ".[pytest,spark]"
```

or, for PySpark 3.5.9 or a later 3.5 patch:

```shell
python -m pip install -e ".[pytest,spark35]"
```

Do not combine `spark` and `spark35`. See the
[compatibility policy](spark-compatibility.md) for the tested local-mode matrix.
The `pytest` extra provides the plugin, but does not load it or create Spark.

## Root conftest.py

Save this as `conftest.py` at the root of a test directory. If your suite
already owns a Spark session fixture, keep it and add only `pytest_plugins`.
The session below belongs to the test, not to Streamcase.

```python
import pytest

pytest_plugins = ("streamcase.pytest_plugin",)


@pytest.fixture
def spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-expected-failure-guide")
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

Load the plugin in the **root** `conftest.py`, not a nested one. Alternatively,
use `python -m pytest -p streamcase.pytest_plugin`. Neither method implicitly
starts a Spark session.

## Test file

Save this as `test_streaming_failure.py` beside `conftest.py`, then run
`python -m pytest -q test_streaming_failure.py` from that directory:

```python
from streamcase import (
    assert_failure,
    assert_rows_equal,
    batch,
    expect_failure,
    restart,
    scenario,
)

MARKER = "STREAMCASE_GUIDE_BAD_RECORD"


def reject_bad(source):
    return source.selectExpr(
        "CASE WHEN value = 'bad' "
        "THEN raise_error('STREAMCASE_GUIDE_BAD_RECORD') "
        "ELSE value END AS value"
    )


def test_expected_stream_failure(spark_session, streamcase_runner):
    case = scenario(
        batch({"value": "good"}),
        restart(),
        expect_failure(batch({"value": "bad"}), message_contains=MARKER),
    )
    result = streamcase_runner(
        spark_session,
        case,
        schema="value STRING",
        transform=reject_bad,
    )

    assert_rows_equal(result, [{"value": "good"}])
    assert_failure(result, action_index=2)
```

The first batch finishes and its output is captured. `restart()` stops this
run's query and starts a replacement with the same checkpoint and caller-owned
session. The terminal `expect_failure` wraps exactly one `Batch`; its action
index is 2, not 3. The declaration checks the **full owned-query exception**
for the caller-controlled marker. `assert_failure` checks the returned action
index and `query_terminated` kind. Do not repeat the marker in
`assert_failure(message_contains=...)`: that assertion sees only a bounded
1,024-character diagnostic preview, and Spark may put the marker beyond it.

`result.rows` and `result.progress` contain only fully completed observations.
The failing action need not have a progress record, and may have completed
callbacks before terminating. Neither partial callback output nor a live Spark
exception is returned. A declared failure that never happens raises
`ExpectedFailureNotObservedError`; a wrong-action, wrong-selector, input,
capture, limit, or cleanup failure remains an error rather than passing this
test. The runner stops only its own query and removes its run directory after
the call; the caller's session remains usable until the fixture stops it.

The current source defaults to 10,000 captured output rows, 8 MiB of encoded
input per batch, and 64 MiB per run. These are small-test guardrails, **not**
limits on Spark state, process memory, or execution time. The runner has no
per-action timeout. Read the [bounded-runner safety policy](design/0005-bounded-capture-policy.md)
and [deadline no-go](design/0006-spark-deadline-feasibility.md) before using
larger or unbounded scenarios. See [ADR 0007](design/0007-expected-streaming-failure.md)
for selector and error-precedence details.
