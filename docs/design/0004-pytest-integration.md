# ADR 0004: Opt-in pytest runner integration

- Status: Accepted
- Decision scope: Phase 6
- Related issue: #112

## Context

Streamcase already exposes `streamcase.spark.run_scenario(spark, scenario, ...)`.
It accepts a caller-owned `SparkSession`, creates independent files and a
checkpoint per run, stops only its own query, and returns an immutable result.
Tests often already obtain a session from their own pytest fixture. A pytest
integration should make per-test runner directories convenient without taking
over session ownership or making PySpark and pytest core import dependencies.
The existing [runner ownership decision](0002-spark-runner-api.md) remains in
force.

Pytest can load a plugin explicitly through `pytest_plugins` in a root
`conftest.py` or through `-p`. A `pytest11` entry point would instead be
discovered automatically in ordinary pytest runs. Pytest's `tmp_path` fixture
provides a separate directory for each test function. See the
[pytest plugin guide](https://docs.pytest.org/en/stable/how-to/writing_plugins.html)
and [temporary-directory guide](https://docs.pytest.org/en/stable/how-to/tmp_path.html).

## Decision

Issue #113 will add an importable module named `streamcase.pytest_plugin` with
one function-scoped, non-autouse fixture named `streamcase_runner`. There will
be no `pytest11` entry point, automatic registration, marker, or implicit Spark
session fixture. The plugin is activated explicitly, for example in a test
project's root `conftest.py`:

```python
pytest_plugins = ("streamcase.pytest_plugin",)
```

Alternatively, a test invocation can use `pytest -p streamcase.pytest_plugin`.
Do not recommend `pytest_plugins` in a non-root `conftest.py`; pytest warns
because loading a plugin has suite-wide effects. Activation makes the fixture
available, but only tests requesting it set it up.

The fixture value is a callable named `streamcase_runner`, not a new runner
class. It takes an explicit `SparkSession` as its first argument and otherwise
forwards the public `run_scenario` inputs and result unchanged, except that
`base_dir` is fixed to that test's `tmp_path`. It accepts `retain_artifacts`;
retained run roots remain under `tmp_path` for inspection during that test.
Callers needing a different base directory use `run_scenario` directly.

The compatibility target is:

```python
from streamcase import assert_rows_equal, batch, scenario


def test_one_batch(spark_session, streamcase_runner):
    result = streamcase_runner(
        spark_session,  # supplied and owned by the test suite
        scenario(batch({"id": 1})),
        schema="id LONG",
        transform=lambda source: source.selectExpr("id"),
    )
    assert_rows_equal(result, [{"id": 1}])
```

`spark_session` is a fixture or value defined by the user or another plugin;
Streamcase neither defines nor requests a fixture with that name. The callable
accepts the public runner's `schema`, `transform`, `output_mode`,
`source_options`, `query_options`, and `retain_artifacts` parameters with the
same defaults, validation, exception types, and return value. It does not
expose `base_dir`, introduce implicit assertions, or change query timing.

## Dependency and import boundary

Issue #113 will add a small `pytest` optional extra using the existing
`pytest>=8.3,<10` support range, separate from the contributor-oriented `test`
extra. Users combine it with exactly one Spark extra, such as
`streamcase[pytest,spark]` or `streamcase[pytest,spark35]`. The core package
keeps no required runtime dependencies.

`import streamcase` must not import pytest, PySpark, the plugin, or start a
JVM. Importing `streamcase.pytest_plugin` requires pytest but must not import
PySpark. Fixture setup may obtain `tmp_path` but must not import PySpark or
create a session. Only calling `streamcase_runner(...)` loads the optional
Spark runner; absent PySpark yields its actionable installation error. Thus a
core-only pytest suite can register the plugin without installing PySpark, and
a suite that never registers it never pays its import cost.

## Ownership, scope, and cleanup

| Resource | Owner | Rule |
| --- | --- | --- |
| `SparkSession`, Spark context, and pre-existing queries | Caller | The plugin never creates, stops, or reconfigures them |
| One scenario query and generated run root | Public `run_scenario` | Its existing success and failure cleanup guarantees apply to each call |
| Per-test `tmp_path` parent | pytest | The plugin passes it as `base_dir` but never deletes it |
| Returned `ScenarioResult` | Caller | No live Spark or pytest object is retained in it |

The fixture is function-scoped because `tmp_path` is function-scoped. Multiple
calls within a test create separate run roots below the same parent; different
tests never share a parent. There is no session-scoped cache and no plugin
teardown that could stop a caller-owned query. If the runner raises, its own
cleanup attempt and original exception semantics remain intact. If an
assertion fails after a successful call, the query and non-retained run root
have already been cleaned. Retained artifacts follow pytest's temporary-path
retention policy and are not a durable archive; use the public runner with an
explicit base directory for durable artifacts.

## Alternatives considered

- `pytest11` auto-discovery: rejected because simply installing Streamcase
  would load the plugin in unrelated pytest runs, contrary to opt-in behavior.
- A fixture that creates or finds a global `SparkSession`: rejected because it
  would obscure ownership and could stop a session supplied by another plugin.
- A fixture that requests a particular `spark` or `spark_session` fixture:
  rejected because existing suites use different names and scopes. Passing the
  session explicitly works with all of them.
- A session-scoped runner fixture or public runner class: rejected because it
  introduces cross-test state and lifecycle questions for no current benefit.
- A teardown finalizer that stops active queries: rejected because it cannot
  safely distinguish Streamcase-owned queries from the caller's queries; the
  public runner already owns its cleanup.
- No pytest integration: remains valid for users needing custom paths or
  lifecycles, but does not provide a consistent per-test directory convention.

## Consequences and verification

- Issue #113 implements the explicit plugin and fixture without changing
  `run_scenario` or core imports.
- Issue #114 proves core-only collection, per-test isolation, repeated calls,
  caller-session survival, and query/directory cleanup on success and failure
  in both supported Spark CI jobs, without timing sleeps.
- Issue #115 documents installation, explicit activation, ownership, and a
  runnable public-API example. The guide must distinguish current source from
  published releases until the new extra is actually released.
