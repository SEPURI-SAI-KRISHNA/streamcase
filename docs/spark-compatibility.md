# Spark compatibility

Streamcase keeps its backend-independent scenario, result, and assertion APIs
usable without Apache Spark. Spark runner users opt into one of two deliberately
narrow, tested dependency lines.

## Supported matrix in the current source

| Scope | Tested combination | Policy |
| --- | --- | --- |
| Backend-independent package | Python 3.10, 3.11, 3.12, and 3.13; no Spark or Java required | Every listed Python version runs the core test suite |
| Spark 4.2 runner | Python 3.11, Java 17, and PySpark 4.2.x (`>=4.2,<4.3`) together | The `spark` extra and Spark 4.2 CI matrix job test this combination in local mode |
| Spark 3.5 runner | Python 3.11, Java 17, and PySpark 3.5.9 or later 3.5 patch (`>=3.5.9,<3.6`) together | The `spark35` extra and Spark 3.5 CI matrix job test this combination in local mode |

Python 3.10, 3.12, or 3.13 passing the core suite does not establish Spark
runner compatibility on those versions. An extra may install on them, but the
runner support claim covers only the complete combinations tested in CI.

The path-filtered Spark workflow runs on runner, packaging, compatibility-policy,
smoke-check, and Spark-test changes. For each supported line, it builds both
distributions, installs the matching extra in clean environments, and runs a
local two-batch smoke example. It also runs the complete Spark integration
suite, including the version check, from the installed wheel.
The ordinary quality job smoke-tests both distributions without PySpark; unit
and Python compatibility jobs exclude tests marked `spark`.

Apache Spark 4.2 supports Python 3.10 or newer and Java 17, 21, and 25; Spark
3.5 supports Python 3.8 or newer and Java 8, 11, and 17. Streamcase uses the
smaller matrix above so each advertised runner combination is verified in CI.
Other Python and Java combinations, Spark 3.5.0-3.5.8, Spark 4.0-4.1, and other
Spark minors are not in this support claim.

The [Spark 3.5 assessment](spark35-assessment.md) records the passing evidence
and the decision to add the second line. The existing `spark` extra remains
restricted to Spark 4.2; neither extra silently admits Spark 4.0-4.1.

The upstream requirements are documented in the
[Spark 4.2 overview](https://spark.apache.org/docs/4.2.0/index.html) and
[PySpark installation guide](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html),
and the [Spark 3.5 overview](https://spark.apache.org/docs/3.5.9/index.html).

## Installation

The core package is available on PyPI without a PySpark dependency. Use
`--pre` to allow alpha releases:

```shell
python -m pip install --pre streamcase
```

The published `0.1.0a2` release provides the Spark 4.2 `spark` extra:

```shell
python -m pip install --pre "streamcase[spark]"
```

The `spark35` extra is new in the current source and is **not yet available in
the published `0.1.0a2` package**. To try it before the next release, install
from a checkout using Python 3.11 and Java 17:

```shell
python -m pip install -e ".[spark35]"
```

For an editable contributor installation that includes both development and
Spark dependencies, use:

```shell
python -m pip install -e ".[dev,spark]"
```

PySpark is a large source distribution, and neither extra installs Java.
Install a Java 17 JDK separately and make it available through `JAVA_HOME` or
`PATH` before running Spark integration tests.

## Dependency boundaries

- `streamcase` has no required runtime dependencies.
- Only the `spark` and `spark35` extras install PySpark; do not combine them.
- Importing `streamcase` and its backend-independent APIs does not import
  PySpark or initialize a JVM.
- Importing the `streamcase.spark` namespace requires one Spark extra and
  produces an actionable installation error when PySpark is absent.
- The installed PySpark minor version should match a remote Spark cluster's
  minor version. Streamcase's initial runner and CI use local mode.

These boundaries keep assertion-only environments small and allow future Spark
compatibility expansions to be reviewed independently.

## Updating the policy

Support for a new Spark minor, Spark-runner Python version, or Java runtime
requires a scoped issue and pull request that update the relevant parts of:

1. the Spark extras or Python metadata in `pyproject.toml`, if needed;
2. this support matrix;
3. the Spark integration CI lane; and
4. any version-specific runner compatibility code or tests.

Patch releases within the declared 3.5.9+ and 4.2 lines are accepted
automatically and retested by the CI matrix.
Pre-releases are not part of the supported matrix.
