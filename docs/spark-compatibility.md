# Spark compatibility

Streamcase keeps its backend-independent scenario, result, and assertion APIs
usable without Apache Spark. Spark runner users opt into a deliberately narrow,
tested dependency line.

## Initial support matrix

| Scope | Tested combination | Policy |
| --- | --- | --- |
| Backend-independent package | Python 3.10, 3.11, 3.12, and 3.13; no Spark or Java required | Every listed Python version runs the core test suite |
| Spark runner | Python 3.11, Java 17, and PySpark 4.2.x (`>=4.2,<4.3`) together | The dedicated Spark integration lane tests this complete combination in local mode |

Python 3.10, 3.12, or 3.13 passing the core suite does not establish Spark
runner compatibility on those versions. The `spark` extra can be installed on
them, but the first-alpha runner support claim covers only the complete
combination tested in the dedicated Spark integration lane.

The path-filtered Spark workflow runs on runner, packaging, compatibility-policy,
smoke-check, and Spark-test changes. It builds both distributions, installs each
with the `spark` extra in its own clean environment, and runs a local two-batch
smoke example. It also runs the Spark integration suite from the installed wheel.
The ordinary quality job smoke-tests both distributions without PySpark; unit
and Python compatibility jobs exclude tests marked `spark`.

Apache Spark 4.2 supports Python 3.10 or newer and Java 17, 21, and 25. Streamcase
starts with the smaller matrix above so the advertised Spark runner combination
is verified in CI. Other Python versions with Spark, Java 21 and 25, other Spark
4.x minors, and Spark 3.x may work but are not supported until dedicated
compatibility issues add them to the matrix.

The upstream requirements are documented in the
[Spark 4.2 overview](https://spark.apache.org/docs/4.2.0/index.html) and
[PySpark installation guide](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html).

## Installation

The core package is available on PyPI without a PySpark dependency. Use
`--pre` to allow alpha releases:

```shell
python -m pip install --pre streamcase
```

The published `spark` extra installs the Spark integration dependency:

```shell
python -m pip install --pre "streamcase[spark]"
```

For an editable contributor installation that includes both development and
Spark dependencies, use:

```shell
python -m pip install -e ".[dev,spark]"
```

PySpark is a large source distribution, and the extra does not install Java.
Install a Java 17 JDK separately and make it available through `JAVA_HOME` or
`PATH` before running Spark integration tests.

## Dependency boundaries

- `streamcase` has no required runtime dependencies.
- Only the `spark` extra installs PySpark.
- Importing `streamcase` and its backend-independent APIs does not import
  PySpark or initialize a JVM.
- Importing the `streamcase.spark` namespace requires the extra and
  produces an actionable installation error when it is absent.
- The installed PySpark minor version should match a remote Spark cluster's
  minor version. Streamcase's initial runner and CI use local mode.

These boundaries keep assertion-only environments small and allow future Spark
compatibility expansions to be reviewed independently.

## Updating the policy

Support for a new Spark minor, Spark-runner Python version, or Java runtime
requires a scoped issue and pull request that update the relevant parts of:

1. the `spark` extra or Python metadata in `pyproject.toml`, if needed;
2. this support matrix;
3. the Spark integration CI lane; and
4. any version-specific runner compatibility code or tests.

Patch releases within the supported PySpark 4.2 line are accepted automatically.
Pre-releases are not part of the supported matrix.
