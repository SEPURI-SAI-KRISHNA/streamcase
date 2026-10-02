# Spark compatibility

Streamcase keeps its backend-independent scenario, result, and assertion APIs
usable without Apache Spark. Spark runner users opt into a deliberately narrow,
tested dependency line.

## Initial support matrix

| Component | Supported versions | Policy |
| --- | --- | --- |
| Python | 3.10, 3.11, 3.12, and 3.13 | Every version runs the core test suite |
| PySpark | 4.2.x (`>=4.2,<4.3`) | The Spark integration lane uses this minor line |
| Java | 17 | The Spark integration lane uses this LTS baseline |

All three rows apply together. A Python version passing the core suite alone does
not establish Spark compatibility; the dedicated integration lane is the source
of truth for the complete combination.

The path-filtered Spark workflow runs on runner, packaging, compatibility-policy,
and Spark-test changes. It reports the Python, Java, and PySpark versions before
starting local-mode integration tests. The ordinary unit and Python compatibility
jobs explicitly exclude tests marked `spark` and do not install PySpark.

Apache Spark 4.2 supports Python 3.10 or newer and Java 17, 21, and 25. Streamcase
starts with the smaller matrix above so every advertised combination can be
verified in CI. Java 21 and 25, other Spark 4.x minors, and Spark 3.x may work but
are not supported until dedicated compatibility issues add them to the matrix.

The upstream requirements are documented in the
[Spark 4.2 overview](https://spark.apache.org/docs/4.2.0/index.html) and
[PySpark installation guide](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html).

## Installation

The default installation contains no PySpark dependency:

```shell
python -m pip install streamcase
```

Install Streamcase with its Spark integration dependency using the `spark`
extra:

```shell
python -m pip install "streamcase[spark]"
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
- Importing the future `streamcase.spark` namespace will require the extra and
  produce an actionable installation error when it is absent.
- The installed PySpark minor version should match a remote Spark cluster's
  minor version. Streamcase's initial runner and CI use local mode.

These boundaries keep assertion-only environments small and allow future Spark
compatibility expansions to be reviewed independently.

## Updating the policy

Support for a new Spark minor, Python version, or Java runtime requires a scoped
issue and pull request that update all of the following together:

1. the `spark` extra in `pyproject.toml`;
2. this support matrix;
3. the Spark integration CI lane; and
4. any version-specific runner compatibility code or tests.

Patch releases within the supported PySpark 4.2 line are accepted automatically.
Pre-releases are not part of the supported matrix.
