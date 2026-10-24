# Streamcase

[![CI](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml/badge.svg)](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/LICENSE)

Streamcase is an early-development pytest-oriented toolkit for deterministic Apache Spark
Structured Streaming tests.

> **Alpha status:** Streamcase is available on PyPI, but its API remains
> experimental and may change in later alphas. The features in a source
> checkout can be newer than a published package. Check the release notes for
> the exact capabilities and tested combinations of the version you install.

The [0.2.0a1 release notes](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/releases/0.2.0a1.md)
describe its expanded alpha scope and limitations. The verified prerelease is
available on [PyPI](https://pypi.org/project/streamcase/0.2.0a1/).

## Problem

Ordinary DataFrame assertions do not exercise streaming semantics such as
micro-batch boundaries, late data, watermarks, checkpoint restarts, or state
growth. Streamcase will make those behaviors explicit and reproducible in Python
tests while executing against Spark's public Structured Streaming interfaces.

## Installation

Install the lightweight backend-independent package from PyPI. The `--pre`
option allows alpha releases; pin a version when reproducibility matters:

```shell
python -m pip install --pre streamcase
```

For the Spark runner, install the optional `spark` extra and provide a
compatible Java JDK separately:

```shell
python -m pip install --pre "streamcase[spark]"
```

The `0.2.0a1` release adds a separate `spark35` extra for PySpark 3.5.9 or
later 3.5 patches. Install it with:

```shell
python -m pip install "streamcase[spark35]==0.2.0a1"
```

For the opt-in pytest fixture, combine its `pytest` extra with exactly one
Spark extra. For the Spark 4.2 line of `0.2.0a1`:

```shell
python -m pip install "streamcase[pytest,spark]==0.2.0a1"
```

See the [pytest integration guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/pytest-integration.md)
for explicit activation, a runnable example, and session ownership. The older
`0.1.0a2` distribution does not include the `pytest` or `spark35` extras.

These extras keep PySpark out of the core dependency set. See the
[Spark compatibility policy](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/spark-compatibility.md) for the supported
Python, Java, and Spark combination. Contributors working from a checkout can
use the [editable-install instructions](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/CONTRIBUTING.md#development-setup).

## Capabilities in 0.2.0a1

Streamcase can describe immutable logical input batches and checkpoint-preserving
restart boundaries, represent captured output, and assert row equality, batch
counts, unique keys, and captured progress metrics without importing PySpark.
The public Spark runner executes batches and restarts against a caller-owned
session. It also offers an opt-in pytest runner fixture. See the
[scenario model guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/scenario-model.md) and
[results and assertions guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/results-and-assertions.md), plus the
[progress and state assertions guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/progress-and-state-assertions.md),
[two-batch Spark quick start and runner contract](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/spark-runner.md) and
[checkpoint restart example](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/restart-scenarios.md),
and [pytest integration guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/pytest-integration.md).

## Repository status

This bootstrap establishes:

- Python packaging with a `src` layout;
- Apache License 2.0 and project governance;
- issue and pull-request conventions;
- formatting, linting, type checking, tests, and package validation in CI;
- security and release policies.

See [ROADMAP.md](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/ROADMAP.md) for the planned delivery sequence and
[docs/architecture.md](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/architecture.md) for the proposed technical shape.

## Local checks

```shell
python -m venv .venv
python -m pip install -e ".[dev]"
ruff format --check .
ruff check .
mypy
pytest -m "not spark"
python -m build
python -m twine check dist/*
```

Spark contributors can run the isolated integration suite with the setup in the
[contribution guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/CONTRIBUTING.md#development-setup).

## Contributing

All material work starts with a GitHub issue and reaches `main` only through a
reviewed pull request. Read [CONTRIBUTING.md](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/CONTRIBUTING.md) before making a
change.

## Trademark notice

Apache Spark, Spark, Apache Flink, and Flink are trademarks of the Apache Software
Foundation. Streamcase is independent software and is not endorsed by the Apache
Software Foundation.

## License

Licensed under the [Apache License 2.0](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/LICENSE).
