# Streamcase

[![CI](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml/badge.svg)](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/LICENSE)

Streamcase is an early-development pytest-oriented toolkit for deterministic Apache Spark
Structured Streaming tests.

> **Status:** the Spark runner, including checkpoint-preserving restart actions,
> scenario model, results, and assertions are implemented. No release is published yet.
> The proposed [first-alpha scope](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/ROADMAP.md#first-alpha-candidate-010a1) is
> narrower than the longer-term roadmap.

## Problem

Ordinary DataFrame assertions do not exercise streaming semantics such as
micro-batch boundaries, late data, watermarks, checkpoint restarts, or state
growth. Streamcase will make those behaviors explicit and reproducible in Python
tests while executing against Spark's public Structured Streaming interfaces.

## Installation

Until the first PyPI release, install the lightweight backend-independent
package from this checkout with:

```shell
python -m pip install -e .
```

Install the approved PySpark line for the Spark runner from this checkout with:

```shell
python -m pip install -e ".[spark]"
```

The Spark extra keeps PySpark out of the core dependency set. See the
[Spark compatibility policy](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/spark-compatibility.md) for the supported
Python, Java, and Spark combination.

## Available today

Streamcase can describe immutable logical input batches and checkpoint-preserving
restart boundaries, represent captured output, and assert row equality, batch
counts, and unique keys without importing PySpark. The public Spark runner
executes batches and restarts against a caller-owned session. See the
[scenario model guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/scenario-model.md) and
[results and assertions guide](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/results-and-assertions.md), plus the
[two-batch Spark quick start and runner contract](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/spark-runner.md) and
[checkpoint restart example](https://github.com/SEPURI-SAI-KRISHNA/streamcase/blob/main/docs/restart-scenarios.md).

## Planned capabilities

- progress, watermark, and state-store assertions;
- pytest fixtures;
- compatibility testing across supported Spark releases.

These items are roadmap goals, not current package features. Each capability has
its own issue, tests, documentation, and pull request.

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
