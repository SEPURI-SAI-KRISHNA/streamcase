# Streamcase

[![CI](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml/badge.svg)](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Streamcase is a planned pytest-oriented toolkit for deterministic Apache Spark
Structured Streaming tests.

> **Status:** project scaffold only. No streaming test API has been implemented or
> published yet. Development will proceed through small, issue-linked pull
> requests.

## Problem

Ordinary DataFrame assertions do not exercise streaming semantics such as
micro-batch boundaries, late data, watermarks, checkpoint restarts, or state
growth. Streamcase will make those behaviors explicit and reproducible in Python
tests while executing against Spark's public Structured Streaming interfaces.

## Planned capabilities

- declarative input batches and lifecycle actions;
- deterministic file-backed micro-batch execution;
- explicit stop and restart using the same checkpoint;
- output-row and duplicate-key assertions;
- progress, watermark, and state-store assertions;
- pytest fixtures;
- compatibility testing across supported Spark releases.

These items are roadmap goals, not current package features. Each capability will
have its own issue, tests, documentation, and pull request.

## Repository status

This bootstrap establishes:

- Python packaging with a `src` layout;
- Apache License 2.0 and project governance;
- issue and pull-request conventions;
- formatting, linting, type checking, tests, and package validation in CI;
- security and release policies.

See [ROADMAP.md](ROADMAP.md) for the planned delivery sequence and
[docs/architecture.md](docs/architecture.md) for the proposed technical shape.

## Local scaffold checks

```shell
python -m venv .venv
python -m pip install -e ".[dev]"
ruff format --check .
ruff check .
mypy
pytest
python -m build
python -m twine check dist/*
```

## Contributing

All material work starts with a GitHub issue and reaches `main` only through a
reviewed pull request. Read [CONTRIBUTING.md](CONTRIBUTING.md) before making a
change.

## Trademark notice

Apache Spark, Spark, Apache Flink, and Flink are trademarks of the Apache Software
Foundation. Streamcase is independent software and is not endorsed by the Apache
Software Foundation.

## License

Licensed under the [Apache License 2.0](LICENSE).

