# Changelog

All notable changes will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

No changes yet.

## [0.1.0a1] - Candidate (not published)

This prerelease is prepared but has no publication date yet. See the
[first-alpha release notes](docs/releases/0.1.0a1.md) for the tested support
matrix and known limitations. Add the publication date only after #99 verifies
the release.

### Added

- Clean wheel and sdist installation smoke checks for the core package and the
  supported Spark combination.
- Trusted Publishing release flow with checked tag provenance and SHA-256
  verification across the Actions-artifact transfer to PyPI.
- Public `Restart` execution that rebuilds the Spark stream and resumes later
  batches with the same checkpoint, query settings, and output capture.
- A private Spark query lifecycle controller with idempotent stop and
  restart-safe writer configuration.
- A two-batch public Spark runner quick start and matching integration test.
- Public `streamcase.spark.run_scenario` API with caller-owned session,
  transformation validation, and optional PySpark installation guidance.
- Validated Spark query output modes and non-conflicting writer options for the
  private scenario runner.
- Managed cleanup for runner-owned Spark queries and run directories, preserving
  execution failures when cleanup also fails.
- Immutable `ScenarioResult` assembly after successful Spark batch execution,
  ready for the existing result assertions.
- Ordered `Batch` execution with one published input file and a Spark
  `processAllAvailable()` barrier per action.
- Immutable driver-side capture of Spark `foreachBatch` output, including empty
  output batches and recursively converted nested values.
- A deterministic Spark JSON file-stream source with explicit schemas and
  runner-owned discovery options.
- Atomic publication of deterministic JSON Lines files for scenario batches.
- Isolated runner-owned input, checkpoint, and temporary directory layouts.
- Dedicated Java 17 and PySpark 4.2 integration-test CI lane.
- Optional `spark` extra for the supported PySpark 4.2 line and its compatibility
  policy.
- Immutable `Batch` scenario action and `batch()` convenience constructor.
- Immutable `Restart` lifecycle action and `restart()` convenience constructor.
- Immutable ordered `Scenario` model and `scenario()` convenience constructor.
- Initial package scaffold.
- Packaging, licensing, governance, and contribution documentation.
- Issue forms, pull-request template, and automated quality gates.
- Roadmap for incremental implementation.

### Changed

- Restart failures identify the failed transition and retry cleanup of a
  replacement query registered before Spark raises, preserving cleanup errors
  alongside the original failure.
- Package and distribution metadata now read the version from one source.
- Release automation now rejects tags that do not match the package version.
