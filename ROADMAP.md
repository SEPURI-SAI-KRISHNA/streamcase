# Roadmap

The roadmap is intentionally split into small, reviewable changes. Every phase
requires its own GitHub issue and pull request. Later phases may be adjusted based
on evidence from earlier ones.

## Phase 0: project foundation

- Packaging, licensing, governance, contribution policy, and CI.
- No public streaming API.

## Phase 1: scenario model

- Immutable input-batch and lifecycle action types.
- Validation and serialization rules.
- Unit tests only; no Spark execution.

## Phase 2: result model and assertions

- Immutable captured batch/result types.
- Row equality, batch-count, and duplicate-key assertions.
- Readable failure diagnostics.

## Phase 3: file-backed Spark runner

- One JSON input file per logical batch.
- `processAllAvailable()` synchronization.
- `foreachBatch` output capture.
- One supported Spark line initially.

## Phase 4: checkpoint restart scenarios

- Explicit stop/start lifecycle action.
- Same-checkpoint restart guarantees.
- Failure cleanup and isolation tests.

## First alpha release: 0.1.0a1

The first alpha was published on 2026-10-09 after Phase 4 and before Phases 5
and 6. It is a prerelease, not a promise of a stable API. Release work is
tracked in the `0.1.0a1 - First alpha` milestone; issue #99 records the
publication and verification evidence.

The first alpha includes:

- Backend-independent `Batch`, `Restart`, `Scenario`, `CapturedBatch`, and
  `ScenarioResult` models; their public constructors and helpers; and row
  equality, batch-count, and duplicate-key assertions.
- A file-backed JSON Spark runner that executes batches, captures output with
  `foreachBatch`, and restarts a query using the same checkpoint. The caller
  owns the Spark session; the runner owns its query and temporary resources.
- Core-package support on Python 3.10 through 3.13, as tested by the Python
  compatibility CI matrix. The Spark runner's initially tested combination is
  Python 3.11, Java 17, and PySpark 4.2.x in local mode. Other combinations
  are not part of the first-alpha support claim; see
  [Spark compatibility](docs/spark-compatibility.md).

The first alpha does **not** include progress, watermark, or state-store
assertions; Spark 3.5 or other Spark-version compatibility; pytest fixtures or
plugin registration; Kafka, Delta Lake, PyFlink, production monitoring, or
checkpoint mutation. Phases 5 and 6 remain independent follow-up work, not
prerequisites for `0.1.0a1`. A later alpha may include them after their own
issues, tests, and documentation; no stable-release date or API guarantee is
implied.

The release gate followed a sequence of small, reviewed changes: audit the
PyPI name, metadata, and distributions (#93); decide tag provenance (#94);
reconcile GitHub release assets with PyPI artifacts (#95); configure Trusted
Publishing and the protected environment (#96); test clean wheel and sdist
installs (#97); review the version, changelog, and release notes (#98); then
publish and verify the alpha (#99). CI, the release checklist, and the
documented artifact path agreed before publication. Issue #126 tracks the
post-release documentation correction needed before a subsequent prerelease.

## Phase 5: progress and state assertions

- Cross-version progress normalization.
- Watermark and state-store row assertions.
- Assess Spark 3.5 compatibility and add a CI lane only if the assessment passes.

## Phase 6: pytest integration

- Opt-in fixtures and plugin registration.
- Session ownership and cleanup semantics.
- Documentation examples.

Kafka, Delta Lake, PyFlink, production monitoring, and checkpoint mutation are
explicitly outside the initial roadmap. Each requires a separate design proposal.
