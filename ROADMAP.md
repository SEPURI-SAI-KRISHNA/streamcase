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
checkpoint mutation. Phases 5 and 6 were independent follow-up work, not
prerequisites for `0.1.0a1`. Their completed source work is a candidate for
`0.2.0a1`, subject to its separate release gates; no stable API guarantee is
implied.

The release gate followed a sequence of small, reviewed changes: audit the
PyPI name, metadata, and distributions (#93); decide tag provenance (#94);
reconcile GitHub release assets with PyPI artifacts (#95); configure Trusted
Publishing and the protected environment (#96); test clean wheel and sdist
installs (#97); review the version, changelog, and release notes (#98); then
publish and verify the alpha (#99). CI, the release checklist, and the
documented artifact path agreed before publication. Issue #126 tracked the
post-release documentation correction that shipped in `0.1.0a2`.

## Documentation-correction alpha: 0.1.0a2

`0.1.0a2` was published on 2026-10-09. It corrected PyPI-facing installation
guidance without changing runtime APIs or dependencies. Issue #130 records
publication and verification. Like `0.1.0a1`, it does not contain Phase 5 or 6
features or Spark 3.5 support; see the
[release notes](docs/releases/0.1.0a2.md).

## Phase 5: progress and state assertions

- Cross-version progress normalization.
- Watermark and state-store row assertions.
- Assess Spark 3.5 compatibility and add a CI lane only if the assessment passes.

## Phase 6: pytest integration

- Opt-in fixtures and plugin registration.
- Session ownership and cleanup semantics.
- Documentation examples.

## Proposed next alpha: 0.2.0a1

The `0.2.0a1 - Progress and pytest integration` milestone tracks a proposed
new minor-line alpha. It is **not yet published**; the current PyPI release is
`0.1.0a2`. Issue #152 defines its scope, and issues #153-#158 track the
audit, test, candidate, publication, and follow-up gates. Issue #156 prepares
the source version through a reviewed candidate PR; the `v0.2.0a1` tag must
wait until that PR is merged and issue #157's publication gates pass.

The proposed release adds the completed Phase 5 immutable progress/state
snapshots and input-row, watermark, and state-metric assertions; tested Spark
3.5 support alongside Spark 4.2; and Phase 6's explicitly loaded pytest
fixture with a separate `pytest` extra. The core package remains free of
required PySpark and pytest dependencies. The tested combinations are core
Python 3.10-3.13 without Spark, plus local Spark runs on Python 3.11 and Java
17 with either PySpark 3.5.9 or later 3.5 patches, or PySpark 4.2.x. Other
Python/Java/Spark combinations are not part of this release claim; see the
[compatibility policy](docs/spark-compatibility.md).

Before tagging, the release must pass the
[release checklist](docs/releasing.md): review package metadata and install
guidance, prove clean wheel and sdist installs including the pytest extra on
both Spark lines, pass required CI and security/dependency checks, and merge a
reviewed version/changelog/release-notes candidate PR. Publication then uses
the protected PyPI Trusted Publishing path, with published artifact hashes and
clean installs verified before announcement. Failure of a gate delays the
release; no tag or PyPI upload is implied by completing Phases 5 and 6.

This remains an experimental alpha, not a stable API commitment. Kafka, Delta
Lake, PyFlink, production monitoring, checkpoint mutation, and wider Spark
compatibility remain out of scope. Each excluded backend or integration
requires its own design proposal before entering a later release plan.
