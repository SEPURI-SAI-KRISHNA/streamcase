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

## Phase 5: progress and state assertions

- Cross-version progress normalization.
- Watermark and state-store row assertions.
- Spark 3.5 and Spark 4.x compatibility tests.

## Phase 6: pytest integration

- Opt-in fixtures and plugin registration.
- Session ownership and cleanup semantics.
- Documentation examples.

## Phase 7: first alpha release

- API review and compatibility statement.
- PyPI Trusted Publishing configuration.
- Signed release and installation verification.

Kafka, Delta Lake, PyFlink, production monitoring, and checkpoint mutation are
explicitly outside the initial roadmap. Each requires a separate design proposal.

