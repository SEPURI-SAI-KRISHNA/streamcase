# ADR 0001: Result and assertion API

- Status: Accepted
- Decision scope: Phase 2
- Related issue: #41

## Context

Streamcase needs backend-independent values that can represent output captured
from streaming micro-batches. Those values must remain useful after a Spark query
has stopped and must not retain a DataFrame, Row, session, JVM handle, or callback
owned mutable state.

Assertions also need semantics suited to streaming tests. Spark does not
generally promise row collection order, but duplicate rows and logical
micro-batch boundaries are meaningful. The API must make that distinction clear
without introducing pandas or requiring PySpark for unit tests.

## Decision

Phase 2 will introduce two immutable public result types and three free assertion
functions:

```python
from streamcase import (
    CapturedBatch,
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
)
```

The exact constructor parameter names and assertion signatures defined below are
the compatibility target for the Phase 2 implementation issues.

## Result models

### CapturedBatch

```python
CapturedBatch(
    batch_id: int,
    rows: Iterable[Mapping[str, object]],
)
```

`batch_id` is a non-negative logical output-batch identifier. It is named
generically even when a Spark runner obtains it from the `foreachBatch` epoch
identifier. A captured batch may contain zero rows.

Rows retain their input order for inspection and deterministic diagnostics. Each
row must have string keys and is recursively snapshotted into ordinary,
read-only Python values. The initial normalized value domain is:

- `None`, `bool`, `int`, `float`, `str`, and `bytes`;
- `Decimal`, `date`, and `datetime`;
- tuples containing normalized values;
- read-only string-keyed mappings containing normalized values.

Input lists and tuples both normalize to tuples. Input mappings normalize
recursively to read-only mappings. Unsupported mutable or custom values fail at
construction with their row and value path. This prevents later caller mutation
and keeps live backend objects out of public results.

### ScenarioResult

```python
ScenarioResult(
    batches: Iterable[CapturedBatch],
)
```

A scenario result snapshots captured batches in execution order and may be
empty. It exposes:

- `batches`: the immutable tuple of captured batches;
- `rows`: a read-only flattened tuple of rows in batch order.

The model does not require consecutive batch identifiers. The future Spark
runner, including restart handling, owns the relationship between engine epoch
identifiers and captured batches.

Both result types use stable value equality and useful representations. They can
be imported and constructed without PySpark.

## Assertion style

Assertions are free functions instead of methods:

```python
assert_rows_equal(result, expected_rows)
assert_batch_count(result, expected_count)
assert_unique_keys(result, "account_id", "event_date")
```

Keeping assertions separate prevents the immutable data models from accumulating
test-runner policy and allows future assertion modules to evolve without changing
the result containers.

Successful assertions return `None`. A data mismatch raises `AssertionError`
so pytest reports it naturally. Invalid API arguments raise `TypeError` or
`ValueError`; examples include a negative expected count or an empty key-field
list.

The initial assertion target is `ScenarioResult`. Batch-specific inspection is
available through `result.batches`; batch-scoped assertion overloads are
deferred until a demonstrated use case requires them.

## Row equality

`assert_rows_equal(result, expected_rows)` compares all flattened result rows
as an unordered multiset:

- row order does not affect equality;
- duplicate multiplicity does affect equality;
- mapping key order does not affect equality;
- sequence element order does affect equality;
- scalar values require the same runtime type and equal value;
- two floating-point NaN values compare equal for assertion purposes.

Type-sensitive scalar comparison means `True` does not equal `1`, even though
ordinary Python equality considers them equal.

Expected rows are normalized through the same value rules as captured rows. The
implementation must not require rows or nested values to be hashable. A
deterministic first-unmatched comparison is acceptable because Streamcase targets
small test datasets rather than production-scale result comparison.

Failure information identifies missing and unexpected rows while retaining
duplicate counts. Detailed shared formatting and output truncation are delivered
by the later diagnostics issue.

## Batch-count assertion

`assert_batch_count(result, expected_count)` compares the number of captured
output batches, including captured batches with zero rows. A mismatch reports
both expected and actual counts.

The expected count must be a non-negative `int`; `bool` is rejected even
though it subclasses `int`.

## Unique-key assertion

`assert_unique_keys(result, *fields)` verifies that a scalar or composite key is
unique across all flattened result rows and all captured batches.

- At least one unique string field name is required.
- Repeating a field name is invalid API usage.
- A missing field is an assertion failure, not an invalid function call.
- Duplicate comparison uses the same recursive, type-sensitive value equality as
  row comparison and does not require hashable values.
- Failures identify the key, occurrence count, batch identifier, and row
  position.

The function verifies results but never deduplicates or rewrites them.

## Diagnostic principles

All assertion output must be deterministic. Until shared diagnostics are added,
each assertion provides a concise mismatch message. The dedicated diagnostics
work will standardize:

- expected and actual summaries;
- missing and unexpected rows;
- duplicate keys and occurrence locations;
- stable mapping-key ordering;
- explicit truncation counts for large differences.

Tests should assert meaningful diagnostic fragments rather than entire
presentation strings unless the exact format becomes a documented public
contract.

## Public and private boundaries

The result models and three assertion functions are public and exported from
`streamcase`. Recursive normalization and comparison helpers remain private.

Phase 2 imports no PySpark modules. The future runner converts collected backend
values into this approved model boundary before returning to user code.

The initial API deliberately excludes:

- DataFrame or Spark Row assertion inputs;
- schema comparison;
- approximate numeric equality;
- per-batch assertion overloads;
- progress, watermark, and state-store assertions;
- custom value-normalization plugins.

Each excluded capability requires evidence and a separate issue before it expands
the public contract.

## Alternatives considered

### Assertion methods on ScenarioResult

Rejected because methods couple a stable data container to an expanding
assertion policy and make optional assertion namespaces harder to organize.

### Ordered row comparison

Rejected as the default because Spark output collection order is generally not a
semantic guarantee. Users can inspect `result.rows` directly when order is
deliberately meaningful in a backend-independent unit test.

### Set comparison

Rejected because sets discard duplicate multiplicity and require hashable nested
values.

### pandas-based comparison

Rejected because it adds a heavy dependency, changes type semantics, and is not
needed for small deterministic streaming tests.

### Returning live Spark objects

Rejected because it ties assertions to an active JVM and undermines isolation,
serialization, and failure cleanup.

## Consequences

- Issues #42 through #46 have one shared compatibility target.
- Result construction performs recursive normalization work up front.
- Unordered duplicate-aware matching may be quadratic, which is acceptable for
  intentionally small test results.
- New normalized value types and assertion variants require explicit,
  issue-linked API review.
