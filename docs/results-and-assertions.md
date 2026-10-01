# Results and assertions

Streamcase provides immutable, backend-independent result objects and
pytest-friendly assertion functions. They can be constructed and tested without
PySpark, Java, a checkpoint, or a running streaming query.

The future Spark runner will return the same public `ScenarioResult` model after
capturing query output. Until that runner exists, the API is directly useful for
unit tests, assertion exploration, and integration development.

## Quick start

```python
from streamcase import (
    CapturedBatch,
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
)

result = ScenarioResult(
    [
        CapturedBatch(
            0,
            [
                {"order_id": 1001, "status": "created"},
                {"order_id": 1002, "status": "created"},
            ],
        ),
        CapturedBatch(
            1,
            [{"order_id": 1001, "status": "paid"}],
        ),
    ],
)

assert_batch_count(result, 2)
assert_rows_equal(
    result,
    [
        {"order_id": 1001, "status": "paid"},
        {"order_id": 1002, "status": "created"},
        {"order_id": 1001, "status": "created"},
    ],
)
assert_unique_keys(result, "order_id", "status")
```

All three assertions return `None` when they pass and raise `AssertionError`
when captured data does not meet the expectation. Invalid function arguments
raise `TypeError` or `ValueError`.

## Immutable result hierarchy

### CapturedBatch

`CapturedBatch(batch_id, rows)` represents one captured output micro-batch.
The batch identifier must be a non-negative integer; booleans are not accepted
as integers. Unlike an input `Batch`, a captured batch may contain zero rows.

Rows are recursively snapshotted:

- row and nested mapping keys must be strings;
- mappings become read-only mappings with their original insertion order;
- lists and tuples become immutable tuples;
- `None`, booleans, integers, floats, strings, bytes, decimals, dates, and
  datetimes are retained;
- unsupported mutable or custom values raise `TypeError` with a row and value
  path.

Mutating a source row, list, or nested mapping after construction cannot change
the captured batch.

### ScenarioResult

`ScenarioResult(batches)` snapshots captured batches in execution order and
may be empty. It exposes:

- `result.batches`: captured batches in execution order;
- `result.rows`: all captured rows flattened in batch and row order.

Batch identifiers do not need to be consecutive. The future runner owns their
relationship to backend epoch identifiers, including across restarts.

Both models are frozen, use stable value equality, and contain no Spark or JVM
objects.

## Row equality

`assert_rows_equal(result, expected_rows)` compares all flattened result rows
as an unordered multiset.

- Row order does not matter.
- Duplicate multiplicity does matter.
- Mapping key order does not matter.
- Tuple element order does matter.
- Scalar types must match, so `True` is different from `1`.
- Two floating-point NaN values compare equal.
- Nested mappings and tuples do not need to be hashable.

Expected rows are normalized through the same recursive rules as captured rows.
The expected iterable and its nested values are not mutated.

For example, one missing row and one unexpected duplicate produce:

```text
Rows differ.
Expected: 2 row(s).
Actual: 2 row(s).
Missing rows (1):
  - {'order_id': 1002}
Unexpected rows (1):
  - {'order_id': 1001}
```

## Batch count

`assert_batch_count(result, expected_count)` checks the exact number of
captured output batches. Empty captured batches count because they still
represent output micro-batch callbacks.

A mismatch reports both values:

```text
Captured batch count differs.
Expected: 2 batch(es).
Actual: 1 batch(es).
```

The expected count must be a non-negative integer. A boolean, float, string, or
negative integer is invalid API usage rather than an assertion mismatch.

## Unique keys

`assert_unique_keys(result, *fields)` verifies one scalar or composite key
across every row in every captured batch:

```python
assert_unique_keys(result, "order_id", "status")
```

At least one unique string field name is required. Repeating a field name raises
`ValueError`. A missing field is an assertion failure because the captured data
does not satisfy the requested key contract.

Key values use the same recursive, type-sensitive equality as row comparison.
Nested mappings and tuples are supported without hashing. Duplicate diagnostics
include the occurrence count, batch identifier, and row position:

```text
Unique-key assertion failed.
Duplicate keys (1):
  - Duplicate key (order_id=1001) occurred 2 times at batch 0 row 0, batch 1 row 0.
```

The assertion never deduplicates or rewrites results.

## Deterministic bounded diagnostics

Failure output sorts mapping keys for stable display while retaining captured
row and occurrence order. Missing rows, unexpected rows, duplicate-key groups,
and occurrence locations display at most five entries per section.

When more entries exist, the message keeps the total count and states exactly
how many were omitted:

```text
  ... 3 more item(s) omitted
```

Tests should normally assert meaningful message fragments instead of the entire
presentation string. Public assertion behavior and diagnostic content are
stable; whitespace and presentation details may evolve.

## Boundaries

The Phase 2 API intentionally excludes:

- Spark DataFrames and Spark Row objects as assertion inputs;
- schema comparison and approximate numeric equality;
- batch-scoped assertion overloads;
- progress, watermark, and state-store assertions;
- custom result-value normalizers.

Those capabilities require separate issue-linked design work. The accepted
[result and assertion API decision](design/0001-result-assertion-api.md) records
the rationale and alternatives for the current contract.
