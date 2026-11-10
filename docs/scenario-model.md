# Scenario model

Streamcase scenarios describe logical streaming input and lifecycle boundaries
without importing or starting Spark. The public
[`run_scenario()` runner](spark-runner.md) executes them when the optional Spark
extra is installed.

## Quick start

```python
from streamcase import batch, restart, scenario

order_lifecycle = scenario(
    batch(
        {"order_id": 1001, "status": "created"},
        {"order_id": 1002, "status": "created"},
    ),
    restart(),
    batch({"order_id": 1001, "status": "paid"}),
)

assert len(order_lifecycle.actions) == 3
```

The factories return the immutable public `Batch`, `Restart`, and `Scenario`
types. In source checkouts after issue #173, `ExpectFailure` is also a public
declaration type; it does not execute yet. The `Action` type alias represents
`Batch | Restart | ExpectFailure`. These names are exported from `streamcase`;
serialization helpers and runner components remain private.

## Batch contract

`batch(*rows)` creates one non-empty logical input batch:

- row order and key insertion order are preserved;
- every row mapping is copied when the batch is created;
- copied row mappings are exposed as read-only mappings;
- every row key must be a string;
- nested values are neither copied nor frozen.

The snapshot is intentionally shallow:

```python
from streamcase import batch

events = ["created"]
orders = batch({"order_id": 1001, "events": events})

events.append("paid")

assert orders.rows[0]["events"] == ["created", "paid"]
```

Constructing a batch with no rows raises `ValueError`. A non-string row key
raises `TypeError` and reports the zero-based row index and offending key type.

## Scenario contract

`scenario(*actions)` snapshots a non-empty ordered sequence containing only
`Batch`, `Restart`, and `ExpectFailure` actions. The scenario container is
frozen and has stable value equality.

A restart represents a stop and start of the same transformed streaming
query with the same checkpoint. It must be surrounded by batches:

```text
valid:   Batch
valid:   Batch -> Restart -> Batch
invalid: Restart -> Batch
invalid: Batch -> Restart
invalid: Batch -> Restart -> Restart -> Batch
```

Leading, trailing, and consecutive restarts raise `ValueError` with the invalid
zero-based action index. An empty scenario raises `ValueError`. Any action that
is not one of the three supported types raises `TypeError` with its index and
runtime type.

## Expected-failure declaration (source checkout only)

The accepted [Phase 8 design](design/0007-expected-streaming-failure.md)
adds a terminal action wrapping exactly one batch:

```python
from streamcase import batch, expect_failure, restart, scenario

negative_case = scenario(
    batch({"value": "good"}),
    restart(),
    expect_failure(
        batch({"value": "bad"}),
        message_contains="INVALID_RECORD_MARKER",
    ),
)
```

`expect_failure` requires a `Batch` and at least one non-blank, case-sensitive
selector: `message_contains` or `condition`. If both are supplied, both must
match when execution support arrives. The generic Spark `STREAM_FAILED`
condition cannot be the only selector; pair it with a distinctive message
marker. The wrapped batch counts as one action at its own zero-based index.
Only one expected-failure action is allowed, and it must be last. It may follow
a `Restart`, but no action may follow it. Declaration and placement validation
are pure Python and do not import PySpark.

This source checkout **cannot run** such a scenario yet: the runner raises
`NotImplementedError` before creating run artifacts or starting Spark. Issue
#175 adds execution and #174 adds the result observation. Published 0.2.0a1
does not expose `expect_failure`; existing `batch`/`restart` scenarios need no
migration. Do not copy this source-only example into a test pinned to 0.2.0a1.

## Internal JSON Lines contract

The file-backed runner encodes each `Batch` as one JSON Lines input
file. The encoder is private because users describe data with `batch()` rather
than serializing actions themselves.

The internal format has these guarantees:

- one compact JSON object is emitted per row, in batch order;
- object keys are sorted so equivalent mappings produce identical text;
- Unicode characters are preserved rather than ASCII-escaped;
- every encoded batch ends with exactly one newline;
- `NaN`, positive infinity, and negative infinity are rejected as non-standard
  JSON;
- unsupported values raise the standard `TypeError` or `ValueError`, augmented
  with the zero-based row index and chained from the original exception.

`Batch` construction does not recursively validate values. A value such as a
date, decimal, or user-defined object can therefore be modeled but will fail if
the internal JSON encoder cannot represent it. Custom value encoders are not part
of the Phase 1 contract.

## Validation examples

The following examples summarize every Phase 1 construction error:

| Invalid construction | Result |
| --- | --- |
| `batch()` | `ValueError`: a batch needs at least one row |
| `batch({1: "created"})` | `TypeError`: row keys must be strings |
| `scenario()` | `ValueError`: a scenario needs at least one action |
| `Scenario([batch({"id": 1}), "restart"])` | `TypeError`: unsupported action at index 1 |
| `scenario(restart(), batch({"id": 1}))` | `ValueError`: restart cannot be first |
| `scenario(batch({"id": 1}), restart())` | `ValueError`: restart cannot be last |
| two adjacent `restart()` actions | `ValueError`: restarts cannot be consecutive |

None of these model operations requires PySpark, Java, a checkpoint, or a running
streaming query.
