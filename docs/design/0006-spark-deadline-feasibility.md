# ADR 0006: Per-action Spark deadline feasibility

- Status: Decision for 0.3.0a1: defer a guaranteed in-process deadline
- Decision scope: Phase 7, issue #170; follow-up #171
- Evidence: guarded probe in `tests/spark/test_deadline_probe.py`, run by both
  Spark 3.5 and 4.2 jobs in `.github/workflows/spark.yml`

## Question

Can Streamcase promise that a Spark `Batch` action has stopped and its owned
query has been cleaned up within a user-specified deadline, without stopping
the caller's Spark session or unrelated queries?

## Public API findings

Spark documents `StreamingQuery.processAllAvailable()` as a blocking testing
barrier and warns that it may block forever with continually arriving data.
It has no timeout parameter. See the [Spark 4.2 reference](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.processAllAvailable.html)
and the [Spark 3.5 query API](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html).

`StreamingQuery.awaitTermination(timeout)` bounds how long the *caller waits
for termination*. Its return value does not stop the query or bound an ongoing
`processAllAvailable()` call. Spark's [3.5 query API](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html)
and [4.2 query API](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/query_management.html)
list waiting and stopping as separate operations. `StreamingQuery.stop()`
stops a query but exposes no deadline or bounded-completion result in either
the [3.5](https://spark.apache.org/docs/3.5.9/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.stop.html)
or [4.2](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.stop.html)
public reference.

This is an API-contract assessment, not a claim that `stop()` always hangs.
An observed quick stop on a cooperative local query cannot establish a
guarantee for a non-cooperative callback, blocked I/O, a remote cluster, or
Spark-managed cancellation. Killing the caller's process or Spark session to
force a deadline would violate Streamcase's ownership contract.

## Reproducible cross-line probe

The guarded probe runs against the installed wheel in both CI Spark lanes.
It starts a file-stream query with a `foreachBatch` callback held by a Python
event, calls `processAllAvailable()` from another thread, and checks that a
fired Python timer and a timed `awaitTermination()` leave that action and query
active. It then requests `stop()` on a separate thread, records whether stop
finished before the callback was released, releases the callback, and checks
eventual query termination and caller-session survival. Each wait has a guard;
the CI step also has a 180-second process timeout. The two
`spark-deadline-probe-*` CI artifacts contain one JSON observation per lane.

The probe is deliberately cooperative after recording its observation. It
tests actual public API behavior on both supported lines without making the
false inference that a successful local cancellation proves a universal
deadline. It records `processAllAvailable()`'s return/error outcome because
concurrent stop can surface a Spark exception instead of normal completion.
Restart reuses the same query ownership boundary: a deadline on one action
would still need to prove that the old query stopped before starting a
replacement. The current API does not supply that bounded proof.

## Decision

Do not add `timeout`, `deadline`, or a Python timer wrapper to
`run_scenario()` or the pytest fixture in 0.3.0a1. A timer can report elapsed
wall time but cannot honestly guarantee that the Spark action has finished,
that `stop()` has returned, or that a restart is safe. A background worker
still using the owned query after the runner returns would violate cleanup
and exception-preservation guarantees. `awaitTermination(timeout)` is not a
substitute for action cancellation.

Issue #171 will record the release-facing no-go and a narrower future
research path. A future deadline proposal needs a separately owned execution
boundary with a defined termination protocol, evidence under non-cooperative
callbacks and both supported Spark lines, and a clear account of what can
remain running after timeout. Until then, Streamcase promises bounded
captured rows and published input bytes, not bounded execution time.
