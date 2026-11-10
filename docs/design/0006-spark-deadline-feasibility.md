# ADR 0006: Per-action Spark deadline feasibility

- Status: Accepted no-go for 0.3.0a1: defer a guaranteed in-process deadline
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

The guarded probe ran against the installed wheel in both CI Spark lanes on
2026-11-10. The [Spark 3.5.9 job](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/runs/42779931132/job/129707238623)
and [Spark 4.2.1 job](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/runs/42779931132/job/129707238682)
both passed and uploaded their `spark-deadline-probe-*` observation artifacts.
The recorded outcomes were the same on both lines:

| Observation | Spark 3.5.9 | Spark 4.2.1 |
| --- | --- | --- |
| Python timer fired while `processAllAvailable()` remained blocked | Yes | Yes |
| Timed `awaitTermination(1)` reported query termination | No | No |
| `stop()` completed within the 0.2-second window before callback release | No | No |
| After callback release, `processAllAvailable()` and `stop()` | Both returned | Both returned |
| Query inactive and caller session usable afterward | Yes | Yes |

The probe starts a file-stream query with a `foreachBatch` callback held by a Python
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

Issue #171 records the release-facing no-go and a narrower future research
path. A future deadline proposal needs a separately owned execution
boundary with a defined termination protocol, evidence under non-cooperative
callbacks and both supported Spark lines, and a clear account of what can
remain running after timeout. Until then, Streamcase promises bounded
captured rows and published input bytes, not bounded execution time.

One narrower research candidate is a **separate opt-in process runner** that
creates and owns its own Spark session, returns results over an explicit
inter-process protocol, and can escalate from graceful stop to worker-process
termination. It cannot reuse or kill a session passed to the current
`run_scenario(spark, ...)` API without breaking caller ownership. Such a
proposal would need to specify what happens to checkpoints and external
cluster work after a hard kill, preserve the primary deadline failure when
cleanup also fails, and prove behavior with non-cooperative Python callbacks
and blocked Spark/JVM work on both supported lines. This is research scope,
not a promised feature or a guarantee of graceful cleanup.
