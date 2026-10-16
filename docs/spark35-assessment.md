# Spark 3.5 compatibility assessment

Issue #110 evaluates the current public runner on one reproducible Spark 3.5
combination. It does not expand Streamcase's supported Spark range. The `spark`
extra remains `pyspark>=4.2,<4.3`; [the support matrix](spark-compatibility.md)
continues to describe only the Spark 4.2 combination.

## Assessment matrix

| Component | Assessment version | Reason |
| --- | --- | --- |
| Streamcase | Wheel built from the #110 pull request | Exercises the installable artifact, not an editable checkout |
| Python | 3.11 | Shared with the supported Spark 4.2 lane |
| Java | Temurin 17 | Shared with the supported Spark 4.2 lane |
| PySpark | 3.5.9 | Current Spark 3.5 maintenance release at the time of assessment |
| Execution | GitHub Actions Ubuntu local mode | Matches the supported lane's platform and execution mode |

Apache's [Spark 3.5 overview](https://spark.apache.org/docs/3.5.9/index.html)
lists Python 3.8+ and Java 8/11/17. The
[PySpark installation guide](https://spark.apache.org/docs/3.5.9/api/python/getting_started/install.html)
also documents these runtime prerequisites. Spark 3.5.9 is listed in the
[Apache release history](https://spark.apache.org/downloads/).

## Reproduction and boundaries

The temporary `.github/workflows/spark35-assessment.yml` workflow builds a
wheel, installs it with its `test` extra and an explicit `pyspark==3.5.9` in a
clean environment, checks dependencies, reports runtime versions, and runs
the Spark-marked integration suite. Installing the `spark` extra would pull
PySpark 4.2 by design, so the assessment installs 3.5.9 explicitly instead.

The suite includes public runner and restart examples, cleanup checks,
progress and state assertions, and executable documentation examples. It
excludes only `tests/spark/test_environment.py`, whose purpose is to assert
the *supported* 4.2 version rather than test API compatibility. The ordinary
Spark 4.2 integration workflow still runs separately on this pull request.

The result is limited to this exact runtime combination, local mode, and the
tested public behavior. It does not establish remote-cluster compatibility,
other Spark 3.5 patches, or all Python/Java combinations. A permanent 3.5 CI
lane and dependency-policy change belong to follow-up issue #111 if this
assessment passes.

Supporting both 3.5 and 4.2 would also require maintaining two dependency
lines, two Spark integration runs, and version-sensitive progress/state
expectations. This assessment checks whether that extra maintenance has a
working baseline; it does not commit the project to that cost.

## Evidence and decision

Pending the #110 pull-request CI run. Record its run URL, total and failed
tests, any behavior differences, and a support-or-defer decision here before
merging the assessment. Do not merge this document with a pending verdict.
