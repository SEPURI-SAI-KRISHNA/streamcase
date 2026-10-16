# Spark 3.5 compatibility assessment

Issue #110 evaluated the public runner on one reproducible Spark 3.5
combination before issue #111 added a separate, CI-enforced `spark35` extra.
The existing `spark` extra remains `pyspark>=4.2,<4.3`; see the current
[support matrix](spark-compatibility.md) for the two distinct lines.

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

The one-off Spark 3.5 assessment workflow built a wheel, installed it with its
`test` extra and an explicit `pyspark==3.5.9` in a clean environment, checked
dependencies, reported runtime versions, and ran the Spark-marked integration
suite. At that point no `spark35` extra existed; installing `spark` would have
pulled PySpark 4.2 by design. The one-off workflow was retired in #111 in
favor of the permanent two-line Spark CI matrix.

The suite includes public runner and restart examples, cleanup checks,
progress and state assertions, and executable documentation examples. It
excluded only `tests/spark/test_environment.py`, which then enforced the 4.2
support policy rather than cross-version behavior. The permanent Spark 3.5 CI
job now runs that version check as well as the public-behavior suite.

The result is limited to this exact runtime combination, local mode, and the
tested public behavior. It does not establish remote-cluster compatibility,
other Spark 3.5 patches, or all Python/Java combinations. The follow-up policy
starts at the tested 3.5.9 patch, admits later 3.5 patches, and tests the
resolved version on each relevant CI run.

Supporting both 3.5 and 4.2 requires maintaining two dependency lines, two
Spark integration jobs, and version-sensitive progress/state expectations.
The passing baseline justified taking on that cost with independent CI jobs.

## Evidence and decision

The [#110 Spark 3.5 assessment run](https://github.com/SEPURI-SAI-KRISHNA/streamcase/actions/runs/38860906634)
completed successfully at commit `b6fab7865e11a088b7baae53e3d73c72995e0181`.
Local collection selected 17 Spark integration cases after excluding the
4.2-only environment-policy case. The successful job indicates no failure in
that selected suite; no behavior difference was observed within its coverage.
The run does not establish equivalence outside those tests.

**Decision:** proceed with narrowly scoped support for Python 3.11, Java 17,
and PySpark `>=3.5.9,<3.6` in local mode, enforced by a permanent CI job and a
separate `spark35` extra in #111. Do not widen the existing `spark` extra or
claim support for Spark 4.0-4.1, earlier 3.5 patches, remote clusters, or
unverified Python/Java combinations. The `spark35` extra is available from
current source only until a later Streamcase release is published.
