from __future__ import annotations

from pathlib import Path

import pytest

from streamcase import ExpectedFailureNotObservedError, batch, expect_failure, restart, scenario

pytestmark = pytest.mark.spark

_MARKER = "STREAMCASE_EXPECTED_BAD_INPUT"


def test_expected_query_failure_after_restart_returns_partial_result(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-expected-failure-smoke")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    def reject_bad(source: DataFrame) -> DataFrame:
        return source.selectExpr(
            f"CASE WHEN value = 'bad' THEN raise_error('{_MARKER}') ELSE value END AS value"
        )

    try:
        result = run_scenario(
            spark,
            scenario(
                batch({"value": "good"}),
                restart(),
                expect_failure(batch({"value": "bad"}), message_contains=_MARKER),
            ),
            schema="value STRING",
            transform=reject_bad,
            base_dir=tmp_path,
        )

        assert result.rows == ({"value": "good"},)
        assert [item.action_index for item in result.progress] == [0]
        assert result.failure is not None
        assert result.failure.action_index == 2
        assert result.failure.kind == "query_terminated"
        assert len(result.failure.message) <= 1_024
        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
        assert spark.range(1).count() == 1
    finally:
        spark.stop()


def test_expected_failure_missing_or_wrong_marker_does_not_pass(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import DataFrame, SparkSession

    from streamcase.spark import run_scenario

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-expected-failure-negative-smoke")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    def reject_bad(source: DataFrame) -> DataFrame:
        return source.selectExpr(
            f"CASE WHEN value = 'bad' THEN raise_error('{_MARKER}') ELSE value END AS value"
        )

    try:
        with pytest.raises(ExpectedFailureNotObservedError, match="action index 0"):
            run_scenario(
                spark,
                scenario(expect_failure(batch({"value": "good"}), message_contains=_MARKER)),
                schema="value STRING",
                transform=reject_bad,
                base_dir=tmp_path,
            )

        with pytest.raises(RuntimeError, match="did not match its selectors"):
            run_scenario(
                spark,
                scenario(expect_failure(batch({"value": "bad"}), message_contains="WRONG_MARKER")),
                schema="value STRING",
                transform=reject_bad,
                base_dir=tmp_path,
            )

        assert list(tmp_path.glob("streamcase-run-*")) == []
        assert not any(query.name.startswith("streamcase-run-") for query in spark.streams.active)
        assert spark.range(1).count() == 1
    finally:
        spark.stop()
