"""Guarded observations of public Spark query waiting and stopping behavior."""

from __future__ import annotations

import json
from pathlib import Path
from threading import Event, Thread, Timer, current_thread
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery

pytestmark = [pytest.mark.spark, pytest.mark.deadline_probe]


def test_timer_does_not_finish_a_blocked_spark_action(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamcase-deadline-feasibility-probe")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "batch.json").write_text('{"id":1}\n', encoding="utf-8", newline="\n")
    callback_entered = Event()
    release_callback = Event()
    timer_fired = Event()
    process_finished = Event()
    stop_finished = Event()
    process_errors: list[str] = []
    stop_errors: list[str] = []
    callback_threads: list[str] = []
    process_thread: Thread | None = None
    stop_thread: Thread | None = None
    started_query: StreamingQuery | None = None

    def blocked_callback(_frame: DataFrame, _batch_id: int) -> None:
        callback_threads.append(current_thread().name)
        callback_entered.set()
        if not release_callback.wait(timeout=30):
            raise TimeoutError("deadline probe callback guard expired")

    try:
        query = (
            spark.readStream.schema("id LONG")
            .json(str(input_dir))
            .writeStream.foreachBatch(blocked_callback)
            .option("checkpointLocation", str(tmp_path / "checkpoint"))
            .queryName("streamcase_deadline_probe")
            .start()
        )
        started_query = query

        def process_available() -> None:
            try:
                query.processAllAvailable()
            except BaseException as error:
                process_errors.append(type(error).__name__)
            finally:
                process_finished.set()

        process_thread = Thread(target=process_available, name="spark-process-probe", daemon=True)
        process_thread.start()
        assert callback_entered.wait(timeout=45), "Spark callback did not start within probe guard"

        Timer(0.05, timer_fired.set).start()
        assert timer_fired.wait(timeout=5), "Python timer did not fire"
        assert not process_finished.is_set(), "processAllAvailable unexpectedly finished"
        assert query.isActive, "the query stopped before any stop request"
        timed_wait_result = query.awaitTermination(1)
        assert timed_wait_result is False
        assert query.isActive, "awaitTermination(timeout) stopped the query"

        def stop_query() -> None:
            try:
                query.stop()
            except BaseException as error:
                stop_errors.append(type(error).__name__)
            finally:
                stop_finished.set()

        stop_thread = Thread(target=stop_query, name="spark-stop-probe", daemon=True)
        stop_thread.start()
        stop_completed_before_release = stop_finished.wait(timeout=0.2)
        release_callback.set()
        stop_thread.join(timeout=45)
        process_thread.join(timeout=45)
        assert not stop_thread.is_alive(), "stop did not finish after callback release"
        assert not process_thread.is_alive(), "processAllAvailable did not unblock after stop"
        assert stop_errors == []
        assert not query.isActive
        assert spark.range(1).count() == 1

        observation = {
            "spark_version": spark.version,
            "timer_fired_while_action_blocked": True,
            "callback_thread": callback_threads[0],
            "process_thread": process_thread.name,
            "stop_thread": stop_thread.name,
            "timed_await_terminated_query": timed_wait_result,
            "stop_completed_before_callback_release": stop_completed_before_release,
            "process_all_available_outcome": process_errors[0] if process_errors else "returned",
            "stop_outcome": "returned",
            "query_active_after_stop": query.isActive,
            "caller_session_alive_after_stop": True,
        }
        print(f"STREAMCASE_DEADLINE_PROBE {json.dumps(observation, sort_keys=True)}")
    finally:
        release_callback.set()
        if stop_thread is not None:
            stop_thread.join(timeout=45)
        if process_thread is not None:
            process_thread.join(timeout=45)
        if (
            started_query is not None
            and started_query.isActive
            and (stop_thread is None or not stop_thread.is_alive())
        ):
            started_query.stop()
        if stop_thread is None or not stop_thread.is_alive():
            spark.stop()
