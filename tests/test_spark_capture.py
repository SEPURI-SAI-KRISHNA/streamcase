from __future__ import annotations

from typing import Any, cast

import pytest

from streamcase import ScenarioLimitError
from streamcase._spark_capture import _BatchCapture


class _FakeRow:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values
        self.recursive_arguments: list[bool] = []

    def asDict(self, recursive: bool = False) -> dict[str, object]:  # noqa: N802
        self.recursive_arguments.append(recursive)
        return self.values


class _FakeDataFrame:
    def __init__(self, *rows: _FakeRow) -> None:
        self.rows = list(rows)
        self.collect_calls = 0
        self.limit_calls: list[int] = []

    def limit(self, count: int) -> _FakeDataFrame:
        self.limit_calls.append(count)
        return _FakeDataFrame(*self.rows[:count])

    def collect(self) -> list[_FakeRow]:
        self.collect_calls += 1
        return list(self.rows)


def test_callbacks_preserve_order_ids_rows_and_empty_batches() -> None:
    capture = _BatchCapture()
    tags = ["new"]
    first_row = _FakeRow({"id": 1, "details": {"tags": tags}})
    first_dataframe = _FakeDataFrame(first_row)
    empty_dataframe = _FakeDataFrame()

    capture.callback(cast(Any, first_dataframe), 7)
    first_snapshot = capture.snapshot()
    capture.callback(cast(Any, empty_dataframe), 3)
    tags.append("changed")

    assert first_dataframe.collect_calls == 1
    assert empty_dataframe.collect_calls == 1
    assert first_row.recursive_arguments == [True]
    assert [batch.batch_id for batch in capture.snapshot()] == [7, 3]
    assert capture.snapshot()[0].rows == ({"id": 1, "details": {"tags": ("new",)}},)
    assert capture.snapshot()[1].rows == ()
    assert len(first_snapshot) == 1


def test_failed_conversion_does_not_publish_a_partial_batch() -> None:
    capture = _BatchCapture()
    invalid_dataframe = _FakeDataFrame(_FakeRow({"unsupported": object()}))

    with pytest.raises(TypeError, match="unsupported type object"):
        capture.callback(cast(Any, invalid_dataframe), 0)

    assert capture.snapshot() == ()

    capture.callback(cast(Any, _FakeDataFrame(_FakeRow({"id": 1}))), 1)
    assert [batch.batch_id for batch in capture.snapshot()] == [1]


def test_limited_capture_accepts_exact_boundary_across_callbacks_and_empty_output() -> None:
    capture = _BatchCapture(max_output_rows=2)
    first = _FakeDataFrame(_FakeRow({"id": 1}))
    empty = _FakeDataFrame()
    last = _FakeDataFrame(_FakeRow({"id": 2}))

    capture.callback(cast(Any, first), 0)
    capture.callback(cast(Any, empty), 1)
    capture.callback(cast(Any, last), 2)

    assert [frame.limit_calls for frame in (first, empty, last)] == [[3], [2], [2]]
    assert [frame.collect_calls for frame in (first, empty, last)] == [0, 0, 0]
    assert [len(item.rows) for item in capture.snapshot()] == [1, 0, 1]
    assert capture.limit_error() is None


def test_overflow_collects_only_allowance_plus_one_and_publishes_no_partial_batch() -> None:
    capture = _BatchCapture(max_output_rows=2)
    capture.set_action_index(4)
    first = _FakeDataFrame(_FakeRow({"id": 1}))
    overflow = _FakeDataFrame(*(_FakeRow({"id": index}) for index in range(100)))
    capture.callback(cast(Any, first), 7)

    with pytest.raises(ScenarioLimitError, match="Batch action at index 4") as error_info:
        capture.callback(cast(Any, overflow), 8)

    error = error_info.value
    assert (error.resource, error.limit, error.observed_at_least, error.batch_id) == (
        "output_rows",
        2,
        3,
        8,
    )
    assert overflow.limit_calls == [2]
    assert overflow.collect_calls == 0
    assert capture.snapshot()[0].rows == ({"id": 1},)
    assert len(capture.snapshot()) == 1
    assert capture.limit_error() is error


def test_zero_remaining_allows_an_empty_callback_but_rejects_one_more_row() -> None:
    capture = _BatchCapture(max_output_rows=1)
    capture.callback(cast(Any, _FakeDataFrame(_FakeRow({"id": 1}))), 0)
    empty = _FakeDataFrame()
    extra = _FakeDataFrame(_FakeRow({"id": 2}))

    capture.callback(cast(Any, empty), 1)
    with pytest.raises(ScenarioLimitError) as error_info:
        capture.callback(cast(Any, extra), 2)

    assert empty.limit_calls == [1]
    assert extra.limit_calls == [1]
    assert error_info.value.observed_at_least == 2
    assert [len(item.rows) for item in capture.snapshot()] == [1, 0]


def test_none_opt_out_keeps_the_existing_full_collect_behavior() -> None:
    capture = _BatchCapture(max_output_rows=None)
    frame = _FakeDataFrame(*(_FakeRow({"id": index}) for index in range(3)))

    capture.callback(cast(Any, frame), 0)

    assert frame.collect_calls == 1
    assert frame.limit_calls == []
    assert len(capture.snapshot()[0].rows) == 3
