from __future__ import annotations

from typing import Any, cast

import pytest

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
