from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from datetime import date, datetime
from decimal import Decimal

import pytest

from streamcase import CapturedBatch, ScenarioResult


def test_captured_batch_recursively_snapshots_and_freezes_rows() -> None:
    tags = ["new"]
    metadata = {"priority": 1}
    row: dict[str, object] = {
        "order_id": 1001,
        "tags": tags,
        "metadata": metadata,
        "optional": None,
        "amount": Decimal("10.50"),
        "created_on": date(2026, 9, 29),
        "created_at": datetime(2026, 9, 29, 12, 30),
        "payload": b"ok",
    }

    captured = CapturedBatch(0, [row])
    tags.append("paid")
    metadata["priority"] = 2
    row["order_id"] = 9999

    assert captured.rows[0]["order_id"] == 1001
    assert captured.rows[0]["tags"] == ("new",)
    assert captured.rows[0]["metadata"] == {"priority": 1}
    assert captured.rows[0]["optional"] is None
    assert captured.rows[0]["amount"] == Decimal("10.50")
    assert captured.rows[0]["created_on"] == date(2026, 9, 29)
    assert captured.rows[0]["created_at"] == datetime(2026, 9, 29, 12, 30)
    assert captured.rows[0]["payload"] == b"ok"
    assert isinstance(captured.rows[0], Mapping)
    assert isinstance(captured.rows[0]["metadata"], Mapping)


def test_captured_batch_exposes_read_only_nested_mappings() -> None:
    captured = CapturedBatch(0, [{"metadata": {"priority": 1}}])
    metadata = captured.rows[0]["metadata"]

    assert isinstance(metadata, Mapping)
    with pytest.raises(TypeError):
        metadata["priority"] = 2  # type: ignore[index]


def test_captured_batch_allows_zero_rows() -> None:
    assert CapturedBatch(0, []).rows == ()


@pytest.mark.parametrize("batch_id", [True, 1.5, "1"])
def test_captured_batch_rejects_non_integer_id(batch_id: object) -> None:
    with pytest.raises(TypeError, match=rf"ID must be an int; got {type(batch_id).__name__}"):
        CapturedBatch(batch_id, [])  # type: ignore[arg-type]


def test_captured_batch_rejects_negative_id() -> None:
    with pytest.raises(ValueError, match="ID must be non-negative"):
        CapturedBatch(-1, [])


def test_captured_batch_rejects_non_mapping_row_with_index() -> None:
    with pytest.raises(TypeError, match=r"row at index 1.*got list"):
        CapturedBatch(0, [{"order_id": 1}, []])  # type: ignore[list-item]


def test_captured_batch_rejects_non_string_nested_key_with_path() -> None:
    with pytest.raises(TypeError, match=r"row\[0\]\.metadata.*got int"):
        CapturedBatch(
            0,
            [{"metadata": {1: "priority"}}],
        )


def test_captured_batch_rejects_unsupported_value_with_path() -> None:
    with pytest.raises(TypeError, match=r"row\[1\]\.values\[0\].*set"):
        CapturedBatch(
            0,
            [
                {"values": [1]},
                {"values": [{1, 2}]},
            ],
        )


def test_captured_batch_is_frozen_and_has_stable_value_equality() -> None:
    captured = CapturedBatch(3, [{"order_id": 1001}])

    assert captured == CapturedBatch(3, [{"order_id": 1001}])
    with pytest.raises(FrozenInstanceError):
        captured.batch_id = 4  # type: ignore[misc]


def test_scenario_result_snapshots_batches_and_flattens_rows() -> None:
    first = CapturedBatch(4, [{"order_id": 1001}, {"order_id": 1002}])
    second = CapturedBatch(9, [{"order_id": 1003}])
    batches = [first, second]

    result = ScenarioResult(batches)
    batches.clear()

    assert result.batches == (first, second)
    assert result.rows == (
        {"order_id": 1001},
        {"order_id": 1002},
        {"order_id": 1003},
    )


def test_scenario_result_allows_zero_batches() -> None:
    result = ScenarioResult([])

    assert result.batches == ()
    assert result.rows == ()


def test_scenario_result_rejects_unsupported_batch_with_index() -> None:
    with pytest.raises(TypeError, match=r"batch at index 1.*got str"):
        ScenarioResult([CapturedBatch(0, []), "batch"])  # type: ignore[list-item]


def test_scenario_result_is_frozen_and_has_stable_value_equality() -> None:
    result = ScenarioResult([CapturedBatch(0, [{"order_id": 1001}])])

    assert result == ScenarioResult([CapturedBatch(0, [{"order_id": 1001}])])
    with pytest.raises(FrozenInstanceError):
        result.batches = ()  # type: ignore[misc]
