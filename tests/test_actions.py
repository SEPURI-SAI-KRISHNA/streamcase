from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError

import pytest

from streamcase import Batch, Restart, batch, restart


def test_batch_preserves_row_order() -> None:
    first = {"order_id": 1}
    second = {"order_id": 2}

    action = batch(first, second)

    assert action.rows == (first, second)
    assert isinstance(action, Batch)


def test_batch_takes_a_snapshot_of_each_row() -> None:
    row: dict[str, object] = {"status": "created"}

    action = batch(row)
    row["status"] = "cancelled"

    assert action.rows[0]["status"] == "created"


def test_batch_exposes_read_only_rows() -> None:
    action = batch({"status": "created"})
    exposed_row = action.rows[0]

    assert isinstance(exposed_row, Mapping)
    with pytest.raises(TypeError):
        exposed_row["status"] = "cancelled"  # type: ignore[index]


def test_batch_does_not_deep_freeze_values() -> None:
    events = ["created"]
    action = batch({"events": events})

    events.append("paid")

    assert action.rows[0]["events"] == ["created", "paid"]


def test_batch_container_is_frozen() -> None:
    action = batch({"order_id": 1})

    with pytest.raises(FrozenInstanceError):
        action.rows = ()  # type: ignore[misc]


def test_batch_rejects_zero_rows() -> None:
    with pytest.raises(ValueError, match="at least one row"):
        batch()


def test_batch_rejects_non_string_key_with_row_index_and_key_type() -> None:
    with pytest.raises(TypeError, match=r"row at index 1.*type int"):
        batch({"order_id": 1}, {2: "created"})  # type: ignore[dict-item]


def test_restart_factory_returns_restart_action() -> None:
    assert isinstance(restart(), Restart)


def test_restart_actions_have_stable_value_equality() -> None:
    assert restart() == Restart()


def test_restart_action_has_no_mutable_instance_namespace() -> None:
    assert not hasattr(restart(), "__dict__")
