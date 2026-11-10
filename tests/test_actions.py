from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError

import pytest

from streamcase import Batch, ExpectFailure, Restart, batch, expect_failure, restart


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


def test_expected_failure_wraps_a_snapshotted_batch_and_exports_public_type() -> None:
    row = {"value": "bad"}
    input_batch = batch(row)
    declaration = expect_failure(input_batch, message_contains="INVALID_RECORD_MARKER")
    row["value"] = "changed"

    assert isinstance(declaration, ExpectFailure)
    assert declaration.batch is input_batch
    assert declaration.batch.rows[0]["value"] == "bad"
    assert declaration.message_contains == "INVALID_RECORD_MARKER"
    assert declaration.condition is None
    assert declaration == ExpectFailure(input_batch, message_contains="INVALID_RECORD_MARKER")
    assert not hasattr(declaration, "__dict__")
    with pytest.raises(FrozenInstanceError):
        declaration.condition = "OTHER"  # type: ignore[misc]


def test_expected_failure_accepts_condition_and_combined_selectors() -> None:
    input_batch = batch({"value": "bad"})

    assert expect_failure(input_batch, condition="CAST_INVALID_INPUT").condition == (
        "CAST_INVALID_INPUT"
    )
    assert (
        expect_failure(
            input_batch,
            condition="STREAM_FAILED",
            message_contains="INVALID_RECORD_MARKER",
        ).message_contains
        == "INVALID_RECORD_MARKER"
    )


@pytest.mark.parametrize("input_value", [None, "bad", restart()])
def test_expected_failure_rejects_non_batch_input(input_value: object) -> None:
    with pytest.raises(TypeError, match="input must be a Batch"):
        expect_failure(input_value, message_contains="marker")  # type: ignore[arg-type]


@pytest.mark.parametrize("selector", [None, 12, True])
def test_expected_failure_rejects_missing_or_wrong_message_selector(selector: object) -> None:
    if selector is None:
        with pytest.raises(ValueError, match="requires message_contains or condition"):
            expect_failure(batch({"id": 1}))
    else:
        with pytest.raises(TypeError, match="message_contains must be a string"):
            expect_failure(batch({"id": 1}), message_contains=selector)  # type: ignore[arg-type]


@pytest.mark.parametrize("selector", [12, True])
def test_expected_failure_rejects_wrong_condition_type(selector: object) -> None:
    with pytest.raises(TypeError, match="condition must be a string"):
        expect_failure(batch({"id": 1}), condition=selector)  # type: ignore[arg-type]


def test_expected_failure_rejects_blank_message_selector() -> None:
    with pytest.raises(ValueError, match="message_contains must not be blank"):
        expect_failure(batch({"id": 1}), message_contains="  \t ")


def test_expected_failure_rejects_blank_condition_selector() -> None:
    with pytest.raises(ValueError, match="condition must not be blank"):
        expect_failure(batch({"id": 1}), condition="  \t ")


def test_expected_failure_rejects_generic_query_condition_alone() -> None:
    with pytest.raises(ValueError, match="STREAM_FAILED requires"):
        expect_failure(batch({"id": 1}), condition="STREAM_FAILED")
