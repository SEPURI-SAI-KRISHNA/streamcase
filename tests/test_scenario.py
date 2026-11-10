from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from streamcase import Action, ExpectFailure, Scenario, batch, expect_failure, restart, scenario


def test_scenario_preserves_action_order() -> None:
    first = batch({"order_id": 1})
    boundary = restart()
    second = batch({"order_id": 2})

    model = scenario(first, boundary, second)

    assert model.actions == (first, boundary, second)
    assert isinstance(model, Scenario)


def test_scenario_snapshots_action_iterable() -> None:
    first = batch({"order_id": 1})
    actions: list[Action] = [first]

    model = Scenario(actions)
    actions.append(restart())

    assert model.actions == (first,)


def test_scenario_container_is_frozen() -> None:
    model = scenario(batch({"order_id": 1}))

    with pytest.raises(FrozenInstanceError):
        model.actions = ()  # type: ignore[misc]


def test_scenario_has_stable_value_equality() -> None:
    first = batch({"order_id": 1})
    second = batch({"order_id": 2})

    assert scenario(first, restart(), second) == Scenario((first, restart(), second))


def test_scenario_rejects_zero_actions() -> None:
    with pytest.raises(ValueError, match="at least one action"):
        scenario()


def test_scenario_rejects_unsupported_action_with_index() -> None:
    with pytest.raises(TypeError, match=r"index 1.*got str"):
        Scenario([batch({"order_id": 1}), "restart"])  # type: ignore[list-item]


def test_scenario_rejects_leading_restart_with_index() -> None:
    with pytest.raises(ValueError, match=r"index 0.*cannot be first"):
        scenario(restart(), batch({"order_id": 1}))


def test_scenario_rejects_trailing_restart_with_index() -> None:
    with pytest.raises(ValueError, match=r"index 1.*cannot be last"):
        scenario(batch({"order_id": 1}), restart())


def test_scenario_rejects_consecutive_restart_with_second_index() -> None:
    with pytest.raises(ValueError, match=r"index 2.*follow another Restart"):
        scenario(
            batch({"order_id": 1}),
            restart(),
            restart(),
            batch({"order_id": 2}),
        )


def test_scenario_accepts_terminal_expected_failure_after_restart() -> None:
    terminal = expect_failure(batch({"id": 2}), message_contains="bad-input")
    model = scenario(batch({"id": 1}), restart(), terminal)

    assert model.actions[-1] is terminal
    assert model == Scenario((batch({"id": 1}), restart(), terminal))
    assert isinstance(model.actions[-1], ExpectFailure)
    with pytest.raises(FrozenInstanceError):
        model.actions = ()  # type: ignore[misc]


def test_scenario_accepts_only_terminal_expected_failure() -> None:
    terminal = expect_failure(batch({"id": 1}), condition="CAST_INVALID_INPUT")
    assert scenario(terminal).actions == (terminal,)


def test_scenario_rejects_expected_failure_before_another_action() -> None:
    terminal = expect_failure(batch({"id": 1}), message_contains="bad-input")
    with pytest.raises(ValueError, match=r"ExpectFailure action at index 0 must be last"):
        scenario(terminal, batch({"id": 2}))


def test_scenario_rejects_restart_after_expected_failure() -> None:
    terminal = expect_failure(batch({"id": 1}), message_contains="bad-input")
    with pytest.raises(ValueError, match=r"ExpectFailure action at index 0 must be last"):
        scenario(terminal, restart(), batch({"id": 2}))


def test_scenario_rejects_duplicate_expected_failures() -> None:
    first = expect_failure(batch({"id": 1}), message_contains="bad-input")
    second = expect_failure(batch({"id": 2}), message_contains="bad-input")
    with pytest.raises(ValueError, match=r"another at index 1"):
        scenario(first, second)
