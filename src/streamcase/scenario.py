"""Immutable scenario model for ordered streaming-test actions."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias

from streamcase.actions import Batch, ExpectFailure, Restart

Action: TypeAlias = Batch | Restart | ExpectFailure


@dataclass(frozen=True, slots=True, init=False)
class Scenario:
    """A non-empty, ordered sequence of supported scenario actions.

    Restart actions must appear between batch actions and cannot be consecutive.
    At most one ExpectFailure may appear, and it must be terminal.
    """

    actions: tuple[Action, ...]

    def __init__(self, actions: Iterable[Action]) -> None:
        """Snapshot and validate an ordered iterable of actions."""
        snapshot = tuple(actions)
        if not snapshot:
            raise ValueError("Scenario must contain at least one action.")

        for index, action in enumerate(snapshot):
            if not isinstance(action, (Batch, Restart, ExpectFailure)):
                message = (
                    f"Scenario action at index {index} must be a Batch, Restart, "
                    "or ExpectFailure; "
                    f"got {type(action).__name__}."
                )
                raise TypeError(message)

        expected_indices = [
            index for index, action in enumerate(snapshot) if isinstance(action, ExpectFailure)
        ]
        if len(expected_indices) > 1:
            raise ValueError(
                "Scenario may contain at most one ExpectFailure action; "
                f"found another at index {expected_indices[1]}."
            )
        if expected_indices and expected_indices[0] != len(snapshot) - 1:
            raise ValueError(
                f"Scenario ExpectFailure action at index {expected_indices[0]} must be last."
            )

        if isinstance(snapshot[0], Restart):
            raise ValueError("Scenario Restart action at index 0 cannot be first.")

        last_index = len(snapshot) - 1
        if isinstance(snapshot[last_index], Restart):
            message = f"Scenario Restart action at index {last_index} cannot be last."
            raise ValueError(message)

        for index in range(1, len(snapshot)):
            if isinstance(snapshot[index - 1], Restart) and isinstance(snapshot[index], Restart):
                message = (
                    f"Scenario Restart action at index {index} cannot immediately "
                    "follow another Restart."
                )
                raise ValueError(message)

        object.__setattr__(self, "actions", snapshot)


def scenario(*actions: Action) -> Scenario:
    """Create a :class:`Scenario` from one or more ordered actions."""
    return Scenario(actions)
