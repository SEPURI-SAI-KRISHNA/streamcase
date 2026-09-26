"""Immutable scenario model for ordered streaming-test actions."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias

from streamcase.actions import Batch, Restart

Action: TypeAlias = Batch | Restart


@dataclass(frozen=True, slots=True, init=False)
class Scenario:
    """A non-empty, ordered sequence of supported scenario actions."""

    actions: tuple[Action, ...]

    def __init__(self, actions: Iterable[Action]) -> None:
        """Snapshot and validate an ordered iterable of actions."""
        snapshot = tuple(actions)
        if not snapshot:
            raise ValueError("Scenario must contain at least one action.")

        for index, action in enumerate(snapshot):
            if not isinstance(action, (Batch, Restart)):
                message = (
                    f"Scenario action at index {index} must be a Batch or Restart; "
                    f"got {type(action).__name__}."
                )
                raise TypeError(message)

        object.__setattr__(self, "actions", snapshot)


def scenario(*actions: Action) -> Scenario:
    """Create a :class:`Scenario` from one or more ordered actions."""
    return Scenario(actions)
