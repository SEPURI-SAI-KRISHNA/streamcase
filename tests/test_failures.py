from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from streamcase import ExpectedFailureNotObservedError, FailureObservation


def test_failure_observation_has_stable_backend_independent_fields() -> None:
    observed = FailureObservation(2, message="invalid input", condition="CAST_INVALID_INPUT")

    assert observed.action_index == 2
    assert observed.kind == "query_terminated"
    assert observed.condition == "CAST_INVALID_INPUT"
    assert observed.message == "invalid input"
    assert observed.truncated is False
    assert observed == FailureObservation(
        2, message="invalid input", condition="CAST_INVALID_INPUT"
    )
    assert not hasattr(observed, "__dict__")


def test_failure_observation_keeps_absent_condition_absent() -> None:
    observed = FailureObservation(0, message="")

    assert observed.condition is None
    assert observed.message == ""
    assert observed.truncated is False


def test_failure_observation_normalizes_newlines_and_truncates_text() -> None:
    raw = "a" * 1_023 + "\r\nb" + "x" * 10_000
    observed = FailureObservation(1, message=raw)

    assert observed.message == "a" * 1_023 + "\n"
    assert len(observed.message) == 1_024
    assert observed.truncated is True
    assert "x" not in observed.message


def test_failure_observation_accepts_exact_message_and_condition_bounds() -> None:
    observed = FailureObservation(1, message="🙂" * 1_024, condition="C" * 256)

    assert observed.message == "🙂" * 1_024
    assert observed.condition == "C" * 256
    assert observed.truncated is False


def test_failure_observation_hides_text_from_automatic_repr() -> None:
    observed = FailureObservation(1, message="SYNTHETIC_SECRET_MARKER")

    assert "SYNTHETIC_SECRET_MARKER" not in repr(observed)


def test_failure_observation_does_not_accept_raw_exception_objects() -> None:
    error = RuntimeError("synthetic error")
    error.__cause__ = ValueError("nested cause")

    with pytest.raises(TypeError, match="message must be a string"):
        FailureObservation(1, message=error)  # type: ignore[arg-type]


@pytest.mark.parametrize("action_index", [True, 1.5, "1"])
def test_failure_observation_rejects_non_integer_action_index(action_index: object) -> None:
    with pytest.raises(TypeError, match="action index must be an int"):
        FailureObservation(action_index, message="failure")  # type: ignore[arg-type]


def test_failure_observation_rejects_negative_action_index() -> None:
    with pytest.raises(ValueError, match="action index must be non-negative"):
        FailureObservation(-1, message="failure")


def test_failure_observation_rejects_non_string_condition() -> None:
    with pytest.raises(TypeError, match="condition must be a string or None"):
        FailureObservation(1, message="failure", condition=7)  # type: ignore[arg-type]


def test_failure_observation_rejects_blank_condition() -> None:
    with pytest.raises(ValueError, match="condition must not be blank"):
        FailureObservation(1, message="failure", condition="  \t ")


def test_failure_observation_rejects_oversized_condition() -> None:
    with pytest.raises(ValueError, match="at most 256 characters"):
        FailureObservation(1, message="failure", condition="C" * 257)


def test_failure_observation_is_frozen() -> None:
    observed = FailureObservation(1, message="failure")

    with pytest.raises(FrozenInstanceError):
        observed.message = "changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        observed.truncated = False  # type: ignore[misc]


def test_missing_expected_failure_has_distinct_error_and_action_index() -> None:
    error = ExpectedFailureNotObservedError(3)

    assert isinstance(error, AssertionError)
    assert error.action_index == 3
    assert str(error) == "Expected query failure at scenario action index 3 was not observed."


@pytest.mark.parametrize("action_index", [False, 1.5, "1"])
def test_missing_expected_failure_rejects_non_integer_action_index(action_index: object) -> None:
    with pytest.raises(TypeError, match="action index must be an int"):
        ExpectedFailureNotObservedError(action_index)  # type: ignore[arg-type]


def test_missing_expected_failure_rejects_negative_action_index() -> None:
    with pytest.raises(ValueError, match="action index must be non-negative"):
        ExpectedFailureNotObservedError(-1)
