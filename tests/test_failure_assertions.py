from __future__ import annotations

import pytest

from streamcase import FailureObservation, ScenarioResult, assert_failure


def _result(
    action_index: int = 2,
    *,
    message: str = "STREAMCASE_BAD_RECORD: invalid input",
    condition: str | None = "CAST_INVALID_INPUT",
) -> ScenarioResult:
    return ScenarioResult(
        (),
        failure=FailureObservation(action_index, message=message, condition=condition),
    )


def test_assert_failure_accepts_action_kind_condition_and_marker() -> None:
    result = _result()

    assert_failure(result, action_index=2)
    assert_failure(
        result,
        action_index=2,
        kind="query_terminated",
        condition="CAST_INVALID_INPUT",
        message_contains="STREAMCASE_BAD_RECORD",
    )


@pytest.mark.parametrize(
    "message",
    [
        "Spark 3.5 wrapper: STREAMCASE_BAD_RECORD: query aborted",
        "Spark 4.2 different prose: STREAMCASE_BAD_RECORD: task failed",
    ],
)
def test_assert_failure_matches_caller_marker_not_spark_version_prose(message: str) -> None:
    assert_failure(
        _result(message=message), action_index=2, message_contains="STREAMCASE_BAD_RECORD"
    )


def test_assert_failure_reports_absent_observation_without_exception_text() -> None:
    with pytest.raises(AssertionError) as failure:
        assert_failure(ScenarioResult(()), action_index=2)

    assert str(failure.value) == (
        "Failure assertion failed: failure observation.\n"
        "Expected: action 2, kind query_terminated.\n"
        "Actual: none."
    )


def test_assert_failure_reports_wrong_action() -> None:
    with pytest.raises(AssertionError) as failure:
        assert_failure(_result(1), action_index=2)

    assert str(failure.value) == (
        "Failure assertion failed: action index.\nExpected: 2.\nActual: 1."
    )


def test_assert_failure_reports_wrong_classification() -> None:
    observation = FailureObservation(2, message="marker")
    object.__setattr__(observation, "kind", "callback_error")
    result = ScenarioResult((), failure=observation)

    with pytest.raises(AssertionError) as failure:
        assert_failure(result, action_index=2)

    assert str(failure.value) == (
        "Failure assertion failed: failure kind.\n"
        "Expected: query_terminated.\n"
        "Actual: 'callback_error'."
    )


def test_assert_failure_reports_missing_condition_distinctly() -> None:
    with pytest.raises(AssertionError) as failure:
        assert_failure(_result(condition=None), action_index=2, condition="CAST_INVALID_INPUT")

    assert "Failure assertion failed: condition." in str(failure.value)
    assert "Actual: unavailable." in str(failure.value)


def test_assert_failure_reports_wrong_condition_with_bounded_text() -> None:
    with pytest.raises(AssertionError) as failure:
        assert_failure(_result(condition="A" * 256), action_index=2, condition="EXPECTED")

    assert "Expected: 'EXPECTED'." in str(failure.value)
    assert "more chars omitted" in str(failure.value)
    assert len(str(failure.value)) < 250


def test_assert_failure_reports_missing_marker_without_leaking_full_message() -> None:
    result = _result(message="private-data-" * 100)

    with pytest.raises(AssertionError) as failure:
        assert_failure(result, action_index=2, message_contains="EXPECTED_MARKER")

    assert "Failure assertion failed: message marker." in str(failure.value)
    assert "truncated=True" in str(failure.value)
    assert "private-data" not in str(failure.value)


def test_assert_failure_marker_matching_is_case_sensitive_and_uses_preview() -> None:
    result = _result(message="prefix " + "x" * 1_024 + " LATE_MARKER")

    with pytest.raises(AssertionError, match="message marker"):
        assert_failure(result, action_index=2, message_contains="LATE_MARKER")

    with pytest.raises(AssertionError, match="message marker"):
        assert_failure(_result(), action_index=2, message_contains="streamcase_bad_record")


@pytest.mark.parametrize("invalid", [None, object(), "result"])
def test_assert_failure_rejects_non_result(invalid: object) -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult"):
        assert_failure(invalid, action_index=0)  # type: ignore[arg-type]


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_assert_failure_rejects_non_integer_action_index(invalid: object) -> None:
    with pytest.raises(TypeError, match="action_index must be an int"):
        assert_failure(_result(), action_index=invalid)  # type: ignore[arg-type]


def test_assert_failure_rejects_negative_action_index() -> None:
    with pytest.raises(ValueError, match="action_index must be non-negative"):
        assert_failure(_result(), action_index=-1)


def test_assert_failure_rejects_non_string_kind() -> None:
    with pytest.raises(TypeError, match="kind must be a str"):
        assert_failure(_result(), action_index=2, kind=1)  # type: ignore[arg-type]


@pytest.mark.parametrize("invalid", ["", "callback_error", "QUERY_TERMINATED"])
def test_assert_failure_rejects_unsupported_expected_kind(invalid: str) -> None:
    with pytest.raises(ValueError, match="kind must be 'query_terminated'"):
        assert_failure(_result(), action_index=2, kind=invalid)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["condition", "message_contains"])
def test_assert_failure_rejects_non_string_optional_field(field: str) -> None:
    with pytest.raises(TypeError, match=f"{field} must be a str or None"):
        assert_failure(_result(), action_index=2, **{field: 1})  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["condition", "message_contains"])
def test_assert_failure_rejects_blank_optional_field(field: str) -> None:
    with pytest.raises(ValueError, match=f"{field} must not be blank"):
        assert_failure(_result(), action_index=2, **{field: " \t "})  # type: ignore[arg-type]


@pytest.mark.parametrize(("field", "size"), [("condition", 257), ("message_contains", 1_025)])
def test_assert_failure_rejects_unmatchable_expected_field(field: str, size: int) -> None:
    with pytest.raises(ValueError, match=f"{field} must contain at most"):
        assert_failure(_result(), action_index=2, **{field: "A" * size})  # type: ignore[arg-type]


def test_assert_failure_accepts_exact_expected_field_bounds() -> None:
    result = _result(message="M" * 1_024, condition="C" * 256)

    assert_failure(
        result,
        action_index=2,
        condition="C" * 256,
        message_contains="M" * 1_024,
    )


def test_assert_failure_bounds_long_expected_marker_in_diagnostic() -> None:
    with pytest.raises(AssertionError) as failure:
        assert_failure(_result(), action_index=2, message_contains="LONG_MARKER_" * 20)

    assert "more chars omitted" in str(failure.value)
    assert len(str(failure.value)) < 250
