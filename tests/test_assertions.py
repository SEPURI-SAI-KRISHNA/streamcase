from __future__ import annotations

import math

import pytest

from streamcase import CapturedBatch, ScenarioResult, assert_rows_equal


def test_assert_rows_equal_ignores_row_and_mapping_key_order() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [
                    {"order_id": 1001, "status": "created"},
                    {"order_id": 1002, "status": "paid"},
                ],
            ),
        ],
    )
    expected = [
        {"status": "paid", "order_id": 1002},
        {"status": "created", "order_id": 1001},
    ]

    assert_rows_equal(result, expected)


def test_assert_rows_equal_preserves_duplicate_multiplicity() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [
                    {"order_id": 1001},
                    {"order_id": 1001},
                    {"order_id": 1002},
                ],
            ),
        ],
    )

    with pytest.raises(AssertionError) as error_info:
        assert_rows_equal(
            result,
            [
                {"order_id": 1001},
                {"order_id": 1003},
                {"order_id": 1003},
            ],
        )

    message = str(error_info.value)
    assert "expected 3 row(s), captured 3" in message
    assert "Missing rows (2)" in message
    assert message.count("'order_id': 1003") == 2
    assert "Unexpected rows (2)" in message
    assert "'order_id': 1002" in message


def test_assert_rows_equal_compares_nested_values_recursively() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [{"payload": {"codes": [1, 2], "active": True}}],
            ),
        ],
    )

    assert_rows_equal(
        result,
        [{"payload": {"active": True, "codes": [1, 2]}}],
    )

    with pytest.raises(AssertionError):
        assert_rows_equal(
            result,
            [{"payload": {"active": True, "codes": [2, 1]}}],
        )


def test_assert_rows_equal_detects_nested_mapping_keys_and_sequence_lengths() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [{"payload": {"codes": [1, 2]}}],
            ),
        ],
    )

    with pytest.raises(AssertionError):
        assert_rows_equal(result, [{"payload": {"other": [1, 2]}}])

    with pytest.raises(AssertionError):
        assert_rows_equal(result, [{"payload": {"codes": [1]}}])


def test_assert_rows_equal_is_type_sensitive() -> None:
    result = ScenarioResult([CapturedBatch(0, [{"value": True}])])

    with pytest.raises(AssertionError):
        assert_rows_equal(result, [{"value": 1}])


def test_assert_rows_equal_treats_float_nan_values_as_equal() -> None:
    result = ScenarioResult([CapturedBatch(0, [{"value": math.nan}])])

    assert_rows_equal(result, [{"value": math.nan}])

    with pytest.raises(AssertionError):
        assert_rows_equal(result, [{"value": 1.0}])


def test_assert_rows_equal_accepts_generator_without_mutating_source() -> None:
    codes = [1, 2]
    expected = [{"payload": {"codes": codes}}]
    result = ScenarioResult([CapturedBatch(0, expected)])

    assert_rows_equal(result, (row for row in expected))

    assert codes == [1, 2]
    assert expected == [{"payload": {"codes": [1, 2]}}]


def test_assert_rows_equal_reports_only_missing_rows() -> None:
    result = ScenarioResult([])

    with pytest.raises(AssertionError, match=r"Missing rows \(1\)") as error_info:
        assert_rows_equal(result, [{"order_id": 1001}])

    assert "Unexpected rows" not in str(error_info.value)


def test_assert_rows_equal_reports_only_unexpected_rows() -> None:
    result = ScenarioResult([CapturedBatch(0, [{"order_id": 1001}])])

    with pytest.raises(AssertionError, match=r"Unexpected rows \(1\)") as error_info:
        assert_rows_equal(result, [])

    assert "Missing rows" not in str(error_info.value)


def test_assert_rows_equal_rejects_non_result_argument() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult; got list"):
        assert_rows_equal([], [])  # type: ignore[arg-type]
