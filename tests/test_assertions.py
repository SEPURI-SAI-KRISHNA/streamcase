from __future__ import annotations

import math

import pytest

from streamcase import (
    CapturedBatch,
    ScenarioResult,
    assert_batch_count,
    assert_rows_equal,
    assert_unique_keys,
)


def test_assert_batch_count_includes_empty_captured_batches() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(0, []),
            CapturedBatch(1, [{"order_id": 1001}]),
        ],
    )

    assert_batch_count(result, 2)


def test_assert_batch_count_accepts_empty_result() -> None:
    assert_batch_count(ScenarioResult([]), 0)


def test_assert_batch_count_reports_expected_and_actual_counts() -> None:
    result = ScenarioResult([CapturedBatch(0, [])])

    with pytest.raises(
        AssertionError,
        match=r"Expected 2 captured batch\(es\), got 1",
    ):
        assert_batch_count(result, 2)


@pytest.mark.parametrize("expected_count", [True, 1.5, "1"])
def test_assert_batch_count_rejects_non_integer_count(expected_count: object) -> None:
    with pytest.raises(
        TypeError,
        match=rf"expected_count must be an int; got {type(expected_count).__name__}",
    ):
        assert_batch_count(ScenarioResult([]), expected_count)  # type: ignore[arg-type]


def test_assert_batch_count_rejects_negative_count() -> None:
    with pytest.raises(ValueError, match="expected_count must be non-negative"):
        assert_batch_count(ScenarioResult([]), -1)


def test_assert_batch_count_rejects_non_result_argument() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult; got list"):
        assert_batch_count([], 0)  # type: ignore[arg-type]


def test_assert_unique_keys_accepts_unique_composite_keys_across_batches() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                4,
                [
                    {"account_id": 1001, "event_date": "2026-09-29"},
                    {"account_id": 1002, "event_date": "2026-09-29"},
                ],
            ),
            CapturedBatch(
                9,
                [{"account_id": 1001, "event_date": "2026-09-30"}],
            ),
        ],
    )

    assert_unique_keys(result, "account_id", "event_date")


def test_assert_unique_keys_reports_duplicate_counts_and_locations() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(4, [{"account_id": 1001}, {"account_id": 1002}]),
            CapturedBatch(9, [{"account_id": 1001}]),
        ],
    )

    with pytest.raises(AssertionError) as error_info:
        assert_unique_keys(result, "account_id")

    message = str(error_info.value)
    assert "Duplicate key (account_id=1001) occurred 2 times" in message
    assert "batch 4 row 0" in message
    assert "batch 9 row 0" in message


def test_assert_unique_keys_reports_missing_fields_with_location() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                4,
                [{"account_id": 1001}],
            ),
        ],
    )

    with pytest.raises(AssertionError) as error_info:
        assert_unique_keys(result, "account_id", "event_date")

    message = str(error_info.value)
    assert "batch 4 row 0" in message
    assert "event_date" in message


def test_assert_unique_keys_supports_unhashable_nested_values() -> None:
    nested_key = {"region": "eu", "parts": [1, 2]}
    result = ScenarioResult(
        [
            CapturedBatch(0, [{"key": nested_key}]),
            CapturedBatch(1, [{"key": {"parts": [1, 2], "region": "eu"}}]),
        ],
    )

    with pytest.raises(AssertionError, match=r"occurred 2 times"):
        assert_unique_keys(result, "key")


def test_assert_unique_keys_uses_type_sensitive_values() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [
                    {"key": True},
                    {"key": 1},
                ],
            ),
        ],
    )

    assert_unique_keys(result, "key")


def test_assert_unique_keys_treats_float_nan_values_as_duplicates() -> None:
    result = ScenarioResult(
        [
            CapturedBatch(
                0,
                [
                    {"key": math.nan},
                    {"key": math.nan},
                ],
            ),
        ],
    )

    with pytest.raises(AssertionError, match=r"occurred 2 times"):
        assert_unique_keys(result, "key")


def test_assert_unique_keys_accepts_empty_result() -> None:
    assert_unique_keys(ScenarioResult([]), "account_id")


def test_assert_unique_keys_requires_at_least_one_field() -> None:
    with pytest.raises(ValueError, match="At least one key field"):
        assert_unique_keys(ScenarioResult([]))


@pytest.mark.parametrize("field", [1, None])
def test_assert_unique_keys_rejects_non_string_field(field: object) -> None:
    with pytest.raises(
        TypeError,
        match=rf"field at index 0 must be a string; got {type(field).__name__}",
    ):
        assert_unique_keys(ScenarioResult([]), field)  # type: ignore[arg-type]


def test_assert_unique_keys_rejects_repeated_field() -> None:
    with pytest.raises(ValueError, match=r"fields must be unique; repeated 'account_id'"):
        assert_unique_keys(ScenarioResult([]), "account_id", "account_id")


def test_assert_unique_keys_rejects_non_result_argument() -> None:
    with pytest.raises(TypeError, match="result must be a ScenarioResult; got list"):
        assert_unique_keys([], "account_id")  # type: ignore[arg-type]


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
