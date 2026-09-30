"""Backend-independent assertions for captured streaming results."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import cast

from streamcase._diagnostics import (
    _format_batch_count_mismatch,
    _format_duplicate_keys,
    _format_missing_key_fields,
    _format_rows_mismatch,
)
from streamcase.results import CapturedBatch, ScenarioResult


def assert_batch_count(result: ScenarioResult, expected_count: int) -> None:
    """Assert the exact number of captured output batches."""
    if not isinstance(result, ScenarioResult):
        message = f"result must be a ScenarioResult; got {type(result).__name__}."
        raise TypeError(message)
    if isinstance(expected_count, bool) or not isinstance(expected_count, int):
        message = f"expected_count must be an int; got {type(expected_count).__name__}."
        raise TypeError(message)
    if expected_count < 0:
        raise ValueError("expected_count must be non-negative.")

    actual_count = len(result.batches)
    if actual_count != expected_count:
        raise AssertionError(_format_batch_count_mismatch(expected_count, actual_count))


def _values_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False

    if isinstance(left, float):
        right_float = cast(float, right)
        if math.isnan(left) and math.isnan(right_float):
            return True
        return left == right_float

    if isinstance(left, Mapping):
        right_mapping = cast(Mapping[object, object], right)
        if set(left) != set(right_mapping):
            return False
        for key, left_value in left.items():
            if not _values_equal(left_value, right_mapping[key]):
                return False
        return True

    if isinstance(left, tuple):
        right_tuple = cast(tuple[object, ...], right)
        if len(left) != len(right_tuple):
            return False
        for left_value, right_value in zip(left, right_tuple, strict=True):
            if not _values_equal(left_value, right_value):
                return False
        return True

    return left == right


def assert_unique_keys(result: ScenarioResult, *fields: str) -> None:
    """Assert that selected fields form unique keys across all result rows."""
    if not isinstance(result, ScenarioResult):
        message = f"result must be a ScenarioResult; got {type(result).__name__}."
        raise TypeError(message)
    if not fields:
        raise ValueError("At least one key field is required.")

    seen_fields: set[str] = set()
    for index, field in enumerate(fields):
        if not isinstance(field, str):
            message = f"Key field at index {index} must be a string; got {type(field).__name__}."
            raise TypeError(message)
        if field in seen_fields:
            raise ValueError(f"Key fields must be unique; repeated {field!r}.")
        seen_fields.add(field)

    groups: list[tuple[tuple[object, ...], list[tuple[int, int]]]] = []
    for captured_batch in result.batches:
        for row_index, row in enumerate(captured_batch.rows):
            missing_fields = tuple(field for field in fields if field not in row)
            if missing_fields:
                raise AssertionError(
                    _format_missing_key_fields(
                        captured_batch.batch_id,
                        row_index,
                        missing_fields,
                    ),
                )

            key = tuple(row[field] for field in fields)
            matched_locations: list[tuple[int, int]] | None = None
            for existing_key, locations in groups:
                if _values_equal(key, existing_key):
                    matched_locations = locations
                    break

            location = (captured_batch.batch_id, row_index)
            if matched_locations is None:
                groups.append((key, [location]))
            else:
                matched_locations.append(location)

    duplicates: list[tuple[tuple[str, ...], tuple[object, ...], tuple[tuple[int, int], ...]]] = []
    for key, locations in groups:
        if len(locations) < 2:
            continue
        duplicates.append((fields, key, tuple(locations)))

    if duplicates:
        raise AssertionError(_format_duplicate_keys(duplicates))


def assert_rows_equal(
    result: ScenarioResult,
    expected_rows: Iterable[Mapping[str, object]],
) -> None:
    """Assert duplicate-aware equality of unordered flattened result rows."""
    if not isinstance(result, ScenarioResult):
        message = f"result must be a ScenarioResult; got {type(result).__name__}."
        raise TypeError(message)

    expected = CapturedBatch(0, expected_rows).rows
    unmatched_actual = list(result.rows)
    missing: list[Mapping[str, object]] = []

    for expected_row in expected:
        matched_index: int | None = None
        for index, actual_row in enumerate(unmatched_actual):
            if _values_equal(expected_row, actual_row):
                matched_index = index
                break

        if matched_index is None:
            missing.append(expected_row)
        else:
            unmatched_actual.pop(matched_index)

    if not missing and not unmatched_actual:
        return

    raise AssertionError(
        _format_rows_mismatch(
            len(expected),
            len(result.rows),
            missing,
            unmatched_actual,
        ),
    )
