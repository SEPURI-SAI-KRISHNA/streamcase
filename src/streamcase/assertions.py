"""Backend-independent assertions for captured streaming results."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import cast

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
        message = f"Expected {expected_count} captured batch(es), got {actual_count}."
        raise AssertionError(message)


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
                message = (
                    f"Result row at batch {captured_batch.batch_id} row {row_index} "
                    f"is missing key field(s): {missing_fields!r}."
                )
                raise AssertionError(message)

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

    duplicate_messages: list[str] = []
    for key, locations in groups:
        if len(locations) < 2:
            continue

        rendered_key = ", ".join(
            f"{field}={value!r}" for field, value in zip(fields, key, strict=True)
        )
        rendered_locations = ", ".join(
            f"batch {batch_id} row {row_index}" for batch_id, row_index in locations
        )
        duplicate_messages.append(
            f"Duplicate key ({rendered_key}) occurred {len(locations)} times at "
            f"{rendered_locations}."
        )

    if duplicate_messages:
        raise AssertionError(" ".join(duplicate_messages))


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

    details = [
        f"Rows differ: expected {len(expected)} row(s), captured {len(result.rows)}.",
    ]
    if missing:
        missing_rows = tuple(dict(row) for row in missing)
        details.append(f"Missing rows ({len(missing)}): {missing_rows!r}.")
    if unmatched_actual:
        unexpected_rows = tuple(dict(row) for row in unmatched_actual)
        details.append(f"Unexpected rows ({len(unmatched_actual)}): {unexpected_rows!r}.")
    raise AssertionError(" ".join(details))
