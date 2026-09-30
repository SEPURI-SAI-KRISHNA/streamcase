"""Deterministic internal formatting for assertion failures."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeAlias, cast

_MAX_DIAGNOSTIC_ITEMS = 5

_Location: TypeAlias = tuple[int, int]
_DuplicateKey: TypeAlias = tuple[
    tuple[str, ...],
    tuple[object, ...],
    Sequence[_Location],
]


def _render_value(value: object) -> str:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[str, object], value)
        rendered_mapping_items = (
            f"{key!r}: {_render_value(mapping[key])}" for key in sorted(mapping)
        )
        return "{" + ", ".join(rendered_mapping_items) + "}"

    if isinstance(value, tuple):
        rendered_tuple_items = ", ".join(_render_value(item) for item in value)
        trailing_comma = "," if len(value) == 1 else ""
        return f"({rendered_tuple_items}{trailing_comma})"

    return repr(value)


def _format_item_section(title: str, items: Sequence[str]) -> list[str]:
    lines = [f"{title} ({len(items)}):"]
    visible_items = items[:_MAX_DIAGNOSTIC_ITEMS]
    lines.extend(f"  - {item}" for item in visible_items)

    omitted_count = len(items) - len(visible_items)
    if omitted_count:
        lines.append(f"  ... {omitted_count} more item(s) omitted")
    return lines


def _format_batch_count_mismatch(expected_count: int, actual_count: int) -> str:
    return "\n".join(
        (
            "Captured batch count differs.",
            f"Expected: {expected_count} batch(es).",
            f"Actual: {actual_count} batch(es).",
        ),
    )


def _format_rows_mismatch(
    expected_count: int,
    actual_count: int,
    missing: Sequence[Mapping[str, object]],
    unexpected: Sequence[Mapping[str, object]],
) -> str:
    lines = [
        "Rows differ.",
        f"Expected: {expected_count} row(s).",
        f"Actual: {actual_count} row(s).",
    ]
    if missing:
        rendered_missing = [_render_value(row) for row in missing]
        lines.extend(_format_item_section("Missing rows", rendered_missing))
    if unexpected:
        rendered_unexpected = [_render_value(row) for row in unexpected]
        lines.extend(_format_item_section("Unexpected rows", rendered_unexpected))
    return "\n".join(lines)


def _format_missing_key_fields(
    batch_id: int,
    row_index: int,
    missing_fields: Sequence[str],
) -> str:
    items = [f"batch {batch_id} row {row_index}: {field!r}" for field in missing_fields]
    lines = ["Unique-key assertion failed."]
    lines.extend(_format_item_section("Missing key fields", items))
    return "\n".join(lines)


def _format_duplicate_keys(duplicates: Sequence[_DuplicateKey]) -> str:
    items: list[str] = []
    for fields, key, locations in duplicates:
        rendered_key = ", ".join(
            f"{field}={_render_value(value)}" for field, value in zip(fields, key, strict=True)
        )

        visible_locations = locations[:_MAX_DIAGNOSTIC_ITEMS]
        rendered_locations = ", ".join(
            f"batch {batch_id} row {row_index}" for batch_id, row_index in visible_locations
        )
        omitted_count = len(locations) - len(visible_locations)
        if omitted_count:
            rendered_locations += f", ... {omitted_count} more occurrence(s) omitted"

        items.append(
            f"Duplicate key ({rendered_key}) occurred {len(locations)} times at "
            f"{rendered_locations}."
        )

    lines = ["Unique-key assertion failed."]
    lines.extend(_format_item_section("Duplicate keys", items))
    return "\n".join(lines)
