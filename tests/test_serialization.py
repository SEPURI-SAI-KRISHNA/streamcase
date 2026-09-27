from __future__ import annotations

import math

import pytest

import streamcase
from streamcase import batch
from streamcase._serialization import _encode_batch_json_lines


def test_batch_json_lines_encoding_is_compact_and_sorts_keys() -> None:
    first = batch({"status": "created", "order_id": 1})
    second = batch({"order_id": 1, "status": "created"})

    first_encoded = _encode_batch_json_lines(first)
    second_encoded = _encode_batch_json_lines(second)

    assert first_encoded == '{"order_id":1,"status":"created"}\n'
    assert second_encoded == first_encoded


def test_batch_json_lines_encoding_preserves_row_order_and_unicode() -> None:
    action = batch({"city": "Zürich"}, {"city": "東京"})

    encoded = _encode_batch_json_lines(action)

    assert encoded == '{"city":"Zürich"}\n{"city":"東京"}\n'


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_batch_json_lines_encoding_rejects_non_finite_floats(value: float) -> None:
    with pytest.raises(ValueError, match="Out of range float values"):
        _encode_batch_json_lines(batch({"value": value}))


def test_batch_json_lines_encoder_is_not_exported() -> None:
    assert not hasattr(streamcase, "_encode_batch_json_lines")
