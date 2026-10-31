from __future__ import annotations

from typing import Any, cast

import pytest

from streamcase import ScenarioLimitError
from streamcase.limits import _validate_positive_limit


def test_limit_error_exposes_action_resource_and_lower_bound_without_spark() -> None:
    error = ScenarioLimitError(
        action_index=2,
        resource="output_rows",
        limit=5,
        observed_at_least=6,
        batch_id=9,
    )

    assert isinstance(error, RuntimeError)
    assert (error.action_index, error.resource, error.limit, error.observed_at_least) == (
        2,
        "output_rows",
        5,
        6,
    )
    assert error.batch_id == 9
    assert "Batch action at index 2 (Spark batch 9) exceeded output_rows limit 5" in str(error)

    input_error = ScenarioLimitError(
        action_index=1,
        resource="input_bytes_per_batch",
        limit=8,
        observed_at_least=9,
    )
    assert input_error.batch_id is None
    assert str(input_error).startswith("Batch action at index 1 exceeded input_bytes_per_batch")


@pytest.mark.parametrize("value", [None, 1, 10_000])
def test_positive_limit_validation_accepts_none_and_positive_ints(value: int | None) -> None:
    assert _validate_positive_limit("max_output_rows", value) is value


@pytest.mark.parametrize("value", [True, False, 1.5, "10", object()])
def test_positive_limit_validation_rejects_other_types(value: object) -> None:
    with pytest.raises(TypeError, match="max_output_rows must be a positive integer or None"):
        _validate_positive_limit("max_output_rows", cast(Any, value))


@pytest.mark.parametrize("value", [0, -1])
def test_positive_limit_validation_rejects_nonpositive_ints(value: int) -> None:
    with pytest.raises(ValueError, match="max_output_rows must be a positive integer or None"):
        _validate_positive_limit("max_output_rows", value)
