from __future__ import annotations

from typing import Any, cast

import pytest

from streamcase._spark_query_options import _prepare_query_configuration


@pytest.mark.parametrize("mode", ["append", "complete", "update"])
def test_approved_output_modes_keep_a_detached_option_copy(mode: str) -> None:
    caller_options = {"customOption": "original"}

    approved_mode, approved_options = _prepare_query_configuration(mode, caller_options)
    caller_options["customOption"] = "changed"

    assert approved_mode == mode
    assert approved_options == {"customOption": "original"}


def test_missing_options_become_an_empty_mapping() -> None:
    assert _prepare_query_configuration("append", None) == ("append", {})


def test_non_string_output_mode_is_rejected() -> None:
    with pytest.raises(TypeError, match="output mode must be a string"):
        _prepare_query_configuration(cast(Any, None), None)


@pytest.mark.parametrize("mode", ["APPEND", "overwrite", ""])
def test_unsupported_output_mode_is_rejected(mode: str) -> None:
    with pytest.raises(ValueError, match=r"append.*complete.*update"):
        _prepare_query_configuration(mode, None)


def test_non_mapping_options_are_rejected() -> None:
    with pytest.raises(TypeError, match="must be a mapping"):
        _prepare_query_configuration("append", cast(Any, [("key", "value")]))


def test_non_string_option_name_is_rejected() -> None:
    with pytest.raises(TypeError, match="names must be strings"):
        _prepare_query_configuration("append", cast(Any, {1: "value"}))


def test_non_string_option_value_is_rejected() -> None:
    with pytest.raises(TypeError, match="must have a string value"):
        _prepare_query_configuration("append", cast(Any, {"key": 1}))


def test_case_insensitive_duplicate_option_names_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicates a case-insensitive name"):
        _prepare_query_configuration("append", {"custom": "one", "CUSTOM": "two"})


@pytest.mark.parametrize(
    "name",
    [
        "CHECKPOINTLOCATION",
        "QueryName",
        "outputMODE",
        "Format",
        "PATH",
        "sink",
        "FoReAcH",
        "FoReAcHbAtCh",
        "TRIGGER",
        "processingTIME",
        "partitionBY",
    ],
)
def test_runner_owned_query_option_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="Runner-owned query options cannot be overridden"):
        _prepare_query_configuration("append", {name: "override"})


def test_all_reserved_options_are_reported_in_stable_order() -> None:
    with pytest.raises(ValueError, match="Runner-owned query options") as error_info:
        _prepare_query_configuration("append", {"QueryName": "x", "checkpointLOCATION": "y"})

    assert str(error_info.value) == (
        "Runner-owned query options cannot be overridden: 'checkpointLOCATION', 'QueryName'."
    )
