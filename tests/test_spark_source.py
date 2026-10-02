from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pytest

from streamcase._directories import RunDirectories, create_run_directories
from streamcase._spark_source import _build_json_stream


class _FakeDataFrame:
    isStreaming = True  # noqa: N815 - mirrors the PySpark API


class _RecordingReader:
    def __init__(self) -> None:
        self.schema_value: object | None = None
        self.option_values: list[tuple[str, object]] = []
        self.caller_options: dict[str, str] | None = None
        self.path: str | None = None

    def schema(self, value: object) -> _RecordingReader:
        self.schema_value = value
        return self

    def option(self, name: str, value: object) -> _RecordingReader:
        self.option_values.append((name, value))
        return self

    def options(self, **options: str) -> _RecordingReader:
        self.caller_options = options
        return self

    def json(self, path: str) -> _FakeDataFrame:
        self.path = path
        return _FakeDataFrame()


class _RecordingSpark:
    def __init__(self) -> None:
        self.reader = _RecordingReader()
        self.reader_accesses = 0

    @property
    def readStream(self) -> _RecordingReader:  # noqa: N802 - mirrors the PySpark API
        self.reader_accesses += 1
        return self.reader


@pytest.fixture
def run_directories(tmp_path: Path) -> Iterator[RunDirectories]:
    directories = create_run_directories(base_dir=tmp_path)
    try:
        yield directories
    finally:
        directories.cleanup()


def test_json_source_uses_runner_options_and_copies_caller_options(
    run_directories: RunDirectories,
) -> None:
    spark = _RecordingSpark()
    schema = object()
    options = {"timestampFormat": "yyyy-MM-dd HH:mm:ss", "locale": "en-US"}
    read_only_options = MappingProxyType(options)

    stream = _build_json_stream(
        cast(Any, spark),
        run_directories,
        schema=cast(Any, schema),
        source_options=read_only_options,
    )

    assert stream.isStreaming
    assert spark.reader_accesses == 1
    assert spark.reader.schema_value is schema
    assert spark.reader.option_values == [
        ("maxFilesPerTrigger", 1),
        ("multiLine", "false"),
        ("mode", "FAILFAST"),
    ]
    assert spark.reader.caller_options == options
    assert spark.reader.caller_options is not options
    assert options == {"timestampFormat": "yyyy-MM-dd HH:mm:ss", "locale": "en-US"}
    assert spark.reader.path == str(run_directories.input_dir)


def test_source_options_default_to_an_independent_empty_copy(
    run_directories: RunDirectories,
) -> None:
    spark = _RecordingSpark()

    _build_json_stream(cast(Any, spark), run_directories, schema="id LONG")

    assert spark.reader.caller_options == {}


@pytest.mark.parametrize("schema", [None, "", "   "])
def test_missing_schema_fails_before_spark_reader_access(
    run_directories: RunDirectories,
    schema: object,
) -> None:
    spark = _RecordingSpark()

    with pytest.raises(ValueError, match="schema"):
        _build_json_stream(cast(Any, spark), run_directories, schema=cast(Any, schema))

    assert spark.reader_accesses == 0


@pytest.mark.parametrize(
    "option_name",
    [
        "PaTh",
        "recursiveFileLookup",
        "LATESTFIRST",
        "maxFilesPerTrigger",
        "multiLINE",
        "MODE",
        "schema",
        "encoding",
        "badRecordsPath",
    ],
)
def test_reserved_options_fail_case_insensitively_before_reader_access(
    run_directories: RunDirectories,
    option_name: str,
) -> None:
    spark = _RecordingSpark()

    with pytest.raises(ValueError, match="Runner-owned source options"):
        _build_json_stream(
            cast(Any, spark),
            run_directories,
            schema="id LONG",
            source_options={option_name: "conflict"},
        )

    assert spark.reader_accesses == 0


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (cast(Any, {1: "value"}), "names must be strings"),
        (cast(Any, {"locale": 1}), "must have a string value"),
    ],
)
def test_invalid_source_option_types_fail_before_reader_access(
    run_directories: RunDirectories,
    options: Any,
    message: str,
) -> None:
    spark = _RecordingSpark()

    with pytest.raises(TypeError, match=message):
        _build_json_stream(
            cast(Any, spark),
            run_directories,
            schema="id LONG",
            source_options=options,
        )

    assert spark.reader_accesses == 0
