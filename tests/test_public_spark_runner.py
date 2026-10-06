from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from streamcase import batch, restart, scenario


class _FakeSparkSession:
    pass


class _FakeStructType:
    pass


class _FakeDataFrame:
    def __init__(
        self,
        spark: _FakeSparkSession,
        *,
        streaming: bool = True,
        columns: list[str] | None = None,
    ) -> None:
        self.sparkSession = spark
        self.isStreaming = streaming
        self.columns = ["id"] if columns is None else columns


@pytest.fixture
def spark_api(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    fake_pyspark = ModuleType("pyspark")
    fake_pyspark.__path__ = []
    fake_sql = ModuleType("pyspark.sql")
    fake_sql.__path__ = []
    fake_sql.__dict__["DataFrame"] = _FakeDataFrame
    fake_sql.__dict__["SparkSession"] = _FakeSparkSession
    fake_types = ModuleType("pyspark.sql.types")
    fake_types.__dict__["StructType"] = _FakeStructType
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_types)
    monkeypatch.delitem(sys.modules, "streamcase.spark", raising=False)
    api = importlib.import_module("streamcase.spark")
    yield api
    monkeypatch.delitem(sys.modules, "streamcase.spark", raising=False)


def test_missing_pyspark_explains_the_installation_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.delitem(sys.modules, "streamcase.spark", raising=False)

    with pytest.raises(ImportError, match=r"streamcase\[spark\]"):
        importlib.import_module("streamcase.spark")


def test_unrelated_missing_dependency_is_not_hidden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pyspark = ModuleType("pyspark")
    fake_pyspark.__path__ = []

    class BrokenSqlModule(ModuleType):
        def __getattr__(self, _name: str) -> object:
            raise ModuleNotFoundError("No module named 'py4j'", name="py4j")

    broken_sql = BrokenSqlModule("pyspark.sql")
    broken_sql.__path__ = []
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", broken_sql)
    monkeypatch.delitem(sys.modules, "streamcase.spark", raising=False)

    with pytest.raises(ModuleNotFoundError, match="py4j"):
        importlib.import_module("streamcase.spark")


def test_public_runner_composes_validated_source_and_managed_execution(
    spark_api: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spark = _FakeSparkSession()
    source = _FakeDataFrame(spark)
    output = _FakeDataFrame(spark, columns=["id", "value"])
    expected_scenario = scenario(batch({"id": 1}), restart(), batch({"id": 2}))
    source_calls: list[tuple[object, object, object, object]] = []
    managed_calls: list[dict[str, object]] = []
    transformation_calls: list[object] = []
    sentinel = object()

    def fake_source(
        supplied_spark: object,
        directories: object,
        *,
        schema: object,
        source_options: object,
    ) -> _FakeDataFrame:
        source_calls.append((supplied_spark, directories, schema, source_options))
        return source

    def transform(frame: _FakeDataFrame) -> _FakeDataFrame:
        transformation_calls.append(frame)
        return output

    def fake_managed(
        supplied_scenario: object,
        build_stream: Any,
        **kwargs: object,
    ) -> object:
        assert supplied_scenario is expected_scenario
        managed_calls.append(kwargs)
        assert build_stream(SimpleNamespace(root=tmp_path)) is output
        return sentinel

    monkeypatch.setattr(spark_api, "_build_json_stream", fake_source)
    monkeypatch.setattr(spark_api, "_run_managed_batches", fake_managed)
    caller_source_options = {"timestampFormat": "yyyy-MM-dd"}
    caller_query_options = {"customWriterOption": "yes"}

    result = spark_api.run_scenario(
        spark,
        expected_scenario,
        schema="id LONG",
        transform=transform,
        output_mode="update",
        source_options=caller_source_options,
        query_options=caller_query_options,
        base_dir=tmp_path,
        retain_artifacts=True,
    )

    assert result is sentinel
    assert len(source_calls) == 1
    supplied_spark, supplied_directories, supplied_schema, supplied_options = source_calls[0]
    assert supplied_spark is spark
    assert supplied_directories is not None
    assert supplied_schema == "id LONG"
    assert supplied_options == {"timestampFormat": "yyyy-MM-dd"}
    assert transformation_calls == [source]
    assert managed_calls == [
        {
            "base_dir": tmp_path,
            "retain_artifacts": True,
            "output_mode": "update",
            "query_options": {"customWriterOption": "yes"},
        }
    ]
    assert caller_source_options == {"timestampFormat": "yyyy-MM-dd"}
    assert caller_query_options == {"customWriterOption": "yes"}


def test_public_runner_rejects_invalid_arguments_before_managed_execution(
    spark_api: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = _FakeSparkSession()
    valid_scenario = scenario(batch({"id": 1}))

    def unexpected_managed(*_args: object, **_kwargs: object) -> None:
        pytest.fail("managed execution was called")

    monkeypatch.setattr(spark_api, "_run_managed_batches", unexpected_managed)
    valid = {"schema": "id LONG", "transform": lambda frame: frame}

    with pytest.raises(TypeError, match="spark must be"):
        spark_api.run_scenario(object(), valid_scenario, **valid)
    with pytest.raises(TypeError, match="scenario must be"):
        spark_api.run_scenario(spark, object(), **valid)
    with pytest.raises(TypeError, match="schema must be"):
        spark_api.run_scenario(spark, valid_scenario, schema=None, transform=lambda frame: frame)
    with pytest.raises(ValueError, match="schema must not be empty"):
        spark_api.run_scenario(spark, valid_scenario, schema=" ", transform=lambda frame: frame)
    with pytest.raises(TypeError, match="transform must be callable"):
        spark_api.run_scenario(spark, valid_scenario, schema="id LONG", transform=None)
    with pytest.raises(ValueError, match="Runner-owned source options"):
        spark_api.run_scenario(
            spark, valid_scenario, **valid, source_options={"MAXFILESPERTRIGGER": "2"}
        )
    with pytest.raises(ValueError, match="Runner-owned query options"):
        spark_api.run_scenario(
            spark, valid_scenario, **valid, query_options={"CHECKPOINTLOCATION": "other"}
        )
    with pytest.raises(ValueError, match="output mode must be"):
        spark_api.run_scenario(spark, valid_scenario, **valid, output_mode="overwrite")


def test_struct_type_schema_is_accepted(
    spark_api: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = _FakeSparkSession()
    struct_type = _FakeStructType()

    def fake_managed(_scenario: object, _builder: object, **_kwargs: object) -> object:
        return object()

    monkeypatch.setattr(spark_api, "_run_managed_batches", fake_managed)
    assert (
        spark_api.run_scenario(
            spark,
            scenario(batch({"id": 1})),
            schema=struct_type,
            transform=lambda frame: frame,
        )
        is not None
    )


@pytest.mark.parametrize(
    ("output", "error_type", "message"),
    [
        (object(), TypeError, "must return a DataFrame"),
        ("batch", TypeError, "must return a DataFrame"),
    ],
)
def test_non_dataframe_transform_output_is_rejected(
    spark_api: ModuleType,
    output: object,
    error_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error_type, match=message):
        spark_api._validate_output_frame(output, _FakeSparkSession())


def test_batch_dataframe_and_different_session_are_rejected(spark_api: ModuleType) -> None:
    spark = _FakeSparkSession()
    with pytest.raises(ValueError, match="streaming DataFrame"):
        spark_api._validate_output_frame(_FakeDataFrame(spark, streaming=False), spark)
    with pytest.raises(ValueError, match="supplied session"):
        spark_api._validate_output_frame(_FakeDataFrame(_FakeSparkSession()), spark)


def test_duplicate_output_names_are_rejected_case_insensitively(spark_api: ModuleType) -> None:
    spark = _FakeSparkSession()
    with pytest.raises(ValueError, match="duplicate column names: 'ID'"):
        spark_api._validate_output_frame(_FakeDataFrame(spark, columns=["id", "ID"]), spark)


def test_invalid_transform_output_cleans_generated_run_directory(
    spark_api: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spark = _FakeSparkSession()
    source = _FakeDataFrame(spark)
    monkeypatch.setattr(spark_api, "_build_json_stream", lambda *_args, **_kwargs: source)

    with pytest.raises(TypeError, match="must return a DataFrame"):
        spark_api.run_scenario(
            spark,
            scenario(batch({"id": 1})),
            schema="id LONG",
            transform=lambda _frame: cast(Any, object()),
            base_dir=tmp_path,
        )

    assert list(tmp_path.glob("streamcase-run-*")) == []
