from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from subprocess import run
from sys import executable
from types import ModuleType

import pytest

from streamcase import ScenarioResult, batch, scenario

pytest_plugins = ("streamcase.pytest_plugin",)


def test_plugin_import_does_not_load_pyspark_or_change_core_imports() -> None:
    script = (
        "import sys; import streamcase; "
        "assert 'streamcase.pytest_plugin' not in sys.modules; "
        "assert 'pytest' not in sys.modules; "
        "import streamcase.pytest_plugin; "
        "assert not any(n == 'pyspark' or n.startswith('pyspark.') for n in sys.modules)"
    )
    completed = run([executable, "-c", script], capture_output=True, text=True, timeout=30)

    assert completed.returncode == 0, completed.stderr


def test_explicit_plugin_load_allows_core_only_collection() -> None:
    completed = run(
        [
            executable,
            "-m",
            "pytest",
            "-p",
            "streamcase.pytest_plugin",
            "--collect-only",
            "-q",
            "tests/test_actions.py",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "test_actions.py::" in completed.stdout


def test_fixture_forwards_public_runner_arguments(
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, object] = {}
    expected = ScenarioResult(())
    spark = object()
    steps = scenario(batch({"id": 1}))

    def transform(source: object) -> object:
        return source

    source_options = {"maxFilesPerTrigger": "1"}
    query_options = {"queryName": "example"}

    def fake_run_scenario(*args: object, **kwargs: object) -> ScenarioResult:
        received["args"] = args
        received["kwargs"] = kwargs
        return expected

    fake_module = ModuleType("streamcase.spark")
    fake_module.__dict__["run_scenario"] = fake_run_scenario
    monkeypatch.setitem(sys.modules, "streamcase.spark", fake_module)

    result = streamcase_runner(
        spark,
        steps,
        schema="id LONG",
        transform=transform,
        output_mode="complete",
        source_options=source_options,
        query_options=query_options,
        retain_artifacts=True,
    )

    assert result is expected
    assert received == {
        "args": (spark, steps),
        "kwargs": {
            "schema": "id LONG",
            "transform": transform,
            "output_mode": "complete",
            "source_options": source_options,
            "query_options": query_options,
            "base_dir": tmp_path,
            "retain_artifacts": True,
        },
    }


def test_fixture_uses_public_defaults(
    streamcase_runner: Callable[..., ScenarioResult],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, object] = {}
    expected = ScenarioResult(())

    def fake_run_scenario(*args: object, **kwargs: object) -> ScenarioResult:
        received.update(kwargs)
        return expected

    fake_module = ModuleType("streamcase.spark")
    fake_module.__dict__["run_scenario"] = fake_run_scenario
    monkeypatch.setitem(sys.modules, "streamcase.spark", fake_module)

    assert (
        streamcase_runner(object(), scenario(batch({"id": 1})), schema="id LONG", transform=id)
        is expected
    )
    assert received == {
        "schema": "id LONG",
        "transform": id,
        "output_mode": "append",
        "source_options": None,
        "query_options": None,
        "base_dir": tmp_path,
        "retain_artifacts": False,
    }
