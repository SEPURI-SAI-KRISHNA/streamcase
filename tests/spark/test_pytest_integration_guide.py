"""Run the pytest guide's exact files against the installed Spark wheel."""

from __future__ import annotations

import os
from pathlib import Path
from subprocess import run
from sys import executable

import pytest

pytestmark = pytest.mark.spark


def _guide_block(source: str, heading: str) -> str:
    marker = f"## {heading}\n\n"
    assert source.count(marker) == 1
    section = source.split(marker, 1)[1].split("\n## ", 1)[0]
    return section.split("```python\n", 1)[1].split("\n```", 1)[0]


def test_pytest_guide_runs_as_copied(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")

    guide = Path(__file__).resolve().parents[2] / "docs" / "pytest-integration.md"
    source = guide.read_text(encoding="utf-8")
    (tmp_path / "conftest.py").write_text(
        _guide_block(source, "Root conftest.py"), encoding="utf-8"
    )
    (tmp_path / "test_streaming.py").write_text(_guide_block(source, "Test file"), encoding="utf-8")

    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    completed = run(
        [executable, "-m", "pytest", "-q", "test_streaming.py"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "1 passed" in completed.stdout
