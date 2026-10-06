"""Run the public example in the checkpoint restart guide."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.spark


def test_restart_guide_example_runs() -> None:
    guide = Path(__file__).resolve().parents[2] / "docs" / "restart-scenarios.md"
    marker = "## Minimal example\n\n```python\n"
    source = guide.read_text(encoding="utf-8")
    assert source.count(marker) == 1
    code = source.split(marker, 1)[1].split("\n```", 1)[0]
    compiled = compile(code, str(guide), "exec")

    pytest.importorskip("pyspark")
    exec(compiled, {"__name__": "__main__"})
