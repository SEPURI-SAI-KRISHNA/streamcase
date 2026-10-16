"""Keep the progress guide's Spark example executable in the supported lane."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.spark


def test_fixed_event_time_progress_guide_example_runs() -> None:
    guide = Path(__file__).resolve().parents[2] / "docs" / "progress-and-state-assertions.md"
    marker = "## Spark example (fixed event time)\n\n"
    source = guide.read_text(encoding="utf-8")
    section = source.split(marker, 1)[1].split("\n## ", 1)[0]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    assert source.count(marker) == 1

    pytest.importorskip("pyspark")
    exec(compile(code, str(guide), "exec"), {"__name__": "__main__"})
