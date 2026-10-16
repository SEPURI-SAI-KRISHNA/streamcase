"""Keep the progress guide's core-only example executable."""

from __future__ import annotations

from pathlib import Path


def test_core_progress_guide_example_runs_without_spark() -> None:
    guide = Path(__file__).resolve().parents[1] / "docs" / "progress-and-state-assertions.md"
    marker = "## Core-only example\n\n"
    source = guide.read_text(encoding="utf-8")
    section = source.split(marker, 1)[1].split("\n## ", 1)[0]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    assert source.count(marker) == 1

    exec(compile(code, str(guide), "exec"), {"__name__": "__main__"})
