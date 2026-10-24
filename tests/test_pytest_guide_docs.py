"""Keep the pytest guide's copyable files syntactically valid without Spark."""

from __future__ import annotations

from pathlib import Path


def test_pytest_guide_blocks_compile_without_pyspark() -> None:
    guide = Path(__file__).resolve().parents[1] / "docs" / "pytest-integration.md"
    source = guide.read_text(encoding="utf-8")

    for heading in ("Root conftest.py", "Test file"):
        marker = f"## {heading}\n\n"
        assert source.count(marker) == 1
        section = source.split(marker, 1)[1].split("\n## ", 1)[0]
        code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
        compile(code, str(guide), "exec")

    assert "[0.2.0a1 prerelease](releases/0.2.0a1.md)" in source
    assert "Use an editable checkout until a later release" not in source
