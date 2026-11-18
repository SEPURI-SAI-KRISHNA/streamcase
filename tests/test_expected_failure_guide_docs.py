"""Keep the expected-failure guide copyable without requiring local Spark."""

from __future__ import annotations

from pathlib import Path


def _guide_block(source: str, heading: str) -> str:
    marker = f"## {heading}\n\n"
    assert source.count(marker) == 1
    section = source.split(marker, 1)[1].split("\n## ", 1)[0]
    return section.split("```python\n", 1)[1].split("\n```", 1)[0]


def test_expected_failure_guide_blocks_compile_without_pyspark() -> None:
    guide = Path(__file__).resolve().parents[1] / "docs" / "expected-failures.md"
    source = guide.read_text(encoding="utf-8")

    for heading in ("Root conftest.py", "Test file"):
        compile(_guide_block(source, heading), str(guide), "exec")

    assert "not** in the published 0.2.0a1 package" in source
    assert "design/0005-bounded-capture-policy.md" in source
    assert 'pytest_plugins = ("streamcase.pytest_plugin",)' in source
