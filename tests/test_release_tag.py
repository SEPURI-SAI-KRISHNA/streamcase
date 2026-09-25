from __future__ import annotations

from pathlib import Path

import pytest

import streamcase
from scripts.check_release_tag import load_version, validate_release_tag


@pytest.mark.parametrize(
    ("tag", "version"),
    [
        ("v1.2.3", "1.2.3"),
        ("v1.2.3rc1", "1.2.3rc1"),
    ],
)
def test_release_tag_accepts_exact_version(tag: str, version: str) -> None:
    validate_release_tag(tag, version)


@pytest.mark.parametrize(
    "tag",
    [
        "1.2.3",
        "v1.2.4",
        "v1.2.3-extra",
    ],
)
def test_release_tag_rejects_nonmatching_tag(tag: str) -> None:
    with pytest.raises(ValueError, match=r"expected 'v1\.2\.3'"):
        validate_release_tag(tag, "1.2.3")


def test_version_loader_reads_authoritative_source() -> None:
    version_file = Path(__file__).parents[1] / "src" / "streamcase" / "_version.py"

    assert load_version(version_file) == streamcase.__version__
