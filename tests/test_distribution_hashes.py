from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scripts.check_distribution_hashes import MANIFEST_NAME, record_hashes, verify_hashes


@pytest.fixture
def distributions(tmp_path: Path) -> Path:
    (tmp_path / "streamcase-0.1.0a1-py3-none-any.whl").write_bytes(b"wheel bytes")
    (tmp_path / "streamcase-0.1.0a1.tar.gz").write_bytes(b"sdist bytes")
    return tmp_path


def test_record_and_verify_exact_distribution_hashes(distributions: Path) -> None:
    recorded = record_hashes(distributions)

    assert recorded == (
        f"{hashlib.sha256(b'wheel bytes').hexdigest()}  streamcase-0.1.0a1-py3-none-any.whl\n"
        f"{hashlib.sha256(b'sdist bytes').hexdigest()}  streamcase-0.1.0a1.tar.gz\n"
    )
    assert (distributions / MANIFEST_NAME).read_text(encoding="utf-8") == recorded
    assert verify_hashes(distributions) == recorded


def test_verify_rejects_changed_distribution(distributions: Path) -> None:
    record_hashes(distributions)
    (distributions / "streamcase-0.1.0a1.tar.gz").write_bytes(b"changed")

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        verify_hashes(distributions)


def test_verify_rejects_extra_distribution(distributions: Path) -> None:
    record_hashes(distributions)
    (distributions / "surprise.whl").write_bytes(b"extra")

    with pytest.raises(ValueError, match="exactly one wheel and one source distribution"):
        verify_hashes(distributions)


def test_verify_rejects_missing_distribution(distributions: Path) -> None:
    record_hashes(distributions)
    (distributions / "streamcase-0.1.0a1.tar.gz").unlink()

    with pytest.raises(ValueError, match="exactly one wheel and one source distribution"):
        verify_hashes(distributions)


def test_verify_rejects_invalid_manifest(distributions: Path) -> None:
    record_hashes(distributions)
    (distributions / MANIFEST_NAME).write_text("not a hash\n", encoding="utf-8")

    with pytest.raises(ValueError, match="does not list exactly two files"):
        verify_hashes(distributions)


def test_verify_rejects_unexpected_manifest_filename(distributions: Path) -> None:
    recorded = record_hashes(distributions)
    (distributions / MANIFEST_NAME).write_text(
        recorded.replace("streamcase-0.1.0a1.tar.gz", "other-0.1.0a1.tar.gz"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid or unexpected filename"):
        verify_hashes(distributions)


def test_verify_rejects_missing_manifest(distributions: Path) -> None:
    record_hashes(distributions)
    (distributions / MANIFEST_NAME).unlink()

    with pytest.raises(ValueError, match="missing distribution hash manifest"):
        verify_hashes(distributions)
