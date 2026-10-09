"""Record and verify the two distribution files passed between release jobs."""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

MANIFEST_NAME = "SHA256SUMS"
MANIFEST_LINE = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9][A-Za-z0-9._+-]*)\Z")


def _distribution_files(directory: Path, *, expect_manifest: bool) -> list[Path]:
    if not directory.is_dir():
        raise ValueError(f"distribution directory does not exist: {directory}")

    manifest = directory / MANIFEST_NAME
    if expect_manifest and not manifest.is_file():
        raise ValueError(f"missing distribution hash manifest: {manifest}")

    files = sorted(
        (path for path in directory.iterdir() if path.name != MANIFEST_NAME or not expect_manifest),
        key=lambda path: path.name,
    )
    if any(not path.is_file() or path.is_symlink() for path in files):
        raise ValueError("distribution directory contains a non-file or symbolic link")
    if len(files) != 2 or sum(path.name.endswith(".whl") for path in files) != 1:
        raise ValueError("expected exactly one wheel and one source distribution")
    if sum(path.name.endswith(".tar.gz") for path in files) != 1:
        raise ValueError("expected exactly one wheel and one source distribution")
    return files


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def record_hashes(directory: Path) -> str:
    """Write a deterministic manifest for one wheel and one sdist."""
    files = _distribution_files(directory, expect_manifest=False)
    manifest = "".join(f"{_sha256(path)}  {path.name}\n" for path in files)
    (directory / MANIFEST_NAME).write_text(manifest, encoding="utf-8")
    return manifest


def verify_hashes(directory: Path) -> str:
    """Reject missing, extra, or changed files before publishing."""
    files = _distribution_files(directory, expect_manifest=True)
    manifest = (directory / MANIFEST_NAME).read_text(encoding="utf-8")
    lines = manifest.splitlines()
    if len(lines) != len(files):
        raise ValueError("distribution hash manifest does not list exactly two files")

    for line, path in zip(lines, files, strict=True):
        match = MANIFEST_LINE.fullmatch(line)
        if match is None or match.group(2) != path.name:
            raise ValueError("distribution hash manifest has an invalid or unexpected filename")
        if match.group(1) != _sha256(path):
            raise ValueError(f"SHA-256 mismatch for {path.name}")
    return manifest


def main(argv: list[str] | None = None) -> int:
    """Run the release-artifact hash check from CI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("record", "verify"))
    parser.add_argument("directory", type=Path)
    arguments = parser.parse_args(argv)

    try:
        manifest = (
            record_hashes(arguments.directory)
            if arguments.mode == "record"
            else verify_hashes(arguments.directory)
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))

    print(manifest, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
