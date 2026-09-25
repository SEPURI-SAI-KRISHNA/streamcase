"""Validate that a release tag matches the Streamcase package version."""

from __future__ import annotations

import argparse
import runpy
from pathlib import Path

VERSION_FILE = Path(__file__).resolve().parents[1] / "src" / "streamcase" / "_version.py"


def load_version(path: Path) -> str:
    """Load a version string from a trusted Python source file."""
    version = runpy.run_path(str(path)).get("__version__")
    if not isinstance(version, str) or not version:
        message = f"{path} must define a non-empty string __version__"
        raise ValueError(message)
    return version


def validate_release_tag(tag: str, version: str) -> None:
    """Require an exact v-prefixed tag for the supplied package version."""
    expected = f"v{version}"
    if tag != expected:
        message = (
            f"release tag {tag!r} does not match package version {version!r}; expected {expected!r}"
        )
        raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    """Run the release-tag validation command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="the v-prefixed GitHub release tag")
    parser.add_argument(
        "--version-file",
        type=Path,
        default=VERSION_FILE,
        help="path to the authoritative Python version module",
    )
    arguments = parser.parse_args(argv)

    try:
        version = load_version(arguments.version_file)
        validate_release_tag(arguments.tag, version)
    except ValueError as error:
        parser.error(str(error))

    print(f"Release tag {arguments.tag!r} matches package version {version!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
