"""Private isolated filesystem layouts for scenario runs."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

_RUN_DIRECTORY_PREFIX = "streamcase-run-"


def _resolve_base_directory(base_dir: str | os.PathLike[str]) -> Path:
    try:
        raw_path = os.fspath(base_dir)
    except TypeError as error:
        raise TypeError("Runner base directory must be a string or path-like object.") from error

    if not isinstance(raw_path, str):
        raise TypeError("Runner base directory must resolve to a string path.")
    if not raw_path:
        raise ValueError("Runner base directory must not be empty.")

    path = Path(raw_path)
    if not path.exists():
        raise FileNotFoundError(f"Runner base directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Runner base path is not a directory: {path}")
    return path.resolve(strict=True)


def _require_direct_child(path: Path, parent: Path, description: str) -> None:
    if path.parent != parent:
        raise RuntimeError(f"Generated {description} must be a direct child of {parent}: {path}")


@dataclass(frozen=True, slots=True)
class RunDirectories:
    """One private, immutable set of runner-owned directories."""

    root: Path
    input_dir: Path
    checkpoint_dir: Path
    temporary_dir: Path
    retain_artifacts: bool
    _base_dir: Path = field(repr=False, compare=False)

    def cleanup(self) -> None:
        """Remove the generated run root unless artifact retention is enabled."""
        if self.retain_artifacts:
            return
        if not self.root.exists():
            return

        resolved_root = self.root.resolve(strict=True)
        _require_direct_child(resolved_root, self._base_dir, "run root")
        shutil.rmtree(resolved_root)


def create_run_directories(
    *,
    base_dir: str | os.PathLike[str] | None = None,
    retain_artifacts: bool = False,
) -> RunDirectories:
    """Create a unique, isolated directory layout for one scenario run."""
    if not isinstance(retain_artifacts, bool):
        raise TypeError("retain_artifacts must be a bool.")
    if retain_artifacts and base_dir is None:
        raise ValueError("retain_artifacts=True requires an explicit base directory.")

    if base_dir is None:
        root = Path(tempfile.mkdtemp(prefix=_RUN_DIRECTORY_PREFIX)).resolve(strict=True)
        resolved_base = root.parent.resolve(strict=True)
    else:
        resolved_base = _resolve_base_directory(base_dir)
        root = Path(tempfile.mkdtemp(prefix=_RUN_DIRECTORY_PREFIX, dir=resolved_base)).resolve(
            strict=True
        )

    _require_direct_child(root, resolved_base, "run root")

    input_dir = root / "input"
    checkpoint_dir = root / "checkpoint"
    temporary_dir = root / "temporary"
    try:
        input_dir.mkdir()
        input_dir = input_dir.resolve(strict=True)
        _require_direct_child(input_dir, root, "input directory")
        checkpoint_dir.mkdir()
        checkpoint_dir = checkpoint_dir.resolve(strict=True)
        _require_direct_child(checkpoint_dir, root, "checkpoint directory")
        temporary_dir.mkdir()
        temporary_dir = temporary_dir.resolve(strict=True)
        _require_direct_child(temporary_dir, root, "temporary directory")
    except BaseException:
        shutil.rmtree(root)
        raise

    return RunDirectories(
        root=root,
        input_dir=input_dir,
        checkpoint_dir=checkpoint_dir,
        temporary_dir=temporary_dir,
        retain_artifacts=retain_artifacts,
        _base_dir=resolved_base,
    )
