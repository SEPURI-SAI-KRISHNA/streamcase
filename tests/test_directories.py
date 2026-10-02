from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from streamcase._directories import (
    _require_direct_child,
    create_run_directories,
)


def test_automatically_managed_layout_is_created_and_cleaned_up() -> None:
    directories = create_run_directories()

    assert directories.root.name.startswith("streamcase-run-")
    assert directories.input_dir == directories.root / "input"
    assert directories.checkpoint_dir == directories.root / "checkpoint"
    assert directories.temporary_dir == directories.root / "temporary"
    assert all(
        path.is_dir()
        for path in (
            directories.root,
            directories.input_dir,
            directories.checkpoint_dir,
            directories.temporary_dir,
        )
    )

    root = directories.root
    directories.cleanup()
    directories.cleanup()

    assert not root.exists()


def test_caller_base_receives_disjoint_runs_and_is_never_removed(tmp_path: Path) -> None:
    existing = tmp_path / "existing.txt"
    existing.write_text("caller-owned", encoding="utf-8")
    existing_directories = tuple(tmp_path / name for name in ("input", "checkpoint", "temporary"))
    for directory in existing_directories:
        directory.mkdir()

    first = create_run_directories(base_dir=tmp_path)
    second = create_run_directories(base_dir=tmp_path)
    try:
        first_paths = {
            first.root,
            first.input_dir,
            first.checkpoint_dir,
            first.temporary_dir,
        }
        second_paths = {
            second.root,
            second.input_dir,
            second.checkpoint_dir,
            second.temporary_dir,
        }
        assert first_paths.isdisjoint(second_paths)
        assert first.root.parent == tmp_path.resolve()
        assert second.root.parent == tmp_path.resolve()
        assert existing.read_text(encoding="utf-8") == "caller-owned"
        assert all(directory.is_dir() for directory in existing_directories)
    finally:
        first.cleanup()
        second.cleanup()

    assert tmp_path.is_dir()
    assert existing.read_text(encoding="utf-8") == "caller-owned"
    assert all(directory.is_dir() for directory in existing_directories)


def test_retained_run_requires_and_remains_under_caller_base(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match="retain_artifacts=True requires an explicit base directory",
    ):
        create_run_directories(retain_artifacts=True)

    directories = create_run_directories(base_dir=tmp_path, retain_artifacts=True)
    directories.cleanup()

    assert directories.root.is_dir()
    assert directories.root.parent == tmp_path.resolve()


@pytest.mark.parametrize("base_dir", ["", cast(Any, object()), cast(Any, b"bytes")])
def test_invalid_base_directory_types_and_values_are_rejected(base_dir: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        create_run_directories(base_dir=base_dir)


def test_missing_and_non_directory_bases_are_rejected(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        create_run_directories(base_dir=missing)

    file_path = tmp_path / "file.txt"
    file_path.write_text("not a directory", encoding="utf-8")
    with pytest.raises(NotADirectoryError, match="is not a directory"):
        create_run_directories(base_dir=file_path)


def test_retain_artifacts_must_be_a_bool() -> None:
    with pytest.raises(TypeError, match="retain_artifacts must be a bool"):
        create_run_directories(retain_artifacts=cast(Any, 1))


def test_unsafe_generated_path_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="must be a direct child"):
        _require_direct_child(tmp_path.parent, tmp_path, "test path")


def test_partial_layout_is_removed_when_child_creation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_mkdir = Path.mkdir

    def fail_for_checkpoint(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if path.name == "checkpoint":
            raise OSError("injected directory creation failure")
        original_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", fail_for_checkpoint)

    with pytest.raises(OSError, match="injected directory creation failure"):
        create_run_directories(base_dir=tmp_path)

    assert list(tmp_path.glob("streamcase-run-*")) == []
