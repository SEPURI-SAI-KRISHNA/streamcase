from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from streamcase import batch
from streamcase._directories import RunDirectories, create_run_directories
from streamcase._input_files import AtomicBatchWriter


@pytest.fixture
def run_directories(tmp_path: Path) -> Iterator[RunDirectories]:
    directories = create_run_directories(base_dir=tmp_path)
    try:
        yield directories
    finally:
        directories.cleanup()


def test_one_batch_produces_one_utf8_json_lines_file(
    run_directories: RunDirectories,
) -> None:
    writer = AtomicBatchWriter(run_directories)

    published = writer.publish(batch({"city": "Z\u00fcrich", "id": 1}))

    assert published == run_directories.input_dir / "batch-00000000000000000000.json"
    assert published.read_bytes() == '{"city":"Z\u00fcrich","id":1}\n'.encode()
    assert list(run_directories.input_dir.iterdir()) == [published]


def test_batch_names_are_deterministic_and_monotonic(
    run_directories: RunDirectories,
) -> None:
    writer = AtomicBatchWriter(run_directories)

    first = writer.publish(batch({"id": 1}))
    second = writer.publish(batch({"id": 2}))

    assert first.name == "batch-00000000000000000000.json"
    assert second.name == "batch-00000000000000000001.json"
    assert sorted(path.name for path in run_directories.input_dir.iterdir()) == [
        first.name,
        second.name,
    ]


def test_final_path_appears_only_after_complete_content_is_written(
    run_directories: RunDirectories,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = AtomicBatchWriter(run_directories)
    expected = b'{"id":1}\n{"id":2}\n'
    observed_rename = False
    original_rename = Path.rename

    def inspect_then_rename(
        source: Path,
        destination: str | os.PathLike[str],
    ) -> Path:
        nonlocal observed_rename
        destination_path = Path(destination)
        assert source.read_bytes() == expected
        assert not destination_path.exists()
        observed_rename = True
        return original_rename(source, destination)

    monkeypatch.setattr(Path, "rename", inspect_then_rename)

    published = writer.publish(batch({"id": 1}, {"id": 2}))

    assert observed_rename
    assert published.read_bytes() == expected
    assert not any(path.name.startswith(".") for path in run_directories.input_dir.iterdir())


def test_existing_destination_collision_fails_without_overwriting(
    run_directories: RunDirectories,
) -> None:
    destination = run_directories.input_dir / "batch-00000000000000000000.json"
    destination.write_text("existing\n", encoding="utf-8", newline="\n")
    writer = AtomicBatchWriter(run_directories)

    with pytest.raises(FileExistsError, match="destination already exists"):
        writer.publish(batch({"id": 1}))

    assert destination.read_text(encoding="utf-8") == "existing\n"
    assert list(run_directories.input_dir.iterdir()) == [destination]


def test_existing_temporary_file_collision_fails_clearly(
    run_directories: RunDirectories,
) -> None:
    temporary = run_directories.input_dir / ".batch-00000000000000000000.json.tmp"
    temporary.write_text("existing\n", encoding="utf-8", newline="\n")
    writer = AtomicBatchWriter(run_directories)

    with pytest.raises(FileExistsError, match="Temporary batch input file already exists"):
        writer.publish(batch({"id": 1}))

    assert temporary.read_text(encoding="utf-8") == "existing\n"


def test_temporary_file_open_race_does_not_remove_competing_file(
    run_directories: RunDirectories,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temporary = run_directories.input_dir / ".batch-00000000000000000000.json.tmp"
    writer = AtomicBatchWriter(run_directories)
    original_open = Path.open

    def create_competing_file_then_fail(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> object:
        if path == temporary and mode == "x":
            with original_open(temporary, "w", encoding="utf-8", newline="\n") as handle:
                handle.write("competing\n")
            raise FileExistsError("injected exclusive-open collision")
        return original_open(
            path,
            mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    monkeypatch.setattr(Path, "open", create_competing_file_then_fail)

    with pytest.raises(FileExistsError, match="injected exclusive-open collision"):
        writer.publish(batch({"id": 1}))

    assert temporary.read_text(encoding="utf-8") == "competing\n"


def test_destination_appearing_before_rename_is_preserved(
    run_directories: RunDirectories,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = run_directories.input_dir / "batch-00000000000000000000.json"
    temporary = run_directories.input_dir / ".batch-00000000000000000000.json.tmp"
    writer = AtomicBatchWriter(run_directories)
    original_fsync = os.fsync

    def create_destination_after_sync(file_descriptor: int) -> None:
        original_fsync(file_descriptor)
        destination.write_text("competing\n", encoding="utf-8", newline="\n")

    monkeypatch.setattr(os, "fsync", create_destination_after_sync)

    with pytest.raises(FileExistsError, match="destination already exists"):
        writer.publish(batch({"id": 1}))

    assert destination.read_text(encoding="utf-8") == "competing\n"
    assert not temporary.exists()


def test_publish_failure_removes_temporary_file_and_does_not_advance_index(
    run_directories: RunDirectories,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = AtomicBatchWriter(run_directories)
    original_rename = Path.rename

    def fail_rename(
        source: Path,
        destination: str | os.PathLike[str],
    ) -> Path:
        raise OSError(f"injected rename failure: {source} -> {destination}")

    monkeypatch.setattr(Path, "rename", fail_rename)
    with pytest.raises(OSError, match="injected rename failure"):
        writer.publish(batch({"id": 1}))

    assert list(run_directories.input_dir.iterdir()) == []

    monkeypatch.setattr(Path, "rename", original_rename)
    published = writer.publish(batch({"id": 1}))

    assert published.name == "batch-00000000000000000000.json"


def test_encoding_failure_creates_no_input_file(run_directories: RunDirectories) -> None:
    writer = AtomicBatchWriter(run_directories)

    with pytest.raises(TypeError, match="row at index 0"):
        writer.publish(batch({"unsupported": object()}))

    assert list(run_directories.input_dir.iterdir()) == []
