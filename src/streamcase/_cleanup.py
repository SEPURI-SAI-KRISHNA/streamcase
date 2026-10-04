"""Preserve execution failures while attempting runner-owned cleanup."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager


def _record_cleanup_failure(primary: BaseException, resource: str, error: BaseException) -> None:
    failures = primary.__dict__.get("_streamcase_cleanup_failures", ())
    primary.__dict__["_streamcase_cleanup_failures"] = (*failures, (resource, error))

    note = f"Streamcase cleanup failed for {resource}: {type(error).__name__}: {error}"
    add_note = getattr(primary, "add_note", None)
    if callable(add_note):
        add_note(note)
    else:
        primary.args = (*primary.args, note)


@contextmanager
def _cleanup_on_exit(resource: str, cleanup: Callable[[], None]) -> Iterator[None]:
    """Run cleanup and keep a primary failure when cleanup also fails."""
    try:
        yield
    except BaseException as primary:
        try:
            cleanup()
        except BaseException as error:
            _record_cleanup_failure(primary, resource, error)
        raise
    else:
        cleanup()
