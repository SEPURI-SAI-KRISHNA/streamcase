from __future__ import annotations

from collections.abc import Callable

import pytest

from streamcase._cleanup import _cleanup_on_exit


def test_cleanup_failure_after_success_is_surfaced() -> None:
    calls: list[str] = []

    def cleanup() -> None:
        calls.append("cleanup")
        raise OSError("cleanup failed")

    with pytest.raises(OSError, match="cleanup failed"), _cleanup_on_exit("run directory", cleanup):
        calls.append("work")

    assert calls == ["work", "cleanup"]


def test_primary_failure_survives_successful_cleanup() -> None:
    calls: list[str] = []

    with (
        pytest.raises(AssertionError, match="work failed"),
        _cleanup_on_exit("run directory", lambda: calls.append("cleanup")),
    ):
        raise AssertionError("work failed")

    assert calls == ["cleanup"]


def test_two_cleanup_failures_remain_attached_to_the_primary_exception() -> None:
    query_error = OSError("query stop failed")
    directory_error = OSError("directory cleanup failed")

    def fail_with(error: BaseException) -> Callable[[], None]:
        def cleanup() -> None:
            raise error

        return cleanup

    def fail_work() -> None:
        with (
            _cleanup_on_exit("run directory", fail_with(directory_error)),
            _cleanup_on_exit("streaming query", fail_with(query_error)),
        ):
            raise AssertionError("work failed")

    with pytest.raises(AssertionError, match="work failed") as error_info:
        fail_work()

    primary = error_info.value
    assert primary.__dict__["_streamcase_cleanup_failures"] == (
        ("streaming query", query_error),
        ("run directory", directory_error),
    )
    details = " ".join((*getattr(primary, "__notes__", ()), str(primary)))
    assert "query stop failed" in details
    assert "directory cleanup failed" in details


def test_python_310_fallback_keeps_cleanup_context_in_primary_message() -> None:
    primary = AssertionError("work failed")
    primary.__dict__["add_note"] = None
    cleanup_error = OSError("cleanup failed")

    def cleanup() -> None:
        raise cleanup_error

    with pytest.raises(AssertionError), _cleanup_on_exit("run directory", cleanup):
        raise primary

    assert "work failed" in str(primary)
    assert "cleanup failed" in str(primary)
    assert primary.__dict__["_streamcase_cleanup_failures"] == (("run directory", cleanup_error),)
