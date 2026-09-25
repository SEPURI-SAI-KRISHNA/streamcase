from __future__ import annotations

from importlib.metadata import version

import streamcase


def test_package_exposes_version() -> None:
    assert streamcase.__version__ == version("streamcase")
