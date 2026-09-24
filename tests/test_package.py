from __future__ import annotations

import streamcase


def test_package_exposes_version() -> None:
    assert streamcase.__version__ == "0.0.0"
