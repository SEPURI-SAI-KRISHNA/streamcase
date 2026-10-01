from __future__ import annotations

from importlib.metadata import metadata, version
from subprocess import run
from sys import executable

import streamcase


def test_package_exposes_version() -> None:
    assert streamcase.__version__ == version("streamcase")


def test_spark_extra_declares_the_supported_pyspark_line() -> None:
    requirements = metadata("streamcase").get_all("Requires-Dist") or []
    pyspark_requirements = [
        requirement for requirement in requirements if requirement.lower().startswith("pyspark")
    ]

    assert len(pyspark_requirements) == 1
    dependency, separator, marker = pyspark_requirements[0].partition(";")
    assert dependency.strip() == "pyspark<4.3,>=4.2"
    assert separator == ";"
    assert marker.strip() in {"extra == 'spark'", 'extra == "spark"'}


def test_core_import_does_not_import_pyspark() -> None:
    script = (
        "import sys; import streamcase; "
        "assert not any(n == 'pyspark' or n.startswith('pyspark.') for n in sys.modules)"
    )
    completed = run(
        [executable, "-c", script],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
