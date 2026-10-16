from __future__ import annotations

from importlib.metadata import distribution, metadata, version
from subprocess import run
from sys import executable

import streamcase


def test_package_exposes_version() -> None:
    assert streamcase.__version__ == version("streamcase")


def test_spark_extras_declare_distinct_supported_pyspark_lines() -> None:
    requirements = metadata("streamcase").get_all("Requires-Dist") or []
    pyspark_requirements = [
        requirement for requirement in requirements if requirement.lower().startswith("pyspark")
    ]

    assert len(pyspark_requirements) == 2
    requirements_by_extra: dict[str, str] = {}
    for requirement in pyspark_requirements:
        dependency, separator, marker = requirement.partition(";")
        assert separator == ";"
        normalized_marker = marker.strip().replace('"', "'")
        assert normalized_marker in {"extra == 'spark'", "extra == 'spark35'"}
        extra = normalized_marker.removeprefix("extra == '").removesuffix("'")
        requirements_by_extra[extra] = dependency.strip()

    assert requirements_by_extra == {
        "spark": "pyspark<4.3,>=4.2",
        "spark35": "pyspark<3.6,>=3.5.9",
    }


def test_pytest_extra_is_explicitly_opt_in() -> None:
    package_metadata = metadata("streamcase")
    requirements = package_metadata.get_all("Requires-Dist") or []

    assert "pytest" in (package_metadata.get_all("Provides-Extra") or [])
    assert "pytest<10,>=8.3; extra == 'pytest'" in requirements
    assert not any(entry.group == "pytest11" for entry in distribution("streamcase").entry_points)


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
