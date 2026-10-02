"""Private construction of the runner-owned Spark JSON input source."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from streamcase._directories import RunDirectories

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


_RESERVED_SOURCE_OPTIONS = frozenset(
    {
        "badrecordspath",
        "basepath",
        "cleansource",
        "columnnameofcorruptrecord",
        "discardcachedinputratio",
        "encoding",
        "filenameonly",
        "ignorecorruptfiles",
        "ignoremissingfiles",
        "inferschema",
        "latestfirst",
        "linesep",
        "maxbytespertrigger",
        "maxcachedfiles",
        "maxfileage",
        "maxfilespertrigger",
        "mode",
        "modifiedafter",
        "modifiedbefore",
        "multiline",
        "path",
        "pathglobfilter",
        "paths",
        "recursivefilelookup",
        "samplingratio",
        "schema",
        "sourcearchivedir",
        "wholefile",
    }
)


def _copy_source_options(source_options: Mapping[str, str] | None) -> dict[str, str]:
    copied = {} if source_options is None else dict(source_options)
    conflicts: list[str] = []

    for name, value in copied.items():
        if not isinstance(name, str):
            raise TypeError("Source option names must be strings.")
        if not isinstance(value, str):
            raise TypeError(f"Source option {name!r} must have a string value.")
        if name.casefold() in _RESERVED_SOURCE_OPTIONS:
            conflicts.append(name)

    if conflicts:
        rendered = ", ".join(repr(name) for name in sorted(conflicts, key=str.casefold))
        raise ValueError(f"Runner-owned source options cannot be overridden: {rendered}.")

    return copied


def _build_json_stream(
    spark: SparkSession,
    directories: RunDirectories,
    *,
    schema: StructType | str,
    source_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Build a deterministic streaming JSON source without starting a query."""
    if schema is None:
        raise ValueError("An explicit Spark input schema is required.")
    if isinstance(schema, str) and not schema.strip():
        raise ValueError("The Spark input schema must not be empty.")

    approved_options = _copy_source_options(source_options)
    reader = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .option("multiLine", "false")
        .option("mode", "FAILFAST")
        .options(**approved_options)
    )
    stream: DataFrame = reader.json(str(directories.input_dir))
    return stream
