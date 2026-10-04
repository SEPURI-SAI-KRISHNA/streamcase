"""Validate the narrow, runner-safe Spark streaming writer configuration."""

from __future__ import annotations

from collections.abc import Mapping

_OUTPUT_MODES = frozenset({"append", "complete", "update"})
_RESERVED_QUERY_OPTIONS = frozenset(
    {
        "availablenow",
        "checkpointlocation",
        "clusterby",
        "continuous",
        "foreach",
        "foreachbatch",
        "format",
        "once",
        "outputmode",
        "partitionby",
        "path",
        "paths",
        "processingtime",
        "queryname",
        "sink",
        "table",
        "totable",
        "trigger",
    }
)


def _prepare_query_configuration(
    output_mode: str,
    query_options: Mapping[str, str] | None,
) -> tuple[str, dict[str, str]]:
    """Return a validated mode and detached copy of caller writer options."""
    if not isinstance(output_mode, str):
        raise TypeError("Spark output mode must be a string.")
    if output_mode not in _OUTPUT_MODES:
        raise ValueError("Spark output mode must be 'append', 'complete', or 'update'.")

    if query_options is None:
        return output_mode, {}
    if not isinstance(query_options, Mapping):
        raise TypeError("Spark query options must be a mapping of strings to strings.")

    copied = dict(query_options)
    seen: set[str] = set()
    conflicts: list[str] = []
    for name, value in copied.items():
        if not isinstance(name, str):
            raise TypeError("Spark query option names must be strings.")
        if not isinstance(value, str):
            raise TypeError(f"Spark query option {name!r} must have a string value.")

        normalized = name.casefold()
        if normalized in seen:
            raise ValueError(f"Spark query option {name!r} duplicates a case-insensitive name.")
        seen.add(normalized)
        if normalized in _RESERVED_QUERY_OPTIONS:
            conflicts.append(name)

    if conflicts:
        rendered = ", ".join(repr(name) for name in sorted(conflicts, key=str.casefold))
        raise ValueError(f"Runner-owned query options cannot be overridden: {rendered}.")

    return output_mode, copied
