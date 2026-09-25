"""Deterministic testing for Apache Spark Structured Streaming."""

from streamcase._version import __version__
from streamcase.actions import Batch, batch

__all__ = ["Batch", "__version__", "batch"]
