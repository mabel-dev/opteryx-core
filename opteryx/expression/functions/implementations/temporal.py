"""
Temporal function kernels.

Thin re-export layer from C++ nanobind implementations.
All computation and dispatch logic lives in:
  - opteryx/compiled/nanobind/vector_temporal_arith.cpp
  - opteryx/compiled/nanobind/vector_temporal_convert.cpp

This module provides backward-compatible imports for the registrar.
"""

from opteryx.compiled.nanobind.vector_temporal_arith import (
    date_part,
    date_diff,
    date_format,
    time_diff,
    trunc_date,
    trunc_timestamp,
)
from opteryx.compiled.nanobind.vector_temporal_convert import (
    date_floor,
    unixtime,
    from_unixtimestamp,
)

__all__ = [
    "date_part",
    "date_diff",
    "date_format",
    "date_floor",
    "time_diff",
    "trunc_date",
    "trunc_timestamp",
    "unixtime",
    "from_unixtimestamp",
]
