# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Skene IO — schema glue between the filesystem connector and libskene.

Unlike parquet (_rugo_schema.py's lossy string-typed mapping) and JSONL
(sampled inference), a skene footer carries the exact DrakenType and
LogicalType descriptor each column was written with — the conversion here is
an identity reconstruction, not a translation. IPv4 stays IPV4, DECIMAL keeps
its precision/scale, TIMESTAMP keeps its unit.
"""

from typing import Any, Dict

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

from opteryx.types.logical_type import ColumnType
from opteryx.types.schema import ColumnDescriptor
from opteryx.types.schema import RelationDescriptor

__all__ = [
    "skene_column_type",
    "skene_metadata_to_schema",
]

# skene format.h StatFlag bits.
def resolve_skene_coalesce_tuning(variables) -> tuple:
    """``(waste_ratio, max_bytes)`` for a v3 skene scan's range coalescer —
    skene's own knobs (design R12), resolved default -> env -> SET.

    Session variables only: a skene reader honours no per-scan WITH(...)
    settings, and the planner refuses one rather than accept an inert knob.
    """
    from opteryx import config
    from opteryx.variables import resolve

    return (
        float(resolve("skene_io_coalesce_waste_ratio", variables,
                      config.SKENE_IO_COALESCE_WASTE_RATIO)),
        int(resolve("skene_io_coalesce_max_bytes", variables,
                    config.SKENE_IO_COALESCE_MAX_BYTES)),
    )


def skene_column_type(column: Dict[str, Any]) -> ColumnType:
    """Reconstruct a ColumnType from one skene footer column entry
    (skene.read_metadata()'s per-column dict — raw draken enum ints)."""
    physical = DrakenType(column["type"])
    logical_entry = column.get("logical")
    logical = None
    if logical_entry is not None:
        logical = LogicalType(
            kind=LogicalKind(logical_entry["kind"]),
            unit=TimestampUnit(logical_entry["unit"]),
            offset_minutes=logical_entry["offset_minutes"],
            precision=logical_entry["precision"],
            scale=logical_entry["scale"],
            dimension=logical_entry["dimension"],
        )
    element = None
    if physical == DrakenType.ARRAY:
        children = column.get("children") or []
        # A well-formed skene ARRAY column carries exactly one child (the
        # element); a childless one is malformed and read_morsel would reject
        # it, so failing here is early, not different.
        element = skene_column_type(children[0])
    return ColumnType(physical, logical, element)


def skene_metadata_to_schema(metadata: Dict[str, Any], schema_name: str) -> RelationDescriptor:
    """RelationDescriptor from skene.read_metadata() output. Exact, not inferred."""
    columns = [
        ColumnDescriptor(
            name=column["name"],
            column_type=skene_column_type(column),
        )
        for column in metadata["columns"]
    ]
    return RelationDescriptor(name=schema_name, columns=columns)
