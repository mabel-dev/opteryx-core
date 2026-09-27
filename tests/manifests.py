"""The ONE way a test hand-builds a Manifest (architect ruling 2026-09-27 (5b)).

A test describes each file as a `FileSpec` - the per-file facts a producer
would record - and `build_manifest` feeds them to the native builder exactly as
an in-process producer does. Every per-column map is keyed by the column's
LOAD-TIME POSITION in `schema` (the manifest's one key space).

    build_manifest(schema, [FileSpec("a.parquet", record_count=10,
                                     lower_bounds={0: 1}, upper_bounds={0: 9})])

Bounds are in the manifest's dialect: ordinal int64 keys when
`bounds_are_ordinal`, else real values, dispatched on their Python type
(bool, int, float, str, bytes, Decimal) as a decoding producer records them.
"""

import decimal
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import opteryx.types  # noqa: F401  (enter through opteryx.types: column_type <-> opteryx.types cycle)
from opteryx.compiled.planner.native_manifest import NativeManifestBuilder
from opteryx.models.manifest import Manifest

NULL_FLAG = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


@dataclass
class FileSpec:
    file_path: str
    record_count: Optional[int] = 0
    file_size_in_bytes: int = 0
    file_format: str = "PARQUET"
    uncompressed_size_in_bytes: Optional[int] = None
    row_group_count: Optional[int] = None
    histogram_bins: Optional[int] = None
    deleted_record_count: int = 0
    delete_file_path: Optional[str] = None
    delete_positions: Optional[Tuple[int, ...]] = None
    # per column, keyed by load-time position
    lower_bounds: Dict[int, Any] = field(default_factory=dict)
    upper_bounds: Dict[int, Any] = field(default_factory=dict)
    null_value_counts: Dict[int, int] = field(default_factory=dict)
    min_length_bounds: Dict[int, int] = field(default_factory=dict)
    max_length_bounds: Dict[int, int] = field(default_factory=dict)
    char_total_bytes: Dict[int, int] = field(default_factory=dict)
    column_uncompressed_sizes: Dict[int, int] = field(default_factory=dict)
    distinct_value_counts: Dict[int, Tuple[int, bool]] = field(default_factory=dict)
    distinct_floors: Dict[int, int] = field(default_factory=dict)
    distinct_sketches: Dict[int, List[int]] = field(default_factory=dict)
    distinct_sketch_family: int = 0
    # a parquet footer's statistics (FileColumnStats), as the footer reader gives them
    column_stats: Any = None


def _set_bound(builder, row, position, is_min, value, bounds_are_ordinal):
    kind = type(value)
    if bounds_are_ordinal:
        if kind is not int:
            raise TypeError(f"an ordinal-dialect bound must be an int, not {kind.__name__}")
        if value != NULL_FLAG:
            builder.set_ordinal_bound(row, position, is_min, value)
    elif kind is bool:
        builder.set_bool_bound(row, position, is_min, value)
    elif kind is int and value > _INT64_MAX:
        builder.set_uint_bound(row, position, is_min, value)
    elif kind is int:
        builder.set_int_bound(row, position, is_min, value)
    elif kind is float:
        builder.set_double_bound(row, position, is_min, value)
    elif kind is str:
        builder.set_text_bound(row, position, is_min, value)
    elif kind is bytes:
        builder.set_bytes_bound(row, position, is_min, value)
    elif kind is decimal.Decimal:
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
        builder.set_decimal_bound(row, position, is_min, unscaled, -exponent, float(value))
    else:
        raise TypeError(f"a decoded bound of type {kind.__name__} has no manifest representation")


def _unknown(value):
    return -1 if value is None else value


def build_manifest(
    schema,
    files: List[FileSpec],
    *,
    bounds_are_ordinal: bool = False,
    stats_are_authoritative: bool = True,
    sketches: Optional[dict] = None,
) -> Manifest:
    """The Manifest over `schema` holding `files`, in order. `sketches` is the
    whole-column sketch vectors (min_k_hashes / histogram_counts /
    char_class_counts), one outer row per file in file order."""
    columns = schema.columns
    width = len(columns)
    builder = NativeManifestBuilder(
        tuple(column.name for column in columns),
        tuple(column.column_type.physical for column in columns),
        bounds_are_ordinal,
        stats_are_authoritative,
    )
    for vector_row, spec in enumerate(files):
        row = builder.add_file(
            spec.file_path,
            spec.file_format,
            _unknown(spec.record_count),
            spec.file_size_in_bytes,
            _unknown(spec.row_group_count),
            _unknown(spec.uncompressed_size_in_bytes),
            spec.histogram_bins or -1,
            spec.deleted_record_count,
            spec.delete_file_path,
            None if spec.delete_positions is None else tuple(spec.delete_positions),
            vector_row,
        )
        if spec.column_stats is not None:
            builder.set_footer(row, spec.column_stats)
        for bounds, is_min in ((spec.lower_bounds, True), (spec.upper_bounds, False)):
            for position, value in bounds.items():
                if position >= width:
                    raise IndexError(f"bound for column {position} of a {width}-column schema")
                if value is not None:
                    _set_bound(builder, row, position, is_min, value, bounds_are_ordinal)
        for position in range(width):
            builder.set_counts(
                row,
                position,
                _unknown(spec.null_value_counts.get(position)),
                _unknown(spec.min_length_bounds.get(position)),
                _unknown(spec.max_length_bounds.get(position)),
                _unknown(spec.char_total_bytes.get(position)),
                _unknown(spec.column_uncompressed_sizes.get(position)),
                _unknown(spec.distinct_floors.get(position)),
            )
        for position, (count, exact) in spec.distinct_value_counts.items():
            builder.set_distinct_count(row, position, count, exact)
        for position, hashes in spec.distinct_sketches.items():
            builder.set_distinct_sketch(row, position, list(hashes), spec.distinct_sketch_family)
    return Manifest(builder.build(dict(sketches or {})), schema)
