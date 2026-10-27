"""Parquet oracle: PyArrow (pyarrow.parquet) is the independent reader."""

import io
import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq

from . import arrow_codec, canonical, thrift_pages
from .errors import Unsupported

READER = {"pyarrow": pa.__version__}


def _stat_raw(physical_type: str, raw) -> object:
    """Physical-type canonical form of a statistics min/max (PyArrow hands back the stored value)."""
    if raw is None:
        return None
    if physical_type == "BOOLEAN":
        return raw
    if physical_type == "INT32":
        return raw
    if physical_type == "INT64":
        return canonical.encode_i64(raw)
    if physical_type == "FLOAT":
        return canonical.encode_float_value("f32", raw)
    if physical_type == "DOUBLE":
        return canonical.encode_float_value("f64", raw)
    if physical_type in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", "INT96"):
        return canonical.encode_binary(raw)
    raise Unsupported("statistics for physical type %s are not encoded by the oracle" % physical_type)


def _stat_logical(leaf: dict, physical_type: str, raw) -> object:
    """Logical-type canonical form of a statistics min/max; None where the physical form is already the logical one."""
    if raw is None:
        return None
    kind = leaf["logical_type"]["Type"]
    if kind == "Decimal":
        scale = leaf["logical_type"]["scale"]
        if physical_type in ("INT32", "INT64"):
            return canonical.encode_decimal(raw, scale)
        return canonical.encode_decimal(int.from_bytes(raw, "big", signed=True), scale)
    if kind == "Int" and not leaf["logical_type"]["isSigned"]:
        width = leaf["logical_type"]["bitWidth"]
        value = raw & ((1 << width) - 1)
        return canonical.encode_u64(value) if width == 64 else value
    if kind == "Int":
        return canonical.encode_i64(raw) if leaf["logical_type"]["bitWidth"] == 64 else raw
    if kind == "Timestamp":
        return canonical.encode_unit_int("ts", _UNITS[leaf["logical_type"]["timeUnit"]], raw)
    if kind == "Time":
        return canonical.encode_unit_int("time", _UNITS[leaf["logical_type"]["timeUnit"]], raw)
    if kind == "Date":
        return "date32:%d" % raw
    if kind in ("String", "Enum", "Json"):
        return raw.decode("utf-8")
    return None


_UNITS = {"milliseconds": "ms", "microseconds": "us", "nanoseconds": "ns"}


def _leaf(column: "pq.ParquetSchema") -> dict:
    return {
        "path": column.path,
        "physical_type": column.physical_type,
        "logical_type": json.loads(column.logical_type.to_json()),
        "converted_type": column.converted_type,
        "type_length": column.length,
        "max_definition_level": column.max_definition_level,
        "max_repetition_level": column.max_repetition_level,
    }


def _statistics(leaf: dict, chunk: "pq.ColumnChunkMetaData") -> "dict | None":
    if not chunk.is_stats_set:
        return None
    st = chunk.statistics
    physical = st.physical_type
    out = {
        "has_min_max": st.has_min_max,
        "null_count": st.null_count if st.has_null_count else None,
        "distinct_count": st.distinct_count if st.has_distinct_count else None,
        "num_values": st.num_values,
        "min_raw": None,
        "max_raw": None,
        "min": None,
        "max": None,
    }
    if st.has_min_max:
        out["min_raw"] = _stat_raw(physical, st.min_raw)
        out["max_raw"] = _stat_raw(physical, st.max_raw)
        lo = _stat_logical(leaf, physical, st.min_raw)
        hi = _stat_logical(leaf, physical, st.max_raw)
        out["min"] = out["min_raw"] if lo is None else lo
        out["max"] = out["max_raw"] if hi is None else hi
    return out


def _column_chunk(data: bytes, leaf: dict, chunk: "pq.ColumnChunkMetaData") -> dict:
    # the dictionary page, when there is one, is the chunk's first page
    start = chunk.dictionary_page_offset if chunk.has_dictionary_page else chunk.data_page_offset
    pages = thrift_pages.column_chunk_pages(data, start, chunk.total_compressed_size)
    return {
        "path": chunk.path_in_schema,
        "physical_type": chunk.physical_type,
        "compression": chunk.compression,
        "encodings": sorted(chunk.encodings),
        "data_page_versions": thrift_pages.data_page_versions(pages),
        "pages": pages,
        "num_values": chunk.num_values,
        "total_compressed_size": chunk.total_compressed_size,
        "total_uncompressed_size": chunk.total_uncompressed_size,
        "has_dictionary_page": chunk.has_dictionary_page,
        "has_column_index": chunk.has_column_index,
        "has_offset_index": chunk.has_offset_index,
        "bloom_filter_offset": chunk.bloom_filter_offset,
        "bloom_filter_length": chunk.bloom_filter_length,
        "statistics": _statistics(leaf, chunk),
    }


def inspect(data: bytes) -> dict:
    pf = pq.ParquetFile(io.BytesIO(data))
    md = pf.metadata
    leaves = [_leaf(md.schema.column(i)) for i in range(md.num_columns)]
    row_groups = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        row_groups.append(
            {
                "index": g,
                "num_rows": rg.num_rows,
                "total_byte_size": rg.total_byte_size,
                "columns": [_column_chunk(data, leaves[c], rg.column(c)) for c in range(rg.num_columns)],
            }
        )
    table = pf.read()
    return {
        "file": {
            "num_rows": md.num_rows,
            "num_row_groups": md.num_row_groups,
            "num_columns": md.num_columns,
            "created_by": md.created_by,
            "format_version": md.format_version,
            "serialized_footer_size": md.serialized_size,
            "key_value_metadata": _key_values(md.metadata),
            "parquet_schema": repr(md.schema).split("\n", 1)[1],
        },
        "schema": {"fields": arrow_codec.describe_schema(table.schema), "parquet_leaves": leaves},
        "row_groups": row_groups,
        "columns": arrow_codec.encode_table_columns(table),
    }


def _key_values(metadata: dict) -> dict:
    if metadata is None:
        return {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}
