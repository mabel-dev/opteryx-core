"""
Arrow → canonical JSON, shared by the Parquet and ORC oracles (both read via PyArrow).

Dispatch is on the Arrow type id, never on a Python value's type. Child buffers are
indexed through offsets/validity directly so sliced and chunked arrays are exact.
"""

import pyarrow as pa

from . import canonical
from .errors import Unsupported

_L = pa.lib

_PLAIN_INT = (_L.Type_INT8, _L.Type_INT16, _L.Type_INT32, _L.Type_UINT8, _L.Type_UINT16, _L.Type_UINT32)
_TEXT = (_L.Type_STRING, _L.Type_LARGE_STRING, _L.Type_STRING_VIEW)
_BYTES = (_L.Type_BINARY, _L.Type_LARGE_BINARY, _L.Type_BINARY_VIEW, _L.Type_FIXED_SIZE_BINARY)
_DECIMALS = (_L.Type_DECIMAL32, _L.Type_DECIMAL64, _L.Type_DECIMAL128, _L.Type_DECIMAL256)
_LISTS = (_L.Type_LIST, _L.Type_LARGE_LIST)

# float type id -> (canonical float kind, same-width unsigned view type)
_FLOAT_VIEWS = {
    _L.Type_HALF_FLOAT: ("f16", pa.uint16()),
    _L.Type_FLOAT: ("f32", pa.uint32()),
    _L.Type_DOUBLE: ("f64", pa.uint64()),
}

_KIND = {
    _L.Type_NA: "null",
    _L.Type_BOOL: "bool",
    _L.Type_INT8: "int8",
    _L.Type_INT16: "int16",
    _L.Type_INT32: "int32",
    _L.Type_INT64: "int64",
    _L.Type_UINT8: "uint8",
    _L.Type_UINT16: "uint16",
    _L.Type_UINT32: "uint32",
    _L.Type_UINT64: "uint64",
    _L.Type_HALF_FLOAT: "float16",
    _L.Type_FLOAT: "float32",
    _L.Type_DOUBLE: "float64",
    _L.Type_STRING: "string",
    _L.Type_LARGE_STRING: "large_string",
    _L.Type_STRING_VIEW: "string_view",
    _L.Type_BINARY: "binary",
    _L.Type_LARGE_BINARY: "large_binary",
    _L.Type_BINARY_VIEW: "binary_view",
    _L.Type_FIXED_SIZE_BINARY: "fixed_size_binary",
    _L.Type_DECIMAL32: "decimal32",
    _L.Type_DECIMAL64: "decimal64",
    _L.Type_DECIMAL128: "decimal128",
    _L.Type_DECIMAL256: "decimal256",
    _L.Type_DATE32: "date32",
    _L.Type_DATE64: "date64",
    _L.Type_TIME32: "time32",
    _L.Type_TIME64: "time64",
    _L.Type_TIMESTAMP: "timestamp",
    _L.Type_DURATION: "duration",
    _L.Type_LIST: "list",
    _L.Type_LARGE_LIST: "large_list",
    _L.Type_FIXED_SIZE_LIST: "fixed_size_list",
    _L.Type_MAP: "map",
    _L.Type_STRUCT: "struct",
    _L.Type_DICTIONARY: "dictionary",
}


def _is_extension(t: "pa.DataType") -> bool:
    return t.id not in _KIND and str(t).startswith("extension<")


def describe_field(field: "pa.Field") -> dict:
    return {"name": field.name, "nullable": field.nullable, "type": describe_type(field.type)}


def describe_type(t: "pa.DataType") -> dict:
    """Physical AND logical description of an Arrow type, recursively."""
    if _is_extension(t):
        out = describe_type(t.storage_type)
        out["extension"] = t.extension_name
        out["arrow"] = str(t)
        return out
    if t.id not in _KIND:
        raise Unsupported("arrow type %s is not described by the oracle" % t)
    out = {"arrow": str(t), "kind": _KIND[t.id]}
    if t.id == _L.Type_TIMESTAMP:
        out["unit"] = t.unit
        out["timezone"] = t.tz
    elif t.id in (_L.Type_TIME32, _L.Type_TIME64, _L.Type_DURATION):
        out["unit"] = t.unit
    elif t.id in _DECIMALS:
        out["precision"] = t.precision
        out["scale"] = t.scale
        out["bit_width"] = t.bit_width
    elif t.id == _L.Type_FIXED_SIZE_BINARY:
        out["byte_width"] = t.byte_width
    elif t.id in _LISTS:
        out["element"] = describe_field(t.value_field)
    elif t.id == _L.Type_FIXED_SIZE_LIST:
        out["element"] = describe_field(t.value_field)
        out["list_size"] = t.list_size
    elif t.id == _L.Type_MAP:
        out["key"] = describe_field(t.key_field)
        out["item"] = describe_field(t.item_field)
        out["keys_sorted"] = t.keys_sorted
    elif t.id == _L.Type_STRUCT:
        out["fields"] = [describe_field(t.field(i)) for i in range(t.num_fields)]
    elif t.id == _L.Type_DICTIONARY:
        out["index_type"] = str(t.index_type)
        out["value_type"] = describe_type(t.value_type)
        out["ordered"] = t.ordered
    return out


def describe_schema(schema: "pa.Schema") -> list:
    return [describe_field(schema.field(i)) for i in range(len(schema))]


def _masked(valid: list, values: list) -> list:
    return [v if ok else None for ok, v in zip(valid, values)]


def encode_array(arr: "pa.Array") -> list:
    """Column-wise canonical values of one Arrow array (nulls as None)."""
    t = arr.type
    n = len(arr)
    if _is_extension(t):
        return encode_array(arr.storage)
    if t.id not in _KIND:
        raise Unsupported("arrow type %s is not encoded by the oracle" % t)
    if t.id == _L.Type_NA:
        return [None] * n
    if t.id == _L.Type_DICTIONARY:
        return encode_array(arr.dictionary_decode())
    if t.id == _L.Type_BOOL or t.id in _PLAIN_INT or t.id in _TEXT:
        return arr.to_pylist()
    if t.id == _L.Type_INT64:
        return [None if v is None else canonical.encode_i64(v) for v in arr.to_pylist()]
    if t.id == _L.Type_UINT64:
        return [None if v is None else canonical.encode_u64(v) for v in arr.to_pylist()]
    if t.id in _FLOAT_VIEWS:
        kind, view_type = _FLOAT_VIEWS[t.id]
        return [None if b is None else canonical.encode_float_bits(kind, b) for b in arr.view(view_type).to_pylist()]
    if t.id in _BYTES:
        return [None if v is None else canonical.encode_binary(v) for v in arr.to_pylist()]
    if t.id in _DECIMALS:
        return _encode_decimals(arr)
    if t.id == _L.Type_DATE32:
        return [None if v is None else "date32:%d" % v for v in arr.view(pa.int32()).to_pylist()]
    if t.id == _L.Type_DATE64:
        return [None if v is None else "date64:%d" % v for v in arr.view(pa.int64()).to_pylist()]
    if t.id == _L.Type_TIME32:
        return [None if v is None else canonical.encode_unit_int("time", t.unit, v) for v in arr.view(pa.int32()).to_pylist()]
    if t.id == _L.Type_TIME64:
        return [None if v is None else canonical.encode_unit_int("time", t.unit, v) for v in arr.view(pa.int64()).to_pylist()]
    if t.id == _L.Type_TIMESTAMP:
        return [None if v is None else canonical.encode_unit_int("ts", t.unit, v) for v in arr.view(pa.int64()).to_pylist()]
    if t.id == _L.Type_DURATION:
        return [None if v is None else canonical.encode_unit_int("dur", t.unit, v) for v in arr.view(pa.int64()).to_pylist()]

    valid = arr.is_valid().to_pylist()
    if t.id in _LISTS:
        child = encode_array(arr.values)
        offsets = arr.offsets.to_pylist()
        return _masked(valid, [child[offsets[i] : offsets[i + 1]] for i in range(n)])
    if t.id == _L.Type_FIXED_SIZE_LIST:
        child = encode_array(arr.values)
        size = t.list_size
        base = arr.offset * size
        return _masked(valid, [child[base + i * size : base + (i + 1) * size] for i in range(n)])
    if t.id == _L.Type_MAP:
        keys = encode_array(arr.keys)
        items = encode_array(arr.items)
        offsets = arr.offsets.to_pylist()
        return _masked(
            valid,
            [[[keys[j], items[j]] for j in range(offsets[i], offsets[i + 1])] for i in range(n)],
        )
    if t.id == _L.Type_STRUCT:
        names = [t.field(i).name for i in range(t.num_fields)]
        if len(set(names)) != len(names):
            raise Unsupported("struct with duplicate field names %r cannot be keyed canonically" % names)
        children = [encode_array(arr.field(i)) for i in range(t.num_fields)]
        return _masked(valid, [{name: children[c][i] for c, name in enumerate(names)} for i in range(n)])
    raise Unsupported("arrow type %s is not encoded by the oracle" % t)


def _encode_decimals(arr: "pa.Array") -> list:
    t = arr.type
    if len(arr) == 0:
        return []
    width = t.bit_width // 8
    raw = arr.buffers()[1].to_pybytes()
    valid = arr.is_valid().to_pylist()
    out = []
    for i, ok in enumerate(valid):
        if not ok:
            out.append(None)
            continue
        start = (arr.offset + i) * width
        unscaled = int.from_bytes(raw[start : start + width], "little", signed=True)
        out.append(canonical.encode_decimal(unscaled, t.scale))
    return out


def encode_table_columns(table: "pa.Table") -> list:
    """All rows, column-wise: [{"name": ..., "values": [...]}, ...] (names may repeat)."""
    columns = []
    for i in range(table.num_columns):
        values = []
        for chunk in table.column(i).chunks:
            values.extend(encode_array(chunk))
        columns.append({"name": table.schema.field(i).name, "values": values})
    return columns
