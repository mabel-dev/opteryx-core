"""
Avro oracle: fastavro AND Apache's reference `avro` package both read the file; the
service refuses to answer if they disagree.

Both readers would otherwise turn logical types into Python objects (Decimal,
datetime, UUID) and lose information. To get raw physical values from BOTH readers
the file header is rewritten with `logicalType` removed from the schema — the data
blocks are passed through byte-for-byte — and the logical interpretation is applied
by this module's canonical encoder, driven by the ORIGINAL schema. The original file
is also read once by fastavro with its normal logical decoding, so a file whose
logical types are invalid is still rejected.
"""

import io
import json

import avro
import avro.datafile
import avro.io
import fastavro

from . import canonical
from .errors import ReaderDisagreement, Unsupported

READER = {"fastavro": fastavro.__version__, "avro": avro.__version__}

_MAGIC = b"Obj\x01"
_PRIMITIVES = ("null", "boolean", "int", "long", "float", "double", "bytes", "string")
_TIME_UNITS = {
    "time-millis": "ms",
    "time-micros": "us",
    "timestamp-millis": "ms",
    "timestamp-micros": "us",
    "timestamp-nanos": "ns",
    "local-timestamp-millis": "ms",
    "local-timestamp-micros": "us",
    "local-timestamp-nanos": "ns",
}


# ── header rewrite ──────────────────────────────────────────────────────────


def _long(buf: bytes, pos: int) -> tuple:
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise Unsupported("truncated Avro header")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return (result >> 1) ^ -(result & 1), pos
        shift += 7


def _put_long(n: int) -> bytes:
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def _parse_header(data: bytes) -> tuple:
    """(metadata {bytes: bytes}, sync marker, offset of the first data block)."""
    if data[:4] != _MAGIC:
        raise Unsupported("not an Avro object container file: bad magic %r" % data[:4])
    pos = 4
    meta = {}
    while True:
        count, pos = _long(data, pos)
        if count == 0:
            break
        if count < 0:
            count = -count
            _size, pos = _long(data, pos)
        for _ in range(count):
            klen, pos = _long(data, pos)
            key = data[pos : pos + klen]
            pos += klen
            vlen, pos = _long(data, pos)
            meta[key] = data[pos : pos + vlen]
            pos += vlen
    sync = data[pos : pos + 16]
    if len(sync) != 16:
        raise Unsupported("truncated Avro header: no sync marker")
    return meta, sync, pos + 16


# A parsed Avro schema node is a JSON string (name), array (union) or object. JSON is
# the one place the oracle must tell those apart; the table makes any other node type fail.
_SHAPES = {str: "name", list: "union", dict: "object"}


def _shape(schema) -> str:
    if type(schema) not in _SHAPES:
        raise Unsupported("schema node of JSON type %s is not valid Avro" % type(schema).__name__)
    return _SHAPES[type(schema)]


def _strip_logical(schema):
    shape = _shape(schema)
    if shape == "name":
        return schema
    if shape == "union":
        return [_strip_logical(s) for s in schema]
    out = {k: v for k, v in schema.items() if k != "logicalType"}
    for key in ("type", "items", "values"):
        if key in out:
            out[key] = _strip_logical(out[key])
    if "fields" in out:
        out["fields"] = [dict(f, type=_strip_logical(f["type"])) for f in out["fields"]]
    return out


def _raw_file(data: bytes, meta: dict, sync: bytes, body: int, original_schema) -> bytes:
    meta = dict(meta)
    meta[b"avro.schema"] = json.dumps(_strip_logical(original_schema)).encode("utf-8")
    out = bytearray(_MAGIC)
    out += _put_long(len(meta))
    for k, v in meta.items():
        out += _put_long(len(k)) + k + _put_long(len(v)) + v
    out += _put_long(0) + sync + data[body:]
    return bytes(out)


# ── schema walking ──────────────────────────────────────────────────────────


def _fullname(schema: dict, ns: str) -> tuple:
    name = schema["name"]
    if "." in name:
        return name, name.rsplit(".", 1)[0]
    space = schema["namespace"] if "namespace" in schema else ns
    return (space + "." + name if space else name), space


def _lookup(names: dict, ref: str, ns: str) -> tuple:
    """(definition, the namespace it was defined in) for a named-type reference."""
    if ns and ns + "." + ref in names:
        return names[ns + "." + ref]
    if ref in names:
        return names[ref]
    raise Unsupported("schema refers to unknown named type %r" % ref)


def _branches(schema: list) -> tuple:
    """(nullable, the single non-null branch). Multi-branch unions are refused, not guessed."""
    non_null = [b for b in schema if b != "null"]
    if len(non_null) != 1:
        raise Unsupported(
            "union %s has %d non-null branches; the oracle cannot tell which a value took"
            % (json.dumps(schema), len(non_null))
        )
    return len(non_null) != len(schema), non_null[0]


def _declare(schema, ns: str, names: dict) -> None:
    """Register every named type (depth-first, definition order) so references resolve."""
    shape = _shape(schema)
    if shape == "union":
        for s in schema:
            _declare(s, ns, names)
    elif shape == "object":
        t = schema["type"]
        if t in ("record", "error", "enum", "fixed"):
            full, space = _fullname(schema, ns)
            names[full] = (schema, ns)
            if t in ("record", "error"):
                for f in schema["fields"]:
                    _declare(f["type"], space, names)
        elif t == "array":
            _declare(schema["items"], ns, names)
        elif t == "map":
            _declare(schema["values"], ns, names)
        else:
            _declare(t, ns, names)


def describe(schema, ns: str, names: dict, seen: set) -> dict:
    shape = _shape(schema)
    if shape == "union":
        nullable, branch = _branches(schema)
        out = describe(branch, ns, names, seen)
        out["nullable"] = nullable
        return out
    if shape == "name":
        if schema in _PRIMITIVES:
            return {"kind": schema, "nullable": schema == "null"}
        target, defined_in = _lookup(names, schema, ns)
        full = _fullname(target, defined_in)[0]
        if full in seen:
            return {"kind": "ref", "name": full, "nullable": False}
        return describe(target, defined_in, names, seen)
    t = schema["type"]
    logical = schema["logicalType"] if "logicalType" in schema else None
    if t in ("record", "error"):
        full, space = _fullname(schema, ns)
        seen = seen | {full}
        return {
            "kind": "record",
            "name": full,
            "nullable": False,
            "fields": [
                {"name": f["name"], "type": describe(f["type"], space, names, seen)} for f in schema["fields"]
            ],
        }
    if t == "enum":
        return {"kind": "enum", "name": _fullname(schema, ns)[0], "symbols": schema["symbols"], "nullable": False}
    if t == "fixed":
        out = {"kind": "fixed", "name": _fullname(schema, ns)[0], "size": schema["size"], "nullable": False}
    elif t == "array":
        return {"kind": "array", "element": describe(schema["items"], ns, names, seen), "nullable": False}
    elif t == "map":
        return {"kind": "map", "values": describe(schema["values"], ns, names, seen), "nullable": False}
    elif t in _PRIMITIVES:
        out = {"kind": t, "nullable": t == "null"}
    else:
        return describe(t, ns, names, seen)
    if logical is not None:
        out["logical_type"] = logical
        if logical == "decimal":
            out["precision"] = schema["precision"]
            out["scale"] = schema["scale"] if "scale" in schema else 0
        if logical in _TIME_UNITS:
            out["unit"] = _TIME_UNITS[logical]
        if logical in ("timestamp-millis", "timestamp-micros", "timestamp-nanos"):
            out["timezone"] = "UTC"
        if logical in ("local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos"):
            out["timezone"] = None
    return out


# ── value encoding ──────────────────────────────────────────────────────────


def encode(schema, value, ns: str, names: dict):
    """Canonical encoding of one RAW (logical-stripped) Avro datum, driven by the original schema."""
    shape = _shape(schema)
    if shape == "union":
        if value is None:
            return None
        return encode(_branches(schema)[1], value, ns, names)
    if shape == "name":
        if schema == "null":
            return None
        if schema == "boolean" or schema == "int" or schema == "string":
            return value
        if schema == "long":
            return canonical.encode_i64(value)
        if schema == "float":
            return canonical.encode_float_value("f32", value)
        if schema == "double":
            return canonical.encode_float_value("f64", value)
        if schema == "bytes":
            return canonical.encode_binary(value)
        target, defined_in = _lookup(names, schema, ns)
        return encode(target, value, defined_in, names)
    t = schema["type"]
    logical = schema["logicalType"] if "logicalType" in schema else None
    if t in ("record", "error"):
        _full, space = _fullname(schema, ns)
        return {f["name"]: encode(f["type"], value[f["name"]], space, names) for f in schema["fields"]}
    if t == "enum":
        return value
    if t == "array":
        return [encode(schema["items"], v, ns, names) for v in value]
    if t == "map":
        return [[k, encode(schema["values"], v, ns, names)] for k, v in value.items()]
    if t in ("bytes", "fixed") and logical == "decimal":
        scale = schema["scale"] if "scale" in schema else 0
        return canonical.encode_decimal(int.from_bytes(value, "big", signed=True), scale)
    if t == "fixed":
        return canonical.encode_binary(value)
    if t == "int" and logical == "date":
        return "date32:%d" % value
    if t in ("int", "long") and logical in _TIME_UNITS:
        tag = "time" if logical.startswith("time-") else "ts"
        return canonical.encode_unit_int(tag, _TIME_UNITS[logical], value)
    return encode(t, value, ns, names)


# ── readers ─────────────────────────────────────────────────────────────────


def _fastavro_rows(data: bytes) -> tuple:
    reader = fastavro.reader(io.BytesIO(data))
    return reader.codec, dict(reader.metadata), list(reader)


def _apache_rows(data: bytes) -> tuple:
    reader = avro.datafile.DataFileReader(io.BytesIO(data), avro.io.DatumReader())
    rows = list(reader)
    return reader.codec, {k: v.decode("utf-8") for k, v in reader.meta.items()}, rows


def inspect(data: bytes) -> dict:
    meta, sync, body = _parse_header(data)
    if b"avro.schema" not in meta:
        raise Unsupported("Avro header has no avro.schema entry")
    schema_text = meta[b"avro.schema"].decode("utf-8")
    schema = json.loads(schema_text)
    names = {}
    _declare(schema, "", names)
    if _shape(schema) != "object" or schema["type"] not in ("record", "error"):
        raise Unsupported("top-level Avro schema is not a record; the oracle describes record files only")
    description = describe(schema, "", names, set())

    # validity pass: the original file through fastavro with normal logical decoding
    _codec0, _meta0, decoded = _fastavro_rows(data)

    raw = _raw_file(data, meta, sync, body, schema)
    fa_codec, fa_meta, fa_rows = _fastavro_rows(raw)
    ap_codec, ap_meta, ap_rows = _apache_rows(raw)

    fa = [encode(schema, r, "", names) for r in fa_rows]
    ap = [encode(schema, r, "", names) for r in ap_rows]
    if len(decoded) != len(fa_rows):
        raise ReaderDisagreement(
            "fastavro read %d records with logical decoding but %d without" % (len(decoded), len(fa_rows))
        )
    if fa_codec != ap_codec:
        raise ReaderDisagreement("codec: fastavro %r, avro %r" % (fa_codec, ap_codec))
    if fa_meta != ap_meta:
        raise ReaderDisagreement("header metadata: fastavro %r, avro %r" % (fa_meta, ap_meta))
    if len(fa) != len(ap):
        raise ReaderDisagreement("record count: fastavro %d, avro %d" % (len(fa), len(ap)))
    for i, (a, b) in enumerate(zip(fa, ap)):
        if a != b:
            raise ReaderDisagreement("record %d differs: fastavro %r, avro %r" % (i, a, b))

    user_meta = {k.decode("utf-8"): v.decode("utf-8") for k, v in meta.items() if not k.startswith(b"avro.")}
    return {
        "file": {
            "num_rows": len(fa),
            "codec": fa_codec,
            "schema_json": schema_text,
            "user_metadata": user_meta,
            "sync_marker": canonical.encode_binary(sync),
        },
        "schema": {"fields": description["fields"], "record": description["name"]},
        "columns": [
            {"name": f["name"], "values": [row[f["name"]] for row in fa]} for f in schema["fields"]
        ],
    }
