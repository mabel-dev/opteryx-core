"""STRUCT / MAP parquet columns are read NATIVELY and rendered as JSON text.

A parquet STRUCT or MAP is stored as several leaf chunks with repetition/definition
levels. The native scan expands a projected group column into its leaves, decodes
them, and folds them back into one NVARCHAR column of JSON documents
(rugo/src/parquet/nested_json.hpp). Until that existed the trampoline silently
NULL-filled the column.

Rulings under test (architect):
  * member order = schema order; a NULL field is JSON `null`; a NULL top-level group
    is SQL NULL, a NULL group nested inside another is `null`
  * MAP = JSON object, string keys only (a non-string key type is refused, loudly)
  * NaN / +-Inf = `null`; DECIMAL = a bare number; DATE/TIMESTAMP/TIME = quoted ISO;
    BINARY = quoted base64; nesting is arbitrary
  * a nested column the native scan cannot take is an ERROR, never NULLs

The oracle is a Python renderer of the same rules over pyarrow's own values. Numbers
are compared through `Decimal` so ryu's exponent spelling never matters.
"""

import base64
import datetime
import decimal
import json
import math
import os
import random
import sys
from decimal import Decimal

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.connectors.parquet_io import pool_reader

NATIVE = {"NativeParquetScanSource", "LatmatScanSource"}


# ── oracle ──────────────────────────────────────────────────────────────────

def _ts_text(dt):
    text = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        text += ".%06d" % dt.microsecond
    return text + "+00:00"


def tree(value, typ):
    """The Python structure the JSON text must parse to (numbers as Decimal)."""
    if value is None:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: tree(value[typ.field(i).name], typ.field(i).type)
                for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        return {k: tree(v, typ.item_type) for k, v in value}
    if pa.types.is_list(typ):
        return [tree(v, typ.value_type) for v in value]
    if pa.types.is_boolean(typ):
        return bool(value)
    if pa.types.is_integer(typ):
        return Decimal(int(value))
    if pa.types.is_floating(typ):
        return None if math.isnan(value) or math.isinf(value) else Decimal(repr(float(value)))
    if pa.types.is_decimal(typ):
        return Decimal(value)
    if pa.types.is_string(typ):
        return value
    if pa.types.is_binary(typ):
        return base64.b64encode(value).decode()
    if pa.types.is_date32(typ):
        return value.isoformat()
    if pa.types.is_timestamp(typ):
        return _ts_text(value)
    if pa.types.is_time(typ):
        text = value.strftime("%H:%M:%S")
        return text + (".%06d" % value.microsecond if value.microsecond else "")
    raise AssertionError(f"oracle has no rule for {typ}")


def parse(text):
    return json.loads(text, parse_float=Decimal, parse_int=Decimal)


# ── harness ─────────────────────────────────────────────────────────────────

def run(directory, sql_tail="", columns="id, s", force_trampoline=False, monkeypatch=None,
        ordered=False):
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    cols = {}
    for morsel in session.execute_to_morsels(f"SELECT {columns} FROM '{directory}' {sql_tail}"):
        for n in morsel.column_names:
            cols.setdefault(n.decode(), []).extend(morsel.column(n).to_pylist())
    sources = set(session.telemetry["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    # An unordered scan has no row order to assert; line the rows up by id unless the
    # query itself orders them.
    if not ordered and "id" in cols:
        order = sorted(range(len(cols["id"])), key=cols["id"].__getitem__)
        cols = {k: [v[i] for i in order] for k, v in cols.items()}
    return cols, sources


def write(tmp_path, table, **kw):
    d = tmp_path / "ds"
    d.mkdir()
    pq.write_table(table, d / "part.parquet", **kw)
    return d


def check_column(cols, table, name="s"):
    typ = table.schema.field(name).type
    expected = [tree(v, typ) for v in table.column(name).to_pylist()]
    got = [None if t is None else parse(t) for t in cols[name]]
    assert got == expected


# ── scalars ─────────────────────────────────────────────────────────────────

def test_struct_scalar_leaves_exact_text(tmp_path):
    t = pa.table({
        "id": pa.array([1, 2, 3], pa.int64()),
        "s": pa.array(
            [
                {"i8": -8, "u8": 200, "i64": -(2**62), "u64": 2**64 - 1, "f": 1.5, "b": True,
                 "s": 'q"\\\né€', "bin": b"\x00\xffab", "d": datetime.date(2024, 2, 29),
                 "dec": decimal.Decimal("-12345.67")},
                None,
                {"i8": None, "u8": None, "i64": None, "u64": None, "f": None, "b": None,
                 "s": None, "bin": None, "d": None, "dec": None},
            ],
            pa.struct([
                ("i8", pa.int8()), ("u8", pa.uint8()), ("i64", pa.int64()), ("u64", pa.uint64()),
                ("f", pa.float64()), ("b", pa.bool_()), ("s", pa.string()), ("bin", pa.binary()),
                ("d", pa.date32()), ("dec", pa.decimal128(18, 2)),
            ]),
        ),
    })
    d = write(tmp_path, t)
    cols, sources = run(d)
    assert sources <= NATIVE, sources
    assert cols["s"][0] == (
        '{"i8":-8,"u8":200,"i64":-4611686018427387904,"u64":18446744073709551615,"f":1.5,'
        '"b":true,"s":"q\\"\\\\\\né€","bin":"AP9hYg==","d":"2024-02-29","dec":-12345.67}')
    assert cols["s"][1] is None                      # NULL group -> SQL NULL
    assert cols["s"][2] == (                         # NULL fields -> JSON null, order kept
        '{"i8":null,"u8":null,"i64":null,"u64":null,"f":null,"b":null,"s":null,'
        '"bin":null,"d":null,"dec":null}')


def test_floats_nan_inf_null_and_negative_zero(tmp_path):
    t = pa.table({
        "id": pa.array([1], pa.int64()),
        "s": pa.array([{"a": float("nan"), "b": float("inf"), "c": float("-inf"), "d": -0.0,
                        "f32": 0.1}],
                      pa.struct([("a", pa.float64()), ("b", pa.float64()), ("c", pa.float64()),
                                 ("d", pa.float64()), ("f32", pa.float32())])),
    })
    cols, sources = run(write(tmp_path, t))
    assert sources <= NATIVE, sources
    doc = parse(cols["s"][0])
    assert doc["a"] is None and doc["b"] is None and doc["c"] is None
    assert doc["d"] == 0                              # the scan's -0.0 canonicalisation
    assert doc["f32"] == Decimal("0.1")               # float32 rendered as float32, not widened


def test_temporal_and_decimal_leaves(tmp_path):
    t = pa.table({
        "id": pa.array([1], pa.int64()),
        "s": pa.array(
            [{"ts_ms": datetime.datetime(2024, 3, 5, 1, 2, 3, 456000),
              "ts_us": datetime.datetime(2024, 3, 5, 1, 2, 3, 456789),
              "ts_s_whole": datetime.datetime(2001, 9, 9, 1, 46, 40),
              "t32": datetime.time(1, 2, 3, 500000), "t64": datetime.time(23, 59, 59, 123456),
              "dec9": decimal.Decimal("1234567.89"), "dec38": decimal.Decimal("-1234567890123456789012345678.9012345678")}],
            pa.struct([("ts_ms", pa.timestamp("ms")), ("ts_us", pa.timestamp("us")),
                       ("ts_s_whole", pa.timestamp("ms")), ("t32", pa.time32("ms")),
                       ("t64", pa.time64("us")), ("dec9", pa.decimal128(9, 2)),
                       ("dec38", pa.decimal128(38, 10))]),
        ),
    })
    cols, sources = run(write(tmp_path, t))
    assert sources <= NATIVE, sources
    assert cols["s"][0] == (
        '{"ts_ms":"2024-03-05T01:02:03.456000+00:00","ts_us":"2024-03-05T01:02:03.456789+00:00",'
        '"ts_s_whole":"2001-09-09T01:46:40+00:00","t32":"01:02:03.500000","t64":"23:59:59.123456",'
        '"dec9":1234567.89,"dec38":-1234567890123456789012345678.9012345678}')


# ── nesting ─────────────────────────────────────────────────────────────────

def test_map_object_empty_null_and_null_values(tmp_path):
    t = pa.table({
        "id": pa.array([1, 2, 3, 4], pa.int64()),
        "s": pa.array([[("a", 1), ("b", None)], [], None, [("z", 26)]],
                      pa.map_(pa.string(), pa.int64())),
    })
    cols, sources = run(write(tmp_path, t))
    assert sources <= NATIVE, sources
    assert cols["s"] == ['{"a":1,"b":null}', '{}', None, '{"z":26}']


def test_nested_struct_list_map_shapes(tmp_path):
    inner = pa.struct([("x", pa.int32()), ("tags", pa.list_(pa.string()))])
    typ = pa.struct([
        ("name", pa.string()),
        ("inner", inner),
        ("items", pa.list_(inner)),
        ("m", pa.map_(pa.string(), inner)),
        ("ll", pa.list_(pa.list_(pa.int64()))),
    ])
    rows = [
        {"name": "a", "inner": {"x": 1, "tags": ["p", "q"]},
         "items": [{"x": 2, "tags": []}, None, {"x": None, "tags": None}],
         "m": [("k", {"x": 9, "tags": ["z"]}), ("n", None)], "ll": [[1, 2], [], [3]]},
        {"name": None, "inner": None, "items": None, "m": None, "ll": None},
        {"name": "c", "inner": {"x": None, "tags": None}, "items": [], "m": [], "ll": [[]]},
    ]
    t = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "s": pa.array(rows, typ)})
    cols, sources = run(write(tmp_path, t))
    assert sources <= NATIVE, sources
    check_column(cols, t)
    assert cols["s"][0].startswith('{"name":"a","inner":{"x":1,"tags":["p","q"]},"items":[')


@pytest.mark.parametrize("use_dictionary", [False, True])
@pytest.mark.parametrize("compression", ["none", "zstd"])
def test_row_groups_dictionary_and_compression(tmp_path, use_dictionary, compression):
    n = 500
    typ = pa.struct([("a", pa.int64()), ("b", pa.string()), ("l", pa.list_(pa.string()))])
    rows = [None if i % 11 == 0 else
            {"a": None if i % 7 == 0 else i % 5, "b": f"v{i % 3}",
             "l": None if i % 13 == 0 else [f"t{i % 4}"] * (i % 3)}
            for i in range(n)]
    t = pa.table({"id": pa.array(range(n), pa.int64()), "s": pa.array(rows, typ)})
    d = write(tmp_path, t, row_group_size=64, use_dictionary=use_dictionary, compression=compression)
    cols, sources = run(d)
    assert sources <= NATIVE, sources
    assert cols["id"] == list(range(n))
    check_column(cols, t)


# ── the scan around the column ──────────────────────────────────────────────

def _filter_table():
    n = 200
    typ = pa.struct([("a", pa.int64()), ("m", pa.map_(pa.string(), pa.int64()))])
    rows = [{"a": i, "m": [(f"k{i % 3}", i)]} for i in range(n)]
    return pa.table({
        "id": pa.array(range(n), pa.int64()),
        "grp": pa.array([f"g{i % 10}" for i in range(n)], pa.string()),
        "s": pa.array(rows, typ),
    })


def test_selective_filter_on_another_column(tmp_path):
    """The live shape: `SELECT * ... WHERE <other column> = x` with the group projected."""
    t = _filter_table()
    d = write(tmp_path, t, row_group_size=32)
    cols, sources = run(d, "WHERE grp = 'g3'", columns="id, grp, s")
    assert sources <= NATIVE, sources
    keep = [i for i in range(200) if i % 10 == 3]
    assert cols["id"] == keep
    expected = tree_rows(t, keep)
    assert [parse(x) for x in cols["s"]] == expected


def tree_rows(t, idx):
    typ = t.schema.field("s").type
    vals = t.column("s").to_pylist()
    return [tree(vals[i], typ) for i in idx]


def test_range_filter_limit_and_order(tmp_path):
    t = _filter_table()
    d = write(tmp_path, t, row_group_size=32)
    cols, sources = run(d, "WHERE id >= 40 AND id < 75 ORDER BY id DESC LIMIT 10", columns="id, s", ordered=True)
    assert sources <= NATIVE, sources
    ids = list(range(74, 64, -1))
    assert cols["id"] == ids
    assert [parse(x) for x in cols["s"]] == tree_rows(t, ids)


def test_project_only_the_group(tmp_path):
    t = _filter_table()
    d = write(tmp_path, t, row_group_size=50)
    cols, sources = run(d, "", columns="s")
    assert sources <= NATIVE, sources
    # no id column to line rows up by: the scan is unordered, so compare by the struct's own key
    assert sorted((parse(x) for x in cols["s"]), key=lambda d: d["a"]) == tree_rows(t, range(200))


def test_group_between_other_columns_keeps_projection_order(tmp_path):
    t = _filter_table()
    d = write(tmp_path, t, row_group_size=64)
    cols, sources = run(d, "WHERE id < 5", columns="grp, s, id")
    assert sources <= NATIVE, sources
    assert list(cols) == ["grp", "s", "id"]
    assert cols["id"] == [0, 1, 2, 3, 4]


# ── randomized shapes ───────────────────────────────────────────────────────

def _rand_type(rng, depth):
    leaves = [pa.int32(), pa.int64(), pa.float64(), pa.bool_(), pa.string(), pa.uint16()]
    if depth >= 3 or rng.random() < 0.35:
        return rng.choice(leaves)
    kind = rng.choice(["struct", "list", "map"])
    if kind == "struct":
        return pa.struct([(f"f{i}", _rand_type(rng, depth + 1)) for i in range(rng.randint(1, 3))])
    if kind == "list":
        return pa.list_(_rand_type(rng, depth + 1))
    return pa.map_(pa.string(), _rand_type(rng, depth + 1))


def _rand_value(rng, typ):
    if rng.random() < 0.2:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: _rand_value(rng, typ.field(i).type) for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        keys = rng.sample(["a", "b", "c", "d", "e"], rng.randint(0, 3))
        return [(k, _rand_value(rng, typ.item_type)) for k in keys]
    if pa.types.is_list(typ):
        return [_rand_value(rng, typ.value_type) for _ in range(rng.randint(0, 3))]
    if pa.types.is_boolean(typ):
        return rng.random() < 0.5
    if pa.types.is_string(typ):
        return rng.choice(["x", "yy", "a b", 'q"t', "é"])
    if pa.types.is_floating(typ):
        return rng.choice([0.5, -2.25, 1e10, 3.0])
    if pa.types.is_unsigned_integer(typ):
        return rng.randint(0, 60000)
    return rng.randint(-1000, 1000)


@pytest.mark.parametrize("seed", range(40))
def test_random_nested_shapes_match_the_oracle(tmp_path, seed):
    rng = random.Random(seed)
    # the top level must be a STRUCT or MAP (a bare LIST is the ARRAY path, not this one)
    typ = rng.choice([pa.struct([(f"f{i}", _rand_type(rng, 1)) for i in range(rng.randint(1, 3))]),
                      pa.map_(pa.string(), _rand_type(rng, 1))])
    n = rng.randint(1, 150)
    t = pa.table({"id": pa.array(range(n), pa.int64()),
                  "s": pa.array([_rand_value(rng, typ) for _ in range(n)], typ)})
    d = write(tmp_path, t, row_group_size=rng.choice([7, 32, 1000]),
              use_dictionary=rng.random() < 0.5,
              compression=rng.choice(["none", "snappy", "zstd"]))
    cols, sources = run(d)
    assert sources <= NATIVE, (sources, typ)
    assert cols["id"] == list(range(n))
    check_column(cols, t)


# ── refusals are loud ───────────────────────────────────────────────────────

def test_non_string_map_key_is_refused_naming_the_column(tmp_path):
    t = pa.table({
        "id": pa.array([1], pa.int64()),
        "s": pa.array([[(1, "a")]], pa.map_(pa.int64(), pa.string())),
    })
    d = write(tmp_path, t)
    with pytest.raises(NotImplementedError, match="(?s)column 's'.*non-string key"):
        run(d)


def test_trampoline_refuses_instead_of_returning_nulls(tmp_path, monkeypatch):
    t = pa.table({
        "id": pa.array([1, 2], pa.int64()),
        "s": pa.array([{"a": 1}, None], pa.struct([("a", pa.int64())])),
    })
    d = write(tmp_path, t)
    with pytest.raises(NotImplementedError, match="column 's'"):
        run(d, force_trampoline=True, monkeypatch=monkeypatch)
