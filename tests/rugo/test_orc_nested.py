# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo's ORC reader and nested columns (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1, ND5): random
STRUCT / LIST / MAP shapes written by pyarrow.orc (the Apache ORC C++ library, an independent
writer) read back against a Python oracle, under every codec, both run-length versions, direct and
dictionary strings, and many stripes. Every expected value is the value the test wrote.

PyArrow is test-only here (CLAUDE.md §4).
"""

import io
import json
import math
import os
import random
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa  # noqa: E402
import pyarrow.orc as po  # noqa: E402

from rugo import orc  # noqa: E402

SCALARS = [pa.int64(), pa.int32(), pa.string(), pa.float64(), pa.bool_()]
WORDS = ["", "a", "é", "日本", 'q"uote', "back\\slash", "tab\t", "x" * 40]


def _tree(value, typ):
    """What the JSON text of `value` must parse to (numbers as Decimal)."""
    if value is None:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: _tree(value[typ.field(i).name], typ.field(i).type) for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        return {k: _tree(v, typ.item_type) for k, v in value}
    if pa.types.is_list(typ):
        return [_tree(v, typ.value_type) for v in value]
    if pa.types.is_boolean(typ):
        return bool(value)
    if pa.types.is_integer(typ):
        return Decimal(int(value))
    if pa.types.is_floating(typ):
        return None if math.isnan(value) or math.isinf(value) else Decimal(repr(float(value)))
    return value


def _type(rng, depth, top=False):
    if not top and (depth == 0 or rng.random() < 0.4):
        return rng.choice(SCALARS)
    shape = rng.choice(["struct", "list", "map"])
    if shape == "struct":
        return pa.struct([(f"f{i}", _type(rng, depth - 1)) for i in range(rng.randint(1, 4))])
    if shape == "list":
        return pa.list_(_type(rng, depth - 1))
    return pa.map_(pa.string(), _type(rng, depth - 1))


def _value(rng, typ, null_rate):
    if rng.random() < null_rate:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: _value(rng, typ.field(i).type, null_rate) for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        keys = rng.sample(WORDS, rng.randint(0, 3))
        return [(k, _value(rng, typ.item_type, null_rate)) for k in keys]
    if pa.types.is_list(typ):
        return [_value(rng, typ.value_type, null_rate) for _ in range(rng.randint(0, 4))]
    if pa.types.is_boolean(typ):
        return rng.random() < 0.5
    if pa.types.is_integer(typ):
        return rng.choice([0, 1, -1, rng.randint(-(2**31), 2**31 - 1), 7, 7, 7])
    if pa.types.is_floating(typ):
        return rng.choice([0.0, 1.5, -2.25, rng.uniform(-1e6, 1e6), float("nan"), float("inf")])
    return rng.choice(WORDS)


def _is_array(typ):
    """A LIST of a plain scalar the engine's ARRAY holds (every SCALARS member), or of lists of one."""
    if not pa.types.is_list(typ):
        return False
    while pa.types.is_list(typ):
        typ = typ.value_type
    return typ in SCALARS


def _nan_free(values):
    """NaN != NaN: compare (nested) lists of floats with NaN as a sentinel."""
    def fix(x):
        if isinstance(x, list):
            return [fix(e) for e in x]
        return "nan" if isinstance(x, float) and math.isnan(x) else x
    return [fix(v) for v in values]


def _read(data, columns):
    out = {}
    for morsel in orc.read_orc(data, columns=columns):
        for name in morsel.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            out.setdefault(key, []).extend(morsel.column(name).to_pylist())
    return out


def _check(table, data):
    got = _read(data, table.column_names)
    for name in table.column_names:
        typ = table.schema.field(name).type
        values = table.column(name).to_pylist()
        if _is_array(typ):
            assert _nan_free(got.get(name, [])) == _nan_free(values), name
            continue
        want = [_tree(v, typ) for v in values]
        have = [None if v is None else json.loads(v, parse_float=Decimal, parse_int=Decimal) for v in got.get(name, [])]
        assert have == want, name


def _table(seed, rows, null_rate):
    rng = random.Random(seed)
    types = {f"c{i}": _type(rng, 3, top=True) for i in range(6)}
    return pa.table({n: pa.array([_value(rng, t, null_rate) for _ in range(rows)], t) for n, t in types.items()})


def _write(table, **options):
    buf = io.BytesIO()
    po.write_table(table, buf, **options)
    return buf.getvalue()


@pytest.mark.parametrize("codec", ["uncompressed", "zlib", "snappy", "lz4", "zstd"])
@pytest.mark.parametrize("seed", range(6))
def test_random_nested_shapes_read_back_as_written(codec, seed):
    table = _table(seed, 300, 0.25)
    _check(table, _write(table, compression=codec))


@pytest.mark.parametrize("version", ["0.11", "0.12"])
@pytest.mark.parametrize("dictionary", [0.0, 1.0], ids=["direct", "dictionary"])
def test_run_length_versions_and_dictionary_strings_under_nesting(version, dictionary):
    table = _table(11, 400, 0.2)
    _check(table, _write(table, file_version=version, dictionary_key_size_threshold=dictionary, compression="zlib"))


@pytest.mark.parametrize("null_rate", [0.0, 1.0])
def test_all_present_and_all_null(null_rate):
    table = _table(21, 120, null_rate)
    _check(table, _write(table))


def test_many_stripes():
    table = _table(31, 40000, 0.3)
    data = _write(table, stripe_size=64 * 1024, compression="zlib")
    assert orc.read_metadata(data).stripes > 1
    _check(table, data)


def test_an_empty_file_has_nested_columns_and_no_rows():
    table = _table(41, 0, 0.0)
    _check(table, _write(table))


def test_a_deeply_nested_column_is_refused_naming_the_depth():
    typ = pa.int64()
    for _ in range(70):
        typ = pa.list_(typ)
    table = pa.table({"deep": pa.array([None], typ)})
    meta = orc.read_metadata(_write(table))
    (column,) = meta.columns
    assert column["readable"] is False and "nested more than 64 levels" in column["refusal"]
