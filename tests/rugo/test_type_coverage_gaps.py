# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Closes the DrakenType gaps a review of `make rt`'s type coverage turned up:
NULL, NVARCHAR, narrow DECIMAL (as distinct from DECIMAL128), VARIANT, and
VECTOR_FP16 were not constructed as their own physical type anywhere in
tests/rugo/. Each gets its own type-identity assertion (not just a value
comparison — a type drift is invisible in a value comparison whenever the
representation happens to coincide, same rationale as
test_parquet_int_width_roundtrip.py), plus a round-trip through whichever
writers actually claim to support it.

None of these are clean, type-preserving round-trips, and all are pinned
down as current behaviour rather than silently avoided:

  * VECTOR_FP16 has no wire representation in any of the three formats (no
    Parquet vector type, no CSV/JSON vector type), so all three writers emit
    it as an array of floats (fp16->fp32, lossy by construction) rather than
    rejecting the column or -- as write_csv/write_jsonl previously did --
    silently rendering every row as NULL/blank regardless of real data. A
    rugo re-read of the Parquet output comes back as an ordinary
    ARRAY<FLOAT64>, not VECTOR_FP16; that type-identity loss is accepted,
    per the architect, in exchange for not rejecting or silently dropping
    the column.

  * VARIANT round-trips through Parquet as a plain string (Parquet has no
    variant/JSON logical type in this codebase, so the tag is lost on
    read-back -- expected, matches VARCHAR's own storage). Through JSONL,
    rugo's writer re-escapes the stored raw-JSON-text as a JSON *string*
    rather than inlining it as a nested object -- consistent with VARIANT's
    documented storage ("same raw-JSON-text storage as VARCHAR, just tagged
    differently", draken_native.cpp), not a bug, but worth pinning down since
    it means VARIANT->JSONL is not a value-preserving-as-a-nested-object
    round trip.
"""

import io
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # test oracle only
import pyarrow.parquet as pq  # test oracle only

import draken  # noqa: F401 — must precede rugo native imports
import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from rugo.csv import write_csv
from rugo.rugo_native import read_csv, read_jsonl, write_jsonl
import rugo.parquet as rp

DrakenType = dn.DrakenType


def _morsel(name, vec):
    return Morsel.from_vectors([name], [vec])


# ─────────────────────────────────────────────────────────────────────────────
# NULL — a column whose PHYSICAL type is NULL, not a nullable column of some
# other type that happens to be all-null.
# ─────────────────────────────────────────────────────────────────────────────


def test_null_type_identity():
    v = dn.vector_null_from_length(3)
    assert v.type == DrakenType.NULL
    assert v.to_pylist() == [None, None, None]


def test_null_parquet_roundtrip():
    """write_parquet's docstring lists "all-null (NULL) columns" as supported.
    Parquet has no NULL physical type on the wire, so rugo picks a placeholder
    (currently INT32); the read-back DrakenType reflects that placeholder, not
    NULL — documented here, not asserted as if it were NULL-preserving."""
    v = dn.vector_null_from_length(3)
    m = _morsel("v", v)
    data = rp.write_parquet(m, compression="none", bloom_filters=False)

    got = pq.read_table(io.BytesIO(data)).column("v").to_pylist()
    assert got == [None, None, None]

    morsels = list(rp.read_parquet(data))
    assert len(morsels) == 1
    col = morsels[0].column(b"v")
    assert col.to_pylist() == [None, None, None]
    assert col.type != DrakenType.NULL, (
        "rugo now round-trips the NULL type tag itself through Parquet — "
        "update this test's expectations, this assertion documents the "
        "PREVIOUS (placeholder-physical-type) behaviour, not a requirement."
    )


def test_null_csv_roundtrip():
    v = dn.vector_null_from_length(3)
    m = _morsel("v", v)
    data = write_csv(m)
    assert data == b"v\n\n\n\n"

    r = read_csv(data)
    assert r["success"]
    assert r["num_rows"] == 3
    assert r["columns"][0].to_pylist() == [None, None, None]


def test_null_jsonl_roundtrip():
    v = dn.vector_null_from_length(3)
    m = _morsel("v", v)
    data = write_jsonl(m)
    rows = [json.loads(line) for line in data.decode().splitlines()]
    assert rows == [{"v": None}, {"v": None}, {"v": None}]


# ─────────────────────────────────────────────────────────────────────────────
# NVARCHAR
# ─────────────────────────────────────────────────────────────────────────────


def test_nvarchar_type_identity():
    v = dn.vector_from_nvarchar_sequence([b"abc", None, "a longer utf-8 ☃ string".encode()])
    assert v.type == DrakenType.NVARCHAR
    assert v.to_pylist() == ["abc", None, "a longer utf-8 ☃ string"]


def test_nvarchar_parquet_write_matches_pyarrow():
    """Parquet has no NVARCHAR-specific annotation distinct from VARCHAR, so
    values survive the write but the type tag does not survive a full rugo
    write -> rugo read cycle -- it comes back as VARCHAR. Expected, since the
    wire format genuinely can't tell them apart; documented, not asserted as
    if NVARCHAR were preserved."""
    values = ["abc", None, "a longer utf-8 ☃ string"]
    v = dn.vector_from_nvarchar_sequence([s.encode() if s is not None else None for s in values])
    m = _morsel("v", v)
    data = rp.write_parquet(m, compression="none", bloom_filters=False)

    got = pq.read_table(io.BytesIO(data)).column("v").to_pylist()
    assert got == values

    morsels = list(rp.read_parquet(data))
    col = morsels[0].column(b"v")
    assert col.to_pylist() == values
    assert col.type == DrakenType.VARCHAR


# ─────────────────────────────────────────────────────────────────────────────
# DECIMAL (narrow, precision<=18) — distinct DrakenType from DECIMAL128.
# vector_decimal_from_sequence enforces precision in [1, 18] and always
# produces DrakenType.DECIMAL; vector_decimal128_from_sequence is the separate
# constructor for precision in [1, 38] / DrakenType.DECIMAL128. Nothing in the
# existing Parquet oracle (decimal_9_2/decimal_38_2) asserts which DrakenType
# comes back — only that the *values* match — so the type identity itself was
# untested. rugo's own write -> read cycle DOES preserve DECIMAL (unlike NULL
# and NVARCHAR above), which this test pins down.
# ─────────────────────────────────────────────────────────────────────────────


def test_decimal_narrow_type_identity():
    import decimal

    v = dn.vector_decimal_from_sequence([decimal.Decimal("1.23"), None, decimal.Decimal("-9.99")], 9, 2)
    assert v.type == DrakenType.DECIMAL


def test_decimal_narrow_roundtrips_through_pyarrow_and_preserves_type():
    import decimal

    values = [decimal.Decimal("1.23"), None, decimal.Decimal("-9.99")]
    v = dn.vector_decimal_from_sequence(values, 9, 2)
    m = _morsel("v", v)
    data = rp.write_parquet(m, compression="none", bloom_filters=False)

    got = pq.read_table(io.BytesIO(data)).column("v").to_pylist()
    assert got == values

    morsels = list(rp.read_parquet(data))
    col = morsels[0].column(b"v")
    assert col.to_pylist() == values
    assert col.type == DrakenType.DECIMAL, (
        "narrow-precision DECIMAL should survive a rugo write->read cycle as "
        "DECIMAL, not widen to DECIMAL128"
    )


# ─────────────────────────────────────────────────────────────────────────────
# VARIANT — only reachable via read_jsonl(parse_objects=True) on a JSON-object
# column; there is no standalone vector_from_variant_sequence constructor.
# ─────────────────────────────────────────────────────────────────────────────


def _variant_column(objects):
    """objects: list[dict | None]. Builds a VARIANT column via the JSONL reader
    (the only construction path) and returns the Draken vector."""
    lines = [json.dumps({"v": o}) for o in objects]
    data = ("\n".join(lines) + "\n").encode()
    r = read_jsonl(
        data, columns=None, predicates=None, explicit_schema=None, infer_schema=True,
        infer_sample_size=5, parse_arrays=True, parse_objects=True, fail_on_error=True,
        use_threads=True,
    )
    assert r["success"]
    return r["columns"][0]


def test_variant_type_identity_from_jsonl_parse_objects():
    col = _variant_column([{"a": 1, "b": "x"}, None, {"c": [1, 2]}])
    assert col.type == DrakenType.VARIANT
    assert col.to_pylist() == ['{"a": 1, "b": "x"}', None, '{"c": [1, 2]}']


def test_variant_parquet_write_degrades_to_plain_string():
    """Parquet has no variant/JSON logical type here -- VARIANT writes as a
    plain UTF8 string, and the raw JSON text survives byte-for-byte."""
    objects = [{"a": 1, "b": "x"}, None, {"c": [1, 2]}]
    expected_text = [json.dumps(o) if o is not None else None for o in objects]
    col = _variant_column(objects)
    m = _morsel("v", col)
    data = rp.write_parquet(m, compression="none", bloom_filters=False)

    got = pq.read_table(io.BytesIO(data)).column("v").to_pylist()
    assert got == expected_text


def test_variant_jsonl_write_reescapes_as_json_string_not_nested_object():
    """Current behaviour: write_jsonl treats a VARIANT column as opaque raw-
    JSON-text storage (matching its documented semantics -- "same raw-JSON-
    text storage as VARCHAR, just tagged differently") and JSON-string-encodes
    it, rather than inlining the parsed object back into the output line.
    A VARIANT round-trip through JSONL is therefore string-preserving, not
    structurally-inlining -- pinned down here, not asserted as a bug."""
    objects = [{"a": 1, "b": "x"}, None]
    col = _variant_column(objects)
    m = _morsel("v", col)
    data = write_jsonl(m)

    rows = [json.loads(line) for line in data.decode().splitlines()]
    assert rows == [{"v": '{"a": 1, "b": "x"}'}, {"v": None}]


# ─────────────────────────────────────────────────────────────────────────────
# VECTOR_FP16 — no wire type in Parquet, CSV, or JSONL; all three writers
# emit it as an array of floats (fp16->fp32, lossy) rather than reject the
# column. write_csv/write_jsonl previously (silently, incorrectly) rendered
# every row as NULL/blank regardless of real data -- that's fixed below to
# assert the real content actually comes through.
# ─────────────────────────────────────────────────────────────────────────────


def test_vector_fp16_type_identity():
    v = dn.vector_fp16_from_sequence([[1.0, 2.0], [3.0, 4.0]], 2)
    assert v.type == DrakenType.VECTOR_FP16
    assert v.to_pylist() == [[1.0, 2.0], [3.0, 4.0]]


def test_vector_fp16_write_parquet_renders_as_array_of_floats():
    """No native Parquet vector type -- VECTOR_FP16 writes as LIST<DOUBLE>.
    The VECTOR_FP16 tag itself does not survive a rugo write->read cycle
    (Parquet has nothing to preserve it as); a re-read comes back as an
    ordinary ARRAY<FLOAT64>, which is accepted, not asserted as a defect."""
    values = [[1.0, 2.0, 3.0], None, [4.5, 5.5, 6.5]]
    v = dn.vector_fp16_from_sequence(values, 3)
    m = _morsel("v", v)
    data = rp.write_parquet(m, compression="none", bloom_filters=False)

    got = pq.read_table(io.BytesIO(data)).column("v").to_pylist()
    assert got == values

    morsels = list(rp.read_parquet(data))
    col = morsels[0].column(b"v")
    assert col.to_pylist() == values
    assert col.type == DrakenType.ARRAY


def test_vector_fp16_write_csv_renders_as_json_array():
    v = dn.vector_fp16_from_sequence([[1.0, 2.0], None, [4.5, 6.5]], 2)
    assert v.is_null_at(0) is False
    assert v.is_null_at(2) is False
    m = _morsel("v", v)

    got = write_csv(m)
    assert got == b'v\n"[1.0,2.0]"\n\n"[4.5,6.5]"\n'


def test_vector_fp16_write_jsonl_renders_as_json_array():
    v = dn.vector_fp16_from_sequence([[1.0, 2.0], None, [4.5, 6.5]], 2)
    m = _morsel("v", v)

    rows = [json.loads(line) for line in write_jsonl(m).decode().splitlines()]
    assert rows == [{"v": [1.0, 2.0]}, {"v": None}, {"v": [4.5, 6.5]}]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
