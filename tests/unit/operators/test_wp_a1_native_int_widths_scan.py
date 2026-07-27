"""A1 — admit NARROW and UNSIGNED INTEGER widths to the native parquet scan.

Before A1 the native scan (NativeParquetScanSource) admitted only bare int32/int64
integer columns (WP-01/02/11). Narrow (int8/int16) and unsigned (uint8/16/32/64)
columns — and INTEGER columns carrying an `INTEGER(bits, signed)` annotation, which
is how real ClickBench columns arrive (AdvEngineID/ResolutionWidth = int16, EventDate
= uint16) — were refused by the C-side footer gate (`native_scan_supported`) and fell
back to the per-morsel Python trampoline (residual reason `footer_gate` / R7b, 77% of
the A0 census). A1 widens the gate + the native decode to admit every width and
signedness.

The correctness gate is A/B PARITY: the native path must produce the SAME survivor
set (values + full logical descriptor) as the forced-trampoline path, which uses the
identical rugo decode. The shared decode already decides the representation:

  * signed int8 / int16 / int32  → widen to DRAKEN_INT64  (decode's direct_kind_for)
  * int64                        → DRAKEN_INT64
  * unsigned uint8/16/32/64      → EXACT width DRAKEN_UINT8/16/32/64 (no widen)

so a fidelity bug shows up as a native-vs-trampoline mismatch: the trampoline consumer
(pool_reader `_wrap_direct` / `_wrap_num_dict_direct`) and the native Source
(`draken_type_for` + `draken_vector_from_dense/_dict`) must tag the SAME ColumnOut
byte-identically. UINT64 above INT64_MAX is representable as a native DRAKEN_UINT64
vector (no truncation), so it is admitted, not fail-closed — the boundary tests below
carry values > 2**63 and > 2**31.

Roles covered per width: projected, projected+predicate (c-native predicate INPUT on
the narrow/unsigned column), and role-3 filter-only (predicate column read but not
emitted). Dense AND dict encodings, with and without nulls. Mirrors the WP-11 harness.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.connectors.parquet_io import pool_reader


def _write(dataset_dir, columns, use_dictionary=True, row_group_size=None):
    """Write one parquet file. `columns` = {name: (pyarrow_type, py_list)}."""
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    kw = {"use_dictionary": use_dictionary}
    if row_group_size is not None:
        kw["row_group_size"] = row_group_size
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _col_sig(morsel, n):
    """Full logical signature of a column: DrakenType tag + logical descriptor. A
    width/signedness drift (e.g. UINT16 decoded as INT64) changes col.type here even
    when the integer magnitudes coincide."""
    col = morsel.column(n)
    if col is None:
        return (None, None, None, None)
    nb = col._nb
    return (col.type, nb.logical_type_unit, nb.logical_type_precision, nb.logical_type_scale)


def _drain(sql, force_trampoline, monkeypatch):
    """Drain `sql`; return ((per-column signature, sorted row multiset), sources)."""
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows = []
    sig = None
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        if sig is None:
            sig = tuple((n, _col_sig(morsel, n)) for n in names)
        for i in range(morsel.num_rows):
            rows.append(tuple(
                repr(None if morsel.column(n) is None else morsel.column(n)[i])
                for n in names
            ))
    src = list(session._telemetry.as_dict()["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    return (sig, tuple(sorted(rows))), src


def _assert_parity(tmp_path, monkeypatch, columns, sql_tail, *, write_kw=None,
                   expect_native=True):
    """Write `columns`, run `SELECT {sql_tail}` native and forced-trampoline, assert
    identical survivor set + descriptor, and (when expect_native) that the native run
    selected NativeParquetScanSource while the forced run did not."""
    ds = _write(str(tmp_path / "a1"), columns, **(write_kw or {}))
    proj, _, where = sql_tail.partition(" WHERE ")
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where

    nat, nat_src = _drain(sql, False, monkeypatch)
    tmp, tmp_src = _drain(sql, True, monkeypatch)

    assert nat == tmp, "native survivor set / descriptor differs from trampoline"
    if expect_native:
        assert nat_src == ["NativeParquetScanSource"], nat_src
        assert tmp_src == ["StreamingScanSource"], tmp_src
    return nat[0], nat[1]


# Per-width (pyarrow type, boundary+ordinary values). The unsigned upper bounds sit
# ABOVE the signed maximum of the same width (uint32 > 2**31, uint64 > 2**63) — the
# exact "can it be represented byte-identically" risk A1 must clear.
_WIDTHS = {
    "int8": (pa.int8(), [-128, -1, 0, 1, 127, -128, 42]),
    "int16": (pa.int16(), [-32768, -1, 0, 1, 32767, -5, 300]),
    "int32": (pa.int32(), [-2147483648, -1, 0, 1, 2147483647, 7, -99]),
    "int64": (pa.int64(), [-9223372036854775808, 0, 9223372036854775807, 1, -1, 2, 3]),
    "uint8": (pa.uint8(), [0, 1, 127, 128, 255, 0, 200]),
    "uint16": (pa.uint16(), [0, 1, 32768, 65535, 40000, 0, 12345]),
    "uint32": (pa.uint32(), [0, 1, 2147483648, 4294967295, 3000000000, 0, 7]),
    "uint64": (pa.uint64(), [0, 1, 9223372036854775808, 18446744073709551615,
                             9223372036854775809, 0, 42]),
}


@pytest.mark.parametrize("width", list(_WIDTHS))
@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "plain"])
def test_int_width_projection(width, use_dictionary, tmp_path, monkeypatch):
    """Projected narrow/unsigned int column, dense and dict encodings, at signed/
    unsigned boundary values."""
    typ, vals = _WIDTHS[width]
    cols = {"v": (typ, vals * 30), "n": (pa.int64(), list(range(len(vals) * 30)))}
    _assert_parity(tmp_path, monkeypatch, cols, "v, n",
                   write_kw={"use_dictionary": use_dictionary})


@pytest.mark.parametrize("width", list(_WIDTHS))
def test_int_width_with_nulls(width, tmp_path, monkeypatch):
    """Projected column with nulls interleaved at the boundary values."""
    typ, vals = _WIDTHS[width]
    payload = []
    for v in vals * 30:
        payload.append(v)
        payload.append(None)
    cols = {"v": (typ, payload)}
    _assert_parity(tmp_path, monkeypatch, cols, "v")


# Every integer width — signed AND unsigned — now goes native as a predicate input.
# Two things had to be true for that, and both now are:
#   * the column's DECLARED type carries its real width (_rugo_schema
#     ._integer_column_type), so `_coerce_literal_physical` re-materializes the
#     comparison literal at that width and draken_compare_dv's identical-type guard
#     is satisfied; and
#   * draken_compare_dv dispatches the unsigned widths to u8/u16/u32/u64
#     _compare_vector, which had existed (and been registered in hash.h) but were
#     never wired into that switch.
# Together these retired the A1 `unsigned_predicate_input` fail-closed entirely.
# Either way the survivor set must match the forced-trampoline run byte-for-byte.

# uint32/uint64 used to be excluded here. A column holding any value >= 2**31 (resp.
# 2**63) silently returned ZERO rows for a predicate on it — on the native AND the
# trampoline path alike — because the statistics were decoded/compared as SIGNED: a
# high value reads as negative, so the row group looked out of range and was pruned
# away before any row was examined. Fixed in rugo.parquet.decode_value (per-row-group
# pruning) and metadata.cpp's CompareStatBytes (min-of-mins across row groups/files).
# The _WIDTHS entries below deliberately straddle those midpoints, so these cases now
# pin the fix.
_PRED_DRAINABLE = ["int8", "int16", "int32", "int64",
                   "uint8", "uint16", "uint32", "uint64"]


@pytest.mark.parametrize("width", _PRED_DRAINABLE)
def test_int_width_predicate_input(width, tmp_path, monkeypatch):
    """The narrow/unsigned column is a c-native predicate INPUT and also projected.
    Native for every width; survivor set identical to the forced-trampoline run."""
    typ, vals = _WIDTHS[width]
    cols = {"v": (typ, vals * 30), "n": (pa.int64(), list(range(len(vals) * 30)))}
    _assert_parity(tmp_path, monkeypatch, cols, "v, n WHERE v > 0")
    _assert_parity(tmp_path, monkeypatch, cols, "v WHERE v = 1")


@pytest.mark.parametrize("width", _PRED_DRAINABLE)
def test_int_width_role3_filter_only(width, tmp_path, monkeypatch):
    """Role-3: the narrow/unsigned column is read only as a predicate input (not
    emitted). Native for every width, same as predicate_input."""
    typ, vals = _WIDTHS[width]
    cols = {"v": (typ, vals * 30), "n": (pa.int64(), list(range(len(vals) * 30)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE v > 0")


# (The former test_wide_unsigned_predicate_input_fails_closed asserted the A1
# fail-closed DECISION via the `any_column_unsigned` footer probe. Both the probe and
# the gate it fed are gone — unsigned predicate inputs are evaluated natively now — so
# the assertion had nothing left to pin. The behaviour that replaced it is covered by
# test_int_width_predicate_input / _role3_filter_only over every width above.)


@pytest.mark.parametrize("dtype,lo,hi,mid", [
    (pa.uint32(), [1, 2, 3], [3200000000, 4000000000, 4294967295], 4000000000),
    (pa.uint64(), [1, 2, 3], [2**63, 2**63 + 7, 2**64 - 1], 2**63 + 7),
])
def test_unsigned_stats_aggregate_across_files(dtype, lo, hi, mid, tmp_path):
    """min/max statistics must AGGREGATE in the unsigned domain across row groups and
    files, not just decode correctly within one.

    One file holds only values below the signed midpoint, another only values above
    it. Comparing the raw stat bytes as signed picks the wrong min-of-mins /
    max-of-maxes, which inverts the file's apparent range and prunes away files that
    genuinely match — the row-group-level fix alone does not cover this path
    (metadata.cpp's CompareStatBytes)."""
    ds = str(tmp_path / "uagg")
    os.makedirs(ds, exist_ok=True)
    for name, vals in (("a", lo * 40), ("b", hi * 40)):
        pq.write_table(
            pa.table({"v": pa.array(vals, dtype),
                      "n": pa.array(list(range(len(vals))), pa.int64())}),
            os.path.join(ds, f"{name}.parquet"), row_group_size=30)

    for lit, expected in ((0, 240), (lo[-1], 120), (mid, 40)):
        session = opteryx.session()
        rows = sum(m.num_rows for m in
                   session.execute_to_morsels("SELECT n FROM '%s' WHERE v > %d" % (ds, lit)))
        assert rows == expected, f"v > {lit}: got {rows}, expected {expected}"


def test_mixed_widths_one_scan(tmp_path, monkeypatch):
    """All widths in a single scan (projection + predicate over several), proving the
    per-column parallel packing (kinds/string_types/decimal_columns/logical_coerce)
    stays aligned when widths are interleaved."""
    cols = {name: (typ, vals * 30) for name, (typ, vals) in _WIDTHS.items()}
    proj = ", ".join(_WIDTHS)
    # Projection of all widths together → native.
    _assert_parity(tmp_path, monkeypatch, cols, proj)
    # Predicate over SIGNED columns only → native (all widths still projected).
    _assert_parity(tmp_path, monkeypatch, cols,
                   "%s WHERE int16 < 100 AND int32 > -10" % proj)
    # A predicate touching an UNSIGNED column → fail closed to the trampoline.
    _assert_parity(tmp_path, monkeypatch, cols,
                   "%s WHERE uint16 > 0 AND int16 < 100" % proj, expect_native=False)


# ── real ClickBench columns (annotated INTEGER: int16 / uint16) ───────────────

_TINY = "testdata.clickbench_tiny"


@pytest.mark.parametrize("sql", [
    "SELECT EventDate FROM %s" % _TINY,               # uint16 projected
    "SELECT AdvEngineID FROM %s" % _TINY,             # int16 (signed narrow)
    "SELECT ResolutionWidth FROM %s" % _TINY,         # int16
    "SELECT AdvEngineID FROM %s WHERE AdvEngineID <> 0" % _TINY,   # signed pred input
    "SELECT UserID FROM %s WHERE AdvEngineID <> 0" % _TINY,        # signed role-3
    "SELECT EventDate, AdvEngineID FROM %s WHERE AdvEngineID <> 0" % _TINY,  # uint projected + signed pred
])
def test_clickbench_annotated_int_columns_native(sql):
    """The real annotated-INTEGER columns select the native scan (the A0 footer_gate
    census population). Unsigned EventDate goes native when projected/role-agnostic;
    signed AdvEngineID/ResolutionWidth go native in every role including predicate."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        _ = morsel.num_rows
    sources = list(session._telemetry.as_dict()["scan_sources"].values())
    assert sources == ["NativeParquetScanSource"], sources


@pytest.mark.parametrize("sql", [
    "SELECT EventDate FROM %s WHERE EventDate > 0" % _TINY,        # uint16 pred input
    "SELECT UserID FROM %s WHERE EventDate > 0" % _TINY,          # uint16 role-3
])
def test_clickbench_unsigned_predicate_input_native(sql):
    """An UNSIGNED column used as a c-native predicate input now goes NATIVE: the
    literal is re-materialized at the column's declared width and draken_compare_dv
    dispatches the unsigned compare kernel. This was the A1 fail-closed case."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        _ = morsel.num_rows
    sources = list(session._telemetry.as_dict()["scan_sources"].values())
    assert sources == ["NativeParquetScanSource"], sources


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
