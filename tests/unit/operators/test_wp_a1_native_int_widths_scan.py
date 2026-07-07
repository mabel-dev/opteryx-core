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


# Signed narrow ints widen to INT64 on decode, so the relocated c-native ExprFilter's
# bytecode VM reads them correctly → they go native as predicate inputs. UNSIGNED
# columns decode to exact-width DK_UINT vectors the VM cannot read (err_op=11), so an
# unsigned PREDICATE INPUT deliberately FAILS CLOSED to the trampoline (WP-11 pattern;
# the uint compare kernel is out-of-scope follow-on). Either way the survivor set must
# match byte-for-byte.
def _is_unsigned(width):
    return width.startswith("uint")


# uint32/uint64 as a PUSHED-PREDICATE input trip a PRE-EXISTING, unrelated trampoline
# latmat pass-1 bug (`set_pass1_predicate` on None — parquet_read.pyx, untouched by
# A1) that crashes even a forced-trampoline run (int64 / uint8 / uint16 are fine). Since
# the A1 fail-closed sends them to that trampoline, they cannot be drained end-to-end;
# their fail-closed DECISION is asserted at the footer level in
# test_wide_unsigned_predicate_input_fails_closed below. No real ClickBench column is
# uint32/uint64, so the pre-existing bug does not touch the footer_gate census.
_PRED_DRAINABLE = ["int8", "int16", "int32", "int64", "uint8", "uint16"]


@pytest.mark.parametrize("width", _PRED_DRAINABLE)
def test_int_width_predicate_input(width, tmp_path, monkeypatch):
    """The narrow/unsigned column is a c-native predicate INPUT and also projected.
    Signed → native (widened to INT64, VM-readable). Unsigned → fail closed to the
    trampoline. Survivor set identical to the forced-trampoline run regardless."""
    typ, vals = _WIDTHS[width]
    cols = {"v": (typ, vals * 30), "n": (pa.int64(), list(range(len(vals) * 30)))}
    native = not _is_unsigned(width)
    _assert_parity(tmp_path, monkeypatch, cols, "v, n WHERE v > 0", expect_native=native)
    _assert_parity(tmp_path, monkeypatch, cols, "v WHERE v = 1", expect_native=native)


@pytest.mark.parametrize("width", _PRED_DRAINABLE)
def test_int_width_role3_filter_only(width, tmp_path, monkeypatch):
    """Role-3: the narrow/unsigned column is read only as a predicate input (not
    emitted). Same signed=native / unsigned=fail-closed split as predicate_input."""
    typ, vals = _WIDTHS[width]
    cols = {"v": (typ, vals * 30), "n": (pa.int64(), list(range(len(vals) * 30)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE v > 0",
                   expect_native=not _is_unsigned(width))


@pytest.mark.parametrize("width", ["uint8", "uint16", "uint32", "uint64"])
def test_wide_unsigned_predicate_input_fails_closed(width, tmp_path):
    """The A1 fail-closed DECISION for an unsigned predicate input, asserted at the
    footer level (`any_column_unsigned`) — the plan-time signal that keeps the scan on
    the trampoline. Covers uint32/uint64, which cannot be drained end-to-end due to the
    pre-existing latmat bug noted above."""
    import glob
    from opteryx.connectors.parquet_io.pool_reader import any_column_unsigned
    typ, vals = _WIDTHS[width]
    ds = _write(str(tmp_path / "a1u"), {"v": (typ, vals * 4),
                                        "n": (pa.int64(), list(range(len(vals) * 4)))})
    paths = glob.glob(os.path.join(ds, "*.parquet"))
    # The unsigned column is flagged (→ fail closed); a plain int64 column is not.
    assert any_column_unsigned(paths, ["v"], None) is True
    assert any_column_unsigned(paths, ["n"], None) is False


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
def test_clickbench_unsigned_predicate_input_fails_closed(sql):
    """An UNSIGNED column used as a c-native predicate input deliberately stays on the
    trampoline (the ExprFilter VM cannot read a UINT vector — err_op=11). This is the
    documented A1 fail-closed case; the trampoline evaluates the predicate correctly."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        _ = morsel.num_rows
    sources = list(session._telemetry.as_dict()["scan_sources"].values())
    assert sources == ["StreamingScanSource"], sources


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
