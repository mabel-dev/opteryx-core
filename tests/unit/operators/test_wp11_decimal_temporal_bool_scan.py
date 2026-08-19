"""WP-11 — admit DECIMAL / DATE / TIMESTAMP / TIME / BOOL to the native parquet scan.

These types complete the common-type coverage of NativeParquetScanSource (WP-01
added strings; WP-02 relocated predicates). A projected — or filter-only — decimal /
temporal / boolean column now decodes natively (no GIL trampoline) and is retagged
in-scan to its exact logical type, byte-identically to the trampoline scan's
`_coerce_vectors`:

  * DATE      → DRAKEN_DATE32   (int64→int32 narrow, no descriptor)
  * TIMESTAMP → DRAKEN_TIMESTAMP64 + LogicalType{unit}
  * TIME      → DRAKEN_TIME32/64 + LogicalType{unit}
  * DECIMAL   → DRAKEN_DECIMAL (int64-backed, +precision/scale) or DRAKEN_DECIMAL128
                (int128, descriptor from the footer)
  * BOOL      → DRAKEN_BOOL

The correctness gate is A/B PARITY: the native path must produce the same survivor
set as the forced-trampoline path — values AND the full logical descriptor
(DrakenType tag + timestamp unit + decimal precision/scale). The per-column
signature folds the descriptor in, so a silently rescaled decimal or a unit-shifted
timestamp changes the signature and fails the test. Comparison is order-insensitive
(a filtered/concurrent scan legitimately reorders row groups).

TIME columns are admitted too, but note opteryx's binder decodes parquet TIME as
plain INT64 (it models no TIME logical type from a scan) — so a time column goes
native decoded as INT64, byte-identically to the trampoline. True TIME logical
typing is a separate binder change, out of WP-11's scan-admission scope.

Not covered here: pyarrow writes DECIMAL as FIXED_LEN_BYTE_ARRAY, and rugo cannot
decode FLBA decimal128 with precision > 18 on EITHER the native or the trampoline
path (a pre-existing rugo limitation — both raise the same decode error). So the
decimal tests use precision ≤ 18 (which decode fine, as int128-backed DK_DECIMAL128
with the footer descriptor). The int64-backed DK_POOL decimal path is exercised by
the TPC-H suite (rugo-written int64 decimals), not by pyarrow.

See docs/WP02_PREDICATE_RELOCATION_DESIGN.md for the column-role model this composes
with.
"""

import datetime
import decimal
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
import opteryx.config as config
from opteryx.connectors.parquet_io import pool_reader

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../dev"))
import instrument_engine as IE  # noqa: E402


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
    """Full logical signature of a column: DrakenType tag + logical KIND + timestamp
    unit + decimal precision/scale (the out-of-band descriptor the tag alone cannot
    carry). A unit/precision/scale drift changes this even when the raw int payload
    matches.

    `logical_type_kind` is in here because every other field is blind to IPV4: it is
    the one kind that REFINES an already-complete physical type, so an IPv4 column
    and a plain unsigned one share a DrakenType tag, carry no unit and no
    precision/scale, and differ only in the kind. Without it this signature reports
    two different columns as identical — which is exactly how a missing IPV4 arm in
    `_coerce_vectors` survived here undetected (see parquet_read.pyx).
    """
    col = morsel.column(n)
    if col is None:
        return (None, None, None, None, None)
    nb = col._nb
    return (col.type, nb.logical_type_kind, nb.logical_type_unit,
            nb.logical_type_precision, nb.logical_type_scale)


def _drain(sql, force_trampoline, monkeypatch):
    """Drain `sql`; return ((per-column signature tuple, sorted row multiset), sources).
    The signature folds the logical descriptor in so a wrong unit/scale is caught even
    when values coincide; the row multiset folds each value's repr per column."""
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
    src = list(session.telemetry["scan_sources"].values())
    if force_trampoline:
        monkeypatch.undo()
    return (sig, tuple(sorted(rows))), src


def _assert_parity(tmp_path, monkeypatch, columns, sql_tail, *, write_kw=None,
                   expect_native=True):
    """Write `columns`, run `SELECT {sql_tail}` native and forced-trampoline, assert
    identical survivor set + descriptor. When `expect_native`, also assert the native
    run selected NativeParquetScanSource (and the forced run did not)."""
    ds = _write(str(tmp_path / "wp11"), columns, **(write_kw or {}))
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
    return nat[0], nat[1]  # (signature, sorted rows)


# ── IPV4 ─────────────────────────────────────────────────────────────────────
#
# IPV4 is the one logical kind with NO physical tag of its own — it is
# DRAKEN_UINT32 plus a descriptor — so a path that forgets to attach it returns a
# perfectly well-formed unsigned integer column and nothing downstream can tell.
# That is how the trampoline's single-pass `_coerce_vectors` shipped without an
# IPV4 arm while its name-keyed twin `_coerce_logical_types` had one: the native
# path retagged (LC_IPV4), the trampoline did not, and any query that failed the
# native scan's footer gate served addresses as integers all the way to the API.
# Measured on home.network.netflow, 2026-08-19.
#
# The file is written by rugo rather than pyarrow because parquet has no IPv4
# logical type: the kind travels in rugo's key-value side channel, which is also
# what lets the footer-derived schema declare the column IPV4 with no catalog in
# the picture.

def _write_ipv4(dataset_dir):
    """One parquet file with an IPV4 column beside a plain UINT32 control column.

    The control column matters: both are physically UINT32 with identical values,
    so anything that retags by physical type rather than by the declared descriptor
    turns the control into an address too, and this catches it.
    """
    import draken.draken_native as dn
    import rugo.parquet as rp
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    addresses = [0x7F000001, 0x0A000001, 0xC0A80101, 0xFFFFFFFF]
    morsel = Morsel.from_vectors(
        ["addr", "plain", "n"],
        [
            Vector(dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(addresses))),
            Vector(dn.vector_uint32_from_sequence(addresses)),
            Vector(dn.vector_from_sequence([1, 2, 3, 4])),
        ],
    )
    os.makedirs(dataset_dir, exist_ok=True)
    with open(os.path.join(dataset_dir, "part.parquet"), "wb") as handle:
        handle.write(rp.write_parquet(morsel, compression="none"))
    return dataset_dir


def _ipv4_parity(tmp_path, monkeypatch, sql_tail):
    """Run `SELECT {sql_tail}` native and forced-trampoline over the IPv4 fixture."""
    ds = _write_ipv4(str(tmp_path / "ipv4"))
    proj, _, where = sql_tail.partition(" WHERE ")
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where
    nat, _ = _drain(sql, False, monkeypatch)
    tmp, _ = _drain(sql, True, monkeypatch)
    assert nat == tmp, "native survivor set / descriptor differs from trampoline"
    return dict(nat[0]), nat[1]


def test_ipv4_projection_keeps_its_descriptor(tmp_path, monkeypatch):
    """Both scan paths return IPV4, not a bare UINT32.

    Parity alone is not enough here: before the fix the two paths DISAGREED, but
    two paths that both dropped the descriptor would agree and still be wrong. So
    assert the kind explicitly, on top of parity.
    """
    sig, rows = _ipv4_parity(tmp_path, monkeypatch, "addr, plain, n")

    from draken.draken_native import LogicalKind
    assert sig[b"addr"][1] == LogicalKind.IPV4, sig
    # The control column is the same bits with no descriptor and must stay that way.
    assert sig[b"plain"][1] is None, sig
    assert sig[b"addr"][0] == sig[b"plain"][0], "both are physically UINT32"


def test_ipv4_renders_dotted_quad(tmp_path, monkeypatch):
    """The descriptor is load-bearing for the VALUE, not just the label: an IPv4
    column renders dotted-decimal while the identical uint32 renders an integer.
    This is the assertion the `<<=` probe cannot make (it is rewritten to an
    integer range compare and never touches the type)."""
    _, rows = _ipv4_parity(tmp_path, monkeypatch, "addr, plain")
    addrs = sorted(row[0] for row in rows)
    plains = sorted(row[1] for row in rows)
    assert addrs == sorted(
        [repr("127.0.0.1"), repr("10.0.0.1"), repr("192.168.1.1"), repr("255.255.255.255")]
    ), addrs
    assert plains == sorted(
        [repr(0x7F000001), repr(0x0A000001), repr(0xC0A80101), repr(0xFFFFFFFF)]
    ), plains


def test_ipv4_survives_a_predicate(tmp_path, monkeypatch):
    """A filtered scan takes a different route through the coercion plan; the
    descriptor must survive it on both paths."""
    sig, rows = _ipv4_parity(tmp_path, monkeypatch, "addr, n WHERE n > 2")

    from draken.draken_native import LogicalKind
    assert sig[b"addr"][1] == LogicalKind.IPV4, sig
    assert len(rows) == 2, rows


def test_ipv4_declared_by_schema_over_an_unannotated_file(tmp_path, monkeypatch):
    """The netflow shape: the SCHEMA declares IPV4, the FILE says nothing.

    The tests above cannot reach this case. They write the file with rugo, so it
    carries the draken logical kind in its key-value metadata and the scan
    recovers the descriptor from the file alone — which MASKS a missing coercion
    arm. Every file written before that side channel existed (i.e. all stored
    data) carries no annotation, and then the only thing making the column an
    address is the schema-driven retag on whichever path runs.

    So the file here is written by PYARROW as a plain uint32 — genuinely
    unannotated — and the IPV4 declaration is injected at `rugo_to_relation_schema`,
    the seam where the schema for a scanned relation is decided. That is the same
    ColumnType a catalog-declared IPV4 column produces and the same shape
    `_sp_ipv4_col_set` is built from.

    Fails with a bare UINT32 if the single-pass coercion plan has no IPV4 arm.
    """
    import rugo.parquet as rp
    from draken.draken_native import LogicalKind
    from opteryx.connectors import _rugo_schema
    from opteryx.connectors import filesystem_connector
    from opteryx.types import logical_type as _lt

    addresses = [0xC0A804B6, 0x7F000001, 0x0A000001]
    ds = _write(str(tmp_path / "ipv4decl"), {
        "addr": (pa.uint32(), addresses),
        "n": (pa.int64(), [1, 2, 3]),
    })
    with open(os.path.join(ds, "part.parquet"), "rb") as handle:
        meta = rp.read_metadata_from_memoryview(memoryview(handle.read()))
    kinds = {column.name: column.draken_logical_kind for column in meta.schema_columns}
    assert kinds["addr"] == 0, "the fixture must carry NO file annotation"

    original = _rugo_schema.rugo_to_relation_schema

    def declares_ipv4(rugo_metadata, schema_name="parquet_schema"):
        schema = original(rugo_metadata, schema_name=schema_name)
        for column in schema.columns:
            if column.name == "addr":
                column.column_type = _lt.IPV4
        return schema

    monkeypatch.setattr(_rugo_schema, "rugo_to_relation_schema", declares_ipv4)
    monkeypatch.setattr(filesystem_connector, "rugo_to_relation_schema", declares_ipv4,
                        raising=False)
    monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)

    seen = []
    for morsel in opteryx.session().execute_to_morsels(
        "SELECT addr, n FROM '%s'" % ds
    ):
        if morsel.num_rows:
            column = morsel.column("addr")
            seen.append((column._nb.logical_type_kind, column.to_pylist()))
    monkeypatch.undo()

    assert seen, "the scan returned no rows"
    for kind, values in seen:
        assert kind == LogicalKind.IPV4, f"schema-declared IPV4 came back as {kind}"
        assert sorted(values) == sorted(["192.168.4.182", "127.0.0.1", "10.0.0.1"]), values


# ── BOOL ─────────────────────────────────────────────────────────────────────

def test_bool_projection(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False, True, False, True] * 40),
            "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "b, n")


def test_bool_with_nulls(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40)}
    _assert_parity(tmp_path, monkeypatch, cols, "b")


def test_bool_all_null(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [None] * 200)}
    _assert_parity(tmp_path, monkeypatch, cols, "b")


def test_bool_all_constant(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True] * 200)}
    _assert_parity(tmp_path, monkeypatch, cols, "b")


# R5 close-out — a BOOL PREDICATE INPUT is now native too. These two tests were
# `test_bool_predicate_role2_fails_closed` / `test_bool_role3_filter_only_fails_closed`:
# WP-11 fail-closed a bool predicate input because draken_compare_dv's type switch
# had no DRAKEN_BOOL branch, so every bool comparison declined to nullptr and the
# relocated ExprFilter (no fallback) raised err_op=11. draken/ops/bool_compare.h now
# supplies that branch — BOOL is BIT-PACKED, so it needs its own kernel rather than a
# fixed-width instantiation: it reads bit `selection[i]` of the bitmap for each logical
# row (the uniform §11 access path — dense / constant / dict all correct through it),
# orders FALSE < TRUE, and marks a result row NULL when EITHER operand row is NULL.
# _assert_parity is the correctness gate: the native survivor set must equal the
# forced-trampoline survivor set, values and descriptor.


def test_bool_predicate_role2_now_native(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b = true")
    assert len(rows) == 100


def test_bool_predicate_eq_false(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b = false")
    assert len(rows) == 100


def test_bool_predicate_not_equal(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b <> true")
    assert len(rows) == 100


def test_bool_role3_filter_only_now_native(tmp_path, monkeypatch):
    """The BOOL column is READ for the filter but never emitted (role 3) — the
    strictest shape, since a role-3 column must also be native-admissible."""
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = true")
    assert len(rows) == 100


def test_bool_predicate_with_nulls(tmp_path, monkeypatch):
    """A NULL bool row is UNKNOWN, never a survivor, for `= true` OR `= false` —
    the compare_vector null contract (result NULL if EITHER operand is NULL), which
    is what the bit-packed kernel must reproduce over the validity bitmap. 80 TRUE /
    40 FALSE / 80 NULL: the two survivor sets must be disjoint and sum to 120."""
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40),
            "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = true")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = false")
    _, u_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS NULL")
    assert len(t_rows) == 80
    assert len(f_rows) == 40
    assert len(u_rows) == 80
    assert not (set(t_rows) & set(f_rows))
    assert len(t_rows) + len(f_rows) + len(u_rows) == 200


def test_bool_predicate_all_null(tmp_path, monkeypatch):
    """Every row UNKNOWN → no survivors on either polarity."""
    cols = {"b": (pa.bool_(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = true")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = false")
    assert t_rows == () and f_rows == ()


def test_bool_predicate_all_constant(tmp_path, monkeypatch):
    """A single-valued bool column decodes to the CONSTANT shape (data_length == 1,
    selection = the global zero vector). The kernel has no shape discriminant, so
    this must come out through the same uniform bit read."""
    cols = {"b": (pa.bool_(), [True] * 200), "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = true")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = false")
    assert len(t_rows) == 200
    assert f_rows == ()


def test_bool_predicate_composed_with_int(tmp_path, monkeypatch):
    """Bool compare AND int compare in ONE relocated c-native span."""
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols,
                             "b, n WHERE b = true AND n > 100")
    # b is true on even n; n > 100 leaves the even values 102..198 → 49 rows.
    assert len(rows) == 49


def test_bool_predicate_unaligned_tail(tmp_path, monkeypatch):
    """Row count not a multiple of 8 — the bitmap's partial last byte. A kernel that
    wrote past the logical length would show up as phantom survivors."""
    n = 203
    cols = {"b": (pa.bool_(), [i % 3 == 0 for i in range(n)]),
            "n": (pa.int64(), list(range(n)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b = true")
    assert len(rows) == len([i for i in range(n) if i % 3 == 0])


# ---------------------------------------------------------------------------
# `IS TRUE` / `IS FALSE` / `IS NOT TRUE` / `IS NOT FALSE` — the SQL `IS`-predicate
# form, a distinct bytecode opcode (UOP_IS_TRUE et al.) from `= TRUE`/`<> TRUE`
# above, with different NULL semantics: `NULL IS TRUE` is FALSE (never NULL),
# whereas `NULL = TRUE` is NULL (never a survivor). `draken_vm_bool_truth_test`
# (draken/core/bitmap_ops.cpp, over draken/ops/bool_logical.h::bool_truth_test)
# is the never-null kernel; `_dv_unary_bool_test_c` (evaluation.pyx) wires it
# into the nogil VM's BC_UNARY_OP dispatch.
# ---------------------------------------------------------------------------


def test_bool_is_true_predicate_now_native(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b IS TRUE")
    assert len(rows) == 100


def test_bool_is_false_predicate_now_native(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b IS FALSE")
    assert len(rows) == 100


def test_bool_is_not_true_predicate_now_native(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b IS NOT TRUE")
    assert len(rows) == 100


def test_bool_is_not_false_predicate_now_native(tmp_path, monkeypatch):
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "b, n WHERE b IS NOT FALSE")
    assert len(rows) == 100


def test_bool_is_predicate_role3_filter_only_now_native(tmp_path, monkeypatch):
    """The BOOL column is READ for the filter but never emitted (role 3)."""
    cols = {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS TRUE")
    assert len(rows) == 100


def test_bool_is_predicate_with_nulls(tmp_path, monkeypatch):
    """The NULL-collapsing semantics that make IS TRUE/FALSE a DISTINCT opcode from
    `= TRUE`/`= FALSE`: a NULL row is never a survivor for IS TRUE or IS FALSE, but
    IS ALWAYS a survivor for IS NOT TRUE and IS NOT FALSE (unlike `<> TRUE`/`!= FALSE`,
    which are also NULL, never survivors, for a NULL operand). 80 TRUE / 40 FALSE /
    80 NULL out of 200."""
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40),
            "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS TRUE")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS FALSE")
    _, nt_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS NOT TRUE")
    _, nf_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS NOT FALSE")
    assert len(t_rows) == 80
    assert len(f_rows) == 40
    assert len(nt_rows) == 120   # FALSE ∪ NULL
    assert len(nf_rows) == 160   # TRUE ∪ NULL
    assert not (set(t_rows) & set(f_rows))
    # every NULL row survives BOTH negated forms; every non-NULL row survives exactly one
    null_rows = set(nt_rows) & set(nf_rows)
    assert len(null_rows) == 80
    assert set(nt_rows) == set(f_rows) | null_rows
    assert set(nf_rows) == set(t_rows) | null_rows
    assert set(t_rows) | set(f_rows) | null_rows == set(t_rows) | set(nt_rows) | set(nf_rows)
    assert len(set(t_rows) | set(f_rows) | null_rows) == 200


def test_bool_is_predicate_all_null(tmp_path, monkeypatch):
    """Every row NULL → IS TRUE/FALSE have no survivors; IS NOT TRUE/IS NOT FALSE
    survive on EVERY row (unlike `<> TRUE`/`!= FALSE`, which stay NULL too)."""
    cols = {"b": (pa.bool_(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS TRUE")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS FALSE")
    _, nt_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS NOT TRUE")
    _, nf_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS NOT FALSE")
    assert t_rows == () and f_rows == ()
    assert len(nt_rows) == 200 and len(nf_rows) == 200


def test_bool_is_predicate_all_constant(tmp_path, monkeypatch):
    """A single-valued bool column decodes to the CONSTANT shape (data_length == 1,
    selection = the global zero vector) — the kernel has no shape discriminant, so
    this must come out through the same uniform bit read as bool_and/bool_or."""
    cols = {"b": (pa.bool_(), [True] * 200), "n": (pa.int64(), list(range(200)))}
    _, t_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS TRUE")
    _, f_rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS FALSE")
    assert len(t_rows) == 200
    assert f_rows == ()


def test_bool_is_predicate_unaligned_tail(tmp_path, monkeypatch):
    """Row count not a multiple of 8 — the bitmap's partial last byte. A kernel that
    wrote past the logical length would show up as phantom survivors."""
    n = 203
    cols = {"b": (pa.bool_(), [i % 3 == 0 for i in range(n)]),
            "n": (pa.int64(), list(range(n)))}
    _, rows = _assert_parity(tmp_path, monkeypatch, cols, "n WHERE b IS TRUE")
    assert len(rows) == len([i for i in range(n) if i % 3 == 0])


def test_bool_is_true_projection(tmp_path, monkeypatch):
    """IS TRUE as a PROJECTED expression (not a predicate) — exercises the same
    opcode through ExprMultiProjectOperator / `bytecode_ops_all_c_native`'s
    projection-eligibility path rather than the Filter-node predicate path."""
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40)}
    _assert_parity(tmp_path, monkeypatch, cols, "b IS TRUE AS t, b IS FALSE AS f")


# ── DATE ─────────────────────────────────────────────────────────────────────

def _dates(n=200):
    base = datetime.date(2000, 1, 1)
    return [base + datetime.timedelta(days=i) for i in range(n)]


def test_date_projection(tmp_path, monkeypatch):
    cols = {"d": (pa.date32(), _dates()), "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "d, n")


def test_date_with_nulls(tmp_path, monkeypatch):
    ds = _dates(200)
    ds[3] = ds[7] = ds[199] = None
    cols = {"d": (pa.date32(), ds)}
    _assert_parity(tmp_path, monkeypatch, cols, "d")


def test_date_epoch_and_boundary(tmp_path, monkeypatch):
    cols = {"d": (pa.date32(), [datetime.date(1970, 1, 1), datetime.date(1900, 1, 1),
                                datetime.date(2262, 4, 11), datetime.date(9999, 12, 31)] * 20)}
    _assert_parity(tmp_path, monkeypatch, cols, "d")


def test_date_role3_filter_only(tmp_path, monkeypatch):
    cols = {"d": (pa.date32(), _dates()), "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n > 100")


# ── TIMESTAMP (multiple units, boundaries) ───────────────────────────────────

def _timestamps(n=200):
    base = datetime.datetime(2020, 1, 1, 12, 0, 0)
    return [base + datetime.timedelta(seconds=i * 37) for i in range(n)]


@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_timestamp_projection_units(tmp_path, monkeypatch, unit):
    # The engine canonicalizes the timestamp column to its SCHEMA unit (both paths
    # identically) — _assert_parity's signature folds the emitted unit in, so this
    # confirms native and trampoline agree on unit + values for every file unit.
    # 'ns' is excluded: an ns timestamp overflows the engine's value display on BOTH
    # paths (a pre-existing unit-handling issue, not a WP-11 scan concern).
    cols = {"t": (pa.timestamp(unit), _timestamps()), "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "t, n")


def test_timestamp_with_nulls(tmp_path, monkeypatch):
    ts = _timestamps(200)
    ts[1] = ts[50] = ts[199] = None
    cols = {"t": (pa.timestamp("us"), ts)}
    _assert_parity(tmp_path, monkeypatch, cols, "t")


def test_timestamp_epoch_and_boundary(tmp_path, monkeypatch):
    cols = {"t": (pa.timestamp("us"), [
        datetime.datetime(1970, 1, 1, 0, 0, 0),
        datetime.datetime(1900, 1, 1, 0, 0, 0),
        datetime.datetime(2262, 1, 1, 0, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59),
    ] * 20)}
    _assert_parity(tmp_path, monkeypatch, cols, "t")


def test_timestamp_all_constant(tmp_path, monkeypatch):
    cols = {"t": (pa.timestamp("ms"), [datetime.datetime(2021, 6, 6, 6, 6, 6)] * 200)}
    _assert_parity(tmp_path, monkeypatch, cols, "t")


def test_timestamp_role3_filter_only(tmp_path, monkeypatch):
    cols = {"t": (pa.timestamp("us"), _timestamps()), "n": (pa.int64(), list(range(200)))}
    _assert_parity(tmp_path, monkeypatch, cols, "n WHERE n < 50")


# ── TIME (32 = ms, 64 = us/ns) ───────────────────────────────────────────────

def _times(n=200):
    return [datetime.time((i * 7) % 24, (i * 11) % 60, (i * 13) % 60) for i in range(n)]


def test_time32_ms_projection(tmp_path, monkeypatch):
    cols = {"tm": (pa.time32("ms"), _times())}
    _assert_parity(tmp_path, monkeypatch, cols, "tm")


@pytest.mark.parametrize("unit", ["us", "ns"])
def test_time64_projection_units(tmp_path, monkeypatch, unit):
    cols = {"tm": (pa.time64(unit), _times())}
    _assert_parity(tmp_path, monkeypatch, cols, "tm")


def test_time_with_nulls(tmp_path, monkeypatch):
    tms = _times(200)
    tms[2] = tms[99] = None
    cols = {"tm": (pa.time64("us"), tms)}
    _assert_parity(tmp_path, monkeypatch, cols, "tm")


# ── DECIMAL (varied precision/scale, negatives, zero, max precision) ──────────

def _decimals(precision, scale, n=200):
    q = decimal.Decimal(1).scaleb(-scale)
    out = []
    for i in range(n):
        v = decimal.Decimal((i - n // 2) * 3) + decimal.Decimal(i) / decimal.Decimal(100)
        out.append(v.quantize(q))
    return out


@pytest.mark.parametrize("precision,scale", [(5, 2), (10, 0), (18, 6)])
def test_decimal_projection(tmp_path, monkeypatch, precision, scale):
    # precision ≤ 18: rugo decodes the pyarrow FLBA decimal as int128 DK_DECIMAL128;
    # the descriptor (precision/scale) comes from the footer, both paths.
    cols = {"d": (pa.decimal128(precision, scale), _decimals(precision, scale)),
            "n": (pa.int64(), list(range(200)))}
    sig, _ = _assert_parity(tmp_path, monkeypatch, cols, "d, n")
    # _col_sig is (type, kind, unit, precision, scale) — precision/scale are [3]/[4].
    assert sig[0][1][3] == precision and sig[0][1][4] == scale, sig


def test_decimal_with_nulls(tmp_path, monkeypatch):
    ds = _decimals(18, 6, 200)
    ds[0] = ds[100] = ds[199] = None
    cols = {"d": (pa.decimal128(18, 6), ds)}
    _assert_parity(tmp_path, monkeypatch, cols, "d")


def test_decimal_zero_and_negative(tmp_path, monkeypatch):
    cols = {"d": (pa.decimal128(12, 3), [
        decimal.Decimal("0.000"), decimal.Decimal("-1.500"),
        decimal.Decimal("-999999.999"), decimal.Decimal("999999.999"),
    ] * 50)}
    _assert_parity(tmp_path, monkeypatch, cols, "d")


def test_decimal_all_constant(tmp_path, monkeypatch):
    cols = {"d": (pa.decimal128(9, 2), [decimal.Decimal("12.34")] * 200)}
    _assert_parity(tmp_path, monkeypatch, cols, "d")


def test_decimal_predicate_role2(tmp_path, monkeypatch):
    cols = {"d": (pa.decimal128(10, 2), [decimal.Decimal(i) / 4 for i in range(200)]),
            "n": (pa.int64(), list(range(200)))}
    # parity is the invariant; source is informational (predicate may or may not lower).
    _assert_parity(tmp_path, monkeypatch, cols, "d, n WHERE d > 10.0", expect_native=False)


# ── mixed decimal + timestamp + bool in one scan ─────────────────────────────

def test_mixed_decimal_timestamp_bool(tmp_path, monkeypatch):
    cols = {
        "d": (pa.decimal128(18, 4), _decimals(18, 4)),
        "t": (pa.timestamp("us"), _timestamps()),
        "b": (pa.bool_(), [True, False] * 100),
        "n": (pa.int64(), list(range(200))),
    }
    _assert_parity(tmp_path, monkeypatch, cols, "d, t, b, n")


# ── fail-closed: a genuinely unadmitted type stays on the trampoline ─────────

def test_projected_uint_now_native(tmp_path, monkeypatch):
    """UINT was WP-11's deferred "separate follow-on"; A1 landed it. A PROJECTED uint
    column now decodes on the native scan (exact-width DRAKEN_UINT*, byte-identical to
    the trampoline), so this scan selects NativeParquetScanSource. (An unsigned column
    used as a c-native PREDICATE INPUT still fails closed — covered by the A1 suite
    test_wp_a1_native_int_widths_scan; here it is projection-only, so it goes native.)"""
    cols = {"u": (pa.uint32(), list(range(200))), "n": (pa.int64(), list(range(200)))}
    ds = _write(str(tmp_path / "fc"), cols)
    sql = "SELECT u, n FROM '%s'" % ds

    nat, nat_src = _drain(sql, False, monkeypatch)
    tmp, tmp_src = _drain(sql, True, monkeypatch)

    assert nat_src == ["NativeParquetScanSource"], nat_src
    assert tmp_src == ["StreamingScanSource"], tmp_src
    assert nat == tmp


# ── instrumentation: zero-Python on the admitted decimal/timestamp scan ──────

def test_instrumentation_decimal_timestamp_zero_gil(tmp_path, monkeypatch):
    cols = {"d": (pa.decimal128(18, 4), _decimals(18, 4)),
            "t": (pa.timestamp("us"), _timestamps())}
    ds = _write(str(tmp_path / "instr"), cols)
    sql = "SELECT d, t FROM '%s'" % ds

    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    td = session.telemetry

    assert list(td["scan_sources"].values()) == ["NativeParquetScanSource"]
    assert td["gil_held_ns"] == 0
    assert td.get("worker_gil_sites", []) == []
    IE.assert_native_worker_purity(td, whitelist=())


def test_instrumentation_trampoline_calls_zero(tmp_path, monkeypatch):
    cols = {"d": (pa.decimal128(18, 4), _decimals(18, 4)),
            "t": (pa.timestamp("us"), _timestamps())}
    ds = _write(str(tmp_path / "instr2"), cols)
    sql = "SELECT d, t FROM '%s'" % ds
    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    res = IE.measure_query_allocations(sql)
    assert res["trampoline_calls"] == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
