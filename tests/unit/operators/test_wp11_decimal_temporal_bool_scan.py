"""WP-11 — DECIMAL / DATE / TIMESTAMP / TIME / BOOL / IPV4 on the native parquet scan.

These types complete the common-type coverage of NativeParquetScanSource (WP-01
added strings; WP-02 relocated predicates). A projected — or filter-only — decimal /
temporal / boolean column decodes natively and is retagged in-scan to its exact
logical type:

  * DATE      → DRAKEN_DATE32   (no descriptor)
  * TIMESTAMP → DRAKEN_TIMESTAMP64 + LogicalKind.TIMESTAMP + the FILE's unit
                (parquet has no seconds unit — pyarrow writes `timestamp[s]` as ms)
  * TIME      → the binder declares TIME[us]; the engine's TIME value (as CAST
                produces it) is INT64 + LogicalKind.TIME + unit us, microseconds
                since midnight
  * DECIMAL   → DRAKEN_DECIMAL (int64-backed, precision ≤ 18) or DRAKEN_DECIMAL128
                (precision > 18), + LogicalKind.DECIMAL + precision/scale
  * BOOL      → DRAKEN_BOOL
  * IPV4      → DRAKEN_UINT32 + LogicalKind.IPV4

There is no fallback scan (ruled 2026-10-03): a scan the native Source cannot read
is refused. So the correctness gate is an INDEPENDENT plain-Python oracle: the
values the test itself wrote, with any WHERE predicate evaluated in Python under SQL
three-valued logic. The survivor multiset must match, AND every output column's
logical descriptor (DrakenType tag + logical kind + timestamp unit + decimal
precision/scale) must equal an explicit expected value — a silently rescaled decimal
or a unit-shifted timestamp fails even when the raw payload coincides. Comparison is
order-insensitive (a filtered/concurrent scan legitimately reorders row groups).

Every scan must also select NativeParquetScanSource (a refused scan raises).

See docs/WP02_PREDICATE_RELOCATION_DESIGN.md for the column-role model this composes
with.
"""

import collections
import datetime
import decimal
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/) — WRITES the fixtures only
import pyarrow.parquet as pq
import pytest
from draken.draken_native import DrakenType, LogicalKind

import opteryx

_UTC = datetime.timezone.utc

#: Field wildcard for `_expect`. Used ONLY for the unit slot of a DECIMAL / IPV4
#: descriptor: the descriptor struct always carries a unit field and it defaults to
#: "us" for kinds that have no unit (a `CAST(... AS DECIMAL(5,2))` literal reports the
#: same), so it is meaningless for those kinds and not asserted.
_ANY = object()


def _expect(tag, kind=None, unit=None, precision=None, scale=None):
    """Expected descriptor tuple, same field order as `_col_sig`."""
    return (tag, kind, unit, precision, scale)


_INT64 = _expect(DrakenType.INT64)
_UINT32 = _expect(DrakenType.UINT32)
_BOOL = _expect(DrakenType.BOOL)
_DATE = _expect(DrakenType.DATE32)
_TIME = _expect(DrakenType.INT64, LogicalKind.TIME, "us")
_IPV4 = _expect(DrakenType.UINT32, LogicalKind.IPV4, _ANY)


def _ts(unit):
    return _expect(DrakenType.TIMESTAMP64, LogicalKind.TIMESTAMP, unit)


def _dec(precision, scale):
    tag = DrakenType.DECIMAL if precision <= 18 else DrakenType.DECIMAL128
    return _expect(tag, LogicalKind.DECIMAL, _ANY, precision, scale)


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
    precision/scale, and differ only in the kind.
    """
    col = morsel.column(n)
    nb = col._nb
    return (col.type, nb.logical_type_kind, nb.logical_type_unit,
            nb.logical_type_precision, nb.logical_type_scale)


def _drain(sql):
    """Drain `sql` natively; return (per-column signature dict, row Counter, session).
    Rows are tuples of Python values in projection order."""
    session = opteryx.session()
    rows = collections.Counter()
    sig = None
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        if sig is None:
            sig = {n.decode(): _col_sig(morsel, n) for n in names}
        cols = [morsel.column(n).to_pylist() for n in names]
        for i in range(morsel.num_rows):
            rows[tuple(c[i] for c in cols)] += 1
    return sig, rows, session


def _assert_native(session):
    assert list(session.telemetry["scan_sources"].values()) == ["NativeParquetScanSource"], (
        session.telemetry["scan_sources"])


def _assert_sig(sig, expected_sig):
    assert sig is not None, "the scan emitted no morsel to read a descriptor from"
    assert list(sig) == list(expected_sig), (list(sig), list(expected_sig))
    for name, want in expected_sig.items():
        got = sig[name]
        for field, (g, w) in enumerate(zip(got, want)):
            if w is not _ANY:
                assert g == w, "column %r descriptor field %d: got %r, expected %r (full %r)" % (
                    name, field, g, w, got)


def _run_and_check(sql, expected_rows, expected_sig):
    """Drain `sql`, assert native Source + no residual, the survivor multiset equals
    `expected_rows`, and (when any morsel was emitted) the descriptor equals
    `expected_sig`. Returns the survivor Counter."""
    sig, rows, session = _drain(sql)
    _assert_native(session)
    expected = collections.Counter(expected_rows)
    if expected or sig is not None:
        _assert_sig(sig, expected_sig)
    missing = expected - rows
    extra = rows - expected
    assert not missing and not extra, (
        "survivor set differs from the Python oracle: missing %r, extra %r"
        % (sorted(missing.items(), key=repr)[:10], sorted(extra.items(), key=repr)[:10]))
    return rows


def _oracle_rows(columns, project, keep=None, normalize=None):
    """Plain-Python oracle over the values the test wrote. `project` = row-dict →
    output tuple; `keep` = row-dict → bool (SQL WHERE under
    three-valued logic: UNKNOWN is not a survivor); `normalize` = {name: fn} applied
    to the written value to give the engine's representation of it."""
    names = list(columns)
    n = len(columns[names[0]][1])
    normalize = normalize or {}
    out = []
    for i in range(n):
        row = {}
        for name in names:
            v = columns[name][1][i]
            fn = normalize.get(name)
            row[name] = v if (v is None or fn is None) else fn(v)
        if keep is not None and not keep(row):
            continue
        out.append(project(row))
    return out


def _check(tmp_path, columns, proj, expected_sig, *, keep=None, where=None,
           normalize=None, write_kw=None, project=None):
    """Write `columns`, run `SELECT {proj} FROM ds [WHERE {where}]`, check it against
    the oracle. `proj` is the SQL projection; by default the oracle projects the same
    column names, and `project` (row-dict → tuple) overrides that for expressions."""
    ds = _write(str(tmp_path / "wp11"), columns, **(write_kw or {}))
    if project is None:
        names = [c.strip() for c in proj.split(",")]
        project = lambda r: tuple(r[c] for c in names)  # noqa: E731
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where
    expected = _oracle_rows(columns, project, keep, normalize)
    return _run_and_check(sql, expected, expected_sig)


def _utc(v):
    """pyarrow writes a naive datetime as UTC wall time; the engine returns it
    tz-aware UTC. Oracle normalization only."""
    return v.replace(tzinfo=_UTC)


def _time_us(v):
    """TIME as the engine represents it: microseconds since midnight."""
    return ((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond


# ── IPV4 ─────────────────────────────────────────────────────────────────────
#
# IPV4 is the one logical kind with NO physical tag of its own — it is
# DRAKEN_UINT32 plus a descriptor — so a path that forgets to attach it returns a
# perfectly well-formed unsigned integer column and nothing downstream can tell.
# Measured on home.network.netflow, 2026-08-19.
#
# The file is written by rugo rather than pyarrow because parquet has no IPv4
# logical type: the kind travels in rugo's key-value side channel, which is also
# what lets the footer-derived schema declare the column IPV4 with no catalog in
# the picture.

_IPV4_ADDRESSES = [0x7F000001, 0x0A000001, 0xC0A80101, 0xFFFFFFFF]
_IPV4_DOTTED = ["127.0.0.1", "10.0.0.1", "192.168.1.1", "255.255.255.255"]
_IPV4_N = [1, 2, 3, 4]


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

    morsel = Morsel.from_vectors(
        ["addr", "plain", "n"],
        [
            Vector(dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(_IPV4_ADDRESSES))),
            Vector(dn.vector_uint32_from_sequence(_IPV4_ADDRESSES)),
            Vector(dn.vector_int64_from_sequence(_IPV4_N)),
        ],
    )
    os.makedirs(dataset_dir, exist_ok=True)
    with open(os.path.join(dataset_dir, "part.parquet"), "wb") as handle:
        handle.write(rp.write_parquet(morsel, compression="none"))
    return dataset_dir


def _ipv4_sql(tmp_path, sql_tail):
    ds = _write_ipv4(str(tmp_path / "ipv4"))
    proj, _, where = sql_tail.partition(" WHERE ")
    sql = "SELECT %s FROM '%s'" % (proj, ds)
    if where:
        sql += " WHERE %s" % where
    return sql


def test_ipv4_projection_keeps_its_descriptor(tmp_path):
    """IPV4 comes back as IPV4, not a bare UINT32; the same-bits control column
    carries no descriptor and must stay that way."""
    expected = list(zip(_IPV4_DOTTED, _IPV4_ADDRESSES, _IPV4_N))
    _run_and_check(_ipv4_sql(tmp_path, "addr, plain, n"), expected,
                   {"addr": _IPV4, "plain": _UINT32, "n": _INT64})


def test_ipv4_renders_dotted_quad(tmp_path):
    """The descriptor is load-bearing for the VALUE, not just the label: an IPv4
    column renders dotted-decimal while the identical uint32 renders an integer.
    This is the assertion the `<<=` probe cannot make (it is rewritten to an
    integer range compare and never touches the type)."""
    expected = list(zip(_IPV4_DOTTED, _IPV4_ADDRESSES))
    _run_and_check(_ipv4_sql(tmp_path, "addr, plain"), expected,
                   {"addr": _IPV4, "plain": _UINT32})


def test_ipv4_survives_a_predicate(tmp_path):
    """A filtered scan takes a different route through the coercion plan; the
    descriptor must survive it."""
    expected = [(a, n) for a, n in zip(_IPV4_DOTTED, _IPV4_N) if n > 2]
    rows = _run_and_check(_ipv4_sql(tmp_path, "addr, n WHERE n > 2"), expected,
                          {"addr": _IPV4, "n": _INT64})
    assert sum(rows.values()) == 2, rows


def test_ipv4_declared_by_schema_over_an_unannotated_file(tmp_path, monkeypatch):
    """The netflow shape: the SCHEMA declares IPV4, the FILE says nothing.

    The tests above cannot reach this case. They write the file with rugo, so it
    carries the draken logical kind in its key-value metadata and the scan
    recovers the descriptor from the file alone — which MASKS a missing coercion
    arm. Every file written before that side channel existed (i.e. all stored
    data) carries no annotation, and then the only thing making the column an
    address is the schema-driven retag in the scan.

    So the file here is written by PYARROW as a plain uint32 — genuinely
    unannotated — and the IPV4 declaration is injected at `rugo_to_relation_schema`,
    the seam where the schema for a scanned relation is decided. That is the same
    ColumnType a catalog-declared IPV4 column produces.
    """
    import rugo.parquet as rp
    from opteryx.connectors import _rugo_schema
    from opteryx.connectors import filesystem_connector
    from opteryx.types import logical_type as _lt

    addresses = [0xC0A804B6, 0x7F000001, 0x0A000001]
    columns = {
        "addr": (pa.uint32(), addresses),
        "n": (pa.int64(), [1, 2, 3]),
    }
    ds = _write(str(tmp_path / "ipv4decl"), columns)
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

    expected = [("192.168.4.182", 1), ("127.0.0.1", 2), ("10.0.0.1", 3)]
    _run_and_check("SELECT addr, n FROM '%s'" % ds, expected,
                   {"addr": _IPV4, "n": _INT64})


# ── BOOL ─────────────────────────────────────────────────────────────────────

def _b_eq(want):
    """`b = want` — NULL compares UNKNOWN, never a survivor."""
    return lambda r: r["b"] is not None and r["b"] == want


def _b_ne(want):
    return lambda r: r["b"] is not None and r["b"] != want


def _b_is(want):
    """`b IS want` — never UNKNOWN: a NULL row is simply not `want`."""
    return lambda r: r["b"] is want


def _b_is_not(want):
    return lambda r: r["b"] is not want


def test_bool_projection(tmp_path):
    cols = {"b": (pa.bool_(), [True, False, True, False, True] * 40),
            "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "b, n", {"b": _BOOL, "n": _INT64})


def test_bool_with_nulls(tmp_path):
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40)}
    _check(tmp_path, cols, "b", {"b": _BOOL})


def test_bool_all_null(tmp_path):
    cols = {"b": (pa.bool_(), [None] * 200)}
    _check(tmp_path, cols, "b", {"b": _BOOL})


def test_bool_all_constant(tmp_path):
    cols = {"b": (pa.bool_(), [True] * 200)}
    _check(tmp_path, cols, "b", {"b": _BOOL})


# A BOOL PREDICATE INPUT is native. draken/ops/bool_compare.h supplies the
# DRAKEN_BOOL branch of draken_compare_dv — BOOL is BIT-PACKED, so it needs its own
# kernel rather than a fixed-width instantiation: it reads bit `selection[i]` of the
# bitmap for each logical row (the uniform §11 access path — dense / constant / dict
# all correct through it), orders FALSE < TRUE, and marks a result row NULL when
# EITHER operand row is NULL.

def _alt():
    return {"b": (pa.bool_(), [True, False] * 100), "n": (pa.int64(), list(range(200)))}


def _nulls5():
    return {"b": (pa.bool_(), [True, None, False, None, True] * 40),
            "n": (pa.int64(), list(range(200)))}


def test_bool_predicate_role2_now_native(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b = true", keep=_b_eq(True))
    assert sum(rows.values()) == 100


def test_bool_predicate_eq_false(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b = false", keep=_b_eq(False))
    assert sum(rows.values()) == 100


def test_bool_predicate_not_equal(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b <> true", keep=_b_ne(True))
    assert sum(rows.values()) == 100


def test_bool_role3_filter_only_now_native(tmp_path):
    """The BOOL column is READ for the filter but never emitted (role 3) — the
    strictest shape, since a role-3 column must also be native-admissible."""
    rows = _check(tmp_path, _alt(), "n", {"n": _INT64},
                  where="b = true", keep=_b_eq(True))
    assert sum(rows.values()) == 100


def test_bool_predicate_with_nulls(tmp_path):
    """A NULL bool row is UNKNOWN, never a survivor, for `= true` OR `= false` —
    the compare_vector null contract (result NULL if EITHER operand is NULL), which
    is what the bit-packed kernel must reproduce over the validity bitmap. 80 TRUE /
    40 FALSE / 80 NULL: the two survivor sets must be disjoint and sum to 120."""
    t_rows = _check(tmp_path / "t", _nulls5(), "n", {"n": _INT64},
                    where="b = true", keep=_b_eq(True))
    f_rows = _check(tmp_path / "f", _nulls5(), "n", {"n": _INT64},
                    where="b = false", keep=_b_eq(False))
    u_rows = _check(tmp_path / "u", _nulls5(), "n", {"n": _INT64},
                    where="b IS NULL", keep=lambda r: r["b"] is None)
    assert sum(t_rows.values()) == 80
    assert sum(f_rows.values()) == 40
    assert sum(u_rows.values()) == 80
    assert not (set(t_rows) & set(f_rows))


def test_bool_predicate_all_null(tmp_path):
    """Every row UNKNOWN → no survivors on either polarity."""
    cols = {"b": (pa.bool_(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    t_rows = _check(tmp_path / "t", cols, "n", {"n": _INT64},
                    where="b = true", keep=_b_eq(True))
    f_rows = _check(tmp_path / "f", cols, "n", {"n": _INT64},
                    where="b = false", keep=_b_eq(False))
    assert not t_rows and not f_rows


def test_bool_predicate_all_constant(tmp_path):
    """A single-valued bool column decodes to the CONSTANT shape (data_length == 1,
    selection = the global zero vector). The kernel has no shape discriminant, so
    this must come out through the same uniform bit read."""
    cols = {"b": (pa.bool_(), [True] * 200), "n": (pa.int64(), list(range(200)))}
    t_rows = _check(tmp_path / "t", cols, "n", {"n": _INT64},
                    where="b = true", keep=_b_eq(True))
    f_rows = _check(tmp_path / "f", cols, "n", {"n": _INT64},
                    where="b = false", keep=_b_eq(False))
    assert sum(t_rows.values()) == 200
    assert not f_rows


def test_bool_predicate_composed_with_int(tmp_path):
    """Bool compare AND int compare in ONE relocated c-native span."""
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b = true AND n > 100",
                  keep=lambda r: r["b"] is True and r["n"] > 100)
    # b is true on even n; n > 100 leaves the even values 102..198 → 49 rows.
    assert sum(rows.values()) == 49


def test_bool_predicate_unaligned_tail(tmp_path):
    """Row count not a multiple of 8 — the bitmap's partial last byte. A kernel that
    wrote past the logical length would show up as phantom survivors."""
    n = 203
    cols = {"b": (pa.bool_(), [i % 3 == 0 for i in range(n)]),
            "n": (pa.int64(), list(range(n)))}
    rows = _check(tmp_path, cols, "n", {"n": _INT64}, where="b = true", keep=_b_eq(True))
    assert sum(rows.values()) == len([i for i in range(n) if i % 3 == 0])


# ---------------------------------------------------------------------------
# `IS TRUE` / `IS FALSE` / `IS NOT TRUE` / `IS NOT FALSE` — the SQL `IS`-predicate
# form, a distinct bytecode opcode (UOP_IS_TRUE et al.) from `= TRUE`/`<> TRUE`
# above, with different NULL semantics: `NULL IS TRUE` is FALSE (never NULL),
# whereas `NULL = TRUE` is NULL (never a survivor). `draken_vm_bool_truth_test`
# (draken/core/bitmap_ops.cpp, over draken/ops/bool_logical.h::bool_truth_test)
# is the never-null kernel; `_dv_unary_bool_test_c` (evaluation.pyx) wires it
# into the nogil VM's BC_UNARY_OP dispatch.
# ---------------------------------------------------------------------------


def test_bool_is_true_predicate_now_native(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b IS TRUE", keep=_b_is(True))
    assert sum(rows.values()) == 100


def test_bool_is_false_predicate_now_native(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b IS FALSE", keep=_b_is(False))
    assert sum(rows.values()) == 100


def test_bool_is_not_true_predicate_now_native(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b IS NOT TRUE", keep=_b_is_not(True))
    assert sum(rows.values()) == 100


def test_bool_is_not_false_predicate_now_native(tmp_path):
    rows = _check(tmp_path, _alt(), "b, n", {"b": _BOOL, "n": _INT64},
                  where="b IS NOT FALSE", keep=_b_is_not(False))
    assert sum(rows.values()) == 100


def test_bool_is_predicate_role3_filter_only_now_native(tmp_path):
    """The BOOL column is READ for the filter but never emitted (role 3)."""
    rows = _check(tmp_path, _alt(), "n", {"n": _INT64},
                  where="b IS TRUE", keep=_b_is(True))
    assert sum(rows.values()) == 100


def test_bool_is_predicate_with_nulls(tmp_path):
    """The NULL-collapsing semantics that make IS TRUE/FALSE a DISTINCT opcode from
    `= TRUE`/`= FALSE`: a NULL row is never a survivor for IS TRUE or IS FALSE, but
    IS ALWAYS a survivor for IS NOT TRUE and IS NOT FALSE. 80 TRUE / 40 FALSE /
    80 NULL out of 200."""
    t_rows = _check(tmp_path / "t", _nulls5(), "n", {"n": _INT64},
                    where="b IS TRUE", keep=_b_is(True))
    f_rows = _check(tmp_path / "f", _nulls5(), "n", {"n": _INT64},
                    where="b IS FALSE", keep=_b_is(False))
    nt_rows = _check(tmp_path / "nt", _nulls5(), "n", {"n": _INT64},
                     where="b IS NOT TRUE", keep=_b_is_not(True))
    nf_rows = _check(tmp_path / "nf", _nulls5(), "n", {"n": _INT64},
                     where="b IS NOT FALSE", keep=_b_is_not(False))
    assert sum(t_rows.values()) == 80
    assert sum(f_rows.values()) == 40
    assert sum(nt_rows.values()) == 120   # FALSE ∪ NULL
    assert sum(nf_rows.values()) == 160   # TRUE ∪ NULL


def test_bool_is_predicate_all_null(tmp_path):
    """Every row NULL → IS TRUE/FALSE have no survivors; IS NOT TRUE/IS NOT FALSE
    survive on EVERY row (unlike `<> TRUE`/`!= FALSE`, which stay NULL too)."""
    cols = {"b": (pa.bool_(), [None] * 200), "n": (pa.int64(), list(range(200)))}
    t_rows = _check(tmp_path / "t", cols, "n", {"n": _INT64},
                    where="b IS TRUE", keep=_b_is(True))
    f_rows = _check(tmp_path / "f", cols, "n", {"n": _INT64},
                    where="b IS FALSE", keep=_b_is(False))
    nt_rows = _check(tmp_path / "nt", cols, "n", {"n": _INT64},
                     where="b IS NOT TRUE", keep=_b_is_not(True))
    nf_rows = _check(tmp_path / "nf", cols, "n", {"n": _INT64},
                     where="b IS NOT FALSE", keep=_b_is_not(False))
    assert not t_rows and not f_rows
    assert sum(nt_rows.values()) == 200 and sum(nf_rows.values()) == 200


def test_bool_is_predicate_all_constant(tmp_path):
    """A single-valued bool column decodes to the CONSTANT shape (data_length == 1,
    selection = the global zero vector) — the kernel has no shape discriminant, so
    this must come out through the same uniform bit read as bool_and/bool_or."""
    cols = {"b": (pa.bool_(), [True] * 200), "n": (pa.int64(), list(range(200)))}
    t_rows = _check(tmp_path / "t", cols, "n", {"n": _INT64},
                    where="b IS TRUE", keep=_b_is(True))
    f_rows = _check(tmp_path / "f", cols, "n", {"n": _INT64},
                    where="b IS FALSE", keep=_b_is(False))
    assert sum(t_rows.values()) == 200
    assert not f_rows


def test_bool_is_predicate_unaligned_tail(tmp_path):
    """Row count not a multiple of 8 — the bitmap's partial last byte. A kernel that
    wrote past the logical length would show up as phantom survivors."""
    n = 203
    cols = {"b": (pa.bool_(), [i % 3 == 0 for i in range(n)]),
            "n": (pa.int64(), list(range(n)))}
    rows = _check(tmp_path, cols, "n", {"n": _INT64}, where="b IS TRUE", keep=_b_is(True))
    assert sum(rows.values()) == len([i for i in range(n) if i % 3 == 0])


def test_bool_is_true_projection(tmp_path):
    """IS TRUE as a PROJECTED expression (not a predicate) — exercises the same
    opcode through ExprMultiProjectOperator / `bytecode_ops_all_c_native`'s
    projection-eligibility path rather than the Filter-node predicate path. The
    result is never NULL, even for a NULL operand."""
    cols = {"b": (pa.bool_(), [True, None, False, None, True] * 40)}
    _check(tmp_path, cols, "b IS TRUE AS t, b IS FALSE AS f", {"t": _BOOL, "f": _BOOL},
           project=lambda r: (r["b"] is True, r["b"] is False))


# ── DATE ─────────────────────────────────────────────────────────────────────

def _dates(n=200):
    base = datetime.date(2000, 1, 1)
    return [base + datetime.timedelta(days=i) for i in range(n)]


def test_date_projection(tmp_path):
    cols = {"d": (pa.date32(), _dates()), "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "d, n", {"d": _DATE, "n": _INT64})


def test_date_with_nulls(tmp_path):
    ds = _dates(200)
    ds[3] = ds[7] = ds[199] = None
    cols = {"d": (pa.date32(), ds)}
    _check(tmp_path, cols, "d", {"d": _DATE})


def test_date_epoch_and_boundary(tmp_path):
    cols = {"d": (pa.date32(), [datetime.date(1970, 1, 1), datetime.date(1900, 1, 1),
                                datetime.date(2262, 4, 11), datetime.date(9999, 12, 31)] * 20)}
    _check(tmp_path, cols, "d", {"d": _DATE})


def test_date_role3_filter_only(tmp_path):
    cols = {"d": (pa.date32(), _dates()), "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "n", {"n": _INT64}, where="n > 100", keep=lambda r: r["n"] > 100)


# ── TIMESTAMP (multiple units, boundaries) ───────────────────────────────────

def _timestamps(n=200):
    base = datetime.datetime(2020, 1, 1, 12, 0, 0)
    return [base + datetime.timedelta(seconds=i * 37) for i in range(n)]


#: The unit the scan must carry for each pyarrow write unit. Parquet's TIMESTAMP
#: logical type has no seconds unit, so pyarrow stores `timestamp[s]` as MILLIS; the
#: footer then says ms and the scan honours the FILE's unit.
_PARQUET_TS_UNIT = {"s": "ms", "ms": "ms", "us": "us"}


@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_timestamp_projection_units(tmp_path, unit):
    # 'ns' is excluded: an ns timestamp overflows the engine's value display (a
    # pre-existing unit-handling issue, not a WP-11 scan concern).
    cols = {"t": (pa.timestamp(unit), _timestamps()), "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "t, n", {"t": _ts(_PARQUET_TS_UNIT[unit]), "n": _INT64},
           normalize={"t": _utc})


def test_timestamp_with_nulls(tmp_path):
    ts = _timestamps(200)
    ts[1] = ts[50] = ts[199] = None
    cols = {"t": (pa.timestamp("us"), ts)}
    _check(tmp_path, cols, "t", {"t": _ts("us")}, normalize={"t": _utc})


def test_timestamp_epoch_and_boundary(tmp_path):
    cols = {"t": (pa.timestamp("us"), [
        datetime.datetime(1970, 1, 1, 0, 0, 0),
        datetime.datetime(1900, 1, 1, 0, 0, 0),
        datetime.datetime(2262, 1, 1, 0, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59),
    ] * 20)}
    _check(tmp_path, cols, "t", {"t": _ts("us")}, normalize={"t": _utc})


def test_timestamp_all_constant(tmp_path):
    cols = {"t": (pa.timestamp("ms"), [datetime.datetime(2021, 6, 6, 6, 6, 6)] * 200)}
    _check(tmp_path, cols, "t", {"t": _ts("ms")}, normalize={"t": _utc})


def test_timestamp_role3_filter_only(tmp_path):
    cols = {"t": (pa.timestamp("us"), _timestamps()), "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "n", {"n": _INT64}, where="n < 50", keep=lambda r: r["n"] < 50)


# ── TIME (32 = ms, 64 = us/ns) ───────────────────────────────────────────────
#
# The binder declares every parquet TIME column TIME[us] (the canonical TIME), and
# the engine's own TIME value — `CAST('01:02:03.5' AS TIME)` — is INT64 +
# LogicalKind.TIME + unit us holding microseconds since midnight. A scanned TIME
# column must match the type its schema declares, whatever unit the file stored.

def _times(n=200):
    return [datetime.time((i * 7) % 24, (i * 11) % 60, (i * 13) % 60) for i in range(n)]


def test_time32_ms_projection(tmp_path):
    cols = {"tm": (pa.time32("ms"), _times())}
    _check(tmp_path, cols, "tm", {"tm": _TIME}, normalize={"tm": _time_us})


@pytest.mark.parametrize("unit", ["us", "ns"])
def test_time64_projection_units(tmp_path, unit):
    cols = {"tm": (pa.time64(unit), _times())}
    _check(tmp_path, cols, "tm", {"tm": _TIME}, normalize={"tm": _time_us})


def test_time_with_nulls(tmp_path):
    tms = _times(200)
    tms[2] = tms[99] = None
    cols = {"tm": (pa.time64("us"), tms)}
    _check(tmp_path, cols, "tm", {"tm": _TIME}, normalize={"tm": _time_us})


# ── DECIMAL (varied precision/scale, negatives, zero, max precision) ──────────
#
# pyarrow writes DECIMAL as FIXED_LEN_BYTE_ARRAY. Precision ≤ 18 must come back
# int64-backed DRAKEN_DECIMAL, precision > 18 int128-backed DRAKEN_DECIMAL128, with
# precision/scale from the footer either way.

def _decimals(precision, scale, n=200):
    q = decimal.Decimal(1).scaleb(-scale)
    out = []
    for i in range(n):
        v = decimal.Decimal((i - n // 2) * 3) + decimal.Decimal(i) / decimal.Decimal(100)
        out.append(v.quantize(q))
    return out


@pytest.mark.parametrize("precision,scale", [(5, 2), (10, 0), (18, 6)])
def test_decimal_projection(tmp_path, precision, scale):
    cols = {"d": (pa.decimal128(precision, scale), _decimals(precision, scale)),
            "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "d, n", {"d": _dec(precision, scale), "n": _INT64})


def test_decimal_with_nulls(tmp_path):
    ds = _decimals(18, 6, 200)
    ds[0] = ds[100] = ds[199] = None
    cols = {"d": (pa.decimal128(18, 6), ds)}
    _check(tmp_path, cols, "d", {"d": _dec(18, 6)})


def test_decimal_zero_and_negative(tmp_path):
    cols = {"d": (pa.decimal128(12, 3), [
        decimal.Decimal("0.000"), decimal.Decimal("-1.500"),
        decimal.Decimal("-999999.999"), decimal.Decimal("999999.999"),
    ] * 50)}
    _check(tmp_path, cols, "d", {"d": _dec(12, 3)})


def test_decimal_all_constant(tmp_path):
    cols = {"d": (pa.decimal128(9, 2), [decimal.Decimal("12.34")] * 200)}
    _check(tmp_path, cols, "d", {"d": _dec(9, 2)})


@pytest.mark.parametrize("use_dictionary", [True, False], ids=["dict", "plain"])
def test_decimal128_wide_projection(tmp_path, use_dictionary):
    """precision > 18 (int128). pyarrow DICTIONARY-encodes it by default, which
    rugo emits as DK_DECIMAL128_DICT (an __int128 dictionary + codes); the plain
    encoding goes through the int128 values. Values, nulls and the precision/scale
    descriptor must survive both."""
    pool = [decimal.Decimal(v) for v in (
        "0.00", "-1.50", "12345678901234567890123.45", "-98765432109876543210987.65", "7.77")]
    ds = [pool[i % len(pool)] for i in range(200)]
    ds[3] = ds[150] = None
    cols = {"d": (pa.decimal128(38, 2), ds), "n": (pa.int64(), list(range(200)))}
    rows = _check(tmp_path, cols, "d, n", {"d": _dec(38, 2), "n": _INT64},
                  write_kw={"use_dictionary": use_dictionary})
    assert sum(rows.values()) == 200


def test_decimal128_dict_predicate(tmp_path):
    pool = [decimal.Decimal(v) for v in ("1.10", "2.20", "33333333333333333333.33")]
    cols = {"d": (pa.decimal128(38, 2), [pool[i % 3] for i in range(200)]),
            "n": (pa.int64(), list(range(200)))}
    rows = _check(tmp_path, cols, "d, n", {"d": _dec(38, 2), "n": _INT64},
                  where="d > 2.0",
                  keep=lambda r: r["d"] is not None and r["d"] > decimal.Decimal("2.0"))
    assert sum(rows.values()) == 133  # every row whose d is 2.20 or 33333333333333333333.33


def test_decimal_predicate_role2(tmp_path):
    q = decimal.Decimal("0.01")
    cols = {"d": (pa.decimal128(10, 2), [(decimal.Decimal(i) / 4).quantize(q) for i in range(200)]),
            "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "d, n", {"d": _dec(10, 2), "n": _INT64},
           where="d > 10.0",
           keep=lambda r: r["d"] is not None and r["d"] > decimal.Decimal("10.0"))


# ── mixed decimal + timestamp + bool in one scan ─────────────────────────────

def test_mixed_decimal_timestamp_bool(tmp_path):
    cols = {
        "d": (pa.decimal128(18, 4), _decimals(18, 4)),
        "t": (pa.timestamp("us"), _timestamps()),
        "b": (pa.bool_(), [True, False] * 100),
        "n": (pa.int64(), list(range(200))),
    }
    _check(tmp_path, cols, "d, t, b, n",
           {"d": _dec(18, 4), "t": _ts("us"), "b": _BOOL, "n": _INT64},
           normalize={"t": _utc})


# ── UINT projection ──────────────────────────────────────────────────────────

def test_projected_uint_now_native(tmp_path):
    """A PROJECTED uint column decodes on the native scan as exact-width
    DRAKEN_UINT*. (An unsigned column used as a c-native PREDICATE INPUT is covered
    by the A1 suite test_wp_a1_native_int_widths_scan; here it is projection-only.)"""
    cols = {"u": (pa.uint32(), list(range(200))), "n": (pa.int64(), list(range(200)))}
    _check(tmp_path, cols, "u, n", {"u": _UINT32, "n": _INT64})


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
