"""A0 acceptance gate — native SELECT-path residual reasons.

The native C++ engine runs plain SELECT end-to-end EXCEPT for parquet scans that
fall back to the per-morsel Python trampoline (StreamingScanSource). Every such
fallback is one of the `return None` guards in
`opteryx/managers/execution/compiler.py::_native_scan_plan`, and each now records
a stable machine-readable reason code on query telemetry
(`scan_residual_reasons`, keyed by scan identity).

This module is the acceptance gate a close-out chip points at:

  * REACHABILITY — each still-open residual category is triggered by a canonical
    query and its reason string asserted. This proves every code is reachable and
    correctly wired (it is the guard against a tag silently drifting / dying).

  * FRONTIER (xfail) — one strict-xfail test per open category asserts the scan
    goes NATIVE. It fails today (the scan is trampoline → xfail) and FLIPS TO A
    HARD FAILURE (xpass, strict) the moment a close-out chip admits that shape
    natively — the signal to delete the marker and move the category to "closed".

The census tool + reason enumeration live in `dev/native_residual_census.py`;
the ordered close-out plan is `docs/NATIVE_RESIDUAL_PLAN.md`.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pytest

import native_residual_census as census  # dev/native_residual_census.py


def assert_scan_native(sql):
    """A close-out chip's assertion: every parquet scan in `sql` selects a zero-Python
    Source — i.e. NO residual reason was recorded. There are two such Sources:
    `NativeParquetScanSource` (single-pass) and `LatmatScanSource` (R3's two-pass
    late-materialization scan); neither touches Python during execution, and
    `census._NATIVE_SOURCES` is the single list both this and the census read.
    Raises AssertionError (with the residual reasons) if any scan fell back."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert sources, "no parquet scan observed — query did not reach the native scan path"
    assert all(v in census._NATIVE_SOURCES for v in sources.values()), (
        f"scan fell back to the trampoline; sources={sources} "
        f"residual reasons={reasons}")


# ---------------------------------------------------------------------------
# REACHABILITY — every residual reason string is reachable and correctly wired.
# ---------------------------------------------------------------------------

def test_residual_reasons_reachable():
    """Each canonical query forces exactly its guard and tags the matching reason.
    A reason may carry a `:<detail>` suffix, so match on the prefix.

    HAND_SET holds one entry today — `footer_gate` via schema evolution, the last
    residual with a live SQL trigger. Written as a loop rather than a parametrize so
    that HAND_SET emptying out (when that last one closes) stays a real passing
    assertion instead of an empty parameter set, which pytest turns into a skip."""
    for expected_reason, sql in census.HAND_SET.items():
        sources, reasons, err = census.scan_residuals(sql)
        assert err is None, f"{expected_reason}: query raised: {err}"
        assert set(sources.values()) == {"StreamingScanSource"}, (
            f"{expected_reason}: expected trampoline fallback, got sources={sources}")
        observed = set(reasons.values())
        assert any(
            r == expected_reason or r.startswith(expected_reason + ":") for r in observed
        ), f"expected residual reason {expected_reason!r}, got {sorted(observed)}"


def test_native_scan_records_no_residual():
    """The positive control: a plainly-admissible scan goes native and records NO
    residual reason (guards against a false-positive tag on the native path)."""
    sources, reasons, err = census.scan_residuals(
        "SELECT user_name, followers FROM 'testdata/flat/formats/parquet'")
    assert err is None
    assert set(sources.values()) == {"NativeParquetScanSource"}
    assert reasons == {}


# ---------------------------------------------------------------------------
# FRONTIER — one strict-xfail per open category. Flips to a HARD FAILURE (xpass)
# when a close-out chip admits that shape natively → delete the marker then.
# ---------------------------------------------------------------------------

# (category, canonical SQL) — the residual frontier. `footer_gate` (R7b) was
# CLOSED by A1 for the integer widths: narrow / unsigned / annotated INTEGER
# columns now select the native scan (see test_footer_gate_int_widths_now_native
# below). It stays reachable as a residual only via schema evolution — see
# HAND_SET / test_residual_reason_reachable — which is a distinct, still-open
# structural gap, NOT the integer admission this test tracked.
#
# `fused_topn` (R3) is CLOSED. A3 admitted the NO-predicate scan-fused
# `ORDER BY ... LIMIT`; the composed shape (fused TopN WITH a predicate, e.g.
# ClickBench Q24) is now served by `LatmatScanSource` — a genuinely native
# two-pass late-materialization scan that KEEPS the decode-skip whose loss caused
# the earlier ~400% Q24 regression. See test_fused_topn_with_predicate_now_native
# below and the correctness matrix in test_wp_r3_latmat_scan.py. The reason code
# has left this list and HAND_SET.
#
# `pushed_limit` (R2) is CLOSED: NativeParquetScanSource enforces a scan-pushed
# LIMIT natively (see test_pushed_limit_now_native below), so it is no longer a
# reachable residual and has left both this list and HAND_SET.
#
# `bool_predicate_input` (R5) is CLOSED: draken_compare_dv now has a DRAKEN_BOOL
# branch (see test_bool_predicate_input_now_native below), so a BOOL predicate
# input is admitted natively and the reason code has left this list and HAND_SET.
#
# `non_admissible_kind` (R6) is CLOSED: ARRAY was the only kind it was ever
# observed with, and ARRAY now decodes natively (see test_array_column_now_native
# below). The reason code has no reachable SQL trigger left, so it has left this
# list and HAND_SET — see the retirement note in dev/native_residual_census.py for
# what stays behind the guard defensively.
#
# `unlowerable_predicate` (R4) is CLOSED — and it was the LAST one, so the frontier
# is now empty. Its trigger was a pushed regex predicate; the native regex kernels
# closed it incidentally, which is why the marker outlived the category by some
# months. See test_regex_predicate_now_native below.
_OPEN_CATEGORIES: list = []


def test_residual_frontier():
    """The residual frontier: every category listed here must still be on the
    trampoline. When a close-out chip admits one natively this turns RED — the
    signal to retire its entry and record the category closed.

    `_OPEN_CATEGORIES` is EMPTY: R4 was the last entry. That does NOT mean nothing
    reaches the trampoline — `footer_gate` via schema evolution still does (see
    HAND_SET and test_residual_reasons_reachable); it was never in this frontier
    list because its integer sub-case closed in A1 and the schema-evolution
    remainder is a distinct structural gap. A loop rather than a strict-xfail
    parametrize so the empty frontier is a passing assertion, not a skip — and so
    the RED signal returns automatically if a category is ever re-opened."""
    for category, sql in _OPEN_CATEGORIES:
        sources, reasons, err = census.scan_residuals(sql)
        assert err is None, f"{category}: query raised: {err}"
        assert set(sources.values()) == {"StreamingScanSource"}, (
            f"{category} is no longer on the trampoline (sources={sources}) — "
            f"retire its _OPEN_CATEGORIES/HAND_SET entry and record it closed")
        assert reasons, f"{category}: expected a residual reason"


# ---------------------------------------------------------------------------
# CLOSED — A1 footer_gate integer admission. These were the strict-xfail
# `footer_gate` frontier; they are now real passing assertions. clickbench_tiny
# carries the full integer family: EventDate (parquet int32 / logical uint16),
# AdvEngineID + ResolutionWidth (int32 / int16, signed-narrow — widen to INT64 on
# decode), CounterID (int32 / int32), UserID (int64 / int64). Every width is
# admitted byte-identically to the trampoline, so NONE is left fail-closed (UINT64
# has a native DRAKEN_UINT64 vector — no truncation, so it too is admitted, not
# fail-closed). The columns must go native in all four scan roles.
_TINY = "testdata.clickbench_tiny"


@pytest.mark.parametrize("sql", [
    # projected (unsigned, signed-narrow, plain int32, int64)
    "SELECT EventDate FROM %s" % _TINY,
    "SELECT AdvEngineID FROM %s" % _TINY,
    "SELECT ResolutionWidth FROM %s" % _TINY,
    "SELECT CounterID FROM %s" % _TINY,
    "SELECT UserID FROM %s" % _TINY,
    "SELECT EventDate, AdvEngineID, ResolutionWidth, CounterID, UserID FROM %s" % _TINY,
    # SIGNED narrow as a c-native predicate input (widens to INT64, VM-readable)
    "SELECT AdvEngineID FROM %s WHERE AdvEngineID <> 0" % _TINY,
    "SELECT ResolutionWidth FROM %s WHERE ResolutionWidth >= 1024" % _TINY,
    # role-3 filter-only over a SIGNED narrow column: read but not emitted
    "SELECT UserID FROM %s WHERE AdvEngineID <> 0" % _TINY,
    # UNSIGNED column PROJECTED alongside a signed predicate — still native
    "SELECT EventDate FROM %s WHERE AdvEngineID <> 0" % _TINY,
])
def test_footer_gate_int_widths_now_native(sql):
    """A1: narrow / unsigned / annotated INTEGER columns now select the native scan
    when projected, and signed-narrow columns go native in every role including as a
    c-native predicate input. Was the footer_gate strict-xfail frontier; now a hard
    pass."""
    assert_scan_native(sql)


@pytest.mark.parametrize("sql", [
    "SELECT EventDate FROM %s WHERE EventDate > 0" % _TINY,   # uint16 predicate input
    "SELECT UserID FROM %s WHERE EventDate > 0" % _TINY,      # uint16 role-3 filter
])
def test_unsigned_predicate_input_now_native(sql):
    """Was the A1 `unsigned_predicate_input` fail-closed. An UNSIGNED integer column
    used as a c-native predicate input is now admitted natively: the schema declares
    the column's real width, so the comparison literal is re-materialized at that
    width, and draken_compare_dv dispatches the u8/u16/u32/u64 compare kernels that
    switch previously never wired in. The reason code no longer exists."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


# ---------------------------------------------------------------------------
# CLOSED — R5 `bool_predicate_input`. Was the strict-xfail frontier; now real
# passing assertions.
#
# The gate existed because `draken_compare_dv`'s type switch had NO DRAKEN_BOOL
# branch: every bool comparison fell to `default: return nullptr` ("declined"),
# which the relocated c-native ExprFilter — which has no fallback — surfaced as
# err_op=11. So the whole scan failed closed even though
# `bytecode_is_all_c_native` correctly reported the predicate lowerable.
#
# BOOL cannot reuse a fixed-width kernel: its `data` buffer is BIT-PACKED, so
# `data[selection[i]]` means *bit* selection[i]. `draken/ops/bool_compare.h` is
# that kernel — a uniform loop over the bitmap (dense / constant / dict all read
# correctly through it), FALSE < TRUE ordering, and the compare_vector null
# contract (result row NULL when EITHER operand row is NULL, NOT Kleene AND/OR).
# It is NOT discriminant-free: dense-identity operands WITH nulls take a
# byte-wise fast path, a CLAUDE.md §11 shape discriminant added without the
# pre-approval §11 requires and ratified after the fact. See that header's
# ACCESS comment; a fuzz test asserts the two paths agree bit-for-bit.
#
# Value parity against the trampoline is asserted in
# tests/unit/operators/test_wp11_decimal_temporal_bool_scan.py (the A/B harness:
# native vs forced-trampoline in one run, covering NULLs and every role).
# ---------------------------------------------------------------------------

_BOOL_FLAT = "testdata/flat/formats/parquet"   # user_verified: 711 TRUE / 99289 FALSE


@pytest.mark.parametrize("sql", [
    # projected + predicate (role-2)
    "SELECT user_id, user_verified FROM '%s' WHERE user_verified = TRUE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified = FALSE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified <> TRUE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified != FALSE" % _BOOL_FLAT,
    # role-3: the BOOL column is READ for the filter but never emitted
    "SELECT user_id FROM '%s' WHERE user_verified = TRUE" % _BOOL_FLAT,
    # composed with a non-bool predicate in the same c-native span
    "SELECT user_id FROM '%s' WHERE user_verified = TRUE AND followers > 100" % _BOOL_FLAT,
    # zero-projection (A2 shape) over a bool predicate
    "SELECT COUNT(*) FROM '%s' WHERE user_verified = TRUE" % _BOOL_FLAT,
    # the canonical HAND_SET trigger this category was censused by
    "SELECT userid FROM 'testdata/flat/ten_files' WHERE user_verified = TRUE",
])
def test_bool_predicate_input_now_native(sql):
    """R5 close-out: a BOOL column used as a c-native predicate input now selects
    the zero-Python native Source, in every role. The reason code no longer exists."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


@pytest.mark.parametrize("predicate,expected", [
    ("user_verified = TRUE", 711),
    ("user_verified = FALSE", 99289),
    ("user_verified <> TRUE", 99289),
    ("user_verified != FALSE", 711),
])
def test_bool_predicate_survivor_count_matches_trampoline(predicate, expected):
    """The survivor counts the TRAMPOLINE produced before the kernel landed —
    captured from the pre-change path, not re-derived from the new one. A bool
    compare that inverted an op, mis-read the bit-packed layout, or dropped the
    validity mask would move these."""
    import opteryx

    session = opteryx.session()
    rows = sum(
        morsel.num_rows
        for morsel in session.execute_to_morsels(
            "SELECT user_id FROM '%s' WHERE %s" % (_BOOL_FLAT, predicate))
    )
    assert rows == expected
    assert set(session.telemetry["scan_sources"].values()) == {
        "NativeParquetScanSource"}


# ---------------------------------------------------------------------------
# CLOSED — the `compiler.py` hard-error class for `IS TRUE` / `IS FALSE` /
# `IS NOT TRUE` / `IS NOT FALSE` over a BOOL column (the R5 close-out above
# widened `bool = TRUE/FALSE`; this is the adjacent SQL `IS`-predicate form,
# a distinct UOP_IS_TRUE/UOP_IS_FALSE/UOP_IS_NOT_TRUE/UOP_IS_NOT_FALSE bytecode
# opcode with different NULL semantics — `NULL IS TRUE` is FALSE, not NULL, so
# it cannot reuse the compare kernel).
#
# `bytecode_ops_all_c_native` (opteryx/operators/_operators.pyx) previously
# admitted only UOP_IS_NULL/UOP_IS_NOT_NULL for BC_UNARY_OP; a bare `WHERE
# bool_col IS TRUE` predicate does not push to the scan, becomes a standalone
# Filter node, and hard-errored at `compiler.py`'s `_lower_expression`
# ("... outside the c-native kernel set ... no fallback engine"). The nogil
# span (`c_execute_dv_inner`) had no DV*-only kernel for these ops at all.
#
# `draken/ops/bool_logical.h::bool_truth_test` supplies that kernel — the
# SAME uniform `data[selection[i]]` loop as `bool_and`/`bool_or`/`bool_not`,
# but a never-null truth test (result is unconditionally all-valid) rather
# than Kleene NULL-preserving logic. `_dv_unary_bool_test_c` (evaluation.pyx)
# wires it into the VM; `bytecode_ops_all_c_native` and `build_bytecode`'s
# `is_all_c_native` (compiled_expression.pyx) both admit the four opcodes.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sql", [
    "SELECT user_id FROM '%s' WHERE user_verified IS TRUE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified IS FALSE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified IS NOT TRUE" % _BOOL_FLAT,
    "SELECT user_id FROM '%s' WHERE user_verified IS NOT FALSE" % _BOOL_FLAT,
    # role-3 (filter-only, column never emitted)
    "SELECT user_id FROM '%s' WHERE user_verified IS TRUE" % _BOOL_FLAT,
    # composed with a non-bool predicate in the same c-native span
    "SELECT user_id FROM '%s' WHERE user_verified IS TRUE AND followers > 100" % _BOOL_FLAT,
    # zero-projection (A2 shape) over an IS-predicate
    "SELECT COUNT(*) FROM '%s' WHERE user_verified IS TRUE" % _BOOL_FLAT,
    # the canonical trigger this task's reproduction used
    "SELECT userid FROM 'testdata/flat/ten_files' WHERE user_verified IS TRUE",
])
def test_bool_is_predicate_now_native(sql):
    """IS TRUE/FALSE/NOT TRUE/NOT FALSE over a BOOL predicate input now selects
    the zero-Python native Source, in every role, matching the R5 `= TRUE`
    close-out's coverage shape exactly."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


@pytest.mark.parametrize("predicate,expected", [
    ("user_verified IS TRUE", 711),
    ("user_verified IS FALSE", 99289),
    ("user_verified IS NOT TRUE", 99289),
    ("user_verified IS NOT FALSE", 711),
])
def test_bool_is_predicate_survivor_count_matches_eq_form(predicate, expected):
    """`_BOOL_FLAT` has no NULLs, so IS TRUE/FALSE must match the already-verified
    `= TRUE`/`= FALSE` survivor counts (`test_bool_predicate_survivor_count_matches_trampoline`)
    exactly — a kernel that mishandled the never-null collapse would only show up
    once NULLs are present (covered by the A/B harness in
    test_wp11_decimal_temporal_bool_scan.py), so this pins the NULL-free baseline."""
    import opteryx

    session = opteryx.session()
    rows = sum(
        morsel.num_rows
        for morsel in session.execute_to_morsels(
            "SELECT user_id FROM '%s' WHERE %s" % (_BOOL_FLAT, predicate))
    )
    assert rows == expected
    assert set(session.telemetry["scan_sources"].values()) == {
        "NativeParquetScanSource"}


# ---------------------------------------------------------------------------
# CLOSED — R6 `non_admissible_kind:ARRAY`. Was the strict-xfail frontier; now
# real passing assertions.
#
# A parquet LIST column always lands DK_POOL — rugo's direct_kind_for routes any
# column with repetition levels to the pool, regardless of encoding — and is
# serialized as TAG_ARRAY by ipc_serialize.hpp. Both scan paths share that
# producer; only the consumer differed. `src/cpp/engine/native_array_pool_decode.hpp`
# is the PyObject-free port of the trampoline's Cython `_build_array_vector*`
# (column_deserializer.pyx), and `array_columns` (plan-time, parallel to
# column_names — the same mechanism as decimal_columns / varchar_columns) is what
# tells the Source that a given pool blob is a list rather than a decimal/varchar.
#
# Value parity against the trampoline is asserted in
# tests/unit/operators/test_wp_r6_array_scan.py (the A/B harness: native vs
# forced-trampoline in one run, covering every element type, empty lists, NULL
# lists, NULL elements inside a present list, and nested list<list<...>>).
# ---------------------------------------------------------------------------

_ARRAY_TYPES = "testdata/flat/array_types"   # the R6 parity corpus (dev/generate_array_testdata.py)
_STRUCT_ARRAY = "testdata/flat/struct_array"


@pytest.mark.parametrize("sql", [
    # the canonical HAND_SET trigger this category was censused by
    "SELECT data FROM '%s'" % _STRUCT_ARRAY,
    "SELECT id, data FROM '%s'" % _STRUCT_ARRAY,
    # every element type the wire format can carry, in isolation
    "SELECT ints FROM '%s'" % _ARRAY_TYPES,
    "SELECT strs FROM '%s'" % _ARRAY_TYPES,
    "SELECT floats FROM '%s'" % _ARRAY_TYPES,
    "SELECT bools FROM '%s'" % _ARRAY_TYPES,
    "SELECT stamps FROM '%s'" % _ARRAY_TYPES,
    "SELECT smalls FROM '%s'" % _ARRAY_TYPES,
    "SELECT uints FROM '%s'" % _ARRAY_TYPES,
    "SELECT nested FROM '%s'" % _ARRAY_TYPES,          # list<list<int64>>
    # an array column alongside ordinary columns, and SELECT *
    "SELECT id, ints, strs FROM '%s'" % _ARRAY_TYPES,
    "SELECT * FROM '%s'" % _ARRAY_TYPES,
    "SELECT * FROM testdata.astronauts",
    "SELECT * FROM 'testdata/flat/formats/parquet'",
    # ROLE-3: the array column is READ for the pushed predicate but never emitted
    "SELECT id FROM '%s' WHERE ints IS NULL" % _ARRAY_TYPES,
    "SELECT id FROM '%s' WHERE ints IS NOT NULL" % _ARRAY_TYPES,
    # zero-projection (A2 shape) over an array predicate input
    "SELECT COUNT(*) FROM '%s' WHERE strs IS NULL" % _ARRAY_TYPES,
    # array-consuming SQL over the native scan
    "SELECT ARRAY_CONTAINS(ints, 5) FROM '%s'" % _ARRAY_TYPES,
    "SELECT u FROM '%s' CROSS JOIN UNNEST(ints) AS u" % _ARRAY_TYPES,
])
def test_array_column_now_native(sql):
    """R6 close-out: a read-set ARRAY column (projected OR role-3 filter-only) now
    selects the zero-Python native Source. The reason code no longer exists."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


def test_struct_column_does_not_reach_the_r6_guard():
    """STRUCT stays outside R6 entirely, and always did: rugo's footer annotates a
    struct node `json`, so the binder types it as a string column and the scan is
    admitted through the ordinary VARCHAR path — it never had a `non_admissible_kind`
    tag to close. Pinned so a future type-system change that starts routing STRUCT
    through the R6 guard is visible rather than silent."""
    sources, reasons, err = census.scan_residuals("SELECT * FROM 'testdata/flat/struct'")
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


# ---------------------------------------------------------------------------
# CLOSED — R7b `footer_gate`, CAST-driven temporal retag.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_cast_driven_timestamp_now_native(unit):
    """R7b close-out: `EventTime::TIMESTAMP[unit]` asks for a TIMESTAMP column whose
    FOOTER carries no temporal annotation (a bare int64) — the temporal-ness comes
    from the SQL cast, not the file. This was the last `footer_gate` residual in the
    census battery. The cast is a pure bit-reinterpret and the native decode path is
    already unit-parametrized, so only the gate's kind-classification needed widening."""
    assert_scan_native(
        "SELECT EventTime::TIMESTAMP[%s] FROM %s" % (unit, _TINY))


@pytest.mark.parametrize("unit", ["s", "ms", "us"])
def test_cast_driven_timestamp_matches_trampoline(unit):
    """The retag must be VALUE-identical to the trampoline's `_coerce_vectors`, not
    merely admitted. EventTime holds Unix SECONDS, so reinterpreting it at each unit
    lands on a different (but exactly predictable) instant — these are the values the
    trampoline produced before the scan went native."""
    import datetime

    import opteryx

    expected = {
        "s": datetime.datetime(2013, 7, 27, 20, 0, tzinfo=datetime.timezone.utc),
        "ms": datetime.datetime(1970, 1, 16, 21, 55, 55, 200000,
                                tzinfo=datetime.timezone.utc),
        "us": datetime.datetime(1970, 1, 1, 0, 22, 54, 955200,
                                tzinfo=datetime.timezone.utc),
    }[unit]
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
            "SELECT EventTime::TIMESTAMP[%s] AS t FROM %s ORDER BY t LIMIT 5"
            % (unit, _TINY)):
        morsel.materialize()
        rows.extend(morsel[i] for i in range(morsel.num_rows))
    assert rows, "query returned no rows"
    assert rows[0][0] == expected, (unit, rows[0][0], expected)


# ---------------------------------------------------------------------------
# CLOSED — R2 `pushed_limit`. Was the strict-xfail frontier; now hard passes.
# ---------------------------------------------------------------------------

_LIMIT_FLAT = "testdata/flat/formats/parquet"


@pytest.mark.parametrize("sql", [
    "SELECT followers FROM '%s' LIMIT 5" % _LIMIT_FLAT,
    "SELECT followers FROM '%s' LIMIT 1" % _LIMIT_FLAT,
    # LIMIT above the row-group boundary, and above the whole table
    "SELECT followers FROM '%s' LIMIT 100000" % _LIMIT_FLAT,
    "SELECT followers FROM '%s' LIMIT 10000000" % _LIMIT_FLAT,
    "SELECT l_orderkey FROM testdata.tpch_001.lineitem LIMIT 5",
])
def test_pushed_limit_now_native(sql):
    """R2 close-out: a scan-pushed LIMIT selects the zero-Python native Source.
    The reason code is no longer reachable (retired from HAND_SET)."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


@pytest.mark.parametrize("limit", [1, 2, 5, 99, 100, 1000, 65535, 65536, 100000, 10000000])
def test_pushed_limit_row_count_exact(limit):
    """R2 is a CORRECTNESS obligation, not just an I/O optimization:
    LimitPushdownStrategy REMOVES the Limit node from the plan when it pushes into
    a scan, so the Source is the only thing enforcing the cap. Exercises limits
    below, at, and above a row-group boundary, and above the whole table."""
    import opteryx

    table_rows = 60175  # testdata.tpch_001.lineitem
    session = opteryx.session()
    rows = sum(
        morsel.num_rows
        for morsel in session.execute_to_morsels(
            "SELECT l_orderkey FROM testdata.tpch_001.lineitem LIMIT %d" % limit)
    )
    assert rows == min(limit, table_rows)


def test_pushed_limit_skips_uncontributing_row_groups():
    """The I/O win the pushdown exists for: row groups that provably cannot
    contribute to the LIMIT are never decoded. The submit frontier is capped from
    the footer's per-row-group row counts, so a small LIMIT over a many-row-group
    file decodes ONE row group — not the whole prefetch window (in_flight_limit,
    == workers+2, plus one per worker racing in before the first emit).

    Deliberately NOT on tpch_001 like its neighbours in this file. That fixture's
    lineitem is a single 60,175-row file holding ONE row group, so "decoded one
    row group" would hold no matter what the frontier did — the assertion would
    pass without exercising anything.

    `testdata/tpch_1/lineitem` exists SOLELY for this test: one SF1 lineitem file,
    6,001,215 rows in 23 row groups, the rest of SF1 having been retired when the
    benchmarks moved to SF10. It is a fixture, not a benchmark dataset — do not
    point performance work at it, and do not "tidy" it away as a leftover of the
    old scale. Delete it only together with this test.
    """
    import opteryx

    session = opteryx.session()
    for _ in session.execute_to_morsels(
            "SELECT l_orderkey FROM testdata.tpch_1.lineitem LIMIT 5"):
        pass
    diagnostics = session.telemetry["io_scan_diagnostics"][0]
    # 23 row groups in the file; LIMIT 5 fits entirely in the first.
    assert diagnostics["enqueue_count"] == 1, diagnostics


# ---------------------------------------------------------------------------
# CLOSED — A2 zero-projection COUNT(*) WITH a pushed predicate. Was the
# `zero_projection` strict-xfail frontier; now a real passing assertion. The
# no-predicate bare `SELECT COUNT(*) FROM t` shape is NOT part of this residual
# at all — it never reaches a scan (StatisticsOnlyResponseStrategy rewrites it to
# a manifest-count literal at the optimizer level) — so it is not tracked here.
# See tests/unit/operators/test_wp_a2_zero_projection_count_scan.py for the A/B
# correctness parity harness (native vs forced-trampoline row counts).
# ---------------------------------------------------------------------------


def test_zero_projection_predicate_now_native():
    """A2: COUNT(*) WITH a pushed predicate now selects the native scan — the
    read-set is the role-3 predicate column(s), the emit-set is empty, and the row
    count rides on the same `zero_col_rows` degenerate path the trampoline already
    used. Was the `zero_projection` strict-xfail frontier; now a hard pass."""
    assert_scan_native("SELECT COUNT(*) FROM 'testdata/flat/formats/parquet' WHERE followers > 0")


# ---------------------------------------------------------------------------
# CLOSED — R3 scan-fused TopN, BOTH sub-cases. These were the `fused_topn`
# strict-xfail frontier; they are now real passing assertions.
#
# `scan._topn_sort_name` is a decode-skip hint stamped by
# `TopNScanPushdownStrategy`. The actual sort/limit/tie-break/null-order is
# always performed by the native `HeapSortNode` -> `set_topn_sink` operator
# downstream of the scan, generically over the incoming layout and independent
# of scan Source — so the hint never changes WHICH rows reach the client, only
# how much has to be decoded to find them.
#
#   * NO predicate (A3): no late-materialization happens on either path, so a
#     plain single-pass native scan is exactly equivalent.
#   * WITH a predicate: the decode-skip is load-bearing — ClickBench Q24
#     (`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT
#     10`) reads only URL + EventTime for the whole table, then ~100 more
#     columns for the handful of survivors. An earlier attempt to admit this
#     shape as a plain single-pass scan measured ~400% slower on Q24 and was
#     reverted. It is now served by `LatmatScanSource`
#     (src/cpp/engine/native_latmat_scan_source.hpp), which does the two passes
#     natively and KEEPS the skip.
#
# The correctness matrix for the two-pass path — ties at and across the
# boundary, ties spanning row groups, all-NULL keys, fewer-than-n non-null
# rows, ASC/DESC, N above the survivor count, string/float keys, and
# pass-1/pass-2 row alignment — lives in
# tests/unit/operators/test_wp_r3_latmat_scan.py. The no-predicate sub-case's
# order-sensitive A/B harness is test_wp_a3_fused_topn_scan.py.
# ---------------------------------------------------------------------------


def test_fused_topn_no_predicate_now_native():
    """A3: a scan-fused `ORDER BY ... LIMIT` with NO predicate selects the native
    single-pass scan (the trampoline does no decode-skip for this shape either)."""
    assert_scan_native("SELECT * FROM testdata.clickbench_tiny ORDER BY EventTime LIMIT 10")


def test_fused_topn_with_predicate_now_native():
    """R3: the composed shape — a scan-fused `ORDER BY ... LIMIT` WITH a pushed
    predicate, i.e. the ClickBench Q24 shape — now runs on the native two-pass
    late-materialization Source instead of the trampoline. This was the last
    reachable residual in the census."""
    sql = ("SELECT * FROM testdata.clickbench_tiny WHERE URL LIKE '%google%' "
           "ORDER BY EventTime LIMIT 10")
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"LatmatScanSource"}, sources
    assert reasons == {}, reasons


def test_fused_topn_without_pass2_columns_takes_the_single_pass_scan():
    """A fused TopN whose projection is entirely covered by the predicate + sort-key
    columns has nothing for a second pass to fetch, so late-materialization would be
    pure overhead — the trampoline refuses it too (`bool(_pass2_names)`). It must fall
    through to the ordinary single-pass native scan, NOT to the trampoline."""
    sql = ("SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' "
           "ORDER BY SearchPhrase LIMIT 10")
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


# ---------------------------------------------------------------------------
# CLOSED — R4 `unlowerable_predicate`. Was the last strict-xfail frontier entry;
# now real passing assertions.
#
# The guard is `bytecode_is_all_c_native(filter_bc)` in compiler.py: a PUSHED
# predicate whose bytecode is not entirely c-native declines the whole native
# scan. Its canonical trigger was a regex (`WHERE text RLIKE 'a'`), and the
# native regex work (the SIMD op-program matcher) closed it INCIDENTALLY — there
# was never an R4 close-out chip, which is why the marker outlived the category.
#
# The `return None` guard STAYS, defensively, exactly as R6's does: what is
# retired is the claim that SQL can still reach it.
#
# Note the adjacent class this test does NOT cover: a non-lowerable predicate
# that never PUSHES becomes a standalone Filter and hard-errors in
# `_lower_expression` ("outside the c-native kernel set ... no fallback engine").
# That is a different failure mode which R4 never tagged — see
# docs/NATIVE_RESIDUAL_PLAN.md finding 2.
# ---------------------------------------------------------------------------

_REGEX_FLAT = "testdata/flat/formats/parquet"


@pytest.mark.parametrize("predicate", [
    "text RLIKE 'a'",                 # the canonical HAND_SET trigger
    "text NOT RLIKE 'a'",
    "text SIMILAR TO 'a.*'",
    "text NOT SIMILAR TO 'a.*'",
    "text ~ 'a'",
    "text !~ 'a'",
    # composed with a plain compare in the SAME pushed span, and negated
    "text RLIKE 'a' AND followers > 5",
    "text RLIKE 'a' OR followers > 5",
    "NOT (text RLIKE 'a')",
    # role-3: the regex column is read for the filter but never emitted
    "text RLIKE 'a'",
    # other once-suspect predicate kernels that also lower now
    "SOUNDEX(user_name) = 'A000'",
    "LEVENSHTEIN(user_name, 'x') < 3",
    "SPLIT(text, ' ')[1] = 'a'",
    "CASE WHEN followers > 5 THEN 1 ELSE 0 END = 1",
    "COALESCE(user_name, 'x') = 'x'",
])
def test_regex_predicate_now_native(predicate):
    """R4 close-out: a pushed predicate that once could not lower to a c-native
    span now selects the zero-Python native Source. The reason code no longer has
    a reachable SQL trigger."""
    sources, reasons, err = census.scan_residuals(
        "SELECT followers FROM '%s' WHERE %s" % (_REGEX_FLAT, predicate))
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"NativeParquetScanSource"}, sources
    assert reasons == {}, reasons


def test_regex_predicate_survivor_count_matches_trampoline():
    """Admitted natively is not enough — the regex must SELECT THE SAME ROWS. Runs
    the native path against a forced-trampoline baseline in one process, so this
    compares the two implementations rather than a hard-coded number."""
    import opteryx
    from opteryx.connectors.parquet_io import pool_reader

    sql = "SELECT user_id FROM '%s' WHERE text RLIKE 'a'" % _REGEX_FLAT

    def _count():
        session = opteryx.session()
        rows = sum(m.num_rows for m in session.execute_to_morsels(sql))
        return rows, set(session.telemetry["scan_sources"].values())

    native_rows, native_src = _count()

    original = pool_reader.native_scan_supported
    pool_reader.native_scan_supported = lambda *a, **k: False
    try:
        tramp_rows, tramp_src = _count()
    finally:
        pool_reader.native_scan_supported = original

    assert native_src == {"NativeParquetScanSource"}, native_src
    assert tramp_src == {"StreamingScanSource"}, tramp_src
    assert native_rows == tramp_rows, (native_rows, tramp_rows)
    assert native_rows > 0, "predicate matched nothing — not a meaningful parity check"


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
