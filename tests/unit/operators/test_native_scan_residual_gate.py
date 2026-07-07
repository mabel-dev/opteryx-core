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
    """A close-out chip's assertion: every parquet scan in `sql` selects the
    zero-Python NativeParquetScanSource — i.e. NO residual reason was recorded.
    Raises AssertionError (with the residual reasons) if any scan fell back."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert sources, "no parquet scan observed — query did not reach the native scan path"
    assert all(v == "NativeParquetScanSource" for v in sources.values()), (
        f"scan fell back to the trampoline; residual reasons={reasons}")


# ---------------------------------------------------------------------------
# REACHABILITY — every residual reason string is reachable and correctly wired.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("expected_reason,sql", list(census.HAND_SET.items()))
def test_residual_reason_reachable(expected_reason, sql):
    """Each canonical query forces exactly its guard and tags the matching reason.
    `non_admissible_kind` carries a `:<DrakenType>` suffix (ARRAY → :NONE), so match
    on the prefix."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"StreamingScanSource"}, (
        f"expected trampoline fallback, got sources={sources}")
    observed = set(reasons.values())
    assert any(r == expected_reason or r.startswith(expected_reason + ":") for r in observed), (
        f"expected residual reason {expected_reason!r}, got {sorted(observed)}")


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
_OPEN_CATEGORIES = [
    ("pushed_limit", census.HAND_SET["pushed_limit"]),
    ("fused_topn", census.HAND_SET["fused_topn"]),
    ("unlowerable_predicate", census.HAND_SET["unlowerable_predicate"]),
    ("bool_predicate_input", census.HAND_SET["bool_predicate_input"]),
    ("non_admissible_kind", census.HAND_SET["non_admissible_kind"]),
]


@pytest.mark.parametrize("category,sql", _OPEN_CATEGORIES,
                         ids=[c for c, _ in _OPEN_CATEGORIES])
@pytest.mark.xfail(strict=True,
                   reason="A0 residual frontier: category still on the Python trampoline")
def test_category_now_native(category, sql):
    """XFAIL while `category` is an open residual. When its close-out chip lands
    (the scan goes native), this xpasses → strict-xfail turns it RED, telling the
    author to retire this marker and mark the category closed."""
    assert_scan_native(sql)


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
def test_unsigned_predicate_input_fails_closed(sql):
    """A1 documented fail-closed: an UNSIGNED integer column used as a c-native
    predicate input stays on the trampoline (the relocated ExprFilter's bytecode VM
    cannot read a UINT vector — err_op=11; the uint compare kernel is out-of-scope
    follow-on). It is tagged `unsigned_predicate_input`, NOT admitted natively."""
    sources, reasons, err = census.scan_residuals(sql)
    assert err is None, f"query raised: {err}"
    assert set(sources.values()) == {"StreamingScanSource"}, sources
    assert set(reasons.values()) == {"unsigned_predicate_input"}, reasons


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


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
