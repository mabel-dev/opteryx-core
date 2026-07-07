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

# (category, canonical SQL) — the residual frontier as of this A0 census.
_OPEN_CATEGORIES = [
    ("zero_projection", census.HAND_SET["zero_projection"]),
    ("pushed_limit", census.HAND_SET["pushed_limit"]),
    ("fused_topn", census.HAND_SET["fused_topn"]),
    ("unlowerable_predicate", census.HAND_SET["unlowerable_predicate"]),
    ("bool_predicate_input", census.HAND_SET["bool_predicate_input"]),
    ("non_admissible_kind", census.HAND_SET["non_admissible_kind"]),
    ("footer_gate", census.HAND_SET["footer_gate"]),
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


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
