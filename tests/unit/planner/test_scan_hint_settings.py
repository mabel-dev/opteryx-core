"""Per-scan IO settings via table hints: `WITH(name = value)`.

The point is PER-DATASET scope: two scans in one query may carry different
values, which a session-level `SET` cannot express. A join between a small local
table and a large remote one wants different IO shaping on each leg.

Three things are pinned:

1. The vocabulary is the per-scan variable names themselves — no alias table to
   drift out of step with `SHOW VARIABLES`. A name that sizes SHARED engine
   resources (worker pools) is refused, because a per-scan value is not
   expressible for it.
2. A hint runs the SAME permission gate as `SET` (`check_settable`). Every
   per-scan knob is `Visibility.RESTRICTED`, so a hint must not be a way around
   the entitlement check — it is inline SQL text.
3. A hint OUTRANKS the session's `SET`, and applies only to its own scan.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.connectors.parquet_io.io_tuning import PER_SCAN_VARIABLES
from opteryx.exceptions import PermissionsError, UnsupportedSyntaxError

TABLE = "testdata.flat.formats.parquet"
BASE = f"SELECT user_id FROM {TABLE}{{hint}} WHERE user_id > 0"


def _windows(sql, entitlements=("platform_admin",), setup=()):
    session = opteryx.session(entitlements=list(entitlements) if entitlements else None)
    for statement in setup:
        for _ in session.execute_to_morsels(statement):
            pass
    for _ in session.execute_to_morsels(sql):
        pass
    diagnostics = session.telemetry.get("io_scan_diagnostics") or []
    return [d.get("in_flight_limit") for d in diagnostics]


@pytest.mark.parametrize("value", [3, 11, 29])
def test_hint_moves_the_scan_submission_window(value):
    """Prove the hint MOVES the measured value, not just that it parses."""
    assert _windows(BASE.format(hint=f" WITH(parquet_io_in_flight_limit={value})")) == [value]


def test_no_hint_is_auto():
    auto = _windows(BASE.format(hint=""))
    assert auto and auto[0] not in (3, 11, 29)


def test_two_scans_in_one_query_hold_different_values():
    """THE reason hints exist here — per-dataset, not per-session."""
    sql = (
        f"SELECT a.user_id FROM {TABLE} AS a WITH(parquet_io_in_flight_limit=3) "
        f"INNER JOIN {TABLE} AS b WITH(parquet_io_in_flight_limit=29) "
        "ON a.user_id = b.user_id WHERE a.user_id > 0"
    )
    assert sorted(_windows(sql)) == [3, 29]


def test_hint_outranks_session_set():
    setup = ("SET parquet_io_in_flight_limit = 13",)
    assert _windows(BASE.format(hint=""), setup=setup) == [13]
    assert _windows(
        BASE.format(hint=" WITH(parquet_io_in_flight_limit=25)"), setup=setup
    ) == [25]


def test_hint_does_not_bypass_the_set_permission_gate():
    """Every per-scan knob is RESTRICTED — a hint is not a way around that."""
    with pytest.raises(PermissionsError, match="platform_admin"):
        _windows(BASE.format(hint=" WITH(parquet_io_in_flight_limit=5)"), entitlements=None)


def test_hint_type_is_checked_like_a_set():
    with pytest.raises((PermissionsError, ValueError)):
        _windows(BASE.format(hint=" WITH(parquet_io_in_flight_limit='not-a-number')"))


def test_shared_resource_settings_are_refused():
    """Worker pools are shared by every scan, so a per-scan value is meaningless."""
    with pytest.raises(UnsupportedSyntaxError, match="cannot be set on a single relation"):
        _windows(BASE.format(hint=" WITH(parquet_gcs_io_workers=8)"))


def test_unknown_setting_is_refused():
    with pytest.raises(UnsupportedSyntaxError):
        _windows(BASE.format(hint=" WITH(no_such_setting=1)"))


def test_valued_form_required_for_settings():
    with pytest.raises(UnsupportedSyntaxError, match="needs a value"):
        _windows(BASE.format(hint=" WITH(parquet_io_in_flight_limit)"))


def test_bare_hint_rejects_a_value():
    with pytest.raises(UnsupportedSyntaxError, match="does not take a value"):
        _windows(BASE.format(hint=" WITH(NO_CACHE=1)"))


def test_repeated_setting_is_refused():
    with pytest.raises(UnsupportedSyntaxError, match="more than once"):
        _windows(
            BASE.format(hint=" WITH(parquet_io_in_flight_limit=3,parquet_io_in_flight_limit=4)")
        )


def test_legacy_bare_hints_still_parse():
    assert _windows(BASE.format(hint=" WITH(NO_CACHE)"))
    assert _windows(BASE.format(hint=" WITH(NO_PARTITION)"))


def test_setting_is_refused_where_the_reader_cannot_honour_it():
    """An inert knob must SAY so — it is indistinguishable from a broken one."""
    with pytest.raises(UnsupportedSyntaxError, match="cannot be set on"):
        _windows("SELECT name FROM $planets WITH(parquet_io_in_flight_limit=7)")


def test_setting_is_refused_on_a_computed_relation():
    """A function dataset has no IO to shape."""
    with pytest.raises(UnsupportedSyntaxError, match="computes its rows"):
        _windows(
            "SELECT * FROM generate_series(3) AS g WITH(parquet_io_in_flight_limit=7)"
        )


def test_unknown_hint_on_a_computed_relation_is_refused():
    """These were discarded silently, garbage included, before the hints were
    validated on the function-dataset branch too."""
    with pytest.raises(UnsupportedSyntaxError, match="not supported"):
        _windows("SELECT * FROM generate_series(3) AS g WITH(TOTAL_GARBAGE)")


def test_legacy_bare_hints_still_work_on_a_computed_relation():
    """`WITH (NO_CACHE)` on generate_series is used in the shape tests."""
    session = opteryx.session()
    rows = sum(
        m.num_rows
        for m in session.execute_to_morsels(
            "SELECT * FROM generate_series(3) AS g WITH (NO_CACHE)"
        )
    )
    assert rows == 3


def test_shared_pool_knobs_are_not_in_the_per_scan_vocabulary():
    """Pins the scope claim: these size shared pools and must stay out."""
    for name in (
        "parquet_gcs_io_workers",
        "parquet_local_io_workers",
        "max_execution_workers",
    ):
        assert name not in PER_SCAN_VARIABLES


def test_every_per_scan_name_is_a_real_variable():
    """The vocabulary cannot drift from the variables table."""
    from opteryx.variables import SYSTEM_VARIABLES_DEFAULTS

    assert PER_SCAN_VARIABLES <= set(SYSTEM_VARIABLES_DEFAULTS)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
