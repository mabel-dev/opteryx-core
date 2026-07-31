"""Regression tests: blending TIMESTAMP64 columns of DIFFERENT units.

A TIMESTAMP64 value is an unscaled int64 whose meaning depends entirely on its
unit (s/ms/us/ns) — a LogicalType descriptor the DrakenVector itself does not
carry (CLAUDE.md §11/§14). CASE, UNION, and COALESCE/IFNULL/IFNOTNULL/IIF all
pick one branch's raw payload and hand it back — if the branches disagree on
unit, and nothing rescales first, the result silently reinterprets one side's
value at the wrong unit. A ms-unit 2024-01-02 read as us collapses to
1970-01-20 — wrong by 54 years, not a rounding error.

Three call sites shared this bug, fixed together (2026-07-31):

  1. CASE branch coercion (opteryx/planner/binder/binder.py, NodeType.CASE) —
     compared only `.physical` before deciding a branch didn't need a CAST.
     TIMESTAMP64 columns at different units share one physical tag, so a
     genuine unit mismatch slipped through uncast.
  2. UNION-leg coercion (opteryx/planner/binder/set_ops.py,
     `_cast_leg_columns_to`) — same bug, compared only `.category`.
  3. COALESCE/IFNULL/IFNOTNULL/IIF had NO branch-coercion step at all
     (binder.py, NodeType.FUNCTION) — nothing ever aligned their arguments.

All three now route a same-physical, different-descriptor pair through a real
CAST (`draken_cast_timestamp_rescale`) via the shared
`binder._descriptor_carries_meaning` / `_bound_cast_node` helpers — the same
mechanism that fixed the identical bug for DECIMAL scale (see
test_decimal_int_promotion.py / test_decimal_scale_fidelity.py).

Mixed-unit COMPARISON (`ts_ms = ts_us`) was never affected — binary_op_ctx
already carries left_unit/right_unit — and is checked here only as a sanity
control.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import datetime
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.connectors import DiskConnector

_ROW0 = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
_ROW1 = datetime.datetime(2024, 6, 7, 8, 9, 10, tzinfo=datetime.timezone.utc)
_EXPECTED = [_ROW0, _ROW1]


def _run(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is not None:
            out.extend(morsel[i] for i in range(morsel.num_rows))
    return out


def _make_fixture(tmp):
    """Write one parquet file with sibling TIMESTAMP columns at three units.

    PyArrow's `timestamp("s")` has no native Parquet physical encoding, so it
    round-trips as millisecond precision — hence `ts_s` also lands on `ms`.
    `ts_ms` and `ts_us` are the two genuinely distinct units under test.
    """
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "ts_ms": pa.array([_ROW0, _ROW1], type=pa.timestamp("ms")),
            "ts_us": pa.array([_ROW0, _ROW1], type=pa.timestamp("us")),
        }
    )
    data_dir = os.path.join(tmp, "ts_units_ws", "ts_units")
    os.makedirs(data_dir)
    pq.write_table(table, os.path.join(data_dir, "data.parquet"))
    return "ts_units_ws.ts_units"


def _with_fixture(fn):
    """Register the fixture workspace under cwd=tmp (matches the DiskConnector
    prefix-resolution convention used by test_flba_decimal.py), run `fn(table)`,
    and restore cwd on the way out."""
    with tempfile.TemporaryDirectory() as tmp:
        table = _make_fixture(tmp)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace("ts_units_ws", DiskConnector)
            fn(table)
        finally:
            os.chdir(cwd)


def test_case_blends_mismatched_timestamp_units():
    def go(table):
        for sql in (
            f"SELECT CASE WHEN id = 1 THEN ts_ms ELSE ts_us END AS x FROM {table}",
            f"SELECT CASE WHEN id = 1 THEN ts_us ELSE ts_ms END AS x FROM {table}",
        ):
            assert _run(sql) == [(v,) for v in _EXPECTED], sql

    _with_fixture(go)


def test_union_blends_mismatched_timestamp_units():
    def go(table):
        sql = f"SELECT ts_ms AS v FROM {table} UNION ALL SELECT ts_us AS v FROM {table}"
        assert _run(sql) == [(v,) for v in _EXPECTED * 2], sql

    _with_fixture(go)


def test_coalesce_family_blends_mismatched_timestamp_units():
    def go(table):
        for sql in (
            f"SELECT COALESCE(ts_ms, ts_us) AS x FROM {table}",
            f"SELECT COALESCE(ts_us, ts_ms) AS x FROM {table}",
            f"SELECT IFNULL(ts_ms, ts_us) AS x FROM {table}",
            f"SELECT IIF(id = 1, ts_ms, ts_us) AS x FROM {table}",
            f"SELECT IIF(id = 1, ts_us, ts_ms) AS x FROM {table}",
        ):
            assert _run(sql) == [(v,) for v in _EXPECTED], sql

    _with_fixture(go)


def test_mismatched_unit_comparison_was_never_broken():
    """Sanity control: binary_op_ctx already carries left_unit/right_unit, so a
    direct comparison across units was never the bug — only branch-selecting
    (CASE/UNION/COALESCE) was."""

    def go(table):
        assert _run(f"SELECT ts_ms = ts_us AS eq FROM {table}") == [(True,), (True,)]

    _with_fixture(go)


if __name__ == "__main__":
    test_case_blends_mismatched_timestamp_units()
    test_union_blends_mismatched_timestamp_units()
    test_coalesce_family_blends_mismatched_timestamp_units()
    test_mismatched_unit_comparison_was_never_broken()
    print("✅ TIMESTAMP unit-blend regression tests passed")
