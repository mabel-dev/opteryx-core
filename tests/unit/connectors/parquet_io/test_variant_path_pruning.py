"""Filters on VARIANT paths pushed into the Parquet scan, and the row groups they skip
(Parts D and E of docs/parquet_variant_path_read_design.md).

A `->>` path the scan reads is admitted to pushdown as text (FEATURE_DISABLE_VARIANT_PATH_PUSHDOWN),
and its filter is tested on the path's destination leaf statistics - but only in a row group
whose field's `value` chunk holds nothing, where the leaf's values ARE the path's. Every test
compares the answers with the switch on and off.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import duckdb

import opteryx
from opteryx import config
from rugo import parquet as rp

KINDS = ["account", "commit", "identity"]


def _write(tmp_path, docs, name="v", rows_per_group=50):
    src = tmp_path / (name + ".jsonl")
    src.write_text("\n".join(json.dumps(d, separators=(",", ":")) for d in docs) + "\n")
    morsel = next(iter(opteryx.session().execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{src}', as_variant => 'data')"
    )))
    path = str(tmp_path / (name + ".parquet"))
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsel, max_rows_per_row_group=rows_per_group))
    return path


def _run(sql, pushdown):
    config.features.disable_variant_path_pushdown = not pushdown
    try:
        session = opteryx.session()
        rows = []
        for morsel in session.execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
        readings = [r for r in session.telemetry.get("operations", {}).values() if "row_groups_read" in r]
        assert len(readings) == 1, readings
        session.close()
        return sorted(rows, key=repr), readings[0]
    finally:
        config.features.disable_variant_path_pushdown = False


def _both(sql):
    on, on_reading = _run(sql, True)
    off, off_reading = _run(sql, False)
    assert on == off
    assert off_reading.get("row_groups_pruned", 0) == 0
    return on, on_reading


def _excluded(path, leaf, test):
    """Row groups whose `leaf` statistics rule the filter out: what the scan should skip.
    (The writer rounds a row group up to a multiple of 8 rows, so counts are read, not assumed.)"""
    stats = duckdb.sql(
        f"SELECT stats_min_value, stats_max_value FROM parquet_metadata('{path}') "
        f"WHERE path_in_schema = '{leaf}'"
    ).fetchall()
    return sum(1 for lo, hi in stats if test(lo, hi))


KIND = "data, typed_value, kind, typed_value"
# Sorted by kind: 200 of each.
SORTED = [{"i": i, "kind": KINDS[i // 200], "n": f"n{i % 7}"} for i in range(600)]


def test_a_path_filter_is_pushed_and_skips_row_groups(tmp_path):
    path = _write(tmp_path, SORTED)
    explain = list(opteryx.session().execute_to_morsels(
        f"EXPLAIN SELECT data->>'i' FROM READ_PARQUET('{path}') WHERE data->>'kind' = 'identity'"
    ))[0]
    plan = [r.decode() if type(r) is bytes else r for r in explain.column(explain.column_names[0]).to_pylist()]
    assert not any("Filter" in line for line in plan), plan
    rows, reading = _both(f"SELECT data->>'i' FROM READ_PARQUET('{path}') WHERE data->>'kind' = 'identity'")
    assert len(rows) == 200
    skipped = _excluded(path, KIND, lambda lo, hi: not lo <= "identity" <= hi)
    assert skipped > 0 and reading["row_groups_pruned"] == skipped, reading


def test_in_lists_and_ranges(tmp_path):
    path = _write(tmp_path, SORTED)
    rows, reading = _both(
        f"SELECT COUNT(*) FROM READ_PARQUET('{path}') WHERE data->>'kind' IN ('account', 'identity')"
    )
    skipped = _excluded(path, KIND, lambda lo, hi: not any(lo <= v <= hi for v in ("account", "identity")))
    assert rows == [(400,)] and skipped > 0 and reading["row_groups_pruned"] == skipped, reading
    rows, reading = _both(f"SELECT COUNT(*) FROM READ_PARQUET('{path}') WHERE data->>'kind' > 'commit'")
    skipped = _excluded(path, KIND, lambda lo, hi: hi <= "commit")
    assert rows == [(200,)] and skipped > 0 and reading["row_groups_pruned"] == skipped, reading


def test_a_row_group_whose_value_holds_data_is_not_skipped(tmp_path):
    # Shredding is learnt from the first row group (strings). The second holds `kind` as a
    # number in one row, so its `value` is not empty: its leaf statistics ("a".."a") do not
    # cover that row, which `->>` reads as '5', and it must not be skipped.
    docs = [{"kind": "a"} for _ in range(16)]
    docs[13]["kind"] = 5
    path = _write(tmp_path, docs, rows_per_group=8)
    assert duckdb.sql(f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{path}')").fetchall() == [(2,)]
    rows, reading = _both(f"SELECT data->>'kind' FROM READ_PARQUET('{path}') WHERE data->>'kind' = '5'")
    assert rows == [("5",)]
    # The first row group (all "a", `value` empty) is skipped; the second is read.
    assert reading["row_groups_pruned"] == 1, reading


def test_a_nested_path(tmp_path):
    docs = [{"commit": {"collection": f"c{i // 100}"}} for i in range(400)]
    path = _write(tmp_path, docs)
    rows, reading = _both(
        f"SELECT COUNT(*) FROM READ_PARQUET('{path}') WHERE data->'commit'->>'collection' = 'c2'"
    )
    leaf = "data, typed_value, commit, typed_value, collection, typed_value"
    skipped = _excluded(path, leaf, lambda lo, hi: not lo <= "c2" <= hi)
    assert rows == [(100,)] and skipped > 0 and reading["row_groups_pruned"] == skipped, reading


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
