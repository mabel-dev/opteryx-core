"""DuckDB-comparison harness for native temporal binary arithmetic.

Verifies Opteryx == DuckDB for:
  - date/timestamp ± interval (month day-clamping, leap years, year rollover)
  - interval ± interval (via a date anchor)
  - date − date, timestamp − timestamp (delta), NULL propagation

DuckDB + PyArrow are used here only for test-data generation / oracle comparison
(allowed in dev/, banned in the engine). Run from the repo root:

    python3 dev/temporal_arith_duckdb_harness.py
"""

import datetime
import os
import sys
import tempfile

sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import opteryx

# --- edge-case fixtures: month-ends, leap years, pre-epoch, NULLs ---
DATES = [
    datetime.date(2020, 1, 31), datetime.date(2021, 1, 31), datetime.date(2020, 2, 29),
    datetime.date(2019, 3, 31), datetime.date(2020, 12, 31), datetime.date(2000, 2, 29),
    datetime.date(1969, 12, 31), datetime.date(1970, 1, 1), None,
]
DATES2 = [datetime.date(2020, 1, 1)] * 9
TS = [
    datetime.datetime(2020, 1, 31, 12, 30, 45), datetime.datetime(2021, 1, 31, 23, 59, 59),
    datetime.datetime(2020, 2, 29, 0, 0, 0), datetime.datetime(2019, 3, 31, 6, 0, 0),
    datetime.datetime(2020, 12, 31, 18, 0, 0), datetime.datetime(2000, 2, 29, 1, 2, 3),
    datetime.datetime(1969, 12, 31, 23, 0, 0), datetime.datetime(1970, 1, 1, 0, 0, 0), None,
]
TS2 = [datetime.datetime(2020, 1, 1, 0, 0, 0)] * 9


def _build(tmp):
    tbl = pa.table({
        "d": pa.array(DATES, pa.date32()),
        "d2": pa.array(DATES2, pa.date32()),
        "ts": pa.array(TS, pa.timestamp("us")),
        "ts2": pa.array(TS2, pa.timestamp("us")),
    })
    ds_dir = os.path.join(tmp, "edgeds")
    os.makedirs(ds_dir, exist_ok=True)
    flat = os.path.join(tmp, "edge.parquet")
    pq.write_table(tbl, flat)
    pq.write_table(tbl, os.path.join(ds_dir, "data.parquet"))
    return flat, ds_dir


def _norm(v):
    if isinstance(v, datetime.datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, datetime.timedelta):
        return int(v.total_seconds() * 1_000_000)
    return v


def main():
    tmp = tempfile.mkdtemp()
    flat, ds_dir = _build(tmp)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{flat}')")
    sess = opteryx.session()

    def opx(sql):
        rows = []
        for m in sess.execute_to_morsels(sql):
            cols = {nm: getattr(m.column(nm), "_nb", m.column(nm)).to_pylist()
                    for nm in m.column_names}
            n = len(next(iter(cols.values())))
            for i in range(n):
                rows.append(tuple(cols[nm][i] for nm in m.column_names))
        return rows

    def ddb(sql):
        return con.execute(sql).fetchall()

    allok = True

    # date/timestamp ± interval — direct equality (engine TIMESTAMP, DuckDB TIMESTAMP)
    direct = [
        ("d + 1mo", "SELECT d + INTERVAL '1' MONTH AS r FROM {t}"),
        ("d - 1mo", "SELECT d - INTERVAL '1' MONTH AS r FROM {t}"),
        ("d + 13mo", "SELECT d + INTERVAL '13' MONTH AS r FROM {t}"),
        ("d + 1day", "SELECT d + INTERVAL '1' DAY AS r FROM {t}"),
        ("d - 1day", "SELECT d - INTERVAL '1' DAY AS r FROM {t}"),
        ("ts + 1mo", "SELECT ts + INTERVAL '1' MONTH AS r FROM {t}"),
        ("ts - 1mo", "SELECT ts - INTERVAL '1' MONTH AS r FROM {t}"),
        ("ts + 90min", "SELECT ts + INTERVAL '90' MINUTE AS r FROM {t}"),
        ("d + (1mo+10day)", "SELECT d + (INTERVAL '1' MONTH + INTERVAL '10' DAY) AS r FROM {t}"),
        ("d + (5mo-2mo)", "SELECT d + (INTERVAL '5' MONTH - INTERVAL '2' MONTH) AS r FROM {t}"),
    ]
    for label, tmpl in direct:
        o = [tuple(_norm(x) for x in r) for r in opx(tmpl.format(t=f"'{ds_dir}'"))]
        e = [tuple(_norm(x) for x in r) for r in ddb(tmpl.format(t="t"))]
        ok = o == e
        allok &= ok
        print(("PASS" if ok else "FAIL"), label)
        if not ok:
            for i, (a, b) in enumerate(zip(o, e)):
                if a != b:
                    print("   row", i, "opx=", a, "ddb=", b)

    # date − date / timestamp − timestamp: engine INTERVAL(0, µs) vs DuckDB BIGINT/timedelta
    o = opx(f"SELECT d - d2 AS r FROM '{ds_dir}'")
    e = ddb("SELECT d - d2 AS r FROM t")
    for i, (a, b) in enumerate(zip(o, e)):
        av, bv = a[0], b[0]
        if av is None:
            ok = bv is None
        else:
            mo, us = av
            ok = mo == 0 and bv is not None and abs(us / 86_400_000_000 - bv) < 1e-9
        allok &= ok
        print(("PASS" if ok else "FAIL"), f"d-d2 row{i}")

    o = opx(f"SELECT ts - ts2 AS r FROM '{ds_dir}'")
    e = ddb("SELECT ts - ts2 AS r FROM t")
    for i, (a, b) in enumerate(zip(o, e)):
        av, bv = a[0], b[0]
        if av is None:
            ok = bv is None
        else:
            mo, us = av
            ok = mo == 0 and us == int(bv.total_seconds() * 1_000_000)
        allok &= ok
        print(("PASS" if ok else "FAIL"), f"ts-ts2 row{i}")

    print("ALL PASS" if allok else "SOME FAILED")
    return 0 if allok else 1


if __name__ == "__main__":
    raise SystemExit(main())
