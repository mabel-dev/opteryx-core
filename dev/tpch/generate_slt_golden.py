"""
Generate golden TPC-H results for the SLT result-checking battery.

DEV TOOLING — not packaged, not run at test time. Uses DuckDB as the oracle
over the SAME parquet files the engine reads, at scale factor 0.01
(testdata/tpch_<scale>). For each query it runs the query in DuckDB, normalises
the result, and writes it as the golden.

DuckDB-only by design: co-hosting DuckDB and Opteryx (mimalloc) in one process
is unstable, so verification of Opteryx against these goldens happens separately
in the test battery (Opteryx-only process). Queries DuckDB cannot parse
(Opteryx-dialect SQL, e.g. LEFT SEMI JOIN) are reported and left out.

Usage:
    python dev/tpch/generate_slt_golden.py
"""

import glob
import json
import os
import re
import sys

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "tests", "integration", "sql_battery"))

import duckdb  # dev/test oracle only — never imported by production code

from _tpch_golden import GOLDEN_PATH, TPCH_SCALE, normalize_rows

_QUERY_DIR = os.path.join(_REPO, "tests", "performance", "tpch", "opteryx", "queries")
_TABLES = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]


def _duckdb_connection():
    con = duckdb.connect()
    for table in _TABLES:
        files = glob.glob(os.path.join(_REPO, f"testdata/tpch_{TPCH_SCALE}/{table}/*.parquet"))
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet({files!r})")
    return con


def main() -> int:
    con = _duckdb_connection()
    golden = {}
    written, dialect = [], []

    for path in sorted(glob.glob(os.path.join(_QUERY_DIR, "query*.sql"))):
        qid = os.path.basename(path).replace(".sql", "")
        raw = open(path).read()
        duckdb_sql = re.sub(r"testdata\.tpch\.", "", raw)
        try:
            rows = con.execute(duckdb_sql).fetchall()
        except Exception as err:
            dialect.append((qid, str(err).splitlines()[0][:60]))
            continue
        golden[qid] = normalize_rows(rows)
        written.append(qid)

    os.makedirs(os.path.dirname(GOLDEN_PATH), exist_ok=True)
    with open(GOLDEN_PATH, "w") as handle:
        json.dump(golden, handle, indent=1, sort_keys=True)
        handle.write("\n")

    print(f"Golden written: {GOLDEN_PATH}")
    print(f"  duckdb goldens: {len(written)} -> {', '.join(written)}")
    print(f"  duckdb-can't-parse (excluded): {len(dialect)}")
    for qid, info in dialect:
        print(f"      {qid}: {info}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
