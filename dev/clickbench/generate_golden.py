"""
Generate golden ClickBench results for the result-checking battery.

DEV TOOLING — not packaged, not run at test time. Uses DuckDB as the oracle
over the SAME parquet files the engine reads. For each query it runs the DuckDB
phrasing (from the duckdb clickbench runner), normalises the result, and writes
it as the golden keyed by ``qNN``.

DuckDB-only by design: co-hosting DuckDB and Opteryx (mimalloc) in one process
is unstable, so verification of Opteryx against these goldens happens separately
in tests/integration/sql_battery/test_battery_clickbench_results.py (Opteryx-only
process).

Usage:
    python dev/clickbench/generate_golden.py [--dataset tiny|full]
"""

import argparse
import glob
import json
import os
import sys

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "tests", "integration", "sql_battery"))
sys.path.insert(0, os.path.join(_REPO, "tests", "performance", "clickbench", "duckdb"))

import duckdb  # dev/test oracle only — never imported by production code

from _clickbench_golden import DATASETS, golden_path, normalize_rows
from runner import QUERIES  # duckdb-dialect ClickBench statements, index-aligned


def _connection(parquet_glob: str):
    con = duckdb.connect()
    con.execute("SET parquet_metadata_cache=true")
    con.execute(f"""
        CREATE VIEW hits AS
        SELECT * REPLACE (make_date(EventDate) AS EventDate)
        FROM read_parquet('{parquet_glob}', binary_as_string=True)
    """)
    con.execute("CREATE MACRO toDateTime(t) AS epoch_ms(CAST(t AS BIGINT) * 1000)")
    return con


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate ClickBench golden results")
    parser.add_argument("--dataset", default="tiny", choices=sorted(DATASETS))
    args = parser.parse_args()

    _, glob_rel = DATASETS[args.dataset]
    parquet_glob = os.path.join(_REPO, glob_rel)
    files = glob.glob(parquet_glob)
    if not files:
        print(f"ERROR: no parquet files match {parquet_glob}", file=sys.stderr)
        return 1

    con = _connection(parquet_glob)
    golden = {}
    written, errored = [], []

    for idx, query in enumerate(QUERIES, start=1):
        qid = f"q{idx:02d}"
        try:
            rows = con.execute(query).fetchall()
        except Exception as err:
            errored.append((qid, str(err).splitlines()[0][:80]))
            continue
        golden[qid] = normalize_rows(rows)
        written.append(qid)

    con.close()

    out = golden_path(args.dataset)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as handle:
        json.dump(golden, handle, indent=1, sort_keys=True)
        handle.write("\n")

    print(f"Golden written: {out}")
    print(f"  dataset       : {args.dataset} ({len(files)} parquet files)")
    print(f"  goldens       : {len(written)}")
    print(f"  duckdb errors : {len(errored)}")
    for qid, info in errored:
        print(f"      {qid}: {info}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
