#!/usr/bin/env python3
"""
Generate the Star Schema Benchmark (SSB) dataset as parquet.

    python dev/ssb_generate.py <scale> [-j N]

Writes testdata/ssb_<scale>/<table>/<table>-NNN.parquet in the same layout as
the tpchgen-cli output, so dev/parquet_to_skene.py converts it unchanged.

Data comes from eyalroz/ssb-dbgen, pinned to SSB_DBGEN_COMMIT. SSB is NOT TPC-H
data: it is its own generator with its own tables (lineorder, customer,
supplier, part, date). The generator is a dev-only tool - downloaded and built
under scratch/ssb (not packaged, never imported by the engine).

Disk: the .tbl text is ~100 bytes/row of lineorder, so SF100 is ~60 GB of text.
It is never held whole - each lineorder chunk is generated, converted to
parquet and deleted inside one worker, so only -j chunks of text exist at once.

The parquet is written with DuckDB (dev tooling; the engine never imports it),
typed from doc/ssb.ddl: INTEGER for every numeric column, VARCHAR otherwise.
"""

import argparse
import concurrent.futures
import os
import shutil
import subprocess
import sys
import tarfile
import urllib.request

SSB_DBGEN_COMMIT = "ae1e254aa4d603d8ef1f44078e5abed011634b23"
SSB_DBGEN_URL = f"https://codeload.github.com/eyalroz/ssb-dbgen/tar.gz/{SSB_DBGEN_COMMIT}"

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOOL_ROOT = os.path.join(REPO_ROOT, "scratch", "ssb")
SRC_DIR = os.path.join(TOOL_ROOT, f"ssb-dbgen-{SSB_DBGEN_COMMIT}")
DBGEN = os.path.join(SRC_DIR, "build", "dbgen")
DISTS = os.path.join(SRC_DIR, "build", "dists.dss")

# table -> (dbgen -T letter, [(column, duckdb type)])  — from doc/ssb.ddl
INT, STR = "INTEGER", "VARCHAR"
TABLES = {
    "customer": ("c", [("c_custkey", INT), ("c_name", STR), ("c_address", STR), ("c_city", STR),
                       ("c_nation", STR), ("c_region", STR), ("c_phone", STR), ("c_mktsegment", STR)]),
    "supplier": ("s", [("s_suppkey", INT), ("s_name", STR), ("s_address", STR), ("s_city", STR),
                       ("s_nation", STR), ("s_region", STR), ("s_phone", STR)]),
    "part": ("p", [("p_partkey", INT), ("p_name", STR), ("p_mfgr", STR), ("p_category", STR),
                   ("p_brand1", STR), ("p_color", STR), ("p_type", STR), ("p_size", INT),
                   ("p_container", STR)]),
    "date": ("d", [("d_datekey", INT), ("d_date", STR), ("d_dayofweek", STR), ("d_month", STR),
                   ("d_year", INT), ("d_yearmonthnum", INT), ("d_yearmonth", STR),
                   ("d_daynuminweek", INT), ("d_daynuminmonth", INT), ("d_daynuminyear", INT),
                   ("d_monthnuminyear", INT), ("d_weeknuminyear", INT), ("d_sellingseason", STR),
                   ("d_lastdayinweekfl", STR), ("d_lastdayinmonthfl", STR), ("d_holidayfl", STR),
                   ("d_weekdayfl", STR)]),
    "lineorder": ("l", [("lo_orderkey", INT), ("lo_linenumber", INT), ("lo_custkey", INT),
                        ("lo_partkey", INT), ("lo_suppkey", INT), ("lo_orderdate", INT),
                        ("lo_orderpriority", STR), ("lo_shippriority", STR), ("lo_quantity", INT),
                        ("lo_extendedprice", INT), ("lo_ordertotalprice", INT), ("lo_discount", INT),
                        ("lo_revenue", INT), ("lo_supplycost", INT), ("lo_tax", INT),
                        ("lo_commitdate", INT), ("lo_shipmode", STR)]),
}

LINEORDER_CHUNKS = 16  # matches the tpchgen-cli --parts 16 the TPC-H mirrors use


def ensure_dbgen() -> None:
    if os.path.isfile(DBGEN) and os.path.isfile(DISTS):
        return
    os.makedirs(TOOL_ROOT, exist_ok=True)
    if not os.path.isdir(SRC_DIR):
        print(f"Downloading eyalroz/ssb-dbgen @ {SSB_DBGEN_COMMIT[:7]} ...")
        tarball = os.path.join(TOOL_ROOT, "ssb-dbgen.tar.gz")
        urllib.request.urlretrieve(SSB_DBGEN_URL, tarball)
        with tarfile.open(tarball) as tf:
            tf.extractall(TOOL_ROOT)
        os.remove(tarball)
    # EOL_HANDLING=ON: no trailing '|' on each line, so the column list is exact.
    subprocess.run(
        ["cmake", "-S", SRC_DIR, "-B", os.path.join(SRC_DIR, "build"),
         "-DCMAKE_BUILD_TYPE=Release", "-DEOL_HANDLING=ON"],
        check=True,
    )
    subprocess.run(["cmake", "--build", os.path.join(SRC_DIR, "build"), "-j8"], check=True)


def to_parquet(tbl_path: str, columns: list[tuple[str, str]], out_path: str) -> int:
    import duckdb

    col_spec = ", ".join(f"'{name}': '{typ}'" for name, typ in columns)
    con = duckdb.connect()
    # Write to a temp name then rename, so a killed run never leaves a
    # truncated parquet that looks like a finished one.
    tmp = out_path + ".tmp"
    con.execute(
        f"COPY (SELECT * FROM read_csv('{tbl_path}', delim='|', header=false, quote='', "
        f"columns={{{col_spec}}})) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 65536)"
    )
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
    os.rename(tmp, out_path)
    return rows


def build_table(table: str, scale: str, out_dir: str, chunk: int | None) -> tuple[str, int]:
    letter, columns = TABLES[table]
    work = os.path.join(TOOL_ROOT, f"work_{scale}_{table}_{chunk or 0}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    try:
        cmd = [DBGEN, "-b", DISTS, "-s", scale, "-T", letter, "-f", "-q"]
        if chunk is not None:
            cmd += ["-C", str(LINEORDER_CHUNKS), "-S", str(chunk)]
        subprocess.run(cmd, cwd=work, check=True)
        tbl = os.path.join(work, f"{table}.tbl" + (f".{chunk}" if chunk is not None else ""))
        part = f"{table}-{chunk:03d}.parquet" if chunk is not None else f"{table}-000.parquet"
        return part, to_parquet(tbl, columns, os.path.join(out_dir, part))
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate SSB parquet via eyalroz/ssb-dbgen")
    parser.add_argument("scale", help="Scale factor (integer): 1, 10, 100, ...")
    parser.add_argument("-j", type=int, default=min(8, os.cpu_count() or 1), help="Parallel workers")
    args = parser.parse_args()

    root = os.path.join(REPO_ROOT, "testdata", f"ssb_{args.scale}")
    if os.path.exists(root):
        print(f"ERROR: {root} already exists - remove it to regenerate")
        return 1

    ensure_dbgen()
    staging = root + ".partial"
    shutil.rmtree(staging, ignore_errors=True)
    for table in TABLES:
        os.makedirs(os.path.join(staging, table))

    jobs: list[tuple[str, int | None]] = [(t, None) for t in TABLES if t != "lineorder"]
    jobs += [("lineorder", c) for c in range(1, LINEORDER_CHUNKS + 1)]

    totals: dict[str, int] = dict.fromkeys(TABLES, 0)
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.j) as pool:
        futures = {
            pool.submit(build_table, t, args.scale, os.path.join(staging, t), c): (t, c)
            for t, c in jobs
        }
        for fut in concurrent.futures.as_completed(futures):
            table, _ = futures[fut]
            part, rows = fut.result()  # a failed chunk raises here: no partial dataset is published
            totals[table] += rows
            print(f"  {table}/{part}: {rows:,} rows")

    os.rename(staging, root)
    print(f"\nWrote {root}")
    for table, rows in totals.items():
        print(f"  {table:<10} {rows:>14,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
