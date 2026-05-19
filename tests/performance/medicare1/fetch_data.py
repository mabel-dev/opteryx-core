"""
Public BI Benchmark (Medicare1) — data + query fetcher.

Downloads:
  1. Medicare1 CSV data (two tables, ~200MB bzip2 compressed each) from
     event.cwi.nl/da/PublicBIbenchmark/Medicare1/
  2. The 10 Medicare1 query files from cwida/public_bi_benchmark.

Converts each CSV to a single Parquet file under testdata/medicare1/<table>/<table>.parquet
using SNAPPY compression.

Idempotent: skips downloads / conversions whose outputs already exist.

Dev dependency
--------------
This script uses PyArrow as a one-shot dev tool to write Parquet files.
This is NOT engine usage of PyArrow — it never runs at benchmark time and
is not imported by run.py. The build-time PyArrow scan only flags
production code paths under opteryx/draken/rugo, not tests/.

Usage
-----
    python tests/performance/medicare1/fetch_data.py            # fetch everything
    python tests/performance/medicare1/fetch_data.py --queries  # queries only
    python tests/performance/medicare1/fetch_data.py --data     # data only
    python tests/performance/medicare1/fetch_data.py --force-convert  # re-emit Parquet
"""

from __future__ import annotations

import argparse
import bz2
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MEDICARE_DIR = Path(__file__).resolve().parent
QUERIES_DIR = MEDICARE_DIR / "queries"
DATA_DIR = ROOT / "testdata" / "medicare1"
DOWNLOAD_DIR = ROOT / "testdata" / "_downloads" / "medicare1"

TABLES = ["Medicare1_1", "Medicare1_2"]
DATASET_BASE_URL = "http://event.cwi.nl/da/PublicBIbenchmark/Medicare1"
QUERY_RAW_BASE = (
    "https://raw.githubusercontent.com/cwida/public_bi_benchmark/master/benchmark/Medicare1/queries/"
)

# Column names for both tables (from the benchmark schema)
COLUMNS = [
    "BENE_COUNT",
    "BENE_COUNT_GE65",
    "BENE_COUNT_GE65_REDACT_FLAG",
    "Calculation_3170826185336909",
    "Calculation_3170826185505725",
    "Calculation_7130826185400024",
    "Calculation_9030826185528129",
    "DESCRIPTION_FLAG",
    "DRUG_NAME",
    "GE65_REDACT_FLAG",
    "GENERIC_NAME",
    "NPI",
    "NPPES_PROVIDER_CITY",
    "NPPES_PROVIDER_FIRST_NAME",
    "NPPES_PROVIDER_LAST_ORG_NAME",
    "NPPES_PROVIDER_STATE",
    "Number of Records",
    "SPECIALTY_DESC",
    "TOTAL_CLAIM_COUNT",
    "TOTAL_CLAIM_COUNT_GE65",
    "TOTAL_DAY_SUPPLY",
    "TOTAL_DAY_SUPPLY_GE65",
    "TOTAL_DRUG_COST",
    "TOTAL_DRUG_COST_GE65",
    "Calculation_6710826185428006",
    "Avg Day Supply/Bene (bin)",
]


def _http_download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    print(f"  downloading {url}")
    print(f"  -> {dest}")
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        total = int(resp.headers.get("Content-Length") or 0)
        read = 0
        chunk = 1 << 20  # 1 MiB
        last_pct = -1
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            out.write(buf)
            read += len(buf)
            if total:
                pct = (read * 100) // total
                if pct != last_pct and pct % 5 == 0:
                    print(
                        f"    {pct:3d}%  ({read / 1e9:.2f} / {total / 1e9:.2f} GB)"
                    )
                    last_pct = pct
    tmp.rename(dest)


def fetch_and_decompress_data() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for table in TABLES:
        url = f"{DATASET_BASE_URL}/{table}.csv.bz2"
        bz2_path = DOWNLOAD_DIR / f"{table}.csv.bz2"
        csv_path = DOWNLOAD_DIR / f"{table}.csv"

        if csv_path.exists():
            print(f"[data] CSV already present: {csv_path}")
            continue

        if not bz2_path.exists():
            print(f"[data] fetching {table}.csv.bz2")
            _http_download(url, bz2_path)

        print(f"[data] decompressing {bz2_path.name}")
        with bz2.open(bz2_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            f_out.write(f_in.read())
        print(f"      -> {csv_path}")


def convert_to_parquet(force: bool = False) -> None:
    try:
        import pandas as pd  # type: ignore
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        sys.exit(
            "pandas and pyarrow are required for CSV->Parquet conversion. "
            "Install them: `pip install pandas pyarrow`. "
            "(Used only by this fetch script — never at benchmark time.)"
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    pending = []
    for table in TABLES:
        out_dir = DATA_DIR / table
        out_path = out_dir / f"{table}.parquet"
        if out_path.exists() and not force:
            print(f"[data] Parquet already present: {out_path}")
            continue
        pending.append((table, out_dir, out_path))

    if not pending:
        print(
            f"[data] all Parquet files already present in {DATA_DIR} "
            f"(use --force-convert to re-emit)"
        )
        return

    print(f"[data] converting {len(pending)} CSV(s) -> Parquet (SNAPPY)")
    for table, out_dir, out_path in pending:
        csv_path = DOWNLOAD_DIR / f"{table}.csv"
        if not csv_path.exists():
            print(f"  ! missing source CSV: {csv_path}")
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        if out_path.exists():
            out_path.unlink()

        # Read CSV with pandas; Medicare1 uses pipe (|) delimiter
        # The CSV has no header row, so we provide the column names
        try:
            df = pd.read_csv(csv_path, delimiter="|", header=None, names=COLUMNS, low_memory=False)
            num_rows = len(df)
            # Convert to PyArrow Table and write to Parquet
            table_obj = pa.Table.from_pandas(df)
            pq.write_table(table_obj, out_path.as_posix(), compression="snappy")
            print(f"  {table:<20} {num_rows:>10,} rows -> {out_path.relative_to(ROOT)}")
        except Exception as e:
            print(f"  ! error converting {csv_path}: {e}")
            continue

    print(f"[data] done -> {DATA_DIR}")


def fetch_queries() -> None:
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(p.name for p in QUERIES_DIR.glob("*.sql"))
    if len(existing) >= 10:
        print(f"[queries] {len(existing)} query files already present")
        return

    print(f"[queries] downloading 10 query files")
    for i in range(1, 11):
        name = f"{i}.sql"
        out = QUERIES_DIR / name
        if out.exists():
            continue
        url = QUERY_RAW_BASE + name
        try:
            with urllib.request.urlopen(url) as r:
                out.write_bytes(r.read())
                print(f"  {name}")
        except Exception as e:
            print(f"  ! failed to fetch {name}: {e}")
    print(f"[queries] done -> {QUERIES_DIR}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", action="store_true", help="fetch + convert Medicare1 only"
    )
    parser.add_argument(
        "--queries", action="store_true", help="fetch query files only"
    )
    parser.add_argument(
        "--force-convert",
        action="store_true",
        help="re-emit Parquet files even if they already exist",
    )
    args = parser.parse_args()

    do_data = args.data or not args.queries
    do_queries = args.queries or not args.data

    if do_queries:
        fetch_queries()
    if do_data:
        fetch_and_decompress_data()
        convert_to_parquet(force=args.force_convert)

    print("[ok] Medicare1 fixtures ready. Run `make medicare1` to execute the benchmark.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
