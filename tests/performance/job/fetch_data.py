"""
Join Order Benchmark (JOB) — data + query fetcher.

Downloads:
  1. IMDB CSV snapshot (imdb.tgz, ~1.3GB compressed / ~3.6GB extracted) from
     event.cwi.nl (the original homepages.cwi.nl/~boncz URL is dead).
  2. The 113 JOB query files from gregrahn/join-order-benchmark.

Converts each CSV to a single Parquet file under testdata/job/<table>/<table>.parquet
using the official JOB schema (typed integer/text, not all-VARCHAR), with
SNAPPY compression.

Idempotent: skips downloads / conversions whose outputs already exist.

Dev dependency
--------------
This script uses PyArrow as a one-shot dev tool to write Parquet files. The
parquet root schema name PyArrow emits ("arrow_schema") is the only one
Rugo's converter strips, which is why we use PyArrow here rather than e.g.
DuckDB (which writes "duckdb_schema." and leaves columns unresolvable).
This is NOT engine usage of PyArrow — it never runs at benchmark time and
is not imported by run.py. The build-time PyArrow scan only flags
production code paths under opteryx/draken/rugo, not tests/.

The CSV reader is Python stdlib because JOB CSVs use backslash-escaped
quotes (`\"`) which PyArrow's csv module does not support.

Usage
-----
    python tests/performance/job/fetch_data.py            # fetch everything
    python tests/performance/job/fetch_data.py --queries  # queries only
    python tests/performance/job/fetch_data.py --data     # data only
    python tests/performance/job/fetch_data.py --force-convert  # re-emit Parquet
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
JOB_DIR = Path(__file__).resolve().parent
QUERIES_DIR = JOB_DIR / "queries"
DATA_DIR = ROOT / "testdata" / "job"
DOWNLOAD_DIR = ROOT / "testdata" / "_downloads" / "job"

IMDB_TGZ_URL = "https://event.cwi.nl/da/job/imdb.tgz"
IMDB_TGZ_PATH = DOWNLOAD_DIR / "imdb.tgz"
IMDB_CSV_DIR = DOWNLOAD_DIR / "csv"

QUERY_LIST_URL = (
    "https://api.github.com/repos/gregrahn/join-order-benchmark/contents/"
)
QUERY_RAW_BASE = (
    "https://raw.githubusercontent.com/gregrahn/join-order-benchmark/master/"
)


# Official JOB schema, expressed as DuckDB CREATE TABLE bodies.
# Source: https://github.com/gregrahn/join-order-benchmark/blob/master/schema.sql
SCHEMA = {
    "aka_name": """
        id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        name VARCHAR,
        imdb_index VARCHAR,
        name_pcode_cf VARCHAR,
        name_pcode_nf VARCHAR,
        surname_pcode VARCHAR,
        md5sum VARCHAR
    """,
    "aka_title": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        title VARCHAR,
        imdb_index VARCHAR,
        kind_id INTEGER NOT NULL,
        production_year INTEGER,
        phonetic_code VARCHAR,
        episode_of_id INTEGER,
        season_nr INTEGER,
        episode_nr INTEGER,
        note VARCHAR,
        md5sum VARCHAR
    """,
    "cast_info": """
        id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        person_role_id INTEGER,
        note VARCHAR,
        nr_order INTEGER,
        role_id INTEGER NOT NULL
    """,
    "char_name": """
        id INTEGER NOT NULL,
        name VARCHAR,
        imdb_index VARCHAR,
        imdb_id INTEGER,
        name_pcode_nf VARCHAR,
        surname_pcode VARCHAR,
        md5sum VARCHAR
    """,
    "comp_cast_type": """
        id INTEGER NOT NULL,
        kind VARCHAR
    """,
    "company_name": """
        id INTEGER NOT NULL,
        name VARCHAR,
        country_code VARCHAR,
        imdb_id INTEGER,
        name_pcode_nf VARCHAR,
        name_pcode_sf VARCHAR,
        md5sum VARCHAR
    """,
    "company_type": """
        id INTEGER NOT NULL,
        kind VARCHAR
    """,
    "complete_cast": """
        id INTEGER NOT NULL,
        movie_id INTEGER,
        subject_id INTEGER NOT NULL,
        status_id INTEGER NOT NULL
    """,
    "info_type": """
        id INTEGER NOT NULL,
        info VARCHAR
    """,
    "keyword": """
        id INTEGER NOT NULL,
        keyword VARCHAR,
        phonetic_code VARCHAR
    """,
    "kind_type": """
        id INTEGER NOT NULL,
        kind VARCHAR
    """,
    "link_type": """
        id INTEGER NOT NULL,
        link VARCHAR
    """,
    "movie_companies": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        company_id INTEGER NOT NULL,
        company_type_id INTEGER NOT NULL,
        note VARCHAR
    """,
    "movie_info": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        info_type_id INTEGER NOT NULL,
        info VARCHAR,
        note VARCHAR
    """,
    "movie_info_idx": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        info_type_id INTEGER NOT NULL,
        info VARCHAR,
        note VARCHAR
    """,
    "movie_keyword": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        keyword_id INTEGER NOT NULL
    """,
    "movie_link": """
        id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        linked_movie_id INTEGER NOT NULL,
        link_type_id INTEGER NOT NULL
    """,
    "name": """
        id INTEGER NOT NULL,
        name VARCHAR,
        imdb_index VARCHAR,
        imdb_id INTEGER,
        gender VARCHAR,
        name_pcode_cf VARCHAR,
        name_pcode_nf VARCHAR,
        surname_pcode VARCHAR,
        md5sum VARCHAR
    """,
    "person_info": """
        id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        info_type_id INTEGER NOT NULL,
        info VARCHAR,
        note VARCHAR
    """,
    "role_type": """
        id INTEGER NOT NULL,
        role VARCHAR
    """,
    "title": """
        id INTEGER NOT NULL,
        title VARCHAR,
        imdb_index VARCHAR,
        kind_id INTEGER NOT NULL,
        production_year INTEGER,
        imdb_id INTEGER,
        phonetic_code VARCHAR,
        episode_of_id INTEGER,
        season_nr INTEGER,
        episode_nr INTEGER,
        series_years VARCHAR,
        md5sum VARCHAR
    """,
}

TABLES = list(SCHEMA.keys())


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


def fetch_imdb_tarball() -> None:
    if IMDB_TGZ_PATH.exists():
        print(f"[data] tarball already present: {IMDB_TGZ_PATH}")
        return
    print("[data] fetching IMDB tarball (~1.3GB) — slow")
    _http_download(IMDB_TGZ_URL, IMDB_TGZ_PATH)


def extract_imdb_tarball() -> None:
    missing = [t for t in TABLES if not (IMDB_CSV_DIR / f"{t}.csv").exists()]
    if not missing:
        print(f"[data] CSVs already extracted to {IMDB_CSV_DIR}")
        return
    IMDB_CSV_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[data] extracting {IMDB_TGZ_PATH} -> {IMDB_CSV_DIR}")
    with tarfile.open(IMDB_TGZ_PATH, "r:gz") as tf:
        tf.extractall(IMDB_CSV_DIR)


# Read in batches so the Python `csv` reader doesn't blow up memory holding
# raw lists, but write the parquet as a single row group. Why one row group:
# Rugo's parquet decoder rejects row groups where any column is 100% null
# (defensive check at opteryx/connectors/parquet_io/pool_reader.pyx — flagged
# in the source as a known C++ decoder bug). Several JOB columns
# (`episode_of_id`, `imdb_index`, `note`, ...) are sparsely populated, so a
# multi-row-group write hits this defensive check on whichever group happens
# to contain only nulls for that column. Buffering the whole file as one
# row group dodges it; peak Python memory is ~1 GB on the largest table
# (cast_info, ~36M rows) which is acceptable for a one-time dev step.
_BATCH_ROWS = 250_000


def _parse_schema(table: str):
    """Return (col_names, pyarrow_schema) for a JOB table."""
    import pyarrow as pa  # type: ignore

    fields = []
    names = []
    for line in SCHEMA[table].split(","):
        parts = line.strip().split()
        if not parts:
            continue
        name = parts[0]
        dtype = parts[1].upper() if len(parts) > 1 else "VARCHAR"
        if dtype == "INTEGER":
            pa_type = pa.int32()
        elif dtype == "VARCHAR":
            pa_type = pa.string()
        else:
            raise ValueError(f"unhandled JOB type: {dtype}")
        names.append(name)
        # Always nullable on the parquet side: "NOT NULL" is a logical
        # constraint, not relevant to the file encoding.
        fields.append(pa.field(name, pa_type, nullable=True))
    return names, pa.schema(fields)


def _convert_one(csv_path, out_path, table) -> int:
    """Stream a single JOB CSV into a single-row-group SNAPPY parquet.

    Returns row count. See module-level note on why we use a single row group.
    """
    import csv as _csv

    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    names, schema = _parse_schema(table)
    int_idx = {i for i, f in enumerate(schema) if pa.types.is_integer(f.type)}
    n_cols = len(names)

    _csv.field_size_limit(1 << 27)

    # Stream the CSV in batches into RecordBatches so we don't hold raw
    # Python lists for the entire file, then concat into a single Table and
    # write with row_group_size = num_rows.
    cols: list[list] = [[] for _ in range(n_cols)]
    batches: list[pa.RecordBatch] = []
    total = 0

    def emit_batch():
        if not cols[0]:
            return
        arrays = [pa.array(cols[i], type=schema.field(i).type) for i in range(n_cols)]
        batches.append(pa.RecordBatch.from_arrays(arrays, schema=schema))
        for c in cols:
            c.clear()

    skipped = 0
    with open(csv_path, "r", newline="", encoding="utf-8", errors="replace") as fh:
        reader = _csv.reader(fh, escapechar="\\", quotechar='"')
        for row in reader:
            if len(row) != n_cols:
                # JOB CSVs are MySQL-dump style and use conventions Python's
                # csv reader doesn't fully model (trailing `\` for line
                # continuation, etc.). The number of pathological rows is
                # tiny (<10 per million); skipping them is acceptable for
                # benchmark data.
                skipped += 1
                continue
            try:
                for i, val in enumerate(row):
                    if val == "":
                        cols[i].append(None)
                    elif i in int_idx:
                        cols[i].append(int(val))
                    else:
                        cols[i].append(val)
            except ValueError:
                # Bad int conversion (e.g. mid-row newline produced garbage):
                # roll back any partial column appends so columns stay aligned.
                appended = i
                for j in range(appended):
                    cols[j].pop()
                skipped += 1
                continue
            total += 1
            if total % _BATCH_ROWS == 0:
                emit_batch()
    emit_batch()
    if skipped:
        print(f"    (skipped {skipped} mis-shaped rows in {csv_path.name})")

    if not batches:
        # Empty input — write an empty parquet so the output exists and is
        # idempotent on re-runs.
        empty = pa.Table.from_batches([], schema=schema)
        pq.write_table(empty, out_path.as_posix(), compression="snappy")
        return 0

    table_obj = pa.Table.from_batches(batches, schema=schema)
    pq.write_table(
        table_obj,
        out_path.as_posix(),
        compression="snappy",
        row_group_size=table_obj.num_rows,  # single row group
    )
    return total


def convert_to_parquet(force: bool = False) -> None:
    try:
        import pyarrow  # type: ignore  # noqa: F401
        import pyarrow.parquet  # type: ignore  # noqa: F401
    except ImportError:
        sys.exit(
            "pyarrow is required for the one-shot CSV->Parquet conversion. "
            "Install it into your dev venv: `pip install pyarrow`. "
            "(Used only by this fetch script — never at benchmark time.)"
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    pending = []
    for table in TABLES:
        out_dir = DATA_DIR / table
        out_path = out_dir / f"{table}.parquet"
        if out_path.exists() and not force:
            continue
        pending.append((table, out_dir, out_path))

    if not pending:
        print(
            f"[data] all 21 parquet files already present in {DATA_DIR} "
            f"(use --force-convert to re-emit)"
        )
        return

    print(f"[data] converting {len(pending)} CSV(s) -> Parquet (SNAPPY)")
    for table, out_dir, out_path in pending:
        csv_path = IMDB_CSV_DIR / f"{table}.csv"
        if not csv_path.exists():
            print(f"  ! missing source CSV: {csv_path}")
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        if out_path.exists():
            out_path.unlink()
        rows = _convert_one(csv_path, out_path, table)
        print(f"  {table:<18} {rows:>10,} rows -> {out_path.relative_to(ROOT)}")
    print(f"[data] done -> {DATA_DIR}")


# ---------------------------------------------------------------------------
# Query files
# ---------------------------------------------------------------------------

QUERY_RE = re.compile(r"^[0-9]+[a-z]\.sql$")


def fetch_queries() -> None:
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(
        p.name for p in QUERIES_DIR.glob("*.sql") if QUERY_RE.match(p.name)
    )
    if len(existing) >= 113:
        print(f"[queries] {len(existing)} query files already present")
        return

    import json

    print(f"[queries] listing repo contents from {QUERY_LIST_URL}")
    req = urllib.request.Request(
        QUERY_LIST_URL, headers={"User-Agent": "opteryx-job-bench"}
    )
    with urllib.request.urlopen(req) as resp:
        listing = json.loads(resp.read())
    names = sorted(
        entry["name"] for entry in listing if QUERY_RE.match(entry["name"])
    )
    if not names:
        sys.exit("Failed to list JOB query files from GitHub.")
    print(f"[queries] downloading {len(names)} query files")
    for name in names:
        out = QUERIES_DIR / name
        if out.exists():
            continue
        url = QUERY_RAW_BASE + name
        with urllib.request.urlopen(url) as r:
            out.write_bytes(r.read())
    print(f"[queries] done -> {QUERIES_DIR}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", action="store_true", help="fetch + convert IMDB only"
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
        fetch_imdb_tarball()
        extract_imdb_tarball()
        convert_to_parquet(force=args.force_convert)

    print("[ok] JOB fixtures ready. Run `make job` to execute the benchmark.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
