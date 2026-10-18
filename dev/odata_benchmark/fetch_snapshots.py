#!/usr/bin/env python3
"""
Pulls local parquet snapshots of the public OData tables that the odata.opteryx.app
query-log analysis (see docs/) is benchmarked against. Not packaged, not imported by
production code - a one-shot data puller for tests/performance/odata_dashboard/.

Usage:
    python3 dev/odata_benchmark/fetch_snapshots.py exploit_db vulnerabilities_per_week
    python3 dev/odata_benchmark/fetch_snapshots.py --all

Tables are named explicitly (or --all): a run rewrites every table it is given, and the
sources are live, so re-pulling a table that is fine only changes it.

Integrity: no table is paged with $skip - the server's unordered $skip paging was observed
returning blocks of rows from elsewhere in the table, which duplicated some rows and dropped
others with no error. Tables with a unique key are keyset-paged: paging ends only on an empty
page, key order must be strictly increasing. exploit_db's key is not unique upstream, so it is
fetched in fixed key-range windows (duplicates kept as served), and a window that fills a
whole page raises as possibly truncated. Any violation raises before anything is written.
vulnerabilities_per_week has no key; it is a projection of nvd_vulnerabilities
(week = DATE_TRUNC('week', published_at)) and is derived locally from that snapshot, so
nvd_vulnerabilities must be pulled (or already present) first.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

sys.path.insert(1, os.path.join(sys.path[0], "..", ".."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import read_parquet, write_parquet

BASE_URL = "https://odata.opteryx.app/api/v4"
OUT_DIR = Path(__file__).resolve().parents[2] / "testdata" / "public"
PAGE_SIZE = 20_000  # server 500s above ~30-40k rows/page for wide tables

# EDM -> Draken type map
EDM_TYPES = {
    "Edm.Int64": DrakenType.INT64,
    "Edm.String": DrakenType.VARCHAR,
    "Edm.Double": DrakenType.FLOAT64,
    "Edm.Boolean": DrakenType.BOOL,
    "Edm.DateTimeOffset": DrakenType.TIMESTAMP64,
}

TABLES = {
    "gdelt_events": {
        "path": "public/geopolitics/gdelt_events",
        "limit": 1_000_000,
        # $skip + this many projected columns 500/503s server-side on this
        # 20M-row table (reproduced independently of page size); keyset
        # pagination on the PK avoids OFFSET entirely.
        "keyset_column": "global_event_id",
        "schema": {
            "global_event_id": "Edm.Int64",
            "event_date": "Edm.DateTimeOffset",
            "actor1_code": "Edm.String",
            "actor1_name": "Edm.String",
            "actor1_country_code": "Edm.String",
            "actor2_code": "Edm.String",
            "actor2_name": "Edm.String",
            "actor2_country_code": "Edm.String",
            "is_root_event": "Edm.Boolean",
            "event_code": "Edm.String",
            "event_base_code": "Edm.String",
            "event_root_code": "Edm.String",
            "quad_class": "Edm.Int64",
            "goldstein_scale": "Edm.Double",
            "num_mentions": "Edm.Int64",
            "num_sources": "Edm.Int64",
            "num_articles": "Edm.Int64",
            "avg_tone": "Edm.Double",
            "action_geo_type": "Edm.Int64",
            "action_geo_full_name": "Edm.String",
            "action_geo_country_code": "Edm.String",
            "action_geo_lat": "Edm.Double",
            "action_geo_long": "Edm.Double",
            "date_added": "Edm.DateTimeOffset",
            "source_url": "Edm.String",
        },
    },
    "nvd_vulnerabilities": {
        "path": "public/security/nvd_vulnerabilities",
        "limit": None,
        "keyset_column": "cve_id",
        "schema": {
            "cve_id": "Edm.String",
            "published_at": "Edm.DateTimeOffset",
            "severity": "Edm.String",
            "cvss_score": "Edm.Double",
            "cvss_vector": "Edm.String",
            "vendor": "Edm.String",
            "product": "Edm.String",
            "description": "Edm.String",
            # cwes / references are Collection(Edm.String); not needed for the
            # benchmark query shapes observed in the log, dropped to keep the
            # snapshot lean (the app never selects them in the logged traffic).
        },
    },
    "exploited_vulnerabilities": {
        "path": "public/security/exploited_vulnerabilities",
        "limit": None,
        "keyset_column": "cve_id",
        "schema": {
            "cve_id": "Edm.String",
            "published_at": "Edm.DateTimeOffset",
            "cvss_score": "Edm.Double",
            "cvss_vector": "Edm.String",
            "vendor": "Edm.String",
        },
    },
    "vulnerabilities_per_week": {
        # No key to page on, so not fetched: derived from the local
        # nvd_vulnerabilities snapshot (see derive_vulnerabilities_per_week).
        "derived_from": "nvd_vulnerabilities",
        "schema": {
            "published_at": "Edm.DateTimeOffset",
            "cvss_score": "Edm.Double",
            "vendor": "Edm.String",
            "week": "Edm.DateTimeOffset",
        },
    },
    "exploit_db": {
        "path": "public/security/exploit_db",
        "limit": None,
        # exploit_id is NOT unique upstream (the server serves identical rows for
        # some ids), so keyset paging cannot be used: a repeated row straddling a
        # page boundary would be skipped. Fixed key-range windows keep the rows
        # exactly as served.
        "key_window": {"column": "exploit_id", "width": 2_000},
        "schema": {
            "exploit_id": "Edm.Int64",
            "file_path": "Edm.String",
            "description": "Edm.String",
            "date_published": "Edm.DateTimeOffset",
            "author": "Edm.String",
            "exploit_type": "Edm.String",
            "platform": "Edm.String",
            "port": "Edm.Int64",
            "date_added": "Edm.DateTimeOffset",
            "date_updated": "Edm.DateTimeOffset",
            "verified": "Edm.Boolean",
            "cve_ids": "Edm.String",
            "tags": "Edm.String",
            "source_url": "Edm.String",
        },
    },
}


def fetch_page(path, select_cols, top, odata_filter=None, orderby=None):
    select = ",".join(select_cols)
    url = f"{BASE_URL}/{path}?$select={select}&$top={top}"
    if odata_filter is not None:
        url += f"&$filter={quote(odata_filter)}"
    if orderby is not None:
        url += f"&$orderby={quote(orderby)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def odata_literal(value, edm_type):
    if edm_type == "Edm.Int64":
        return str(value)
    if edm_type == "Edm.String":
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"keyset column type {edm_type} is not supported")


def check_page_keys(name, keyset_column, page, last_val):
    """Raise unless the page's keys are non-null and strictly increasing past last_val."""
    prev = last_val
    for row in page:
        key = row[keyset_column]
        if key is None:
            raise ValueError(f"[{name}] null {keyset_column} in a keyset page")
        if prev is not None and not key > prev:
            raise ValueError(
                f"[{name}] {keyset_column} not strictly increasing: {key!r} after {prev!r}"
            )
        prev = key


def fetch_table_by_keyset(name, path, cols, limit, keyset_column, key_type):
    rows = []
    last_val = None
    page_size = PAGE_SIZE
    while limit is None or len(rows) < limit:
        want = page_size if limit is None else min(page_size, limit - len(rows))
        odata_filter = (
            None if last_val is None
            else f"{keyset_column} gt {odata_literal(last_val, key_type)}"
        )
        try:
            body = fetch_page(
                path, cols, want,
                odata_filter=odata_filter,
                orderby=f"{keyset_column} asc",
            )
        except Exception as exc:
            if page_size > 2_000:
                page_size //= 2
                print(f"  [{name}] page failed ({exc}); backing off to {page_size}/page")
                continue
            raise
        page = body.get("value", [])
        # Paging ends only on an empty page: a short page is not proof of the end,
        # since a server that silently caps $top would truncate the table.
        if not page:
            break
        check_page_keys(name, keyset_column, page, last_val)
        rows.extend(page)
        last_val = page[-1][keyset_column]
        print(f"  [{name}] {len(rows)} rows fetched ({keyset_column} > {last_val})")
    return rows


def fetch_table_by_key_windows(name, path, cols, column, width, key_type):
    """Fetch a table whose int key is not unique: one request per fixed key range
    (lo, lo+width], keys ascending, duplicates kept as served. A window that fills
    a whole page may have been truncated, so it raises instead of continuing."""
    if key_type != "Edm.Int64":
        raise ValueError(f"[{name}] key windows need an Edm.Int64 key, not {key_type}")
    first = fetch_page(path, [column], 1, orderby=f"{column} asc")["value"]
    last = fetch_page(path, [column], 1, orderby=f"{column} desc")["value"]
    if not first or not last:
        raise ValueError(f"[{name}] table is empty")
    lo = first[0][column] - 1
    top_key = last[0][column]
    rows = []
    while lo < top_key:
        hi = lo + width
        page = fetch_page(
            path, cols, PAGE_SIZE,
            odata_filter=f"{column} gt {lo} and {column} le {hi}",
            orderby=f"{column} asc",
        )["value"]
        if len(page) >= PAGE_SIZE:
            raise ValueError(
                f"[{name}] {column} window ({lo}, {hi}] returned {len(page)} rows, a full "
                f"page; it may be truncated - reduce the window width"
            )
        prev = lo
        for row in page:
            key = row[column]
            if key is None or not (lo < key <= hi) or key < prev:
                raise ValueError(
                    f"[{name}] {column} {key!r} out of order or outside window ({lo}, {hi}]"
                )
            prev = key
        rows.extend(page)
        print(f"  [{name}] {len(rows)} rows fetched ({column} <= {hi})")
        lo = hi
    return rows


def fetch_table(name, cfg):
    path = cfg["path"]
    cols = list(cfg["schema"].keys())
    limit = cfg["limit"]

    if "key_window" in cfg:
        window = cfg["key_window"]
        column = window["column"]
        rows = fetch_table_by_key_windows(
            name, path, cols, column, window["width"], cfg["schema"][column]
        )
    else:
        keyset_column = cfg["keyset_column"]
        key_type = cfg["schema"][keyset_column]
        rows = fetch_table_by_keyset(name, path, cols, limit, keyset_column, key_type)

    if limit is not None:
        rows = rows[:limit]
    return rows


def columns_to_morsel(columns, schema):
    names = []
    vectors = []
    for col, edm_type in schema.items():
        vectors.append(vector_from_sequence(columns[col], EDM_TYPES[edm_type]))
        names.append(col.encode())
    return Morsel.from_vectors(names, vectors)


def rows_to_morsel(rows, schema):
    columns = {}
    for col, edm_type in schema.items():
        values = [r.get(col) for r in rows]
        if edm_type == "Edm.DateTimeOffset":
            values = [datetime.fromisoformat(v) if v is not None else None for v in values]
        columns[col] = values
    return columns_to_morsel(columns, schema)


def derive_vulnerabilities_per_week(cfg):
    """Project the local nvd_vulnerabilities snapshot to (published_at, cvss_score,
    vendor, week), week being the UTC Monday that starts published_at's week."""
    source_name = cfg["derived_from"]
    source = OUT_DIR / source_name / f"{source_name}.parquet"
    if not source.exists():
        raise FileNotFoundError(f"{source} must exist first: pull {source_name}")
    columns = {"published_at": [], "cvss_score": [], "vendor": []}
    with read_parquet(str(source), columns=list(columns)) as reader:
        for morsel in reader:
            for col, values in columns.items():
                values.extend(morsel.column(col).to_pylist())
    columns["week"] = [
        None if v is None
        else (v - timedelta(days=v.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        for v in columns["published_at"]
    ]
    return columns_to_morsel(columns, cfg["schema"]), len(columns["published_at"])


def main():
    parser = argparse.ArgumentParser(description="Pull/derive local snapshots of the public OData tables.")
    parser.add_argument("tables", nargs="*", help=f"tables to write: {', '.join(TABLES)}")
    parser.add_argument("--all", action="store_true", help="write every table")
    args = parser.parse_args()
    if args.all == bool(args.tables):
        parser.error("name the tables to write, or pass --all (not both)")
    unknown = [t for t in args.tables if t not in TABLES]
    if unknown:
        parser.error(f"unknown table(s): {', '.join(unknown)}")
    wanted = list(TABLES) if args.all else args.tables

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, cfg in TABLES.items():
        if name not in wanted:
            continue
        # Each dataset is its own directory (opteryx's FileSystemConnector lists
        # a directory of part files; a bare "<name>.parquet" file is not
        # discoverable as "public.<name>").
        table_dir = OUT_DIR / name
        table_dir.mkdir(parents=True, exist_ok=True)
        out_path = table_dir / f"{name}.parquet"
        print(f"== {name} -> {out_path} ==")
        start = time.time()
        if "derived_from" in cfg:
            morsel, row_count = derive_vulnerabilities_per_week(cfg)
        else:
            rows = fetch_table(name, cfg)
            morsel = rows_to_morsel(rows, cfg["schema"])
            row_count = len(rows)
        out_path.write_bytes(write_parquet(morsel))
        print(f"  wrote {row_count} rows, {out_path.stat().st_size / 1e6:.1f} MB "
              f"in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
