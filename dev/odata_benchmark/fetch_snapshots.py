#!/usr/bin/env python3
"""
Pulls local parquet snapshots of the public OData tables that the odata.opteryx.app
query-log analysis (see docs/) is benchmarked against. Not packaged, not imported by
production code - a one-shot data puller for tests/performance/odata_dashboard/.

Usage:
    python3 dev/odata_benchmark/fetch_snapshots.py
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

sys.path.insert(1, os.path.join(sys.path[0], "..", ".."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

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
        "schema": {
            "cve_id": "Edm.String",
            "published_at": "Edm.DateTimeOffset",
            "cvss_score": "Edm.Double",
            "cvss_vector": "Edm.String",
            "vendor": "Edm.String",
        },
    },
    "vulnerabilities_per_week": {
        "path": "public/security/vulnerabilities_per_week",
        "limit": None,
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


def fetch_page(path, select_cols, top, skip=None, odata_filter=None, orderby=None):
    select = ",".join(select_cols)
    url = f"{BASE_URL}/{path}?$select={select}&$top={top}"
    if skip is not None:
        url += f"&$skip={skip}"
    if odata_filter is not None:
        url += f"&$filter={quote(odata_filter)}"
    if orderby is not None:
        url += f"&$orderby={quote(orderby)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def fetch_table_by_skip(name, path, cols, limit):
    rows = []
    skip = 0
    page_size = PAGE_SIZE
    while limit is None or len(rows) < limit:
        want = page_size if limit is None else min(page_size, limit - len(rows))
        try:
            body = fetch_page(path, cols, want, skip=skip)
        except Exception as exc:
            if page_size > 2_000:
                page_size //= 2
                print(f"  [{name}] page failed ({exc}); backing off to {page_size}/page")
                continue
            raise
        page = body.get("value", [])
        rows.extend(page)
        skip += len(page)
        print(f"  [{name}] {len(rows)} rows fetched (skip={skip})")
        if len(page) < want:
            break  # server returned fewer than asked -> exhausted
    return rows


def fetch_table_by_keyset(name, path, cols, limit, keyset_column):
    rows = []
    last_val = 0
    page_size = PAGE_SIZE
    while limit is None or len(rows) < limit:
        want = page_size if limit is None else min(page_size, limit - len(rows))
        try:
            body = fetch_page(
                path, cols, want,
                odata_filter=f"{keyset_column} gt {last_val}",
                orderby=f"{keyset_column} asc",
            )
        except Exception as exc:
            if page_size > 2_000:
                page_size //= 2
                print(f"  [{name}] page failed ({exc}); backing off to {page_size}/page")
                continue
            raise
        page = body.get("value", [])
        if not page:
            break
        rows.extend(page)
        last_val = page[-1][keyset_column]
        print(f"  [{name}] {len(rows)} rows fetched ({keyset_column} > {last_val})")
        if len(page) < want:
            break  # server returned fewer than asked -> exhausted
    return rows


def fetch_table(name, cfg):
    path = cfg["path"]
    cols = list(cfg["schema"].keys())
    limit = cfg["limit"]
    keyset_column = cfg.get("keyset_column")

    if keyset_column:
        rows = fetch_table_by_keyset(name, path, cols, limit, keyset_column)
    else:
        rows = fetch_table_by_skip(name, path, cols, limit)

    if limit is not None:
        rows = rows[:limit]
    return rows


def rows_to_morsel(rows, schema):
    names = []
    vectors = []
    for col, edm_type in schema.items():
        values = [r.get(col) for r in rows]
        if edm_type == "Edm.DateTimeOffset":
            values = [datetime.fromisoformat(v) if v is not None else None for v in values]
        vectors.append(vector_from_sequence(values, EDM_TYPES[edm_type]))
        names.append(col.encode())
    return Morsel.from_vectors(names, vectors)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, cfg in TABLES.items():
        # Each dataset is its own directory (opteryx's FileSystemConnector lists
        # a directory of part files; a bare "<name>.parquet" file is not
        # discoverable as "public.<name>").
        table_dir = OUT_DIR / name
        table_dir.mkdir(parents=True, exist_ok=True)
        out_path = table_dir / f"{name}.parquet"
        print(f"== {name} -> {out_path} ==")
        start = time.time()
        rows = fetch_table(name, cfg)
        morsel = rows_to_morsel(rows, cfg["schema"])
        out_path.write_bytes(write_parquet(morsel))
        print(f"  wrote {len(rows)} rows, {out_path.stat().st_size / 1e6:.1f} MB "
              f"in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
