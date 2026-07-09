#!/usr/bin/env python3
"""
Synthetic generator for the "signals" (security scanner findings) dataset shape.

Matches the schema, null-rate profile, cardinality, and value-length
distributions observed in scratch/signals/*.parquet (a 763k-row sample of
real customer data), but every value is fabricated: toolsets, IPs, teams,
applications, hostnames, and vulnerability titles are all synthetic. Real,
public CVE identifiers are used where the source data embeds a CVE in the
finding title, since those are public vulnerability records, not customer
data.

Chunked/streamed generation so memory stays bounded regardless of total row
count -- this is designed to scale from 10M rows today up to 100M/1B later
by raising --rows (and optionally --max-rows-per-file / --row-group-size
for file layout).

Usage:
    python dev/generate_signals_data.py --rows 10_000_000 --out scratch/signals_synthetic
    python dev/generate_signals_data.py --rows 1_000_000_000 --out scratch/signals_synthetic \
        --max-rows-per-file 20_000_000 --row-group-size 250_000

This script uses pyarrow and numpy (dev/-only, not the engine).
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("finding_id", pa.int64()),
        ("identifier", pa.string()),
        ("finding_ref", pa.string()),
        ("toolset", pa.string()),
        ("priority", pa.string()),
        ("is_open", pa.bool_()),
        ("is_potential", pa.bool_()),
        ("fix_due_at", pa.timestamp("us")),
        ("first_seen_at", pa.timestamp("us")),
        ("updated_at", pa.timestamp("us")),
        ("status_at", pa.timestamp("us")),
        ("is_risk_accepted", pa.bool_()),
        ("is_tech_exception", pa.bool_()),
        ("is_kyndryl_excepted", pa.bool_()),
        ("ip", pa.string()),
        ("application", pa.list_(pa.string())),
        ("summary_text", pa.string()),
        ("location", pa.string()),
        ("mapped_report", pa.list_(pa.string())),
        ("finding_subcategory", pa.string()),
        ("type", pa.string()),
        ("host_support_group", pa.string()),
        ("environment", pa.string()),
        ("area", pa.list_(pa.string())),
        ("team", pa.list_(pa.string())),
        ("platform", pa.string()),
        ("resolved_at", pa.timestamp("us")),
    ]
)

# --- toolsets -----------------------------------------------------------
# name -> (identifier prefix, relative weight). Weight shape mirrors the
# single-dominant-scanner skew seen in the source sample.
TOOLSETS = {
    "Qualys": ("Q", 0.70),
    "Nessus": ("N", 0.15),
    "Rapid7": ("R", 0.10),
    "Wiz": ("W", 0.05),
}
TOOLSET_NAMES = list(TOOLSETS.keys())
TOOLSET_PREFIX = {k: v[0] for k, v in TOOLSETS.items()}
TOOLSET_WEIGHTS = np.array([v[1] for v in TOOLSETS.values()])
TOOLSET_WEIGHTS = TOOLSET_WEIGHTS / TOOLSET_WEIGHTS.sum()

PRIORITIES = np.array(["Medium", "High", "Low"])
PRIORITY_WEIGHTS = np.array([0.88, 0.06, 0.06])

TYPES = np.array(["Confirmed", "Potential", None], dtype=object)
TYPE_WEIGHTS = np.array([0.92, 0.005, 0.075])

FINDING_SUBCATEGORIES = np.array(["MFL", "SOX", None], dtype=object)
FINDING_SUBCATEGORY_WEIGHTS = np.array([0.068, 0.012, 0.92])

ENVIRONMENTS = np.array(
    [
        "Production",
        "Pre-Production",
        "Development",
        "Test",
        "Staging",
        "UAT",
        "Disaster Recovery",
        "Sandbox",
        "N/A (See Virtual Items)",
    ],
    dtype=object,
)
ENVIRONMENT_WEIGHTS = np.array([0.42, 0.10, 0.08, 0.07, 0.05, 0.05, 0.03, 0.02, 0.18])

# --- corporate teams / areas ---------------------------------------------
TEAMS = [
    "Sales",
    "Marketing",
    "Finance",
    "Human Resources",
    "Legal",
    "IT Operations",
    "IT Security",
    "Engineering",
    "Product Management",
    "Customer Support",
    "Procurement",
    "Facilities",
    "Compliance",
    "Risk Management",
    "Data & Analytics",
    "Infrastructure",
    "Network Operations",
    "DevOps",
    "Quality Assurance",
    "Corporate Communications",
    "Treasury",
    "Internal Audit",
    "Supply Chain",
    "Research & Development",
]
AREA_POOL = np.array(
    [t for t in TEAMS] + [f"{t} (Tech)" for t in TEAMS] + [f"{t} Platform" for t in TEAMS],
    dtype=object,
)
TEAM_POOL = np.array(TEAMS + [""], dtype=object)

# --- host support groups (fabricated dot-separated codes) ---------------
_HSG_SEG1 = [
    "CLIENT",
    "WINTEL",
    "NETWORK",
    "SECURITY",
    "DATABASE",
    "MIDDLEWARE",
    "CLOUD",
    "STORAGE",
    "UNIX",
    "APPLICATION",
    "VOICE",
    "MAINFRAME",
]
_HSG_SEG2 = [
    "THIRDLEVEL.SUPPORT",
    "SERVER-SUPPORT.FRONTOFFICE",
    "SERVER-SUPPORT.BACKOFFICE",
    "OPERATIONS.SUPPORT",
    "ENGINEERING.SUPPORT",
    "SECONDLEVEL.SUPPORT",
]
HOST_SUPPORT_GROUPS = np.array(
    [f"IT.{s1}.{s2}" for s1 in _HSG_SEG1 for s2 in _HSG_SEG2], dtype=object
)

# --- applications (fabricated) -------------------------------------------
_ADJ = [
    "Atlas", "Nimbus", "Falcon", "Orion", "Vertex", "Zenith", "Quantum", "Nova",
    "Titan", "Apex", "Pulse", "Horizon", "Summit", "Catalyst", "Beacon",
    "Vantage", "Meridian", "Cascade", "Ember", "Halcyon", "Lumen", "Onyx",
]
_NOUN = [
    "Ledger", "Gateway", "Portal", "Hub", "Engine", "Suite", "Platform",
    "Console", "Exchange", "Vault", "Bridge", "Dashboard", "Registry",
    "Pipeline", "Workbench", "Studio", "Connect", "Sync", "Core", "Flow",
]
_APP_CODES = ["ALPHA", "BRAVO", "GLBL", "EMEA", "APAC", "CORE", "LEGACY", "EU"]
_rng_vocab = np.random.default_rng(1)
APPLICATIONS = np.array(
    sorted(
        {
            f"{a} {n}" + (f" ({c})" if _rng_vocab.random() < 0.3 else "")
            for a in _ADJ
            for n in _NOUN
            for c in [_rng_vocab.choice(_APP_CODES)]
        }
    ),
    dtype=object,
)

MAPPED_REPORTS = np.array(
    sorted({f"{t} {n}" for t in TEAMS for n in ["Platform", "Services", "Program"]}),
    dtype=object,
)

# --- vulnerability catalog (identifier <-> summary_text) -----------------
_PRODUCTS = [
    "Google Chrome", "Mozilla Firefox", "Microsoft Edge", "Notepad++",
    "Apache Log4j", "OpenSSL", "Microsoft Office", "Oracle Java SE",
    "Adobe Reader", "7-Zip", "PuTTY", "VMware ESXi", "Cisco IOS",
    "OpenSSH", "nginx", "Apache HTTP Server", "Docker Engine",
    "Kubernetes", "PostgreSQL", "MySQL", "Redis", "MongoDB", "Jenkins",
    "GitLab", "Elasticsearch", "Microsoft Windows", "Microsoft SQL Server",
    "Apache Tomcat", "Node.js", "Python", "PHP", "WordPress", "Drupal",
    "F5 BIG-IP", "Fortinet FortiOS", "Palo Alto PAN-OS", "SolarWinds Orion",
    "Zoom", "Slack Desktop Client", "Citrix ADC", "IBM WebSphere",
]
_VULN_TEMPLATES = [
    "{p} Remote Code Execution Vulnerability",
    "{p} Multiple Vulnerabilities",
    "{p} Privilege Escalation Vulnerability",
    "{p} Denial of Service Vulnerability",
    "{p} Information Disclosure Vulnerability",
    "{p} Security Update",
    "{p} DLL Hijacking Vulnerability",
    "{p} Out-of-Bounds Write Vulnerability",
    "{p} Authentication Bypass Vulnerability",
    "{p} SSL/TLS Certificate Vulnerability",
    "{p} Outdated Version Detected",
    "{p} End of Life Detected",
    "{p} Cross-Site Scripting Vulnerability",
    "{p} Security Bypass Vulnerability",
]
# Real, public CVE identifiers -- these are publicly disclosed vulnerability
# records, not customer data, and are only used to make catalog entries that
# happen to reference a CVE look realistic.
_REAL_CVES = [
    "CVE-2021-44228", "CVE-2014-0160", "CVE-2017-5638", "CVE-2019-0708",
    "CVE-2020-1472", "CVE-2021-34527", "CVE-2022-30190", "CVE-2023-23397",
    "CVE-2016-2107", "CVE-2022-32168", "CVE-2013-1493", "CVE-2021-45046",
    "CVE-2021-21972", "CVE-2020-0601", "CVE-2018-7600", "CVE-2019-11510",
    "CVE-2021-26855", "CVE-2021-27065", "CVE-2020-14882", "CVE-2022-22965",
    "CVE-2021-3156", "CVE-2019-19781", "CVE-2020-5902", "CVE-2018-13379",
    "CVE-2021-22986", "CVE-2022-1388", "CVE-2023-34362", "CVE-2023-4966",
    "CVE-2024-3400", "CVE-2021-40444", "CVE-2017-0144", "CVE-2018-8174",
    "CVE-2020-0796", "CVE-2019-1181", "CVE-2021-1675", "CVE-2022-26925",
    "CVE-2023-21716", "CVE-2020-1350", "CVE-2016-0800", "CVE-2015-1637",
]


def _build_catalog(rng: np.random.Generator, size: int) -> tuple[np.ndarray, np.ndarray]:
    """Build `size` (identifier-body, summary_text) catalog entries shared
    across all toolsets; identifier prefix is applied per-row from the
    row's sampled toolset."""
    numbers = rng.integers(10_000, 999_999, size=size)
    identifiers = numbers.astype(str)
    titles = []
    cve_rate = 0.05  # catalog-level rate tuned so realized row-level CVE
    # mention rate lands close to the ~1.7% observed in the source sample
    for _ in range(size):
        product = rng.choice(_PRODUCTS)
        template = rng.choice(_VULN_TEMPLATES)
        title = template.format(p=product)
        if rng.random() < cve_rate:
            cve = rng.choice(_REAL_CVES)
            title = f"{title} ({cve})"
        titles.append(title[:255])
    return identifiers, np.array(titles, dtype=object)


def _zipf_weights(rng: np.random.Generator, size: int, a: float = 1.3) -> np.ndarray:
    ranks = np.arange(1, size + 1)
    w = 1.0 / np.power(ranks, a)
    rng.shuffle(w)
    return w / w.sum()


def _list_lengths(rng: np.random.Generator, n: int, min_len: int, lam: float) -> np.ndarray:
    lengths = rng.poisson(lam=lam, size=n) + min_len
    tail = rng.random(n) < 0.0005
    lengths[tail] = rng.integers(10, 100, size=tail.sum())
    return lengths


def _sample_list_column(rng: np.random.Generator, pool: np.ndarray, lengths: np.ndarray) -> pa.Array:
    flat_idx = rng.integers(0, len(pool), size=int(lengths.sum()))
    flat_values = pool[flat_idx]
    offsets = np.zeros(len(lengths) + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    values_array = pa.array(flat_values, type=pa.string())
    return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), values_array)


def _random_timestamps(rng: np.random.Generator, n: int, start: datetime, end: datetime) -> np.ndarray:
    start_us = np.int64(start.timestamp() * 1_000_000)
    end_us = np.int64(end.timestamp() * 1_000_000)
    return rng.integers(start_us, end_us, size=n, dtype=np.int64)


def _apply_nulls(rng: np.random.Generator, arr: np.ndarray, null_rate: float) -> np.ndarray:
    if null_rate <= 0:
        return arr
    mask = rng.random(len(arr)) < null_rate
    out = arr.astype(object)
    out[mask] = None
    return out


def generate_chunk(rng: np.random.Generator, n: int, row_offset: int, catalog: tuple[np.ndarray, np.ndarray], now: datetime) -> pa.RecordBatch:
    cat_ids, cat_titles = catalog
    catalog_size = len(cat_ids)
    catalog_weights = _zipf_weights(rng, catalog_size)

    toolset_idx = rng.choice(len(TOOLSET_NAMES), size=n, p=TOOLSET_WEIGHTS)
    toolsets = np.array(TOOLSET_NAMES, dtype=object)[toolset_idx]
    prefixes = np.array([TOOLSET_PREFIX[t] for t in TOOLSET_NAMES], dtype=object)[toolset_idx]

    cat_idx = rng.choice(catalog_size, size=n, p=catalog_weights)
    identifiers = np.char.add(np.char.add(prefixes.astype(str), "-"), cat_ids[cat_idx])
    summary_text = cat_titles[cat_idx]

    finding_id = 200_000_000 + row_offset + np.arange(n, dtype=np.int64)
    finding_ref = (900_000_000 + row_offset + np.arange(n, dtype=np.int64)).astype(str)

    priority = rng.choice(PRIORITIES, size=n, p=PRIORITY_WEIGHTS)

    is_open = rng.random(n) < 0.37
    is_potential = rng.random(n) < 0.0022
    is_risk_accepted = rng.random(n) < 0.0176
    is_tech_exception = rng.random(n) < 0.0068
    is_kyndryl_excepted = _apply_nulls(rng, rng.random(n) < 0.34, 0.4007)

    fix_due_at = _random_timestamps(rng, n, datetime(2020, 1, 1), now).astype(object)
    fix_due_at = _apply_nulls(rng, fix_due_at, 0.0632)
    first_seen_at = _random_timestamps(rng, n, datetime(2017, 11, 3), now)
    updated_at = _random_timestamps(rng, n, now - timedelta(days=180), now)
    status_at = _random_timestamps(rng, n, now - timedelta(days=180), now)

    # 10.0.0.0/8, drawn from a bounded pool so hosts repeat like real scan data
    pool_size = max(1, n // 18)
    ip_pool_b = rng.integers(0, 256, size=pool_size)
    ip_pool_c = rng.integers(0, 256, size=pool_size)
    ip_pool_d = rng.integers(1, 255, size=pool_size)
    ip_pool = np.array([f"10.{b}.{c}.{d}" for b, c, d in zip(ip_pool_b, ip_pool_c, ip_pool_d)], dtype=object)
    ip = ip_pool[rng.integers(0, pool_size, size=n)]
    ip = _apply_nulls(rng, ip, 0.0651)

    type_ = rng.choice(TYPES, size=n, p=TYPE_WEIGHTS)
    finding_subcategory = rng.choice(FINDING_SUBCATEGORIES, size=n, p=FINDING_SUBCATEGORY_WEIGHTS)
    host_support_group = _apply_nulls(rng, rng.choice(HOST_SUPPORT_GROUPS, size=n), 0.0195)
    environment = _apply_nulls(rng, rng.choice(ENVIRONMENTS, size=n, p=ENVIRONMENT_WEIGHTS), 0.0245)

    # fabricated hostnames: 3-4 lowercase letters + 4-6 alnum
    letters = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    alnum = np.array(list("abcdefghijklmnopqrstuvwxyz0123456789"))
    loc_letters = rng.choice(letters, size=(n, 4))
    loc_tail = rng.choice(alnum, size=(n, 6))
    location = np.array(
        ["".join(row_l) + "".join(row_t) for row_l, row_t in zip(loc_letters, loc_tail)], dtype=object
    )

    app_lengths = _list_lengths(rng, n, min_len=0, lam=1.8)
    application = _sample_list_column(rng, APPLICATIONS, app_lengths)
    mr_lengths = _list_lengths(rng, n, min_len=1, lam=1.2)
    mapped_report = _sample_list_column(rng, MAPPED_REPORTS, mr_lengths)

    area_lengths = _list_lengths(rng, n, min_len=0, lam=1.5)
    area = _sample_list_column(rng, AREA_POOL, area_lengths)
    team_lengths = _list_lengths(rng, n, min_len=0, lam=1.2)
    team = _sample_list_column(rng, TEAM_POOL, team_lengths)
    area_null_mask = rng.random(n) < 0.1939
    team_null_mask = area_null_mask  # area/team are null together in the source data

    def _null_out(list_array: pa.Array, mask: np.ndarray) -> pa.Array:
        idx = np.arange(len(mask))
        keep = pa.array(~mask)
        return pc.if_else(keep, list_array, pa.nulls(len(mask), type=list_array.type))

    area = _null_out(area, area_null_mask)
    team = _null_out(team, team_null_mask)

    columns = {
        "finding_id": pa.array(finding_id, type=pa.int64()),
        "identifier": pa.array(identifiers, type=pa.string()),
        "finding_ref": pa.array(finding_ref, type=pa.string()),
        "toolset": pa.array(toolsets, type=pa.string()),
        "priority": pa.array(priority, type=pa.string()),
        "is_open": pa.array(is_open, type=pa.bool_()),
        "is_potential": pa.array(is_potential, type=pa.bool_()),
        "fix_due_at": pa.array(fix_due_at, type=pa.timestamp("us")),
        "first_seen_at": pa.array(first_seen_at, type=pa.timestamp("us")),
        "updated_at": pa.array(updated_at, type=pa.timestamp("us")),
        "status_at": pa.array(status_at, type=pa.timestamp("us")),
        "is_risk_accepted": pa.array(is_risk_accepted, type=pa.bool_()),
        "is_tech_exception": pa.array(is_tech_exception, type=pa.bool_()),
        "is_kyndryl_excepted": pa.array(is_kyndryl_excepted, type=pa.bool_()),
        "ip": pa.array(ip, type=pa.string()),
        "application": application,
        "summary_text": pa.array(summary_text, type=pa.string()),
        "location": pa.array(location, type=pa.string()),
        "mapped_report": mapped_report,
        "finding_subcategory": pa.array(finding_subcategory, type=pa.string()),
        "type": pa.array(type_, type=pa.string()),
        "host_support_group": pa.array(host_support_group, type=pa.string()),
        "environment": pa.array(environment, type=pa.string()),
        "area": area,
        "team": team,
        "platform": pa.nulls(n, type=pa.string()),
        "resolved_at": pa.nulls(n, type=pa.timestamp("us")),
    }
    return pa.RecordBatch.from_arrays([columns[name] for name in SCHEMA.names], schema=SCHEMA)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic 'signals' scanner-findings Parquet data")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Total rows to generate (default 10,000,000)")
    parser.add_argument("--out", type=str, default="scratch/signals_synthetic", help="Output directory")
    parser.add_argument("--prefix", type=str, default="signals", help="Output filename prefix")
    parser.add_argument("--chunk-size", type=int, default=500_000, help="Rows generated per in-memory chunk")
    parser.add_argument("--row-group-size", type=int, default=250_000, help="Parquet row group size")
    parser.add_argument("--max-rows-per-file", type=int, default=2_000_000, help="Rows per output file")
    parser.add_argument("--catalog-size", type=int, default=6000, help="Distinct vulnerability catalog entries")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    catalog = _build_catalog(rng, args.catalog_size)
    now = datetime(2026, 7, 10)

    total_rows = args.rows
    rows_written = 0
    file_idx = 0
    writer = None
    rows_in_current_file = 0

    while rows_written < total_rows:
        chunk_n = min(args.chunk_size, total_rows - rows_written)

        if writer is None:
            fname = outdir / f"{args.prefix}-{file_idx:04d}.parquet"
            writer = pq.ParquetWriter(fname, SCHEMA)
            rows_in_current_file = 0
            print(f"writing {fname}")

        batch = generate_chunk(rng, chunk_n, rows_written, catalog, now)
        table = pa.Table.from_batches([batch])
        writer.write_table(table, row_group_size=args.row_group_size)

        rows_written += chunk_n
        rows_in_current_file += chunk_n

        if rows_in_current_file >= args.max_rows_per_file or rows_written >= total_rows:
            writer.close()
            writer = None
            file_idx += 1

        print(f"  {rows_written:,}/{total_rows:,} rows")

    print("done")


if __name__ == "__main__":
    main()
