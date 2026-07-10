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

Written with rugo's native Parquet writer (draken Vectors -> Morsel ->
rugo.parquet.write_parquet), not PyArrow, so file layout (row groups, page
splitting, dictionary/bloom encoding) matches what the rest of the engine
actually produces. write_parquet serializes one Morsel to one complete file
in a single call (no incremental row-group append), so generation is
per-file: each file's rows are built in memory, then written once. Per-file
row count is bounded by --max-rows-per-file to keep memory bounded
regardless of total row count -- this is designed to scale from 10M rows
today up to 100M/1B later by raising --rows (and --max-rows-per-file /
--row-group-size to tune file layout at that scale).

Usage:
    python dev/generate_signals_data.py --rows 10_000_000 --out scratch/signals_synthetic
    python dev/generate_signals_data.py --rows 1_000_000_000 --out scratch/signals_synthetic \
        --max-rows-per-file 2_000_000 --row-group-size 250_000

This script uses numpy (dev/-only, not the engine) plus draken/rugo (the
real writer path -- no PyArrow).
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import numpy as np
from draken.draken_native import DrakenType
from draken.draken_native import vector_array_from_sequence
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.parquet import write_parquet

COLUMN_NAMES = [
    "finding_id",
    "identifier",
    "finding_ref",
    "toolset",
    "priority",
    "is_open",
    "is_potential",
    "fix_due_at",
    "first_seen_at",
    "updated_at",
    "status_at",
    "is_risk_accepted",
    "is_tech_exception",
    "ip",
    "application",
    "summary_text",
    "location",
    "mapped_report",
    "finding_subcategory",
    "type",
    "host_support_group",
    "environment",
    "area",
    "team",
    "platform",
    "resolved_at",
]

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


def _sample_lists(rng: np.random.Generator, pool: np.ndarray, lengths: np.ndarray) -> list:
    """Build a Python list[list[str]] by sampling `pool` according to
    per-row `lengths` (0 -> empty list, never None)."""
    flat_idx = rng.integers(0, len(pool), size=int(lengths.sum()))
    flat_values = pool[flat_idx]
    offsets = np.zeros(len(lengths) + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    return [flat_values[offsets[i]:offsets[i + 1]].tolist() for i in range(len(lengths))]


def _random_timestamps_us(rng: np.random.Generator, n: int, start: datetime, end: datetime) -> np.ndarray:
    start_us = np.int64(start.timestamp() * 1_000_000)
    end_us = np.int64(end.timestamp() * 1_000_000)
    return rng.integers(start_us, end_us, size=n, dtype=np.int64)


def _timestamps_with_nulls(rng: np.random.Generator, micros: np.ndarray, null_rate: float) -> list:
    """Vectorized int64-micros -> datetime conversion (numpy datetime64 cast,
    not a per-row Python loop), with `null_rate` fraction set to None."""
    dt_obj = micros.astype("datetime64[us]").astype(object)
    if null_rate > 0:
        mask = rng.random(len(micros)) < null_rate
        dt_obj[mask] = None
    return dt_obj.tolist()


def _apply_nulls(rng: np.random.Generator, arr: np.ndarray, null_rate: float) -> np.ndarray:
    if null_rate <= 0:
        return arr
    mask = rng.random(len(arr)) < null_rate
    out = arr.astype(object)
    out[mask] = None
    return out


def generate_morsel(rng: np.random.Generator, n: int, row_offset: int, catalog: tuple[np.ndarray, np.ndarray], now: datetime) -> Morsel:
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

    fix_due_at = _timestamps_with_nulls(rng, _random_timestamps_us(rng, n, datetime(2020, 1, 1), now), 0.0632)
    first_seen_at = _timestamps_with_nulls(rng, _random_timestamps_us(rng, n, datetime(2017, 11, 3), now), 0.0)
    updated_at = _timestamps_with_nulls(rng, _random_timestamps_us(rng, n, now - timedelta(days=180), now), 0.0)
    status_at = _timestamps_with_nulls(rng, _random_timestamps_us(rng, n, now - timedelta(days=180), now), 0.0)

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

    # fabricated hostnames: 4 lowercase letters + 6 alnum
    letters = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    alnum = np.array(list("abcdefghijklmnopqrstuvwxyz0123456789"))
    loc_letters = rng.choice(letters, size=(n, 4))
    loc_tail = rng.choice(alnum, size=(n, 6))
    location = np.array(
        ["".join(row_l) + "".join(row_t) for row_l, row_t in zip(loc_letters, loc_tail)], dtype=object
    )

    app_lengths = _list_lengths(rng, n, min_len=0, lam=1.8)
    application = _sample_lists(rng, APPLICATIONS, app_lengths)
    mr_lengths = _list_lengths(rng, n, min_len=1, lam=1.2)
    mapped_report = _sample_lists(rng, MAPPED_REPORTS, mr_lengths)

    area_lengths = _list_lengths(rng, n, min_len=0, lam=1.5)
    area = _sample_lists(rng, AREA_POOL, area_lengths)
    team_lengths = _list_lengths(rng, n, min_len=0, lam=1.2)
    team = _sample_lists(rng, TEAM_POOL, team_lengths)
    area_null_mask = rng.random(n) < 0.1939
    # area/team are null together in the source data
    for i in np.flatnonzero(area_null_mask):
        area[i] = None
        team[i] = None

    vectors = {
        "finding_id": vector_from_sequence(finding_id.tolist(), DrakenType.INT64),
        "identifier": vector_from_sequence(identifiers.tolist(), DrakenType.VARCHAR),
        "finding_ref": vector_from_sequence(finding_ref.tolist(), DrakenType.VARCHAR),
        "toolset": vector_from_sequence(toolsets.tolist(), DrakenType.VARCHAR),
        "priority": vector_from_sequence(priority.tolist(), DrakenType.VARCHAR),
        "is_open": vector_from_sequence(is_open.tolist(), DrakenType.BOOL),
        "is_potential": vector_from_sequence(is_potential.tolist(), DrakenType.BOOL),
        "fix_due_at": vector_from_sequence(fix_due_at, DrakenType.TIMESTAMP64),
        "first_seen_at": vector_from_sequence(first_seen_at, DrakenType.TIMESTAMP64),
        "updated_at": vector_from_sequence(updated_at, DrakenType.TIMESTAMP64),
        "status_at": vector_from_sequence(status_at, DrakenType.TIMESTAMP64),
        "is_risk_accepted": vector_from_sequence(is_risk_accepted.tolist(), DrakenType.BOOL),
        "is_tech_exception": vector_from_sequence(is_tech_exception.tolist(), DrakenType.BOOL),
        "ip": vector_from_sequence(ip.tolist(), DrakenType.VARCHAR),
        "application": Vector(vector_array_from_sequence(
            application, element_type=DrakenType.VARCHAR.value, nesting_depth=1)),
        "summary_text": vector_from_sequence(summary_text.tolist(), DrakenType.VARCHAR),
        "location": vector_from_sequence(location.tolist(), DrakenType.VARCHAR),
        "mapped_report": Vector(vector_array_from_sequence(
            mapped_report, element_type=DrakenType.VARCHAR.value, nesting_depth=1)),
        "finding_subcategory": vector_from_sequence(finding_subcategory.tolist(), DrakenType.VARCHAR),
        "type": vector_from_sequence(type_.tolist(), DrakenType.VARCHAR),
        "host_support_group": vector_from_sequence(host_support_group.tolist(), DrakenType.VARCHAR),
        "environment": vector_from_sequence(environment.tolist(), DrakenType.VARCHAR),
        "area": Vector(vector_array_from_sequence(
            area, element_type=DrakenType.VARCHAR.value, nesting_depth=1)),
        "team": Vector(vector_array_from_sequence(
            team, element_type=DrakenType.VARCHAR.value, nesting_depth=1)),
        "platform": vector_from_sequence([None] * n, DrakenType.VARCHAR),
        "resolved_at": vector_from_sequence([None] * n, DrakenType.TIMESTAMP64),
    }
    return Morsel.from_vectors(
        [name.encode() for name in COLUMN_NAMES],
        [vectors[name] for name in COLUMN_NAMES],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic 'signals' scanner-findings Parquet data")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Total rows to generate (default 10,000,000)")
    parser.add_argument("--out", type=str, default="scratch/signals_synthetic", help="Output directory")
    parser.add_argument("--prefix", type=str, default="signals", help="Output filename prefix")
    parser.add_argument("--row-group-size", type=int, default=250_000, help="Parquet max_rows_per_row_group")
    parser.add_argument("--max-page-bytes", type=int, default=0, help="rugo write_parquet max_page_bytes (0 = single page per chunk)")
    parser.add_argument("--max-rows-per-file", type=int, default=2_000_000, help="Rows per output file (built in memory, then written in one write_parquet call)")
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

    while rows_written < total_rows:
        file_n = min(args.max_rows_per_file, total_rows - rows_written)
        fname = outdir / f"{args.prefix}-{file_idx:04d}.parquet"
        print(f"writing {fname} ({file_n:,} rows)")

        morsel = generate_morsel(rng, file_n, rows_written, catalog, now)
        data = write_parquet(
            morsel,
            compression="zstd",
            bloom_filters=True,
            dictionary=True,
            max_rows_per_row_group=args.row_group_size,
            max_page_bytes=args.max_page_bytes,
        )
        with open(fname, "wb") as f:
            f.write(data)

        rows_written += file_n
        file_idx += 1
        print(f"  {rows_written:,}/{total_rows:,} rows")

    print("done")


if __name__ == "__main__":
    main()
