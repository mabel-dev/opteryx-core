#!/usr/bin/env python3
"""
Build a human-authored commit message dataset in public.examples.messages from GH Archive.

- Reads all GH Archive hourly files for 2025-06-01 (00-23)
- Extracts commit messages from PushEvent payloads
- Filters out likely bots
- Targets ~1,000,000 rows
- Streams in ~128MB chunks (pre-compression) to avoid high memory use
"""

import contextlib
import gzip
import json
import os
import glob
from datetime import datetime

import pyarrow as pa
from pyiceberg_firestore_gcs import FirestoreCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

# GCP / Catalog configuration
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

WORKSPACE = "public"
SCHEMA_NAME = "examples"
TABLE_NAME = "messages"
TARGET_ROWS = 1_000_000
FILE_GLOB = "/Users/justin/Nextcloud/mabel/opteryx-cloud/2025-06-01-*.json.gz"
TARGET_FILE_BYTES = 128 * 1024 * 1024  # ~128MB preferred size (pre-compression)

print(f"Source files: {FILE_GLOB}")
print(f"Target table: {WORKSPACE}.{SCHEMA_NAME}.{TABLE_NAME}")


def looks_like_bot(actor_login: str, author_name: str, author_email: str, message: str) -> bool:
    parts = []
    for value in (actor_login, author_name, author_email):
        if value:
            parts.append(value.lower())
    text = " ".join(parts)

    if "bot" in text:
        return True
    if "dependabot" in text or "renovate" in text:
        return True
    if "github-actions" in text or "actions" in text:
        return True
    if author_email and "noreply" in author_email.lower():
        return True
    if message and len(message.strip()) < 8:
        return True
    return False


schema = pa.schema([
    pa.field("id", pa.string()),
    pa.field("created_at", pa.timestamp("us", tz="UTC")),
    pa.field("repo", pa.string()),
    pa.field("author", pa.string()),
    pa.field("message", pa.string()),
])

print("\nSetting up Iceberg catalog...")
catalog = FirestoreCatalog(
    WORKSPACE,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,
)

with contextlib.suppress(NamespaceAlreadyExistsError):
    catalog.create_namespace(SCHEMA_NAME)
    print(f"Created namespace: {SCHEMA_NAME}")

full_name = f"{SCHEMA_NAME}.{TABLE_NAME}"
try:
    catalog.drop_table(full_name)
    print(f"Dropped existing table: {full_name}")
except Exception:
    pass

iceberg_table = catalog.create_table(full_name, schema=schema, properties={"iceberg_compatible": "false"})
print("Table created; streaming appends targeting ~128MB chunks...")


def flush_batch(batch_ids, batch_ts, batch_repo, batch_author, batch_message, batch_no):
    if not batch_ids:
        return 0
    arrays = [
        pa.array(batch_ids, type=schema.field("id").type),
        pa.array(batch_ts, type=schema.field("created_at").type),
        pa.array(batch_repo, type=schema.field("repo").type),
        pa.array(batch_author, type=schema.field("author").type),
        pa.array(batch_message, type=schema.field("message").type),
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    iceberg_table.append(table)
    print(f"  ✓ Written chunk {batch_no} ({table.num_rows:,} rows)")
    return table.num_rows


def est_row_bytes(commit_id, repo_name, author, message):
    return (
        len(commit_id or "")
        + len(repo_name or "")
        + len(author or "")
        + len(message or "")
        + 32
    )


total_rows = 0
batch_rows = 0
batch_bytes = 0
batch_no = 1

batch_ids = []
batch_ts = []
batch_repo = []
batch_author = []
batch_message = []

total_events = 0
files = sorted(glob.glob(FILE_GLOB))
if not files:
    raise SystemExit("No GH Archive files found for pattern: " + FILE_GLOB)

for path in files:
    if total_rows >= TARGET_ROWS:
        break
    print(f"Reading {path} ...")
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if total_rows >= TARGET_ROWS:
                break
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_events += 1
            if record.get("type") != "PushEvent":
                continue

            payload = record.get("payload", {})
            commits = payload.get("commits", []) or []
            if not commits:
                continue

            event_created_at = record.get("created_at")
            try:
                ts = datetime.fromisoformat(event_created_at.replace("Z", "+00:00")) if event_created_at else None
            except Exception:
                ts = None

            repo_name = (record.get("repo") or {}).get("name")
            actor_login = (record.get("actor") or {}).get("login")

            for commit in commits:
                if total_rows >= TARGET_ROWS:
                    break

                author = (commit.get("author") or {}).get("name")
                author_email = (commit.get("author") or {}).get("email")
                message = commit.get("message") or ""

                if looks_like_bot(actor_login, author, author_email, message):
                    continue

                commit_id = commit.get("sha") or commit.get("id")
                if not commit_id:
                    continue

                if not message.strip():
                    continue

                row_bytes = est_row_bytes(commit_id, repo_name, author, message)
                if batch_bytes + row_bytes > TARGET_FILE_BYTES and batch_rows > 0:
                    total_rows += flush_batch(batch_ids, batch_ts, batch_repo, batch_author, batch_message, batch_no)
                    batch_no += 1
                    batch_ids.clear()
                    batch_ts.clear()
                    batch_repo.clear()
                    batch_author.clear()
                    batch_message.clear()
                    batch_rows = 0
                    batch_bytes = 0

                batch_ids.append(commit_id)
                batch_ts.append(ts)
                batch_repo.append(repo_name)
                batch_author.append(author)
                batch_message.append(message)
                batch_rows += 1
                batch_bytes += row_bytes

    print(f"  Collected so far: {total_rows + batch_rows:,} rows")

# Flush remainder
total_rows += flush_batch(batch_ids, batch_ts, batch_repo, batch_author, batch_message, batch_no)

print("✓ Data written to Iceberg in sized chunks")

print("\nDone!")
print(f"Table: {WORKSPACE}.{SCHEMA_NAME}.{TABLE_NAME}")
print(f"Rows: {total_rows:,}")
print("Sample query:")
print(f"  SELECT author, COUNT(*) AS commits FROM {WORKSPACE}.{SCHEMA_NAME}.{TABLE_NAME} GROUP BY author ORDER BY commits DESC LIMIT 10;")
