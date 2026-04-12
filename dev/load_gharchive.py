#!/usr/bin/env python3
"""
Load GH Archive data into Opteryx as public.examples.events.

This script:
1. Unzips the downloaded GH Archive JSON file
2. Reads the JSON lines and converts to PyArrow table
3. Loads the data into an Iceberg table at public.examples.events
"""

import os
import sys
import gzip
import json
import contextlib
from datetime import datetime

import pyarrow as pa
from pyiceberg_firestore_gcs import FirestoreCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

# Configuration
workspace = "public"
schema_name = "examples"
table_name = "events"
gharchive_file = "/Users/justin/Nextcloud/mabel/opteryx-cloud/2025-01-01-12.json.gz"

print(f"Loading GH Archive data from {gharchive_file}")
print(f"Target table: {workspace}.{schema_name}.{table_name}")

# Step 1: Read and parse the JSONL file
print("\n📖 Reading JSONL file...")
records = []
with gzip.open(gharchive_file, 'rt', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i % 50000 == 0 and i > 0:
            print(f"  Read {i:,} records...")
        try:
            record = json.loads(line)
            records.append(record)
        except json.JSONDecodeError as e:
            print(f"  Warning: Skipping invalid JSON at line {i+1}: {e}")
            continue

print(f"✓ Read {len(records):,} records")

# Step 2: Convert to PyArrow table
print("\n🔄 Converting to PyArrow table...")

# The GH Archive schema is complex with nested structures
# We'll flatten some key fields for easier querying
flattened_records = []
for record in records:
    flattened = {
        'id': record.get('id'),
        'type': record.get('type'),
        'public': record.get('public'),
        'created_at': record.get('created_at'),
        
        # Actor fields
        'actor_id': record.get('actor', {}).get('id'),
        'actor_login': record.get('actor', {}).get('login'),
        'actor_display_login': record.get('actor', {}).get('display_login'),
        'actor_url': record.get('actor', {}).get('url'),
        'actor_avatar_url': record.get('actor', {}).get('avatar_url'),
        
        # Repo fields
        'repo_id': record.get('repo', {}).get('id'),
        'repo_name': record.get('repo', {}).get('name'),
        'repo_url': record.get('repo', {}).get('url'),
        
        # Org fields (if present)
        'org_id': record.get('org', {}).get('id') if record.get('org') else None,
        'org_login': record.get('org', {}).get('login') if record.get('org') else None,
        
        # Keep the full payload as JSON
        'payload': json.dumps(record.get('payload', {})),
    }
    flattened_records.append(flattened)

# Convert created_at to timestamp
for record in flattened_records:
    if record['created_at']:
        try:
            record['created_at'] = datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))
        except:
            record['created_at'] = None

# Create PyArrow table with explicit schema
schema = pa.schema([
    pa.field('id', pa.string()),
    pa.field('type', pa.string()),
    pa.field('public', pa.bool_()),
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
    pa.field('actor_id', pa.int64()),
    pa.field('actor_login', pa.string()),
    pa.field('actor_display_login', pa.string()),
    pa.field('actor_url', pa.string()),
    pa.field('actor_avatar_url', pa.string()),
    pa.field('repo_id', pa.int64()),
    pa.field('repo_name', pa.string()),
    pa.field('repo_url', pa.string()),
    pa.field('org_id', pa.int64()),
    pa.field('org_login', pa.string()),
    pa.field('payload', pa.string()),
])

# Extract columns from flattened records
columns = {field.name: [] for field in schema}
for record in flattened_records:
    for field in schema:
        columns[field.name].append(record.get(field.name))

# Create arrays with proper types
arrays = []
for field in schema:
    arrays.append(pa.array(columns[field.name], type=field.type))

arrow_table = pa.Table.from_arrays(arrays, schema=schema)
print(f"✓ Created PyArrow table with {arrow_table.num_rows:,} rows and {arrow_table.num_columns} columns")

# Step 3: Create Iceberg catalog and load data
print("\n🗄️  Setting up Iceberg catalog...")
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,  # Use Parquet manifests for better performance
)

# Create namespace if it doesn't exist
with contextlib.suppress(NamespaceAlreadyExistsError):
    catalog.create_namespace(schema_name)
    print(f"✓ Created namespace: {schema_name}")

# Drop existing table if it exists
full_table_name = f"{schema_name}.{table_name}"
try:
    catalog.drop_table(full_table_name)
    print(f"✓ Dropped existing table: {full_table_name}")
except:
    pass

# Create new table
print(f"\n📝 Creating table: {full_table_name}")
table = catalog.create_table(
    full_table_name,
    schema=arrow_table.schema,
    properties={"iceberg_compatible": "false"}
)
print(f"✓ Table created")

# Append data
print(f"\n⬆️  Appending {arrow_table.num_rows:,} rows...")
table.append(arrow_table)
print(f"✓ Data loaded successfully!")

print(f"\n✅ Done! Table available at: {workspace}.{schema_name}.{table_name}")
print(f"   Total records: {arrow_table.num_rows:,}")
print(f"\nExample query:")
print(f"  SELECT type, COUNT(*) as count")
print(f"  FROM {workspace}.{schema_name}.{table_name}")
print(f"  GROUP BY type")
print(f"  ORDER BY count DESC")
