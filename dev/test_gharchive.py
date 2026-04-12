#!/usr/bin/env python3
"""
Test query to verify the GH Archive data loaded into Opteryx.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "opteryx-core"))

import opteryx
from opteryx.connectors.iceberg_connector import IcebergConnector
from pyiceberg_firestore_gcs import FirestoreCatalog

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

# Register Opteryx Iceberg connector
catalog = FirestoreCatalog(
    "public",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

opteryx.register_store("public", IcebergConnector, catalog=catalog, remove_prefix=True)

# Run test queries
print("=" * 70)
print("Test Query 1: Count total events")
print("=" * 70)
result = opteryx.query("SELECT COUNT(*) as total_events FROM public.examples.events")
print(result)

print("\n" + "=" * 70)
print("Test Query 2: Events by type")
print("=" * 70)
result = opteryx.query("""
    SELECT type, COUNT(*) as count 
    FROM public.examples.events 
    GROUP BY type 
    ORDER BY count DESC
""")
print(result)

print("\n" + "=" * 70)
print("Test Query 3: Top 10 most active actors")
print("=" * 70)
result = opteryx.query("""
    SELECT actor_login, COUNT(*) as event_count 
    FROM public.examples.events 
    GROUP BY actor_login 
    ORDER BY event_count DESC 
    LIMIT 10
""")
print(result)

print("\n" + "=" * 70)
print("Test Query 4: Top 10 most active repositories")
print("=" * 70)
result = opteryx.query("""
    SELECT repo_name, COUNT(*) as event_count 
    FROM public.examples.events 
    GROUP BY repo_name 
    ORDER BY event_count DESC 
    LIMIT 10
""")
print(result)

print("\n✅ All test queries completed successfully!")
