#!/usr/bin/env python3
"""
JSONL interpretation phase isolation test.
Tests only structural scan + interpretation (no vector construction).
Goal: Measure throughput of the fast path.
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from rugo._jsonl import read_jsonl_raw

def generate_test_data(num_rows: int) -> bytes:
    """Generate JSONL test data."""
    rows = []
    for i in range(num_rows):
        rows.append({
            "id": i,
            "name": f"user_{i % 10000}",
            "score": float(i % 100),
            "timestamp": f"2024-01-{(i % 28) + 1:02d}",
            "active": i % 2 == 0,
            "email": f"user{i % 1000}@example.com",
        })

    jsonl_data = "\n".join(json.dumps(r) for r in rows)
    return jsonl_data.encode('utf-8')

def main():
    # Generate 256 MB of test data
    print("Generating test data...")
    target_mb = 256
    estimated_bytes_per_row = 150  # rough estimate
    num_rows = int((target_mb * 1024 * 1024) / estimated_bytes_per_row)

    jsonl_bytes = generate_test_data(num_rows)
    actual_mb = len(jsonl_bytes) / 1024 / 1024

    print(f"\n{'='*60}")
    print(f"Test 1: Full Scan (no projection, no predicates)")
    print(f"{'='*60}")
    print(f"Data size: {actual_mb:.2f} MB")
    print(f"Rows: {num_rows:,}")

    result = read_jsonl_raw(jsonl_bytes)

    if result['success']:
        throughput = result['buffer_size_mb'] / (result['elapsed_ms'] / 1000)
        print(f"Time: {result['elapsed_ms']:.1f} ms")
        print(f"Throughput: {throughput:.1f} MB/s")
        print(f"Rows interpreted: {result['num_rows']:,}")
        print(f"Columns detected: {result['column_names']}")

        if result['sample_record']:
            print(f"\nSample first record (first 3 fields):")
            for field in result['sample_record'][:3]:
                print(f"  {field}")
    else:
        print("FAILED")
        return 1

    # Test 2: With projection (2 columns)
    print(f"\n{'='*60}")
    print(f"Test 2: With Projection (columns: id, name)")
    print(f"{'='*60}")

    result = read_jsonl_raw(jsonl_bytes, columns=['id', 'name'])

    if result['success']:
        throughput = result['buffer_size_mb'] / (result['elapsed_ms'] / 1000)
        print(f"Time: {result['elapsed_ms']:.1f} ms")
        print(f"Throughput: {throughput:.1f} MB/s")
        print(f"Rows: {result['num_rows']:,}")
        print(f"Columns: {result['column_names']}")
    else:
        print("FAILED")
        return 1

    # Test 3: With predicate
    print(f"\n{'='*60}")
    print(f"Test 3: With Predicate (id > 1000000)")
    print(f"{'='*60}")

    result = read_jsonl_raw(jsonl_bytes, predicates=[('id', '>', '1000000')])

    if result['success']:
        throughput = result['buffer_size_mb'] / (result['elapsed_ms'] / 1000)
        print(f"Time: {result['elapsed_ms']:.1f} ms")
        print(f"Throughput: {throughput:.1f} MB/s")
        print(f"Rows passed predicate: {result['num_rows']:,}")
        print(f"Filter efficiency: {100*result['num_rows']/num_rows:.1f}%")
    else:
        print("FAILED")
        return 1

    print(f"\n{'='*60}")
    print("INTERPRETATION PHASE ISOLATED - TARGET: >100 MB/s for 256 MB")
    print(f"{'='*60}\n")

    return 0

if __name__ == '__main__':
    sys.exit(main())
