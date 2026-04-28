#!/usr/bin/env python3
"""
Benchmark document map creation: structural scan + interpretation only.
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from rugo._jsonl import benchmark_document_map

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
    print("Generating 256 MB test data...")
    target_mb = 256
    estimated_bytes_per_row = 150
    num_rows = int((target_mb * 1024 * 1024) / estimated_bytes_per_row)

    jsonl_bytes = generate_test_data(num_rows)
    actual_mb = len(jsonl_bytes) / 1024 / 1024

    print(f"\nBenchmarking document map creation (scan + interpret)")
    print(f"{'='*60}")
    print(f"Data size: {actual_mb:.2f} MB")
    print(f"Rows: {num_rows:,}")

    result = benchmark_document_map(jsonl_bytes)

    if result:
        scan_throughput = actual_mb / (result['scan_ms'] / 1000)
        interpret_throughput = actual_mb / (result['interpret_ms'] / 1000)
        total_throughput = actual_mb / (result['total_ms'] / 1000)

        print(f"\nStructural scan:        {result['scan_ms']:8.1f} ms  ({scan_throughput:7.1f} MB/s)")
        print(f"Document map building:  {result['interpret_ms']:8.1f} ms  ({interpret_throughput:7.1f} MB/s)")
        print(f"{'─'*60}")
        print(f"Total:                  {result['total_ms']:8.1f} ms  ({total_throughput:7.1f} MB/s)")
        print(f"\nRecords created: {result['num_records']:,}")

        if result['sample_map']:
            print(f"\nSample first record (first 3 fields):")
            for i, field in enumerate(result['sample_map'][:3]):
                key_start, key_end = field['key']
                val_start, val_end = field['value']
                print(f"  Field {i}: key[{key_start}:{key_end}] value[{val_start}:{val_end}] type={field['type']}")
    else:
        print("FAILED")
        return 1

    print(f"\n{'='*60}")
    print(f"Target: >1000 MB/s for structural scan + interpretation")
    print(f"{'='*60}\n")

    return 0

if __name__ == '__main__':
    sys.exit(main())
