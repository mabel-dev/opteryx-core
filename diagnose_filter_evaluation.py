#!/usr/bin/env python
"""Diagnostic script to trace WHERE clause filter evaluation."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import opteryx

print("=" * 80)
print("DIAGNOSTIC: WHERE Clause Filter Evaluation")
print("=" * 80)

session = opteryx.session()

tests = [
    ("SELECT * FROM $planets", 9, "All rows"),
    ("SELECT * FROM $planets WHERE id = 1", 1, "id = 1"),
    ("SELECT * FROM $planets WHERE id > 3", 6, "id > 3"),
    ("SELECT * FROM $planets WHERE id < 5", 4, "id < 5"),
    ("SELECT * FROM $planets WHERE id >= 5", 5, "id >= 5"),
    ("SELECT * FROM $planets WHERE id <= 5", 5, "id <= 5"),
    ("SELECT * FROM $planets WHERE id != 1", 8, "id != 1"),
    ("SELECT * FROM $planets WHERE id BETWEEN 3 AND 6", 4, "id BETWEEN 3 AND 6"),
    ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", 3, "id IN (1, 3, 5)"),
    ("SELECT * FROM $planets WHERE id NOT IN (1, 3, 5)", 6, "id NOT IN (1, 3, 5)"),
]

for query, expected, description in tests:
    print(f"\n[TEST] {description}")
    print(f"Query: {query}")
    print("-" * 80)
    try:
        morsels = list(session.execute_to_morsels(query))
        total = sum(len(m) for m in morsels)
        status = "✓" if total == expected else "✗"
        print(f"{status} Returned {total} rows (expected {expected})")
        if total != expected:
            print(f"   MISMATCH: Expected {expected} but got {total}")
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}: {e}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
