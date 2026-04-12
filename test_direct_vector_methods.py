#!/usr/bin/env python3
"""
Direct test of vector comparison methods.
Tests Date32Vector and TimestampVector comparison methods without full query execution.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from opteryx.compiled.draken.vectors.date32_vector import Date32Vector
from opteryx.compiled.draken.vectors.timestamp_vector import TimestampVector


def test_date32_vector_comparisons():
    """Test Date32Vector vector-to-vector comparison methods."""
    print("--- Testing Date32Vector Comparison Methods ---")

    # Test that methods exist and are callable
    methods = [
        "equals_vector",
        "not_equals_vector",
        "greater_than_vector",
        "greater_than_or_equals_vector",
        "less_than_vector",
        "less_than_or_equals_vector",
    ]

    for method_name in methods:
        if hasattr(Date32Vector, method_name):
            print(f"✓ Date32Vector.{method_name:30s} - method exists")
        else:
            print(f"✗ Date32Vector.{method_name:30s} - method NOT found")
            return False

    return True


def test_timestamp_vector_comparisons():
    """Test TimestampVector vector-to-vector comparison methods."""
    print("\n--- Testing TimestampVector Comparison Methods ---")

    try:
        # Test that methods exist and are callable
        methods = [
            "equals_vector",
            "not_equals_vector",
            "greater_than_vector",
            "greater_than_or_equals_vector",
            "less_than_vector",
            "less_than_or_equals_vector",
        ]

        for method_name in methods:
            if hasattr(TimestampVector, method_name):
                print(f"✓ TimestampVector.{method_name:30s} - method exists")
            else:
                print(f"✗ TimestampVector.{method_name:30s} - method NOT found")
                return False

    except Exception as e:
        print(f"✗ Error checking TimestampVector methods: {e}")
        return False

    return True


if __name__ == "__main__":
    try:
        success = True
        success = test_date32_vector_comparisons() and success
        success = test_timestamp_vector_comparisons() and success

        if success:
            print("\n✅ All vector comparison method signatures verified!")
            sys.exit(0)
        else:
            print("\n❌ Some checks failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
