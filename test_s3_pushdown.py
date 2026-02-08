#!/usr/bin/env python
"""Test S3 Select pushdown integration."""

import os
import sys

import brace

# Now run a test query
import opteryx

# Import brace to set up the environment
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scratch'))


if __name__ == "__main__":
    print("\n" + "="*80)
    print("Testing S3 Select pushdown")
    print("="*80 + "\n")
    
    q = opteryx.session(user="justin", query_id="test-s3")
    
    try:
        # Query that requires reading actual data (not just using statistics)
        q.execute('SELECT name FROM public.examples.planets LIMIT 10')
        print("\nResults:")
        print(q.display())
        print(f"\nShape: {q.shape}")
        print(f"Messages: {q.messages}")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
