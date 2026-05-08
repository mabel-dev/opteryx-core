#!/usr/bin/env python3
"""
Runner script for all shape battery tests.
Executes all test_shapes_*.py files in sequence.
"""
import subprocess
import sys
import time
from pathlib import Path

# Define test files in order. The other shape batteries have been migrated
# to sqllogictest in the sibling sqllogictest repo (examples/opteryx/tests/shapes/).
test_files = [
    'test_shapes_basic.py',
]

test_dir = Path(__file__).parent
print(f"Test directory: {test_dir}")
total_start = time.time()
failed_files = []

print("=" * 70)
print("RUNNING SHAPES AND ERRORS BATTERY TEST SUITE")
print("=" * 70)

for test_file in test_files:
    test_path = test_dir / test_file
    if not test_path.exists():
        print(f"⚠️  Skipping {test_file} (not found)")
        continue
    
    print(f"\n{'▶ ' + test_file:─<70}")
    start_time = time.time()
    ret = subprocess.call([sys.executable, str(test_path)])
    elapsed = time.time() - start_time
    
    if ret != 0:
        print(f"❌ FAILED in {elapsed:.2f}s (exit code: {ret})")
        failed_files.append(test_file)
    else:
        print(f"✅ PASSED in {elapsed:.2f}s")

print("\n" + "=" * 70)
total_elapsed = time.time() - total_start
print(f"✅ COMPLETED IN {total_elapsed:.2f}s")

if failed_files:
    print(f"\n❌ FAILED FILES ({len(failed_files)}):")
    for f in failed_files:
        print(f"   - {f}")
    print("\n💥 EXITING WITH ERROR CODE 1")
    sys.exit(1)
else:
    print("✨ ALL TESTS PASSED")
    print("🎉 EXITING WITH SUCCESS CODE 0")
