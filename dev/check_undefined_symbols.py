#!/usr/bin/env python3
"""
Pre-wheel-build check: verify no undefined symbols in compiled extensions.
Run this after `make c` and before building wheels.

Usage: python dev/check_undefined_symbols.py
Exit code 0 = all good, 1 = undefined symbols found
"""

import subprocess
import sys
from pathlib import Path

def get_undefined_symbols(so_file):
    """Return set of undefined symbols in a .so file."""
    try:
        result = subprocess.run(
            ["nm", "-u", str(so_file)],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode != 0:
            return set()
        
        symbols = set()
        for line in result.stdout.splitlines():
            # Lines with undefined symbols start with whitespace then 'U'
            parts = line.split()
            if len(parts) >= 2 and parts[0] == 'U':
                sym = parts[1]
                # Skip Python C API symbols (expected)
                if not sym.startswith('_Py') and not sym.startswith('PyInit_'):
                    symbols.add(sym)
        return symbols
    except Exception as e:
        print(f"Error checking {so_file}: {e}")
        return set()

def main():
    so_files = list(Path(".").glob("**/*.so"))
    if not so_files:
        print("No .so files found. Run `make c` first.")
        return 1
    
    errors = []
    for so_file in sorted(so_files):
        undefined = get_undefined_symbols(so_file)
        if undefined:
            errors.append((so_file, undefined))
    
    if errors:
        print("❌ Found undefined symbols in compiled extensions:\n")
        for so_file, symbols in errors:
            print(f"  {so_file}:")
            for sym in sorted(symbols)[:10]:  # Show first 10
                print(f"    - {sym}")
            if len(symbols) > 10:
                print(f"    ... and {len(symbols) - 10} more")
        return 1
    else:
        print(f"✓ All {len(so_files)} .so files have symbols resolved")
        return 0

if __name__ == "__main__":
    sys.exit(main())
