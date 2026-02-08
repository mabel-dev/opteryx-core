#!/usr/bin/env python
"""Test S3 Select directly."""

import os
import sys

import brace

from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scratch'))


if __name__ == "__main__":
    print("\nTesting S3 Select directly\n")
    
    # Create S3 filesystem
    fs = OpteryxS3FileSystem()
    
    # Try to open an S3 file with columns
    try:
        f = fs.open_input_file("s3://opteryx_data/public/examples/planets/data/data-1767295392972.parquet", columns=["name"])
        print(f"Successfully opened file: {type(f)}")
        print(f"File size: {len(f.getvalue())} bytes")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
