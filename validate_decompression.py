#!/usr/bin/env python3
"""
Validate our decompression against PyArrow reference.
"""
import struct

import pyarrow.parquet as pq

# Read with PyArrow
pf = pq.ParquetFile('testdata/catalog/opteryx/tweets/data/opteryx_test_tweets_data_data-1767142478530.parquet')

# Get the cves column's metadata from first row group
rg = pf.metadata.row_group(0)
col_meta = None
for i in range(rg.num_columns):
    col = rg.column(i)
    if 'cves' in col.path_in_schema:
        col_meta = col
        break

if col_meta:
    print(f"PyArrow cves column metadata:")
    print(f"  Path: {col_meta.path_in_schema}")
    print(f"  Num values: {col_meta.num_values}")
    print(f"  Total compressed size: {col_meta.total_compressed_size}")
    print(f"  Total uncompressed size: {col_meta.total_uncompressed_size}")
    
    # Read the file and extract the exact bytes for cves column
    with open('testdata/catalog/opteryx/tweets/data/opteryx_test_tweets_data_data-1767142478530.parquet', 'rb') as f:
        data = f.read()
    
    # Get column chunk offset from metadata
    # This requires parsing the Parquet footer
    footer_size = struct.unpack('<I', data[-8:-4])[0]
    print(f"\nFile footer size: {footer_size} bytes")
    print(f"Total file size: {len(data)} bytes")
    print(f"First 16 bytes: {' '.join(f'{b:02x}' for b in data[:16])}")
    print(f"Last 16 bytes: {' '.join(f'{b:02x}' for b in data[-16:])}")
else:
    print("cves column not found!")
