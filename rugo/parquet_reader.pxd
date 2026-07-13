# Re-export point for cimport "rugo.parquet_reader" — the declarations live
# under rugo/src/parquet/ (colocated with their C++ headers); this file exists
# so cross-package cimports (e.g. opteryx/compiled/structures/column_stats.pyx)
# can resolve "rugo.parquet_reader" via Cython's standard package-path search.
include "src/parquet/parquet_reader.pxd"
