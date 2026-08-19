# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False

# Single consolidated Cython extension for rugo.
# All six reader/writer modules are included here so that draken bridge symbols
# (draken_vector_own_raw, draken_vector_own_string, etc.) are resolved within
# one translation unit — no cross-.so symbol lookup needed.

# draken LogicalKind ordinals (draken/core/draken_bridge.h): 0 NONE,
# 1 TIMESTAMP, 2 TIME, 3 DECIMAL, 4 VECTOR, 5 IPV4. IPV4 is the ONLY kind that
# travels in the parquet key-value side channel — every other kind draken models
# has a parquet logical type of its own and round-trips through the schema
# annotation. Declared here because the parquet reader and writer .pxi below
# share this one module namespace and both need it.
cdef int _DRAKEN_LK_IPV4 = 5

include "_text_render.pxi"          # shared descriptor for the CSV / JSONL writers
include "parquet/parquet_reader.pxi"
include "parquet/parquet_writer.pxi"
include "jsonl/_jsonl_reader.pxi"
include "jsonl/_jsonl_writer.pxi"
include "csv/_csv_reader.pxi"
include "csv/_csv_writer.pxi"
