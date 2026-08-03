# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False

# Single consolidated Cython extension for rugo.
# All six reader/writer modules are included here so that draken bridge symbols
# (draken_vector_own_raw, draken_vector_own_string, etc.) are resolved within
# one translation unit — no cross-.so symbol lookup needed.

include "_text_render.pxi"          # shared descriptor for the CSV / JSONL writers
include "parquet/parquet_reader.pxi"
include "parquet/parquet_writer.pxi"
include "jsonl/_jsonl_reader.pxi"
include "jsonl/_jsonl_writer.pxi"
include "csv/_csv_reader.pxi"
include "csv/_csv_writer.pxi"
