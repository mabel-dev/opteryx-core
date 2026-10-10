# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False

# Single consolidated Cython extension for rugo.
# All six reader/writer modules are included here so that draken bridge symbols
# (draken_vector_own_raw, draken_vector_own_string, etc.) are resolved within
# one translation unit — no cross-.so symbol lookup needed.

# draken LogicalKind ordinals (draken/vectors/_vector_bridge.h): 0 NONE,
# 1 TIMESTAMP, 2 TIME, 3 DECIMAL, 4 VECTOR, 5 IPV4. IPV4 is the ONLY kind that
# travels in the parquet key-value side channel — every other kind draken models
# has a parquet logical type of its own and round-trips through the schema
# annotation. Declared here because the parquet reader and writer .pxi below
# share this one module namespace and both need it.
cdef int _DRAKEN_LK_IPV4 = 5

from cpython.ref cimport PyObject

# The Python-edge column wrappers (csv/_csv_column_wrap.hpp, jsonl/_jsonl_column_wrap.hpp,
# parquet/_parquet_column_wrap.hpp) are declared PyObject* ... except NULL, never
# `object` (CLAUDE.md §3): each returns a NEW reference, or NULL with an exception set
# (which `except NULL` re-raises). _rugo_steal takes ownership of that reference:
# <object> increfs, the C-level Py_DECREF balances it. Cython 3's cpython.ref.Py_DECREF
# takes `object`, hence the raw shim (same idiom as draken/vectors/bool_vector.pyx).
cdef extern from *:
    """static inline void _rugo_decref(PyObject* op) { Py_DECREF(op); }"""
    void _rugo_decref(PyObject* op)


cdef inline object _rugo_steal(PyObject* raw):
    cdef object obj = <object>raw
    _rugo_decref(raw)
    return obj


include "_text_render.pxi"          # shared descriptor for the CSV / JSONL writers
include "_predicate_literal.pxi"     # predicate literal kinds for the CSV / JSONL readers
include "compression/_decompress.pxi"   # gzip/zstd/lz4 input for the JSONL / CSV readers
include "parquet/parquet_reader.pxi"
include "parquet/parquet_writer.pxi"
include "jsonl/_jsonl_reader.pxi"
include "jsonl/_jsonl_writer.pxi"
include "csv/_csv_reader.pxi"
include "csv/_csv_writer.pxi"
include "avro/_avro_reader.pxi"
