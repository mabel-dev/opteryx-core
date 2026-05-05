# pool_reader.pxd — public Cython interface for pool_reader.pyx
#
# Cython rule: when a .pxd exists for a .pyx, ALL cdef class C attributes
# MUST be declared here; the .pyx body can only add method implementations.
#
# parquet_read.pyx cimports:
#   CppIOPipeline            (to hold a typed reference + call submit_work_native)
#   _read_footer_payload     (cdef function → sequential footer fetch)
#   _rg_passes_predicates_native (cdef function → row-group pruning)

from libc.stdint cimport uint8_t, int32_t, int64_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from rugo.parquet_reader cimport ColumnStats, RowGroupStats


cdef extern from "io_pipeline.hpp" namespace "rugo":
    cdef cppclass MorselRef:
        string path
        int rg_idx
        vector[string] column_names
        vector[vector[uint8_t]] column_ipc_bytes
        int64_t bytes_fetched
        uint64_t read_ns
        uint64_t decode_ns
        string error
        bint success

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(int decode_workers, size_t queue_capacity) except +
        void submit_row_group(
            const string& path, int rg_idx,
            const vector[string]& column_names,
            const vector[ColumnStats]& column_stats,
        ) nogil
        bint try_get_result(MorselRef& out) nogil
        bint wait_and_get_result(MorselRef& out) nogil
        void wait_shutdown() nogil
        int pending_work_count() nogil
        uint64_t spin_iterations() nogil
        uint64_t enqueue_count() nogil
        size_t queue_high_watermark() nogil


cdef class CppIOPipeline:
    cdef ParquetIOPipeline* pipeline
    cdef public MemoryPool pool        # `public` → accessible as Python attribute
    cdef submit_work_native(self, str cpp_path, int rg_idx, list column_names, RowGroupStats* rg)


cdef tuple _read_footer_payload(
    object filesystem,
    str path,
    object file_size_in,
    object connector,
    object footer_cache,
)

cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates)
