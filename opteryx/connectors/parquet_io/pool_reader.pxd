# pool_reader.pxd — public Cython interface for pool_reader.pyx
#
# Cython rule: when a .pxd exists for a .pyx, ALL cdef class C attributes
# MUST be declared here; the .pyx body can only add method implementations.
#
# parquet_read.pyx cimports:
#   CppIOPipeline            (to hold a typed reference + call submit_work_native)
#   _read_footer_payload     (cdef function → sequential footer fetch)
#   _rg_passes_predicates_native (cdef function → row-group pruning)

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.structures.memory_pool cimport MemoryPool, CppMemoryPool
from opteryx.compiled.structures.footer_cache cimport ParquetFooterBytesCache
from rugo.parquet_reader cimport ColumnStats, RowGroupStats


cdef extern from "io_pipeline.hpp" namespace "rugo":
    cdef cppclass ColumnOut:
        int direct_kind      # 0=pool; 1=int64 2=float32 3=float64 4=bool 5=decimal128
        void* data           # direct: draken_alloc'd positional values
        uint8_t* validity    # direct: draken_alloc'd null bitmap, or NULL
        uint32_t length      # direct: row count
        int64_t ref_id       # pool path: MemoryPool ref
        uint8_t dec_precision # DK_DECIMAL128 descriptor
        uint8_t dec_scale

    cdef cppclass MorselRef:
        string path
        int rg_idx
        vector[string] column_names
        vector[ColumnOut] columns
        int64_t bytes_fetched
        uint64_t read_ns
        uint64_t decode_ns
        string error
        bint success

    # Take ownership of column i's direct buffers (data returned, validity via
    # out param); nulls both slots so MorselRef's destructor won't free them.
    void* morsel_take_direct(MorselRef& m, size_t i, uint8_t** out_validity) nogil

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(int decode_workers, size_t queue_capacity) except +
        void submit_row_group(
            const string& path, int rg_idx,
            const vector[string]& column_names,
            const vector[ColumnStats]& column_stats,
        ) nogil
        void submit_row_group(
            const string& path, int rg_idx,
            const vector[string]& column_names,
            const vector[ColumnStats]& column_stats,
            const vector[uint8_t]& row_mask,
        ) nogil
        bint try_get_result(MorselRef& out) nogil
        bint wait_and_get_result(MorselRef& out) nogil
        void wait_shutdown() nogil
        int pending_work_count() nogil
        uint64_t spin_iterations() nogil
        uint64_t enqueue_count() nogil
        size_t queue_high_watermark() nogil
        uint64_t http_request_count() nogil
        uint64_t http_fetch_ops() nogil
        int http_latency_bucket_count() nogil
        uint64_t http_latency_bucket_bound_ms(int i) nogil
        uint64_t http_latency_bucket(int i) nogil
        uint64_t worker_blocked_ns() nogil
        uint64_t ipc_bytes_serialized() nogil


cdef extern from "pool_sink_adapter.hpp" namespace "opteryx":
    # Wire an opteryx::MemoryPool into the pipeline so workers serialize columns
    # straight into pool-reserved memory (no heap buffer, no commit() copy).
    void wire_pool_sink(ParquetIOPipeline* pipe, CppMemoryPool* pool)


cdef class CppIOPipeline:
    cdef ParquetIOPipeline* pipeline
    cdef public MemoryPool pool        # `public` → accessible as Python attribute
    # Legacy commit-copy counter. WP-6a eliminated the consumer-side commit
    # memcpy (workers now serialize straight into pool-reserved memory), so this
    # stays 0 — its being 0 while ipc_bytes_serialized is large is the evidence
    # the copy is gone. Kept for the harness before/after comparison.
    cdef public uint64_t committed_bytes
    cdef submit_work_native(self, str cpp_path, int rg_idx, list column_names, RowGroupStats* rg)
    cdef submit_work_native_masked(self, str cpp_path, int rg_idx, list column_names, RowGroupStats* rg, bytes row_mask)


cdef tuple _read_footer_payload(
    str path,
    int64_t file_size,
    ParquetFooterBytesCache footer_cache,
)

cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates)
