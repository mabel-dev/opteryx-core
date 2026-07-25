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
from libcpp.pair cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport shared_ptr

from opteryx.compiled.structures.memory_pool cimport MemoryPool, CppMemoryPool
from opteryx.compiled.structures.footer_cache cimport ParquetFooterBytesCache
from opteryx.compiled.thread_pool cimport CppThreadPool, PriorityPool
from rugo.parquet_reader cimport ColumnStats, RowGroupStats, FileStats


cdef extern from "io_pipeline.hpp" namespace "rugo":
    cdef cppclass ColumnOut:
        int direct_kind      # 0=pool 1=int64 2=float32 3=float64 4=bool 5=decimal128 6=varchar
        void* data           # direct: draken_alloc'd positional values / string slots
        uint8_t* validity    # direct: draken_alloc'd null bitmap, or NULL
        uint32_t length      # direct: logical row count
        int64_t ref_id       # pool path: MemoryPool ref
        uint8_t dec_precision # DK_DECIMAL128 descriptor
        uint8_t dec_scale
        void* arena          # DK_VARCHAR*: long-string byte arena
        size_t arena_len     # DK_VARCHAR*: valid arena bytes
        void* codes          # DK_VARCHAR_DICT: uint32 code per row
        uint32_t data_length # DK_VARCHAR_DICT: unique-value slot count
        bint dict_sorted     # dict shapes: `data` is ascending (is_sorted)
        void* keyhash        # E37: per-data-element hash seed (uint64), or NULL

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
        bint empty_filtered
        int64_t empty_rows
        vector[uint8_t] survivor_mask

    # Take ownership of column i's direct buffers (data returned, validity via
    # out param); nulls both slots so MorselRef's destructor won't free them.
    void* morsel_take_direct(MorselRef& m, size_t i, uint8_t** out_validity) nogil
    # Take ownership of column i's DK_VARCHAR* arena + dict codes (nulls both so
    # the destructor won't double-free what draken_vector_own_string frees).
    void morsel_take_string(MorselRef& m, size_t i, void** out_arena, void** out_codes) nogil

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(int decode_workers, size_t queue_capacity) except +
        # Gap #3 Phase 2b: shares an externally-owned priority pool (the query's
        # exec CppThreadPool) instead of self-constructing one — see the injecting
        # constructor in io_pipeline.hpp for the ownership/lifetime contract.
        ParquetIOPipeline(shared_ptr[PriorityPool] pool, size_t queue_capacity) except +
        # E37: per-projected-column key flag; 1 = build the hash seed for this
        # string column. Set once at plan time; empty → no string hashing.
        void set_hash_key_columns(const vector[uint8_t]& v)
        # Query-scoped HTTP tuning (host-connection cap / retries / bandwidth-
        # derived timeout floor). Set once at plan time, by value — see
        # HttpTuning's comment in http_client.hpp for why this is never stored
        # on the (thread_local, process-lifetime) HttpClient itself.
        void set_coalesce_tuning(double waste_ratio, int64_t max_bytes)
        void set_http_tuning(long max_host_connections, int max_retries,
                              double min_bandwidth_bytes_per_s, long timeout_floor_ms,
                              bint use_multiplexing, bint use_pipewait, bint force_http11)
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
        void add_int_needles(const string& column, const vector[int64_t]& needles) nogil
        void add_str_pred(const string& column, int kind, const vector[string]& vals) nogil
        void set_pass1_predicate(void* fn, void* ctx, const vector[string]& cols) nogil
        void clear_eq_needles() nogil
        bint try_get_result(MorselRef& out) nogil
        bint wait_and_get_result(MorselRef& out) nogil
        void cancel() nogil
        void wait_shutdown() nogil
        int pending_work_count() nogil
        uint64_t cancelled_skips() nogil
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
        uint64_t bytes_fetched() nogil
        uint64_t http_retries() nogil


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

cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates, str cpp_path)


# NativeScanPlan — planning-time output for the fully-native (zero-Python)
# scan-pull path (src/cpp/engine/native_parquet_scan_source.hpp). See the class
# docstring in pool_reader.pyx for the scope boundary (first landing).
cdef class NativeScanPlan:
    cdef ParquetIOPipeline* pipeline_ptr
    cdef unordered_map[string, FileStats]* footer_map
    cdef vector[pair[string, int]] work_items
    cdef vector[string] column_names
    # Per-projected-column declared string DrakenType (parallel to column_names):
    # DRAKEN_VARCHAR/NVARCHAR/VARBINARY tag for string columns, 0 for non-string.
    # The native Source borrows &string_types for per-column typing + DK_POOL
    # varchar routing (WP-01).
    cdef vector[int] string_types
    # WP-11: parallel to column_names. `decimal_columns[i]` = 1 marks an int64-backed
    # DECIMAL column (DK_POOL) so the native Source routes it to the decimal decoder;
    # `logical_coerce[i]` packs the DATE/TIMESTAMP/TIME/DECIMAL retag kind + unit /
    # precision-scale (see LC_* packing in native_parquet_scan_source.hpp). 0 = none.
    cdef vector[uint8_t] decimal_columns
    cdef vector[int] logical_coerce
    # E37: parallel to column_names. 1 = this column is a GROUP BY/JOIN/DISTINCT key
    # downstream, so the native Source carries its hash seed (keyhash_buf). All-zero
    # (the default) → no sidecar built — the pay-for-use gate.
    cdef vector[uint8_t] hash_key_columns
    cdef int in_flight_limit
    cdef int n_items
    # WP-02: row groups excluded by pushed-predicate min/max + bloom pruning at
    # plan time. n_items is the SURVIVING (scanned) count; pruned + n_items ==
    # every row group in the projected files. 0 when no predicates are pushed.
    cdef int pruned_items
    cdef bint _closed
    cdef MemoryPool _pool
    # Wall time spent fetching/parsing footers not already in _PARSED_FOOTER_CACHE
    # (one network round-trip per uncached file, serial). This is genuine IO, not
    # plan compilation — kept separate so callers can report it as its own cost
    # instead of it silently inflating whatever timer wraps this function.
    cdef public uint64_t footer_fetch_ns

    cpdef void close(self)


cpdef NativeScanPlan open_native_scan_plan(
    paths,
    column_names,
    int decode_workers=*,
    predicates=*,
    file_sizes=*,
    string_types=*,
    decimal_columns=*,
    logical_coerce=*,
    hash_key_columns=*,
    pool=*,
    filesystem=*,
    footer_bytes_cache=*,
)

# Plan-time eligibility gate for the native scan Source: proves from parsed
# footers that every projected column, in every row group, decodes to a
# DirectKind the Source supports (increment-1 scope: plain numerics only).
# `filesystem` supplies the signed-URL rewrite that makes a remote path eligible.
cpdef bint native_scan_supported(paths, column_names, expected_kinds, file_sizes=*,
                                 filesystem=*, footer_bytes_cache=*)
