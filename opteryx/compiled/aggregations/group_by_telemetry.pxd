from libc.stddef cimport size_t


cdef void initialize_groupby_readings(object self, object key_store_limit_bytes) noexcept

cdef inline void record_finalize_backend_time(object self, long long started_ns) noexcept
cdef inline void record_finalize_rows_to_vectors_time(object self, long long started_ns) noexcept
cdef inline void record_finalize_morsel_build_time(object self, long long started_ns) noexcept
cdef inline void record_finalize_rows_count(object self, Py_ssize_t rows) noexcept
cdef inline void record_finalize_chunk_emitted(object self) noexcept
cdef inline void record_finalize_fast_path_hit(object self) noexcept

cdef inline void record_feature_groupby_engine_carchar(object self) noexcept
cdef inline void record_feature_groupby_engine_constant(object self) noexcept
cdef inline void record_feature_groupby_engine_multi_key_fixed(object self) noexcept
cdef inline void record_feature_groupby_engine_multi_key_object(object self) noexcept

cdef inline void record_dict_groupby_fastpath_hit(object self) noexcept
cdef inline void record_groupby_key_store_bytes(object self, size_t key_store_bytes) noexcept
cdef inline void record_constant_groupby_vector(object self, object vec) noexcept

cdef inline void record_ingest_state_assign_time(object self, long long started_ns) noexcept
cdef inline void record_ingest_hit_miss_counts(
    object self,
    Py_ssize_t hits,
    Py_ssize_t misses,
) noexcept
cdef inline void record_groupby_hash_time(object self, long long started_ns) noexcept
cdef inline void record_groupby_reserve_time(object self, long long started_ns) noexcept
cdef inline void record_groupby_accumulate_time(object self, long long started_ns) noexcept

cdef inline void record_bloom_stats(
    object self,
    Py_ssize_t checks,
    Py_ssize_t skips,
    Py_ssize_t fps,
) noexcept
