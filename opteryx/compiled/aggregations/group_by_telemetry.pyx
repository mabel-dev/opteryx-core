# cython: language_level=3

import time

from libc.stddef cimport size_t


cdef void initialize_groupby_readings(object self, object key_store_limit_bytes) noexcept:
    self._readings = {
        "time_groupby_finalize_backend_ns": 0,
        "time_groupby_finalize_rows_to_vectors_ns": 0,
        "time_groupby_finalize_morsel_build_ns": 0,
        "groupby_finalize_rows_count": 0,
        "groupby_finalize_chunks_emitted": 0,
        "groupby_finalize_fast_path_hits": 0,
        "draken_dict_groupby_fastpath_hits": 0,
        "draken_dict_groupby_fastpath_fallbacks": 0,
        "draken_constant_groupby_fastpath_hits": 0,
        "draken_constant_groupby_fastpath_fallbacks": 0,
        "draken_constant_groupby_output_vector_hits": 0,
        "draken_constant_groupby_output_vector_fallbacks": 0,
        "groupby_key_store_limit_bytes": 0 if key_store_limit_bytes is None else key_store_limit_bytes,
        "groupby_key_store_bytes": 0,
        "feature_groupby_engine_carchar": 0,
        "feature_groupby_engine_constant": 0,
        "feature_groupby_engine_multi_key_fixed": 0,
        "feature_groupby_engine_multi_key_object": 0,
        # ingest hot-loop diagnostics
        "groupby_ingest_hits": 0,
        "groupby_ingest_misses": 0,
        "time_groupby_ingest_state_assign_ns": 0,
        # bloom filter diagnostics (zero until bloom filter is wired up)
        "groupby_bloom_checks": 0,
        "groupby_bloom_skips": 0,
        "groupby_bloom_false_positives": 0,
        # ingest phase breakdown (identify the unaccounted ~33s)
        "time_groupby_hash_ns": 0,
        "time_groupby_reserve_ns": 0,
        "time_groupby_accumulate_ns": 0,
    }


cdef inline void record_finalize_backend_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_finalize_backend_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_finalize_rows_to_vectors_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_finalize_rows_to_vectors_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_finalize_morsel_build_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_finalize_morsel_build_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_finalize_rows_count(object self, Py_ssize_t rows) noexcept:
    self._readings["groupby_finalize_rows_count"] += rows


cdef inline void record_finalize_chunk_emitted(object self) noexcept:
    self._readings["groupby_finalize_chunks_emitted"] += 1


cdef inline void record_finalize_fast_path_hit(object self) noexcept:
    self._readings["groupby_finalize_fast_path_hits"] += 1


cdef inline void record_feature_groupby_engine_carchar(object self) noexcept:
    self._readings["feature_groupby_engine_carchar"] += 1


cdef inline void record_feature_groupby_engine_constant(object self) noexcept:
    self._readings["feature_groupby_engine_constant"] += 1


cdef inline void record_feature_groupby_engine_multi_key_fixed(object self) noexcept:
    self._readings["feature_groupby_engine_multi_key_fixed"] += 1


cdef inline void record_feature_groupby_engine_multi_key_object(object self) noexcept:
    self._readings["feature_groupby_engine_multi_key_object"] += 1


cdef inline void record_dict_groupby_fastpath_hit(object self) noexcept:
    self._readings["draken_dict_groupby_fastpath_hits"] += 1


cdef inline void record_groupby_key_store_bytes(object self, size_t key_store_bytes) noexcept:
    self._readings["groupby_key_store_bytes"] = key_store_bytes


cdef inline void record_constant_groupby_vector(object self, object vec) noexcept:
    if self._is_constant_like_vector(vec):
        self._readings["draken_constant_groupby_output_vector_hits"] += 1
    else:
        self._readings["draken_constant_groupby_output_vector_fallbacks"] += 1


cdef inline void record_ingest_state_assign_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_ingest_state_assign_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_ingest_hit_miss_counts(
    object self,
    Py_ssize_t hits,
    Py_ssize_t misses,
) noexcept:
    self._readings["groupby_ingest_hits"] += hits
    self._readings["groupby_ingest_misses"] += misses


cdef inline void record_groupby_hash_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_hash_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_groupby_reserve_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_reserve_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_groupby_accumulate_time(object self, long long started_ns) noexcept:
    self._readings["time_groupby_accumulate_ns"] += time.monotonic_ns() - started_ns


cdef inline void record_bloom_stats(
    object self,
    Py_ssize_t checks,
    Py_ssize_t skips,
    Py_ssize_t fps,
) noexcept:
    self._readings["groupby_bloom_checks"] += checks
    self._readings["groupby_bloom_skips"] += skips
    self._readings["groupby_bloom_false_positives"] += fps
