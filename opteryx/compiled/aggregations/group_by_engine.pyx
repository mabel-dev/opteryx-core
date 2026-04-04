# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from __future__ import annotations

from array import array
import time

from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.bloom_filter cimport BloomFilter

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcmp
from libcpp.vector cimport vector

from opteryx.compiled.draken.core.buffers cimport ConstAccessor
from opteryx.compiled.draken.core.buffers cimport DictAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from opteryx.compiled.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.compiled.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT8
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT16
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_INT64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_STRING
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.compiled.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.compiled.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.integer_vector cimport IntegerVector
from opteryx.compiled.draken.vectors.date32_vector cimport Date32Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.time_vector cimport TimeVector
from opteryx.compiled.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.exceptions import UnsupportedSyntaxError
from libcpp.string cimport string
from opteryx.compiled.aggregations.key_codec cimport append_multi_key_record
from opteryx.compiled.aggregations.key_codec cimport append_single_encoded_key_record
from opteryx.compiled.aggregations.key_codec cimport append_single_fixed_key_record
from opteryx.compiled.aggregations.key_codec cimport decode_single_fixed_key_record
from opteryx.compiled.aggregations.key_codec cimport decode_multi_payload_keys
from opteryx.compiled.aggregations.key_codec cimport decode_single_payload_key
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_key_kind
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_read_float_value
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_read_int_value
from opteryx.compiled.aggregations.vector_readers cimport _dict_accessor_value_kind
from opteryx.compiled.aggregations.vector_readers cimport _dict_read_code
from opteryx.compiled.aggregations.vector_readers cimport _vector_dict_accessor
from opteryx.compiled.aggregations.vector_readers cimport _vector_value_dict_accessor
from opteryx.compiled.aggregations.group_by_finalize cimport build_encoded_key_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_multi_encoded_key_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_multi_object_state_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_native_object_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_object_state_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_key_vectors
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_object_aggregate_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_scalar_aggregate_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_multi_aggregate_vectors
from opteryx.compiled.aggregations.group_by_finalize cimport build_constant_groupby_vectors
from opteryx.compiled.aggregations.group_by_finalize cimport build_payload_multi_key_vectors
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_single_key_vector
from opteryx.compiled.aggregations.group_by_finalize cimport build_single_fixed_key_vector
from opteryx.compiled.aggregations.kernels.count_star cimport count_star_accumulate
from opteryx.compiled.aggregations.kernels.count_star cimport count_star_multi_accumulate
from opteryx.compiled.aggregations.kernels.sum_float64 cimport sum_f64_accumulate
from opteryx.compiled.aggregations.kernels.sum_float64 cimport sum_f64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.sum_float64 cimport sum_f64_multi_accumulate
from opteryx.compiled.aggregations.kernels.sum_float64 cimport sum_f64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_i64_accumulate
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_i64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_integer_accumulate
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_i64_multi_accumulate
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_i64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.sum_int64 cimport sum_integer_multi_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_f64_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_f64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_i64_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_i64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_integer_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_f64_multi_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_f64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_i64_multi_accumulate
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_i64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.min_max_fixed cimport minmax_integer_multi_accumulate
from opteryx.compiled.aggregations.kernels.avg_float64 cimport avg_f64_accumulate
from opteryx.compiled.aggregations.kernels.avg_float64 cimport avg_f64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.avg_float64 cimport avg_f64_multi_accumulate
from opteryx.compiled.aggregations.kernels.avg_float64 cimport avg_f64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_i64_accumulate
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_i64_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_integer_accumulate
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_i64_multi_accumulate
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_i64_multi_accumulate_from_dict
from opteryx.compiled.aggregations.kernels.avg_int64 cimport avg_integer_multi_accumulate
from opteryx.compiled.aggregations.kernels.count_distinct cimport count_distinct_accumulate
from opteryx.compiled.aggregations.kernels.count_distinct cimport count_distinct_multi_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_multi_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_integer_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_integer_multi_accumulate
from opteryx.compiled.aggregations.kernels.any_value_var cimport any_value_var_accumulate
from opteryx.compiled.aggregations.kernels.any_value_var cimport any_value_var_multi_accumulate
from opteryx.compiled.aggregations.kernels.min_max_var cimport minmax_var_accumulate
from opteryx.compiled.aggregations.kernels.min_max_var cimport minmax_var_multi_accumulate
# --- constant-key ingest (inlined from constant_keys.pyx) ---

cdef void _ingest_constant_distinct(object self, Morsel morsel, object value_vector, Py_ssize_t row_count):
    cdef Py_ssize_t row_idx
    cdef DrakenFixedBuffer* value_ptr
    cdef int64_t* value_i64_data
    cdef uint8_t* value_nulls
    cdef uint64_t[::1] value_hashes

    if self._constant_distinct_set is None:
        self._constant_distinct_set = CarcharSetWrapper()

    if isinstance(value_vector, Int64Vector):
        value_ptr = (<Int64Vector> value_vector).ptr
        value_i64_data = <int64_t*> value_ptr.data
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx) and (<CarcharSetWrapper> self._constant_distinct_set).insert(<uint64_t> value_i64_data[row_idx]):
                self._constant_count += 1
        return

    if isinstance(value_vector, IntegerVector):
        value_ptr = (<IntegerVector> value_vector).ptr
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx) and (<CarcharSetWrapper> self._constant_distinct_set).insert(<uint64_t> _read_integer_value(value_ptr, row_idx)):
                self._constant_count += 1
        return

    value_nulls = (<Vector> value_vector).null_bitmap_ptr()
    value_hashes = morsel.hash([self._value_column])
    for row_idx in range(row_count):
        if _bitmap_is_valid(value_nulls, row_idx) and (<CarcharSetWrapper> self._constant_distinct_set).insert(value_hashes[row_idx]):
            self._constant_count += 1


cdef void _ingest_constant_const_accessor(
    object self,
    Morsel morsel,
    object value_vector,
    ConstAccessor* value_const_accessor,
    Py_ssize_t row_count,
):
    cdef object value_obj
    cdef double val_decoded_f64
    cdef int64_t val_decoded_i64

    if self._agg_mode == 2:
        if value_const_accessor.is_null == 0:
            self._constant_count += row_count
        return

    if value_const_accessor.is_null != 0:
        return

    value_obj = value_vector[0]
    if self._agg_mode == 8:
        if self._constant_seen == 0:
            self._constant_object_state = value_obj
        self._constant_seen = 1
        return

    if self._value_kind == 3:
        if self._agg_mode == 4:
            if self._constant_seen == 0 or value_obj < self._constant_object_state:
                self._constant_object_state = value_obj
            self._constant_seen = 1
        elif self._agg_mode == 5:
            if self._constant_seen == 0 or value_obj > self._constant_object_state:
                self._constant_object_state = value_obj
            self._constant_seen = 1
        elif self._agg_mode == 7:
            _ingest_constant_distinct(self, morsel, value_vector, row_count)
        return

    if self._value_kind == 2:
        val_decoded_f64 = <double> value_obj
        if self._agg_mode == 3:
            self._constant_f64_state += row_count * val_decoded_f64
            self._constant_seen = 1
        elif self._agg_mode == 4:
            if self._constant_seen == 0 or val_decoded_f64 < self._constant_f64_state:
                self._constant_f64_state = val_decoded_f64
            self._constant_seen = 1
        elif self._agg_mode == 5:
            if self._constant_seen == 0 or val_decoded_f64 > self._constant_f64_state:
                self._constant_f64_state = val_decoded_f64
            self._constant_seen = 1
        elif self._agg_mode == 6:
            self._constant_avg_sum += row_count * val_decoded_f64
            self._constant_avg_count += row_count
        elif self._agg_mode == 7:
            _ingest_constant_distinct(self, morsel, value_vector, row_count)
        return

    val_decoded_i64 = <int64_t> value_obj
    if self._agg_mode == 3:
        self._constant_i64_state += row_count * val_decoded_i64
        self._constant_seen = 1
    elif self._agg_mode == 4:
        if self._constant_seen == 0 or val_decoded_i64 < self._constant_i64_state:
            self._constant_i64_state = val_decoded_i64
        self._constant_seen = 1
    elif self._agg_mode == 5:
        if self._constant_seen == 0 or val_decoded_i64 > self._constant_i64_state:
            self._constant_i64_state = val_decoded_i64
        self._constant_seen = 1
    elif self._agg_mode == 6:
        self._constant_avg_sum += row_count * val_decoded_i64
        self._constant_avg_count += row_count
    elif self._agg_mode == 7:
        _ingest_constant_distinct(self, morsel, value_vector, row_count)


cdef void _ingest_constant_vector_values(object self, object value_vector, Py_ssize_t row_count):
    cdef Py_ssize_t row_idx
    cdef DrakenFixedBuffer* value_ptr
    cdef int64_t* value_i64_data
    cdef double* value_f64_data
    cdef uint8_t* value_nulls
    cdef DictAccessor* value_dict_accessor = NULL
    cdef double val_decoded_f64
    cdef int64_t val_decoded_i64

    if isinstance(value_vector, Float64Vector):
        value_ptr = (<Float64Vector> value_vector).ptr
        value_f64_data = <double*> value_ptr.data
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if self._agg_mode == 2:
                if _bitmap_is_valid(value_nulls, row_idx):
                    self._constant_count += 1
            elif _bitmap_is_valid(value_nulls, row_idx):
                if self._agg_mode == 3:
                    self._constant_f64_state += value_f64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 4:
                    if self._constant_seen == 0 or value_f64_data[row_idx] < self._constant_f64_state:
                        self._constant_f64_state = value_f64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 5:
                    if self._constant_seen == 0 or value_f64_data[row_idx] > self._constant_f64_state:
                        self._constant_f64_state = value_f64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 6:
                    self._constant_avg_sum += value_f64_data[row_idx]
                    self._constant_avg_count += 1
        return

    if isinstance(value_vector, Int64Vector):
        value_ptr = (<Int64Vector> value_vector).ptr
        value_i64_data = <int64_t*> value_ptr.data
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if self._agg_mode == 2:
                if _bitmap_is_valid(value_nulls, row_idx):
                    self._constant_count += 1
            elif _bitmap_is_valid(value_nulls, row_idx):
                if self._agg_mode == 3:
                    self._constant_i64_state += value_i64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 4:
                    if self._constant_seen == 0 or value_i64_data[row_idx] < self._constant_i64_state:
                        self._constant_i64_state = value_i64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 5:
                    if self._constant_seen == 0 or value_i64_data[row_idx] > self._constant_i64_state:
                        self._constant_i64_state = value_i64_data[row_idx]
                    self._constant_seen = 1
                elif self._agg_mode == 6:
                    self._constant_avg_sum += value_i64_data[row_idx]
                    self._constant_avg_count += 1
        return

    value_dict_accessor = _vector_value_dict_accessor(value_vector)
    if value_dict_accessor != NULL:
        value_nulls = value_dict_accessor.row_nulls
        if self._value_kind == 5:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    val_decoded_f64 = _dict_accessor_read_float_value(value_dict_accessor, row_idx)
                    if self._agg_mode == 3:
                        self._constant_f64_state += val_decoded_f64
                        self._constant_seen = 1
                    elif self._agg_mode == 4:
                        if self._constant_seen == 0 or val_decoded_f64 < self._constant_f64_state:
                            self._constant_f64_state = val_decoded_f64
                        self._constant_seen = 1
                    elif self._agg_mode == 5:
                        if self._constant_seen == 0 or val_decoded_f64 > self._constant_f64_state:
                            self._constant_f64_state = val_decoded_f64
                        self._constant_seen = 1
            return
        elif self._value_kind == 4:
            for row_idx in range(row_count):
                if _bitmap_is_valid(value_nulls, row_idx):
                    val_decoded_i64 = _dict_accessor_read_int_value(value_dict_accessor, row_idx)
                    if self._agg_mode == 3:
                        self._constant_i64_state += val_decoded_i64
                        self._constant_seen = 1
                    elif self._agg_mode == 4:
                        if self._constant_seen == 0 or val_decoded_i64 < self._constant_i64_state:
                            self._constant_i64_state = val_decoded_i64
                        self._constant_seen = 1
                    elif self._agg_mode == 5:
                        if self._constant_seen == 0 or val_decoded_i64 > self._constant_i64_state:
                            self._constant_i64_state = val_decoded_i64
                        self._constant_seen = 1
            return

    value_ptr = (<IntegerVector> value_vector).ptr
    value_nulls = <uint8_t*> value_ptr.null_bitmap
    for row_idx in range(row_count):
        if self._agg_mode == 2:
            if _bitmap_is_valid(value_nulls, row_idx):
                self._constant_count += 1
        elif _bitmap_is_valid(value_nulls, row_idx):
            val_decoded_i64 = _read_integer_value(value_ptr, row_idx)
            if self._agg_mode == 3:
                self._constant_i64_state += val_decoded_i64
                self._constant_seen = 1
            elif self._agg_mode == 4:
                if self._constant_seen == 0 or val_decoded_i64 < self._constant_i64_state:
                    self._constant_i64_state = val_decoded_i64
                self._constant_seen = 1
            elif self._agg_mode == 5:
                if self._constant_seen == 0 or val_decoded_i64 > self._constant_i64_state:
                    self._constant_i64_state = val_decoded_i64
                self._constant_seen = 1
            elif self._agg_mode == 6:
                self._constant_avg_sum += val_decoded_i64
                self._constant_avg_count += 1


cdef void ingest_constant_mode(object self, Morsel morsel) except *:
    cdef Py_ssize_t row_count = morsel.num_rows
    cdef object value_vector
    cdef ConstAccessor* value_const_accessor = NULL

    if self._agg_mode == 1:
        self._constant_count += row_count
        return

    if self._agg_mode == 7:
        value_vector = morsel.column(self._value_column)
        _ingest_constant_distinct(self, morsel, value_vector, row_count)
        return

    value_vector = morsel.column(self._value_column)
    value_const_accessor = _vector_const_accessor(value_vector)
    if value_const_accessor != NULL:
        _ingest_constant_const_accessor(self, morsel, value_vector, value_const_accessor, row_count)
        return

    _ingest_constant_vector_values(self, value_vector, row_count)


# --- groupby telemetry (inlined from groupby_telemetry.pyx) ---

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
    if _is_constant_like_vector(vec):
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


cdef extern from "carchar_index.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharIndex:
        CarcharIndex(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries)
        size_t size() const
        bint lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        size_t insert_new(uint64_t key, int64_t payload_ref) except +


cdef int MODE_UNINITIALIZED = 0
cdef int MODE_CARCHAR = 2
cdef int MODE_CONSTANT = 3

cdef int AGG_UNSUPPORTED = 0
cdef int AGG_COUNT_STAR = 1
cdef int AGG_COUNT_VALUE = 2
cdef int AGG_SUM = 3
cdef int AGG_MIN = 4
cdef int AGG_MAX = 5
cdef int AGG_AVG = 6
cdef int AGG_COUNT_DISTINCT = 7
cdef int AGG_ANY_VALUE = 8

cdef double CARCHAR_INDEX_LOAD_FACTOR = 0.70

cdef int VALUE_NONE = 0
cdef int VALUE_INT64 = 1
cdef int VALUE_FLOAT64 = 2
cdef int VALUE_OBJECT = 3
cdef int VALUE_DICT_INT64 = 4
cdef int VALUE_DICT_FLOAT64 = 5

cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6


cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t index) noexcept:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline void _bitmap_set_valid(uint8_t* bitmap, Py_ssize_t index) noexcept:
    bitmap[index >> 3] |= <uint8_t>(1 << (index & 7))


cdef inline int64_t _read_integer_value(DrakenFixedBuffer* ptr, Py_ssize_t index) noexcept:
    if ptr.itemsize == 1:
        return (<char*> ptr.data)[index]
    if ptr.itemsize == 2:
        return (<short*> ptr.data)[index]
    if ptr.itemsize == 4:
        return (<int*> ptr.data)[index]
    return (<int64_t*> ptr.data)[index]


cdef inline uint32_t _read_dictionary_code(DrakenDictionaryBuffer* ptr, Py_ssize_t index) noexcept:
    if ptr.code_width == 1:
        return (<uint8_t*> ptr.codes)[index]
    if ptr.code_width == 2:
        return (<uint16_t*> ptr.codes)[index]
    return (<uint32_t*> ptr.codes)[index]


cdef inline int64_t _dictionary_type_to_key_kind(int dict_type) noexcept:
    if (
        dict_type == DRAKEN_INT8
        or dict_type == DRAKEN_INT16
        or dict_type == DRAKEN_INT32
        or dict_type == DRAKEN_INT64
        or dict_type == DRAKEN_BOOL
    ):
        return KEY_MULTI_FIXED_INT
    if dict_type == DRAKEN_DATE32:
        return KEY_MULTI_FIXED_DATE32
    if dict_type == DRAKEN_TIME32:
        return KEY_MULTI_FIXED_TIME32
    if dict_type == DRAKEN_TIME64:
        return KEY_MULTI_FIXED_TIME64
    if dict_type == DRAKEN_TIMESTAMP64:
        return KEY_MULTI_FIXED_TIMESTAMP64
    if dict_type == DRAKEN_STRING:
        return KEY_MULTI_ENCODED_STRING
    return 0


cdef inline ConstAccessor* _vector_const_accessor(object vec) noexcept:
    if isinstance(vec, Vector):
        return (<Vector> vec).const_accessor()
    return NULL


cdef inline bint _is_constant_like_vector(object vec) noexcept:
    return _vector_const_accessor(vec) != NULL


cdef inline bint _const_accessor_is_null(ConstAccessor* accessor) noexcept:
    return accessor == NULL or accessor.is_null != 0


cdef object _const_accessor_scalar(ConstAccessor* accessor):
    cdef DrakenConstantStringPayload* payload

    if accessor == NULL or accessor.is_null != 0:
        return None
    if accessor.value_type == DRAKEN_INT8:
        return (<int8_t*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_INT16:
        return (<int16_t*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_INT32 or accessor.value_type == DRAKEN_DATE32 or accessor.value_type == DRAKEN_TIME32:
        return (<int32_t*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_INT64 or accessor.value_type == DRAKEN_TIME64 or accessor.value_type == DRAKEN_TIMESTAMP64:
        return (<int64_t*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_FLOAT32:
        return (<float*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_FLOAT64:
        return (<double*>accessor.value_ptr)[0]
    if accessor.value_type == DRAKEN_BOOL:
        return (<uint8_t*>accessor.value_ptr)[0] != 0
    if accessor.value_type == DRAKEN_STRING:
        payload = <DrakenConstantStringPayload*>accessor.value_ptr
        return PyBytes_FromStringAndSize(<const char*>payload.data, payload.length)
    return None


cdef inline uint8_t* _vector_null_bitmap(object vec) noexcept:
    if isinstance(vec, Vector):
        return (<Vector> vec).null_bitmap_ptr()
    return NULL


cdef inline uint8_t* _alloc_valid_bitmap(Py_ssize_t length) except NULL:
    cdef Py_ssize_t nbytes
    cdef uint8_t* bitmap

    if length <= 0:
        return NULL

    nbytes = (length + 7) >> 3
    bitmap = <uint8_t*> malloc(nbytes)
    if bitmap == NULL:
        raise MemoryError()
    memset(bitmap, 0, nbytes)
    return bitmap


cdef class CarcharGroupStateEngine:
    cdef list _group_by_columns
    cdef list _aggregations
    cdef public dict _readings
    cdef object _key_store_limit_bytes

    cdef int _mode
    cdef Py_ssize_t _multi_agg_count
    cdef bint _use_object_keys
    cdef public int _agg_mode
    cdef public int _value_kind
    cdef int64_t _single_key_kind
    cdef bytes _group_column
    cdef public object _value_column
    cdef CarcharIndex* _index
    cdef bint _multi_key_object_mode
    cdef bint _multi_key_fixed_mode

    cdef object _constant_key_scalar
    cdef public object _constant_distinct_set
    cdef public object _constant_object_state
    cdef int64_t _constant_key_valid
    cdef public int64_t _constant_count
    cdef public int64_t _constant_i64_state
    cdef public double _constant_f64_state
    cdef public int64_t _constant_seen
    cdef public double _constant_avg_sum
    cdef public int64_t _constant_avg_count

    cdef vector[int64_t] _group_key_values
    cdef vector[int64_t] _group_key_valid
    cdef vector[int64_t] _counts
    cdef vector[int64_t] _i64_state
    cdef vector[double] _f64_state
    cdef vector[int64_t] _seen
    cdef vector[double] _avg_sums
    cdef vector[int64_t] _avg_counts
    cdef vector[vector[int64_t]] _multi_group_key_values
    cdef vector[vector[int64_t]] _multi_group_key_valid
    cdef vector[uint8_t] _encoded_key_bytes
    cdef vector[int32_t] _encoded_key_offsets
    cdef vector[int64_t] _encoded_key_valid
    cdef vector[vector[uint8_t]] _multi_encoded_key_bytes
    cdef vector[vector[int32_t]] _multi_encoded_key_offsets
    cdef vector[vector[int64_t]] _multi_encoded_key_valid
    cdef vector[int64_t] _multi_group_key_kinds
    cdef list _object_state
    cdef vector[uint8_t] _object_state_bytes
    cdef vector[int32_t] _object_state_starts
    cdef vector[int32_t] _object_state_lengths
    cdef list _distinct_sets
    cdef list _multi_value_columns
    cdef vector[int64_t] _multi_agg_modes
    cdef vector[int64_t] _multi_value_kinds
    cdef vector[int64_t] _multi_counts
    cdef vector[int64_t] _multi_i64_state
    cdef vector[double] _multi_f64_state
    cdef vector[int64_t] _multi_seen
    cdef vector[double] _multi_avg_sums
    cdef vector[int64_t] _multi_avg_counts
    cdef list _multi_object_state
    cdef vector[uint8_t] _multi_object_state_bytes
    cdef vector[int32_t] _multi_object_state_starts
    cdef vector[int32_t] _multi_object_state_lengths
    cdef list _multi_distinct_sets
    cdef vector[uint8_t] _key_payload_bytes
    cdef vector[int64_t] _key_payload_offsets
    cdef public object _debug_last_finalize_stage

    cdef BloomFilter _groupby_bloom          # None until second morsel; never reset after creation
    cdef vector[uint64_t] _bloom_hashes      # hash of each new state during first morsel
    cdef bint _use_bloom                     # True once bloom is ready to use

    def __cinit__(
        self,
        list group_by_columns,
        list aggregations,
        object key_store_limit_bytes=None,
    ):
        self._group_by_columns = group_by_columns
        self._aggregations = aggregations
        self._key_store_limit_bytes = key_store_limit_bytes
        self._mode = MODE_UNINITIALIZED
        self._multi_agg_count = 0
        self._use_object_keys = False
        self._agg_mode = AGG_UNSUPPORTED
        self._value_kind = VALUE_NONE
        self._single_key_kind = KEY_MULTI_FIXED_INT
        self._group_column = group_by_columns[0] if len(group_by_columns) == 1 else b""
        self._value_column = aggregations[0][2] if len(aggregations) == 1 else None
        self._multi_key_object_mode = False
        self._multi_key_fixed_mode = False
        self._multi_value_columns = []
        self._multi_distinct_sets = []
        self._index = NULL
        self._debug_last_finalize_stage = None
        self._groupby_bloom = None
        self._use_bloom = False
        self._constant_key_scalar = None
        self._constant_distinct_set = None
        self._constant_object_state = None
        self._constant_key_valid = 0
        self._constant_count = 0
        self._constant_i64_state = 0
        self._constant_f64_state = 0.0
        self._constant_seen = 0
        self._constant_avg_sum = 0.0
        self._constant_avg_count = 0
        self._object_state = []
        self._object_state_starts.clear()
        self._object_state_lengths.clear()
        self._distinct_sets = []
        self._multi_object_state = []
        self._multi_object_state_starts.clear()
        self._multi_object_state_lengths.clear()
        self._encoded_key_offsets.push_back(0)
        self._key_payload_offsets.push_back(0)
        initialize_groupby_readings(self, key_store_limit_bytes)

    def __dealloc__(self):
        if self._index is not NULL:
            del self._index
            self._index = NULL

    @property
    def backend(self):
        return self

    @property
    def readings(self):
        return self._readings

    cdef void _raise_unsupported_shape(self):
        raise UnsupportedSyntaxError(
            "Carchar group-state engine does not support this query shape."
        )

    cdef void _init_legacy_backend(self):
        self._raise_unsupported_shape()

    cdef inline bint _has_multi_agg(self) noexcept:
        return self._multi_agg_count > 0

    cdef inline Py_ssize_t _state_count(self) noexcept:
        if self._mode == MODE_CARCHAR and self._key_payload_offsets.size() > 0:
            return <Py_ssize_t>(self._key_payload_offsets.size() - 1)
        return <Py_ssize_t> self._group_key_values.size()

    cdef inline Py_ssize_t _multi_offset(self, int64_t state_index, Py_ssize_t agg_idx) noexcept:
        return <Py_ssize_t>state_index * self._multi_agg_count + agg_idx

    cdef object _debug_key_payload_value(self, Py_ssize_t state_index):
        cdef list key_values
        cdef list key_valids

        if <Py_ssize_t> self._key_payload_offsets.size() < state_index + 2:
            if self._multi_key_fixed_mode:
                key_values = []
                key_valids = []
                for key_idx in range(len(self._group_by_columns)):
                    key_values.append(self._multi_group_key_values[key_idx][state_index])
                    key_valids.append(self._multi_group_key_valid[key_idx][state_index])
                return tuple(key_values), tuple(key_valids)
            return self._group_key_values[state_index], self._group_key_valid[state_index]

        if len(self._group_by_columns) > 1:
            return decode_multi_payload_keys(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._multi_group_key_kinds,
                state_index,
            )

        return decode_single_payload_key(
            self._key_payload_bytes,
            self._key_payload_offsets,
            state_index,
            self._single_key_kind,
        )

    cdef inline bint _agg_output_is_float(self, Py_ssize_t agg_idx) noexcept:
        if self._multi_agg_count > 0:
            return (
                self._multi_agg_modes[agg_idx] == AGG_AVG
                or (
                    self._multi_value_kinds[agg_idx] in (VALUE_FLOAT64, VALUE_DICT_FLOAT64)
                    and self._multi_agg_modes[agg_idx] in (AGG_SUM, AGG_MIN, AGG_MAX)
                )
            )
        return self._agg_mode == AGG_AVG or self._value_kind in (VALUE_FLOAT64, VALUE_DICT_FLOAT64)

    cdef inline bint _agg_output_is_object(self, Py_ssize_t agg_idx) noexcept:
        if self._multi_agg_count > 0:
            return (
                self._multi_value_kinds[agg_idx] == VALUE_OBJECT
                and self._multi_agg_modes[agg_idx] in (AGG_MIN, AGG_MAX, AGG_ANY_VALUE)
            )
        return self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX, AGG_ANY_VALUE)

    cdef inline bint _is_stringlike_vector(self, object vec) noexcept:
        cdef DictAccessor* dict_accessor = NULL
        if isinstance(vec, StringVector):
            return True
        dict_accessor = _vector_dict_accessor(vec)
        if dict_accessor != NULL:
            return _dict_accessor_key_kind(dict_accessor) == KEY_MULTI_ENCODED_STRING
        return False

    cdef inline int _compare_bytes(
        self,
        const char* left_ptr,
        Py_ssize_t left_len,
        const uint8_t* right_ptr,
        Py_ssize_t right_len,
    ) noexcept:
        cdef Py_ssize_t shared = left_len if left_len < right_len else right_len
        cdef int cmp = 0
        if shared > 0:
            cmp = memcmp(left_ptr, <const char*> right_ptr, <size_t> shared)
            if cmp != 0:
                return cmp
        if left_len < right_len:
            return -1
        if left_len > right_len:
            return 1
        return 0

    cdef inline void _store_object_state_bytes(
        self,
        Py_ssize_t state_index,
        const char* data_ptr,
        Py_ssize_t data_len,
    ) noexcept:
        cdef Py_ssize_t idx
        self._object_state_starts[state_index] = <int32_t> self._object_state_bytes.size()
        self._object_state_lengths[state_index] = <int32_t> data_len
        for idx in range(data_len):
            self._object_state_bytes.push_back(<uint8_t> data_ptr[idx])

    cdef inline void _store_multi_object_state_bytes(
        self,
        Py_ssize_t offset,
        const char* data_ptr,
        Py_ssize_t data_len,
    ) noexcept:
        cdef Py_ssize_t idx
        self._multi_object_state_starts[offset] = <int32_t> self._multi_object_state_bytes.size()
        self._multi_object_state_lengths[offset] = <int32_t> data_len
        for idx in range(data_len):
            self._multi_object_state_bytes.push_back(<uint8_t> data_ptr[idx])

    cdef inline uint8_t* _value_null_bitmap(self, object value_vector):
        return _vector_null_bitmap(value_vector)

    cdef inline void _init_multi_fixed_key_columns(self, Py_ssize_t key_count):
        cdef Py_ssize_t idx
        self._multi_group_key_values.clear()
        self._multi_group_key_valid.clear()
        for idx in range(key_count):
            self._multi_group_key_values.push_back(vector[int64_t]())
            self._multi_group_key_valid.push_back(vector[int64_t]())

    cdef inline void _init_multi_encoded_key_columns(self, Py_ssize_t key_count):
        cdef Py_ssize_t idx
        self._multi_encoded_key_bytes.clear()
        self._multi_encoded_key_offsets.clear()
        self._multi_encoded_key_valid.clear()
        for idx in range(key_count):
            self._multi_encoded_key_bytes.push_back(vector[uint8_t]())
            self._multi_encoded_key_offsets.push_back(vector[int32_t]())
            self._multi_encoded_key_offsets[idx].push_back(0)
            self._multi_encoded_key_valid.push_back(vector[int64_t]())

    cdef inline bint _is_multi_fixed_kind(self, int64_t key_kind) noexcept:
        return (
            key_kind == KEY_MULTI_FIXED_INT
            or key_kind == KEY_MULTI_FIXED_DATE32
            or key_kind == KEY_MULTI_FIXED_TIME32
            or key_kind == KEY_MULTI_FIXED_TIME64
            or key_kind == KEY_MULTI_FIXED_TIMESTAMP64
        )

    cdef inline bint _supports_count_distinct_value(self, object value_vector) noexcept:
        cdef DictAccessor* dict_accessor = _vector_value_dict_accessor(value_vector)

        if isinstance(value_vector, (Int64Vector, IntegerVector, StringVector)):
            return True
        if dict_accessor != NULL:
            return not (
                dict_accessor.value_type == DRAKEN_FLOAT32
                or dict_accessor.value_type == DRAKEN_FLOAT64
            )
        return False

    cdef inline int64_t _read_dictionary_fixed_key(
        self,
        object key_vector,
        Py_ssize_t row_idx,
        int64_t* key_valid_flag,
    ) except *:
        cdef DictAccessor* dict_accessor = _vector_dict_accessor(key_vector)
        cdef DrakenVarBuffer* dict_values = NULL
        cdef uint8_t* nulls = NULL
        cdef uint32_t code = 0

        key_valid_flag[0] = 0
        if dict_accessor == NULL or dict_accessor.dict_values == NULL:
            return 0
        if not _bitmap_is_valid(dict_accessor.row_nulls, row_idx):
            return 0

        dict_values = dict_accessor.dict_values
        code = _dict_read_code(dict_accessor, row_idx)
        if code >= dict_values.length:
            raise IndexError("Dictionary code out of range")

        nulls = <uint8_t*>dict_values.null_bitmap
        if not _bitmap_is_valid(nulls, code):
            return 0

        key_valid_flag[0] = 1
        if dict_accessor.value_type == DRAKEN_INT8:
            return (<int8_t*>dict_values.data)[code]
        if dict_accessor.value_type == DRAKEN_INT16:
            return (<int16_t*>dict_values.data)[code]
        if (
            dict_accessor.value_type == DRAKEN_INT32
            or dict_accessor.value_type == DRAKEN_DATE32
            or dict_accessor.value_type == DRAKEN_TIME32
        ):
            return (<int32_t*>dict_values.data)[code]
        if (
            dict_accessor.value_type == DRAKEN_INT64
            or dict_accessor.value_type == DRAKEN_TIME64
            or dict_accessor.value_type == DRAKEN_TIMESTAMP64
        ):
            return (<int64_t*>dict_values.data)[code]
        if dict_accessor.value_type == DRAKEN_BOOL:
            return 1 if (<uint8_t*>dict_values.data)[code] != 0 else 0

        raise UnsupportedSyntaxError(
            "Carchar group-state engine only supports fixed-width and string dictionary keys."
        )

    cdef inline void _append_single_encoded_key(
        self,
        const char* data_ptr,
        Py_ssize_t data_len,
        int64_t valid_flag,
    ) except *:
        cdef Py_ssize_t idx
        cdef int32_t next_offset = self._encoded_key_offsets[self._encoded_key_offsets.size() - 1]

        if valid_flag != 0 and data_len > 0:
            for idx in range(data_len):
                self._encoded_key_bytes.push_back(<uint8_t> data_ptr[idx])
            next_offset += <int32_t> data_len
        self._encoded_key_offsets.push_back(next_offset)
        self._encoded_key_valid.push_back(valid_flag)

    cdef inline void _append_multi_encoded_key(
        self,
        Py_ssize_t key_idx,
        const char* data_ptr,
        Py_ssize_t data_len,
        int64_t valid_flag,
    ) except *:
        cdef Py_ssize_t idx
        cdef int32_t next_offset = self._multi_encoded_key_offsets[key_idx][
            self._multi_encoded_key_offsets[key_idx].size() - 1
        ]

        if valid_flag != 0 and data_len > 0:
            for idx in range(data_len):
                self._multi_encoded_key_bytes[key_idx].push_back(<uint8_t> data_ptr[idx])
            next_offset += <int32_t> data_len
        self._multi_encoded_key_offsets[key_idx].push_back(next_offset)
        self._multi_encoded_key_valid[key_idx].push_back(valid_flag)

    cdef inline int64_t _extract_stringlike_key(
        self,
        object key_vector,
        Py_ssize_t row_idx,
        const char** data_ptr,
        Py_ssize_t* data_len,
    ) except *:
        cdef DrakenVarBuffer* str_ptr
        cdef DictAccessor* dict_accessor = NULL
        cdef DrakenVarBuffer* dict_values
        cdef uint8_t* nulls
        cdef uint32_t code
        cdef int32_t start
        cdef int32_t stop

        if isinstance(key_vector, StringVector):
            str_ptr = (<StringVector> key_vector).ptr
            nulls = <uint8_t*> str_ptr.null_bitmap
            if not _bitmap_is_valid(nulls, row_idx):
                data_ptr[0] = NULL
                data_len[0] = 0
                return 0
            start = str_ptr.offsets[row_idx]
            stop = str_ptr.offsets[row_idx + 1]
            data_ptr[0] = <const char*> str_ptr.data + start
            data_len[0] = stop - start
            return 1

        dict_accessor = _vector_dict_accessor(key_vector)
        if dict_accessor != NULL:
            nulls = dict_accessor.row_nulls
            if not _bitmap_is_valid(nulls, row_idx):
                data_ptr[0] = NULL
                data_len[0] = 0
                return 0
            dict_values = dict_accessor.dict_values
            if dict_values == NULL or dict_accessor.value_type != DRAKEN_STRING:
                raise UnsupportedSyntaxError(
                    "Carchar group-state engine only supports string dictionary keys on the native encoded-key path."
                )
            code = _dict_read_code(dict_accessor, row_idx)
            nulls = <uint8_t*> dict_values.null_bitmap
            if not _bitmap_is_valid(nulls, code):
                data_ptr[0] = NULL
                data_len[0] = 0
                return 0
            start = dict_values.offsets[code]
            stop = dict_values.offsets[code + 1]
            data_ptr[0] = <const char*> dict_values.data + start
            data_len[0] = stop - start
            return 1

        raise UnsupportedSyntaxError(
            "Carchar group-state engine only supports native encoded storage for string-like group keys."
        )

    cdef inline void _append_single_payload_key(
        self,
        const char* data_ptr,
        Py_ssize_t data_len,
        int64_t key_valid_flag,
    ) except *:
        if not append_single_encoded_key_record(
            self._key_payload_bytes,
            self._key_payload_offsets,
            data_ptr,
            data_len,
            key_valid_flag,
        ):
            raise RuntimeError("failed to serialize encoded group key record")

    cdef inline void _append_single_fixed_payload_key(
        self,
        int64_t key_value,
        int64_t key_valid_flag,
    ) except *:
        if not append_single_fixed_key_record(
            self._key_payload_bytes,
            self._key_payload_offsets,
            key_value,
            key_valid_flag,
        ):
            raise RuntimeError("failed to serialize fixed group key record")



    cdef inline void _append_multi_fixed_payload_key_from_vectors(
        self,
        list key_vectors,
        Py_ssize_t row_idx,
    ) except *:
        cdef Py_ssize_t key_idx
        cdef object key_vector
        cdef DrakenFixedBuffer* key_ptr
        cdef uint8_t* key_null_bitmap
        cdef int64_t key_value
        cdef int64_t key_valid_flag
        cdef vector[int64_t] fixed_values
        cdef vector[int64_t] fixed_valids
        cdef vector[string] encoded_values
        cdef vector[int64_t] encoded_valids

        for key_idx in range(len(key_vectors)):
            key_vector = key_vectors[key_idx]

            # Extract pointer and null bitmap based on actual vector type
            if isinstance(key_vector, Int64Vector):
                key_ptr = (<Int64Vector> key_vector).ptr
                key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
            elif isinstance(key_vector, Date32Vector):
                key_ptr = (<Date32Vector> key_vector).ptr
                key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
            elif isinstance(key_vector, TimeVector):
                key_ptr = (<TimeVector> key_vector).ptr
                key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
            elif isinstance(key_vector, TimestampVector):
                key_ptr = (<TimestampVector> key_vector).ptr
                key_null_bitmap = <uint8_t*> key_ptr.null_bitmap
            else:
                key_ptr = (<IntegerVector> key_vector).ptr
                key_null_bitmap = <uint8_t*> key_ptr.null_bitmap

            key_valid_flag = 1 if _bitmap_is_valid(key_null_bitmap, row_idx) else 0
            key_value = _read_integer_value(key_ptr, row_idx) if key_valid_flag != 0 else 0
            fixed_values.push_back(key_value)
            fixed_valids.push_back(key_valid_flag)

        if not append_multi_key_record(
            self._key_payload_bytes,
            self._key_payload_offsets,
            fixed_values,
            fixed_valids,
            encoded_values,
            encoded_valids,
        ):
            raise RuntimeError("failed to serialize multi fixed group key record")

    cdef inline void _append_multi_payload_key(
        self,
        list key_vectors,
        Py_ssize_t row_idx,
    ) except *:
        cdef Py_ssize_t key_idx
        cdef int64_t key_kind
        cdef int64_t key_valid_flag
        cdef int64_t key_value
        cdef const char* data_ptr = NULL
        cdef Py_ssize_t data_len = 0
        cdef object key_vector
        cdef vector[int64_t] fixed_values
        cdef vector[int64_t] fixed_valids
        cdef vector[string] encoded_values
        cdef vector[int64_t] encoded_valids
        cdef string encoded_value

        for key_idx in range(len(key_vectors)):
            key_vector = key_vectors[key_idx]
            key_kind = self._multi_group_key_kinds[key_idx]
            if self._is_multi_fixed_kind(key_kind):
                if _vector_dict_accessor(key_vector) != NULL:
                    key_value = self._read_dictionary_fixed_key(key_vector, row_idx, &key_valid_flag)
                elif isinstance(key_vector, Int64Vector):
                    key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<Int64Vector> key_vector).ptr.null_bitmap, row_idx) else 0
                    key_value = _read_integer_value((<Int64Vector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
                elif isinstance(key_vector, IntegerVector):
                    key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<IntegerVector> key_vector).ptr.null_bitmap, row_idx) else 0
                    key_value = _read_integer_value((<IntegerVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
                elif isinstance(key_vector, Date32Vector):
                    key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<Date32Vector> key_vector).ptr.null_bitmap, row_idx) else 0
                    key_value = _read_integer_value((<Date32Vector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
                elif isinstance(key_vector, TimeVector):
                    key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<TimeVector> key_vector).ptr.null_bitmap, row_idx) else 0
                    key_value = _read_integer_value((<TimeVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
                elif isinstance(key_vector, TimestampVector):
                    key_valid_flag = 1 if _bitmap_is_valid(<uint8_t*> (<TimestampVector> key_vector).ptr.null_bitmap, row_idx) else 0
                    key_value = _read_integer_value((<TimestampVector> key_vector).ptr, row_idx) if key_valid_flag != 0 else 0
                else:
                    raise UnsupportedSyntaxError(
                        "Unsupported fixed-width group key vector in Carchar payload arena."
                    )
                fixed_values.push_back(key_value)
                fixed_valids.push_back(key_valid_flag)
                continue

            key_valid_flag = self._extract_stringlike_key(key_vector, row_idx, &data_ptr, &data_len)
            if key_valid_flag != 0:
                encoded_value.assign(data_ptr, <size_t> data_len)
            else:
                encoded_value.clear()
            encoded_values.push_back(encoded_value)
            encoded_valids.push_back(key_valid_flag)

        if not append_multi_key_record(
            self._key_payload_bytes,
            self._key_payload_offsets,
            fixed_values,
            fixed_valids,
            encoded_values,
            encoded_valids,
        ):
            raise RuntimeError("failed to serialize multi group key record")

    cdef void _maybe_init_carchar_mode(self, Morsel morsel):
        cdef object fn
        cdef object column
        cdef object key_vector
        cdef object value_vector
        cdef tuple aggregation
        cdef ConstAccessor* key_const_accessor = NULL
        cdef Py_ssize_t agg_idx
        cdef int64_t key_kind
        cdef bint stringlike_key_vector = False
        cdef DictAccessor* dict_accessor = NULL
        cdef DictAccessor* key_dict_accessor = NULL

        if self._mode != MODE_UNINITIALIZED or morsel is None or morsel.num_rows == 0:
            return

        if len(self._group_by_columns) == 0:
            self._init_legacy_backend()
            return

        if len(self._group_by_columns) > 1:
            self._multi_key_object_mode = False
            self._multi_key_fixed_mode = False
            self._multi_group_key_kinds.clear()
            for column in self._group_by_columns:
                key_vector = morsel.column(column)
                dict_accessor = _vector_dict_accessor(key_vector)
                if isinstance(key_vector, Int64Vector):
                    self._multi_group_key_kinds.push_back(KEY_MULTI_FIXED_INT)
                    continue
                if isinstance(key_vector, IntegerVector):
                    self._multi_group_key_kinds.push_back(KEY_MULTI_FIXED_INT)
                    continue
                if isinstance(key_vector, Date32Vector):
                    self._multi_group_key_kinds.push_back(KEY_MULTI_FIXED_DATE32)
                    continue
                if isinstance(key_vector, TimeVector):
                    key_kind = (
                        KEY_MULTI_FIXED_TIME64
                        if (<TimeVector> key_vector).is_time64 else
                        KEY_MULTI_FIXED_TIME32
                    )
                    self._multi_group_key_kinds.push_back(key_kind)
                    continue
                if isinstance(key_vector, TimestampVector):
                    self._multi_group_key_kinds.push_back(KEY_MULTI_FIXED_TIMESTAMP64)
                    continue
                if isinstance(key_vector, StringVector):
                    self._multi_group_key_kinds.push_back(KEY_MULTI_ENCODED_STRING)
                    self._use_object_keys = True
                    self._multi_key_object_mode = True
                    continue
                if dict_accessor != NULL:
                    key_kind = _dict_accessor_key_kind(dict_accessor)
                    if key_kind == 0:
                        self._init_legacy_backend()
                        return
                    self._multi_group_key_kinds.push_back(key_kind)
                    if key_kind == KEY_MULTI_ENCODED_STRING:
                        self._use_object_keys = True
                        self._multi_key_object_mode = True
                    continue
                self._init_legacy_backend()
                return

            if not self._multi_key_object_mode:
                self._multi_key_fixed_mode = True
                self._use_object_keys = False
                self._init_multi_fixed_key_columns(len(self._group_by_columns))
                self._init_multi_encoded_key_columns(len(self._group_by_columns))
                record_feature_groupby_engine_multi_key_fixed(self)
            else:
                self._init_multi_fixed_key_columns(len(self._group_by_columns))
                self._init_multi_encoded_key_columns(len(self._group_by_columns))
                record_feature_groupby_engine_multi_key_object(self)

            if len(self._aggregations) > 1:
                for agg_idx in range(len(self._aggregations)):
                    aggregation = self._aggregations[agg_idx]
                    fn = aggregation[1]
                    column = aggregation[2]

                    if fn == "count":
                        if column is None:
                            self._multi_agg_modes.push_back(AGG_COUNT_STAR)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(None)
                        else:
                            value_vector = morsel.column(column)
                            if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                                self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                                self._multi_value_kinds.push_back(VALUE_NONE)
                                self._multi_value_columns.append(column)
                            else:
                                self._init_legacy_backend()
                                return
                    elif fn == "sum":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_SUM)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_SUM)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            if dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                                self._multi_agg_modes.push_back(AGG_SUM)
                                self._multi_value_kinds.push_back(VALUE_DICT_FLOAT64)
                                self._multi_value_columns.append(column)
                            else:
                                self._multi_agg_modes.push_back(AGG_SUM)
                                self._multi_value_kinds.push_back(VALUE_DICT_INT64)
                                self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "min":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, StringVector):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_OBJECT)
                            self._multi_value_columns.append(column)
                            self._use_object_keys = True
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            if dict_accessor.value_type == DRAKEN_STRING:
                                self._multi_agg_modes.push_back(AGG_MIN)
                                self._multi_value_kinds.push_back(VALUE_OBJECT)
                                self._multi_value_columns.append(column)
                                self._use_object_keys = True
                            elif dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                                self._multi_agg_modes.push_back(AGG_MIN)
                                self._multi_value_kinds.push_back(VALUE_DICT_FLOAT64)
                                self._multi_value_columns.append(column)
                            else:
                                self._multi_agg_modes.push_back(AGG_MIN)
                                self._multi_value_kinds.push_back(VALUE_DICT_INT64)
                                self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "max":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, StringVector):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_OBJECT)
                            self._multi_value_columns.append(column)
                            self._use_object_keys = True
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            if dict_accessor.value_type == DRAKEN_STRING:
                                self._multi_agg_modes.push_back(AGG_MAX)
                                self._multi_value_kinds.push_back(VALUE_OBJECT)
                                self._multi_value_columns.append(column)
                                self._use_object_keys = True
                            elif dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                                self._multi_agg_modes.push_back(AGG_MAX)
                                self._multi_value_kinds.push_back(VALUE_DICT_FLOAT64)
                                self._multi_value_columns.append(column)
                            else:
                                self._multi_agg_modes.push_back(AGG_MAX)
                                self._multi_value_kinds.push_back(VALUE_DICT_INT64)
                                self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "avg" or fn == "mean":
                        value_vector = morsel.column(column)
                        if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                            self._multi_agg_modes.push_back(AGG_AVG)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "count_distinct" or fn == "distinct":
                        value_vector = morsel.column(column)
                        if self._supports_count_distinct_value(value_vector):
                            self._multi_agg_modes.push_back(AGG_COUNT_DISTINCT)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    else:
                        self._init_legacy_backend()
                        return

                self._multi_agg_count = len(self._aggregations)
                self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
                self._mode = MODE_CARCHAR
                record_feature_groupby_engine_carchar(self)
                return

            fn = self._aggregations[0][1]
            column = self._aggregations[0][2]
            if fn == "count":
                if column is None:
                    self._agg_mode = AGG_COUNT_STAR
                    self._value_kind = VALUE_NONE
                else:
                    value_vector = morsel.column(column)
                    if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                        self._agg_mode = AGG_COUNT_VALUE
                        self._value_kind = VALUE_NONE
                    else:
                        self._init_legacy_backend()
                        return
            elif fn == "sum":
                value_vector = morsel.column(column)
                dict_accessor = _vector_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_INT64
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    if dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                        self._agg_mode = AGG_SUM
                        self._value_kind = VALUE_DICT_FLOAT64
                    else:
                        self._agg_mode = AGG_SUM
                        self._value_kind = VALUE_DICT_INT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "min":
                value_vector = morsel.column(column)
                dict_accessor = _vector_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_OBJECT
                    self._use_object_keys = True
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    if dict_accessor.value_type == DRAKEN_STRING:
                        self._agg_mode = AGG_MIN
                        self._value_kind = VALUE_OBJECT
                        self._use_object_keys = True
                    elif dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                        self._agg_mode = AGG_MIN
                        self._value_kind = VALUE_DICT_FLOAT64
                    else:
                        self._agg_mode = AGG_MIN
                        self._value_kind = VALUE_DICT_INT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "max":
                value_vector = morsel.column(column)
                dict_accessor = _vector_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_OBJECT
                    self._use_object_keys = True
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    if dict_accessor.value_type == DRAKEN_STRING:
                        self._agg_mode = AGG_MAX
                        self._value_kind = VALUE_OBJECT
                        self._use_object_keys = True
                    elif dict_accessor.value_type == DRAKEN_FLOAT64 or dict_accessor.value_type == DRAKEN_FLOAT32:
                        self._agg_mode = AGG_MAX
                        self._value_kind = VALUE_DICT_FLOAT64
                    else:
                        self._agg_mode = AGG_MAX
                        self._value_kind = VALUE_DICT_INT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "avg" or fn == "mean":
                value_vector = morsel.column(column)
                if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                    self._agg_mode = AGG_AVG
                    self._value_kind = VALUE_FLOAT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "count_distinct" or fn == "distinct":
                value_vector = morsel.column(column)
                if self._supports_count_distinct_value(value_vector):
                    self._agg_mode = AGG_COUNT_DISTINCT
                    self._value_kind = VALUE_NONE
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "any_value":
                self._agg_mode = AGG_ANY_VALUE
                self._value_kind = VALUE_OBJECT
                self._use_object_keys = True
            else:
                self._init_legacy_backend()
                return

            self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
            self._mode = MODE_CARCHAR
            record_feature_groupby_engine_carchar(self)
            return

        key_vector = morsel.column(self._group_column)
        stringlike_key_vector = isinstance(key_vector, StringVector)
        dict_accessor = _vector_dict_accessor(key_vector)
        if dict_accessor != NULL:
            key_kind = _dict_accessor_key_kind(dict_accessor)
            if key_kind == 0:
                self._init_legacy_backend()
                return
            stringlike_key_vector = key_kind == KEY_MULTI_ENCODED_STRING

        if stringlike_key_vector:
            self._use_object_keys = True
            self._single_key_kind = KEY_MULTI_ENCODED_STRING

            if len(self._aggregations) > 1:
                self._multi_value_columns = []
                self._multi_distinct_sets = []
                self._multi_agg_modes.clear()
                self._multi_value_kinds.clear()

                for agg_idx in range(len(self._aggregations)):
                    aggregation = self._aggregations[agg_idx]
                    fn = aggregation[1]
                    column = aggregation[2]

                    if fn == "count":
                        if column is None:
                            self._multi_agg_modes.push_back(AGG_COUNT_STAR)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(None)
                        else:
                            value_vector = morsel.column(column)
                            dict_accessor = _vector_value_dict_accessor(value_vector)
                            if isinstance(
                                value_vector,
                                (Int64Vector, IntegerVector, Float64Vector, StringVector),
                            ):
                                self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                                self._multi_value_kinds.push_back(VALUE_NONE)
                                self._multi_value_columns.append(column)
                            elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                                self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                                self._multi_value_kinds.push_back(VALUE_NONE)
                                self._multi_value_columns.append(column)
                            else:
                                self._init_legacy_backend()
                                return
                    elif fn == "sum":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_value_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_SUM)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_SUM)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            self._multi_agg_modes.push_back(AGG_SUM)
                            self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                            if self._multi_value_kinds[self._multi_value_kinds.size() - 1] == VALUE_OBJECT:
                                self._init_legacy_backend()
                                return
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "min":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_value_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif self._is_stringlike_vector(value_vector):
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(VALUE_OBJECT)
                            self._multi_value_columns.append(column)
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            self._multi_agg_modes.push_back(AGG_MIN)
                            self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "max":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_value_dict_accessor(value_vector)
                        if isinstance(value_vector, Float64Vector):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_INT64)
                            self._multi_value_columns.append(column)
                        elif self._is_stringlike_vector(value_vector):
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(VALUE_OBJECT)
                            self._multi_value_columns.append(column)
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            self._multi_agg_modes.push_back(AGG_MAX)
                            self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "avg" or fn == "mean":
                        value_vector = morsel.column(column)
                        dict_accessor = _vector_value_dict_accessor(value_vector)
                        if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                            self._multi_agg_modes.push_back(AGG_AVG)
                            self._multi_value_kinds.push_back(VALUE_FLOAT64)
                            self._multi_value_columns.append(column)
                        elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                            self._multi_agg_modes.push_back(AGG_AVG)
                            self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                            if self._multi_value_kinds[self._multi_value_kinds.size() - 1] == VALUE_OBJECT:
                                self._init_legacy_backend()
                                return
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    elif fn == "count_distinct" or fn == "distinct":
                        value_vector = morsel.column(column)
                        if self._supports_count_distinct_value(value_vector):
                            self._multi_agg_modes.push_back(AGG_COUNT_DISTINCT)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                    else:
                        self._init_legacy_backend()
                        return

                self._multi_agg_count = len(self._aggregations)
                self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
                self._mode = MODE_CARCHAR
                record_feature_groupby_engine_carchar(self)
                return

            fn = self._aggregations[0][1]
            column = self._aggregations[0][2]
            if fn == "count":
                if column is None:
                    self._agg_mode = AGG_COUNT_STAR
                    self._value_kind = VALUE_NONE
                else:
                    value_vector = morsel.column(column)
                    if isinstance(
                        value_vector,
                        (Int64Vector, IntegerVector, Float64Vector, StringVector, TimestampVector),
                    ):
                        self._agg_mode = AGG_COUNT_VALUE
                        self._value_kind = VALUE_NONE
                    elif _vector_const_accessor(value_vector) != NULL:
                        # Non-null constant value: COUNT('a') == COUNT(*) for each group
                        if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                            self._init_legacy_backend()
                            return
                        self._agg_mode = AGG_COUNT_STAR
                        self._value_kind = VALUE_NONE
                    else:
                        self._init_legacy_backend()
                        return
            elif fn == "sum":
                value_vector = morsel.column(column)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_INT64
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    self._agg_mode = AGG_SUM
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                    if self._value_kind == VALUE_OBJECT:
                        self._init_legacy_backend()
                        return
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "min":
                value_vector = morsel.column(column)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_OBJECT
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    self._agg_mode = AGG_MIN
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                elif isinstance(value_vector, TimestampVector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_INT64
                elif _vector_const_accessor(value_vector) != NULL:
                    if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                        self._init_legacy_backend()
                        return
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_OBJECT
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "max":
                value_vector = morsel.column(column)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_OBJECT
                elif dict_accessor != NULL and dict_accessor.dict_values != NULL:
                    self._agg_mode = AGG_MAX
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                elif isinstance(value_vector, TimestampVector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_INT64
                elif _vector_const_accessor(value_vector) != NULL:
                    if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                        self._init_legacy_backend()
                        return
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_OBJECT
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "avg" or fn == "mean":
                value_vector = morsel.column(column)
                if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                    self._agg_mode = AGG_AVG
                    self._value_kind = VALUE_FLOAT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "count_distinct" or fn == "distinct":
                value_vector = morsel.column(column)
                if self._supports_count_distinct_value(value_vector):
                    self._agg_mode = AGG_COUNT_DISTINCT
                    self._value_kind = VALUE_NONE
                elif _vector_value_dict_accessor(value_vector) != NULL:
                    self._agg_mode = AGG_COUNT_DISTINCT
                    self._value_kind = VALUE_NONE
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "any_value":
                self._agg_mode = AGG_ANY_VALUE
                self._value_kind = VALUE_OBJECT
            else:
                self._init_legacy_backend()
                return

            self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
            self._mode = MODE_CARCHAR
            record_feature_groupby_engine_carchar(self)
            return
        elif _vector_dict_accessor(key_vector) != NULL and len(self._aggregations) > 1:
            self._single_key_kind = key_kind
            self._multi_value_columns = []
            self._multi_distinct_sets = []
            self._multi_agg_modes.clear()
            self._multi_value_kinds.clear()

            for agg_idx in range(len(self._aggregations)):
                aggregation = self._aggregations[agg_idx]
                fn = aggregation[1]
                column = aggregation[2]

                if fn == "count":
                    if column is None:
                        self._multi_agg_modes.push_back(AGG_COUNT_STAR)
                        self._multi_value_kinds.push_back(VALUE_NONE)
                        self._multi_value_columns.append(None)
                    else:
                        value_vector = morsel.column(column)
                        if isinstance(
                            value_vector,
                            (Int64Vector, IntegerVector, Float64Vector, StringVector),
                        ):
                            self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(column)
                        elif _vector_value_dict_accessor(value_vector) != NULL:
                            self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                elif fn == "sum":
                    value_vector = morsel.column(column)
                    dict_accessor = _vector_value_dict_accessor(value_vector)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_SUM)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_SUM)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    elif dict_accessor != NULL:
                        self._multi_agg_modes.push_back(AGG_SUM)
                        self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                        if self._multi_value_kinds[self._multi_value_kinds.size() - 1] == VALUE_OBJECT:
                            self._init_legacy_backend()
                            return
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "min":
                    value_vector = morsel.column(column)
                    dict_accessor = _vector_value_dict_accessor(value_vector)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, StringVector):
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(VALUE_OBJECT)
                        self._multi_value_columns.append(column)
                    elif dict_accessor != NULL:
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "max":
                    value_vector = morsel.column(column)
                    dict_accessor = _vector_value_dict_accessor(value_vector)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, StringVector):
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(VALUE_OBJECT)
                        self._multi_value_columns.append(column)
                    elif dict_accessor != NULL:
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "avg" or fn == "mean":
                    value_vector = morsel.column(column)
                    dict_accessor = _vector_value_dict_accessor(value_vector)
                    if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                        self._multi_agg_modes.push_back(AGG_AVG)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif dict_accessor != NULL:
                        self._multi_agg_modes.push_back(AGG_AVG)
                        self._multi_value_kinds.push_back(_dict_accessor_value_kind(dict_accessor))
                        if self._multi_value_kinds[self._multi_value_kinds.size() - 1] == VALUE_OBJECT:
                            self._init_legacy_backend()
                            return
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "count_distinct" or fn == "distinct":
                    value_vector = morsel.column(column)
                    if self._supports_count_distinct_value(value_vector):
                        self._multi_agg_modes.push_back(AGG_COUNT_DISTINCT)
                        self._multi_value_kinds.push_back(VALUE_NONE)
                        self._multi_value_columns.append(column)
                    elif _vector_value_dict_accessor(value_vector) != NULL:
                        self._multi_agg_modes.push_back(AGG_COUNT_DISTINCT)
                        self._multi_value_kinds.push_back(VALUE_NONE)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                else:
                    self._init_legacy_backend()
                    return

            self._multi_agg_count = len(self._aggregations)
            self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
            self._mode = MODE_CARCHAR
            record_feature_groupby_engine_carchar(self)
            return
        elif _vector_dict_accessor(key_vector) != NULL:
            self._single_key_kind = key_kind
            fn = self._aggregations[0][1]
            column = self._aggregations[0][2]
            if fn == "count":
                if column is None:
                    self._agg_mode = AGG_COUNT_STAR
                    self._value_kind = VALUE_NONE
                else:
                    value_vector = morsel.column(column)
                    if isinstance(
                        value_vector,
                        (Int64Vector, IntegerVector, Float64Vector, StringVector),
                    ):
                        self._agg_mode = AGG_COUNT_VALUE
                        self._value_kind = VALUE_NONE
                    elif _vector_value_dict_accessor(value_vector) != NULL:
                        self._agg_mode = AGG_COUNT_VALUE
                        self._value_kind = VALUE_NONE
                    else:
                        self._init_legacy_backend()
                        return
            elif fn == "sum":
                value_vector = morsel.column(column)
                dict_accessor = _vector_value_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_SUM
                    self._value_kind = VALUE_INT64
                elif dict_accessor != NULL:
                    self._agg_mode = AGG_SUM
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                    if self._value_kind == VALUE_OBJECT:
                        self._init_legacy_backend()
                        return
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "min":
                value_vector = morsel.column(column)
                dict_accessor = _vector_value_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MIN
                    self._value_kind = VALUE_OBJECT
                elif dict_accessor != NULL:
                    self._agg_mode = AGG_MIN
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "max":
                value_vector = morsel.column(column)
                dict_accessor = _vector_value_dict_accessor(value_vector)
                if isinstance(value_vector, Float64Vector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_FLOAT64
                elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_INT64
                elif isinstance(value_vector, StringVector):
                    self._agg_mode = AGG_MAX
                    self._value_kind = VALUE_OBJECT
                elif dict_accessor != NULL:
                    self._agg_mode = AGG_MAX
                    self._value_kind = _dict_accessor_value_kind(dict_accessor)
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "avg" or fn == "mean":
                value_vector = morsel.column(column)
                if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                    self._agg_mode = AGG_AVG
                    self._value_kind = VALUE_FLOAT64
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "count_distinct" or fn == "distinct":
                value_vector = morsel.column(column)
                if self._supports_count_distinct_value(value_vector):
                    self._agg_mode = AGG_COUNT_DISTINCT
                    self._value_kind = VALUE_NONE
                else:
                    self._init_legacy_backend()
                    return
            elif fn == "any_value":
                self._agg_mode = AGG_ANY_VALUE
                self._value_kind = VALUE_OBJECT
            else:
                self._init_legacy_backend()
                return

            self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
            self._mode = MODE_CARCHAR
            record_feature_groupby_engine_carchar(self)
            return

        self._multi_agg_count = 0
        self._use_object_keys = False
        self._multi_key_fixed_mode = False
        self._multi_group_key_values.clear()
        self._multi_group_key_valid.clear()
        self._encoded_key_bytes.clear()
        self._encoded_key_offsets.clear()
        self._encoded_key_offsets.push_back(0)
        self._encoded_key_valid.clear()
        self._multi_encoded_key_bytes.clear()
        self._multi_encoded_key_offsets.clear()
        self._multi_encoded_key_valid.clear()
        self._object_state_bytes.clear()
        self._object_state_starts.clear()
        self._object_state_lengths.clear()
        self._multi_object_state_bytes.clear()
        self._multi_object_state_starts.clear()
        self._multi_object_state_lengths.clear()
        self._key_payload_bytes.clear()
        self._key_payload_offsets.clear()
        self._key_payload_offsets.push_back(0)
        self._multi_group_key_kinds.clear()
        self._multi_value_columns = []
        self._multi_distinct_sets = []
        self._multi_agg_modes.clear()
        self._multi_value_kinds.clear()
        fn = self._aggregations[0][1]
        column = self._aggregations[0][2]
        key_vector = morsel.column(self._group_column)
        key_dict_accessor = _vector_dict_accessor(key_vector)
        key_const_accessor = _vector_const_accessor(key_vector)

        if key_const_accessor != NULL:
            if key_const_accessor.is_null != 0:
                self._constant_key_scalar = None
                self._constant_key_valid = 0
            else:
                self._constant_key_scalar = _const_accessor_scalar(key_const_accessor)
                self._constant_key_valid = 1

        if not isinstance(
            key_vector,
            (
                Int64Vector,
                IntegerVector,
                StringVector,
                Date32Vector,
                TimeVector,
                TimestampVector,
            ),
        ):
            if key_dict_accessor != NULL:
                pass
            elif hasattr(key_vector, "__getitem__"):
                self._use_object_keys = True
            else:
                self._init_legacy_backend()
                return

        if len(self._aggregations) > 1 or (
            key_dict_accessor != NULL and fn not in ("count_distinct", "distinct")
        ):
            if _is_constant_like_vector(key_vector):
                self._init_legacy_backend()
                return
            if key_dict_accessor != NULL:
                self._use_object_keys = True

            for agg_idx in range(len(self._aggregations)):
                aggregation = self._aggregations[agg_idx]
                fn = aggregation[1]
                column = aggregation[2]

                if fn == "count":
                    if column is None:
                        self._multi_agg_modes.push_back(AGG_COUNT_STAR)
                        self._multi_value_kinds.push_back(VALUE_NONE)
                        self._multi_value_columns.append(None)
                    else:
                        value_vector = morsel.column(column)
                        if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                            self._multi_agg_modes.push_back(AGG_COUNT_VALUE)
                            self._multi_value_kinds.push_back(VALUE_NONE)
                            self._multi_value_columns.append(column)
                        else:
                            self._init_legacy_backend()
                            return
                elif fn == "sum":
                    value_vector = morsel.column(column)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_SUM)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_SUM)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "min":
                    value_vector = morsel.column(column)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_MIN)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "max":
                    value_vector = morsel.column(column)
                    if isinstance(value_vector, Float64Vector):
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                        self._multi_agg_modes.push_back(AGG_MAX)
                        self._multi_value_kinds.push_back(VALUE_INT64)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "avg" or fn == "mean":
                    value_vector = morsel.column(column)
                    if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                        self._multi_agg_modes.push_back(AGG_AVG)
                        self._multi_value_kinds.push_back(VALUE_FLOAT64)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                elif fn == "count_distinct" or fn == "distinct":
                    value_vector = morsel.column(column)
                    if self._supports_count_distinct_value(value_vector):
                        self._multi_agg_modes.push_back(AGG_COUNT_DISTINCT)
                        self._multi_value_kinds.push_back(VALUE_NONE)
                        self._multi_value_columns.append(column)
                    else:
                        self._init_legacy_backend()
                        return
                else:
                    self._init_legacy_backend()
                    return

            self._multi_agg_count = len(self._aggregations)
            self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
            self._mode = MODE_CARCHAR
            record_feature_groupby_engine_carchar(self)
            return

        if fn == "count":
            if column is None:
                self._agg_mode = AGG_COUNT_STAR
                self._value_kind = VALUE_NONE
            else:
                value_vector = morsel.column(column)
                if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                    self._agg_mode = AGG_COUNT_VALUE
                    self._value_kind = VALUE_NONE
                else:
                    self._init_legacy_backend()
                    return
        elif fn == "sum":
            value_vector = morsel.column(column)
            if isinstance(value_vector, Float64Vector):
                self._agg_mode = AGG_SUM
                self._value_kind = VALUE_FLOAT64
            elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                self._agg_mode = AGG_SUM
                self._value_kind = VALUE_INT64
            else:
                self._init_legacy_backend()
                return
        elif fn == "min":
            value_vector = morsel.column(column)
            if isinstance(value_vector, Float64Vector):
                self._agg_mode = AGG_MIN
                self._value_kind = VALUE_FLOAT64
            elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                self._agg_mode = AGG_MIN
                self._value_kind = VALUE_INT64
            else:
                self._init_legacy_backend()
                return
        elif fn == "max":
            value_vector = morsel.column(column)
            if isinstance(value_vector, Float64Vector):
                self._agg_mode = AGG_MAX
                self._value_kind = VALUE_FLOAT64
            elif isinstance(value_vector, (Int64Vector, IntegerVector)):
                self._agg_mode = AGG_MAX
                self._value_kind = VALUE_INT64
            else:
                self._init_legacy_backend()
                return
        elif fn == "avg" or fn == "mean":
            value_vector = morsel.column(column)
            if isinstance(value_vector, (Int64Vector, IntegerVector, Float64Vector)):
                self._agg_mode = AGG_AVG
                self._value_kind = VALUE_FLOAT64
            else:
                self._init_legacy_backend()
                return
        elif fn == "count_distinct" or fn == "distinct":
            value_vector = morsel.column(column)
            if self._supports_count_distinct_value(value_vector):
                self._agg_mode = AGG_COUNT_DISTINCT
                self._value_kind = VALUE_NONE
            else:
                self._init_legacy_backend()
                return
        elif fn == "any_value":
            self._agg_mode = AGG_ANY_VALUE
            self._value_kind = VALUE_OBJECT
        else:
            self._init_legacy_backend()
            return

        if _is_constant_like_vector(key_vector):
            self._mode = MODE_CONSTANT
            record_feature_groupby_engine_constant(self)
            return

        self._single_key_kind = KEY_MULTI_FIXED_INT
        if isinstance(key_vector, Date32Vector):
            self._single_key_kind = KEY_MULTI_FIXED_DATE32
        elif isinstance(key_vector, TimeVector):
            self._single_key_kind = (
                KEY_MULTI_FIXED_TIME64 if (<TimeVector> key_vector).is_time64 else KEY_MULTI_FIXED_TIME32
            )
        elif isinstance(key_vector, TimestampVector):
            self._single_key_kind = KEY_MULTI_FIXED_TIMESTAMP64
        elif dict_accessor != NULL:
            key_kind = _dict_accessor_key_kind(dict_accessor)
            if key_kind == 0:
                self._init_legacy_backend()
                return
            if key_kind == KEY_MULTI_ENCODED_STRING:
                self._use_object_keys = True
            else:
                self._single_key_kind = key_kind
        elif isinstance(key_vector, StringVector) or self._use_object_keys:
            self._use_object_keys = True
        self._groupby_bloom = None
        self._use_bloom = False
        self._bloom_hashes.clear()
        self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), CARCHAR_INDEX_LOAD_FACTOR)
        self._mode = MODE_CARCHAR
        record_feature_groupby_engine_carchar(self)

    cdef void _reserve_for_rows(self, Py_ssize_t row_count):
        # Only pre-reserve the CarcharIndex hash table.  It uses next_power_of_two
        # internally, so repeated calls trigger at most O(log n) resizes total and
        # each resize rehashes into a table twice the previous size — O(n) amortised.
        #
        # The state vectors (_counts, _i64_state, etc.) are intentionally NOT reserved
        # here.  std::vector::reserve(n) allocates *exactly* n slots with no doubling.
        # Calling reserve(state_count + morsel_rows) on every morsel therefore triggers
        # a realloc+memcpy of the full vector content 325 times, producing O(n²/2)
        # total memcpy work (~540 GB for this workload).  Letting push_back drive
        # natural 2× doubling reduces that to O(n) amortised (~3 GB total).
        if self._mode != MODE_CARCHAR or row_count <= 0:
            return
        self._index.reserve(<size_t> self._state_count() + <size_t>row_count)

    cdef inline bint _bloom_might_contain(self, uint64_t h) noexcept:
        """Return True if row may be an existing group (must call lookup_fast).
        Return False only when bloom guarantees this hash has never been inserted."""
        if not self._use_bloom:
            return True
        return self._groupby_bloom._possibly_contains_fast(h)

    cdef inline void _bloom_record_new_state(self, uint64_t row_hash) noexcept:
        """Called whenever a new group state is inserted. Adds hash to bloom or staging vector."""
        if self._use_bloom:
            self._groupby_bloom._add(row_hash)
        else:
            self._bloom_hashes.push_back(row_hash)

    cdef void _maybe_init_bloom(self) except *:
        """Create and populate bloom filter from first-morsel staged hashes."""
        cdef Py_ssize_t state_count = self._state_count()
        cdef size_t estimated_total
        cdef size_t i
        if self._use_bloom or state_count == 0:
            return
        # Size filter for estimated total cardinality; cap at 200M to avoid OOM.
        estimated_total = min(<size_t>state_count * 200, <size_t>200_000_000)
        self._groupby_bloom = BloomFilter(<uint32_t>estimated_total)
        for i in range(self._bloom_hashes.size()):
            self._groupby_bloom._add(self._bloom_hashes[i])
        self._bloom_hashes.clear()
        self._use_bloom = True

    cdef inline int64_t _find_or_insert_state(
        self, uint64_t row_hash, int64_t key_value, int64_t key_valid_flag
    ) except *:
        cdef int64_t payload_ref = -1
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx

        if self._bloom_might_contain(row_hash) and self._index.lookup_fast(row_hash, payload_ref):
            return payload_ref

        payload_ref = <int64_t> self._state_count()
        self._index.insert_new(row_hash, payload_ref)
        self._group_key_values.push_back(key_value)
        self._group_key_valid.push_back(key_valid_flag)
        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)

        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")

        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef inline int64_t _find_or_insert_multi_fixed_state_from_vectors(
        self, uint64_t row_hash, list key_vectors, Py_ssize_t row_idx
    ) except *:
        cdef int64_t payload_ref = -1
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx

        if self._bloom_might_contain(row_hash) and self._index.lookup_fast(row_hash, payload_ref):
            return payload_ref

        payload_ref = <int64_t> self._state_count()
        self._index.insert_new(row_hash, payload_ref)
        self._append_multi_fixed_payload_key_from_vectors(key_vectors, row_idx)

        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)

        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")

        self._bloom_record_new_state(row_hash)
        return payload_ref



    cdef inline int64_t _find_or_insert_encoded_state(
        self,
        uint64_t row_hash,
        const char* data_ptr,
        Py_ssize_t data_len,
        int64_t key_valid_flag,
    ) except *:
        cdef int64_t payload_ref = -1
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx

        if self._bloom_might_contain(row_hash) and self._index.lookup_fast(row_hash, payload_ref):
            return payload_ref

        payload_ref = <int64_t> self._state_count()
        self._index.insert_new(row_hash, payload_ref)
        self._append_single_payload_key(data_ptr, data_len, key_valid_flag)
        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)

        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")

        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef inline int64_t _find_or_insert_multi_encoded_state(
        self, uint64_t row_hash, list key_vectors, Py_ssize_t row_idx
    ) except *:
        cdef int64_t payload_ref = -1
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx

        if self._bloom_might_contain(row_hash) and self._index.lookup_fast(row_hash, payload_ref):
            return payload_ref

        payload_ref = <int64_t> self._state_count()
        self._index.insert_new(row_hash, payload_ref)
        self._append_multi_payload_key(key_vectors, row_idx)
        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)

        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)

        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")

        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef inline int64_t _insert_fixed_state_known_miss(
        self, uint64_t row_hash, int64_t key_value, int64_t key_valid_flag
    ) except *:
        """Insert a new fixed-key state; caller has already verified a miss via lookup_fast.

        Precondition: lookup_fast(row_hash, ...) already returned False.
        Skips the redundant second probe that _find_or_insert_state would perform.
        """
        cdef int64_t payload_ref = <int64_t> self._state_count()
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx
        self._index.insert_new(row_hash, payload_ref)
        self._group_key_values.push_back(key_value)
        self._group_key_valid.push_back(key_valid_flag)
        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)
        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")
        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef inline int64_t _insert_encoded_state_known_miss(
        self,
        uint64_t row_hash,
        const char* data_ptr,
        Py_ssize_t data_len,
        int64_t key_valid_flag,
    ) except *:
        """Insert a new encoded-key state; caller has already verified a miss via lookup_fast.

        Precondition: lookup_fast(row_hash, ...) already returned False.
        Skips the redundant second probe that _find_or_insert_encoded_state would perform.
        """
        cdef int64_t payload_ref = <int64_t> self._state_count()
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx
        self._index.insert_new(row_hash, payload_ref)
        self._append_single_payload_key(data_ptr, data_len, key_valid_flag)
        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)
        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")
        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef inline int64_t _insert_multi_encoded_state_known_miss(
        self, uint64_t row_hash, list key_vectors, Py_ssize_t row_idx
    ) except *:
        """Insert a new multi-encoded-key state; caller has already verified a miss via lookup_fast.

        Precondition: lookup_fast(row_hash, ...) already returned False.
        Skips the redundant second probe that _find_or_insert_multi_encoded_state would perform.
        """
        cdef int64_t payload_ref = <int64_t> self._state_count()
        cdef size_t key_store_bytes
        cdef Py_ssize_t agg_idx
        self._index.insert_new(row_hash, payload_ref)
        self._append_multi_payload_key(key_vectors, row_idx)
        key_store_bytes = <size_t> self._key_payload_bytes.size()
        record_groupby_key_store_bytes(self, key_store_bytes)
        self._object_state.append(None)
        self._object_state_starts.push_back(0)
        self._object_state_lengths.push_back(0)
        if self._agg_mode == AGG_COUNT_DISTINCT:
            self._distinct_sets.append(CarcharSetWrapper())
        if self._multi_agg_count > 0:
            for agg_idx in range(self._multi_agg_count):
                self._multi_counts.push_back(0)
                self._multi_i64_state.push_back(0)
                self._multi_f64_state.push_back(0.0)
                self._multi_seen.push_back(0)
                self._multi_avg_sums.push_back(0.0)
                self._multi_avg_counts.push_back(0)
                self._multi_object_state.append(None)
                self._multi_object_state_starts.push_back(0)
                self._multi_object_state_lengths.push_back(0)
                if self._multi_agg_modes[agg_idx] == AGG_COUNT_DISTINCT:
                    self._multi_distinct_sets.append(CarcharSetWrapper())
                else:
                    self._multi_distinct_sets.append(None)
        else:
            self._counts.push_back(0)
            self._i64_state.push_back(0)
            self._f64_state.push_back(0.0)
            self._seen.push_back(0)
            self._avg_sums.push_back(0.0)
            self._avg_counts.push_back(0)
        if (
            self._key_store_limit_bytes is not None
            and key_store_bytes > <size_t> self._key_store_limit_bytes
        ):
            raise MemoryError("group key store exceeded configured limit")
        self._bloom_record_new_state(row_hash)
        return payload_ref

    cdef list _build_multi_fixed_key_vectors(self, Py_ssize_t start, Py_ssize_t stop):
        if stop < start:
            raise RuntimeError(f"invalid multi-fixed finalize range: start={start}, stop={stop}")
        if <Py_ssize_t> self._key_payload_offsets.size() >= stop + 1:
            return build_payload_multi_key_vectors(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._multi_group_key_kinds,
                start,
                stop,
            )
        cdef Py_ssize_t key_count = len(self._group_by_columns)
        cdef Py_ssize_t key_idx
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t length = stop - start
        cdef object key_vec
        cdef Int64Vector key_vec_i64
        cdef Date32Vector key_vec_d32
        cdef TimeVector key_vec_t32
        cdef TimeVector key_vec_t64
        cdef TimestampVector key_vec_ts
        cdef int64_t* key_data_i64
        cdef int32_t* key_data_i32
        cdef uint8_t* key_nulls
        cdef list vectors = []
        cdef bint needs_key_nulls
        cdef int64_t key_kind

        if key_count == 0:
            raise RuntimeError("multi-fixed finalize requested with empty group-by schema")
        if <Py_ssize_t> self._multi_group_key_values.size() != key_count:
            raise RuntimeError(
                f"multi-fixed finalize value store/key schema mismatch: "
                f"{self._multi_group_key_values.size()} value columns for {key_count} keys"
            )
        if <Py_ssize_t> self._multi_group_key_valid.size() != key_count:
            raise RuntimeError(
                f"multi-fixed finalize validity store/key schema mismatch: "
                f"{self._multi_group_key_valid.size()} validity columns for {key_count} keys"
            )
        if <Py_ssize_t> self._multi_group_key_kinds.size() != key_count:
            raise RuntimeError(
                f"multi-fixed finalize key-kind/schema mismatch: "
                f"{self._multi_group_key_kinds.size()} key kinds for {key_count} keys"
            )

        for key_idx in range(key_count):
            if <Py_ssize_t> self._multi_group_key_values[key_idx].size() < stop:
                raise RuntimeError(
                    f"multi-fixed finalize value store shorter than finalize range for key {key_idx}: "
                    f"have {self._multi_group_key_values[key_idx].size()} rows, need {stop}"
                )
            if <Py_ssize_t> self._multi_group_key_valid[key_idx].size() < stop:
                raise RuntimeError(
                    f"multi-fixed finalize validity store shorter than finalize range for key {key_idx}: "
                    f"have {self._multi_group_key_valid[key_idx].size()} rows, need {stop}"
                )

            needs_key_nulls = False
            for row_idx in range(start, stop):
                if self._multi_group_key_valid[key_idx][row_idx] == 0:
                    needs_key_nulls = True
                    break
            key_kind = self._multi_group_key_kinds[key_idx]
            if not self._is_multi_fixed_kind(key_kind):
                raise RuntimeError(
                    f"multi-fixed finalize encountered non-fixed key kind {key_kind} at key {key_idx}"
                )
            if key_kind == KEY_MULTI_FIXED_DATE32:
                key_vec_d32 = Date32Vector(length)
                key_vec = key_vec_d32
                key_data_i32 = <int32_t*> key_vec_d32.ptr.data
            elif key_kind == KEY_MULTI_FIXED_TIME32:
                key_vec_t32 = TimeVector(length, is_time64=False)
                key_vec = key_vec_t32
                key_data_i32 = <int32_t*> key_vec_t32.ptr.data
            elif key_kind == KEY_MULTI_FIXED_TIME64:
                key_vec_t64 = TimeVector(length, is_time64=True)
                key_vec = key_vec_t64
                key_data_i64 = <int64_t*> key_vec_t64.ptr.data
            elif key_kind == KEY_MULTI_FIXED_TIMESTAMP64:
                key_vec_ts = TimestampVector(length)
                key_vec = key_vec_ts
                key_data_i64 = <int64_t*> key_vec_ts.ptr.data
            else:
                key_vec_i64 = Int64Vector(length)
                key_vec = key_vec_i64
                key_data_i64 = <int64_t*> key_vec_i64.ptr.data
            key_nulls = NULL
            if needs_key_nulls:
                key_nulls = _alloc_valid_bitmap(length)
                if key_nulls == NULL:
                    raise MemoryError("failed to allocate multi-fixed finalize null bitmap")
                if key_kind == KEY_MULTI_FIXED_DATE32:
                    key_vec_d32.ptr.null_bitmap = key_nulls
                elif key_kind == KEY_MULTI_FIXED_TIME32:
                    key_vec_t32.ptr.null_bitmap = key_nulls
                elif key_kind == KEY_MULTI_FIXED_TIME64:
                    key_vec_t64.ptr.null_bitmap = key_nulls
                elif key_kind == KEY_MULTI_FIXED_TIMESTAMP64:
                    key_vec_ts.ptr.null_bitmap = key_nulls
                else:
                    key_vec_i64.ptr.null_bitmap = key_nulls
            for row_idx in range(length):
                if key_kind == KEY_MULTI_FIXED_DATE32 or key_kind == KEY_MULTI_FIXED_TIME32:
                    key_data_i32[row_idx] = <int32_t> self._multi_group_key_values[key_idx][start + row_idx]
                else:
                    key_data_i64[row_idx] = self._multi_group_key_values[key_idx][start + row_idx]
                if key_nulls != NULL and self._multi_group_key_valid[key_idx][start + row_idx] != 0:
                    _bitmap_set_valid(key_nulls, row_idx)
            if key_vec is None:
                raise RuntimeError(f"multi-fixed finalize produced None vector for key {key_idx}")
            vectors.append(key_vec)

        if len(vectors) != key_count:
            raise RuntimeError(
                f"multi-fixed finalize produced {len(vectors)} vectors for {key_count} keys"
            )
        return vectors

    cdef void _ingest_count_distinct_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
    ) except *:
        # Type-specific divergence: build a uniform uint64_t* hash buffer for the
        # kernel.  Int64 values are reinterpret-cast in place; sub-64-bit integers
        # are expanded into a temporary heap buffer; all other types (strings,
        # dict-encoded, etc.) go through morsel.hash().
        cdef object value_vector = morsel.column(self._value_column)
        cdef uint8_t* value_nulls = _vector_null_bitmap(value_vector)
        cdef DrakenFixedBuffer* value_ptr
        cdef uint64_t* temp_hashes = NULL
        cdef uint64_t[::1] hash_view
        cdef Py_ssize_t row_idx

        if isinstance(value_vector, Int64Vector):
            value_ptr = (<Int64Vector> value_vector).ptr
            count_distinct_accumulate(
                self._distinct_sets, self._counts.data(),
                <uint64_t*> value_ptr.data,
                value_nulls,
                state_indices, row_count,
            )
            return

        if isinstance(value_vector, IntegerVector):
            value_ptr = (<IntegerVector> value_vector).ptr
            temp_hashes = <uint64_t*> malloc(row_count * sizeof(uint64_t))
            if temp_hashes == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    temp_hashes[row_idx] = <uint64_t> _read_integer_value(value_ptr, row_idx)
                count_distinct_accumulate(
                    self._distinct_sets, self._counts.data(),
                    temp_hashes, value_nulls,
                    state_indices, row_count,
                )
            finally:
                free(temp_hashes)
            return

        if row_count == 0:
            return

        hash_view = morsel.hash([self._value_column])
        count_distinct_accumulate(
            self._distinct_sets, self._counts.data(),
            <uint64_t*> &hash_view[0],
            value_nulls,
            state_indices, row_count,
        )

    cdef void _ingest_count_distinct_multi_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
        Py_ssize_t agg_idx,
    ) except *:
        # Same type dispatch as the single-agg path, but targets
        # _multi_distinct_sets / _multi_counts and carries multi_agg_count +
        # agg_idx through to the kernel for the offset formula.
        cdef object value_vector = morsel.column(self._multi_value_columns[agg_idx])
        cdef uint8_t* value_nulls = _vector_null_bitmap(value_vector)
        cdef DrakenFixedBuffer* value_ptr
        cdef uint64_t* temp_hashes = NULL
        cdef uint64_t[::1] hash_view
        cdef Py_ssize_t row_idx

        if isinstance(value_vector, Int64Vector):
            value_ptr = (<Int64Vector> value_vector).ptr
            count_distinct_multi_accumulate(
                self._multi_distinct_sets, self._multi_counts.data(),
                <uint64_t*> value_ptr.data,
                value_nulls,
                state_indices, row_count, self._multi_agg_count, agg_idx,
            )
            return

        if isinstance(value_vector, IntegerVector):
            value_ptr = (<IntegerVector> value_vector).ptr
            temp_hashes = <uint64_t*> malloc(row_count * sizeof(uint64_t))
            if temp_hashes == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    temp_hashes[row_idx] = <uint64_t> _read_integer_value(value_ptr, row_idx)
                count_distinct_multi_accumulate(
                    self._multi_distinct_sets, self._multi_counts.data(),
                    temp_hashes, value_nulls,
                    state_indices, row_count, self._multi_agg_count, agg_idx,
                )
            finally:
                free(temp_hashes)
            return

        if row_count == 0:
            return

        hash_view = morsel.hash([self._multi_value_columns[agg_idx]])
        count_distinct_multi_accumulate(
            self._multi_distinct_sets, self._multi_counts.data(),
            <uint64_t*> &hash_view[0],
            value_nulls,
            state_indices, row_count, self._multi_agg_count, agg_idx,
        )

    cdef void _ingest_any_value_var_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
    ) except *:
        """
        ANY_VALUE accumulation for variable-width (string/object) values.
        Keeps the first non-null value seen per group state.
        """
        cdef object value_vector = morsel.column(self._value_column)
        cdef uint8_t* value_nulls = self._value_null_bitmap(value_vector)
        cdef Py_ssize_t row_idx
        cdef int64_t state_index
        cdef object value_obj
        cdef const char* data_ptr = NULL
        cdef Py_ssize_t data_len = 0
        cdef int64_t valid_flag
        cdef bytes const_bytes_obj
        cdef const char** values_data = NULL
        cdef Py_ssize_t* values_lens = NULL
        cdef Py_ssize_t max_bytes = 0
        cdef size_t cursor_start
        cdef size_t arena_cursor

        if _vector_const_accessor(value_vector) != NULL:
            if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                return
            value_obj = _const_accessor_scalar(_vector_const_accessor(value_vector))
            if value_obj is None:
                return
            if isinstance(value_obj, bytes):
                const_bytes_obj = <bytes> value_obj
            elif isinstance(value_obj, str):
                const_bytes_obj = (<str> value_obj).encode('utf-8')
            else:
                const_bytes_obj = str(value_obj).encode('utf-8')
            data_ptr = <const char*> const_bytes_obj
            data_len = len(const_bytes_obj)
            for row_idx in range(row_count):
                state_index = state_indices[row_idx]
                if self._seen[state_index] == 0:
                    self._store_object_state_bytes(state_index, data_ptr, data_len)
                    self._seen[state_index] = 1
            return

        if self._is_stringlike_vector(value_vector):
            values_data = <const char**> malloc(row_count * sizeof(void*))
            values_lens = <Py_ssize_t*> malloc(row_count * sizeof(Py_ssize_t))
            if values_data == NULL or values_lens == NULL:
                free(values_data)
                free(values_lens)
                raise MemoryError()
            max_bytes = 0
            try:
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        values_data[row_idx] = NULL
                        values_lens[row_idx] = 0
                        continue
                    valid_flag = self._extract_stringlike_key(
                        value_vector, row_idx, &values_data[row_idx], &values_lens[row_idx]
                    )
                    if valid_flag == 0:
                        values_data[row_idx] = NULL
                        values_lens[row_idx] = 0
                    else:
                        max_bytes += values_lens[row_idx]
                cursor_start = self._object_state_bytes.size()
                self._object_state_bytes.resize(cursor_start + max_bytes)
                arena_cursor = cursor_start
                any_value_var_accumulate(
                    self._object_state_bytes.data(),
                    self._object_state_starts.data(),
                    self._object_state_lengths.data(),
                    &arena_cursor,
                    self._seen.data(),
                    state_indices,
                    values_data,
                    values_lens,
                    value_nulls,
                    row_count,
                )
                self._object_state_bytes.resize(arena_cursor)
            finally:
                free(values_data)
                free(values_lens)
            return

        for row_idx in range(row_count):
            if not _bitmap_is_valid(value_nulls, row_idx):
                continue
            state_index = state_indices[row_idx]
            value_obj = value_vector[row_idx]
            if value_obj is None:
                continue
            if self._seen[state_index] == 0:
                self._object_state[state_index] = value_obj
                self._seen[state_index] = 1

    cdef void _ingest_any_value_var_multi_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
        Py_ssize_t agg_idx,
    ) except *:
        """
        ANY_VALUE accumulation for variable-width (string/object) values — multi-agg path.
        """
        cdef object value_vector = morsel.column(self._multi_value_columns[agg_idx])
        cdef uint8_t* value_nulls = self._value_null_bitmap(value_vector)
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t offset
        cdef object value_obj
        cdef const char* data_ptr = NULL
        cdef Py_ssize_t data_len = 0
        cdef int64_t valid_flag
        cdef bytes const_bytes_obj
        cdef const char** mv_values_data = NULL
        cdef Py_ssize_t* mv_values_lens = NULL
        cdef Py_ssize_t mv_max_bytes = 0
        cdef size_t mv_cursor_start
        cdef size_t mv_arena_cursor

        if _vector_const_accessor(value_vector) != NULL:
            if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                return
            value_obj = _const_accessor_scalar(_vector_const_accessor(value_vector))
            if value_obj is None:
                return
            if isinstance(value_obj, bytes):
                const_bytes_obj = <bytes> value_obj
            elif isinstance(value_obj, str):
                const_bytes_obj = (<str> value_obj).encode('utf-8')
            else:
                const_bytes_obj = str(value_obj).encode('utf-8')
            data_ptr = <const char*> const_bytes_obj
            data_len = len(const_bytes_obj)
            for row_idx in range(row_count):
                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                if self._multi_seen[offset] == 0:
                    self._store_multi_object_state_bytes(offset, data_ptr, data_len)
                    self._multi_seen[offset] = 1
            return

        if self._is_stringlike_vector(value_vector):
            mv_values_data = <const char**> malloc(row_count * sizeof(void*))
            mv_values_lens = <Py_ssize_t*> malloc(row_count * sizeof(Py_ssize_t))
            if mv_values_data == NULL or mv_values_lens == NULL:
                free(mv_values_data)
                free(mv_values_lens)
                raise MemoryError()
            mv_max_bytes = 0
            try:
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        mv_values_data[row_idx] = NULL
                        mv_values_lens[row_idx] = 0
                        continue
                    valid_flag = self._extract_stringlike_key(
                        value_vector, row_idx, &mv_values_data[row_idx], &mv_values_lens[row_idx]
                    )
                    if valid_flag == 0:
                        mv_values_data[row_idx] = NULL
                        mv_values_lens[row_idx] = 0
                    else:
                        mv_max_bytes += mv_values_lens[row_idx]
                mv_cursor_start = self._multi_object_state_bytes.size()
                self._multi_object_state_bytes.resize(mv_cursor_start + mv_max_bytes)
                mv_arena_cursor = mv_cursor_start
                any_value_var_multi_accumulate(
                    self._multi_object_state_bytes.data(),
                    self._multi_object_state_starts.data(),
                    self._multi_object_state_lengths.data(),
                    &mv_arena_cursor,
                    self._multi_seen.data(),
                    state_indices,
                    mv_values_data,
                    mv_values_lens,
                    value_nulls,
                    row_count,
                    self._multi_agg_count,
                    agg_idx,
                )
                self._multi_object_state_bytes.resize(mv_arena_cursor)
            finally:
                free(mv_values_data)
                free(mv_values_lens)
            return

        for row_idx in range(row_count):
            if not _bitmap_is_valid(value_nulls, row_idx):
                continue
            offset = self._multi_offset(state_indices[row_idx], agg_idx)
            value_obj = value_vector[row_idx]
            if value_obj is None:
                continue
            if self._multi_seen[offset] == 0:
                self._multi_object_state[offset] = value_obj
                self._multi_seen[offset] = 1

    cdef void _ingest_object_minmax_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
    ) except *:
        cdef object value_vector = morsel.column(self._value_column)
        cdef uint8_t* value_nulls = self._value_null_bitmap(value_vector)
        cdef Py_ssize_t row_idx
        cdef int64_t state_index
        cdef object value_obj
        cdef const char* data_ptr = NULL
        cdef Py_ssize_t data_len = 0
        cdef int64_t valid_flag
        cdef bytes const_bytes_obj
        cdef const char** values_data = NULL
        cdef Py_ssize_t* values_lens = NULL
        cdef Py_ssize_t max_bytes = 0
        cdef size_t cursor_start
        cdef size_t arena_cursor

        if _vector_const_accessor(value_vector) != NULL:
            # All rows have the same constant value; initialise unseen states only.
            if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                return  # null constant: no states updated
            value_obj = _const_accessor_scalar(_vector_const_accessor(value_vector))
            if value_obj is None:
                return
            if isinstance(value_obj, bytes):
                const_bytes_obj = <bytes> value_obj
            elif isinstance(value_obj, str):
                const_bytes_obj = (<str> value_obj).encode('utf-8')
            else:
                const_bytes_obj = str(value_obj).encode('utf-8')
            data_ptr = <const char*> const_bytes_obj
            data_len = len(const_bytes_obj)
            for row_idx in range(row_count):
                state_index = state_indices[row_idx]
                if self._seen[state_index] == 0:
                    self._store_object_state_bytes(state_index, data_ptr, data_len)
                    self._seen[state_index] = 1
            return

        if self._is_stringlike_vector(value_vector):
            values_data = <const char**> malloc(row_count * sizeof(void*))
            values_lens = <Py_ssize_t*> malloc(row_count * sizeof(Py_ssize_t))
            if values_data == NULL or values_lens == NULL:
                free(values_data)
                free(values_lens)
                raise MemoryError()
            max_bytes = 0
            try:
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        values_data[row_idx] = NULL
                        values_lens[row_idx] = 0
                        continue
                    valid_flag = self._extract_stringlike_key(
                        value_vector, row_idx, &values_data[row_idx], &values_lens[row_idx]
                    )
                    if valid_flag == 0:
                        values_data[row_idx] = NULL
                        values_lens[row_idx] = 0
                    else:
                        max_bytes += values_lens[row_idx]
                # Pre-allocate worst-case space in the arena, then let the
                # kernel advance the cursor to the actual bytes written.
                cursor_start = self._object_state_bytes.size()
                self._object_state_bytes.resize(cursor_start + max_bytes)
                arena_cursor = cursor_start
                minmax_var_accumulate(
                    self._object_state_bytes.data(),
                    self._object_state_starts.data(),
                    self._object_state_lengths.data(),
                    &arena_cursor,
                    self._seen.data(),
                    state_indices,
                    values_data,
                    values_lens,
                    value_nulls,
                    row_count,
                    self._agg_mode == AGG_MIN,
                )
                # Trim arena to bytes actually written.
                self._object_state_bytes.resize(arena_cursor)
            finally:
                free(values_data)
                free(values_lens)
            return

        for row_idx in range(row_count):
            if not _bitmap_is_valid(value_nulls, row_idx):
                continue
            state_index = state_indices[row_idx]
            value_obj = value_vector[row_idx]
            if value_obj is None:
                continue
            if self._agg_mode == AGG_MIN:
                if self._seen[state_index] == 0 or value_obj < self._object_state[state_index]:
                    self._object_state[state_index] = value_obj
                self._seen[state_index] = 1
            elif self._agg_mode == AGG_MAX:
                if self._seen[state_index] == 0 or value_obj > self._object_state[state_index]:
                    self._object_state[state_index] = value_obj
                self._seen[state_index] = 1

    cdef void _ingest_object_minmax_multi_for_states(
        self,
        Morsel morsel,
        int64_t* state_indices,
        Py_ssize_t row_count,
        Py_ssize_t agg_idx,
    ) except *:
        cdef object value_vector = morsel.column(self._multi_value_columns[agg_idx])
        cdef uint8_t* value_nulls = self._value_null_bitmap(value_vector)
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t offset
        cdef object value_obj
        cdef int64_t agg_mode = self._multi_agg_modes[agg_idx]
        cdef const char* data_ptr = NULL
        cdef Py_ssize_t data_len = 0
        cdef int64_t valid_flag
        cdef bytes const_bytes_obj
        cdef const char** mv_values_data = NULL
        cdef Py_ssize_t* mv_values_lens = NULL
        cdef Py_ssize_t mv_max_bytes = 0
        cdef size_t mv_cursor_start
        cdef size_t mv_arena_cursor

        if _vector_const_accessor(value_vector) != NULL:
            if _const_accessor_is_null(_vector_const_accessor(value_vector)):
                return
            value_obj = _const_accessor_scalar(_vector_const_accessor(value_vector))
            if value_obj is None:
                return
            if isinstance(value_obj, bytes):
                const_bytes_obj = <bytes> value_obj
            elif isinstance(value_obj, str):
                const_bytes_obj = (<str> value_obj).encode('utf-8')
            else:
                const_bytes_obj = str(value_obj).encode('utf-8')
            data_ptr = <const char*> const_bytes_obj
            data_len = len(const_bytes_obj)
            for row_idx in range(row_count):
                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                if self._multi_seen[offset] == 0:
                    self._store_multi_object_state_bytes(offset, data_ptr, data_len)
                    self._multi_seen[offset] = 1
            return

        if self._is_stringlike_vector(value_vector):
            mv_values_data = <const char**> malloc(row_count * sizeof(void*))
            mv_values_lens = <Py_ssize_t*> malloc(row_count * sizeof(Py_ssize_t))
            if mv_values_data == NULL or mv_values_lens == NULL:
                free(mv_values_data)
                free(mv_values_lens)
                raise MemoryError()
            mv_max_bytes = 0
            try:
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        mv_values_data[row_idx] = NULL
                        mv_values_lens[row_idx] = 0
                        continue
                    valid_flag = self._extract_stringlike_key(
                        value_vector, row_idx, &mv_values_data[row_idx], &mv_values_lens[row_idx]
                    )
                    if valid_flag == 0:
                        mv_values_data[row_idx] = NULL
                        mv_values_lens[row_idx] = 0
                    else:
                        mv_max_bytes += mv_values_lens[row_idx]
                mv_cursor_start = self._multi_object_state_bytes.size()
                self._multi_object_state_bytes.resize(mv_cursor_start + mv_max_bytes)
                mv_arena_cursor = mv_cursor_start
                minmax_var_multi_accumulate(
                    self._multi_object_state_bytes.data(),
                    self._multi_object_state_starts.data(),
                    self._multi_object_state_lengths.data(),
                    &mv_arena_cursor,
                    self._multi_seen.data(),
                    state_indices,
                    mv_values_data,
                    mv_values_lens,
                    value_nulls,
                    row_count,
                    self._multi_agg_count,
                    agg_idx,
                    agg_mode == AGG_MIN,
                )
                self._multi_object_state_bytes.resize(mv_arena_cursor)
            finally:
                free(mv_values_data)
                free(mv_values_lens)
            return

        for row_idx in range(row_count):
            if not _bitmap_is_valid(value_nulls, row_idx):
                continue
            offset = self._multi_offset(state_indices[row_idx], agg_idx)
            value_obj = value_vector[row_idx]
            if value_obj is None:
                continue
            if agg_mode == AGG_MIN:
                if self._multi_seen[offset] == 0 or value_obj < self._multi_object_state[offset]:
                    self._multi_object_state[offset] = value_obj
                self._multi_seen[offset] = 1
            elif agg_mode == AGG_MAX:
                if self._multi_seen[offset] == 0 or value_obj > self._multi_object_state[offset]:
                    self._multi_object_state[offset] = value_obj
                self._multi_seen[offset] = 1

    cdef void _ingest_fixed_width_key(self, Morsel morsel, DrakenFixedBuffer* key_ptr):
        if key_ptr == NULL:
            raise ValueError("key_ptr is NULL in _ingest_fixed_width_key")
        cdef uint8_t* key_nulls = <uint8_t*> key_ptr.null_bitmap
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t state_index
        cdef int64_t key_value
        cdef bint key_valid
        cdef int64_t key_valid_flag
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef int64_t* state_indices = NULL
        cdef Py_ssize_t local_hits = 0, local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0, local_bloom_skips = 0, local_bloom_fps = 0
        cdef long long t_hash_start, t_state_assign_start, t_accum_start
        cdef uint64_t[::1] row_hashes

        t_hash_start = time.monotonic_ns()
        row_hashes = morsel.hash([self._group_column])
        record_groupby_hash_time(self, t_hash_start)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            # --- Phase 1: assign state indices with bloom pre-filter and telemetry ---
            t_state_assign_start = time.monotonic_ns()
            for row_idx in range(row_count):
                state_index = -1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                key_valid = _bitmap_is_valid(key_nulls, row_idx)
                key_valid_flag = 1 if key_valid else 0
                key_value = _read_integer_value(key_ptr, row_idx) if key_valid else 0
                state_indices[row_idx] = self._insert_fixed_state_known_miss(row_hashes[row_idx], key_value, key_valid_flag)
            record_ingest_state_assign_time(self, t_state_assign_start)
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            # --- Phase 2: accumulate ---
            t_accum_start = time.monotonic_ns()
            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            value_vector = morsel.column(self._value_column)
            if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_SUM:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, TimestampVector):
                    value_ptr = (<TimestampVector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_AVG:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_ANY_VALUE:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                elif isinstance(value_vector, TimestampVector):
                    value_ptr = (<TimestampVector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    any_value_fixed_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            # Fall-through: AGG_COUNT_VALUE (state_indices precomputed; no per-row key extraction needed)
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, TimestampVector):
                value_ptr = (<TimestampVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            else:
                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            record_groupby_accumulate_time(self, t_accum_start)
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_int64_key_with_const_accessor(self, Morsel morsel, ConstAccessor* key_const_accessor, Py_ssize_t row_count):
        cdef int64_t const_key_value
        cdef int64_t state_index
        cdef Py_ssize_t row_idx
        cdef uint64_t[::1] row_hashes
        cdef int64_t* state_indices = NULL
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls

        if _const_accessor_is_null(key_const_accessor):
            return

        const_key_value = (<int64_t*>key_const_accessor.value_ptr)[0]
        row_hashes = morsel.hash([self._group_column])
        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_indices[row_idx] = state_index
                else:
                    state_indices[row_idx] = self._insert_fixed_state_known_miss(
                        row_hashes[row_idx], const_key_value, 1
                    )

            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                return

            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                return

            value_vector = morsel.column(self._value_column)

            if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
                return

            if self._agg_mode == AGG_SUM:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
                return

            if self._agg_mode == AGG_AVG:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
                return

            if self._agg_mode == AGG_ANY_VALUE:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    any_value_fixed_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                return

            # Fall-through: AGG_COUNT_VALUE
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            else:
                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_int64_key(self, Morsel morsel, Int64Vector key_vector):
        cdef DrakenFixedBuffer* key_ptr
        cdef int64_t* key_data
        cdef uint8_t* key_nulls
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t state_index
        cdef int64_t key_value
        cdef bint key_valid
        cdef int64_t key_valid_flag
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef int64_t* state_indices = NULL
        cdef Py_ssize_t local_hits = 0, local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0, local_bloom_skips = 0, local_bloom_fps = 0
        cdef long long t_hash_start, t_state_assign_start, t_accum_start
        cdef uint64_t[::1] row_hashes
        cdef ConstAccessor* key_const_accessor = NULL

        key_const_accessor = _vector_const_accessor(key_vector)
        if key_const_accessor != NULL:
            self._ingest_int64_key_with_const_accessor(morsel, key_const_accessor, row_count)
            return

        key_ptr = key_vector.ptr
        if key_ptr == NULL:
            raise ValueError("key_vector.ptr is NULL in _ingest_int64_key")
        key_data = <int64_t*> key_ptr.data
        key_nulls = <uint8_t*> key_ptr.null_bitmap

        t_hash_start = time.monotonic_ns()
        row_hashes = morsel.hash([self._group_column])
        record_groupby_hash_time(self, t_hash_start)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            t_state_assign_start = time.monotonic_ns()
            for row_idx in range(row_count):
                state_index = -1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                key_valid = _bitmap_is_valid(key_nulls, row_idx)
                key_valid_flag = 1 if key_valid else 0
                key_value = key_data[row_idx] if key_valid else 0
                state_indices[row_idx] = self._insert_fixed_state_known_miss(row_hashes[row_idx], key_value, key_valid_flag)
            record_ingest_state_assign_time(self, t_state_assign_start)
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            t_accum_start = time.monotonic_ns()
            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            value_vector = morsel.column(self._value_column)
            if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_SUM:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, TimestampVector):
                    value_ptr = (<TimestampVector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_AVG:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_ANY_VALUE:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                elif isinstance(value_vector, TimestampVector):
                    value_ptr = (<TimestampVector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    any_value_fixed_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            # Fall-through: AGG_COUNT_VALUE
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, TimestampVector):
                value_ptr = (<TimestampVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            else:
                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            record_groupby_accumulate_time(self, t_accum_start)
        finally:
            if state_indices != NULL:
                free(state_indices)


    cdef void _ingest_integer_key(self, Morsel morsel, IntegerVector key_vector):
        cdef DrakenFixedBuffer* key_ptr
        cdef uint8_t* key_nulls
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t state_index
        cdef int64_t key_value
        cdef bint key_valid
        cdef int64_t key_valid_flag
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef int64_t* state_indices = NULL
        cdef Py_ssize_t local_hits = 0, local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0, local_bloom_skips = 0, local_bloom_fps = 0
        cdef long long t_hash_start, t_state_assign_start, t_accum_start
        cdef uint64_t[::1] row_hashes
        cdef ConstAccessor* key_const_accessor = NULL
        cdef int64_t const_key_val = 0
        cdef int64_t const_key_valid_flag = 0

        key_const_accessor = _vector_const_accessor(key_vector)
        if key_const_accessor == NULL:
            key_ptr = key_vector.ptr
            if key_ptr == NULL:
                raise ValueError("key_vector.ptr is NULL in _ingest_integer_key")
            key_nulls = <uint8_t*> key_ptr.null_bitmap
        else:
            key_ptr = NULL
            key_nulls = NULL
            if not _const_accessor_is_null(key_const_accessor):
                const_key_val = <int64_t> _const_accessor_scalar(key_const_accessor)
                const_key_valid_flag = 1

        t_hash_start = time.monotonic_ns()
        row_hashes = morsel.hash([self._group_column])
        record_groupby_hash_time(self, t_hash_start)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            # --- Phase 1: assign state indices with bloom pre-filter and telemetry ---
            t_state_assign_start = time.monotonic_ns()
            for row_idx in range(row_count):
                state_index = -1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                if key_const_accessor != NULL:
                    key_valid_flag = const_key_valid_flag
                    key_value = const_key_val
                else:
                    key_valid = _bitmap_is_valid(key_nulls, row_idx)
                    key_valid_flag = 1 if key_valid else 0
                    key_value = _read_integer_value(key_ptr, row_idx) if key_valid else 0
                state_indices[row_idx] = self._insert_fixed_state_known_miss(row_hashes[row_idx], key_value, key_valid_flag)
            record_ingest_state_assign_time(self, t_state_assign_start)
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            # --- Phase 2: accumulate ---
            t_accum_start = time.monotonic_ns()
            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            value_vector = morsel.column(self._value_column)
            if self._agg_mode == AGG_SUM:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_AVG:
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            # Fall-through: AGG_COUNT_VALUE (state_indices precomputed; no per-row key extraction needed)
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            elif isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            else:
                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_indices[row_idx]] = self._counts[state_indices[row_idx]] + 1
            record_groupby_accumulate_time(self, t_accum_start)
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_int64_key_multi(self, Morsel morsel, Int64Vector key_vector) except *:
        cdef DrakenFixedBuffer* key_ptr
        cdef int64_t* key_data
        cdef uint8_t* key_nulls
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef int64_t key_value
        cdef int64_t key_valid_flag
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t offset
        cdef int64_t agg_mode
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef Py_ssize_t local_hits = 0, local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0, local_bloom_skips = 0, local_bloom_fps = 0
        cdef uint64_t[::1] row_hashes
        cdef ConstAccessor* key_const_accessor = NULL
        cdef int64_t const_key_val = 0
        cdef int64_t const_key_valid_flag = 0
        cdef ConstAccessor* value_const_accessor = NULL
        cdef DictAccessor* value_dict_accessor = NULL
        cdef double const_f64_val = 0.0
        cdef int64_t const_i64_val = 0

        row_hashes = morsel.hash([self._group_column])
        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        key_const_accessor = _vector_const_accessor(key_vector)
        if key_const_accessor == NULL:
            key_ptr = key_vector.ptr
            if key_ptr == NULL:
                raise ValueError("key_ptr is NULL in _ingest_int64_key_multi")
            key_data = <int64_t*> key_ptr.data
            key_nulls = <uint8_t*> key_ptr.null_bitmap
        else:
            key_ptr = NULL
            key_data = NULL
            key_nulls = NULL
            if not _const_accessor_is_null(key_const_accessor):
                const_key_valid_flag = 1
                const_key_val = (<int64_t*>key_const_accessor.value_ptr)[0]

        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                if key_const_accessor != NULL:
                    key_valid_flag = const_key_valid_flag
                    key_value = const_key_val
                else:
                    key_valid_flag = 1 if _bitmap_is_valid(key_nulls, row_idx) else 0
                    key_value = key_data[row_idx] if key_valid_flag != 0 else 0
                state_indices[row_idx] = self._insert_fixed_state_known_miss(
                    row_hashes[row_idx], key_value, key_valid_flag
                )
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            for agg_idx in range(self._multi_agg_count):
                agg_mode = self._multi_agg_modes[agg_idx]
                if agg_mode == AGG_COUNT_STAR:
                    count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, self._multi_agg_count, agg_idx)
                    continue
                if agg_mode == AGG_COUNT_DISTINCT:
                    self._ingest_count_distinct_multi_for_states(morsel, state_indices, row_count, agg_idx)
                    continue

                if agg_mode == AGG_SUM:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, add to all states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_f64_state[offset] = self._multi_f64_state[offset] + const_f64_val
                                self._multi_seen[offset] = 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_i64_state[offset] = self._multi_i64_state[offset] + const_i64_val
                                self._multi_seen[offset] = 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        sum_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        sum_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                sum_f64_multi_accumulate_from_dict(
                                    self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                            else:
                                sum_i64_multi_accumulate_from_dict(
                                    self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            sum_integer_multi_accumulate(
                                self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx,
                            )
                    continue

                if agg_mode in (AGG_MIN, AGG_MAX):
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, initialize or update states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_f64_state[offset] = const_f64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_f64_val < self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                                elif agg_mode == AGG_MAX and const_f64_val > self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_i64_state[offset] = const_i64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_i64_val < self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                                elif agg_mode == AGG_MAX and const_i64_val > self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        minmax_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        minmax_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                minmax_f64_multi_accumulate_from_dict(
                                    self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                                )
                            else:
                                minmax_i64_multi_accumulate_from_dict(
                                    self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            minmax_integer_multi_accumulate(
                                self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                            )
                    continue

                if agg_mode == AGG_AVG:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, accumulate sum and count
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + const_f64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + <double>const_i64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        avg_f64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        avg_i64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                avg_f64_multi_accumulate_from_dict(
                                    self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                            else:
                                avg_i64_multi_accumulate_from_dict(
                                    self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            avg_integer_multi_accumulate(
                                self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx,
                            )
                    continue

                value_vector = morsel.column(self._multi_value_columns[agg_idx])
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if offset >= <Py_ssize_t> self._multi_f64_state.size() or offset < 0:
                            raise RuntimeError(f"[int64_multi float] offset out of bounds: {offset}, size={self._multi_f64_state.size()}")
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                if isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if offset >= <Py_ssize_t> self._multi_i64_state.size() or offset < 0:
                            raise RuntimeError(f"[int64_multi int] offset out of bounds: {offset}, size={self._multi_i64_state.size()}")
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_dict_accessor = _vector_value_dict_accessor(value_vector)
                if value_dict_accessor != NULL:
                    value_nulls = value_dict_accessor.row_nulls
                    if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                        for row_idx in range(row_count):
                            if not _bitmap_is_valid(value_nulls, row_idx):
                                continue
                            offset = self._multi_offset(state_indices[row_idx], agg_idx)
                            if agg_mode == AGG_COUNT_VALUE:
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                        continue
                    elif self._multi_value_kinds[agg_idx] == VALUE_DICT_INT64:
                        for row_idx in range(row_count):
                            if not _bitmap_is_valid(value_nulls, row_idx):
                                continue
                            offset = self._multi_offset(state_indices[row_idx], agg_idx)
                            if agg_mode == AGG_COUNT_VALUE:
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                        continue

                # Check for constant vector accessor for the fallback COUNT_VALUE path
                value_const_accessor = _vector_const_accessor(value_vector)
                if value_const_accessor != NULL:
                    # For constant vectors, if not null, increment count for all rows
                    if not _const_accessor_is_null(value_const_accessor):
                        if agg_mode == AGG_COUNT_VALUE:
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        continue
                    offset = self._multi_offset(state_indices[row_idx], agg_idx)
                    if offset >= <Py_ssize_t> self._multi_i64_state.size() or offset < 0:
                        raise RuntimeError(f"[int64_multi integer] offset out of bounds: {offset}, size={self._multi_i64_state.size()}")
                    if agg_mode == AGG_COUNT_VALUE:
                        self._multi_counts[offset] = self._multi_counts[offset] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_integer_key_multi(self, Morsel morsel, IntegerVector key_vector) except *:
        cdef DrakenFixedBuffer* key_ptr
        cdef uint8_t* key_nulls
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef int64_t key_value
        cdef int64_t key_valid_flag
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t offset
        cdef int64_t agg_mode
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef Py_ssize_t local_hits = 0, local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0, local_bloom_skips = 0, local_bloom_fps = 0
        cdef uint64_t[::1] row_hashes
        cdef ConstAccessor* key_const_accessor = NULL
        cdef int64_t const_key_val = 0
        cdef int64_t const_key_valid_flag = 0
        cdef ConstAccessor* value_const_accessor = NULL
        cdef DictAccessor* value_dict_accessor = NULL
        cdef double const_f64_val = 0.0
        cdef int64_t const_i64_val = 0

        row_hashes = morsel.hash([self._group_column])
        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        key_const_accessor = _vector_const_accessor(key_vector)
        if key_const_accessor == NULL:
            key_ptr = key_vector.ptr
            if key_ptr == NULL:
                raise ValueError("key_vector.ptr is NULL in _ingest_integer_key_multi")
            key_nulls = <uint8_t*> key_ptr.null_bitmap
        else:
            key_ptr = NULL
            key_nulls = NULL
            if not _const_accessor_is_null(key_const_accessor):
                const_key_val = <int64_t> _const_accessor_scalar(key_const_accessor)
                const_key_valid_flag = 1
        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                if key_const_accessor != NULL:
                    key_valid_flag = const_key_valid_flag
                    key_value = const_key_val
                else:
                    key_valid_flag = 1 if _bitmap_is_valid(key_nulls, row_idx) else 0
                    key_value = _read_integer_value(key_ptr, row_idx) if key_valid_flag != 0 else 0
                state_indices[row_idx] = self._insert_fixed_state_known_miss(
                    row_hashes[row_idx], key_value, key_valid_flag
                )
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            for agg_idx in range(self._multi_agg_count):
                agg_mode = self._multi_agg_modes[agg_idx]
                if agg_mode == AGG_COUNT_STAR:
                    count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, self._multi_agg_count, agg_idx)
                    continue
                if agg_mode == AGG_COUNT_DISTINCT:
                    self._ingest_count_distinct_multi_for_states(morsel, state_indices, row_count, agg_idx)
                    continue

                if agg_mode == AGG_SUM:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, add to all states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_f64_state[offset] = self._multi_f64_state[offset] + const_f64_val
                                self._multi_seen[offset] = 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_i64_state[offset] = self._multi_i64_state[offset] + const_i64_val
                                self._multi_seen[offset] = 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        sum_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        sum_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        sum_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                if agg_mode in (AGG_MIN, AGG_MAX):
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, initialize or update states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_f64_state[offset] = const_f64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_f64_val < self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                                elif agg_mode == AGG_MAX and const_f64_val > self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_i64_state[offset] = const_i64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_i64_val < self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                                elif agg_mode == AGG_MAX and const_i64_val > self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        minmax_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        minmax_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        minmax_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    continue

                if agg_mode == AGG_AVG:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, accumulate sum and count
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + const_f64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + <double>const_i64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        avg_f64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        avg_i64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        avg_integer_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                value_vector = morsel.column(self._multi_value_columns[agg_idx])
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                if isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        continue
                    offset = self._multi_offset(state_indices[row_idx], agg_idx)
                    if agg_mode == AGG_COUNT_VALUE:
                        self._multi_counts[offset] = self._multi_counts[offset] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_multi_fixed_key_multi(self, Morsel morsel) except *:
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t offset
        cdef int64_t agg_mode
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef list key_vectors = []
        cdef bytes group_name
        cdef object group_vector
        cdef uint64_t[::1] row_hashes

        row_hashes = morsel.hash(self._group_by_columns)
        for group_name in self._group_by_columns:
            group_vector = morsel.column(group_name)
            key_vectors.append(group_vector)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_indices[row_idx] = state_index
                    continue
                state_indices[row_idx] = self._find_or_insert_multi_fixed_state_from_vectors(
                    row_hashes[row_idx],
                    key_vectors,
                    row_idx,
                )

            for agg_idx in range(self._multi_agg_count):
                agg_mode = self._multi_agg_modes[agg_idx]
                if agg_mode == AGG_COUNT_STAR:
                    count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, self._multi_agg_count, agg_idx)
                    continue
                if agg_mode == AGG_COUNT_DISTINCT:
                    self._ingest_count_distinct_multi_for_states(morsel, state_indices, row_count, agg_idx)
                    continue

                if agg_mode == AGG_SUM:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, add to all states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_f64_state[offset] = self._multi_f64_state[offset] + const_f64_val
                                self._multi_seen[offset] = 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_i64_state[offset] = self._multi_i64_state[offset] + const_i64_val
                                self._multi_seen[offset] = 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        sum_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        sum_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        sum_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                if agg_mode in (AGG_MIN, AGG_MAX):
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, initialize or update states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_f64_state[offset] = const_f64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_f64_val < self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                                elif agg_mode == AGG_MAX and const_f64_val > self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_i64_state[offset] = const_i64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_i64_val < self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                                elif agg_mode == AGG_MAX and const_i64_val > self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        minmax_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        minmax_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        minmax_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    continue

                if agg_mode == AGG_AVG:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, accumulate sum and count
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + const_f64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + <double>const_i64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        avg_f64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        avg_i64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        avg_integer_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                value_vector = morsel.column(self._multi_value_columns[agg_idx])
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                if isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        continue
                    offset = self._multi_offset(state_indices[row_idx], agg_idx)
                    if agg_mode == AGG_COUNT_VALUE:
                        self._multi_counts[offset] = self._multi_counts[offset] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_multi_fixed_key(self, Morsel morsel) except *:
        cdef uint64_t[::1] row_hashes = morsel.hash(self._group_by_columns)
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t state_index
        cdef list key_vectors = []
        cdef bytes group_name
        cdef object group_vector
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef int64_t* state_indices = NULL

        for group_name in self._group_by_columns:
            group_vector = morsel.column(group_name)
            key_vectors.append(group_vector)

        if self._agg_mode == AGG_COUNT_STAR:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                    state_indices[row_idx] = state_index
                count_star_accumulate(self._counts.data(), state_indices, row_count)
            finally:
                free(state_indices)
            return

        if self._agg_mode == AGG_COUNT_DISTINCT:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_indices[row_idx] = state_index
                    else:
                        state_indices[row_idx] = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
            finally:
                if state_indices != NULL:
                    free(state_indices)
            return

        if self._agg_mode == AGG_SUM:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                    state_indices[row_idx] = state_index
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
            finally:
                free(state_indices)
            return

        value_vector = morsel.column(self._value_column)
        if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_indices[row_idx] = state_index
                    else:
                        state_indices[row_idx] = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
            finally:
                if state_indices != NULL:
                    free(state_indices)
            return

        if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_indices[row_idx] = state_index
                    else:
                        state_indices[row_idx] = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
            finally:
                if state_indices != NULL:
                    free(state_indices)
            return

        if self._agg_mode in (AGG_MIN, AGG_MAX):
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                    state_indices[row_idx] = state_index
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
            finally:
                free(state_indices)
            return

        if self._agg_mode == AGG_AVG:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                    state_indices[row_idx] = state_index
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
            finally:
                free(state_indices)
            return

        if self._agg_mode == AGG_ANY_VALUE:
            state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
            if state_indices == NULL and row_count > 0:
                raise MemoryError()
            try:
                for row_idx in range(row_count):
                    state_index = -1
                    if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                        state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                            row_hashes[row_idx], key_vectors, row_idx
                        )
                    state_indices[row_idx] = state_index
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    any_value_fixed_integer_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
            finally:
                free(state_indices)
            return

        if isinstance(value_vector, Float64Vector):
            value_ptr = (<Float64Vector> value_vector).ptr
            value_nulls = <uint8_t*> value_ptr.null_bitmap
            for row_idx in range(row_count):
                state_index = -1
                if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                        row_hashes[row_idx], key_vectors, row_idx
                    )
                if self._agg_mode == AGG_COUNT_VALUE:
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_index] = self._counts[state_index] + 1
            return

        if isinstance(value_vector, Int64Vector):
            value_ptr = (<Int64Vector> value_vector).ptr
            value_nulls = <uint8_t*> value_ptr.null_bitmap
            for row_idx in range(row_count):
                state_index = -1
                if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                        row_hashes[row_idx], key_vectors, row_idx
                    )
                if self._agg_mode == AGG_COUNT_VALUE:
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_index] = self._counts[state_index] + 1
            return

        value_ptr = (<IntegerVector> value_vector).ptr
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            state_index = -1
            if not self._index.lookup_fast(row_hashes[row_idx], state_index):
                state_index = self._find_or_insert_multi_fixed_state_from_vectors(
                    row_hashes[row_idx], key_vectors, row_idx
                )
            if self._agg_mode == AGG_COUNT_VALUE:
                if _bitmap_is_valid(value_nulls, row_idx):
                    self._counts[state_index] = self._counts[state_index] + 1

    cdef void _ingest_dictionary_key_multi(self, Morsel morsel, object key_vector) except *:
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t offset
        cdef int64_t agg_mode
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef DictAccessor* value_dict_accessor = NULL
        cdef const char* key_data_ptr = NULL
        cdef Py_ssize_t key_data_len = 0
        cdef int64_t key_valid_flag
        cdef int64_t key_value
        cdef Py_ssize_t local_bloom_checks = 0
        cdef Py_ssize_t local_bloom_skips = 0
        cdef Py_ssize_t local_bloom_fps = 0
        cdef uint64_t[::1] row_hashes
        cdef int64_t key_kind

        row_hashes = morsel.hash(self._group_by_columns)
        key_kind = _dict_accessor_key_kind(_vector_dict_accessor(key_vector))

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._use_bloom:
                    local_bloom_checks += 1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                if self._is_multi_fixed_kind(key_kind):
                    key_value = self._read_dictionary_fixed_key(key_vector, row_idx, &key_valid_flag)
                    state_indices[row_idx] = self._insert_fixed_state_known_miss(
                        row_hashes[row_idx],
                        key_value,
                        key_valid_flag,
                    )
                else:
                    key_valid_flag = self._extract_stringlike_key(
                        key_vector, row_idx, &key_data_ptr, &key_data_len
                    )
                    state_indices[row_idx] = self._insert_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_data_ptr,
                        key_data_len,
                        key_valid_flag,
                    )
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            for agg_idx in range(self._multi_agg_count):
                agg_mode = self._multi_agg_modes[agg_idx]
                if agg_mode == AGG_COUNT_STAR:
                    count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, self._multi_agg_count, agg_idx)
                    continue
                if agg_mode == AGG_COUNT_DISTINCT:
                    self._ingest_count_distinct_multi_for_states(morsel, state_indices, row_count, agg_idx)
                    continue

                if agg_mode == AGG_SUM:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, add to all states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_f64_state[offset] = self._multi_f64_state[offset] + const_f64_val
                                self._multi_seen[offset] = 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_i64_state[offset] = self._multi_i64_state[offset] + const_i64_val
                                self._multi_seen[offset] = 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        sum_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        sum_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                sum_f64_multi_accumulate_from_dict(
                                    self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                            else:
                                sum_i64_multi_accumulate_from_dict(
                                    self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            sum_integer_multi_accumulate(
                                self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx,
                            )
                    continue

                if agg_mode in (AGG_MIN, AGG_MAX):
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, initialize or update states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_f64_state[offset] = const_f64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_f64_val < self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                                elif agg_mode == AGG_MAX and const_f64_val > self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_i64_state[offset] = const_i64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_i64_val < self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                                elif agg_mode == AGG_MAX and const_i64_val > self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        minmax_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        minmax_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                minmax_f64_multi_accumulate_from_dict(
                                    self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                                )
                            else:
                                minmax_i64_multi_accumulate_from_dict(
                                    self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            minmax_integer_multi_accumulate(
                                self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                            )
                    continue

                if agg_mode == AGG_AVG:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, accumulate sum and count
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + const_f64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + <double>const_i64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        avg_f64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        avg_i64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_dict_accessor = _vector_value_dict_accessor(value_vector)
                        if value_dict_accessor != NULL:
                            if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                                avg_f64_multi_accumulate_from_dict(
                                    self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                            else:
                                avg_i64_multi_accumulate_from_dict(
                                    self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                    value_dict_accessor, row_count, self._multi_agg_count, agg_idx,
                                )
                        else:
                            value_ptr = (<IntegerVector> value_vector).ptr
                            avg_integer_multi_accumulate(
                                self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                                value_ptr, row_count, self._multi_agg_count, agg_idx,
                            )
                    continue

                value_vector = morsel.column(self._multi_value_columns[agg_idx])
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                if isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_dict_accessor = _vector_value_dict_accessor(value_vector)
                if value_dict_accessor != NULL:
                    value_nulls = value_dict_accessor.row_nulls
                    if self._multi_value_kinds[agg_idx] == VALUE_DICT_FLOAT64:
                        for row_idx in range(row_count):
                            if not _bitmap_is_valid(value_nulls, row_idx):
                                continue
                            offset = self._multi_offset(state_indices[row_idx], agg_idx)
                            if agg_mode == AGG_COUNT_VALUE:
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                        continue
                    elif self._multi_value_kinds[agg_idx] == VALUE_DICT_INT64:
                        for row_idx in range(row_count):
                            if not _bitmap_is_valid(value_nulls, row_idx):
                                continue
                            offset = self._multi_offset(state_indices[row_idx], agg_idx)
                            if agg_mode == AGG_COUNT_VALUE:
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                        continue

                # Check for constant vector accessor for the fallback COUNT_VALUE path
                value_const_accessor = _vector_const_accessor(value_vector)
                if value_const_accessor != NULL:
                    # For constant vectors, if not null, increment count for all rows
                    if not _const_accessor_is_null(value_const_accessor):
                        if agg_mode == AGG_COUNT_VALUE:
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        continue
                    offset = self._multi_offset(state_indices[row_idx], agg_idx)
                    if agg_mode == AGG_COUNT_VALUE:
                        self._multi_counts[offset] = self._multi_counts[offset] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_dictionary_key(self, Morsel morsel, object key_vector) except *:
        cdef uint64_t[::1] row_hashes = morsel.hash([self._group_column])
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef const char* key_data_ptr = NULL
        cdef Py_ssize_t key_data_len = 0
        cdef int64_t key_valid_flag
        cdef int64_t key_value
        cdef int64_t key_kind = _dict_accessor_key_kind(_vector_dict_accessor(key_vector))
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef DictAccessor* value_dict_accessor = NULL
        cdef Py_ssize_t local_bloom_checks = 0
        cdef Py_ssize_t local_bloom_skips = 0
        cdef Py_ssize_t local_bloom_fps = 0

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            for row_idx in range(row_count):
                state_index = -1
                if self._use_bloom:
                    local_bloom_checks += 1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                if self._is_multi_fixed_kind(key_kind):
                    key_value = self._read_dictionary_fixed_key(key_vector, row_idx, &key_valid_flag)
                    state_indices[row_idx] = self._insert_fixed_state_known_miss(
                        row_hashes[row_idx],
                        key_value,
                        key_valid_flag,
                    )
                else:
                    key_valid_flag = self._extract_stringlike_key(
                        key_vector, row_idx, &key_data_ptr, &key_data_len
                    )
                    state_indices[row_idx] = self._insert_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_data_ptr,
                        key_data_len,
                        key_valid_flag,
                    )
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)

            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                return
            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                return

            if self._agg_mode == AGG_SUM:
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_dict_accessor = _vector_value_dict_accessor(value_vector)
                    if value_dict_accessor != NULL:
                        if self._value_kind == VALUE_DICT_FLOAT64:
                            sum_f64_accumulate_from_dict(
                                self._f64_state.data(), self._seen.data(), state_indices,
                                value_dict_accessor, row_count,
                            )
                        else:
                            sum_i64_accumulate_from_dict(
                                self._i64_state.data(), self._seen.data(), state_indices,
                                value_dict_accessor, row_count,
                            )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        sum_integer_accumulate(
                            self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                        )
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_dict_accessor = _vector_value_dict_accessor(value_vector)
                    if value_dict_accessor != NULL:
                        if self._value_kind == VALUE_DICT_FLOAT64:
                            minmax_f64_accumulate_from_dict(
                                self._f64_state.data(), self._seen.data(), state_indices,
                                value_dict_accessor, row_count, self._agg_mode == AGG_MIN,
                            )
                        else:
                            minmax_i64_accumulate_from_dict(
                                self._i64_state.data(), self._seen.data(), state_indices,
                                value_dict_accessor, row_count, self._agg_mode == AGG_MIN,
                            )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        minmax_integer_accumulate(
                            self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                        )
                return

            if self._agg_mode == AGG_AVG:
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_dict_accessor = _vector_value_dict_accessor(value_vector)
                    if value_dict_accessor != NULL:
                        if self._value_kind == VALUE_DICT_FLOAT64:
                            avg_f64_accumulate_from_dict(
                                self._avg_sums.data(), self._avg_counts.data(), state_indices,
                                value_dict_accessor, row_count,
                            )
                        else:
                            avg_i64_accumulate_from_dict(
                                self._avg_sums.data(), self._avg_counts.data(), state_indices,
                                value_dict_accessor, row_count,
                            )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        avg_integer_accumulate(
                            self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                        )
                return

            if self._agg_mode == AGG_ANY_VALUE:
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    any_value_fixed_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
                else:
                    value_dict_accessor = _vector_value_dict_accessor(value_vector)
                    if value_dict_accessor != NULL:
                        return
                    value_ptr = (<IntegerVector> value_vector).ptr
                    any_value_fixed_integer_accumulate(
                        self._i64_state.data(),
                        self._seen.data(),
                        state_indices,
                        value_ptr,
                        row_count,
                    )
                return

            value_vector = morsel.column(self._value_column)
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    state_index = state_indices[row_idx]
                    if self._agg_mode == AGG_COUNT_VALUE:
                        if _bitmap_is_valid(value_nulls, row_idx):
                            self._counts[state_index] = self._counts[state_index] + 1
                return

            if isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    state_index = state_indices[row_idx]
                    if self._agg_mode == AGG_COUNT_VALUE:
                        if _bitmap_is_valid(value_nulls, row_idx):
                            self._counts[state_index] = self._counts[state_index] + 1
                return

            value_dict_accessor = _vector_value_dict_accessor(value_vector)
            if value_dict_accessor != NULL:
                # MIN/MAX handled by dispatch above; COUNT_VALUE/AVG on dict values handled above
                return

            value_ptr = (<IntegerVector> value_vector).ptr
            value_nulls = <uint8_t*> value_ptr.null_bitmap
            for row_idx in range(row_count):
                state_index = state_indices[row_idx]
                if self._agg_mode == AGG_COUNT_VALUE:
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_index] = self._counts[state_index] + 1
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_object_key(self, Morsel morsel, object key_vector) except *:
        cdef uint64_t[::1] row_hashes
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef list key_vectors = []
        cdef bytes group_name
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef const char* key_data_ptr = NULL
        cdef Py_ssize_t key_data_len = 0
        cdef int64_t key_valid_flag
        cdef Py_ssize_t local_hits = 0
        cdef Py_ssize_t local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0
        cdef Py_ssize_t local_bloom_skips = 0
        cdef Py_ssize_t local_bloom_fps = 0
        cdef long long t_state_assign_start
        cdef long long t_hash_start
        cdef long long t_accum_start

        t_hash_start = time.monotonic_ns()
        row_hashes = morsel.hash(self._group_by_columns)
        record_groupby_hash_time(self, t_hash_start)

        if self._multi_key_object_mode:
            for group_name in self._group_by_columns:
                key_vectors.append(morsel.column(group_name))
        else:
            key_vectors.append(key_vector)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            t_state_assign_start = time.monotonic_ns()
            for row_idx in range(row_count):
                state_index = -1
                if self._use_bloom:
                    local_bloom_checks += 1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                if self._multi_key_object_mode:
                    state_indices[row_idx] = self._insert_multi_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_vectors,
                        row_idx,
                    )
                else:
                    key_valid_flag = self._extract_stringlike_key(
                        key_vectors[0], row_idx, &key_data_ptr, &key_data_len
                    )
                    state_indices[row_idx] = self._insert_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_data_ptr,
                        key_data_len,
                        key_valid_flag,
                    )
            record_ingest_state_assign_time(self, t_state_assign_start)
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)
            t_accum_start = time.monotonic_ns()

            if self._agg_mode == AGG_COUNT_STAR:
                count_star_accumulate(self._counts.data(), state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return
            if self._agg_mode == AGG_COUNT_DISTINCT:
                self._ingest_count_distinct_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_SUM:
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    sum_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    sum_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    sum_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode == AGG_ANY_VALUE:
                self._ingest_any_value_var_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX):
                self._ingest_object_minmax_for_states(morsel, state_indices, row_count)
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode in (AGG_MIN, AGG_MAX):
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    minmax_f64_accumulate(
                        self._f64_state.data(), self._seen.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                elif isinstance(value_vector, TimestampVector):
                    value_ptr = (<TimestampVector> value_vector).ptr
                    minmax_i64_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count, self._agg_mode == AGG_MIN,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    minmax_integer_accumulate(
                        self._i64_state.data(), self._seen.data(), state_indices, value_ptr, row_count, self._agg_mode == AGG_MIN,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if self._agg_mode == AGG_AVG:
                value_vector = morsel.column(self._value_column)
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    avg_f64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                elif isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    avg_i64_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices,
                        <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap, row_count,
                    )
                else:
                    value_ptr = (<IntegerVector> value_vector).ptr
                    avg_integer_accumulate(
                        self._avg_sums.data(), self._avg_counts.data(), state_indices, value_ptr, row_count,
                    )
                record_groupby_accumulate_time(self, t_accum_start)
                return

            value_vector = morsel.column(self._value_column)
            if isinstance(value_vector, Float64Vector):
                value_ptr = (<Float64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    state_index = state_indices[row_idx]
                    if self._agg_mode == AGG_COUNT_VALUE:
                        if _bitmap_is_valid(value_nulls, row_idx):
                            self._counts[state_index] = self._counts[state_index] + 1
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if isinstance(value_vector, Int64Vector):
                value_ptr = (<Int64Vector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    state_index = state_indices[row_idx]
                    if self._agg_mode == AGG_COUNT_VALUE:
                        if _bitmap_is_valid(value_nulls, row_idx):
                            self._counts[state_index] = self._counts[state_index] + 1
                record_groupby_accumulate_time(self, t_accum_start)
                return

            if isinstance(value_vector, TimestampVector):
                value_ptr = (<TimestampVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    state_index = state_indices[row_idx]
                    if self._agg_mode == AGG_COUNT_VALUE:
                        if _bitmap_is_valid(value_nulls, row_idx):
                            self._counts[state_index] = self._counts[state_index] + 1
                record_groupby_accumulate_time(self, t_accum_start)
                return

            value_ptr = (<IntegerVector> value_vector).ptr
            value_nulls = <uint8_t*> value_ptr.null_bitmap
            for row_idx in range(row_count):
                state_index = state_indices[row_idx]
                if self._agg_mode == AGG_COUNT_VALUE:
                    if _bitmap_is_valid(value_nulls, row_idx):
                        self._counts[state_index] = self._counts[state_index] + 1
            record_groupby_accumulate_time(self, t_accum_start)
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_object_key_multi(self, Morsel morsel, object key_vector) except *:
        cdef uint64_t[::1] row_hashes
        cdef Py_ssize_t row_idx
        cdef Py_ssize_t row_count = morsel.num_rows
        cdef int64_t* state_indices = NULL
        cdef int64_t state_index
        cdef Py_ssize_t agg_idx
        cdef Py_ssize_t offset
        cdef int64_t agg_mode
        cdef object value_vector
        cdef DrakenFixedBuffer* value_ptr
        cdef uint8_t* value_nulls
        cdef list key_vectors = []
        cdef bytes group_name
        cdef const char* key_data_ptr = NULL
        cdef Py_ssize_t key_data_len = 0
        cdef int64_t key_valid_flag
        cdef Py_ssize_t local_hits = 0
        cdef Py_ssize_t local_misses = 0
        cdef Py_ssize_t local_bloom_checks = 0
        cdef Py_ssize_t local_bloom_skips = 0
        cdef Py_ssize_t local_bloom_fps = 0
        cdef long long t_state_assign_start
        cdef long long t_hash_start
        cdef long long t_accum_start

        t_hash_start = time.monotonic_ns()
        row_hashes = morsel.hash(self._group_by_columns)
        record_groupby_hash_time(self, t_hash_start)

        if self._multi_key_object_mode:
            for group_name in self._group_by_columns:
                key_vectors.append(morsel.column(group_name))
        else:
            key_vectors.append(key_vector)

        state_indices = <int64_t*> malloc(row_count * sizeof(int64_t))
        if state_indices == NULL and row_count > 0:
            raise MemoryError()

        try:
            t_state_assign_start = time.monotonic_ns()
            for row_idx in range(row_count):
                state_index = -1
                if self._use_bloom:
                    local_bloom_checks += 1
                if self._bloom_might_contain(row_hashes[row_idx]) and self._index.lookup_fast(row_hashes[row_idx], state_index):
                    local_hits += 1
                    state_indices[row_idx] = state_index
                    continue
                if self._use_bloom:
                    if not self._groupby_bloom._possibly_contains_fast(row_hashes[row_idx]):
                        local_bloom_skips += 1
                    else:
                        local_bloom_fps += 1
                local_misses += 1
                if self._multi_key_object_mode:
                    state_indices[row_idx] = self._insert_multi_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_vectors,
                        row_idx,
                    )
                else:
                    key_valid_flag = self._extract_stringlike_key(
                        key_vectors[0], row_idx, &key_data_ptr, &key_data_len
                    )
                    state_indices[row_idx] = self._insert_encoded_state_known_miss(
                        row_hashes[row_idx],
                        key_data_ptr,
                        key_data_len,
                        key_valid_flag,
                    )
            record_ingest_state_assign_time(self, t_state_assign_start)
            record_ingest_hit_miss_counts(self, local_hits, local_misses)
            record_bloom_stats(self, local_bloom_checks, local_bloom_skips, local_bloom_fps)
            t_accum_start = time.monotonic_ns()

            for agg_idx in range(self._multi_agg_count):
                agg_mode = self._multi_agg_modes[agg_idx]
                if agg_mode == AGG_COUNT_STAR:
                    count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, self._multi_agg_count, agg_idx)
                    continue
                if agg_mode == AGG_COUNT_DISTINCT:
                    self._ingest_count_distinct_multi_for_states(morsel, state_indices, row_count, agg_idx)
                    continue
                if (
                    self._multi_value_kinds[agg_idx] == VALUE_OBJECT
                    and agg_mode == AGG_ANY_VALUE
                ):
                    self._ingest_any_value_var_multi_for_states(
                        morsel, state_indices, row_count, agg_idx
                    )
                    continue

                if (
                    self._multi_value_kinds[agg_idx] == VALUE_OBJECT
                    and agg_mode in (AGG_MIN, AGG_MAX)
                ):
                    self._ingest_object_minmax_multi_for_states(
                        morsel, state_indices, row_count, agg_idx
                    )
                    continue

                if agg_mode == AGG_SUM:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        print(f"DEBUG: Detected ConstAccessor for AGG_SUM in _ingest_object_key_multi aggregate {agg_idx}")
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, add to all states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_f64_state[offset] = self._multi_f64_state[offset] + const_f64_val
                                self._multi_seen[offset] = 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_i64_state[offset] = self._multi_i64_state[offset] + const_i64_val
                                self._multi_seen[offset] = 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        sum_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        sum_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        sum_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                if agg_mode in (AGG_MIN, AGG_MAX):
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        print(f"DEBUG: Detected ConstAccessor for AGG_MIN/MAX in _ingest_object_key_multi aggregate {agg_idx}")
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, initialize or update states
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_f64_state[offset] = const_f64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_f64_val < self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                                elif agg_mode == AGG_MAX and const_f64_val > self._multi_f64_state[offset]:
                                    self._multi_f64_state[offset] = const_f64_val
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                if self._multi_seen[offset] == 0:
                                    self._multi_i64_state[offset] = const_i64_val
                                    self._multi_seen[offset] = 1
                                elif agg_mode == AGG_MIN and const_i64_val < self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                                elif agg_mode == AGG_MAX and const_i64_val > self._multi_i64_state[offset]:
                                    self._multi_i64_state[offset] = const_i64_val
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        minmax_f64_multi_accumulate(
                            self._multi_f64_state.data(), self._multi_seen.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        minmax_i64_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        minmax_integer_multi_accumulate(
                            self._multi_i64_state.data(), self._multi_seen.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx, agg_mode == AGG_MIN,
                        )
                    continue

                if agg_mode == AGG_AVG:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    value_const_accessor = _vector_const_accessor(value_vector)
                    if value_const_accessor != NULL:
                        print(f"DEBUG: Detected ConstAccessor for AGG_AVG in _ingest_object_key_multi aggregate {agg_idx}")
                        if _const_accessor_is_null(value_const_accessor):
                            continue
                        # For non-null constant values, accumulate sum and count
                        if self._multi_value_kinds[agg_idx] == VALUE_FLOAT64:
                            const_f64_val = (<double*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + const_f64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        elif self._multi_value_kinds[agg_idx] == VALUE_INT64:
                            const_i64_val = (<int64_t*>value_const_accessor.value_ptr)[0]
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_avg_sums[offset] = self._multi_avg_sums[offset] + <double>const_i64_val
                                self._multi_avg_counts[offset] = self._multi_avg_counts[offset] + 1
                        continue

                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        avg_f64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <double*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        avg_i64_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            <int64_t*> value_ptr.data, <uint8_t*> value_ptr.null_bitmap,
                            row_count, self._multi_agg_count, agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        avg_integer_multi_accumulate(
                            self._multi_avg_sums.data(), self._multi_avg_counts.data(), state_indices,
                            value_ptr, row_count, self._multi_agg_count, agg_idx,
                        )
                    continue

                if agg_mode == AGG_ANY_VALUE and self._multi_value_kinds[agg_idx] != VALUE_OBJECT:
                    value_vector = morsel.column(self._multi_value_columns[agg_idx])
                    if isinstance(value_vector, Float64Vector):
                        value_ptr = (<Float64Vector> value_vector).ptr
                        any_value_fixed_multi_accumulate(
                            self._multi_i64_state.data(),
                            self._multi_seen.data(),
                            state_indices,
                            value_ptr,
                            row_count,
                            self._multi_agg_count,
                            agg_idx,
                        )
                    elif isinstance(value_vector, Int64Vector):
                        value_ptr = (<Int64Vector> value_vector).ptr
                        any_value_fixed_multi_accumulate(
                            self._multi_i64_state.data(),
                            self._multi_seen.data(),
                            state_indices,
                            value_ptr,
                            row_count,
                            self._multi_agg_count,
                            agg_idx,
                        )
                    elif isinstance(value_vector, TimestampVector):
                        value_ptr = (<TimestampVector> value_vector).ptr
                        any_value_fixed_multi_accumulate(
                            self._multi_i64_state.data(),
                            self._multi_seen.data(),
                            state_indices,
                            value_ptr,
                            row_count,
                            self._multi_agg_count,
                            agg_idx,
                        )
                    else:
                        value_ptr = (<IntegerVector> value_vector).ptr
                        any_value_fixed_integer_multi_accumulate(
                            self._multi_i64_state.data(),
                            self._multi_seen.data(),
                            state_indices,
                            value_ptr,
                            row_count,
                            self._multi_agg_count,
                            agg_idx,
                        )
                    continue

                value_vector = morsel.column(self._multi_value_columns[agg_idx])
                if isinstance(value_vector, Float64Vector):
                    value_ptr = (<Float64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                if isinstance(value_vector, Int64Vector):
                    value_ptr = (<Int64Vector> value_vector).ptr
                    value_nulls = <uint8_t*> value_ptr.null_bitmap
                    for row_idx in range(row_count):
                        if not _bitmap_is_valid(value_nulls, row_idx):
                            continue
                        offset = self._multi_offset(state_indices[row_idx], agg_idx)
                        if agg_mode == AGG_COUNT_VALUE:
                            self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                # Check for constant vector accessor for the fallback COUNT_VALUE path
                value_const_accessor = _vector_const_accessor(value_vector)
                if value_const_accessor != NULL:
                    # For constant vectors, if not null, increment count for all rows
                    if not _const_accessor_is_null(value_const_accessor):
                        if agg_mode == AGG_COUNT_VALUE:
                            for row_idx in range(row_count):
                                offset = self._multi_offset(state_indices[row_idx], agg_idx)
                                self._multi_counts[offset] = self._multi_counts[offset] + 1
                    continue

                value_ptr = (<IntegerVector> value_vector).ptr
                value_nulls = <uint8_t*> value_ptr.null_bitmap
                for row_idx in range(row_count):
                    if not _bitmap_is_valid(value_nulls, row_idx):
                        continue
                    offset = self._multi_offset(state_indices[row_idx], agg_idx)
                    if agg_mode == AGG_COUNT_VALUE:
                        self._multi_counts[offset] = self._multi_counts[offset] + 1
            record_groupby_accumulate_time(self, t_accum_start)
        finally:
            if state_indices != NULL:
                free(state_indices)

    cdef void _ingest_constant_mode(self, Morsel morsel):
        ingest_constant_mode(self, morsel)

    cpdef void ingest(self, Morsel morsel):
        cdef object key_vector
        cdef object group_column
        cdef DictAccessor* key_dict_accessor = NULL
        cdef bint saw_dict_group_key = False
        cdef long long t_reserve_start

        self._maybe_init_carchar_mode(morsel)

        if self._mode == MODE_CONSTANT:
            if morsel is None or morsel.num_rows == 0:
                return
            self._ingest_constant_mode(morsel)
            return

        if morsel is None or morsel.num_rows == 0:
            return

        for group_column in self._group_by_columns:
            if _vector_dict_accessor(morsel.column(group_column)) != NULL:
                saw_dict_group_key = True
                break

        if saw_dict_group_key:
            record_dict_groupby_fastpath_hit(self)

        self._maybe_init_bloom()
        t_reserve_start = time.monotonic_ns()
        self._reserve_for_rows(morsel.num_rows)
        record_groupby_reserve_time(self, t_reserve_start)

        key_vector = None
        if not self._multi_key_object_mode and not self._multi_key_fixed_mode:
            key_vector = morsel.column(self._group_column)
            key_dict_accessor = _vector_dict_accessor(key_vector)

        if self._multi_agg_count > 0:
            if self._multi_key_fixed_mode:
                self._ingest_multi_fixed_key_multi(morsel)
            elif self._multi_key_object_mode:
                self._ingest_object_key_multi(morsel, None)
            elif key_dict_accessor != NULL:
                if len(self._group_by_columns) > 1:
                    if self._is_multi_fixed_kind(_dict_accessor_key_kind(key_dict_accessor)):
                        self._ingest_multi_fixed_key_multi(morsel)
                    else:
                        self._ingest_object_key_multi(morsel, None)
                else:
                    self._ingest_dictionary_key_multi(morsel, key_vector)
            elif self._use_object_keys:
                self._ingest_object_key_multi(morsel, key_vector)
            elif isinstance(key_vector, Int64Vector):
                self._ingest_int64_key_multi(morsel, <Int64Vector> key_vector)
            elif isinstance(key_vector, IntegerVector):
                self._ingest_integer_key_multi(morsel, <IntegerVector> key_vector)
            elif isinstance(key_vector, StringVector):
                self._ingest_object_key_multi(morsel, key_vector)
            else:
                raise TypeError("unsupported key vector type for Carchar multi-aggregate engine")
            return

        if self._multi_key_fixed_mode:
            self._ingest_multi_fixed_key(morsel)
        elif self._multi_key_object_mode:
            self._ingest_object_key(morsel, None)
        elif self._use_object_keys:
            self._ingest_object_key(morsel, key_vector)
        elif isinstance(key_vector, Int64Vector):
            self._ingest_int64_key(morsel, <Int64Vector> key_vector)
        elif isinstance(key_vector, IntegerVector):
            self._ingest_integer_key(morsel, <IntegerVector> key_vector)
        elif key_dict_accessor != NULL:
            self._ingest_dictionary_key(morsel, key_vector)
        elif isinstance(key_vector, StringVector):
            self._ingest_object_key(morsel, key_vector)
        elif isinstance(key_vector, Date32Vector):
            self._ingest_fixed_width_key(morsel, (<Date32Vector> key_vector).ptr)
        elif isinstance(key_vector, TimeVector):
            self._ingest_fixed_width_key(morsel, (<TimeVector> key_vector).ptr)
        elif isinstance(key_vector, TimestampVector):
            self._ingest_fixed_width_key(morsel, (<TimestampVector> key_vector).ptr)
        else:
            raise TypeError("unsupported key vector type for Carchar group-state engine")

    cpdef void seal(self):
        return

    cdef list _output_names(self):
        return [alias for alias, _function, _column in self._aggregations] + [
            column.decode("utf-8") for column in self._group_by_columns
        ]

    cdef object _build_single_fixed_key_vector(self, Py_ssize_t start, Py_ssize_t stop):
        return build_single_fixed_key_vector(
            self._key_payload_bytes,
            self._key_payload_offsets,
            self._group_key_values,
            self._group_key_valid,
            self._single_key_kind,
            start,
            stop,
        )

    cdef object _build_encoded_key_vector(self, Py_ssize_t start, Py_ssize_t stop):
        return build_encoded_key_vector(
            self._key_payload_bytes,
            self._key_payload_offsets,
            start,
            stop,
        )

    cdef object _build_multi_encoded_key_vector(
        self, Py_ssize_t key_idx, Py_ssize_t start, Py_ssize_t stop
    ):
        return build_multi_encoded_key_vector(
            self._multi_encoded_key_bytes,
            self._multi_encoded_key_offsets,
            self._multi_encoded_key_valid,
            key_idx,
            start,
            stop,
        )

    cdef object _build_native_object_vector(self, list values):
        return build_native_object_vector(values)

    cdef object _build_object_state_vector(self, Py_ssize_t start, Py_ssize_t stop):
        return build_object_state_vector(
            self._object_state_bytes,
            self._object_state_starts,
            self._object_state_lengths,
            self._seen,
            start,
            stop,
        )

    cdef object _build_multi_object_state_vector(self, Py_ssize_t start, Py_ssize_t stop, Py_ssize_t agg_idx):
        return build_multi_object_state_vector(
            self._multi_object_state_bytes,
            self._multi_object_state_starts,
            self._multi_object_state_lengths,
            self._multi_seen,
            self._multi_agg_count,
            agg_idx,
            start,
            stop,
        )

    cdef Morsel _empty_morsel(self):
        cdef list names = self._output_names()
        cdef list vectors
        cdef Py_ssize_t agg_count = len(self._aggregations)
        cdef Py_ssize_t key_count = len(self._group_by_columns)
        cdef Py_ssize_t idx
        cdef list values

        if self._mode == MODE_UNINITIALIZED:
            if key_count == 0:
                values = []
                for idx in range(agg_count):
                    if self._aggregations[idx][1] in ("count", "count_distinct", "distinct"):
                        values.append(vector_from_sequence([0]))
                    else:
                        values.append(vector_from_sequence([None]))
                return Morsel.from_vectors(names, values)
            return Morsel.from_vectors(names, [vector_from_sequence([]) for _ in names])

        vectors = []
        for idx in range(agg_count):
            if self._agg_output_is_object(idx):
                vectors.append(self._build_native_object_vector([]))
            elif self._agg_output_is_float(idx):
                vectors.append(Float64Vector(0))
            else:
                vectors.append(Int64Vector(0))
        for idx in range(key_count):
            if self._multi_key_fixed_mode:
                vectors.append(Int64Vector(0))
            elif self._use_object_keys:
                vectors.append(self._build_native_object_vector([]))
            else:
                vectors.append(Int64Vector(0))
        return Morsel.from_vectors(names, vectors)

    cpdef object debug_dump(self):
        cdef Py_ssize_t idx
        cdef Py_ssize_t agg_idx
        cdef list out = []
        cdef object key_values
        cdef object key_valids
        if self._mode != MODE_CARCHAR:
            return {"mode": self._mode, "rows": out}
        if self._multi_agg_count > 0:
            for idx in range(self._state_count()):
                key_values, key_valids = self._debug_key_payload_value(idx)
                out.append(
                    (
                        idx,
                        key_values,
                        key_valids,
                        [
                            (
                                self._multi_agg_modes[agg_idx],
                                self._multi_counts[self._multi_offset(idx, agg_idx)],
                                self._multi_i64_state[self._multi_offset(idx, agg_idx)],
                                self._multi_f64_state[self._multi_offset(idx, agg_idx)],
                                self._multi_seen[self._multi_offset(idx, agg_idx)],
                                self._multi_avg_sums[self._multi_offset(idx, agg_idx)],
                                self._multi_avg_counts[self._multi_offset(idx, agg_idx)],
                            )
                            for agg_idx in range(self._multi_agg_count)
                        ],
                    )
                )
            return {"mode": self._mode, "rows": out}
        for idx in range(self._state_count()):
            key_values, key_valids = self._debug_key_payload_value(idx)
            out.append(
                (
                    idx,
                    key_values,
                    key_valids,
                    self._counts[idx],
                    self._i64_state[idx],
                    self._f64_state[idx],
                    self._seen[idx],
                    self._avg_sums[idx],
                    self._avg_counts[idx],
                )
            )
        return {"mode": self._mode, "rows": out}

    cdef Morsel _build_chunk_morsel(self, Py_ssize_t start, Py_ssize_t stop):
        cdef Py_ssize_t length = stop - start
        cdef list names = self._output_names()
        cdef object key_vec
        cdef list key_vectors = []
        cdef Int64Vector key_vec_i64
        cdef int64_t* key_data = NULL
        cdef Int64Vector agg_i64
        cdef Float64Vector agg_f64
        cdef uint8_t* key_nulls = NULL
        cdef Py_ssize_t i
        cdef Py_ssize_t state_index
        cdef bint needs_key_nulls = False
        cdef bint needs_agg_nulls = False
        cdef object agg_object_vec

        if self._agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX, AGG_ANY_VALUE):
            for state_index in range(start, stop):
                if self._seen[state_index] == 0:
                    needs_agg_nulls = True
                    break
        elif self._agg_mode == AGG_AVG:
            for state_index in range(start, stop):
                if self._avg_counts[state_index] == 0:
                    needs_agg_nulls = True
                    break

        if self._multi_key_fixed_mode:
            key_vectors = self._build_multi_fixed_key_vectors(start, stop)
        elif self._use_object_keys:
            key_vectors = build_finalize_key_vectors(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._group_key_values,
                self._group_key_valid,
                self._single_key_kind,
                self._multi_key_object_mode,
                self._multi_group_key_kinds,
                start,
                stop,
            )
            key_vec = key_vectors[0]
        elif (
            self._single_key_kind != KEY_MULTI_FIXED_INT
            or <Py_ssize_t> self._key_payload_offsets.size() >= stop + 1
        ):
            key_vec = build_finalize_single_key_vector(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._group_key_values,
                self._group_key_valid,
                self._single_key_kind,
                start,
                stop,
            )
        else:
            # Legacy non-Carchar path: _group_key_values/_group_key_valid are directly populated.
            for state_index in range(start, stop):
                if self._group_key_valid[state_index] == 0:
                    needs_key_nulls = True
                    break
            key_vec_i64 = Int64Vector(length)
            key_vec = key_vec_i64
            key_data = <int64_t*> key_vec_i64.ptr.data
            if needs_key_nulls:
                key_nulls = _alloc_valid_bitmap(length)
                key_vec_i64.ptr.null_bitmap = key_nulls

        if key_data != NULL:
            for i in range(length):
                state_index = start + i
                key_data[i] = self._group_key_values[state_index]
                if key_nulls != NULL and self._group_key_valid[state_index] != 0:
                    _bitmap_set_valid(key_nulls, i)

        if self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX, AGG_ANY_VALUE):
            agg_object_vec = build_finalize_object_aggregate_vector(
                self._object_state_bytes,
                self._object_state_starts,
                self._object_state_lengths,
                self._seen,
                self._object_state,
                start,
                stop,
            )
            if self._multi_key_fixed_mode:
                return Morsel.from_vectors(names, [agg_object_vec, *key_vectors])
            if self._use_object_keys and self._multi_key_object_mode:
                return Morsel.from_vectors(names, [agg_object_vec, *key_vectors])
            return Morsel.from_vectors(names, [agg_object_vec, key_vec])

        if self._agg_mode == AGG_AVG or self._value_kind == VALUE_FLOAT64 or self._value_kind == VALUE_DICT_FLOAT64:
            agg_f64 = build_finalize_scalar_aggregate_vector(
                self._agg_mode,
                self._value_kind,
                self._counts,
                self._i64_state,
                self._f64_state,
                self._seen,
                self._avg_sums,
                self._avg_counts,
                start,
                stop,
            )

            if self._multi_key_fixed_mode:
                return Morsel.from_vectors(names, [agg_f64, *key_vectors])
            if self._use_object_keys and self._multi_key_object_mode:
                return Morsel.from_vectors(names, [agg_f64, *key_vectors])
            return Morsel.from_vectors(names, [agg_f64, key_vec])

        agg_i64 = build_finalize_scalar_aggregate_vector(
            self._agg_mode,
            self._value_kind,
            self._counts,
            self._i64_state,
            self._f64_state,
            self._seen,
            self._avg_sums,
            self._avg_counts,
            start,
            stop,
        )

        if self._multi_key_fixed_mode:
            return Morsel.from_vectors(names, [agg_i64, *key_vectors])
        if self._use_object_keys and self._multi_key_object_mode:
            return Morsel.from_vectors(names, [agg_i64, *key_vectors])
        return Morsel.from_vectors(names, [agg_i64, key_vec])

    cdef Morsel _build_chunk_morsel_multi(self, Py_ssize_t start, Py_ssize_t stop):
        cdef Py_ssize_t length = stop - start
        cdef list names = self._output_names()
        cdef object key_vec
        cdef list key_vectors = []
        cdef Int64Vector key_vec_i64
        cdef int64_t* key_data = NULL
        cdef uint8_t* key_nulls = NULL
        cdef Py_ssize_t state_index
        cdef bint needs_key_nulls = False
        cdef list vectors
        cdef Py_ssize_t key_idx
        cdef Py_ssize_t expected_total_vectors
        cdef object output_name
        cdef object output_vector
        cdef bint expected_object_output
        cdef bint expected_float_output

        self._debug_last_finalize_stage = f"_build_chunk_morsel_multi enter start={start} stop={stop}"

        if self._multi_key_fixed_mode:
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi before _build_multi_fixed_key_vectors start={start} stop={stop}"
            key_vectors = self._build_multi_fixed_key_vectors(start, stop)
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi after _build_multi_fixed_key_vectors start={start} stop={stop}"
            if key_vectors is None:
                raise RuntimeError("multi-key fixed finalize returned None key vector list")
        elif <Py_ssize_t> self._group_key_valid.size() >= stop:
            # Legacy non-Carchar path: _group_key_valid is directly populated.
            for state_index in range(start, stop):
                if self._group_key_valid[state_index] == 0:
                    needs_key_nulls = True
                    break
        # else: Carchar path — _group_key_valid is not populated; nulls are encoded
        # in _key_payload_bytes/_key_payload_offsets and handled by
        # build_finalize_single_key_vector below.  needs_key_nulls stays False,
        # but that variable is only consumed by the legacy else-branch below which
        # is never reached in Carchar mode (the elif condition is True there).

        if self._multi_key_fixed_mode:
            if len(key_vectors) != len(self._group_by_columns):
                raise RuntimeError(
                    f"multi-key fixed finalize produced {len(key_vectors)} key vectors for "
                    f"{len(self._group_by_columns)} group columns"
                )
            for key_vec in key_vectors:
                if key_vec is None:
                    raise RuntimeError("multi-key fixed finalize returned None key vector")
        elif (
            not self._multi_key_object_mode
            and self._is_multi_fixed_kind(self._single_key_kind)
            and (<Py_ssize_t> self._key_payload_offsets.size() >= stop + 1 or self._encoded_key_valid.size() == 0)
        ):
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi before build_finalize_single_key_vector start={start} stop={stop}"
            key_vec = build_finalize_single_key_vector(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._group_key_values,
                self._group_key_valid,
                self._single_key_kind,
                start,
                stop,
            )
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi after build_finalize_single_key_vector start={start} stop={stop}"
            if key_vec is None:
                raise RuntimeError("single-key finalize returned None key vector")
        elif self._use_object_keys or len(self._group_by_columns) > 1:
            if <Py_ssize_t> self._key_payload_offsets.size() < stop + 1:
                raise RuntimeError(
                    f"multi-key finalize missing payload offsets for rows {start}:{stop}; "
                    f"have {self._key_payload_offsets.size()} offsets"
                )
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi before build_finalize_key_vectors start={start} stop={stop}"
            key_vectors = build_finalize_key_vectors(
                self._key_payload_bytes,
                self._key_payload_offsets,
                self._group_key_values,
                self._group_key_valid,
                self._single_key_kind,
                self._multi_key_object_mode,
                self._multi_group_key_kinds,
                start,
                stop,
            )
            self._debug_last_finalize_stage = f"_build_chunk_morsel_multi after build_finalize_key_vectors start={start} stop={stop}"
            if key_vectors is None:
                raise RuntimeError("multi-key finalize returned None key vector list")
            if len(key_vectors) != len(self._group_by_columns):
                raise RuntimeError(
                    f"multi-key finalize produced {len(key_vectors)} key vectors for "
                    f"{len(self._group_by_columns)} group columns"
                )
            key_vec = key_vectors[0]
            if key_vec is None:
                raise RuntimeError("multi-key finalize returned None first key vector")
        else:
            key_vec_i64 = Int64Vector(length)
            key_vec = key_vec_i64
            key_data = <int64_t*> key_vec_i64.ptr.data
            if needs_key_nulls:
                key_nulls = _alloc_valid_bitmap(length)
                key_vec_i64.ptr.null_bitmap = key_nulls

            for i in range(length):
                state_index = start + i
                key_data[i] = self._group_key_values[state_index]
                if key_nulls != NULL and self._group_key_valid[state_index] != 0:
                    _bitmap_set_valid(key_nulls, i)

        self._debug_last_finalize_stage = f"_build_chunk_morsel_multi before build_finalize_multi_aggregate_vectors start={start} stop={stop}"
        vectors = build_finalize_multi_aggregate_vectors(
            self._multi_agg_modes,
            self._multi_value_kinds,
            self._multi_counts,
            self._multi_i64_state,
            self._multi_f64_state,
            self._multi_seen,
            self._multi_avg_sums,
            self._multi_avg_counts,
            self._multi_object_state_bytes,
            self._multi_object_state_starts,
            self._multi_object_state_lengths,
            self._multi_object_state,
            self._multi_agg_count,
            start,
            stop,
        )
        self._debug_last_finalize_stage = f"_build_chunk_morsel_multi after build_finalize_multi_aggregate_vectors start={start} stop={stop}"
        if vectors is None:
            raise RuntimeError("multi-aggregate finalize returned None aggregate vector list")
        if len(vectors) != self._multi_agg_count:
            raise RuntimeError(
                f"multi-aggregate finalize produced {len(vectors)} aggregate vectors for "
                f"{self._multi_agg_count} aggregates"
            )

        if self._multi_key_fixed_mode:
            if len(key_vectors) == 0:
                raise RuntimeError("multi-key fixed finalize produced empty key vector list")
            vectors.extend(key_vectors)
        elif self._use_object_keys and self._multi_key_object_mode:
            if len(key_vectors) == 0:
                raise RuntimeError("multi-key object finalize produced empty key vector list")
            vectors.extend(key_vectors)
        elif len(self._group_by_columns) > 1:
            if len(key_vectors) == 0:
                raise RuntimeError("multi-key finalize produced empty key vector list")
            vectors.extend(key_vectors)
        else:
            if key_vec is None:
                raise RuntimeError("single-key finalize produced None key vector before morsel construction")
            vectors.append(key_vec)

        expected_total_vectors = self._multi_agg_count + len(self._group_by_columns)
        if len(vectors) != expected_total_vectors:
            raise RuntimeError(
                f"multi-key finalize produced {len(vectors)} total vectors for "
                f"{self._multi_agg_count} aggregates and {len(self._group_by_columns)} group columns; "
                f"expected {expected_total_vectors}"
            )

        # Defensive check: ensure names and vectors match
        if len(names) != len(vectors):
            raise ValueError(
                f"Morsel column count mismatch: {len(names)} names but {len(vectors)} vectors. "
                f"Names: {names}, "
                f"Aggregates: {self._multi_agg_count}, "
                f"Group columns: {len(self._group_by_columns)}"
            )

        for key_idx in range(self._multi_agg_count):
            output_vector = vectors[key_idx]
            if output_vector is None:
                raise RuntimeError(
                    f"multi-key finalize has None aggregate vector at output index {key_idx}"
                )
            output_name = names[key_idx]
            if output_name != self._aggregations[key_idx][0]:
                raise RuntimeError(
                    f"multi-key finalize aggregate output order mismatch at index {key_idx}: "
                    f"name {output_name!r} does not match aggregation alias "
                    f"{self._aggregations[key_idx][0]!r}"
                )
            expected_object_output = self._agg_output_is_object(key_idx)
            expected_float_output = self._agg_output_is_float(key_idx)
            if expected_object_output:
                if not isinstance(output_vector, StringVector):
                    raise RuntimeError(
                        f"multi-key finalize aggregate output type mismatch at index {key_idx}: "
                        f"expected StringVector-compatible object output, got "
                        f"{type(output_vector).__name__}"
                    )
            elif expected_float_output:
                if not isinstance(output_vector, Float64Vector):
                    raise RuntimeError(
                        f"multi-key finalize aggregate output type mismatch at index {key_idx}: "
                        f"expected Float64Vector, got {type(output_vector).__name__}"
                    )
            else:
                if not isinstance(output_vector, Int64Vector):
                    raise RuntimeError(
                        f"multi-key finalize aggregate output type mismatch at index {key_idx}: "
                        f"expected Int64Vector, got {type(output_vector).__name__}"
                    )

        for key_idx in range(len(self._group_by_columns)):
            output_vector = vectors[self._multi_agg_count + key_idx]
            if output_vector is None:
                raise RuntimeError(
                    f"multi-key finalize has None key vector at output index "
                    f"{self._multi_agg_count + key_idx}"
                )
            output_name = names[self._multi_agg_count + key_idx]
            if output_name != self._group_by_columns[key_idx].decode("utf-8"):
                raise RuntimeError(
                    f"multi-key finalize key output order mismatch at index {key_idx}: "
                    f"name {output_name!r} does not match group-by column "
                    f"{self._group_by_columns[key_idx].decode('utf-8')!r}"
                )
            if self._multi_key_fixed_mode:
                if self._multi_group_key_kinds[key_idx] == KEY_MULTI_FIXED_DATE32:
                    if not isinstance(output_vector, Date32Vector):
                        raise RuntimeError(
                            f"multi-key finalize key output type mismatch at index {key_idx}: "
                            f"expected Date32Vector, got {type(output_vector).__name__}"
                        )
                elif self._multi_group_key_kinds[key_idx] == KEY_MULTI_FIXED_TIME32:
                    if not isinstance(output_vector, TimeVector) or (<TimeVector> output_vector).is_time64:
                        raise RuntimeError(
                            f"multi-key finalize key output type mismatch at index {key_idx}: "
                            f"expected TimeVector(time32), got {type(output_vector).__name__}"
                        )
                elif self._multi_group_key_kinds[key_idx] == KEY_MULTI_FIXED_TIME64:
                    if not isinstance(output_vector, TimeVector) or not (<TimeVector> output_vector).is_time64:
                        raise RuntimeError(
                            f"multi-key finalize key output type mismatch at index {key_idx}: "
                            f"expected TimeVector(time64), got {type(output_vector).__name__}"
                        )
                elif self._multi_group_key_kinds[key_idx] == KEY_MULTI_FIXED_TIMESTAMP64:
                    if not isinstance(output_vector, TimestampVector):
                        raise RuntimeError(
                            f"multi-key finalize key output type mismatch at index {key_idx}: "
                            f"expected TimestampVector, got {type(output_vector).__name__}"
                        )
                elif self._multi_group_key_kinds[key_idx] == KEY_MULTI_FIXED_INT:
                    if not isinstance(output_vector, Int64Vector):
                        raise RuntimeError(
                            f"multi-key finalize key output type mismatch at index {key_idx}: "
                            f"expected Int64Vector, got {type(output_vector).__name__}"
                        )
                else:
                    raise RuntimeError(
                        f"multi-key finalize encountered unsupported fixed key kind "
                        f"{self._multi_group_key_kinds[key_idx]} at index {key_idx}"
                    )
            else:
                if not isinstance(output_vector, StringVector):
                    raise RuntimeError(
                        f"multi-key finalize key output type mismatch at index {key_idx}: "
                        f"expected StringVector-compatible encoded/object key output, got "
                        f"{type(output_vector).__name__}"
                    )

        if len(self._group_by_columns) > 1:
            if len(vectors) < len(self._group_by_columns):
                raise RuntimeError(
                    f"multi-key finalize built only {len(vectors)} total vectors for "
                    f"{len(self._group_by_columns)} group columns"
                )
            if len(vectors) != len(names):
                raise RuntimeError(
                    f"multi-key finalize vector/name mismatch before morsel construction: "
                    f"{len(vectors)} vectors vs {len(names)} names"
                )
            for key_vec in vectors[self._multi_agg_count:]:
                if key_vec is None:
                    raise RuntimeError("multi-key finalize has None key vector in morsel payload")

        self._debug_last_finalize_stage = f"_build_chunk_morsel_multi before Morsel.from_vectors start={start} stop={stop}"
        self._debug_last_finalize_stage = (
            f"_build_chunk_morsel_multi native morsel construction call start={start} "
            f"stop={stop}"
        )
        return Morsel.from_vectors(names, vectors)

    cpdef object finalize_fast_columns(self):
        cdef Py_ssize_t n
        cdef Py_ssize_t idx
        cdef object keys
        cdef object values_q
        cdef object values_d
        cdef int64_t[::1] key_view
        cdef int64_t[::1] value_q_view
        cdef double[::1] value_d_view
        cdef int64_t valid_flag

        if self._multi_agg_count > 0:
            return None
        if self._mode == MODE_CONSTANT:
            if self._constant_key_valid == 0 or not isinstance(self._constant_key_scalar, int):
                return None
            keys = array("q", [<int64_t> self._constant_key_scalar])
            if self._agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE):
                return keys, array("q", [self._constant_count])
            if self._agg_mode == AGG_COUNT_DISTINCT:
                return keys, array(
                    "q",
                    [
                        0
                        if self._constant_distinct_set is None
                        else <int64_t> self._constant_distinct_set.size()
                    ],
                )
            if self._value_kind == VALUE_OBJECT:
                return None
            if self._agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
                if self._constant_seen == 0:
                    return None
                if self._value_kind == VALUE_FLOAT64:
                    return keys, array("d", [self._constant_f64_state])
                return keys, array("q", [self._constant_i64_state])
            if self._agg_mode == AGG_AVG:
                if self._constant_avg_count == 0:
                    return None
                return keys, array("d", [self._constant_avg_sum / self._constant_avg_count])
            return None
        if self._mode != MODE_CARCHAR:
            return None
        if self._use_object_keys or self._multi_key_object_mode:
            return None

        n = self._state_count()
        if n == 0:
            return array("q"), array("q")

        if self._value_kind == VALUE_OBJECT:
            return None

        keys = array("q", [0]) * n
        key_view = keys
        if <Py_ssize_t> self._key_payload_offsets.size() >= n + 1:
            for idx in range(n):
                if not decode_single_fixed_key_record(
                    self._key_payload_bytes,
                    self._key_payload_offsets,
                    idx,
                    &key_view[idx],
                    &valid_flag,
                ):
                    return None
                if valid_flag == 0:
                    return None
        else:
            for idx in range(n):
                if self._group_key_valid[idx] == 0:
                    return None
                key_view[idx] = self._group_key_values[idx]

        if self._agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE, AGG_COUNT_DISTINCT):
            values_q = array("q", [0]) * n
            value_q_view = values_q
            for idx in range(n):
                value_q_view[idx] = self._counts[idx]
            return keys, values_q

        if self._agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
            for idx in range(n):
                if self._seen[idx] == 0:
                    return None
            if self._value_kind == VALUE_FLOAT64:
                values_d = array("d", [0.0]) * n
                value_d_view = values_d
                for idx in range(n):
                    value_d_view[idx] = self._f64_state[idx]
                return keys, values_d
            values_q = array("q", [0]) * n
            value_q_view = values_q
            for idx in range(n):
                value_q_view[idx] = self._i64_state[idx]
            return keys, values_q

        if self._agg_mode == AGG_AVG:
            values_d = array("d", [0.0]) * n
            value_d_view = values_d
            for idx in range(n):
                if self._avg_counts[idx] == 0:
                    return None
                value_d_view[idx] = self._avg_sums[idx] / self._avg_counts[idx]
            return keys, values_d

        return None

    cpdef object finalize_fast_columns_chunked(self, Py_ssize_t chunk_size=65536):
        cdef object fast_columns
        cdef object keys
        cdef object values
        cdef Py_ssize_t total
        cdef Py_ssize_t start
        cdef Py_ssize_t stop
        cdef list chunks

        fast_columns = self.finalize_fast_columns()
        if fast_columns is None:
            return None

        keys, values = fast_columns
        total = len(keys)
        chunks = []
        for start in range(0, total, chunk_size):
            stop = min(total, start + chunk_size)
            chunks.append((keys[start:stop], values[start:stop]))
        return chunks

    cpdef object stats(self):
        return dict(self._readings)

    cpdef Morsel finalize(self):
        cdef object morsel
        if self._mode == MODE_UNINITIALIZED:
            return self._empty_morsel()
        for morsel in self.finalize_morsels(65536):
            return morsel
        return self._empty_morsel()

    def finalize_morsels(self, Py_ssize_t chunk_size=65536):
        cdef Py_ssize_t total
        cdef Py_ssize_t start
        cdef Py_ssize_t stop
        cdef object morsel
        cdef long long vector_st
        cdef long long morsel_st
        cdef object agg_value
        cdef object agg_vec
        cdef object key_value
        cdef object key_out_vec
        cdef bint building_multi_chunk

        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        if self._mode == MODE_UNINITIALIZED:
            yield self._empty_morsel()
            return

        if self._mode == MODE_CONSTANT:
            record_finalize_rows_count(self, 1)
            if self._agg_mode == AGG_COUNT_STAR or self._agg_mode == AGG_COUNT_VALUE:
                agg_value = self._constant_count
            elif self._agg_mode == AGG_COUNT_DISTINCT:
                if self._constant_distinct_set is None:
                    agg_value = 0
                else:
                    agg_value = <int64_t> self._constant_distinct_set.size()
            elif self._value_kind == VALUE_OBJECT and self._agg_mode in (AGG_MIN, AGG_MAX, AGG_ANY_VALUE):
                if self._constant_seen == 0:
                    agg_value = None
                else:
                    agg_value = self._constant_object_state
            elif self._agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
                if self._constant_seen == 0:
                    agg_value = None
                elif self._value_kind == VALUE_FLOAT64:
                    agg_value = self._constant_f64_state
                else:
                    agg_value = self._constant_i64_state
            elif self._agg_mode == AGG_AVG:
                if self._constant_avg_count == 0:
                    agg_value = None
                else:
                    agg_value = self._constant_avg_sum / self._constant_avg_count
            else:
                agg_value = None

            key_value = self._constant_key_scalar if self._constant_key_valid != 0 else None
            vector_st = time.monotonic_ns()
            agg_vec, key_out_vec = build_constant_groupby_vectors(
                agg_value,
                key_value,
                self._agg_output_is_object(0),
                self._use_object_keys,
            )
            record_constant_groupby_vector(self, key_out_vec)
            record_finalize_rows_to_vectors_time(self, vector_st)
            morsel_st = time.monotonic_ns()
            morsel = Morsel.from_vectors(self._output_names(), [agg_vec, key_out_vec])
            record_finalize_morsel_build_time(self, morsel_st)
            record_finalize_chunk_emitted(self)
            yield morsel
            return

        total = self._state_count()
        record_finalize_rows_count(self, total)
        if total == 0:
            yield self._empty_morsel()
            return

        for start in range(0, total, chunk_size):
            stop = min(total, start + chunk_size)
            vector_st = time.monotonic_ns()
            building_multi_chunk = self._multi_agg_count > 0
            if building_multi_chunk:
                self._debug_last_finalize_stage = f"before _build_chunk_morsel_multi start={start} stop={stop}"
                morsel = self._build_chunk_morsel_multi(start, stop)
                self._debug_last_finalize_stage = f"after _build_chunk_morsel_multi start={start} stop={stop}"
            else:
                self._debug_last_finalize_stage = f"before _build_chunk_morsel start={start} stop={stop}"
                morsel = self._build_chunk_morsel(start, stop)
                self._debug_last_finalize_stage = f"after _build_chunk_morsel start={start} stop={stop}"
            record_finalize_rows_to_vectors_time(self, vector_st)
            record_finalize_chunk_emitted(self)
            self._debug_last_finalize_stage = f"before yield start={start} stop={stop}"
            yield morsel
            self._debug_last_finalize_stage = f"after yield start={start} stop={stop}"
