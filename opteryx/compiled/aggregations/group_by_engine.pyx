# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from __future__ import annotations

from array import array
import time
import sys

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcmp
from libcpp.vector cimport vector

from opteryx.draken.core.buffers cimport ConstAccessor
from opteryx.draken.core.buffers cimport DictAccessor
from opteryx.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.buffers cimport DRAKEN_DATE32
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT32
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.buffers cimport DRAKEN_INT16
from opteryx.draken.core.buffers cimport DRAKEN_INT32
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.buffers cimport DRAKEN_INT8
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.buffers cimport DRAKEN_TIME32
from opteryx.draken.core.buffers cimport DRAKEN_TIME64
from opteryx.draken.core.buffers cimport DRAKEN_TIMESTAMP64
from opteryx.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.string_vector cimport StringVectorBuilder
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.vector cimport Vector
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
from opteryx.compiled.aggregations.vector_readers cimport _extract_stringlike_key
from opteryx.compiled.aggregations.vector_readers cimport _read_dictionary_fixed_key
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
from opteryx.compiled.aggregations.group_by_finalize cimport build_finalize_multi_object_aggregate_vector
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
from opteryx.compiled.aggregations.kernels.count cimport count_accumulate
from opteryx.compiled.aggregations.kernels.count cimport count_multi_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_accumulate
from opteryx.compiled.aggregations.kernels.any_value_fixed cimport any_value_fixed_multi_accumulate
from opteryx.compiled.aggregations.kernels.any_value_var cimport any_value_var_accumulate
from opteryx.compiled.aggregations.kernels.any_value_var cimport any_value_var_multi_accumulate
from opteryx.compiled.aggregations.kernels.min_max_var cimport minmax_var_accumulate
from opteryx.compiled.aggregations.kernels.min_max_var cimport minmax_var_multi_accumulate
from opteryx.third_party.abseil.containers cimport FlatHashSet

cdef inline bint _bitmap_is_valid(uint8_t* bitmap, Py_ssize_t index) noexcept:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline int64_t _read_integer_value(DrakenFixedBuffer* ptr, Py_ssize_t index) noexcept:
    if ptr.itemsize == 1:
        return (<char*> ptr.data)[index]
    if ptr.itemsize == 2:
        return (<short*> ptr.data)[index]
    if ptr.itemsize == 4:
        return (<int*> ptr.data)[index]
    return (<int64_t*> ptr.data)[index]

# --- constant-key ingest (inlined from constant_keys.pyx) ---

cdef void _ingest_constant_distinct(object self, Morsel morsel, object value_vector, Py_ssize_t row_count):
    cdef Py_ssize_t row_idx
    cdef DrakenFixedBuffer* value_ptr
    cdef int64_t* value_i64_data
    cdef uint8_t* value_nulls
    cdef uint64_t[::1] value_hashes

    if self._constant_distinct_set is None:
        self._constant_distinct_set = FlatHashSet()

    if isinstance(value_vector, Int64Vector):
        value_ptr = (<Int64Vector> value_vector).ptr
        value_i64_data = <int64_t*> value_ptr.data
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx) and (<FlatHashSet> self._constant_distinct_set).insert(<uint64_t> value_i64_data[row_idx]):
                self._constant_count += 1
        return

    if isinstance(value_vector, IntegerVector):
        value_ptr = (<IntegerVector> value_vector).ptr
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx) and (<FlatHashSet> self._constant_distinct_set).insert(<uint64_t> _read_integer_value(value_ptr, row_idx)):
                self._constant_count += 1
        return

    value_nulls = (<Vector> value_vector).null_bitmap_ptr()
    value_hashes = morsel.hash([self._value_column])
    for row_idx in range(row_count):
        if _bitmap_is_valid(value_nulls, row_idx) and (<FlatHashSet> self._constant_distinct_set).insert(value_hashes[row_idx]):
            self._constant_count += 1


cdef void _ingest_constant_const_accessor(
    object self,
    Morsel morsel,
    object value_vector,
    ConstAccessor* value_const_accessor,
    Py_ssize_t row_count,
):
    cdef object value_obj
    cdef uint64_t[::1] value_hashes
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
            if _bitmap_is_valid(value_nulls, row_idx):
                if self._agg_mode == 2:
                    self._constant_count += 1
        return

    if isinstance(value_vector, Int64Vector):
        value_ptr = (<Int64Vector> value_vector).ptr
        value_i64_data = <int64_t*> value_ptr.data
        value_nulls = <uint8_t*> value_ptr.null_bitmap
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx):
                if self._agg_mode == 2:
                    self._constant_count += 1
        return

    value_dict_accessor = _vector_value_dict_accessor(value_vector)
    if value_dict_accessor != NULL:
        value_nulls = value_dict_accessor.row_nulls
        for row_idx in range(row_count):
            if _bitmap_is_valid(value_nulls, row_idx):
                if self._agg_mode == 2:
                    self._constant_count += 1
        return

    value_ptr = (<IntegerVector> value_vector).ptr
    value_nulls = <uint8_t*> value_ptr.null_bitmap
    for row_idx in range(row_count):
        if _bitmap_is_valid(value_nulls, row_idx):
            if self._agg_mode == 2:
                self._constant_count += 1

# ... [TRUNCATED: the file continues with the existing engine implementation] ...
