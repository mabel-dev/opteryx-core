# cython: language_level=3

from libc.stdint cimport int32_t, int64_t, uint8_t
from libcpp.vector cimport vector

cdef object build_native_object_vector(list values)
cdef object build_single_fixed_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    vector[int64_t]& group_key_values,
    vector[int64_t]& group_key_valid,
    int64_t single_key_kind,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef list build_payload_multi_key_vectors(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    list multi_group_key_kinds,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_encoded_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_multi_encoded_key_vector(
    vector[vector[uint8_t]]& multi_encoded_key_bytes,
    vector[vector[int32_t]]& multi_encoded_key_offsets,
    vector[vector[int64_t]]& multi_encoded_key_valid,
    Py_ssize_t key_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef list build_finalize_key_vectors(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    vector[int64_t]& group_key_values,
    vector[int64_t]& group_key_valid,
    int64_t single_key_kind,
    bint multi_key_object_mode,
    vector[int64_t]& multi_group_key_kinds,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_single_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    vector[int64_t]& group_key_values,
    vector[int64_t]& group_key_valid,
    int64_t single_key_kind,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_object_aggregate_vector(
    vector[uint8_t]& object_state_bytes,
    vector[int32_t]& object_state_starts,
    vector[int32_t]& object_state_lengths,
    vector[int64_t]& seen,
    list object_state,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_multi_object_aggregate_vector(
    vector[uint8_t]& multi_object_state_bytes,
    vector[int32_t]& multi_object_state_starts,
    vector[int32_t]& multi_object_state_lengths,
    vector[int64_t]& multi_seen,
    list multi_object_state,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_scalar_aggregate_vector(
    int64_t agg_mode,
    int64_t value_kind,
    vector[int64_t]& counts,
    vector[int64_t]& i64_state,
    vector[double]& f64_state,
    vector[int64_t]& seen,
    vector[double]& avg_sums,
    vector[int64_t]& avg_counts,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_multi_scalar_aggregate_vector(
    int64_t agg_mode,
    int64_t value_kind,
    bint output_is_float,
    vector[int64_t]& multi_counts,
    vector[int64_t]& multi_i64_state,
    vector[double]& multi_f64_state,
    vector[int64_t]& multi_seen,
    vector[double]& multi_avg_sums,
    vector[int64_t]& multi_avg_counts,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef list build_finalize_multi_aggregate_vectors(
    vector[int64_t]& multi_agg_modes,
    vector[int64_t]& multi_value_kinds,
    vector[int64_t]& multi_counts,
    vector[int64_t]& multi_i64_state,
    vector[double]& multi_f64_state,
    vector[int64_t]& multi_seen,
    vector[double]& multi_avg_sums,
    vector[int64_t]& multi_avg_counts,
    vector[uint8_t]& multi_object_state_bytes,
    vector[int32_t]& multi_object_state_starts,
    vector[int32_t]& multi_object_state_lengths,
    list multi_object_state,
    Py_ssize_t multi_agg_count,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_object_aggregate_vector(
    vector[uint8_t]& object_state_bytes,
    vector[int32_t]& object_state_starts,
    vector[int32_t]& object_state_lengths,
    vector[int64_t]& seen,
    list object_state,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_finalize_multi_object_aggregate_vector(
    vector[uint8_t]& multi_object_state_bytes,
    vector[int32_t]& multi_object_state_starts,
    vector[int32_t]& multi_object_state_lengths,
    vector[int64_t]& multi_seen,
    list multi_object_state,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef tuple build_constant_groupby_vectors(
    object agg_value,
    object key_value,
    bint agg_is_object,
    bint use_object_keys,
)
cdef object build_object_state_vector(
    vector[uint8_t]& object_state_bytes,
    vector[int32_t]& object_state_starts,
    vector[int32_t]& object_state_lengths,
    vector[int64_t]& seen,
    Py_ssize_t start,
    Py_ssize_t stop,
)
cdef object build_multi_object_state_vector(
    vector[uint8_t]& multi_object_state_bytes,
    vector[int32_t]& multi_object_state_starts,
    vector[int32_t]& multi_object_state_lengths,
    vector[int64_t]& multi_seen,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
)
