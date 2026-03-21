# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# Finalize-side helpers for grouped aggregation.
#
# These kernels rebuild output vectors from the native Carchar group-state
# store after execution has finished. The coordinator and state engine use
# them for SQL GROUP BY results, plus aggregate surfaces like COUNT, SUM,
# MIN, MAX, AVG, ANY_VALUE, and ARRAY_AGG-style object outputs.

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.compiled.aggregations.key_codec cimport decode_multi_key_record
from opteryx.compiled.aggregations.key_codec cimport decode_single_encoded_key_record
from opteryx.compiled.aggregations.key_codec cimport decode_single_fixed_key_record
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.string_vector cimport StringVectorBuilder
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.vectors.vector cimport Vector


cdef int KEY_MULTI_FIXED_INT = 1
cdef int KEY_MULTI_FIXED_DATE32 = 2
cdef int KEY_MULTI_FIXED_TIME32 = 3
cdef int KEY_MULTI_FIXED_TIME64 = 4
cdef int KEY_MULTI_FIXED_TIMESTAMP64 = 5
cdef int KEY_MULTI_ENCODED_STRING = 6

cdef int AGG_UNSUPPORTED = 0
cdef int AGG_COUNT_STAR = 1
cdef int AGG_COUNT_VALUE = 2
cdef int AGG_SUM = 3
cdef int AGG_MIN = 4
cdef int AGG_MAX = 5
cdef int AGG_AVG = 6
cdef int AGG_COUNT_DISTINCT = 7
cdef int AGG_HASH_ONE = 8

cdef int VALUE_NONE = 0
cdef int VALUE_INT64 = 1
cdef int VALUE_FLOAT64 = 2
cdef int VALUE_OBJECT = 3
cdef int VALUE_DICT_INT64 = 4
cdef int VALUE_DICT_FLOAT64 = 5


cdef inline bint _is_multi_fixed_kind(int64_t key_kind) noexcept:
    return (
        key_kind == KEY_MULTI_FIXED_INT
        or key_kind == KEY_MULTI_FIXED_DATE32
        or key_kind == KEY_MULTI_FIXED_TIME32
        or key_kind == KEY_MULTI_FIXED_TIME64
        or key_kind == KEY_MULTI_FIXED_TIMESTAMP64
    )


cdef inline uint8_t* _alloc_valid_bitmap(Py_ssize_t length) except NULL:
    cdef size_t nbytes = <size_t>((length + 7) // 8)
    cdef uint8_t* bitmap = <uint8_t*> malloc(nbytes)
    if bitmap == NULL:
        return NULL
    memset(bitmap, 0, nbytes)
    return bitmap


cdef inline void _bitmap_set_valid(uint8_t* bitmap, Py_ssize_t index) noexcept:
    bitmap[index >> 3] |= <uint8_t>(1 << (index & 7))


# Build an object-typed vector for SQL surfaces that emit Python-backed values.
# This is used for object aggregates such as ANY_VALUE/ARRAY_AGG-style outputs,
# and for object-key GROUP BY results when we want a StringVector-compatible
# result without re-encoding everything as a generic Arrow sequence.
cdef object build_native_object_vector(list values):
    cdef Py_ssize_t i
    cdef Py_ssize_t total_bytes = 0
    cdef object value
    cdef bytes encoded
    cdef StringVectorBuilder builder

    for i in range(len(values)):
        value = values[i]
        if value is None:
            continue
        if isinstance(value, bytes):
            total_bytes += len(value)
            continue
        if isinstance(value, str):
            total_bytes += len((<str> value).encode("utf-8"))
            continue
        return vector_from_sequence(values)

    builder = StringVectorBuilder.with_counts(len(values), total_bytes)
    for i in range(len(values)):
        value = values[i]
        if value is None:
            builder.append_null()
        elif isinstance(value, bytes):
            builder.append(<bytes> value)
        else:
            encoded = (<str> value).encode("utf-8")
            builder.append(encoded)
    return builder.finish()


# Rebuild a single GROUP BY key column when the engine stored one fixed-width
# key in native payload form. This is the finalize path for SQL like:
#   SELECT k, COUNT(*) FROM t GROUP BY k
# when `k` is an integer, date, time, or timestamp-like key.
cdef object build_single_fixed_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    vector[int64_t]& group_key_values,
    vector[int64_t]& group_key_valid,
    int64_t single_key_kind,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef object key_vec
    cdef Int64Vector key_vec_i64
    cdef int64_t* key_data_i64 = NULL
    cdef uint8_t* key_nulls = NULL
    cdef bint needs_key_nulls = False
    cdef int64_t valid_flag
    cdef int64_t key_value

    if <Py_ssize_t> key_payload_offsets.size() >= stop + 1:
        for row_idx in range(start, stop):
            if not decode_single_fixed_key_record(
                key_payload_bytes, key_payload_offsets, row_idx, &key_value, &valid_flag
            ):
                raise RuntimeError("failed to decode fixed key payload")
            if valid_flag == 0:
                needs_key_nulls = True
                break
    else:
        if <Py_ssize_t> group_key_values.size() < stop:
            raise RuntimeError("single-key fixed value store shorter than finalize range")
        if <Py_ssize_t> group_key_valid.size() < stop:
            raise RuntimeError("single-key fixed validity store shorter than finalize range")
        for row_idx in range(start, stop):
            if group_key_valid[row_idx] == 0:
                needs_key_nulls = True
                break

    key_vec_i64 = Int64Vector(length)
    key_vec = key_vec_i64
    key_data_i64 = <int64_t*> key_vec_i64.ptr.data

    if needs_key_nulls:
        key_nulls = _alloc_valid_bitmap(length)
        key_vec_i64.ptr.null_bitmap = key_nulls

    for row_idx in range(length):
        if <Py_ssize_t> key_payload_offsets.size() >= start + row_idx + 2:
            if not decode_single_fixed_key_record(
                key_payload_bytes,
                key_payload_offsets,
                start + row_idx,
                &key_value,
                &valid_flag,
            ):
                raise RuntimeError("failed to decode fixed key payload")
        else:
            valid_flag = 1 if group_key_valid[start + row_idx] != 0 else 0
            key_value = group_key_values[start + row_idx]
        key_data_i64[row_idx] = key_value
        if key_nulls != NULL and valid_flag != 0:
            _bitmap_set_valid(key_nulls, row_idx)

    return key_vec


# Rebuild multi-column GROUP BY keys from the native payload store.
# This covers SQL GROUP BY shapes like:
#   SELECT a, b, COUNT(*) FROM t GROUP BY a, b
# where the key columns can mix fixed-width and encoded/string-backed values.
cdef list build_payload_multi_key_vectors(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    list multi_group_key_kinds,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t key_count = len(multi_group_key_kinds)
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t key_idx
    cdef int64_t key_kind
    cdef Py_ssize_t fixed_idx
    cdef Py_ssize_t encoded_idx
    cdef list vectors = [None] * key_count
    cdef list builders = [None] * key_count
    cdef list total_bytes = [0] * key_count
    cdef list needs_nulls = [False] * key_count
    cdef list fixed_ptrs = [0] * key_count
    cdef list fixed_null_ptrs = [0] * key_count
    cdef object key_vec
    cdef Int64Vector key_vec_i64
    cdef Date32Vector key_vec_d32
    cdef TimeVector key_vec_t32
    cdef TimeVector key_vec_t64
    cdef TimestampVector key_vec_ts
    cdef StringVectorBuilder builder
    cdef uint8_t* key_nulls = NULL
    cdef vector[int64_t] fixed_values
    cdef vector[int64_t] fixed_valids
    cdef vector[string] encoded_values
    cdef vector[int64_t] encoded_valids

    if <Py_ssize_t> key_payload_offsets.size() < stop + 1:
        raise RuntimeError("encoded key payload offsets shorter than finalize range")

    for row_idx in range(start, stop):
        if not decode_multi_key_record(
            key_payload_bytes,
            key_payload_offsets,
            row_idx,
            fixed_values,
            fixed_valids,
            encoded_values,
            encoded_valids,
        ):
            raise RuntimeError("failed to decode multi-key payload")
        fixed_idx = 0
        encoded_idx = 0
        for key_idx in range(key_count):
            key_kind = <int64_t> multi_group_key_kinds[key_idx]
            if _is_multi_fixed_kind(key_kind):
                if fixed_idx >= <Py_ssize_t> fixed_valids.size():
                    raise RuntimeError("decoded fixed key payload shorter than key schema")
                if fixed_valids[fixed_idx] == 0:
                    needs_nulls[key_idx] = True
                fixed_idx += 1
            else:
                if encoded_idx >= <Py_ssize_t> encoded_valids.size():
                    raise RuntimeError("decoded encoded key payload shorter than key schema")
                if encoded_valids[encoded_idx] == 0:
                    needs_nulls[key_idx] = True
                else:
                    total_bytes[key_idx] = total_bytes[key_idx] + encoded_values[encoded_idx].size()
                encoded_idx += 1

    for key_idx in range(key_count):
        key_kind = <int64_t> multi_group_key_kinds[key_idx]
        if _is_multi_fixed_kind(key_kind):
            if key_kind == KEY_MULTI_FIXED_DATE32:
                key_vec_d32 = Date32Vector(length)
                key_vec = key_vec_d32
                fixed_ptrs[key_idx] = <size_t> key_vec_d32.ptr.data
                if needs_nulls[key_idx]:
                    key_nulls = _alloc_valid_bitmap(length)
                    key_vec_d32.ptr.null_bitmap = key_nulls
                    fixed_null_ptrs[key_idx] = <size_t> key_nulls
            elif key_kind == KEY_MULTI_FIXED_TIME32:
                key_vec_t32 = TimeVector(length, is_time64=False)
                key_vec = key_vec_t32
                fixed_ptrs[key_idx] = <size_t> key_vec_t32.ptr.data
                if needs_nulls[key_idx]:
                    key_nulls = _alloc_valid_bitmap(length)
                    key_vec_t32.ptr.null_bitmap = key_nulls
                    fixed_null_ptrs[key_idx] = <size_t> key_nulls
            elif key_kind == KEY_MULTI_FIXED_TIME64:
                key_vec_t64 = TimeVector(length, is_time64=True)
                key_vec = key_vec_t64
                fixed_ptrs[key_idx] = <size_t> key_vec_t64.ptr.data
                if needs_nulls[key_idx]:
                    key_nulls = _alloc_valid_bitmap(length)
                    key_vec_t64.ptr.null_bitmap = key_nulls
                    fixed_null_ptrs[key_idx] = <size_t> key_nulls
            elif key_kind == KEY_MULTI_FIXED_TIMESTAMP64:
                key_vec_ts = TimestampVector(length)
                key_vec = key_vec_ts
                fixed_ptrs[key_idx] = <size_t> key_vec_ts.ptr.data
                if needs_nulls[key_idx]:
                    key_nulls = _alloc_valid_bitmap(length)
                    key_vec_ts.ptr.null_bitmap = key_nulls
                    fixed_null_ptrs[key_idx] = <size_t> key_nulls
            else:
                key_vec_i64 = Int64Vector(length)
                key_vec = key_vec_i64
                fixed_ptrs[key_idx] = <size_t> key_vec_i64.ptr.data
                if needs_nulls[key_idx]:
                    key_nulls = _alloc_valid_bitmap(length)
                    key_vec_i64.ptr.null_bitmap = key_nulls
                    fixed_null_ptrs[key_idx] = <size_t> key_nulls
            vectors[key_idx] = key_vec
        else:
            builders[key_idx] = StringVectorBuilder.with_counts(length, total_bytes[key_idx])

    for row_idx in range(length):
        if not decode_multi_key_record(
            key_payload_bytes,
            key_payload_offsets,
            start + row_idx,
            fixed_values,
            fixed_valids,
            encoded_values,
            encoded_valids,
        ):
            raise RuntimeError("failed to decode multi-key payload")
        fixed_idx = 0
        encoded_idx = 0
        for key_idx in range(key_count):
            key_kind = <int64_t> multi_group_key_kinds[key_idx]
            if _is_multi_fixed_kind(key_kind):
                if fixed_idx >= <Py_ssize_t> fixed_values.size():
                    raise RuntimeError("decoded fixed key payload shorter than key schema")
                if key_kind == KEY_MULTI_FIXED_DATE32 or key_kind == KEY_MULTI_FIXED_TIME32:
                    (<int32_t*> <size_t> fixed_ptrs[key_idx])[row_idx] = <int32_t> fixed_values[fixed_idx]
                else:
                    (<int64_t*> <size_t> fixed_ptrs[key_idx])[row_idx] = fixed_values[fixed_idx]
                if fixed_null_ptrs[key_idx] != 0 and fixed_valids[fixed_idx] != 0:
                    _bitmap_set_valid(<uint8_t*> <size_t> fixed_null_ptrs[key_idx], row_idx)
                fixed_idx += 1
            else:
                if encoded_idx >= <Py_ssize_t> encoded_values.size():
                    raise RuntimeError("decoded encoded key payload shorter than key schema")
                builder = builders[key_idx]
                if encoded_valids[encoded_idx] == 0:
                    builder.append_null()
                elif encoded_values[encoded_idx].size() > 0:
                    builder.append_bytes(
                        encoded_values[encoded_idx].data(),
                        encoded_values[encoded_idx].size(),
                    )
                else:
                    builder.append_bytes(NULL, 0)
                encoded_idx += 1

    for key_idx in range(key_count):
        if not _is_multi_fixed_kind(<int64_t> multi_group_key_kinds[key_idx]):
            vectors[key_idx] = builders[key_idx].finish()

    return vectors


# Rebuild a single GROUP BY key when the stored form is encoded as bytes.
# This is the finalize path for string-like keys and other encoded key
# surfaces that should come back as a StringVector.
cdef object build_encoded_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t total_bytes = 0
    cdef int64_t valid_flag
    cdef StringVectorBuilder builder
    cdef string raw_value

    if <Py_ssize_t> key_payload_offsets.size() < stop + 1:
        raise RuntimeError("encoded key payload offsets shorter than finalize range")

    for row_idx in range(start, stop):
        if not decode_single_encoded_key_record(
            key_payload_bytes, key_payload_offsets, row_idx, raw_value, &valid_flag
        ):
            raise RuntimeError("failed to decode encoded key payload")
        if valid_flag != 0:
            total_bytes += raw_value.size()

    builder = StringVectorBuilder.with_counts(length, total_bytes)
    for row_idx in range(start, stop):
        if not decode_single_encoded_key_record(
            key_payload_bytes, key_payload_offsets, row_idx, raw_value, &valid_flag
        ):
            raise RuntimeError("failed to decode encoded key payload")
        if valid_flag == 0:
            builder.append_null()
            continue
        if raw_value.size() > 0:
            builder.append_bytes(raw_value.data(), raw_value.size())
        else:
            builder.append_bytes(NULL, 0)
    return builder.finish()


# Rebuild one column out of a multi-column GROUP BY where that key was stored
# in encoded byte form.
cdef object build_multi_encoded_key_vector(
    vector[vector[uint8_t]]& multi_encoded_key_bytes,
    vector[vector[int32_t]]& multi_encoded_key_offsets,
    vector[vector[int64_t]]& multi_encoded_key_valid,
    Py_ssize_t key_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t total_bytes = 0
    cdef int32_t start_offset
    cdef int32_t stop_offset
    cdef StringVectorBuilder builder

    if <Py_ssize_t> multi_encoded_key_valid[key_idx].size() < stop:
        raise RuntimeError("encoded multi-key validity store shorter than finalize range")
    if <Py_ssize_t> multi_encoded_key_offsets[key_idx].size() < stop + 1:
        raise RuntimeError("encoded multi-key offset store shorter than finalize range")

    for row_idx in range(start, stop):
        if multi_encoded_key_valid[key_idx][row_idx] != 0:
            start_offset = multi_encoded_key_offsets[key_idx][row_idx]
            stop_offset = multi_encoded_key_offsets[key_idx][row_idx + 1]
            total_bytes += stop_offset - start_offset

    builder = StringVectorBuilder.with_counts(length, total_bytes)
    for row_idx in range(start, stop):
        if multi_encoded_key_valid[key_idx][row_idx] == 0:
            builder.append_null()
            continue
        start_offset = multi_encoded_key_offsets[key_idx][row_idx]
        stop_offset = multi_encoded_key_offsets[key_idx][row_idx + 1]
        if stop_offset > start_offset:
            builder.append_bytes(
                <const char*> &multi_encoded_key_bytes[key_idx][start_offset],
                stop_offset - start_offset,
            )
        else:
            builder.append_bytes(NULL, 0)
    return builder.finish()


# Rebuild object-valued aggregate output for a single-aggregate GROUP BY.
# This is used for SQL surfaces like MIN/MAX/ANY_VALUE when the aggregate
# state is stored as Python objects instead of native fixed-width scalars.
cdef object build_object_state_vector(
    vector[uint8_t]& object_state_bytes,
    vector[int32_t]& object_state_starts,
    vector[int32_t]& object_state_lengths,
    vector[int64_t]& seen,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t state_index
    cdef StringVectorBuilder builder

    for row_idx in range(length):
        state_index = start + row_idx
        if seen[state_index] != 0:
            total_bytes += object_state_lengths[state_index]

    builder = StringVectorBuilder.with_counts(length, total_bytes)
    for row_idx in range(length):
        state_index = start + row_idx
        if seen[state_index] == 0:
            builder.append_null()
        else:
            builder.append_bytes(
                <const char*> &object_state_bytes[object_state_starts[state_index]],
                object_state_lengths[state_index],
        )
    return builder.finish()


# Decide how to rebuild GROUP BY key vectors during finalize.
# This is the branch family that keeps the engine from having to choose between
# single-key fixed paths, single-key encoded paths, and multi-key object paths
# inline.
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
):
    cdef list vectors
    cdef object built_vec

    if len(multi_group_key_kinds) <= 1 and not multi_key_object_mode:
        if single_key_kind == KEY_MULTI_ENCODED_STRING:
            built_vec = build_encoded_key_vector(
                key_payload_bytes,
                key_payload_offsets,
                start,
                stop,
            )
        else:
            built_vec = build_single_fixed_key_vector(
                key_payload_bytes,
                key_payload_offsets,
                group_key_values,
                group_key_valid,
                single_key_kind,
                start,
                stop,
            )
        return [built_vec]

    vectors = build_payload_multi_key_vectors(
        key_payload_bytes,
        key_payload_offsets,
        multi_group_key_kinds,
        start,
        stop,
    )
    return vectors


# Decide how to rebuild a single GROUP BY key vector during finalize.
# This is the branch family for SQL like:
#   SELECT k, COUNT(*) FROM t GROUP BY k
# where the engine may have stored the key as encoded bytes, a fixed-width
# payload, or a direct native int64 fallback.
cdef object build_finalize_single_key_vector(
    vector[uint8_t]& key_payload_bytes,
    vector[int64_t]& key_payload_offsets,
    vector[int64_t]& group_key_values,
    vector[int64_t]& group_key_valid,
    int64_t single_key_kind,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef object key_vec
    cdef Int64Vector key_vec_i64
    cdef int64_t* key_data_i64 = NULL
    cdef uint8_t* key_nulls = NULL
    cdef bint needs_key_nulls = False

    if single_key_kind == KEY_MULTI_ENCODED_STRING:
        return build_encoded_key_vector(
            key_payload_bytes,
            key_payload_offsets,
            start,
            stop,
        )
    if single_key_kind != KEY_MULTI_FIXED_INT or <Py_ssize_t> key_payload_offsets.size() >= stop + 1:
        return build_single_fixed_key_vector(
            key_payload_bytes,
            key_payload_offsets,
            group_key_values,
            group_key_valid,
            single_key_kind,
            start,
            stop,
        )

    for row_idx in range(start, stop):
        if group_key_valid[row_idx] == 0:
            needs_key_nulls = True
            break

    key_vec_i64 = Int64Vector(length)
    key_vec = key_vec_i64
    key_data_i64 = <int64_t*> key_vec_i64.ptr.data
    if needs_key_nulls:
        key_nulls = _alloc_valid_bitmap(length)
        key_vec_i64.ptr.null_bitmap = key_nulls

    for row_idx in range(length):
        key_data_i64[row_idx] = group_key_values[start + row_idx]
        if key_nulls != NULL and group_key_valid[start + row_idx] != 0:
            _bitmap_set_valid(key_nulls, row_idx)

    return key_vec


# Rebuild object-valued aggregate output for finalize.
# This covers SQL surfaces like MIN/MAX/ANY_VALUE when the aggregate state is
# stored as Python objects instead of native fixed-width scalars.
cdef object build_finalize_object_aggregate_vector(
    vector[uint8_t]& object_state_bytes,
    vector[int32_t]& object_state_starts,
    vector[int32_t]& object_state_lengths,
    vector[int64_t]& seen,
    list object_state,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t state_index
    cdef list agg_objects

    if object_state_lengths.size() == seen.size():
        return build_object_state_vector(
            object_state_bytes,
            object_state_starts,
            object_state_lengths,
            seen,
            start,
            stop,
        )

    agg_objects = []
    for row_idx in range(length):
        state_index = start + row_idx
        if seen[state_index] == 0:
            agg_objects.append(None)
        else:
            agg_objects.append(object_state[state_index])
    return build_native_object_vector(agg_objects)


# Same as above, but for the flattened multi-aggregate state buffers.
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
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t state_index
    cdef Py_ssize_t offset
    cdef list agg_objects

    if multi_object_state_lengths.size() == multi_seen.size():
        return build_multi_object_state_vector(
            multi_object_state_bytes,
            multi_object_state_starts,
            multi_object_state_lengths,
            multi_seen,
            multi_agg_count,
            agg_idx,
            start,
            stop,
        )

    agg_objects = []
    for row_idx in range(length):
        state_index = start + row_idx
        offset = state_index * multi_agg_count + agg_idx
        if multi_seen[offset] == 0:
            agg_objects.append(None)
        else:
            agg_objects.append(multi_object_state[offset])
    return build_native_object_vector(agg_objects)


# Build a scalar aggregate output vector for a single-aggregate GROUP BY.
# This covers SQL surfaces like COUNT, SUM, MIN, MAX, and AVG when the result
# is represented as a native fixed-width vector rather than Python objects.
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
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t state_index
    cdef bint output_is_float = value_kind == VALUE_FLOAT64 or value_kind == VALUE_DICT_FLOAT64
    cdef bint needs_nulls = False
    cdef Int64Vector agg_i64
    cdef Float64Vector agg_f64
    cdef int64_t* agg_i64_data
    cdef double* agg_f64_data
    cdef uint8_t* agg_nulls = NULL

    if agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
        for row_idx in range(length):
            state_index = start + row_idx
            if seen[state_index] == 0:
                needs_nulls = True
                break
    elif agg_mode == AGG_AVG:
        for row_idx in range(length):
            state_index = start + row_idx
            if avg_counts[state_index] == 0:
                needs_nulls = True
                break

    if output_is_float or agg_mode == AGG_AVG:
        agg_f64 = Float64Vector(length)
        agg_f64_data = <double*> agg_f64.ptr.data
        if needs_nulls:
            agg_nulls = _alloc_valid_bitmap(length)
            agg_f64.ptr.null_bitmap = agg_nulls

        for row_idx in range(length):
            state_index = start + row_idx
            if agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE, AGG_COUNT_DISTINCT):
                agg_f64_data[row_idx] = <double> counts[state_index]
                if agg_nulls != NULL:
                    _bitmap_set_valid(agg_nulls, row_idx)
            elif agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
                agg_f64_data[row_idx] = f64_state[state_index]
                if agg_nulls != NULL and seen[state_index] != 0:
                    _bitmap_set_valid(agg_nulls, row_idx)
            else:
                if avg_counts[state_index] == 0:
                    agg_f64_data[row_idx] = 0.0
                else:
                    agg_f64_data[row_idx] = avg_sums[state_index] / avg_counts[state_index]
                if agg_nulls != NULL and avg_counts[state_index] != 0:
                    _bitmap_set_valid(agg_nulls, row_idx)
        return agg_f64

    agg_i64 = Int64Vector(length)
    agg_i64_data = <int64_t*> agg_i64.ptr.data
    if needs_nulls:
        agg_nulls = _alloc_valid_bitmap(length)
        agg_i64.ptr.null_bitmap = agg_nulls

    for row_idx in range(length):
        state_index = start + row_idx
        if agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE, AGG_COUNT_DISTINCT):
            agg_i64_data[row_idx] = counts[state_index]
            if agg_nulls != NULL:
                _bitmap_set_valid(agg_nulls, row_idx)
        else:
            agg_i64_data[row_idx] = i64_state[state_index]
            if agg_nulls != NULL and seen[state_index] != 0:
                _bitmap_set_valid(agg_nulls, row_idx)

    return agg_i64


# Same as above, but for the flattened multi-aggregate state buffers.
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
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t state_index
    cdef Py_ssize_t offset
    cdef bint needs_nulls = False
    cdef Int64Vector agg_i64
    cdef Float64Vector agg_f64
    cdef int64_t* agg_i64_data
    cdef double* agg_f64_data
    cdef uint8_t* agg_nulls = NULL

    if agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
        for row_idx in range(length):
            state_index = start + row_idx
            offset = state_index * multi_agg_count + agg_idx
            if multi_seen[offset] == 0:
                needs_nulls = True
                break
    elif agg_mode == AGG_AVG:
        for row_idx in range(length):
            state_index = start + row_idx
            offset = state_index * multi_agg_count + agg_idx
            if multi_avg_counts[offset] == 0:
                needs_nulls = True
                break

    if output_is_float or agg_mode == AGG_AVG:
        agg_f64 = Float64Vector(length)
        agg_f64_data = <double*> agg_f64.ptr.data
        if needs_nulls:
            agg_nulls = _alloc_valid_bitmap(length)
            agg_f64.ptr.null_bitmap = agg_nulls

        for row_idx in range(length):
            state_index = start + row_idx
            offset = state_index * multi_agg_count + agg_idx
            if agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE, AGG_COUNT_DISTINCT):
                agg_f64_data[row_idx] = <double> multi_counts[offset]
                if agg_nulls != NULL:
                    _bitmap_set_valid(agg_nulls, row_idx)
            elif agg_mode in (AGG_SUM, AGG_MIN, AGG_MAX):
                agg_f64_data[row_idx] = multi_f64_state[offset]
                if agg_nulls != NULL and multi_seen[offset] != 0:
                    _bitmap_set_valid(agg_nulls, row_idx)
            else:
                if multi_avg_counts[offset] == 0:
                    agg_f64_data[row_idx] = 0.0
                else:
                    agg_f64_data[row_idx] = multi_avg_sums[offset] / multi_avg_counts[offset]
                if agg_nulls != NULL and multi_avg_counts[offset] != 0:
                    _bitmap_set_valid(agg_nulls, row_idx)
        return agg_f64

    agg_i64 = Int64Vector(length)
    agg_i64_data = <int64_t*> agg_i64.ptr.data
    if needs_nulls:
        agg_nulls = _alloc_valid_bitmap(length)
        agg_i64.ptr.null_bitmap = agg_nulls

    for row_idx in range(length):
        state_index = start + row_idx
        offset = state_index * multi_agg_count + agg_idx
        if agg_mode in (AGG_COUNT_STAR, AGG_COUNT_VALUE, AGG_COUNT_DISTINCT):
            agg_i64_data[row_idx] = multi_counts[offset]
            if agg_nulls != NULL:
                _bitmap_set_valid(agg_nulls, row_idx)
        else:
            agg_i64_data[row_idx] = multi_i64_state[offset]
            if agg_nulls != NULL and multi_seen[offset] != 0:
                _bitmap_set_valid(agg_nulls, row_idx)

    return agg_i64


# Build all aggregate output vectors for a multi-aggregate GROUP BY in one pass.
# The coordinator and engine still decide key-vector shape, but this helper owns
# the repeated aggregate-state to output-vector mapping for SQL surfaces like
# COUNT, SUM, MIN, MAX, AVG, and object-valued MIN/MAX variants.
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
):
    cdef Py_ssize_t agg_idx
    cdef int64_t agg_mode
    cdef int64_t agg_value_kind
    cdef list vectors = []

    for agg_idx in range(multi_agg_count):
        agg_mode = multi_agg_modes[agg_idx]
        agg_value_kind = multi_value_kinds[agg_idx]
        if agg_value_kind == VALUE_OBJECT and agg_mode in (AGG_MIN, AGG_MAX):
            vectors.append(
                build_finalize_multi_object_aggregate_vector(
                    multi_object_state_bytes,
                    multi_object_state_starts,
                    multi_object_state_lengths,
                    multi_seen,
                    multi_object_state,
                    multi_agg_count,
                    agg_idx,
                    start,
                    stop,
                )
            )
            continue
        vectors.append(
            build_finalize_multi_scalar_aggregate_vector(
                agg_mode,
                agg_value_kind,
                agg_value_kind == VALUE_FLOAT64 or agg_value_kind == VALUE_DICT_FLOAT64,
                multi_counts,
                multi_i64_state,
                multi_f64_state,
                multi_seen,
                multi_avg_sums,
                multi_avg_counts,
                multi_agg_count,
                agg_idx,
                start,
                stop,
            )
        )

    return vectors


# Rebuild object-valued aggregate output for multi-aggregate GROUP BY.
# This is the same finalize path as above, but indexed by aggregate position
# when the query has multiple aggregate expressions.
cdef object build_multi_object_state_vector(
    vector[uint8_t]& multi_object_state_bytes,
    vector[int32_t]& multi_object_state_starts,
    vector[int32_t]& multi_object_state_lengths,
    vector[int64_t]& multi_seen,
    Py_ssize_t multi_agg_count,
    Py_ssize_t agg_idx,
    Py_ssize_t start,
    Py_ssize_t stop,
):
    cdef Py_ssize_t row_idx
    cdef Py_ssize_t length = stop - start
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t state_index
    cdef Py_ssize_t offset
    cdef StringVectorBuilder builder

    for row_idx in range(length):
        state_index = start + row_idx
        offset = state_index * multi_agg_count + agg_idx
        if multi_seen[offset] != 0:
            total_bytes += multi_object_state_lengths[offset]

    builder = StringVectorBuilder.with_counts(length, total_bytes)
    for row_idx in range(length):
        state_index = start + row_idx
        offset = state_index * multi_agg_count + agg_idx
        if multi_seen[offset] == 0:
            builder.append_null()
        else:
            builder.append_bytes(
                <const char*> &multi_object_state_bytes[multi_object_state_starts[offset]],
                multi_object_state_lengths[offset],
            )
    return builder.finish()


# Build the constant-key finalize result pair without keeping the branching
# and vector-construction logic in the engine.
cdef tuple build_constant_groupby_vectors(
    object agg_value,
    object key_value,
    bint agg_is_object,
    bint use_object_keys,
):
    cdef object agg_vec
    cdef object key_vec

    agg_vec = build_native_object_vector([agg_value]) if agg_is_object else vector_from_sequence([agg_value])
    key_vec = build_native_object_vector([key_value]) if use_object_keys else vector_from_sequence([key_value])
    return agg_vec, key_vec
