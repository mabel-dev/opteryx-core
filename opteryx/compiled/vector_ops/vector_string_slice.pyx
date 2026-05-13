# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t, uint16_t, uint32_t
from libc.stddef cimport size_t
from cpython.array cimport array, clone

from draken.vectors.vector cimport Vector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder, from_packed_dict, _materialize_rle_string, _materialize_dict_string
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence, _materialize_rle_int64, _materialize_dict_int64
from draken.vectors.null_vector cimport NullVector
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenRLEBuffer
from draken.core.buffers cimport DRAKEN_ENCODING_RLE, DRAKEN_ENCODING_DICTIONARY

# ---------------------------------------------------------------------------
# Local helpers
#
# Note: _decode_dict_code lives in _helper_string.pyx and is available at
# the consolidated-module level via the `include` directive in vector_ops.pyx.
# ---------------------------------------------------------------------------

cdef inline Int64Vector _prepare_int_arg(Vector arg):
    """Materialise any Int64Vector encoding to const-or-dense for fast per-row reads.

    All arguments must be vectors — NullVector or Int64Vector. Scalars are not accepted.
    RLE and dict-only encodings are materialised once here; the caller then reads
    _has_const/_const_value or data[i] directly in the tight loop.
    """
    cdef Int64Vector iv
    if isinstance(arg, NullVector):
        return None  # caller checks for None → all rows null
    if not isinstance(arg, Int64Vector):
        raise TypeError(f"integer argument must be an Int64Vector or NullVector, got {type(arg).__name__}")
    iv = <Int64Vector>arg
    if iv._has_const:
        return iv
    if iv._encoding == DRAKEN_ENCODING_RLE:
        return _materialize_rle_int64(iv)
    if iv._encoding == DRAKEN_ENCODING_DICTIONARY and iv.ptr.data == NULL:
        return _materialize_dict_int64(iv)
    return iv  # already dense


cdef inline int64_t _read_int_arg(Int64Vector iv, Py_ssize_t row, bint* is_null) noexcept:
    """Read element-at-row from a const-or-dense Int64Vector prepared by _prepare_int_arg."""
    cdef int64_t* data
    cdef uint8_t* nulls
    is_null[0] = False
    if iv._has_const:
        if iv._const_is_null:
            is_null[0] = True
            return 0
        return iv._const_value
    data = <int64_t*>iv.ptr.data
    nulls = iv.ptr.null_bitmap
    if nulls != NULL and not ((nulls[row >> 3] >> (row & 7)) & 1):
        is_null[0] = True
        return 0
    return data[row]


# ---------------------------------------------------------------------------
# vector_string_slice_left
# ---------------------------------------------------------------------------

cpdef StringVector vector_string_slice_left(StringVector vec, Vector length):
    """
    Slice each string from the left (beginning) up to 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: Int64Vector or NullVector — number of bytes to keep.

    Returns:
        StringVector: sliced strings.
    """
    cdef Int64Vector length_iv = _prepare_int_arg(length)
    cdef bint length_is_null_vec = length_iv is None

    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # ------------------------------------------------------------------
    # Const-encoded string
    # ------------------------------------------------------------------
    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()
        const_val = vec._const_value
        const_len = const_val.length
        for i in range(n):
            length_val = _read_int_arg(length_iv, i, &length_null)
            if length_null:
                builder.append_null()
                continue
            if length_val < 0:
                take = const_len + <Py_ssize_t>length_val
                if take < 0:
                    take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > const_len:
                take = const_len
            builder.append_bytes(<const char*>const_val.data, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        return _slice_left_dict(vec, length_iv, n)

    # ------------------------------------------------------------------
    # RLE-encoded string — materialize to dense then fall through
    # ------------------------------------------------------------------
    cdef StringVector dense_vec
    if vec._encoding == DRAKEN_ENCODING_RLE:
        dense_vec = _materialize_rle_string(vec)
        return _slice_left_dense(dense_vec, length_iv, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _slice_left_dense(vec, length_iv, n)


cdef StringVector _slice_left_dict(StringVector vec, Int64Vector length_iv, Py_ssize_t n):
    """Dict encoding path for slice_left."""
    cdef DrakenVarBuffer* dict_ptr = vec._dict_values
    cdef Py_ssize_t dict_size = dict_ptr.length
    cdef int32_t dict_start, dict_end, dict_len
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef Py_ssize_t j
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* row_nulls = vec._dict_accessor.row_nulls
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder

    # When int arg is const we can operate on dict values only — O(dict_size)
    if length_iv._has_const and not length_iv._const_is_null:
        length_val = length_iv._const_value
        dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
        for j in range(dict_size):
            dict_start = dict_ptr.offsets[j]
            dict_end = dict_ptr.offsets[j + 1]
            dict_len = dict_end - dict_start
            if length_val < 0:
                take = dict_len + <Py_ssize_t>length_val
                if take < 0:
                    take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > dict_len:
                take = dict_len
            dict_builder.append_bytes(<const char*>dict_ptr.data + dict_start, take)
        new_dict_sv = dict_builder.finish()
        return from_packed_dict(
            codes, code_width, n,
            new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
            row_nulls,
        )

    # Varying int arg — per-row dict code lookup (no full materialisation)
    if length_iv._has_const and length_iv._const_is_null:
        # const-null length → all rows null
        builder = StringVectorBuilder.with_estimate(n, 0)
        for i in range(n):
            builder.append_null()
        return builder.finish()

    builder = StringVectorBuilder.with_estimate(n, 8)
    for i in range(n):
        if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        code = _decode_dict_code(codes, code_width, i)
        dict_start = dict_ptr.offsets[code]
        dict_end = dict_ptr.offsets[code + 1]
        dict_len = dict_end - dict_start
        if length_val < 0:
            take = dict_len + <Py_ssize_t>length_val
            if take < 0:
                take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > dict_len:
            take = dict_len
        builder.append_bytes(<const char*>dict_ptr.data + dict_start, take)
    return builder.finish()


cdef StringVector _slice_left_dense(StringVector vec, Int64Vector length_iv, Py_ssize_t n):
    """Dense encoding path for slice_left."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_len = end - start
        if length_val < 0:
            take = row_len + <Py_ssize_t>length_val
            if take < 0:
                take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > row_len:
            take = row_len
        builder.append_bytes(<const char*>ptr.data + start, take)
    return builder.finish()


# ---------------------------------------------------------------------------
# vector_string_slice_right
# ---------------------------------------------------------------------------

cpdef StringVector vector_string_slice_right(StringVector vec, Vector length):
    """
    Slice each string from the right (end) keeping 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: Int64Vector or NullVector — number of bytes to keep from the right.

    Returns:
        StringVector: sliced strings.
    """
    cdef Int64Vector length_iv = _prepare_int_arg(length)
    cdef bint length_is_null_vec = length_iv is None

    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # ------------------------------------------------------------------
    # Const-encoded string
    # ------------------------------------------------------------------
    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()
        const_val = vec._const_value
        const_len = const_val.length
        for i in range(n):
            length_val = _read_int_arg(length_iv, i, &length_null)
            if length_null:
                builder.append_null()
                continue
            if length_val < 0:
                take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > const_len:
                take = const_len
            actual_start = const_len - take
            builder.append_bytes(<const char*>const_val.data + actual_start, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        return _slice_right_dict(vec, length_iv, n)

    # ------------------------------------------------------------------
    # RLE-encoded string — materialize to dense then fall through
    # ------------------------------------------------------------------
    cdef StringVector dense_vec
    if vec._encoding == DRAKEN_ENCODING_RLE:
        dense_vec = _materialize_rle_string(vec)
        return _slice_right_dense(dense_vec, length_iv, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _slice_right_dense(vec, length_iv, n)


cdef StringVector _slice_right_dict(StringVector vec, Int64Vector length_iv, Py_ssize_t n):
    """Dict encoding path for slice_right."""
    cdef DrakenVarBuffer* dict_ptr = vec._dict_values
    cdef Py_ssize_t dict_size = dict_ptr.length
    cdef int32_t dict_start, dict_end, dict_len
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef Py_ssize_t j
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* row_nulls = vec._dict_accessor.row_nulls
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder

    if length_iv._has_const and not length_iv._const_is_null:
        length_val = length_iv._const_value
        dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
        for j in range(dict_size):
            dict_start = dict_ptr.offsets[j]
            dict_end = dict_ptr.offsets[j + 1]
            dict_len = dict_end - dict_start
            if length_val < 0:
                take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > dict_len:
                take = dict_len
            actual_start = dict_len - take
            dict_builder.append_bytes(<const char*>dict_ptr.data + dict_start + actual_start, take)
        new_dict_sv = dict_builder.finish()
        return from_packed_dict(
            codes, code_width, n,
            new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
            row_nulls,
        )

    if length_iv._has_const and length_iv._const_is_null:
        builder = StringVectorBuilder.with_estimate(n, 0)
        for i in range(n):
            builder.append_null()
        return builder.finish()

    builder = StringVectorBuilder.with_estimate(n, 8)
    for i in range(n):
        if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        code = _decode_dict_code(codes, code_width, i)
        dict_start = dict_ptr.offsets[code]
        dict_end = dict_ptr.offsets[code + 1]
        dict_len = dict_end - dict_start
        if length_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > dict_len:
            take = dict_len
        actual_start = dict_len - take
        builder.append_bytes(<const char*>dict_ptr.data + dict_start + actual_start, take)
    return builder.finish()


cdef StringVector _slice_right_dense(StringVector vec, Int64Vector length_iv, Py_ssize_t n):
    """Dense encoding path for slice_right."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t start, end, row_len
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_len = end - start
        if length_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > row_len:
            take = row_len
        actual_start = row_len - take
        builder.append_bytes(<const char*>ptr.data + start + actual_start, take)
    return builder.finish()


# ---------------------------------------------------------------------------
# vector_string_substring
# ---------------------------------------------------------------------------

cpdef StringVector vector_string_substring(StringVector vec, object from_pos, object count):
    """SQL SUBSTRING(string, from_pos, count).

    Position is 1-based. Position 0 is treated as 1. Negative positions count
    from the end. count may be a NullVector to mean "slice to end of string".

    Parameters:
        vec: StringVector of source strings.
        from_pos: Int64Vector or NullVector — 1-based start position.
        count: Int64Vector or NullVector — number of bytes to extract.

    Returns:
        StringVector of extracted substrings. NULL propagates from any input.
    """
    cdef Int64Vector pos_iv = _prepare_int_arg(from_pos)
    cdef Int64Vector cnt_iv = _prepare_int_arg(count)
    cdef bint pos_is_null_vec = pos_iv is None
    cdef bint cnt_is_null_vec = cnt_iv is None

    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef DrakenConstantStringPayload* const_val
    cdef int32_t const_len
    cdef const char* row_data
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    # All rows null if from_pos is a NullVector
    if pos_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # ------------------------------------------------------------------
    # Const-encoded string
    # ------------------------------------------------------------------
    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()
        const_val = vec._const_value
        const_len = const_val.length
        for i in range(n):
            start_val = _read_int_arg(pos_iv, i, &pos_null)
            if pos_null:
                builder.append_null()
                continue
            if cnt_is_null_vec:
                count_val = const_len
            else:
                count_val = _read_int_arg(cnt_iv, i, &cnt_null)
                if cnt_null:
                    builder.append_null()
                    continue

            if start_val > 0:
                s_idx = <Py_ssize_t>(start_val - 1)
            elif start_val < 0:
                s_idx = <Py_ssize_t>(const_len + start_val)
                if s_idx < 0:
                    s_idx = 0
            else:
                s_idx = 0
            if s_idx > const_len:
                s_idx = const_len

            if count_val < 0:
                take = 0
            else:
                take = <Py_ssize_t>count_val
            if s_idx + take > const_len:
                take = const_len - s_idx
            if take < 0:
                take = 0

            builder.append_bytes(<const char*>const_val.data + s_idx, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        return _substring_dict(vec, pos_iv, cnt_iv, cnt_is_null_vec, n)

    # ------------------------------------------------------------------
    # RLE-encoded string — materialize to dense then fall through
    # ------------------------------------------------------------------
    cdef StringVector dense_vec
    if vec._encoding == DRAKEN_ENCODING_RLE:
        dense_vec = _materialize_rle_string(vec)
        return _substring_dense(dense_vec, pos_iv, cnt_iv, cnt_is_null_vec, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _substring_dense(vec, pos_iv, cnt_iv, cnt_is_null_vec, n)


cdef StringVector _substring_dict(
    StringVector vec,
    Int64Vector pos_iv,
    Int64Vector cnt_iv,
    bint cnt_is_null_vec,
    Py_ssize_t n,
):
    """Dict encoding path for substring."""
    cdef DrakenVarBuffer* dict_ptr = vec._dict_values
    cdef Py_ssize_t dict_size = dict_ptr.length
    cdef int32_t dict_start, dict_end, dict_len
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef Py_ssize_t j
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* row_nulls = vec._dict_accessor.row_nulls
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder

    # Const pos + const (or null) count → operate on dict values only — O(dict_size)
    if pos_iv._has_const and not pos_iv._const_is_null:
        if cnt_is_null_vec or (cnt_iv._has_const and not cnt_iv._const_is_null):
            start_val = pos_iv._const_value
            dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
            for j in range(dict_size):
                dict_start = dict_ptr.offsets[j]
                dict_end = dict_ptr.offsets[j + 1]
                dict_len = dict_end - dict_start

                if start_val > 0:
                    s_idx = <Py_ssize_t>(start_val - 1)
                elif start_val < 0:
                    s_idx = <Py_ssize_t>(dict_len + start_val)
                    if s_idx < 0:
                        s_idx = 0
                else:
                    s_idx = 0
                if s_idx > dict_len:
                    s_idx = dict_len

                if cnt_is_null_vec:
                    count_val = dict_len
                else:
                    count_val = cnt_iv._const_value

                if count_val < 0:
                    take = 0
                else:
                    take = <Py_ssize_t>count_val
                if s_idx + take > dict_len:
                    take = dict_len - s_idx
                if take < 0:
                    take = 0

                dict_builder.append_bytes(<const char*>dict_ptr.data + dict_start + s_idx, take)
            new_dict_sv = dict_builder.finish()
            return from_packed_dict(
                codes, code_width, n,
                new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
                row_nulls,
            )

    # Const-null pos → all rows null
    if pos_iv._has_const and pos_iv._const_is_null:
        builder = StringVectorBuilder.with_estimate(n, 0)
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # Const-null count → all rows null (count=NULL propagates)
    if not cnt_is_null_vec and cnt_iv is not None and cnt_iv._has_const and cnt_iv._const_is_null:
        builder = StringVectorBuilder.with_estimate(n, 0)
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # Varying args — per-row dict code lookup (no full materialisation)
    builder = StringVectorBuilder.with_estimate(n, 8)
    for i in range(n):
        if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start_val = _read_int_arg(pos_iv, i, &pos_null)
        if pos_null:
            builder.append_null()
            continue

        code = _decode_dict_code(codes, code_width, i)
        dict_start = dict_ptr.offsets[code]
        dict_end = dict_ptr.offsets[code + 1]
        dict_len = dict_end - dict_start

        if cnt_is_null_vec:
            count_val = dict_len
        else:
            count_val = _read_int_arg(cnt_iv, i, &cnt_null)
            if cnt_null:
                builder.append_null()
                continue

        if start_val > 0:
            s_idx = <Py_ssize_t>(start_val - 1)
        elif start_val < 0:
            s_idx = <Py_ssize_t>(dict_len + start_val)
            if s_idx < 0:
                s_idx = 0
        else:
            s_idx = 0
        if s_idx > dict_len:
            s_idx = dict_len

        if count_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>count_val
        if s_idx + take > dict_len:
            take = dict_len - s_idx
        if take < 0:
            take = 0

        builder.append_bytes(<const char*>dict_ptr.data + dict_start + s_idx, take)
    return builder.finish()


cdef StringVector _substring_dense(
    StringVector vec,
    Int64Vector pos_iv,
    Int64Vector cnt_iv,
    bint cnt_is_null_vec,
    Py_ssize_t n,
):
    """Dense encoding path for substring."""
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t row_off, row_len
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef const char* row_data
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start_val = _read_int_arg(pos_iv, i, &pos_null)
        if pos_null:
            builder.append_null()
            continue

        row_off = ptr.offsets[i]
        row_len = ptr.offsets[i + 1] - row_off
        row_data = <const char*>ptr.data + row_off

        if cnt_is_null_vec:
            count_val = row_len
        else:
            count_val = _read_int_arg(cnt_iv, i, &cnt_null)
            if cnt_null:
                builder.append_null()
                continue

        if start_val > 0:
            s_idx = <Py_ssize_t>(start_val - 1)
        elif start_val < 0:
            s_idx = <Py_ssize_t>(row_len + start_val)
            if s_idx < 0:
                s_idx = 0
        else:
            s_idx = 0
        if s_idx > row_len:
            s_idx = row_len

        if count_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>count_val
        if s_idx + take > row_len:
            take = row_len - s_idx
        if take < 0:
            take = 0

        builder.append_bytes(row_data + s_idx, take)
    return builder.finish()
