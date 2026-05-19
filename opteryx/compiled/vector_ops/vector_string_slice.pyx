# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t
from libc.stddef cimport size_t
from cpython.array cimport array, clone

from draken.vectors.vector cimport Vector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder, from_packed_dict, _materialize_dict_string
from draken.vectors.integer64_vector cimport Integer64Vector, from_sequence as int64_from_sequence, _materialize_dict_int64
from draken.vectors.null_vector cimport NullVector
from draken.core.buffers cimport DrakenVector, DrakenVarBuffer, DrakenConstantStringPayload, DrakenGermanArena, GermanString, gs_length, gs_data

# ---------------------------------------------------------------------------
# Local helpers
#
# Note: _decode_dict_code lives in _helper_string.pyx and is available at
# the consolidated-module level via the `include` directive in vector_ops.pyx.
# ---------------------------------------------------------------------------

cdef inline Integer64Vector _prepare_int_arg(object arg, Py_ssize_t row_count):
    """Materialise any Integer64Vector encoding to const-or-dense for fast per-row reads.

    Accepts NullVector or Integer64Vector. Python int scalars are wrapped as a
    constant Integer64Vector of the caller's row count — the common "slice by a
    literal length" path emits a bare int rather than building a constant vector
    at every call site.

    RLE and dict-only encodings are materialised once here; the caller then reads
    unified().data_length / unified().validity or data[i] directly in the tight loop.
    """
    cdef Integer64Vector iv
    if isinstance(arg, NullVector):
        return None  # caller checks for None → all rows null
    if isinstance(arg, int):
        # Wrap a literal scalar as a constant Integer64Vector. The string-slice
        # kernels' "const arg" fast path (uv.data_length == 1) takes over.
        return Integer64Vector.from_constant(<int64_t>arg, row_count)
    if not isinstance(arg, Integer64Vector):
        raise TypeError(f"integer argument must be an Integer64Vector, NullVector or int, got {type(arg).__name__}")
    iv = <Integer64Vector>arg
    if iv._dict_values != NULL:
        return _materialize_dict_int64(iv)
    return iv


cdef inline int64_t _read_int_arg(Integer64Vector iv, Py_ssize_t row, bint* is_null) noexcept:
    """Read element-at-row from a const-or-dense Integer64Vector prepared by _prepare_int_arg."""
    cdef DrakenVector* uv
    cdef uint8_t* nulls
    is_null[0] = False
    uv = iv.unified()
    nulls = uv.validity
    if nulls != NULL and not ((nulls[row >> 3] >> (row & 7)) & 1):
        is_null[0] = True
        return 0
    return (<int64_t*>uv.data)[uv.selection[row]]


# ---------------------------------------------------------------------------
# vector_string_slice_left
# ---------------------------------------------------------------------------

cpdef StringVector vector_string_slice_left(StringVector vec, object length):
    """
    Slice each string from the left (beginning) up to 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: Integer64Vector, NullVector, or Python int — number of bytes to keep.
                An int is wrapped as a constant Integer64Vector matching vec's row count.

    Returns:
        StringVector: sliced strings.
    """
    cdef Py_ssize_t n_rows = <Py_ssize_t>vec.unified().length
    cdef Integer64Vector length_iv = _prepare_int_arg(length, n_rows)
    cdef bint length_is_null_vec = length_iv is None

    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef DrakenConstantStringPayload* csp
    cdef int32_t const_len
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # ------------------------------------------------------------------
    # Const-encoded string
    # ------------------------------------------------------------------
    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()
        csp = <DrakenConstantStringPayload*>uv.data
        const_len = <int32_t>csp.length
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
            builder.append_bytes(<const char*>csp.data, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._german_dict_values != NULL:  # dictionary
        return _slice_left_dict(vec, length_iv, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _slice_left_dense(vec, length_iv, n)


cdef StringVector _slice_left_dict(StringVector vec, Integer64Vector length_iv, Py_ssize_t n):
    """Dict encoding path for slice_left."""
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenGermanArena* sl_gdv = vec._german_dict_values
    cdef GermanString* sl_slot
    cdef const uint8_t* sl_sdata
    cdef uint32_t sl_slen
    cdef Py_ssize_t dict_size = <Py_ssize_t>sl_gdv.length
    cdef Py_ssize_t dict_len
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef Py_ssize_t j
    cdef uint8_t* row_nulls = uv.validity
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder
    cdef DrakenVector* liv_uv

    # When int arg is const we can operate on dict values only — O(dict_size)
    liv_uv = length_iv.unified()
    if liv_uv.data_length == 1 and liv_uv.validity == NULL:  # const non-null
        length_val = (<int64_t*>liv_uv.data)[0]
        dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
        for j in range(dict_size):
            sl_slot = &sl_gdv.slots[j]
            sl_slen = gs_length(sl_slot)
            sl_sdata = gs_data(sl_slot, sl_gdv.arena)
            dict_len = <Py_ssize_t>sl_slen
            if length_val < 0:
                take = dict_len + <Py_ssize_t>length_val
                if take < 0:
                    take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > dict_len:
                take = dict_len
            dict_builder.append_bytes(<const char*>sl_sdata, take)
        new_dict_sv = dict_builder.finish()
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
            row_nulls,
        )

    # Varying int arg — per-row dict code lookup (no full materialisation)
    if liv_uv.data_length == 1 and liv_uv.validity != NULL:  # const-null length → all rows null
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
        code = uv.selection[i]
        sl_slot = &sl_gdv.slots[code]
        sl_slen = gs_length(sl_slot)
        sl_sdata = gs_data(sl_slot, sl_gdv.arena)
        dict_len = <Py_ssize_t>sl_slen
        if length_val < 0:
            take = dict_len + <Py_ssize_t>length_val
            if take < 0:
                take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > dict_len:
            take = dict_len
        builder.append_bytes(<const char*>sl_sdata, take)
    return builder.finish()


cdef StringVector _slice_left_dense(StringVector vec, Integer64Vector length_iv, Py_ssize_t n):
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

cpdef StringVector vector_string_slice_right(StringVector vec, object length):
    """
    Slice each string from the right (end) keeping 'length' bytes.

    Parameters:
        vec: StringVector of strings.
        length: Integer64Vector, NullVector, or Python int — number of bytes to keep from the right.
                An int is wrapped as a constant Integer64Vector matching vec's row count.

    Returns:
        StringVector: sliced strings.
    """
    cdef Py_ssize_t n_rows = <Py_ssize_t>vec.unified().length
    cdef Integer64Vector length_iv = _prepare_int_arg(length, n_rows)
    cdef bint length_is_null_vec = length_iv is None

    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef DrakenConstantStringPayload* csp
    cdef int32_t const_len
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # ------------------------------------------------------------------
    # Const-encoded string
    # ------------------------------------------------------------------
    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()
        csp = <DrakenConstantStringPayload*>uv.data
        const_len = <int32_t>csp.length
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
            builder.append_bytes(<const char*>csp.data + actual_start, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._german_dict_values != NULL:  # dictionary
        return _slice_right_dict(vec, length_iv, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _slice_right_dense(vec, length_iv, n)


cdef StringVector _slice_right_dict(StringVector vec, Integer64Vector length_iv, Py_ssize_t n):
    """Dict encoding path for slice_right."""
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenGermanArena* sr_gdv = vec._german_dict_values
    cdef GermanString* sr_slot
    cdef const uint8_t* sr_sdata
    cdef uint32_t sr_slen
    cdef Py_ssize_t dict_size = <Py_ssize_t>sr_gdv.length
    cdef Py_ssize_t dict_len
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef Py_ssize_t j
    cdef uint8_t* row_nulls = uv.validity
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder
    cdef DrakenVector* liv_uv

    liv_uv = length_iv.unified()
    if liv_uv.data_length == 1 and liv_uv.validity == NULL:  # const non-null
        length_val = (<int64_t*>liv_uv.data)[0]
        dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
        for j in range(dict_size):
            sr_slot = &sr_gdv.slots[j]
            sr_slen = gs_length(sr_slot)
            sr_sdata = gs_data(sr_slot, sr_gdv.arena)
            dict_len = <Py_ssize_t>sr_slen
            if length_val < 0:
                take = 0
            else:
                take = <Py_ssize_t>length_val
            if take > dict_len:
                take = dict_len
            actual_start = dict_len - take
            dict_builder.append_bytes(<const char*>sr_sdata + actual_start, take)
        new_dict_sv = dict_builder.finish()
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
            row_nulls,
        )

    if liv_uv.data_length == 1 and liv_uv.validity != NULL:  # const-null length → all rows null
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
        code = uv.selection[i]
        sr_slot = &sr_gdv.slots[code]
        sr_slen = gs_length(sr_slot)
        sr_sdata = gs_data(sr_slot, sr_gdv.arena)
        dict_len = <Py_ssize_t>sr_slen
        if length_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > dict_len:
            take = dict_len
        actual_start = dict_len - take
        builder.append_bytes(<const char*>sr_sdata + actual_start, take)
    return builder.finish()


cdef StringVector _slice_right_dense(StringVector vec, Integer64Vector length_iv, Py_ssize_t n):
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
        from_pos: Integer64Vector or NullVector — 1-based start position.
        count: Integer64Vector or NullVector — number of bytes to extract.

    Returns:
        StringVector of extracted substrings. NULL propagates from any input.
    """
    cdef Py_ssize_t n_rows = <Py_ssize_t>vec.unified().length
    cdef Integer64Vector pos_iv = _prepare_int_arg(from_pos, n_rows)
    cdef Integer64Vector cnt_iv = _prepare_int_arg(count, n_rows)
    cdef bint pos_is_null_vec = pos_iv is None
    cdef bint cnt_is_null_vec = cnt_iv is None

    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef DrakenConstantStringPayload* csp
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
    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()
        csp = <DrakenConstantStringPayload*>uv.data
        const_len = <int32_t>csp.length
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

            builder.append_bytes(<const char*>csp.data + s_idx, take)
        return builder.finish()

    # ------------------------------------------------------------------
    # Dict-encoded string
    # ------------------------------------------------------------------
    if vec._german_dict_values != NULL:  # dictionary
        return _substring_dict(vec, pos_iv, cnt_iv, cnt_is_null_vec, n)

    # ------------------------------------------------------------------
    # Dense-encoded string
    # ------------------------------------------------------------------
    return _substring_dense(vec, pos_iv, cnt_iv, cnt_is_null_vec, n)


cdef StringVector _substring_dict(
    StringVector vec,
    Integer64Vector pos_iv,
    Integer64Vector cnt_iv,
    bint cnt_is_null_vec,
    Py_ssize_t n,
):
    """Dict encoding path for substring."""
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenGermanArena* ss_gdv = vec._german_dict_values
    cdef GermanString* ss_slot
    cdef const uint8_t* ss_sdata
    cdef uint32_t ss_slen
    cdef Py_ssize_t dict_size = <Py_ssize_t>ss_gdv.length
    cdef Py_ssize_t dict_len
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef Py_ssize_t j
    cdef uint8_t* row_nulls = uv.validity
    cdef uint32_t code
    cdef Py_ssize_t i
    cdef StringVector new_dict_sv
    cdef StringVectorBuilder dict_builder
    cdef StringVectorBuilder builder
    cdef DrakenVector* piv_uv
    cdef DrakenVector* civ_uv

    piv_uv = pos_iv.unified()

    # Const pos + const (or null) count → operate on dict values only — O(dict_size)
    if piv_uv.data_length == 1 and piv_uv.validity == NULL:  # const non-null pos
        civ_uv = cnt_iv.unified() if not cnt_is_null_vec else NULL
        if cnt_is_null_vec or (civ_uv != NULL and civ_uv.data_length == 1 and civ_uv.validity == NULL):
            start_val = (<int64_t*>piv_uv.data)[0]
            dict_builder = StringVectorBuilder.with_estimate(dict_size, 8)
            for j in range(dict_size):
                ss_slot = &ss_gdv.slots[j]
                ss_slen = gs_length(ss_slot)
                ss_sdata = gs_data(ss_slot, ss_gdv.arena)
                dict_len = <Py_ssize_t>ss_slen

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
                    count_val = (<int64_t*>civ_uv.data)[0]

                if count_val < 0:
                    take = 0
                else:
                    take = <Py_ssize_t>count_val
                if s_idx + take > dict_len:
                    take = dict_len - s_idx
                if take < 0:
                    take = 0

                dict_builder.append_bytes(<const char*>ss_sdata + s_idx, take)
            new_dict_sv = dict_builder.finish()
            return from_packed_dict(
                <uint8_t*>uv.selection, 4, n,
                new_dict_sv.ptr.offsets, <const uint8_t*>new_dict_sv.ptr.data, dict_size,
                row_nulls,
            )

    # Const-null pos → all rows null
    if piv_uv.data_length == 1 and piv_uv.validity != NULL:
        builder = StringVectorBuilder.with_estimate(n, 0)
        for i in range(n):
            builder.append_null()
        return builder.finish()

    # Const-null count → all rows null (count=NULL propagates)
    if not cnt_is_null_vec and cnt_iv is not None:
        civ_uv = cnt_iv.unified()
        if civ_uv.data_length == 1 and civ_uv.validity != NULL:
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

        code = uv.selection[i]
        ss_slot = &ss_gdv.slots[code]
        ss_slen = gs_length(ss_slot)
        ss_sdata = gs_data(ss_slot, ss_gdv.arena)
        dict_len = <Py_ssize_t>ss_slen

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

        builder.append_bytes(<const char*>ss_sdata + s_idx, take)
    return builder.finish()


cdef StringVector _substring_dense(
    StringVector vec,
    Integer64Vector pos_iv,
    Integer64Vector cnt_iv,
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
