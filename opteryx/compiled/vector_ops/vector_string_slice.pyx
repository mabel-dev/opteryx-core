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
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from cpython.array cimport array, clone

from draken.vectors.vector cimport Vector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from draken.vectors.integer64_vector cimport Integer64Vector, from_sequence as int64_from_sequence, _materialize_dict_int64
from draken.vectors.null_vector cimport NullVector
from draken.core.buffers cimport DrakenVector, DrakenVarBuffer, DrakenStringArena, DrakenStringSlot, str_length, str_data


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
        # Wrap a literal scalar as a constant Integer64Vector.
        return Integer64Vector.from_constant(<int64_t>arg, row_count)
    if not isinstance(arg, Integer64Vector):
        raise TypeError(f"integer argument must be an Integer64Vector, NullVector or int, got {type(arg).__name__}")
    iv = <Integer64Vector>arg
    if iv._unified_view.data_length < iv._unified_view.length:
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
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take
    cdef bint length_null
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        if length_val < 0:
            take = <Py_ssize_t>slen + <Py_ssize_t>length_val
            if take < 0:
                take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > <Py_ssize_t>slen:
            take = <Py_ssize_t>slen
        builder.append_bytes(<const char*>sdata, take)
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
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t length_val
    cdef Py_ssize_t take, actual_start
    cdef bint length_null
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if length_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        length_val = _read_int_arg(length_iv, i, &length_null)
        if length_null:
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        if length_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>length_val
        if take > <Py_ssize_t>slen:
            take = <Py_ssize_t>slen
        actual_start = <Py_ssize_t>slen - take
        builder.append_bytes(<const char*>sdata + actual_start, take)
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
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int64_t start_val, count_val
    cdef Py_ssize_t s_idx, take
    cdef bint pos_null, cnt_null
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 8)

    if pos_is_null_vec:
        for i in range(n):
            builder.append_null()
        return builder.finish()

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start_val = _read_int_arg(pos_iv, i, &pos_null)
        if pos_null:
            builder.append_null()
            continue

        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)

        if cnt_is_null_vec:
            count_val = <int64_t>slen
        else:
            count_val = _read_int_arg(cnt_iv, i, &cnt_null)
            if cnt_null:
                builder.append_null()
                continue

        if start_val > 0:
            s_idx = <Py_ssize_t>(start_val - 1)
        elif start_val < 0:
            s_idx = <Py_ssize_t>(<Py_ssize_t>slen + start_val)
            if s_idx < 0:
                s_idx = 0
        else:
            s_idx = 0
        if s_idx > <Py_ssize_t>slen:
            s_idx = <Py_ssize_t>slen

        if count_val < 0:
            take = 0
        else:
            take = <Py_ssize_t>count_val
        if s_idx + take > <Py_ssize_t>slen:
            take = <Py_ssize_t>slen - s_idx
        if take < 0:
            take = 0

        builder.append_bytes(<const char*>sdata + s_idx, take)
    return builder.finish()
