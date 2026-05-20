# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t

from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector
from draken.vectors.string_vector cimport _ConstView
from draken.vectors.string_vector cimport _const_view
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot, str_length, str_data


cpdef StringVector vector_soundex(StringVector vec):
    """
    Compute Soundex codes for each element of a StringVector.

    Returns:
        StringVector: Soundex codes (e.g. 'A123' or NULL for empty/null input).
    """
    from opteryx.third_party.fuzzy import soundex

    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes raw
    cdef str text, code
    cdef StringRow row
    cdef _ConstView csp
    cdef DrakenStringArena* sdx_dense_arena
    cdef DrakenStringSlot* sdx_dense_slot
    cdef const uint8_t* sdx_dense_sdata
    cdef uint32_t sdx_dense_slen

    # Constant encoding: process once, replicate
    if vec._unified_view.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 4)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            if csp.length == 0:
                for i in range(n):
                    builder.append_null()
            else:
                raw = PyBytes_FromStringAndSize(<const char*>csp.data, csp.length)
                text = raw.decode("utf-8", "replace")
                code = soundex(text)
                encoded = code.encode("utf-8") if code else b""
                for i in range(n):
                    builder.append(encoded)
        return builder.finish()

    # Dictionary encoding: per-row via string_vec_get_at (soundex can yield null from
    # non-null empty strings, so we cannot use dict->dict transform without rebuilding
    # the null bitmap; per-row access is correct and dict values are typically low cardinality)
    if vec._unified_view.data_length < vec._unified_view.length:  # dictionary
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 4)
        for i in range(n):
            row = string_vec_get_at(vec, i)
            if row.is_null or row.length == 0:
                builder.append_null()
            else:
                raw = PyBytes_FromStringAndSize(row.data, row.length)
                text = raw.decode("utf-8", "replace")
                code = soundex(text)
                builder.append(code.encode("utf-8") if code else b"")
        return builder.finish()

    # Dense encoding: row by row
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 4)
    sdx_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        sdx_dense_slot = &sdx_dense_arena.slots[i]
        sdx_dense_sdata = str_data(sdx_dense_slot, sdx_dense_arena.arena)
        sdx_dense_slen = str_length(sdx_dense_slot)
        if sdx_dense_slen == 0:
            builder.append_null()
            continue
        raw = bytes(sdx_dense_sdata[:sdx_dense_slen])
        text = raw.decode("utf-8", "replace")
        code = soundex(text)
        builder.append(code.encode("utf-8") if code else b"")
    return builder.finish()
