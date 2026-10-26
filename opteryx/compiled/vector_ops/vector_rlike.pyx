# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector, from_decoded
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot
from draken.core.buffers cimport str_length, str_data
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint8_t, uint16_t, uint32_t

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil


cdef extern from "ops/kernels/dfa_walk.h" nogil:
    ctypedef struct DfaProgram "draken_dfa::DfaProgram":
        pass
    int dfa_decode "draken_dfa::decode"(const uint8_t* blob, size_t blob_len, DfaProgram* p)
    int dfa_match "draken_dfa::match"(const DfaProgram* p, const uint8_t* s, uint32_t slen)


# Pattern operand is a pre-compiled DFA blob (vector_dfa_compile.compile_rlike_dfa),
# produced at bind time from a literal pattern by binder/pattern_compile.py's
# compile_rlike_pattern — never a raw regex string. RE2 is not linked here; RE2's
# parser only ever runs at plan time (vector_dfa_compile.pyx). The blob is walked
# by the shared C++ walk (draken/ops/kernels/dfa_walk.h), the same one draken_rlike
# uses. A non-literal or uncompilable pattern is refused with NotSupportedError
# before a predicate ever reaches this function.


cpdef BoolVector vector_rlike(Vector vec, Vector pattern, bint negate=False):
    """Return mask: 1 if element matches the compiled DFA pattern, else 0.
    Propagates NULLs."""
    cdef DrakenVector* puv = pattern.unified()
    if not draken_is_constant(puv):
        raise ValueError(
            "vector_rlike: pattern must be a single value (a constant)"
        )
    cdef DrakenStringArena* parena = <DrakenStringArena*>puv.data
    cdef uint32_t* psel = <uint32_t*>puv.selection
    cdef DrakenStringSlot* pslot = &parena.slots[psel[0]]
    cdef const uint8_t* pat_ptr = str_data(pslot, parena.arena)
    cdef Py_ssize_t pat_len = <Py_ssize_t>str_length(pslot)
    cdef DfaProgram prog
    if not dfa_decode(pat_ptr, <size_t>pat_len, &prog):
        raise ValueError("vector_rlike: malformed compiled DFA blob")

    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef size_t sz = <size_t>nbytes if nbytes > 0 else 1

    cdef uint8_t* dst = <uint8_t*>draken_malloc(sz)
    if dst == NULL:
        raise MemoryError()

    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    # A NULL pattern makes every row NULL.
    if puv.validity != NULL and (puv.validity[0] & 1) == 0:
        draken_free(dst)
        return _all_null_bool(n)

    if negate:
        memset(dst, 0xFF, sz)
        if (n & 7) != 0:
            dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
    else:
        memset(dst, 0, sz)

    if nulls != NULL and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nulls, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask

    if negate:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if dfa_match(&prog, sdata, slen):
                dst[i >> 3] &= ~(1 << (i & 7))
    else:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if dfa_match(&prog, sdata, slen):
                dst[i >> 3] |= (1 << (i & 7))

    return from_decoded(<void*>dst, out_null, <size_t>n)
