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


# Pattern operand is a pre-compiled DFA blob (vector_dfa_compile.compile_rlike_dfa),
# produced at plan time from a literal pattern by predicate_rewriter.py's
# _rewrite_rlike_to_dfa — never a raw regex string. RE2 is not linked here; RE2's
# parser only ever runs at plan time (vector_dfa_compile.pyx). A non-literal or
# uncompilable pattern is refused with NotSupportedError before a predicate ever
# reaches this function — see .claude/CLAUDE.md's plan-time-refusal precedent.
cdef inline bint _dfa_match(
    const uint8_t* blob,
    Py_ssize_t blob_len,
    const uint8_t* sdata,
    uint32_t slen,
) except -1 nogil:
    if blob_len < 4:
        with gil:
            raise ValueError("vector_rlike: malformed DFA blob (too short)")
    cdef uint8_t version = blob[0]
    cdef uint8_t flags = blob[1]
    cdef uint16_t num_states = (<uint16_t>blob[2]) | ((<uint16_t>blob[3]) << 8)
    if version != 1:
        with gil:
            raise ValueError("vector_rlike: unsupported DFA blob version")
    cdef bint has_begin = (flags & 0x01) != 0
    cdef bint has_end = (flags & 0x02) != 0
    cdef Py_ssize_t accept_bitmap_len = (num_states + 7) // 8
    cdef Py_ssize_t expected_len = 4 + accept_bitmap_len + (<Py_ssize_t>num_states) * 256 * 2
    if blob_len != expected_len:
        with gil:
            raise ValueError("vector_rlike: malformed DFA blob (length mismatch)")

    cdef const uint8_t* accept_bitmap = blob + 4
    cdef const uint16_t* table = <const uint16_t*>(blob + 4 + accept_bitmap_len)
    cdef int state = 0
    cdef uint32_t i
    cdef uint16_t next_state

    if not has_end and ((accept_bitmap[0] >> 0) & 1):
        return True

    for i in range(slen):
        next_state = table[(<Py_ssize_t>state) * 256 + sdata[i]]
        if next_state == 0xFFFF:
            # Anchored-start (has_begin): a genuine dead end, no recovery.
            # Unanchored-start: the NFA's self-loop "search anywhere" state is
            # always active, so a real dead transition should not occur here —
            # treat it the same (no match) rather than guess at recovery.
            return False
        state = <int>next_state
        if not has_end and ((accept_bitmap[state >> 3] >> (state & 7)) & 1):
            return True

    if has_end:
        return ((accept_bitmap[state >> 3] >> (state & 7)) & 1) != 0
    return False


cpdef BoolVector vector_rlike(Vector vec, Vector pattern, bint negate=False):
    """Return mask: 1 if element matches the compiled DFA pattern, else 0.
    Propagates NULLs."""
    cdef DrakenVector* puv = pattern.unified()
    if puv.data_length != 1:
        raise ValueError(
            "vector_rlike: pattern must be a single value (data_length == 1)"
        )
    cdef DrakenStringArena* parena = <DrakenStringArena*>puv.data
    cdef uint32_t* psel = <uint32_t*>puv.selection
    cdef DrakenStringSlot* pslot = &parena.slots[psel[0]]
    cdef const uint8_t* pat_ptr = str_data(pslot, parena.arena)
    cdef Py_ssize_t pat_len = <Py_ssize_t>str_length(pslot)

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
            if _dfa_match(pat_ptr, pat_len, sdata, slen):
                dst[i >> 3] &= ~(1 << (i & 7))
    else:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if _dfa_match(pat_ptr, pat_len, sdata, slen):
                dst[i >> 3] |= (1 << (i & 7))

    return from_decoded(<void*>dst, out_null, <size_t>n)
