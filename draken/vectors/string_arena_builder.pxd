# cython: language_level=3

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t

from draken.core.buffers cimport DrakenStringArena
from draken.core.buffers cimport DrakenStringSlot


cdef class StringArenaBuilder:
    """Builder for DrakenStringArena (German-string values payload).

    Mirrors the API shape of StringVectorBuilder but produces the new format:
    a fixed-width DrakenStringSlot slot array (16 bytes each) plus a byte arena
    for payloads > STR_INLINE_MAX (12) bytes.

    Lifetime: the builder owns the arena until finish(); finish() transfers
    ownership to the caller and detaches the builder. After finish(), the
    builder must not be reused. If the builder is dealloc'd before finish(),
    the arena it allocated is freed.
    """
    cdef DrakenStringArena* _arena    # owned until finish() detaches
    cdef Py_ssize_t _length            # declared row count (capacity)
    cdef Py_ssize_t _next_index        # next slot to write
    cdef bint _finished
    cdef bint _resizable               # may grow the byte arena
    cdef bint _strict_capacity         # require exact arena byte budget
    cdef bint _mask_user_provided      # null bitmap installed by caller
    cdef DrakenStringSlot* _slots          # cached pointer into _arena.slots
    cdef uint8_t* _nulls               # cached pointer into _arena.null_bitmap (or NULL)

    cpdef void append(self, bytes value)
    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length)
    cpdef void append_view(self, const uint8_t[::1] value)
    cpdef void append_null(self)
    cpdef void set_validity_mask(self, const uint8_t[::1] mask)

    # Transfer ownership of the built arena to the caller. The builder no
    # longer references it; the caller must free_string_arena() it eventually
    # (typically by attaching it as a DrakenVector.data payload owned by a
    # string vector that frees it in its __dealloc__).
    cdef DrakenStringArena* finish(self) except NULL

    cdef void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *
    cdef void _set_null(self, Py_ssize_t index) except *
    cdef void _ensure_arena_capacity(self, Py_ssize_t to_add) except *
    cdef void _initialize_null_bitmap(self) except *
    cdef void _require_index(self, Py_ssize_t index) except *


cdef class StringArenaHandle:
    """Test/inspection wrapper around a finished DrakenStringArena.

    Owns the arena: dealloc calls free_string_arena. Not used by production
    kernels — production callers attach the arena directly as a DrakenVector
    data payload. This class exists so pytest can construct and inspect an
    arena without going through the full StringVector wiring.
    """
    cdef DrakenStringArena* _arena
    cdef bint _owns

    @staticmethod
    cdef StringArenaHandle _wrap(DrakenStringArena* arena)

    cdef DrakenStringSlot* _slot(self, Py_ssize_t i) except NULL
    cpdef Py_ssize_t length(self)
    cpdef Py_ssize_t arena_used(self)
    cpdef bint is_null(self, Py_ssize_t i)
    cpdef bint is_inline(self, Py_ssize_t i)
    cpdef Py_ssize_t slot_length(self, Py_ssize_t i)
    cpdef bytes slot_bytes(self, Py_ssize_t i)
    cpdef list to_pylist(self)
    # Pairwise equality check (smoke test for str_equals on this arena).
    cpdef bint slots_equal(self, Py_ssize_t i, Py_ssize_t j)
