# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
GermanStringBuilder — produces DrakenGermanArena from appended values.

The arena is the new values payload for string DrakenVectors (Track B,
replacing DrakenVarBuffer). Builder API mirrors StringVectorBuilder:
preallocate row count + byte budget, append values in order, finish to
hand off the populated arena.

Inline encoding decision is per-slot, made at append time:
  len <= 12 → store bytes inline in the slot (no arena write)
  len  > 12 → memcpy bytes into the arena, slot holds prefix + arena_offset

All slots are zero-initialised before write so that short-string prefix
padding is deterministic (lp_word equality is byte-exact across builds).
"""

from cpython.bytes cimport PyBytes_AS_STRING
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport DrakenGermanArena
from draken.core.buffers cimport DrakenType
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.buffers cimport GermanString
from draken.core.buffers cimport GS_INLINE_MAX
from draken.core.buffers cimport gs_data
from draken.core.buffers cimport gs_equals
from draken.core.buffers cimport gs_init_extern
from draken.core.buffers cimport gs_init_inline
from draken.core.buffers cimport gs_init_null
from draken.core.buffers cimport gs_length
from draken.core.german_arena cimport alloc_german_arena
from draken.core.german_arena cimport free_german_arena


cdef class GermanStringBuilder:

    def __cinit__(self, Py_ssize_t length, Py_ssize_t arena_capacity,
                  bint resizable=False, bint strict_capacity=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if arena_capacity < 0:
            raise ValueError("arena_capacity must be non-negative")

        self._arena = alloc_german_arena(DRAKEN_STRING, <size_t>length, <size_t>arena_capacity)
        self._length = length
        self._next_index = 0
        self._finished = False
        self._resizable = resizable
        self._strict_capacity = strict_capacity
        self._mask_user_provided = False
        self._slots = self._arena.slots
        self._nulls = NULL

    def __dealloc__(self):
        # If finish() ran, _arena is NULL — the caller now owns the storage.
        if self._arena != NULL:
            free_german_arena(self._arena)
            self._arena = NULL

    @classmethod
    def with_counts(cls, Py_ssize_t length, Py_ssize_t total_arena_bytes):
        """Exact byte budget; finish() validates strict consumption."""
        return cls(length, total_arena_bytes, False, True)

    @classmethod
    def with_estimate(cls, Py_ssize_t length, Py_ssize_t est_avg_bytes):
        """Resizable arena from an average-bytes estimate (only long-form values
        consume arena bytes; short ones live in the slot itself)."""
        if length < 0:
            raise ValueError("length must be non-negative")
        if est_avg_bytes < 0:
            raise ValueError("est_avg_bytes must be non-negative")
        cdef Py_ssize_t initial = length * est_avg_bytes
        if initial <= 0:
            initial = max(length, 64)
        return cls(length, initial, True, False)

    def __len__(self):
        return self._length

    property arena_capacity:
        def __get__(self):
            return <Py_ssize_t>self._arena.arena_cap if self._arena != NULL else 0

    property arena_used:
        def __get__(self):
            return <Py_ssize_t>self._arena.arena_used if self._arena != NULL else 0

    # ------------------------------------------------------------------
    # Append APIs
    # ------------------------------------------------------------------

    cpdef void append(self, bytes value):
        self._append_with_ptr(self._next_index, PyBytes_AS_STRING(value), len(value))

    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length):
        self._append_with_ptr(self._next_index, ptr, length)

    cpdef void append_view(self, const uint8_t[::1] value):
        cdef Py_ssize_t size = value.shape[0]
        cdef const uint8_t* ptr = NULL
        if size == 0:
            self._append_with_ptr(self._next_index, NULL, 0)
        else:
            ptr = &value[0]
            self._append_with_ptr(self._next_index, <const char*>ptr, size)

    cpdef void append_null(self):
        self._set_null(self._next_index)

    # ------------------------------------------------------------------
    # Validity bitmap (user-provided)
    # ------------------------------------------------------------------

    cpdef void set_validity_mask(self, const uint8_t[::1] mask):
        cdef Py_ssize_t nb_size = (self._length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        if mask.shape[0] < nb_size:
            raise ValueError("validity mask is too small for declared length")
        if self._arena.null_bitmap == NULL:
            self._arena.null_bitmap = <uint8_t*> malloc(<size_t>nb_size)
            if self._arena.null_bitmap == NULL:
                raise MemoryError()
        memcpy(self._arena.null_bitmap, &mask[0], <size_t>nb_size)
        self._nulls = self._arena.null_bitmap
        self._mask_user_provided = True

    # ------------------------------------------------------------------
    # Finish — hand ownership of the arena to the caller
    # ------------------------------------------------------------------

    cdef DrakenGermanArena* finish(self) except NULL:
        if self._finished:
            raise RuntimeError("GermanStringBuilder.finish called twice")
        if self._next_index != self._length:
            raise ValueError(
                f"builder incomplete: appended {self._next_index} of {self._length} entries"
            )
        if self._strict_capacity and <Py_ssize_t>self._arena.arena_used != <Py_ssize_t>self._arena.arena_cap:
            raise ValueError(
                f"builder consumed {self._arena.arena_used} arena bytes "
                f"but expected {self._arena.arena_cap}"
            )
        self._finished = True
        cdef DrakenGermanArena* out = self._arena
        self._arena = NULL  # detach — ownership passes to caller
        self._slots = NULL
        self._nulls = NULL
        return out

    # Python-callable variant used by tests; wraps the raw pointer in a handle
    # that owns the storage.
    def finish_handle(self):
        cdef DrakenGermanArena* a = self.finish()
        return GermanArenaHandle._wrap(a)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    cdef inline void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *:
        self._require_index(index)
        if length < 0:
            raise ValueError("length must be non-negative")
        if length > 0xFFFFFFFF:
            raise ValueError("german string length exceeds uint32_t range")

        cdef GermanString* slot = &self._slots[index]
        cdef Py_ssize_t arena_off

        if length <= GS_INLINE_MAX:
            gs_init_inline(slot, <const uint8_t*>src, <uint32_t>length)
        else:
            self._ensure_arena_capacity(length)
            arena_off = <Py_ssize_t>self._arena.arena_used
            memcpy(self._arena.arena + arena_off, src, <size_t>length)
            gs_init_extern(slot, <const uint8_t*>src, <uint32_t>length, <uint64_t>arena_off)
            self._arena.arena_used = <size_t>(arena_off + length)

        # If the caller installed a validity mask, defer to that. Otherwise mark
        # the row valid via the bitmap (allocating it on first non-null write
        # following any earlier null).
        if self._nulls != NULL and not self._mask_user_provided:
            self._nulls[index >> 3] |= (1 << (index & 7))

        self._next_index += 1

    cdef inline void _set_null(self, Py_ssize_t index) except *:
        self._require_index(index)
        self._initialize_null_bitmap()
        # Clear the valid bit for this row, and zero the slot for determinism.
        self._nulls[index >> 3] &= ~(<uint8_t>(1 << (index & 7)))
        gs_init_null(&self._slots[index])
        self._next_index += 1

    cdef inline void _ensure_arena_capacity(self, Py_ssize_t to_add) except *:
        if to_add <= 0:
            return
        if <Py_ssize_t>(self._arena.arena_used + <size_t>to_add) <= <Py_ssize_t>self._arena.arena_cap:
            return
        if not self._resizable:
            raise ValueError(
                f"german arena out of capacity: have {self._arena.arena_cap}, "
                f"need {self._arena.arena_used + <size_t>to_add}"
            )

        cdef size_t new_cap = self._arena.arena_cap
        if new_cap == 0:
            new_cap = 64
        while new_cap < self._arena.arena_used + <size_t>to_add:
            new_cap *= 2

        cdef uint8_t* new_buf = <uint8_t*>realloc(self._arena.arena, new_cap)
        if new_buf == NULL:
            raise MemoryError()
        self._arena.arena = new_buf
        self._arena.arena_cap = new_cap

    cdef inline void _initialize_null_bitmap(self) except *:
        if self._arena.null_bitmap != NULL:
            return
        cdef Py_ssize_t nb_size = (self._length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        cdef uint8_t* nb = <uint8_t*> malloc(<size_t>nb_size)
        if nb == NULL:
            raise MemoryError()
        # Mark all previously-appended rows as valid; new nulls clear their bit.
        memset(nb, 0xFF, <size_t>nb_size)
        self._arena.null_bitmap = nb
        self._nulls = nb

    cdef inline void _require_index(self, Py_ssize_t index) except *:
        if index < 0 or index >= self._length:
            raise IndexError("german builder index out of range")
        if index != self._next_index:
            raise IndexError(
                f"german builder index {index} != next_index {self._next_index} "
                "(out-of-order appends not supported)"
            )


cdef class GermanArenaHandle:
    """Owning wrapper around a finished DrakenGermanArena. Test-only."""

    def __cinit__(self):
        self._arena = NULL
        self._owns = False

    def __dealloc__(self):
        if self._owns and self._arena != NULL:
            free_german_arena(self._arena)
            self._arena = NULL

    @staticmethod
    cdef GermanArenaHandle _wrap(DrakenGermanArena* arena):
        cdef GermanArenaHandle h = GermanArenaHandle.__new__(GermanArenaHandle)
        h._arena = arena
        h._owns = True
        return h

    cpdef Py_ssize_t length(self):
        return 0 if self._arena == NULL else <Py_ssize_t>self._arena.length

    cpdef Py_ssize_t arena_used(self):
        return 0 if self._arena == NULL else <Py_ssize_t>self._arena.arena_used

    cdef inline GermanString* _slot(self, Py_ssize_t i) except NULL:
        if self._arena == NULL or i < 0 or i >= <Py_ssize_t>self._arena.length:
            raise IndexError("arena slot index out of range")
        return &self._arena.slots[i]

    cpdef bint is_null(self, Py_ssize_t i):
        if self._arena == NULL or i < 0 or i >= <Py_ssize_t>self._arena.length:
            raise IndexError("arena slot index out of range")
        cdef uint8_t* nb = self._arena.null_bitmap
        if nb == NULL:
            return False
        return ((nb[i >> 3] >> (i & 7)) & 1) == 0

    cpdef bint is_inline(self, Py_ssize_t i):
        cdef GermanString* s = self._slot(i)
        return gs_length(s) <= GS_INLINE_MAX

    cpdef Py_ssize_t slot_length(self, Py_ssize_t i):
        return <Py_ssize_t>gs_length(self._slot(i))

    cpdef bytes slot_bytes(self, Py_ssize_t i):
        cdef GermanString* s = self._slot(i)
        cdef Py_ssize_t n = <Py_ssize_t>gs_length(s)
        cdef const uint8_t* p = gs_data(s, self._arena.arena)
        return p[:n]

    cpdef list to_pylist(self):
        cdef Py_ssize_t n = self.length()
        cdef Py_ssize_t i
        cdef list out = []
        for i in range(n):
            if self.is_null(i):
                out.append(None)
            else:
                out.append(self.slot_bytes(i))
        return out

    cpdef bint slots_equal(self, Py_ssize_t i, Py_ssize_t j):
        cdef GermanString* a = self._slot(i)
        cdef GermanString* b = self._slot(j)
        return gs_equals(a, self._arena.arena, b, self._arena.arena) != 0
