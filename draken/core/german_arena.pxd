# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
German-string arena helpers — allocate / free DrakenGermanArena.

Parallels draken/core/var_vector.pxd which provides alloc_var_buffer /
free_var_buffer for DrakenVarBuffer. Used by Track B producers to build
the values payload (slots + arena bytes) for string DrakenVectors.

`alloc_german_arena(dtype, length, arena_cap)` allocates the header,
GermanString slot array, and (when arena_cap > 0) the byte arena.
arena_used starts at 0; the builder fills slots and the arena in parallel
during construction. Long-form slot arena_offsets reference the arena and
remain valid for the lifetime of the arena.

`free_german_arena(a)` releases owned buffers and the header. Slot data
references must not outlive this call.
"""

from libc.stdint cimport uint8_t
from libc.stdlib cimport free, malloc

from draken.core.buffers cimport DrakenType
from draken.core.buffers cimport DrakenGermanArena
from draken.core.buffers cimport GermanString


cdef inline DrakenGermanArena* alloc_german_arena(DrakenType dtype, size_t length, size_t arena_cap):
    cdef DrakenGermanArena* a = <DrakenGermanArena*> malloc(sizeof(DrakenGermanArena))
    if a == NULL:
        raise MemoryError()

    if length > 0:
        a.slots = <GermanString*> malloc(length * sizeof(GermanString))
        if a.slots == NULL:
            free(a)
            raise MemoryError()
    else:
        a.slots = NULL

    if arena_cap > 0:
        a.arena = <uint8_t*> malloc(arena_cap)
        if a.arena == NULL:
            if a.slots != NULL:
                free(a.slots)
            free(a)
            raise MemoryError()
    else:
        a.arena = NULL

    a.length = length
    a.arena_used = 0
    a.arena_cap = arena_cap
    a.null_bitmap = NULL
    a.owns_buffers = 1
    a.type = dtype
    return a


cdef inline void free_german_arena(DrakenGermanArena* a) noexcept:
    if a == NULL:
        return
    if a.owns_buffers != 0:
        if a.slots != NULL:
            free(a.slots)
        if a.arena != NULL:
            free(a.arena)
        if a.null_bitmap != NULL:
            free(a.null_bitmap)
    free(a)
