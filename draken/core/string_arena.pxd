# Stub .pxd for draken.core.string_arena.
# Provides alloc_string_arena for consumer compatibility.

from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free

from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot, DrakenType, DRAKEN_VARCHAR


cdef inline DrakenStringArena* alloc_string_arena(DrakenType dtype, size_t length, size_t arena_cap):
    cdef DrakenStringArena* arena = <DrakenStringArena*>malloc(sizeof(DrakenStringArena))
    if arena == NULL:
        raise MemoryError()
    arena.slots = <DrakenStringSlot*>malloc(length * sizeof(DrakenStringSlot))
    if arena.slots == NULL:
        free(arena)
        raise MemoryError()
    if arena_cap > 0:
        arena.arena = <unsigned char*>malloc(arena_cap)
        if arena.arena == NULL:
            free(arena.slots)
            free(arena)
            raise MemoryError()
    else:
        arena.arena = NULL
    arena.length = length
    arena.arena_used = 0
    arena.arena_cap = arena_cap
    arena.null_bitmap = NULL
    arena.owns_buffers = 1
    arena.payloads_elided = 0
    arena.type = DRAKEN_VARCHAR
    return arena
