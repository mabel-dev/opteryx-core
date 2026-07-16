# Cython declarations for the per-frame allocator.
#
# C API documented in draken/core/frame_arena.h.  The native eval engine
# (and any other Cython consumer that needs scoped allocation across a
# single function call) cimports this surface.
#
# Symbol locality: the implementation lives in draken_native.so; consumer
# extensions reach it via the RTLD_GLOBAL bridge pattern set up at
# `draken/__init__.py:10`. No per-extension linkage changes required.

from libc.stddef cimport size_t


cdef extern from "core/frame_arena.h":
    # Opaque arena handle. Don't instantiate directly — use the create()
    # constructor.
    ctypedef struct DrakenFrameArena:
        pass

    # Construct a new arena. Returns NULL on OOM.
    DrakenFrameArena* draken_frame_arena_create() nogil

    # Free the arena and draken_free every buffer still tracked by it.
    # Buffers that were release()'d are NOT freed (caller owns them).
    # `arena == NULL` is a no-op.
    void draken_frame_arena_destroy(DrakenFrameArena* arena) nogil

    # Allocate `nbytes` via draken_malloc and track. Returns NULL on OOM.
    # Caller must not draken_free the returned pointer directly; the
    # arena frees it on destroy unless release()'d first.
    void* draken_frame_arena_alloc(DrakenFrameArena* arena, size_t nbytes) nogil

    # Remove `ptr` from arena tracking. After release, the caller owns
    # `ptr` and must draken_free it (or hand to draken_vector_own_raw).
    # No-op when `ptr` is NULL or not tracked.
    void draken_frame_arena_release(DrakenFrameArena* arena, void* ptr) nogil

    # Non-zero when `ptr` is currently tracked. The only way to distinguish an
    # independently-allocated (adopted) buffer from an interior pointer into
    # another tracked block — see frame_arena.h.
    int draken_frame_arena_contains(const DrakenFrameArena* arena, const void* ptr) nogil

    # Take ownership of an already-allocated (draken_malloc'd) pointer.
    # The arena will draken_free it on destroy unless release()'d first.
    # Used to fold the results of existing kernels (which allocate via
    # draken_malloc themselves and return draken-owned buffers) into a
    # frame's arena scope. No-op when `ptr` is NULL.
    void draken_frame_arena_adopt(DrakenFrameArena* arena, void* ptr) nogil

    # Test-only introspection: number of pointers currently tracked.
    size_t draken_frame_arena_size(const DrakenFrameArena* arena) nogil
