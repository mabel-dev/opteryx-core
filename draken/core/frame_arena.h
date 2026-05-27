#pragma once
// draken/core/frame_arena.h — per-evaluation-frame allocator.
//
// Provides a lifecycle-scoped pool for the bytecode VM's intermediate
// buffers. The arena tracks every `draken_malloc`'d buffer it issues and
// frees them in one shot at `destroy`. Buffers that need to outlive the
// arena (e.g. the result Vector's data buffer at frame exit) are removed
// from tracking via `release` and become the caller's responsibility.
//
// Backing allocator: draken_malloc / draken_free (mimalloc). No custom
// bump arena — mimalloc's per-size-class fast paths are sufficient for
// the expected allocation pattern (a few dozen mid-sized buffers per
// evaluation frame). If measurement later shows allocator pressure, a
// bump allocator can be layered behind this API without changing
// callers.
//
// Thread safety: NONE. One arena per evaluation frame; no concurrent
// access. Sharing across threads is unsupported.
//
// Composes with the existing bridge: a released buffer can be handed
// directly to `draken_vector_own_raw` (or `_own_string`) — both expect
// `draken_malloc`-allocated memory, which is exactly what the arena
// issues.

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DrakenFrameArena DrakenFrameArena;

// Construct a new arena. Returns NULL on OOM.
DrakenFrameArena* draken_frame_arena_create(void);

// Free the arena and `draken_free` every buffer still tracked by it.
// Buffers that were `release`d are NOT freed (caller owns them).
// `arena == NULL` is a no-op.
void draken_frame_arena_destroy(DrakenFrameArena* arena);

// Allocate `nbytes` via `draken_malloc` and track. Caller must not
// `draken_free` the returned pointer directly; the arena frees it on
// `destroy` unless `release`d first. Returns NULL on OOM (nothing
// tracked in that case).
void* draken_frame_arena_alloc(DrakenFrameArena* arena, size_t nbytes);

// Remove `ptr` from arena tracking. After release, the caller owns
// `ptr` and must `draken_free` it (or hand to `draken_vector_own_raw`,
// which will). No-op when `ptr` is NULL or not tracked.
void draken_frame_arena_release(DrakenFrameArena* arena, void* ptr);

// Take ownership of an already-allocated (`draken_malloc`'d) pointer.
// The arena will `draken_free` it on `destroy` unless `release`d first.
// Used to fold the results of existing kernels (which allocate via
// `draken_malloc` themselves and return draken-owned buffers via
// `VecResult`) into a frame's arena scope without copying. No-op when
// `ptr` is NULL.
void draken_frame_arena_adopt(DrakenFrameArena* arena, void* ptr);

// Test-only introspection: number of pointers currently tracked.
// Used by native tests to verify create/alloc/release/destroy semantics.
size_t draken_frame_arena_size(const DrakenFrameArena* arena);

#ifdef __cplusplus
}
#endif
