// draken/core/frame_arena.cpp — tracked-pointer per-frame allocator.
//
// See frame_arena.h for contract. Implementation: std::vector<void*>
// tracking + draken_malloc/draken_free backing. release() does an O(n)
// find+erase on the tracking list, which is fine for the expected
// per-frame size (a few dozen entries at most). If profiling later
// shows release() hot, swap for an unordered_set without changing the
// public API.

#include "frame_arena.h"
#include "alloc.h"

#include <algorithm>
#include <new>
#include <vector>

struct DrakenFrameArena {
    std::vector<void*> ptrs;
};

extern "C" DrakenFrameArena* draken_frame_arena_create(void) {
    // nothrow because the public API contract says "returns NULL on OOM".
    return new (std::nothrow) DrakenFrameArena{};
}

extern "C" void draken_frame_arena_destroy(DrakenFrameArena* arena) {
    if (arena == nullptr) return;
    for (void* p : arena->ptrs) {
        draken_free(p);
    }
    delete arena;
}

extern "C" void* draken_frame_arena_alloc(DrakenFrameArena* arena, size_t nbytes) {
    if (arena == nullptr) return nullptr;
    void* p = draken_malloc(nbytes);
    if (p == nullptr) return nullptr;
    try {
        arena->ptrs.push_back(p);
    } catch (...) {
        // push_back may throw bad_alloc; don't leak the buffer.
        draken_free(p);
        return nullptr;
    }
    return p;
}

extern "C" void draken_frame_arena_release(DrakenFrameArena* arena, void* ptr) {
    if (arena == nullptr || ptr == nullptr) return;
    auto it = std::find(arena->ptrs.begin(), arena->ptrs.end(), ptr);
    if (it != arena->ptrs.end()) {
        arena->ptrs.erase(it);
    }
    // Silent no-op if not found — per contract.
}

extern "C" void draken_frame_arena_adopt(DrakenFrameArena* arena, void* ptr) {
    if (arena == nullptr || ptr == nullptr) return;
    try {
        arena->ptrs.push_back(ptr);
    } catch (...) {
        // push_back may throw bad_alloc. We don't own the pointer yet (caller
        // hasn't been told the adopt succeeded), so just propagate the failure
        // by NOT adding to tracking. Caller retains ownership and is
        // responsible for `draken_free`ing it themselves.
        // Note: this is best-effort; under OOM the caller may not check.
    }
}

extern "C" int draken_frame_arena_contains(const DrakenFrameArena* arena, const void* ptr) {
    if (arena == nullptr || ptr == nullptr) return 0;
    return std::find(arena->ptrs.begin(), arena->ptrs.end(), ptr) != arena->ptrs.end() ? 1 : 0;
}

extern "C" size_t draken_frame_arena_size(const DrakenFrameArena* arena) {
    if (arena == nullptr) return 0;
    return arena->ptrs.size();
}
