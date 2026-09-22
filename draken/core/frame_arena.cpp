// draken/core/frame_arena.cpp — tracked-pointer per-frame allocator.
//
// See frame_arena.h for contract. Implementation: std::vector<void*>
// tracking + draken_malloc/draken_free backing. release() and
// contains() do an O(n) find over the tracking list.
//
// MEASURED 2026-09-22 — the vector STAYS; do not "optimize" this to an
// unordered_set without new evidence. The scan is not hot, and it is
// structural, not incidental: release()/contains() are called at FRAME
// EXIT for the result slots, never per row or per morsel.
//   ClickBench (46,416 frames): 10 release calls TOTAL, avg scan 1.00
//     elements; 0 contains calls; max 14 tracked.
//   Shapes battery (823 frames): 53 release (avg scan 1.77), 15
//     contains (avg scan 2.13); max 11 tracked.
// At n this small a hash container loses outright — hashing plus node
// allocation plus pointer chasing, against a 1-2 element contiguous
// scan that is already in cache.
//
// The vector also ADMITS DUPLICATES while a set would collapse them.
// That difference is not load-bearing: a duplicate entry would make
// destroy() free the same pointer twice, so a duplicate is a bug in
// either container, never a behaviour to preserve. Measured duplicate
// adopts across all of the above: ZERO.

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
