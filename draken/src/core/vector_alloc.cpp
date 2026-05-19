// vector_alloc.cpp — implementation of the unified DrakenVector constructors
// and the lazy-grown global identity/zero selection buffers.
//
// Threading model: the codebase is freethreading_compatible — Cython kernels
// run without the GIL. The fast path (capacity sufficient) is lock-free via
// atomic loads. The slow path (grow) takes a mutex and is rare (O(log N) per
// process lifetime). Buffers are never freed; old buffers leak intentionally
// because other threads may still hold pointers to them.

#include "vector_alloc.h"
#include <atomic>
#include <mutex>
#include <cstdlib>
#include <cstring>

namespace {

constexpr uint32_t INITIAL_CAPACITY = 4096;

std::atomic<const uint32_t*> g_identity_buf{nullptr};
std::atomic<uint32_t>        g_identity_cap{0};
std::atomic<const uint32_t*> g_zero_buf{nullptr};
std::atomic<uint32_t>        g_zero_cap{0};
std::mutex                   g_grow_mutex;

uint32_t next_capacity(uint32_t current, uint32_t required) {
    uint32_t cap = current ? current : INITIAL_CAPACITY;
    while (cap < required) {
        // Saturate at UINT32_MAX rather than overflow.
        if (cap > (UINT32_MAX / 2)) {
            cap = required;
            break;
        }
        cap *= 2;
    }
    return cap;
}

} // namespace

extern "C" {

const uint32_t* draken_identity_sel(uint32_t length) {
    if (length == 0) {
        // Avoid returning nullptr for empty vectors: a length-0 vector's
        // selection is never indexed, but downstream code may still want a
        // non-NULL pointer for assertions. Return the existing buffer if any.
        const uint32_t* buf = g_identity_buf.load(std::memory_order_acquire);
        if (buf) return buf;
        // Fall through to allocate the initial buffer.
    } else if (length <= g_identity_cap.load(std::memory_order_acquire)) {
        return g_identity_buf.load(std::memory_order_acquire);
    }

    std::lock_guard<std::mutex> lock(g_grow_mutex);
    uint32_t cap = g_identity_cap.load(std::memory_order_relaxed);
    if (length <= cap && cap > 0) {
        return g_identity_buf.load(std::memory_order_relaxed);
    }

    uint32_t new_cap = next_capacity(cap, length ? length : INITIAL_CAPACITY);
    uint32_t* new_buf = static_cast<uint32_t*>(std::malloc(static_cast<size_t>(new_cap) * sizeof(uint32_t)));
    if (!new_buf) {
        // Out of memory — return existing buffer (may be NULL on first call).
        return g_identity_buf.load(std::memory_order_relaxed);
    }
    for (uint32_t i = 0; i < new_cap; ++i) {
        new_buf[i] = i;
    }
    g_identity_buf.store(new_buf, std::memory_order_release);
    g_identity_cap.store(new_cap, std::memory_order_release);
    // Old buffer intentionally leaked — other threads may still reference it.
    return new_buf;
}

const uint32_t* draken_zero_sel(uint32_t length) {
    if (length == 0) {
        const uint32_t* buf = g_zero_buf.load(std::memory_order_acquire);
        if (buf) return buf;
    } else if (length <= g_zero_cap.load(std::memory_order_acquire)) {
        return g_zero_buf.load(std::memory_order_acquire);
    }

    std::lock_guard<std::mutex> lock(g_grow_mutex);
    uint32_t cap = g_zero_cap.load(std::memory_order_relaxed);
    if (length <= cap && cap > 0) {
        return g_zero_buf.load(std::memory_order_relaxed);
    }

    uint32_t new_cap = next_capacity(cap, length ? length : INITIAL_CAPACITY);
    uint32_t* new_buf = static_cast<uint32_t*>(std::calloc(new_cap, sizeof(uint32_t)));
    if (!new_buf) {
        return g_zero_buf.load(std::memory_order_relaxed);
    }
    g_zero_buf.store(new_buf, std::memory_order_release);
    g_zero_cap.store(new_cap, std::memory_order_release);
    return new_buf;
}

DrakenVector draken_vector_from_dense(
    void* data, uint32_t length, DrakenType type, uint8_t* validity)
{
    DrakenVector v;
    v.data        = data;
    v.selection   = draken_identity_sel(length);
    v.data_length = length;
    v.length      = length;
    v.validity    = validity;
    v.type        = type;
    return v;
}

DrakenVector draken_vector_from_constant(
    void* data, uint32_t length, DrakenType type, uint8_t* validity)
{
    DrakenVector v;
    v.data        = data;
    v.selection   = draken_zero_sel(length);
    v.data_length = 1;
    v.length      = length;
    v.validity    = validity;
    v.type        = type;
    return v;
}

DrakenVector draken_vector_from_dict(
    void* data, uint32_t data_length,
    const uint32_t* codes, uint32_t length,
    DrakenType type, uint8_t* validity)
{
    DrakenVector v;
    v.data        = data;
    v.selection   = codes;
    v.data_length = data_length;
    v.length      = length;
    v.validity    = validity;
    v.type        = type;
    return v;
}

} // extern "C"
