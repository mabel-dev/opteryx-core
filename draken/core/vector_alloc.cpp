// Lazy-grown global selection vectors and DrakenVector constructors.
//
// Each draken extension .so has its own copy of these globals (symbol
// visibility is default-hidden; there is no cross-SO sharing). Owned-vs-shared
// discrimination is tracked by VectorOwner in the binding layer, never by
// pointer comparison against these symbols.
//
// Growth is lock-protected; reads after the pointer is obtained are lock-free
// via std::atomic acquire/release. Old backing buffers are intentionally
// LEAKED on growth: other threads may still hold pointers into them, and the
// process-lifetime cost is O(log N) allocations total.
#include "core/buffers.h"
#include "core/alloc.h"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>

#ifdef __cplusplus
extern "C" {
#endif

// Non-NULL sentinel for length == 0. No loop will dereference it.
static const uint32_t g_sel_sentinel = 0;

// ---------- Identity permutation: selection[i] == i ----------

static std::mutex             g_identity_mutex;
static std::atomic<uint32_t>  g_identity_cap{0};
static std::atomic<const uint32_t*> g_identity_ptr{nullptr};

const uint32_t* draken_identity_sel(uint32_t length) {
    if (length == 0) return &g_sel_sentinel;

    // Lock-free fast path.
    if (g_identity_cap.load(std::memory_order_acquire) >= length)
        return g_identity_ptr.load(std::memory_order_acquire);

    // Slow path: grow under mutex.
    std::lock_guard<std::mutex> lk(g_identity_mutex);
    uint32_t cap = g_identity_cap.load(std::memory_order_relaxed);
    if (cap >= length)
        return g_identity_ptr.load(std::memory_order_relaxed);

    uint32_t new_cap = (cap == 0) ? 1024u : cap;
    while (new_cap < length) new_cap *= 2;

    uint32_t* buf = static_cast<uint32_t*>(
        draken_malloc(static_cast<size_t>(new_cap) * sizeof(uint32_t)));
    if (!buf) std::terminate();  // unrecoverable: OOM on a global buffer

    for (uint32_t i = 0; i < new_cap; ++i) buf[i] = i;

    // Old buffer leaked intentionally (other threads may hold pointers).
    g_identity_ptr.store(buf, std::memory_order_release);
    g_identity_cap.store(new_cap, std::memory_order_release);
    return buf;
}

// ---------- Zero selection: selection[i] == 0 ----------

static std::mutex             g_zero_mutex;
static std::atomic<uint32_t>  g_zero_cap{0};
static std::atomic<const uint32_t*> g_zero_ptr{nullptr};

const uint32_t* draken_zero_sel(uint32_t length) {
    if (length == 0) return &g_sel_sentinel;

    if (g_zero_cap.load(std::memory_order_acquire) >= length)
        return g_zero_ptr.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(g_zero_mutex);
    uint32_t cap = g_zero_cap.load(std::memory_order_relaxed);
    if (cap >= length)
        return g_zero_ptr.load(std::memory_order_relaxed);

    uint32_t new_cap = (cap == 0) ? 1024u : cap;
    while (new_cap < length) new_cap *= 2;

    uint32_t* buf = static_cast<uint32_t*>(
        draken_malloc(static_cast<size_t>(new_cap) * sizeof(uint32_t)));
    if (!buf) std::terminate();

    std::memset(buf, 0, static_cast<size_t>(new_cap) * sizeof(uint32_t));

    g_zero_ptr.store(buf, std::memory_order_release);
    g_zero_cap.store(new_cap, std::memory_order_release);
    return buf;
}

// ---------- Zero validity bitmap: every bit == 0 (all null) ----------
// Size is ceil(length/8) padded to a multiple of 8 bytes for SIMD safety.

static std::mutex             g_validity_mutex;
static std::atomic<uint32_t>  g_validity_cap_bytes{0};
static std::atomic<const uint8_t*> g_validity_ptr{nullptr};

const uint8_t* draken_zero_validity(uint32_t length) {
    if (length == 0) return reinterpret_cast<const uint8_t*>(&g_sel_sentinel);

    uint32_t needed = ((((length + 7u) / 8u) + 7u) & ~7u);  // SIMD-padded bytes

    if (g_validity_cap_bytes.load(std::memory_order_acquire) >= needed)
        return g_validity_ptr.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(g_validity_mutex);
    uint32_t cap = g_validity_cap_bytes.load(std::memory_order_relaxed);
    if (cap >= needed)
        return g_validity_ptr.load(std::memory_order_relaxed);

    uint32_t new_cap = (cap == 0) ? 1024u : cap;
    while (new_cap < needed) new_cap *= 2;

    uint8_t* buf = static_cast<uint8_t*>(draken_malloc(new_cap));
    if (!buf) std::terminate();

    std::memset(buf, 0, new_cap);

    g_validity_ptr.store(buf, std::memory_order_release);
    g_validity_cap_bytes.store(new_cap, std::memory_order_release);
    return buf;
}

// ---------- DrakenVector constructors ----------

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
    v.flags       = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
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
    v.flags       = 0;
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
    v.flags       = 0;
    return v;
}

#ifdef __cplusplus
}  // extern "C"
#endif
