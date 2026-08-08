#pragma once
// src/cpp/engine/native_cidr_emit.hpp — turns a Roaring32 set of IPv4 addresses
// into the minimal list of CIDR blocks that covers exactly that set.
//
// MINIMALITY IS NOT A HEURISTIC HERE. For any set of addresses the minimal exact
// cover is UNIQUE: it is the set of maximal full nodes of the binary trie over
// the address space — for every address, walk up until the parent block is not
// wholly contained in the set, and that is its block. So there is nothing to
// tune and nothing to compare implementations on except cost; any correct
// algorithm emits byte-identical output. That also makes this trivially
// testable against a brute-force reference.
//
// THE ALGORITHM is two steps, both linear:
//
//   1. Walk the set in address order and coalesce into maximal RUNS of
//      consecutive addresses. Runs cross container boundaries — a run ending at
//      x.y.255.255 continues into the next /16 — so this is a single stream, not
//      a per-container operation.
//   2. Split each run greedily into CIDRs. At position `s` the block emitted is
//      the largest power of two that BOTH `s` is aligned for and the remaining
//      span can hold: `min(s & -s, 2^floor(log2(e - s + 1)))`. Two constraints,
//      take the tighter. That is the minimal cover of a range, in closed form —
//      no search over prefix lengths.
//
// An earlier design used a two-level AND-pyramid (one over the full-container
// bitmap for /0../16, one within each container for /17../32). It produces the
// same output — it has to, by uniqueness — but needs two code paths and a
// separate maximality rule for each. Runs collapse both into one: a full /16 is
// simply a run of 65536, and 256 adjacent full /16s are one run of 2^24 that
// splits into a single /8. Density costs nothing because run extraction reads
// whole 64-bit words and an all-ones word extends the current run by 64 without
// touching a bit.
//
// OUTPUT SIZE IS NOT BOUNDED BY THE INPUT SIZE in any useful way. The degenerate
// input is HALF density, not full: every even address (2^31 of them) has no
// complete buddy anywhere, so nothing folds and the cover is 2^31 distinct /32s
// — roughly 36GiB of text. Adding the odd addresses REDUCES that to a single
// /0. So the emit side needs its own budget; the collection budget being
// satisfied says nothing about the output fitting, and uniform-random input at
// high density lands within ~1.4x of the degenerate case rather than safely far
// from it.
//
// Nothing here allocates. `emit_cidrs` pushes to a caller-supplied sink which
// returns false to stop (that is how the caller enforces its budget mid-emit
// rather than sizing up front — the size is not knowable up front).

#include <atomic>
#include <cstdint>

#include "core/ipv4.h"
#include "engine/agg_budgets.hpp"
#include "engine/native_roaring32.hpp"

namespace opteryx {
namespace cidr {

// ---------------------------------------------------------------------------
// Emit budget — a ceiling on CIDR TEXT, separate from the collection budget in
// native_roaring32.hpp and not derivable from it (see agg_budgets.hpp).
//
// Charged for the duration of one emit and released when that emit finishes:
// unlike the collection budget, which reserves memory the aggregate holds
// across the whole query, this bounds a single burst of output. Emits from
// different partitions run concurrently, so the counter is global — the ceiling
// is on total text in flight, not on any one group.
// ---------------------------------------------------------------------------
constexpr int64_t kEmitBudgetBytes = opteryx::agg_budgets::kCidrAggEmitBytes;

inline std::atomic<int64_t>& emit_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
}

inline bool emit_budget_take(int64_t delta) noexcept {
    if (delta <= 0) return true;
    if (emit_budget_used().fetch_add(delta) + delta > kEmitBudgetBytes) {
        emit_budget_used().fetch_sub(delta);
        return false;
    }
    return true;
}

inline void emit_budget_give(int64_t delta) noexcept {
    if (delta > 0) emit_budget_used().fetch_sub(delta);
}

using opteryx::roaring32::Container;
using opteryx::roaring32::Roaring32;
using opteryx::roaring32::kBitmapWords;

// "255.255.255.255/32" — the address plus "/nn".
constexpr uint32_t kMaxCidrTextBytes = draken::ipv4::MAX_CIDR_TEXT_LENGTH;

// Bytes `format_cidr` will write, without writing them. Lets a caller charge an
// exact byte budget BEFORE producing the text, rather than emitting first and
// discovering the overrun after the allocation.
inline uint32_t cidr_text_length(uint32_t base, uint8_t prefix) noexcept {
    return draken::ipv4::text_length(base) + (prefix >= 10 ? 3u : 2u);
}

// Render one block. Returns the byte count written; `out` needs
// kMaxCidrTextBytes. No terminator is written — callers copy by length into
// their own string storage.
//
// The address half goes through draken::ipv4::format rather than a local octet
// loop: core/ipv4.h is by its own declaration the ONE place the uint32 <-> IPv4
// mapping is written down, precisely so a change there cannot reach the cast
// kernels and the containment kernels but miss a renderer that reimplemented it.
inline uint32_t format_cidr(uint32_t base, uint8_t prefix, char* out) noexcept {
    uint32_t n = draken::ipv4::format(base, out);
    out[n++] = '/';
    if (prefix >= 10) out[n++] = static_cast<char>('0' + prefix / 10);
    out[n++] = static_cast<char>('0' + prefix % 10);
    return n;
}

// Split the inclusive range [start, end] into the minimal CIDR list.
//
// Arithmetic is 64-bit throughout because the range can be the whole address
// space: a block of size 2^32 and the cursor stepping to 2^32 both overflow
// uint32, and a wrapped cursor would silently re-emit from zero forever.
template <typename Sink>
inline bool split_range(uint32_t start, uint32_t end, Sink&& sink) noexcept {
    uint64_t s = start;
    const uint64_t e = end;
    while (s <= e) {
        // Largest block this base is aligned for. Base 0 is aligned for
        // everything, which `s & -s` reports as 0 rather than 2^32.
        const uint64_t align = (s == 0) ? (1ULL << 32) : (s & (0ULL - s));
        const uint64_t span  = e - s + 1;
        const uint64_t fits  = 1ULL << (63 - __builtin_clzll(span));
        const uint64_t use   = align < fits ? align : fits;
        const uint8_t prefix = static_cast<uint8_t>(32 - __builtin_ctzll(use));
        if (!sink(static_cast<uint32_t>(s), prefix)) return false;
        s += use;
    }
    return true;
}

// Walk one container's values in ascending order, calling
// `on_value(uint32_t absolute_address)`. Bitmap containers are read a word at a
// time so an all-ones word costs one popcount-free loop rather than 64 tests.
template <typename OnValue>
inline bool _walk_container(const Container& c, uint32_t base, OnValue&& on_value) noexcept {
    if (!c.bitmap) {
        for (uint16_t v : c.arr) {
            if (!on_value(base | static_cast<uint32_t>(v))) return false;
        }
        return true;
    }
    for (uint32_t w = 0; w < kBitmapWords; ++w) {
        uint64_t word = c.words[w];
        while (word) {
            const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(word));
            if (!on_value(base | (w << 6) | bit)) return false;
            word &= word - 1;
        }
    }
    return true;
}

// Emit the minimal CIDR cover of `r` in ascending address order.
//
// `sink(uint32_t base, uint8_t prefix)` returns false to abort; this then
// returns false having emitted a prefix of the list. An aborted emit is a
// PARTIAL cover and the caller must treat it as a failure, never as a result —
// a truncated CIDR list is a smaller set than the data holds and nothing
// downstream can tell.
template <typename Sink>
inline bool emit_cidrs(const Roaring32& r, Sink&& sink) noexcept {
    bool have_run = false;
    uint32_t run_start = 0;
    uint32_t run_end = 0;
    bool ok = true;

    for (size_t i = 0; i < r.keys.size() && ok; ++i) {
        const Container& c = r.conts[r.slots[i]];
        if (c.cardinality == 0) continue;
        const uint32_t base = static_cast<uint32_t>(r.keys[i]) << 16;

        ok = _walk_container(c, base, [&](uint32_t v) noexcept {
            if (have_run && v == run_end + 1) {   // extends the open run
                run_end = v;
                return true;
            }
            if (have_run && !split_range(run_start, run_end, sink)) return false;
            have_run = true;
            run_start = v;
            run_end = v;
            return true;
        });
    }

    if (!ok) return false;
    if (have_run && !split_range(run_start, run_end, sink)) return false;
    return true;
}

}} // namespace opteryx::cidr
