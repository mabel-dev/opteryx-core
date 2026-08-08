#pragma once
// src/cpp/engine/native_roaring32.hpp — a native Roaring bitmap over uint32,
// built as the collection state for set-valued aggregates (the CIDR aggregate
// is the first caller).
//
// This is NOT a general Roaring library and deliberately does not aim to be.
// The full CRoaring surface (intersection, difference, xor, rank/select,
// frozen views, a serialisation format) exists to support set algebra and
// on-disk interchange. Neither applies here: the bitmap is transient
// aggregate state and the only thing that ever leaves the process is the
// emitted CIDR list. Three operations are therefore all that is implemented:
//
//   add(v)         — insert one value (dedup is inherent)
//   merge_from(o)  — union, the partial-aggregation combine
//   ordered walk   — drives the CIDR emit (see native_cidr_emit.hpp)
//
// Anything beyond those three is absent on purpose. Adding set algebra here
// without a caller that needs it would be dead code.
//
// STRUCTURE. Values are partitioned on their high 16 bits, so every container
// covers exactly one /16 of the address space — the partition IS the CIDR
// hierarchy, which is what makes the emit pass cheap. Within a container the
// low 16 bits are stored either as a sorted uint16 array (sparse) or as a
// 65536-bit bitmap (dense). The crossover is 4096 values, where a uint16 array
// reaches the 8KB a bitmap costs outright; past that the array is strictly
// worse on both size and lookup.
//
// Run containers (the third CRoaring type) are NOT implemented. They earn
// their keep on contiguous input — an entire /8 present collapses from 256
// full bitmap containers (2MB) to a handful of run pairs — but they also
// multiply the container type-pairs `merge_from` has to handle. The emit pass
// coalesces runs anyway, so their only benefit here is state memory under one
// specific pathology. That is a measurement question, not an assumption:
// build without, measure, add if the numbers say so.
//
// MEMORY. A single Roaring32 is bounded by construction — every container is
// at most 8KB and there are at most 65536 of them, so one set cannot exceed
// ~512MB no matter how many rows feed it, and it converges on that figure
// only when the whole IPv4 space is present. That bound is PER SET, not per
// query: a GROUP BY produces one set per group and the group count is
// unbounded, so the real exposure is the total. This charges a global byte
// budget in the same shape MedianState uses (see _agg_kernels.hpp) — charged
// on capacity growth, released on free, latching `overflowed` and refusing
// further inserts past the ceiling so the sink can raise at finalize. Silent
// truncation would report a smaller set than the data holds, which is a wrong
// answer wearing a green tick.

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <vector>

#include "engine/agg_budgets.hpp"

namespace opteryx {
namespace roaring32 {

// Crossover between the two container encodings. 4096 * sizeof(uint16) == 8192
// == kBitmapWords * sizeof(uint64): the point where a sorted array costs what
// the bitmap costs unconditionally.
constexpr uint32_t kArrayMax        = 4096;
constexpr uint32_t kBitmapWords     = 1024;    // 1024 * 64 == 65536 bits
constexpr uint32_t kContainerValues = 65536;   // one /16

// Shared with variables.py's SHOW VARIABLES report — see engine/agg_budgets.hpp
// for why the value lives there and the counter below stays here.
constexpr int64_t kSetBudgetBytes = opteryx::agg_budgets::kCidrAggStateBytes;

// Per-shared-object counter, matching the MedianState convention: the one
// extension that actually executes the aggregate accounts against a single
// instance.
inline std::atomic<int64_t>& set_budget_used() noexcept {
    static std::atomic<int64_t> used{0};
    return used;
}

// Reserve `delta` bytes against the global budget. Returns false (and reserves
// nothing) if that would breach the ceiling.
inline bool budget_take(int64_t delta) noexcept {
    if (delta <= 0) return true;
    if (set_budget_used().fetch_add(delta) + delta > kSetBudgetBytes) {
        set_budget_used().fetch_sub(delta);
        return false;
    }
    return true;
}

inline void budget_give(int64_t delta) noexcept {
    if (delta > 0) set_budget_used().fetch_sub(delta);
}

// ---------------------------------------------------------------------------
// Container — one /16. Exactly one lane is live, selected by `bitmap`.
//
// `cardinality` is authoritative for both lanes and is maintained on every
// mutation rather than recomputed: the emit pass tests fullness per container
// (cardinality == kContainerValues) once per /16, and a popcount sweep there
// would be 1024 words of work to answer a question a counter already knows.
// ---------------------------------------------------------------------------
struct Container {
    std::vector<uint16_t> arr;     // sorted, unique — live when !bitmap
    std::vector<uint64_t> words;   // kBitmapWords    — live when  bitmap
    uint32_t cardinality = 0;
    bool     bitmap      = false;

    inline bool full() const noexcept { return cardinality == kContainerValues; }

    inline bool contains(uint16_t v) const noexcept {
        if (bitmap) return (words[v >> 6] >> (v & 63)) & 1ULL;
        return std::binary_search(arr.begin(), arr.end(), v);
    }

    // Bytes this container currently holds against the budget. Capacity, not
    // size — the budget must track what is actually reserved from the
    // allocator, otherwise a vector that doubled to 2x its logical size is
    // charged for half of what it holds.
    inline int64_t charged_bytes() const noexcept {
        return static_cast<int64_t>(arr.capacity()) * static_cast<int64_t>(sizeof(uint16_t))
             + static_cast<int64_t>(words.capacity()) * static_cast<int64_t>(sizeof(uint64_t));
    }

    // Convert the sparse lane to a bitmap. Returns false if the budget refuses
    // the 8KB, leaving the container untouched and still usable as an array.
    inline bool promote() noexcept {
        if (bitmap) return true;
        if (!budget_take(static_cast<int64_t>(kBitmapWords) * static_cast<int64_t>(sizeof(uint64_t)))) {
            return false;
        }
        words.assign(kBitmapWords, 0ULL);
        for (uint16_t v : arr) words[v >> 6] |= (1ULL << (v & 63));
        budget_give(static_cast<int64_t>(arr.capacity()) * static_cast<int64_t>(sizeof(uint16_t)));
        arr.clear();
        arr.shrink_to_fit();
        bitmap = true;
        return true;
    }

    // Insert one value. Returns 1 if newly added, 0 if already present, -1 if
    // the budget refused the growth. A -1 is NOT "already present" — the
    // caller must latch overflow rather than treat it as a no-op, or the set
    // silently loses values.
    inline int add(uint16_t v) noexcept {
        if (bitmap) {
            uint64_t& w = words[v >> 6];
            const uint64_t bit = 1ULL << (v & 63);
            if (w & bit) return 0;
            w |= bit;
            ++cardinality;
            return 1;
        }

        auto it = std::lower_bound(arr.begin(), arr.end(), v);
        if (it != arr.end() && *it == v) return 0;

        if (cardinality >= kArrayMax) {
            // Past the crossover — switch encoding, then set the bit. `it` is
            // dead after promote(); the bitmap path does its own lookup.
            if (!promote()) return -1;
            return add(v);
        }

        // Charge the reallocation before it happens; vector::insert would
        // otherwise commit memory the budget has not approved.
        if (arr.size() == arr.capacity()) {
            const size_t next = arr.capacity() == 0 ? 16 : arr.capacity() * 2;
            const int64_t delta = static_cast<int64_t>(next - arr.capacity())
                                * static_cast<int64_t>(sizeof(uint16_t));
            if (!budget_take(delta)) return -1;
            const size_t offset = static_cast<size_t>(it - arr.begin());
            arr.reserve(next);
            it = arr.begin() + static_cast<std::ptrdiff_t>(offset);
        }
        arr.insert(it, v);
        ++cardinality;
        return 1;
    }

    // Union `o` into this container. Returns false if the budget refused.
    inline bool merge_from(const Container& o) noexcept {
        if (o.cardinality == 0) return true;

        if (bitmap && o.bitmap) {
            uint32_t card = 0;
            for (uint32_t i = 0; i < kBitmapWords; ++i) {
                words[i] |= o.words[i];
                card += static_cast<uint32_t>(__builtin_popcountll(words[i]));
            }
            cardinality = card;
            return true;
        }

        if (bitmap) {   // o is an array
            for (uint16_t v : o.arr) {
                uint64_t& w = words[v >> 6];
                const uint64_t bit = 1ULL << (v & 63);
                if (!(w & bit)) { w |= bit; ++cardinality; }
            }
            return true;
        }

        // This side is sparse. If the union cannot possibly stay under the
        // crossover, promote once up front rather than reallocating the array
        // repeatedly on the way there.
        if (o.bitmap || cardinality + o.cardinality > kArrayMax) {
            if (!promote()) return false;
            return merge_from(o);
        }

        for (uint16_t v : o.arr) {
            const int r = add(v);
            if (r < 0) return false;
        }
        return true;
    }
};

// ---------------------------------------------------------------------------
// Roaring32 — the container index.
//
// `keys` is kept sorted so the emit pass can walk /16s in address order
// without a sort, and so lookup is a binary search rather than a scan. It is
// indirected through `slots` so an insert in the middle memmoves 6 bytes per
// displaced entry (a uint16 key and a uint32 slot) instead of relocating
// Container objects.
//
// A dense 65536-entry direct index would make lookup O(1) with no search at
// all, and for a single whole-column aggregate that is the better structure.
// It is rejected because the grouped path holds one Roaring32 PER GROUP: at
// ~3.7MB of index per set, ten thousand groups is 37GB of headers before a
// single value is stored. The index has to be proportional to content.
// ---------------------------------------------------------------------------
struct Roaring32 {
    std::vector<uint16_t>  keys;    // sorted high-16 partition keys
    std::vector<uint32_t>  slots;   // parallel to keys → index into conts
    std::vector<Container> conts;   // stable under slot indices (not pointers)
    uint64_t total = 0;             // distinct values held
    bool overflowed = false;        // budget refused an insert; sink must raise

    Roaring32() noexcept = default;
    Roaring32(const Roaring32&) = delete;
    Roaring32& operator=(const Roaring32&) = delete;
    Roaring32(Roaring32&&) noexcept = default;
    Roaring32& operator=(Roaring32&&) noexcept = default;

    // Index overhead charged per partition: the Container header itself plus
    // its key and slot entries. Bounded at ~4MB per set (65536 partitions),
    // which is noise for one whole-column aggregate and very much not noise
    // across a large GROUP BY — so it is charged rather than waved through.
    static constexpr int64_t kIndexBytesPerContainer =
        static_cast<int64_t>(sizeof(Container)) + 6;

    ~Roaring32() noexcept {
        for (const Container& c : conts) budget_give(c.charged_bytes());
        budget_give(static_cast<int64_t>(conts.size()) * kIndexBytesPerContainer);
    }

    // Most recent partition, cached. Real IP columns cluster — consecutive
    // rows frequently share a /16 — so this collapses the common case to one
    // comparison. Uniformly random input never hits it and pays the binary
    // search, which is the honest cost of that input, not a regression.
    uint16_t cache_key  = 0;
    uint32_t cache_slot = 0;
    bool     cache_live = false;

    inline uint64_t cardinality() const noexcept { return total; }

    static constexpr uint32_t kNoSlot = 0xFFFFFFFFu;

    // Locate the partition for `hi`, creating it if absent. Returns kNoSlot if
    // creating it would breach the budget — never a slot the caller could
    // mistake for a successful lookup.
    //
    // Charging lives HERE and in Container, each owning the bytes it actually
    // reserves, with no second reconciliation pass over the same allocation:
    // two places charging one growth double-counts, and a budget that
    // over-reports refuses queries that would have fit.
    inline uint32_t _slot_for(uint16_t hi) noexcept {
        if (cache_live && cache_key == hi) return cache_slot;

        auto it = std::lower_bound(keys.begin(), keys.end(), hi);
        const size_t pos = static_cast<size_t>(it - keys.begin());
        uint32_t slot;
        if (it != keys.end() && *it == hi) {
            slot = slots[pos];
        } else {
            if (!budget_take(kIndexBytesPerContainer)) return kNoSlot;
            slot = static_cast<uint32_t>(conts.size());
            conts.emplace_back();
            keys.insert(keys.begin() + static_cast<std::ptrdiff_t>(pos), hi);
            slots.insert(slots.begin() + static_cast<std::ptrdiff_t>(pos), slot);
        }
        cache_key = hi; cache_slot = slot; cache_live = true;
        return slot;
    }

    // Insert one value. Returns false once the budget has refused — the caller
    // latches and the sink raises at finalize. Never silently drops.
    inline bool add(uint32_t v) noexcept {
        const uint32_t slot = _slot_for(static_cast<uint16_t>(v >> 16));
        if (slot == kNoSlot) { overflowed = true; return false; }
        const int r = conts[slot].add(static_cast<uint16_t>(v & 0xFFFF));
        if (r < 0) { overflowed = true; return false; }
        total += static_cast<uint64_t>(r);
        return true;
    }

    // Union `o` into this set — the partial-aggregation combine.
    inline bool merge_from(const Roaring32& o) noexcept {
        if (o.overflowed) overflowed = true;
        for (size_t i = 0; i < o.keys.size(); ++i) {
            const Container& src = o.conts[o.slots[i]];
            if (src.cardinality == 0) continue;
            const uint32_t slot = _slot_for(o.keys[i]);
            if (slot == kNoSlot) { overflowed = true; return false; }
            Container& dst = conts[slot];
            const uint32_t had = dst.cardinality;
            const bool ok = dst.merge_from(src);
            total += static_cast<uint64_t>(dst.cardinality - had);
            if (!ok) { overflowed = true; return false; }
        }
        return true;
    }
};

}} // namespace opteryx::roaring32
