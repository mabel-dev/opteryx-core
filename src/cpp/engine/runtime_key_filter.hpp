#pragma once
// src/cpp/engine/runtime_key_filter.hpp — the KEY FILTER of a runtime join filter
// (docs/VELOX_BORROW_PLAN.md item 1). Sibling of runtime_bound.hpp: the bound
// carries a build side's ordinal min/max to the probe scan's row-group zone map;
// this carries its KEYS, tested per row inside the probe scan's decode workers so
// a row no build key can match is never decoded for its other columns and never
// reaches an operator.
//
// What it holds: the build side's 64-bit key hashes — the same draken hash the join
// itself treats as key identity (Join2BuildGlobal "Equality is 64-bit hash
// identity"). So the filter can only ever admit a SUPERSET of the rows the join
// matches: a hash collision admits a row the join then drops, and nothing the join
// would match is ever refused. That is the whole correctness argument, and it is
// why every failure below — an unbuildable filter, a column the worker cannot view,
// a type it cannot reconcile — admits every row rather than guessing.
//
// Shape, decided once from the keyed build row count (an upper bound on distinct
// keys):
//   <= kExactMaxKeys   an exact open-addressing set of the hashes (<= 1 MiB);
//   <= kBloomMaxKeys   a split-block bloom, 8 x 32-bit words per 256-bit block,
//                      the Parquet SBBF salts, ~16 bits per key;
//   otherwise          no filter (a build that large rarely rejects much, and the
//                      filter would cost more memory than it saves).
//
// Lifetime: built in Join2BuildSink::finalize, owned by the Join2BuildGlobal (which
// lives for the query), published by Engine::run at the build/probe barrier into a
// RuntimeKeyFilterSlot, read by the probe scan's decode workers only after that.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "core/alloc.h"          // draken_free
#include "core/buffers.h"
#include "core/draken_capi.h"    // draken_hash_view_rows
#include "core/type_width.h"     // draken_fixed_width / draken_is_string_family
#include "core/vector_alloc.h"   // draken_identity_sel
#include "ladder_widening.hpp"   // is_ladder_widening / ladder_widen

namespace opteryx::engine {

struct RuntimeKeyFilter {
    static constexpr uint64_t kExactMaxKeys = 65536;
    static constexpr uint64_t kBloomMaxKeys = 16ull << 20;

    bool exact = false;
    uint64_t keys = 0;           // keyed build rows the filter was built from
    // Exact: slot value 0 means empty; a hash of 0 is recorded in has_zero.
    std::vector<uint64_t> table;
    uint32_t shift = 64;
    bool has_zero = false;
    // Bloom: blocks of 8 words.
    std::vector<uint32_t> words;
    uint64_t nblocks = 0;

    static constexpr uint32_t kSalt[8] = {0x47b6137bu, 0x44974d91u, 0x8824ad5bu, 0xa2b7289du,
                                          0x705495c7u, 0x2df1424bu, 0x9efc4947u, 0x5c6bfb31u};

    uint64_t slot_of(uint64_t h) const { return (h * 0x9E3779B97F4A7C15ull) >> shift; }

    void insert(uint64_t h) {
        if (exact) {
            if (h == 0) { has_zero = true; return; }
            const uint64_t mask = table.size() - 1;
            for (uint64_t s = slot_of(h);; s = (s + 1) & mask) {
                if (table[s] == h) return;
                if (table[s] == 0) { table[s] = h; return; }
            }
        }
        uint32_t* b = words.data() + 8 * (((h >> 32) * nblocks) >> 32);
        const uint32_t x = static_cast<uint32_t>(h);
        for (int i = 0; i < 8; ++i) b[i] |= 1u << ((x * kSalt[i]) >> 27);
    }

    bool contains(uint64_t h) const {
        if (exact) {
            if (h == 0) return has_zero;
            const uint64_t mask = table.size() - 1;
            for (uint64_t s = slot_of(h);; s = (s + 1) & mask) {
                if (table[s] == h) return true;
                if (table[s] == 0) return false;
            }
        }
        const uint32_t* b = words.data() + 8 * (((h >> 32) * nblocks) >> 32);
        const uint32_t x = static_cast<uint32_t>(h);
        for (int i = 0; i < 8; ++i)
            if (!(b[i] & (1u << ((x * kSalt[i]) >> 27)))) return false;
        return true;
    }

    // From the build side's queued per-worker key hashes (keyed rows only — NULL
    // keys are staged apart and never queued). False = no filter.
    bool build(const std::vector<std::vector<uint64_t>>& chunks) {
        uint64_t n = 0;
        for (const auto& c : chunks) n += c.size();
        if (n == 0 || n > kBloomMaxKeys) return false;
        keys = n;
        exact = n <= kExactMaxKeys;
        if (exact) {
            uint64_t cap = 1;
            uint32_t bits = 0;
            while (cap < 2 * n) { cap <<= 1; ++bits; }
            table.assign(cap, 0);
            shift = 64 - bits;
        } else {
            nblocks = std::max<uint64_t>(1, (n * 16 + 255) / 256);
            words.assign(nblocks * 8, 0);
        }
        for (const auto& c : chunks)
            for (uint64_t h : c) insert(h);
        return true;
    }
};

// Engine-owned, address-stable. `filter` is null until the barrier publishes one
// (and stays null when the build produced none): null admits everything.
struct RuntimeKeyFilterSlot {
    const RuntimeKeyFilter* filter = nullptr;
};

// One per armed probe-scan key column, owned by the scan Source.
struct KeyAdmissionCtx {
    // Rows tested before the keep rate can switch the test off for this scan, and the
    // keep rate above which it does. The filter is a superset of the join's matches,
    // so not testing changes no answer — it only stops paying for a test that rejects
    // too little to repay it. Defaults, to be measured, not policy.
    static constexpr uint64_t kProbeRows = 1ull << 20;
    static constexpr double   kKeepOff = 0.95;

    const RuntimeKeyFilter* filter = nullptr;
    DrakenType want = static_cast<DrakenType>(0);   // the PLAN's type of the key column
    std::atomic<uint64_t> tested{0};
    std::atomic<uint64_t> kept{0};
    std::atomic<uint8_t> off{0};

    void reset(const RuntimeKeyFilter* f) {
        filter = f;
        tested.store(0, std::memory_order_relaxed);
        kept.store(0, std::memory_order_relaxed);
        off.store(0, std::memory_order_relaxed);
    }
};

// The decode-worker callback (rugo's KeyAdmitFn): over `n` rows of the decoded key
// column `col`, set bit i of `out_bits` when row i may match. `in_bits` (nullable)
// marks the rows still alive after the filters tested before this one: only those
// are tested, the rest come back 0 — so a scan with several key filters hashes each
// row once per filter it SURVIVES, not once per filter, the way a join chain only
// probes a later join with an earlier one's survivors. 0 = bits written; anything
// else = declined, and the caller keeps every row. GIL-free, no throw.
//
// The column is brought to the PLAN's type first, by the same rules the pass-1
// predicate entry applies (evaluation.pyx opteryx_pass1_predicate_eval): equal type
// as-is; a ladder widening through engine/ladder_widening.hpp; the same bytes under
// another tag (fixed width, or the string family) re-tagged. So the hash computed
// here is the hash the join computes on the same row after the Source emits it.
inline int opteryx_key_admission_eval(void* ctx, DrakenVector* col, uint32_t n,
                                      const uint8_t* in_bits, uint8_t* out_bits) noexcept {
    auto* c = static_cast<KeyAdmissionCtx*>(ctx);
    if (c == nullptr || c->filter == nullptr || n == 0) return 1;
    if (c->off.load(std::memory_order_relaxed)) return 2;
    DrakenVector v = *col;
    VecResult wr{};
    bool widened = false;
    if (v.type != c->want) {
        if (is_ladder_widening(v.type, c->want)) {
            wr = ladder_widen(c->want, col);
            if (wr.data == nullptr) return 3;
            widened = true;
            v.data = wr.data;
            v.selection = wr.selection;
            v.data_length = wr.data_length;
            v.length = wr.length;
            v.validity = wr.validity;
            v.type = wr.type;
            v.flags = wr.flags;
        } else {
            const size_t have_w = draken_fixed_width(v.type);
            const bool same_bytes =
                (have_w != 0 && have_w == draken_fixed_width(c->want)) ||
                (draken_is_string_family(v.type) && draken_is_string_family(c->want));
            if (!same_bytes) return 4;
            v.type = c->want;
        }
    }
    int rc = 0;
    static thread_local std::vector<uint64_t> hashes;
    static thread_local std::vector<uint32_t> rows;
    static thread_local std::vector<uint8_t> packed;
    const RuntimeKeyFilter& f = *c->filter;
    std::memset(out_bits, 0, (static_cast<size_t>(n) + 7) >> 3);
    uint64_t kept = 0;
    uint64_t tested_rows = n;
    const size_t w = draken_fixed_width(v.type);
    auto valid = [&](uint32_t i) {
        // A NULL key matches nothing in the modes this is armed for.
        return v.validity == nullptr || ((v.validity[i >> 3] >> (i & 7)) & 1u);
    };
    if (in_bits != nullptr && w != 0) {
        // Gather the live, non-null keys into a dense vector of the SAME type and
        // hash only those: the hash is per value, so a gathered value hashes to the
        // same 64 bits as it does in place.
        rows.clear();
        for (uint32_t i = 0; i < n; ++i)
            if (((in_bits[i >> 3] >> (i & 7)) & 1u) && valid(i)) rows.push_back(i);
        const uint32_t k = static_cast<uint32_t>(rows.size());
        tested_rows = k;
        if (k > 0) {
            packed.resize(static_cast<size_t>(k) * w);
            const uint8_t* src = static_cast<const uint8_t*>(v.data);
            for (uint32_t j = 0; j < k; ++j)
                std::memcpy(packed.data() + static_cast<size_t>(j) * w,
                            src + static_cast<size_t>(v.selection[rows[j]]) * w, w);
            DrakenVector g{};
            g.data = packed.data();
            g.selection = draken_identity_sel(k);
            g.data_length = k;
            g.length = k;
            g.validity = nullptr;
            g.type = v.type;
            g.flags = 0;
            hashes.resize(k);
            if (g.selection == nullptr || draken_hash_view_rows(&g, hashes.data()) != 0) {
                rc = 5;
            } else {
                for (uint32_t j = 0; j < k; ++j) {
                    if (!f.contains(hashes[j])) continue;
                    out_bits[rows[j] >> 3] |= static_cast<uint8_t>(1u << (rows[j] & 7));
                    ++kept;
                }
            }
        }
    } else {
        hashes.resize(n);
        if (draken_hash_view_rows(&v, hashes.data()) != 0) {
            rc = 5;
        } else {
            for (uint32_t i = 0; i < n; ++i) {
                if (in_bits != nullptr && !((in_bits[i >> 3] >> (i & 7)) & 1u)) continue;
                if (!valid(i) || !f.contains(hashes[i])) continue;
                out_bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                ++kept;
            }
        }
    }
    if (rc == 0) {
        const uint64_t t = c->tested.fetch_add(tested_rows, std::memory_order_relaxed) + tested_rows;
        const uint64_t k = c->kept.fetch_add(kept, std::memory_order_relaxed) + kept;
        if (t >= KeyAdmissionCtx::kProbeRows &&
            static_cast<double>(k) > KeyAdmissionCtx::kKeepOff * static_cast<double>(t))
            c->off.store(1, std::memory_order_relaxed);
    }
    if (widened) {
        draken_free(wr.data);
        if (wr.validity != nullptr && !wr.validity_embedded) draken_free(wr.validity);
        if (wr.owns_selection) draken_free(const_cast<uint32_t*>(wr.selection));
    }
    return rc;
}

}  // namespace opteryx::engine
