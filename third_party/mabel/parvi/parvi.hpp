#pragma once

// Parvi: a tiny fixed-capacity hash map for low-cardinality keying.
//
// Design goals
// ------------
//   * Zero heap allocations — all storage is inline.
//   * Single SIMD-group probe: 64 slots arranged as 4 groups of 16; the
//     probed group is selected from the key (bits 53–54, disjoint from the
//     tag bits 57–63), so every lookup is still one 16-byte compare + one
//     empty-check — no iterative probing, no spill to neighbouring groups.
//   * API-compatible with opteryx::carchar::CarcharIndex for the hot path
//     (lookup_fast / insert_new / find_or_insert_id / find_or_insert /
//     items) so callers can promote to Carchar in one step when the
//     low-cardinality estimate was wrong.
//
// Capacity & promotion
// --------------------
//   * kCapacity is 64 slots, but a key can only live in its selected group:
//     overflow fires when THAT group is full, not when all 64 slots are.
//     With hashed keys (balls-in-bins over 4 groups of 16) the measured
//     effective capacity before first overflow is ~40–56 distinct keys
//     (p5 = 40 — planner eligibility gates on 40, see
//     opteryx/planner/optimizer/strategies/hash_map_variant.py).
//   * There is deliberately NO spill probing to the next group: spilling
//     recreates Carchar's probe loop and forfeits the flat-cost guarantee.
//     A full group promotes instead.
//   * `drain_into(CarcharIndex&)` / `drain_into(CarcharSet&)` copy the live
//     entries for promotion, preserving keys and payload refs.
//   * `insert_new` / `find_or_insert` / `find_or_insert_id` /
//     `insert_or_ignore` signal overflow so callers can swap to Carchar
//     without silent corruption.
//   * `full()` remains "all 64 slots occupied"; overflow can arrive before
//     full() is true (group-full). Promotion decisions must key off the
//     overflow signals, never off full().
//
// Layout (4 groups × 16 slots):
//   control_[i]      : kEmpty (0x80) or tag (top 7 bits of key)
//   hashes_[i]       : full 64-bit key
//   payload_refs_[i] : int64 payload (ParviMap only)
//
// Keys must not use 0x80 as their top control byte — we reuse carchar's
// key_tag() which masks the top bit, so this is handled for us.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "../carchar/carchar_common.hpp"
#include "../carchar/carchar_index.hpp"
#include "../carchar/carchar_set.hpp"
#include "../carchar/carchar_simd.hpp"

#if defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

namespace opteryx::parvi {

using ::opteryx::carchar::kEmpty;
using ::opteryx::carchar::key_tag;

inline constexpr std::size_t kGroupSlots = 16;
inline constexpr std::size_t kGroupCount = 4;
inline constexpr std::size_t kCapacity = kGroupCount * kGroupSlots;  // 64
static_assert((kGroupCount & (kGroupCount - 1U)) == 0U, "group count must be a power of two");

// Group selection uses bits 53–54: disjoint from the tag bits (57–63, see
// carchar key_tag) and comfortably above Carchar's low slot-index bits, so a
// promoted Carchar re-derives everything independently.
inline std::size_t group_base(std::uint64_t key) noexcept {
    return static_cast<std::size_t>((key >> 53U) & (kGroupCount - 1U)) * kGroupSlots;
}

struct ParviResult {
    std::size_t slot = 0;
    bool        found = false;
};

// Outcome of ParviMap::find_or_insert_id — mirrors the three-way contract of
// CarcharIndex::find_or_insert_id plus the fixed-capacity overflow case.
enum class ParviInsert : std::uint8_t {
    kFound    = 0,  // key existed; payload_out holds its payload
    kInserted = 1,  // new_id inserted; payload_out == new_id
    kFull     = 2,  // key absent and its group full — caller promotes to Carchar
};

// ------------------------------------------------------------------
// Performance improvement #1: faster scalar mask extraction
//
// The old scalar fallback looped over 8 bytes twice. Now we use a
// 64-bit multiply to gather high bits into a single byte per 8-byte
// chunk. This is branchless and reduces ~16 iterations to 2 multiplications.
//
// Operates on ONE 16-byte group (`control` points at the group base).
// ------------------------------------------------------------------
inline std::uint32_t group_match_mask(const std::uint8_t* control, std::uint8_t needle) noexcept {
#if defined(__AVX2__)
    const __m128i group    = _mm_loadu_si128(reinterpret_cast<const __m128i*>(control));
    const __m128i needle_v = _mm_set1_epi8(static_cast<char>(needle));
    return static_cast<std::uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(group, needle_v)));
#elif defined(__riscv) && defined(__riscv_vector)
    static constexpr std::uint8_t kPowersArr[16] = {1, 2, 4, 8, 16, 32, 64, 128,
                                                    1, 2, 4, 8, 16, 32, 64, 128};
    alignas(16) std::uint8_t masked_bytes[16] = {};
    const std::size_t vl = __riscv_vsetvl_e8m4(kGroupSlots);
    if (vl < kGroupSlots) {
        using ::opteryx::carchar::detail::kByteHighBits64;
        using ::opteryx::carchar::detail::load_u64;
        using ::opteryx::carchar::detail::match_mask64;
        const std::uint64_t lo = load_u64(control);
        const std::uint64_t hi = load_u64(control + 8U);
        const std::uint64_t m_lo = needle == kEmpty ? (lo & kByteHighBits64) : match_mask64(lo, needle);
        const std::uint64_t m_hi = needle == kEmpty ? (hi & kByteHighBits64) : match_mask64(hi, needle);
        constexpr std::uint64_t kHighBitMask = 0x8080808080808080ULL;
        constexpr std::uint64_t kGather = 0x8040201008040201ULL;
        const auto to_mask = [](std::uint64_t bits) -> std::uint8_t {
            return static_cast<std::uint8_t>(((bits & kHighBitMask) * kGather) >> 56);
        };
        return static_cast<std::uint32_t>(to_mask(m_lo)) |
               (static_cast<std::uint32_t>(to_mask(m_hi)) << 8U);
    }
    const vuint8m4_t group = __riscv_vle8_v_u8m4(control, vl);
    const vuint8m4_t powers = __riscv_vle8_v_u8m4(kPowersArr, vl);
    const vuint8m4_t zero = __riscv_vmv_v_x_u8m4(0U, vl);
    const vbool2_t eq = __riscv_vmseq_vx_u8m4_b2(group, needle, vl);
    const vuint8m4_t masked = __riscv_vmerge_vvm_u8m4(zero, powers, eq, vl);
    __riscv_vse8_v_u8m4(masked_bytes, masked, vl);
    return static_cast<std::uint32_t>(masked_bytes[0] | masked_bytes[1] | masked_bytes[2] |
                                      masked_bytes[3] | masked_bytes[4] | masked_bytes[5] |
                                      masked_bytes[6] | masked_bytes[7]) |
           (static_cast<std::uint32_t>(masked_bytes[8] | masked_bytes[9] | masked_bytes[10] |
                                       masked_bytes[11] | masked_bytes[12] | masked_bytes[13] |
                                       masked_bytes[14] | masked_bytes[15])
            << 8U);
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
    // Performance improvement #2: static constexpr table avoids re‑initializing
    // the power‑of‑two array on every call.
    static constexpr uint8_t kPowersArr[16] = {1, 2, 4, 8, 16, 32, 64, 128,
                                               1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t group   = vld1q_u8(control);
    const uint8x16_t eq      = vceqq_u8(group, vdupq_n_u8(needle));
    const uint8x16_t powers  = vld1q_u8(kPowersArr);
    const uint8x16_t masked  = vandq_u8(eq, powers);
    const std::uint8_t lo    = vaddv_u8(vget_low_u8(masked));
    const std::uint8_t hi    = vaddv_u8(vget_high_u8(masked));
    return static_cast<std::uint32_t>(lo) | (static_cast<std::uint32_t>(hi) << 8U);
#else
    // Scalar SWAR fallback using multiplication instead of loops.
    using ::opteryx::carchar::detail::kByteHighBits64;
    using ::opteryx::carchar::detail::load_u64;
    using ::opteryx::carchar::detail::match_mask64;
    const std::uint64_t lo = load_u64(control);
    const std::uint64_t hi = load_u64(control + 8U);
    std::uint64_t m_lo, m_hi;
    if (needle == kEmpty) {
        m_lo = lo & kByteHighBits64;
        m_hi = hi & kByteHighBits64;
    } else {
        m_lo = match_mask64(lo, needle);
        m_hi = match_mask64(hi, needle);
    }
    // Isolate only the high bit of each byte.
    constexpr std::uint64_t kHighBitMask = 0x8080808080808080ULL;
    std::uint64_t bits_lo = m_lo & kHighBitMask;
    std::uint64_t bits_hi = m_hi & kHighBitMask;
    // Multiply by a magic constant that spreads the 8 high bits into the
    // topmost byte. Then shift right by 56 to extract an 8-bit mask.
    constexpr std::uint64_t kGather = 0x8040201008040201ULL;
    std::uint8_t mask_lo = static_cast<std::uint8_t>((bits_lo * kGather) >> 56);
    std::uint8_t mask_hi = static_cast<std::uint8_t>((bits_hi * kGather) >> 56);
    return static_cast<std::uint32_t>(mask_lo) | (static_cast<std::uint32_t>(mask_hi) << 8U);
#endif
}

class ParviMap {
public:
    ParviMap() noexcept {
        control_.fill(kEmpty);
    }

    static constexpr std::size_t capacity() noexcept { return kCapacity; }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    bool full() const noexcept { return size_ == kCapacity; }

    // Fast read path. Single SIMD group compare on the key's selected group,
    // then key verification on any tag matches. Returns true iff key is present.
    bool lookup_fast(std::uint64_t key, std::int64_t& payload_ref_out) const noexcept {
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data() + base, tag);
        while (matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                payload_ref_out = payload_refs_[idx];
                return true;
            }
            matches &= (matches - 1U);
        }
        return false;
    }

    // Insert a new key with the given payload.
    //   * If the key already exists  → returns {existing_slot, false}.
    //   * If the key's group is full → returns {kCapacity, false}; caller
    //                                   should promote to CarcharIndex.
    //   * Otherwise                  → inserts and returns {new_slot, true}.
    ParviResult insert_new(std::uint64_t key, std::int64_t payload_ref) noexcept {
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t tag_matches = group_match_mask(control_.data() + base, tag);
        // Check for existing key.
        while (tag_matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {idx, false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        // No existing key: insert if the group has space.
        const std::uint32_t empty_matches = group_match_mask(control_.data() + base, kEmpty);
        if (empty_matches == 0U) {
            return {kCapacity, false};
        }
        const std::size_t slot = base + static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]          = tag;
        hashes_[slot]           = key;
        payload_refs_[slot]     = payload_ref;
        ++size_;
        return {slot, true};
    }

    // Single-probe find-or-insert with a caller-assigned id (the engine's
    // num_groups++ / hashes.size() convention). One tag scan decides hit,
    // insert, and overflow — no second lookup on the hit path.
    ParviInsert find_or_insert_id(std::uint64_t key, std::int64_t new_id,
                                  std::int64_t& payload_out) noexcept {
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t tag_matches = group_match_mask(control_.data() + base, tag);
        while (tag_matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                payload_out = payload_refs_[idx];
                return ParviInsert::kFound;
            }
            tag_matches &= (tag_matches - 1U);
        }
        const std::uint32_t empty_matches = group_match_mask(control_.data() + base, kEmpty);
        if (empty_matches == 0U) {
            return ParviInsert::kFull;
        }
        const std::size_t slot = base + static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]      = tag;
        hashes_[slot]       = key;
        payload_refs_[slot] = new_id;
        ++size_;
        payload_out = new_id;
        return ParviInsert::kInserted;
    }

    // find_or_insert: returns (payload, inserted_bool).
    // On overflow (key's group full and key absent) the out-param `overflow`
    // is set to true and the returned payload is the factory's fresh value
    // which the caller is responsible for routing to the promoted map.
    template <typename PayloadFactory>
    std::pair<std::int64_t, bool> find_or_insert(
        std::uint64_t key, PayloadFactory&& payload_factory, bool& overflow
    ) {
        overflow = false;
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t tag_matches = group_match_mask(control_.data() + base, tag);
        while (tag_matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {payload_refs_[idx], false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        const std::int64_t payload_ref = std::forward<PayloadFactory>(payload_factory)();
        const std::uint32_t empty_matches = group_match_mask(control_.data() + base, kEmpty);
        if (empty_matches == 0U) {
            overflow = true;
            return {payload_ref, true};
        }
        const std::size_t slot = base + static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]          = tag;
        hashes_[slot]           = key;
        payload_refs_[slot]     = payload_ref;
        ++size_;
        return {payload_ref, true};
    }

    // Copy live entries into a CarcharIndex — use when overflow occurs or
    // the caller decides to promote early. reserve() sizes the target with
    // load-factor headroom for the existing entries.
    void drain_into(::opteryx::carchar::CarcharIndex& target) const {
        target.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                target.insert_new(hashes_[i], payload_refs_[i]);
            }
        }
    }

    std::vector<std::pair<std::uint64_t, std::int64_t>> items() const {
        std::vector<std::pair<std::uint64_t, std::int64_t>> out;
        out.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                out.emplace_back(hashes_[i], payload_refs_[i]);
            }
        }
        return out;
    }

    void clear() noexcept {
        control_.fill(kEmpty);
        size_ = 0;
    }

private:

    // Performance improvement #4: align the control array to a 64-byte cache
    // line — at 64 slots the whole control array IS one cache line, and
    // alignment also avoids false sharing when multiple ParviMaps are used
    // concurrently (e.g. per-partition front maps on different threads).
    alignas(64) std::array<std::uint8_t, kCapacity>  control_{};
    std::array<std::uint64_t, kCapacity>             hashes_{};
    std::array<std::int64_t, kCapacity>              payload_refs_{};
    std::size_t                                      size_ = 0;
};

// ------------------------------------------------------------------
// ParviSet: like ParviMap but for set membership (no payloads)
// ------------------------------------------------------------------

struct ParviSetResult {
    bool is_new = false;
    bool overflow = false;
};

class ParviSet {
public:
    ParviSet() noexcept {
        control_.fill(kEmpty);
    }

    static constexpr std::size_t capacity() noexcept { return kCapacity; }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    bool full() const noexcept { return size_ == kCapacity; }

    // Fast contains: single SIMD group compare on the key's selected group,
    // then key verification.
    bool contains(std::uint64_t key) const noexcept {
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t matches = group_match_mask(control_.data() + base, tag);
        while (matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(matches));
            if (hashes_[idx] == key) {
                return true;
            }
            matches &= (matches - 1U);
        }
        return false;
    }

    // Insert a new key.
    //   * If the key already exists → returns {is_new: false, overflow: false}.
    //   * If the key's group is full and key is absent
    //                               → returns {is_new: false, overflow: true}.
    //   * Otherwise                 → inserts and returns {is_new: true, overflow: false}.
    ParviSetResult insert_or_ignore(std::uint64_t key) noexcept {
        const std::size_t base = group_base(key);
        const std::uint8_t tag = key_tag(key);
        std::uint32_t tag_matches = group_match_mask(control_.data() + base, tag);
        // Check for existing key.
        while (tag_matches != 0U) {
            const std::size_t idx = base + static_cast<std::size_t>(__builtin_ctz(tag_matches));
            if (hashes_[idx] == key) {
                return {false, false};
            }
            tag_matches &= (tag_matches - 1U);
        }
        // No existing key: insert if the group has space.
        const std::uint32_t empty_matches = group_match_mask(control_.data() + base, kEmpty);
        if (empty_matches == 0U) {
            return {false, true};
        }
        const std::size_t slot = base + static_cast<std::size_t>(__builtin_ctz(empty_matches));
        control_[slot]  = tag;
        hashes_[slot]   = key;
        ++size_;
        return {true, false};
    }

    // Bulk mark-new-indices operation: like CarcharSet::mark_new_indices_32 but
    // with overflow handling. Returns count of newly-inserted entries and writes
    // their indices into out_indices.
    //
    // overflow=true means we encountered an unseen key that could not be inserted
    // because its group was already full. The set HAS been mutated with a
    // PREFIX of this batch's keys, and the rows carrying them were NOT
    // emitted (the caller's batch is unchanged). Draining into Carchar and
    // replaying the SAME batch would therefore suppress those first
    // occurrences — data loss. Sound recoveries: keep the partial out_indices
    // and continue the remainder on the drained Carchar (native DistinctSink),
    // or replay against a set that excludes this batch's inserts — a fresh
    // set in single-shot use (draken Vector::unique).
    template <typename IndexT>
    std::pair<std::size_t, bool> mark_new_indices(
        const std::uint64_t* keys, IndexT* out_indices, std::size_t length
    ) noexcept {
        std::size_t count = 0;
        for (std::size_t i = 0; i < length; ++i) {
            auto result = insert_or_ignore(keys[i]);
            if (result.is_new) {
                out_indices[count++] = static_cast<IndexT>(i);
                continue;
            }
            // Early exit only on true overflow (unseen key, its group full).
            if (result.overflow) {
                return {count, true};
            }
        }
        return {count, false};
    }

    // Copy live entries into a CarcharSet for promotion.
    void drain_into(::opteryx::carchar::CarcharSet& target) const {
        target.reserve(size_);
        for (std::size_t i = 0; i < kCapacity; ++i) {
            if (control_[i] != kEmpty) {
                target.insert_or_ignore(hashes_[i]);
            }
        }
    }

    void clear() noexcept {
        control_.fill(kEmpty);
        size_ = 0;
    }

private:

    alignas(64) std::array<std::uint8_t, kCapacity>  control_{};
    std::array<std::uint64_t, kCapacity>             hashes_{};
    std::size_t                                      size_ = 0;
};

}  // namespace opteryx::parvi
