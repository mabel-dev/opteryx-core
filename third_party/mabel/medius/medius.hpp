#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "carchar_common.hpp"
#include "carchar_index.hpp"
#include "carchar_simd.hpp"

// Medius — the MIDDLE TIER of the group-key container ladder.
//
// Parvi (tiny, 64 slots) -> MEDIUS (bounded, 512 slots) -> Carchar (unbounded).
// Named 2026-08-14 by the architect to sit alongside its siblings.
//
// Before this, the native GROUP BY sink had exactly two tiers:
//   * ParviMap      — 64 slots (4 group-selected groups of 16), armed only when
//                     the planner's NDV estimate is <= kGBParviGateNDV (64).
//   * CarcharIndex  — unbounded, growing by doubling, ~544 MB of table at 17.6M
//                     groups and one random dependent access per probe.
// Everything from 65 distinct values upward paid full CarcharIndex probe cost.
//
// That is the band both comparators special-case, and they size it far larger
// than 64 (read from source 2026-08-14):
//   duckdb     TryAddDictionaryGroups, dict->group-pointer cache keyed by a
//              dictionary id that survives across vectors ......... 20,000
//   duckdb     same, dictionary with no id (per-vector only) ......  ~1,024
//   clickhouse low_cardinality_max_dictionary_size ................   8,192
//   clickhouse FixedHashTable key8/key16, direct index, NO hashing . 256/65,536
//   us         kGBParviGateNDV ..............................            64
//
// SIZING — why 512 slots and not 16,384.
// The sink partitions by `hash >> kGBPartShift` into kGBParts = 64 partitions,
// and each partition owns its own front map. A column with N total distinct
// values therefore spreads ~N/64 per partition. To cover a 16k-cardinality
// column each partition needs ~256 live entries; at a 0.8 load factor that is
// 320 slots, rounded up to the next power of two = 512. So:
//     512 slots * 16 B/slot   =   8 KB per partition   (L1-resident)
//     x 64 partitions         = 512 KB per worker thread
//     usable                  = 409 entries/partition ~= 26k distinct overall
// A literal 16,384-slot map PER PARTITION would be 256 KB each and 16 MB per
// worker — it would evict itself and every input buffer around it.
//
// LAYOUT — one merged array, per H16 (carchar_index.hpp). `hashes_` and
// `payload_refs_` used to be separate allocations there, costing two random
// accesses at the same index; merging them measured -5.8% on a 17.6M-group
// aggregate and -6.7% on q32. This container is built merged from the start and
// reuses carchar's SIMD probe at HStride = 2 rather than growing a second copy
// of probe logic.
//
// EQUALITY is 64-bit hash identity, the same contract CarcharIndex and ParviMap
// use — no key bytes are stored or compared. The full 64-bit hash is therefore
// kept per slot; truncating it would silently merge distinct groups.
//
// OVERFLOW is a hard bound, NOT a resize: at kThreshold live entries
// find_or_insert_id returns kFull and the caller promotes via drain_into(). This
// is deliberately the ParviMap contract, and it is why this container does not
// contradict [[feedback-hashtable-sizing]] (which rejects PRE-SIZING a growing
// table from NDV estimates): nothing here is sized from an estimate, and a wrong
// estimate costs a promotion, not a bad steady state.
namespace opteryx::medius {

// Mirrors opteryx::parvi::ParviInsert so the sink can treat the tiers alike
// without depending on parvi from here.
enum class MediusInsert : std::uint8_t {
    kFound = 0,     // key present; payload_out holds the existing group id
    kInserted = 1,  // key absent and stored; payload_out holds new_id
    kFull = 2,      // bounded capacity reached; caller must promote
};

template <std::size_t Capacity = 512>
class MediusMap {
    static_assert((Capacity & (Capacity - 1U)) == 0U, "Capacity must be a power of two");
    static_assert(Capacity >= 64, "Capacity below 64 is ParviMap's band");

    struct Slot {
        std::uint64_t hash;
        std::int64_t payload;
    };
    static_assert(sizeof(Slot) == 16, "Slot must be two 64-bit words for HStride=2");

   public:
    // 0.8 load factor: linear probing degrades sharply past ~0.85, and the whole
    // point of this tier is a short probe.
    static constexpr std::size_t kThreshold = (Capacity * 4U) / 5U;

    MediusMap() noexcept { control_.fill(::opteryx::carchar::kEmpty); }

    static constexpr std::size_t capacity() noexcept { return Capacity; }
    static constexpr std::size_t threshold() noexcept { return kThreshold; }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    bool full() const noexcept { return size_ >= kThreshold; }

    // Three-way find-or-insert, mirroring ParviMap/CarcharIndex.
    // Probing is carchar's own SIMD probe at HStride = 2 over the merged slots,
    // so this shares the ISA dispatch and the 16-way tag scan rather than
    // duplicating them.
    MediusInsert find_or_insert_id(std::uint64_t key, std::int64_t new_id,
                                std::int64_t& payload_out) noexcept {
        const std::uint8_t tag = ::opteryx::carchar::key_tag(key);
        const auto result = ::opteryx::carchar::detail::probe_find_slot_direct<2>(
            control_.data(), reinterpret_cast<const std::uint64_t*>(slots_.data()),
            Capacity, key, tag);
        if (result.found) {
            payload_out = slots_[result.slot].payload;
            return MediusInsert::kFound;
        }
        // Absent. Refuse BEFORE storing so the caller can promote with a map
        // whose contents are still wholly valid.
        if (size_ >= kThreshold) {
            return MediusInsert::kFull;
        }
        insert_at(result.slot, key, new_id, tag);
        payload_out = new_id;
        return MediusInsert::kInserted;
    }

    bool lookup_fast(std::uint64_t key, std::int64_t& payload_out) const noexcept {
        const auto result = ::opteryx::carchar::detail::probe_find_slot_direct<2>(
            control_.data(), reinterpret_cast<const std::uint64_t*>(slots_.data()),
            Capacity, key, ::opteryx::carchar::key_tag(key));
        if (!result.found) {
            return false;
        }
        payload_out = slots_[result.slot].payload;
        return true;
    }

    // Promotion path: copy live entries into the unbounded tier. reserve() gives
    // the target load-factor headroom so the drain itself does not rehash.
    void drain_into(::opteryx::carchar::CarcharIndex& target) const {
        target.reserve(size_);
        for (std::size_t i = 0; i < Capacity; ++i) {
            if (control_[i] != ::opteryx::carchar::kEmpty) {
                target.insert_new(slots_[i].hash, slots_[i].payload);
            }
        }
    }

    void clear() noexcept {
        control_.fill(::opteryx::carchar::kEmpty);
        size_ = 0;
    }

    std::size_t estimated_bytes() const noexcept {
        return Capacity * (1U + sizeof(Slot));
    }

   private:
    void insert_at(std::size_t slot, std::uint64_t key, std::int64_t payload,
                   std::uint8_t tag) noexcept {
        control_[slot] = tag;
        // Mirror the tag into the wrap-around padding for slots near the end,
        // exactly as CarcharIndex::insert_at does. Omitting this causes silent
        // DUPLICATE GROUPS on wrap-around — a known trap, see
        // [[parvi-size-curve-and-native-sink-gate]].
        if (slot < (::opteryx::carchar::kGroupWidth - 1U)) {
            control_[Capacity + slot] = tag;
        }
        slots_[slot].hash = key;
        slots_[slot].payload = payload;
        ++size_;
    }

    // control_ carries kGroupWidth-1 bytes of wrap-around padding so a 16-wide
    // group load at the last slot never reads out of bounds. It is the sole
    // authority on occupancy and the constructor fills it, so neither array is
    // value-initialised here: `slots_{}` would be a dead 8 KB memset per
    // partition (x64 partitions x every worker) and `control_{}` a double init.
    // A slot is only ever read after its control byte proved it occupied —
    // the same argument carchar_index.hpp makes for its uninitialized_allocator.
    alignas(64) std::array<std::uint8_t, Capacity + ::opteryx::carchar::kGroupWidth - 1U> control_;
    alignas(64) std::array<Slot, Capacity> slots_;
    std::size_t size_ = 0;
};

}  // namespace opteryx::medius
