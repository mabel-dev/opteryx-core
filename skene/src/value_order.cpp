#include "value_order.h"

#include <algorithm>
#include <unordered_set>
#include <cstdarg>
#include <cstdio>
#include <cstring>

#include "skene/format.h"

#include "encoding.h"

#include "core/alloc.h"
#include "core/interval_slot.h"
#include "core/string_slot.h"

#define XXH_INLINE_ALL
#include "xxhash.h"
#include "ops/interval_ops.h"

namespace skene {
namespace {

Status fail(Code code, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
Status fail(Code code, const char* fmt, ...) {
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    return Status(code, buffer);
}

// ─── Ordering ───────────────────────────────────────────────────────────────
//
// Draken's engine order, per physical type. This does not invent an ordering:
// the numeric orders are the obvious ones, floats follow draken's documented
// convention (NaN highest, -0.0 == 0.0), INTERVAL reuses
// interval_normalize_unchecked — the same normalization interval_compare uses —
// and strings use str_compare, which is draken's own lexicographic comparator.

template <typename T>
int compare_scalar(const void* data, uint32_t a, uint32_t b) {
    const T* values = static_cast<const T*>(data);
    if (values[a] < values[b]) return -1;
    if (values[b] < values[a]) return 1;
    return 0;
}

// Floats: NaN sorts highest, and -0.0 compares equal to 0.0. Both are draken's
// documented conventions (float_ops.h), not choices made here. Dedup keys on
// bits, so -0.0 and 0.0 remain two distinct stored values that happen to
// compare equal — which keeps DICT_KEYS_SORTED's "code_a < code_b ⟹
// data[code_a] <= data[code_b]" true, since they land adjacent.
template <typename T>
int compare_float(const void* data, uint32_t a, uint32_t b) {
    const T* values = static_cast<const T*>(data);
    const T x = values[a];
    const T y = values[b];
    const bool nx = (x != x);
    const bool ny = (y != y);
    if (nx || ny) {
        if (nx && ny) return 0;
        return nx ? 1 : -1;   // NaN highest
    }
    if (x < y) return -1;
    if (y < x) return 1;
    return 0;                 // covers -0.0 == 0.0
}

int compare_int128(const void* data, uint32_t a, uint32_t b) {
    const __int128* values = static_cast<const __int128*>(data);
    if (values[a] < values[b]) return -1;
    if (values[b] < values[a]) return 1;
    return 0;
}

int compare_interval(const void* data, uint32_t a, uint32_t b) {
    const DrakenIntervalSlot* slots = static_cast<const DrakenIntervalSlot*>(data);
    const int64_t x = draken::ops::interval_normalize_unchecked(slots[a].months, slots[a].us);
    const int64_t y = draken::ops::interval_normalize_unchecked(slots[b].months, slots[b].us);
    if (x < y) return -1;
    if (y < x) return 1;
    return 0;
}

int compare_string(const DrakenStringArena* arena, uint32_t a, uint32_t b) {
    return str_compare(&arena->slots[a], arena->arena,
                       &arena->slots[b], arena->arena);
}

using CompareFn = int (*)(const void*, uint32_t, uint32_t);

CompareFn comparator_for(DrakenType type) {
    switch (type) {
        case DRAKEN_INT8:        return compare_scalar<int8_t>;
        case DRAKEN_INT16:       return compare_scalar<int16_t>;
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:      return compare_scalar<int32_t>;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:      return compare_scalar<int64_t>;
        case DRAKEN_UINT8:       return compare_scalar<uint8_t>;
        case DRAKEN_UINT16:      return compare_scalar<uint16_t>;
        case DRAKEN_UINT32:      return compare_scalar<uint32_t>;
        case DRAKEN_UINT64:      return compare_scalar<uint64_t>;
        case DRAKEN_FLOAT32:     return compare_float<float>;
        case DRAKEN_FLOAT64:     return compare_float<double>;
        case DRAKEN_DECIMAL128:  return compare_int128;
        case DRAKEN_INTERVAL:    return compare_interval;
        default:                 return nullptr;
    }
}

}  // namespace

bool type_is_orderable(DrakenType type) {
    if (draken_type_is_string_storage(type)) {
        // VARIANT is string-SHAPED storage but has no defined collation
        // (buffers.h is explicit that storage layout and key eligibility are
        // different questions). Ordering it would invent a collation.
        return type != DRAKEN_VARIANT;
    }
    // BOOL is deliberately excluded even though it is trivially orderable: it is
    // bit-packed at 1 bit per row, so replacing an identity selection with
    // 4-byte codes inflates the column ~32x to buy a binary search over at most
    // two values.
    return comparator_for(type) != nullptr;
}

Status order_column(const DrakenVector& vector, const LogicalType* logical,
                    const char* column_name, OrderedColumn* out) {
    out->applied = false;

    const DrakenType type = vector.type;
    if (!type_is_orderable(type)) return Status::ok();

    const bool is_string = draken_type_is_string_storage(type);
    const DrakenStringArena* arena =
        is_string ? static_cast<const DrakenStringArena*>(vector.data) : nullptr;

    if (is_string) {
        if (arena == nullptr) return Status::ok();
        // A length-only column records lengths but never materialized the
        // payload bytes, so there is nothing to compare. Ordering it would
        // require inventing an order over values we do not have.
        if (arena->payloads_elided) return Status::ok();
    }

    const uint32_t length = vector.length;

    // ── Which data slots does at least one VALID row reference? ──
    //
    // Null rows must NOT contribute a value: FORMAT.md §7.6 requires that
    // `data` contain no value unreferenced by a non-null row, because that is
    // what makes `data_length` the exact distinct count.
    std::vector<uint8_t> referenced(vector.data_length, 0);
    uint32_t referenced_count = 0;
    for (uint32_t row = 0; row < length; ++row) {
        if (vector.validity != nullptr
                && (vector.validity[row >> 3] & (1u << (row & 7u))) == 0)
            continue;
        const uint32_t code = vector.selection[row];
        if (code >= vector.data_length)
            return fail(Code::kMalformed,
                        "column '%s': selection[%u] == %u is out of range for "
                        "data_length %u", column_name, row, code, vector.data_length);
        if (!referenced[code]) { referenced[code] = 1; ++referenced_count; }
    }

    // Nothing to order, and no honest value to put in `data`.
    if (referenced_count == 0) return Status::ok();

    const size_t width = is_string ? sizeof(DrakenStringSlot)
                                   : draken_type_itemsize(type, logical);
    if (!is_string && width == 0)
        return fail(Code::kUnsupportedType,
                    "column '%s': no fixed item width for physical type %u",
                    column_name, static_cast<unsigned>(type));

    const uint8_t* raw = is_string
        ? reinterpret_cast<const uint8_t*>(arena->slots)
        : static_cast<const uint8_t*>(vector.data);

    // ── Equality and hashing, matching the deduplication rule exactly ──
    //
    // BIT PATTERN for fixed-width (so -0.0 and 0.0 stay distinct) and CONTENT
    // for strings (so two slots holding the same bytes at different arena
    // offsets are one value).
    struct ValueKey {
        const uint8_t*           raw;
        size_t                   width;
        const DrakenStringArena* arena;
        bool                     is_string;

        size_t hash(uint32_t index) const {
            if (is_string) {
                const DrakenStringSlot* slot = &arena->slots[index];
                return static_cast<size_t>(
                    XXH3_64bits(str_data(slot, arena->arena), str_length(slot)));
            }
            return static_cast<size_t>(XXH3_64bits(raw + index * width, width));
        }
        bool equal(uint32_t a, uint32_t b) const {
            if (is_string)
                return str_equals(&arena->slots[a], arena->arena,
                                  &arena->slots[b], arena->arena) != 0;
            return std::memcmp(raw + a * width, raw + b * width, width) == 0;
        }
    };
    const ValueKey key{raw, width, arena, is_string};

    struct Hasher {
        const ValueKey* k;
        size_t operator()(uint32_t i) const { return k->hash(i); }
    };
    struct Equal {
        const ValueKey* k;
        bool operator()(uint32_t a, uint32_t b) const { return k->equal(a, b); }
    };

    // ── Pick the deduplication strategy from a SAMPLE ──
    //
    // Neither strategy wins everywhere, and the gap is large in both directions:
    //
    //   hashing   O(N) but a heavy constant — 1M distinct values cost ~200 ms of
    //             hashtable inserts, where sorting them costs ~10 ms.
    //   sorting   O(N log N) but a tiny constant, and nearly free on data that
    //             is already ordered — yet it sorts all N slots even when only
    //             50 distinct values exist, measured at ~98 ms.
    //
    // Low cardinality wants hashing; high cardinality wants sorting. Cardinality
    // is cheap to ESTIMATE and expensive to know, so a stride sample decides.
    // The estimate can only pick a slower strategy, never a wrong answer — both
    // paths produce identical output — which is what makes sampling acceptable
    // here when it would not be for anything affecting correctness.
    constexpr uint32_t kSampleRows = 4096;
    bool prefer_hashing = true;
    {
        std::unordered_set<uint32_t, Hasher, Equal> sample(
            16, Hasher{&key}, Equal{&key});
        const uint32_t stride = length > kSampleRows ? length / kSampleRows : 1u;
        uint32_t sampled = 0;
        for (uint32_t row = 0; row < length && sampled < kSampleRows; row += stride) {
            if (vector.validity != nullptr
                    && (vector.validity[row >> 3] & (1u << (row & 7u))) == 0)
                continue;
            sample.insert(vector.selection[row]);
            ++sampled;
        }
        // More than half the sample distinct means repeats are rare, so hashing
        // would pay its full per-value cost for almost no deduplication.
        prefer_hashing = sampled == 0 || sample.size() * 2 <= sampled;
    }

    // Near-unique and no existing selection: decline. Ordering a near-unique
    // column adds a stored-selection permutation to a column that carried none,
    // and READING it back pays for that forever — bit-unpack the codes, then
    // indirect every access — where the as-written column is a straight dense
    // read. Read performance is king for storage writes (architect ruling
    // 2026-08-07): the write bends to the read, so this declines for EVERY
    // type. Delta-capable types were previously exempted because sorted+delta
    // shrinks the file (~measured 34ms of a 43ms engine scan reconstructing a
    // 97.6%-unique DECIMAL on TPC-H lineitem — file size is the wrong thing to
    // buy with that). A column that ALREADY carries a selection keeps its
    // eligibility: ordering it reuses the indirection the reader pays anyway.
    //
    // Decided from the SAMPLE so the expensive sort is never paid just to
    // discover it was not worth paying — measured at 220 ms on a million
    // near-unique strings.
    //
    // Gated on the column being big enough for the sort to be worth avoiding.
    // Below the sample size the "sample" is the whole column, and a small
    // column with a few repeats reads as high-cardinality on a ratio test —
    // 3 distinct in 5 rows is 60% — so a short column takes the exact path
    // instead, where the sort costs nothing anyway.
    if (length >= kSampleRows && !prefer_hashing && !draken_is_compressed(&vector))
        return Status::ok();   // declines: written as-is

    // representative_of[code] is the canonical index for that code's VALUE.
    std::vector<uint32_t> representative_of(vector.data_length, 0);
    std::vector<uint32_t> order;

    if (prefer_hashing) {
        std::unordered_set<uint32_t, Hasher, Equal> distinct(
            16, Hasher{&key}, Equal{&key});
        distinct.reserve(referenced_count / 4u + 1u);
        for (uint32_t i = 0; i < vector.data_length; ++i) {
            if (!referenced[i]) continue;
            const auto found = distinct.find(i);
            if (found == distinct.end()) {
                distinct.insert(i);
                representative_of[i] = i;
            } else {
                representative_of[i] = *found;
            }
        }
        order.reserve(distinct.size());
        for (uint32_t index : distinct) order.push_back(index);
    } else {
        // High cardinality: sorting everything and collapsing adjacent equals
        // costs what the sort would have cost anyway, and is close to free on
        // data that is already ordered.
        order.reserve(referenced_count);
        for (uint32_t i = 0; i < vector.data_length; ++i)
            if (referenced[i]) order.push_back(i);
    }

    // ── Sort ──
    //
    // Value order first, then bit pattern, then index. The bit tie-break keeps
    // values that compare EQUAL but differ in bits (-0.0 vs 0.0, distinct NaN
    // payloads) as separate entries rather than letting one silently replace the
    // other. The index tie-break makes the result deterministic, which matters
    // because migration verification compares files byte for byte.
    const CompareFn compare = is_string ? nullptr : comparator_for(type);
    auto value_compare = [&](uint32_t a, uint32_t b) -> int {
        return is_string ? compare_string(arena, a, b) : compare(vector.data, a, b);
    };
    auto bits_compare = [&](uint32_t a, uint32_t b) -> int {
        return std::memcmp(raw + a * width, raw + b * width, width);
    };

    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        const int by_value = value_compare(a, b);
        if (by_value != 0) return by_value < 0;
        const int by_bits = bits_compare(a, b);
        if (by_bits != 0) return by_bits < 0;
        return a < b;
    });

    // ── Collapse adjacent equals ──
    //
    // A no-op on the hashing path, where `order` already holds one index per
    // distinct value; the real work on the sorting path. Both paths end here
    // with the same representatives in the same order, which is what lets the
    // strategy be a performance choice with no effect on the bytes written.
    std::vector<uint32_t> representatives;
    representatives.reserve(order.size());
    for (uint32_t index : order) {
        if (representatives.empty() || !key.equal(index, representatives.back()))
            representatives.push_back(index);
        if (!prefer_hashing) representative_of[index] = representatives.back();
    }

    const uint32_t new_data_length = static_cast<uint32_t>(representatives.size());

    // Exact check, now that the distinct count is known rather than sampled:
    // fully unique + no existing selection declines for every type — same
    // read-first reasoning as the sample gate above.
    if (new_data_length == referenced_count && !draken_is_compressed(&vector))
        return Status::ok();

    // ── Assign new codes ──
    std::vector<uint32_t> remap(vector.data_length, 0);
    {
        std::vector<uint32_t> code_of(vector.data_length, 0);
        for (uint32_t i = 0; i < new_data_length; ++i)
            code_of[representatives[i]] = i;
        for (uint32_t i = 0; i < vector.data_length; ++i)
            if (referenced[i]) remap[i] = code_of[representative_of[i]];
    }

    // ── Rebuild data ──
    if (is_string) {
        uint64_t arena_bytes = 0;
        for (uint32_t index : representatives) {
            const DrakenStringSlot* slot = &arena->slots[index];
            if (!str_is_inline(slot)) arena_bytes += str_length(slot);
        }

        const size_t slots_bytes = static_cast<size_t>(new_data_length)
                                 * sizeof(DrakenStringSlot);
        uint8_t* slots_buffer =
            static_cast<uint8_t*>(draken_malloc(slots_bytes > 0 ? slots_bytes : 1));
        if (slots_buffer == nullptr)
            return fail(Code::kOutOfMemory, "column '%s': %zu slot bytes",
                        column_name, slots_bytes);
        OwnedBuffer<uint8_t> slots_guard(slots_buffer);
        std::memset(slots_buffer, 0, slots_bytes > 0 ? slots_bytes : 1);

        OwnedBuffer<uint8_t> arena_guard(nullptr);
        uint8_t* arena_buffer = nullptr;
        if (arena_bytes > 0) {
            arena_buffer = static_cast<uint8_t*>(
                draken_malloc(static_cast<size_t>(arena_bytes)));
            if (arena_buffer == nullptr)
                return fail(Code::kOutOfMemory, "column '%s': %llu arena bytes",
                            column_name, static_cast<unsigned long long>(arena_bytes));
            arena_guard.reset(arena_buffer);
        }

        DrakenStringSlot* destination =
            reinterpret_cast<DrakenStringSlot*>(slots_buffer);
        uint64_t written = 0;
        for (uint32_t i = 0; i < new_data_length; ++i) {
            const DrakenStringSlot* source = &arena->slots[representatives[i]];
            if (str_is_inline(source)) {
                destination[i] = *source;
            } else {
                const uint32_t len = str_length(source);
                std::memcpy(arena_buffer + written,
                            str_data(source, arena->arena), len);
                // Clone verbatim and rebase only the offset: prefix and the
                // rest of the slot are preserved rather than recomputed.
                str_clone_with_offset(&destination[i], source,
                                      static_cast<uint32_t>(written));
                written += len;
            }
        }

        out->data       = std::move(slots_guard);
        out->arena      = std::move(arena_guard);
        out->arena_used = written;
        out->slot_count = new_data_length;
    } else {
        const size_t bytes = static_cast<size_t>(new_data_length) * width;
        uint8_t* buffer = static_cast<uint8_t*>(draken_malloc(bytes > 0 ? bytes : 1));
        if (buffer == nullptr)
            return fail(Code::kOutOfMemory, "column '%s': %zu data bytes",
                        column_name, bytes);
        for (uint32_t i = 0; i < new_data_length; ++i)
            std::memcpy(buffer + i * width, raw + representatives[i] * width, width);
        out->data.reset(buffer);
    }

    // ── Rebuild codes ──
    //
    // A null row's original code may point at a value no valid row references,
    // which is not in the remap. Those rows are masked by validity, so any
    // in-range code is correct; 0 keeps them from introducing a value.
    out->codes.resize(length);
    for (uint32_t row = 0; row < length; ++row) {
        const uint32_t old_code = vector.selection[row];
        const bool valid = vector.validity == nullptr
                        || (vector.validity[row >> 3] & (1u << (row & 7u))) != 0;
        out->codes[row] = valid ? remap[old_code] : 0u;
    }

    // ── Recompute flags ──
    //
    // Inheriting the input's flags here would be a correctness bug: the layout
    // they describe no longer exists. Only ROW_SORTED survives, because value
    // ordering permutes VALUES and leaves logical rows exactly where they were.
    uint8_t flags = static_cast<uint8_t>(
        vector.flags & (DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));

    flags |= DRAKEN_DICT_KEYS_SORTED;   // data is ascending, by construction
    flags |= DRAKEN_DICT_CODES_DENSE;   // every entry is referenced by a valid row

    if (new_data_length == length) flags |= DRAKEN_SEL_PERMUTATION;

    bool identity = (new_data_length == length);
    for (uint32_t row = 0; identity && row < length; ++row)
        if (out->codes[row] != row) identity = false;
    if (identity) flags |= DRAKEN_SEL_IDENTITY;

    out->data_length = new_data_length;
    out->flags       = flags;
    out->applied     = true;
    return Status::ok();
}

}  // namespace skene
