#include "skene/writer.h"

#include <algorithm>
#include <cstdarg>
#include <memory>
#include <string>
#include <cstdio>
#include <cstring>
#include <deque>
#include <type_traits>

#include "skene/checksum.h"
#include "skene/format.h"
#include "bloom.h"
#include "encoding.h"
#include "sketch.h"
#include "staging.h"
#include "statistics.h"
#include "value_order.h"

// draken — imported, never copied.
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_owner.h"
#include "logical_type.h"

namespace skene {
namespace {

// ─── Byte assembly ──────────────────────────────────────────────────────────
//
// Appends to a growable buffer. Used for the records finish() assembles in
// memory before handing them to the sink — directory blocks and the footer —
// which are small against the data and must be checksummed whole.
class ByteWriter {
  public:
    explicit ByteWriter(std::vector<uint8_t>* out) : out_(out) {}

    uint64_t position() const { return out_->size(); }

    void bytes(const void* src, size_t n) {
        if (n == 0) return;
        const uint8_t* p = static_cast<const uint8_t*>(src);
        out_->insert(out_->end(), p, p + n);
    }

    void zeros(size_t n) { out_->insert(out_->end(), n, uint8_t{0}); }

    template <typename T>
    void pod(const T& value) {
        static_assert(std::is_trivially_copyable<T>::value, "pod() needs a POD");
        bytes(&value, sizeof(T));
    }

    void u8(uint8_t v)   { pod(v); }
    void u16(uint16_t v) { pod(v); }
    void u32(uint32_t v) { pod(v); }
    void u64(uint64_t v) { pod(v); }

  private:
    std::vector<uint8_t>* out_;
};

Status fail(Code code, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
Status fail(Code code, const char* fmt, ...) {
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    return Status(code, buffer);
}

// ─── Selection classification ───────────────────────────────────────────────

// Decide how a column's selection is stored, by SCANNING THE ARRAY'S CONTENTS.
//
// Deliberately NOT derived from data_length vs length. Under value ordering an
// all-distinct column has data_length == length and a REAL permutation
// selection, so the shape heuristic ("data_length == length means identity")
// would silently drop that permutation and reorder every row. The contents are
// the only correct source, and the scan short-circuits on the first mismatch —
// on a genuine dict that is one comparison.
//
// The same pass bounds-checks every code, so a malformed input vector is caught
// here, where the producing operator is still nameable, rather than as an
// out-of-bounds read in some later consumer.
Status classify_codes(const uint32_t* sel, uint32_t n, uint32_t data_length,
                      const char* column_name, SelectionKind* out_kind) {
    const struct { uint32_t data_length; uint32_t length; } v{data_length, n};

    if (n > 0 && sel == nullptr)
        return fail(Code::kMalformed,
                    "column '%s': selection is null; the unified vector model "
                    "guarantees it is never null", column_name);

    bool is_identity = true;
    bool is_constant = true;
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t code = sel[i];
        if (code >= v.data_length)
            return fail(Code::kMalformed,
                        "column '%s': selection[%u] == %u is out of range for "
                        "data_length %u", column_name, i, code, v.data_length);
        if (code != i) is_identity = false;
        if (code != 0) is_constant = false;
        if (!is_identity && !is_constant) break;
    }

    // A zero-row column is identity and constant at once; identity is the
    // canonical choice so the classification is deterministic.
    if (is_identity)      *out_kind = SelectionKind::kIdentity;
    else if (is_constant) *out_kind = SelectionKind::kConstant;
    else                  *out_kind = SelectionKind::kStored;

    // Cross-check against the shape the counts imply. These cannot disagree on
    // a well-formed vector, and a disagreement means the producer is broken —
    // so it is caught at the boundary rather than written down and trusted.
    if (*out_kind == SelectionKind::kConstant && v.data_length != 1u && n > 0)
        return fail(Code::kMalformed,
                    "column '%s': every selection code is 0 but data_length is "
                    "%u, so %u data values are unreachable",
                    column_name, v.data_length, v.data_length - 1u);
    if (*out_kind == SelectionKind::kIdentity && n > 0 && v.data_length != n)
        return fail(Code::kMalformed,
                    "column '%s': selection is the identity but data_length "
                    "(%u) != length (%u)", column_name, v.data_length, n);

    return Status::ok();
}

// ─── Logical type ───────────────────────────────────────────────────────────

// A missing descriptor on these is a HARD ERROR, not a degraded column: the
// physical tag alone cannot say what the bits mean, so writing one would produce
// a file nothing can interpret. IPV4 is deliberately absent from this list — it
// REFINES a complete UINT32, so its absence is a display regression, never an
// uninterpretable column. Carrying it anyway is the reason this format exists.
bool type_requires_logical_type(DrakenType t) {
    return t == DRAKEN_TIMESTAMP64 || t == DRAKEN_TIME32 || t == DRAKEN_TIME64
        || t == DRAKEN_DECIMAL || t == DRAKEN_DECIMAL128 || t == DRAKEN_VECTOR_FP16;
}

// ─── Per-column serialization ───────────────────────────────────────────────

// Where one column node's sections go while a row group is encoded. v3 writes
// in TWO PASSES (design R9): pass 1 stages each section's stored bytes, pass 2
// (finish) lays the file out column-major. So a SectionEntry's `offset` here is
// a STAGING offset, rewritten to the final absolute offset in finish(), and no
// alignment padding exists yet — finish() places every body on kSectionAlign.
struct WriteContext {
    Stage*                     stage = nullptr;
    uint32_t                   node = 0;          // column node, depth first
    bool                       index = false;     // staging the node's OPTIONAL sections
    std::vector<SectionEntry>* sections = nullptr;// the node's section list
    SectionCodec               codec = SectionCodec::kNone;
    int                        zstd_level = 0;
};

// The checksum covers the STORED bytes, so it is computed after both stages —
// a reader verifies what it is about to decode, not what the writer started
// from.
Status emit_raw(WriteContext& ctx, SectionKind kind, Encoding encoding,
                SectionCodec codec, const void* stored, size_t stored_bytes,
                size_t encoded_bytes, size_t plain_bytes) {
    SectionEntry entry;
    entry.kind          = static_cast<uint16_t>(kind);
    entry.encoding      = static_cast<uint8_t>(encoding);
    entry.codec         = static_cast<uint8_t>(codec);
    entry.reserved      = 0;
    entry.stored_bytes  = stored_bytes;
    entry.encoded_bytes = encoded_bytes;
    entry.plain_bytes   = plain_bytes;
    entry.checksum      = checksum_xxh3_64(stored, stored_bytes);
    SKENE_RETURN_IF_ERROR(
        ctx.stage->append(ctx.node, ctx.index, stored, stored_bytes, &entry.offset));
    ctx.sections->push_back(entry);
    return Status::ok();
}

// True when every bit in [0, length) is set. The bits above `length` in the
// final byte are padding and carry no meaning, so they are masked out rather
// than required to be anything in particular.
bool bitmap_is_all_set(const uint8_t* bits, uint32_t length) {
    const uint32_t whole = length / 8u;
    for (uint32_t i = 0; i < whole; ++i)
        if (bits[i] != 0xFFu) return false;
    const uint32_t remainder = length % 8u;
    if (remainder != 0) {
        const uint8_t mask = static_cast<uint8_t>((1u << remainder) - 1u);
        if ((bits[whole] & mask) != mask) return false;
    }
    return true;
}

// Offers an ENCODED body (any encoding, kPlain included) to the codec, storing
// whichever form wins. Three gates, each measured rather than assumed
// (format.h):
//
//   kind    only kinds that measurably compress. Bloom bits are random by
//           construction; permutations are near-random row ordinals. v2 CHANGE:
//           SELECTION is now eligible — v1's premise that a bit-packed body had
//           no redundancy left confused per-value width with inter-value
//           sequence, and cost 24% of a real ClickBench file (census).
//   size    only above kCompressMinBytes, on the ENCODED body. Small sections
//           are the large majority by count and a rounding error by bytes.
//   result  a plain body keeps v1's rule — stored compressed only when SMALLER
//           (the encoders return false to say "not worth it", a normal answer).
//           A STACKED body (codec over bitpack/delta) pays a second decode
//           stage per read, so it must clear kStackFloorPercent instead of
//           merely shaving a byte.
Status emit_encoded(WriteContext& ctx, SectionKind kind, Encoding encoding,
                    const void* body, size_t body_bytes, size_t plain_bytes) {
    if (ctx.codec != SectionCodec::kNone
            && body_bytes >= kCompressMinBytes
            && kind_is_compressible(static_cast<uint16_t>(kind))) {
        std::vector<uint8_t> packed;
        bool worthwhile = false;
        switch (ctx.codec) {
            case SectionCodec::kZstd:
                worthwhile = zstd_encode(body, body_bytes, ctx.zstd_level, &packed);
                break;
            case SectionCodec::kLz4:
                worthwhile = lz4_encode(body, body_bytes, &packed);
                break;
            case SectionCodec::kNone:
                break;
        }
        if (worthwhile && encoding != Encoding::kPlain
                && packed.size() * 100u > body_bytes * kStackFloorPercent)
            worthwhile = false;
        if (worthwhile)
            return emit_raw(ctx, kind, encoding, ctx.codec, packed.data(),
                            packed.size(), body_bytes, plain_bytes);
    }
    return emit_raw(ctx, kind, encoding, SectionCodec::kNone, body, body_bytes,
                    body_bytes, plain_bytes);
}

Status emit_section(WriteContext& ctx, SectionKind kind, const void* data, size_t bytes) {
    return emit_encoded(ctx, kind, Encoding::kPlain, data, bytes, bytes);
}

// Selection codes, bit-packed to the width data_length implies. This is where
// the bytes are: under value ordering every non-degenerate column stores one
// code per row, so a column with <= 256 distinct values drops from 4 bytes per
// row to 1, and <= 16 distinct to a half.
Status emit_selection(WriteContext& ctx, const uint32_t* codes, uint32_t length,
                      uint32_t data_length) {
    const size_t plain = static_cast<size_t>(length) * sizeof(uint32_t);
    std::vector<uint8_t> packed;
    if (bitpack_encode_codes(codes, length, data_length, &packed))
        return emit_encoded(ctx, SectionKind::kSelection, Encoding::kBitpack,
                            packed.data(), packed.size(), plain);
    return emit_section(ctx, SectionKind::kSelection, codes, plain);
}

// Fixed-width data. Delta+bitpack applies ONLY to a value-ordered column, where
// ascending order is established by construction — never assumed from a flag or
// from the data happening to look sorted.
Status emit_fixed_data(WriteContext& ctx, const void* data, uint32_t data_length,
                       size_t itemsize, DrakenType type, bool ascending) {
    const size_t plain = static_cast<size_t>(data_length) * itemsize;
    if (ascending && type_supports_delta(type)) {
        std::vector<uint8_t> encoded;
        if (delta_bitpack_encode(data, data_length, itemsize, &encoded))
            return emit_encoded(ctx, SectionKind::kData, Encoding::kDeltaBitpack,
                                encoded.data(), encoded.size(), plain);
    }
    return emit_section(ctx, SectionKind::kData, data, plain);
}

// v2: the 16-byte slot array, stored as four u32 lanes (format.h SectionKind).
// Each lane gets whichever encoding fits its own distribution:
//
//   delta    tried first on every lane. The wrapping-difference construction
//            reconstructs ANY u32 sequence exactly (not only ascending ones);
//            a lane that is not near-monotonic simply produces wide deltas and
//            declines on the size test. It is the natural fit for lane 3, where
//            long slots carry near-sequential arena offsets.
//   bitpack  the arbitrary-max variant (bitpack_encode_u32). Lengths are small;
//            the dead hash32 lane of an all-long column is all zeros and
//            collapses to width 0 — 8 bytes for the whole lane.
//   plain    text-like lanes (prefixes, inline payload bytes) fall through, and
//            the codec picks up what the encodings could not.
// Emits ONE lane in whichever (encoding, codec) form stores smallest.
//
// First-encoding-wins is a TRAP here, measured: on TPC-H l_comment the prefix
// lane bit-packs 32 -> 31 bits — a 3% "win" — but packing at a non-byte width
// misaligns the text-like bytes so the codec's matcher finds nothing, and the
// lane stores at 0.97x where PLAIN-then-zstd reaches ~0.43x. A smaller
// intermediate is not a smaller file. So every viable form is costed to its
// FINAL stored size — encoding alone, and encoding+codec where the codec
// clears its gate — and the smallest wins; ties go to the fewest decode
// stages.
Status emit_slot_lane(WriteContext& ctx, SectionKind kind,
                      const std::vector<uint32_t>& lane) {
    const uint32_t count = static_cast<uint32_t>(lane.size());
    const size_t   plain = static_cast<size_t>(count) * sizeof(uint32_t);

    struct Form {
        Encoding             encoding;
        std::vector<uint8_t> body;      // empty for kPlain (points at the lane)
        std::vector<uint8_t> packed;    // codec output; empty == not applied
    };

    // Candidate order IS the tie-break order: plain first (one decode stage),
    // then delta, then bitpack.
    std::vector<Form> forms;
    forms.push_back(Form{Encoding::kPlain, {}, {}});
    {
        std::vector<uint8_t> body;
        if (delta_bitpack_encode(lane.data(), count, sizeof(uint32_t), &body))
            forms.push_back(Form{Encoding::kDeltaBitpack, std::move(body), {}});
        body.clear();
        if (bitpack_encode_u32(lane.data(), count, &body))
            forms.push_back(Form{Encoding::kBitpack, std::move(body), {}});
    }

    size_t best = 0;
    size_t best_stored = SIZE_MAX;
    for (size_t i = 0; i < forms.size(); ++i) {
        Form& form = forms[i];
        const uint8_t* body = form.encoding == Encoding::kPlain
            ? reinterpret_cast<const uint8_t*>(lane.data()) : form.body.data();
        const size_t body_bytes = form.encoding == Encoding::kPlain
            ? plain : form.body.size();

        size_t stored = body_bytes;
        if (ctx.codec != SectionCodec::kNone && body_bytes >= kCompressMinBytes) {
            std::vector<uint8_t> packed;
            bool worthwhile = ctx.codec == SectionCodec::kZstd
                ? zstd_encode(body, body_bytes, ctx.zstd_level, &packed)
                : lz4_encode(body, body_bytes, &packed);
            if (worthwhile && form.encoding != Encoding::kPlain
                    && packed.size() * 100u > body_bytes * kStackFloorPercent)
                worthwhile = false;
            if (worthwhile) {
                stored = packed.size();
                form.packed = std::move(packed);
            }
        }
        if (stored < best_stored) { best_stored = stored; best = i; }
    }

    Form& winner = forms[best];
    const uint8_t* body = winner.encoding == Encoding::kPlain
        ? reinterpret_cast<const uint8_t*>(lane.data()) : winner.body.data();
    const size_t body_bytes = winner.encoding == Encoding::kPlain
        ? plain : winner.body.size();
    if (!winner.packed.empty())
        return emit_raw(ctx, kind, winner.encoding, ctx.codec, winner.packed.data(),
                        winner.packed.size(), body_bytes, plain);
    return emit_raw(ctx, kind, winner.encoding, SectionCodec::kNone, body,
                    body_bytes, body_bytes, plain);
}

Status emit_slot_lanes(WriteContext& ctx, const DrakenStringSlot* slots,
                       uint64_t slot_count) {
    const size_t n = static_cast<size_t>(slot_count);
    std::vector<uint32_t> lanes[4];
    for (int k = 0; k < 4; ++k) lanes[k].resize(n);
    const uint32_t* words = reinterpret_cast<const uint32_t*>(slots);
    for (size_t i = 0; i < n; ++i) {
        lanes[0][i] = words[i * 4 + 0];
        lanes[1][i] = words[i * 4 + 1];
        lanes[2][i] = words[i * 4 + 2];
        lanes[3][i] = words[i * 4 + 3];
    }
    SKENE_RETURN_IF_ERROR(emit_slot_lane(ctx, SectionKind::kSlotLane0, lanes[0]));
    SKENE_RETURN_IF_ERROR(emit_slot_lane(ctx, SectionKind::kSlotLane1, lanes[1]));
    SKENE_RETURN_IF_ERROR(emit_slot_lane(ctx, SectionKind::kSlotLane2, lanes[2]));
    return emit_slot_lane(ctx, SectionKind::kSlotLane3, lanes[3]);
}

// An optional section body, built alongside the column and staged as the
// node's INDEX sections once its required ones are written.
struct PendingIndex {
    SectionKind          kind;
    std::vector<uint8_t> body;
};

// One column node's facts for one row group — what becomes its ChunkRecord and
// its statistics, plus the identity checked against the file's schema.
struct PlanHead {
    uint32_t field_id = 0;
    uint32_t type = 0;
    uint8_t  logical_present = 0;
    uint8_t  vector_flags = 0;
    uint8_t  selection_kind = 0;
    uint8_t  value_order = 0;
    uint32_t length = 0;
    uint32_t data_length = 0;
    uint32_t child_count = 0;
    uint32_t section_index = 0;
    uint32_t section_count = 0;
    uint32_t index_section_index = 0;
    uint32_t index_section_count = 0;
    uint64_t string_slot_count = 0;
    uint64_t string_arena_used = 0;
    uint64_t string_arena_cap = 0;
    uint8_t  string_payloads_elided = 0;
};

// Everything the footer and directory need about one column node in one row
// group, gathered while its sections are staged. ARRAY children nest.
struct ColumnPlan {
    uint32_t                  node = 0;          // depth-first node ordinal
    PlanHead                  head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    ColumnStatistics          statistics{};
    bool                      has_statistics = false;
    std::vector<PendingIndex> index_sections;
    std::vector<ColumnPlan>   children;
};


// Per-row-chunk VALUE ORDINAL bounds (ZoneMapEntry), for skipping rows within
// one row group — built for every orderable column with more than one row
// chunk, whatever its encoding shape or ordering (format.h, ZoneMapEntry).
inline bool row_is_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

void build_zone_map(const DrakenVector& v, const LogicalType* logical,
                    ColumnPlan* plan) {
    const uint32_t length = v.length;
    const uint32_t chunk_rows = kZoneMapDefaultChunkRows;
    // One chunk is the whole column, which is what the footer's min/max already
    // say — an index that cannot skip anything is pure overhead.
    if (length <= chunk_rows) return;
    if (!type_has_min_max(v.type)) return;

    const uint32_t chunk_count = (length + chunk_rows - 1u) / chunk_rows;

    std::vector<uint8_t> body(sizeof(ZoneMapHeader)
                              + static_cast<size_t>(chunk_count) * sizeof(ZoneMapEntry));
    ZoneMapHeader header{};
    header.chunk_rows  = chunk_rows;
    header.chunk_count = chunk_count;
    std::memcpy(body.data(), &header, sizeof(header));

    ZoneMapEntry* entries =
        reinterpret_cast<ZoneMapEntry*>(body.data() + sizeof(ZoneMapHeader));

    for (uint32_t chunk = 0; chunk < chunk_count; ++chunk) {
        const uint32_t begin = chunk * chunk_rows;
        const uint32_t end   = (begin + chunk_rows < length) ? begin + chunk_rows : length;

        // Empty range until a non-null value is seen. An all-null chunk keeps it,
        // and min > max answers "cannot contain" for every probe.
        int64_t lo = INT64_MAX, hi = INT64_MIN;
        for (uint32_t row = begin; row < end; ++row) {
            if (!row_is_valid(v, row)) continue;
            int64_t ordinal = 0;
            if (!column_ordinal_at(v, logical, v.selection[row], &ordinal)) return;
            if (ordinal < lo) lo = ordinal;
            if (ordinal > hi) hi = ordinal;
        }
        entries[chunk].min_ordinal = lo;
        entries[chunk].max_ordinal = hi;
    }

    plan->index_sections.push_back(PendingIndex{SectionKind::kZoneMap, std::move(body)});
}



// ─── Column nodes across row groups ─────────────────────────────────────────

// One column's statistics in one row group. `present` is not derivable from the
// blob — an all-zero ColumnStatistics is a legal tracked value — so absence is
// written as an explicit zero length rather than inferred.
struct StatSlot {
    bool             present = false;
    ColumnStatistics statistics{};
};

// Everything the file accumulates about one column node, row group by row
// group, until finish() lays it out: its staged sections, one chunk record and
// one statistics slot per row group, and its whole-file sketch.
struct NodeAccum {
    std::vector<SectionEntry> sections;   // `offset` is a STAGING offset until finish()
    std::vector<ChunkRecord>  chunks;     // one per row group
    std::vector<StatSlot>     stats;      // one per row group
    FileSketch                sketch;
    bool                      sketch_void = false;
};

// Hands out depth-first node ordinals while a row group is written. Row group 0
// creates the nodes; every later row group must produce exactly the same ones —
// the schema check that follows names any divergence, this only stops a row
// group from addressing a node the file does not have.
struct NodeCursor {
    std::deque<NodeAccum>& nodes;
    uint32_t               next = 0;
    bool                   create = false;
    uint32_t               row_group = 0;

    Status acquire(const char* column_name, uint32_t* out) {
        if (next >= nodes.size()) {
            if (!create)
                return fail(Code::kMalformed,
                            "row group %u has more column nodes than the file's "
                            "schema (at column '%s') — every row group in a file "
                            "must share one schema", row_group, column_name);
            nodes.emplace_back();
        }
        *out = next++;
        return Status::ok();
    }
};

// The string family is the sharpest edge in the format: DrakenStringArena's
// `slots` and `arena` are ABSOLUTE POINTERS and are never written. The scalar
// fields go in the column directory, the slot block and arena bytes go in their
// own sections, and the reader rebuilds the two pointers into a fresh block.
Status write_string_column(WriteContext& ctx, const DrakenVector& v,
                           const char* name, ColumnPlan* plan) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    if (sa == nullptr)
        return fail(Code::kMalformed, "column '%s': string vector has null data", name);

    // Codes index into the slot array, so a data_length beyond it would let a
    // reader address slots that do not exist.
    if (static_cast<uint64_t>(v.data_length) > sa->length)
        return fail(Code::kMalformed,
                    "column '%s': data_length %u exceeds slot count %llu",
                    name, v.data_length, static_cast<unsigned long long>(sa->length));

    // payloads_elided is not cosmetic. A length-only column has a NULL arena and
    // every long slot stamped STR_ELIDED_PAYLOAD_OFFSET as a TRAP value; losing
    // the flag turns that trap into a 4 GB out-of-bounds read. Verify the
    // invariant on the way OUT as well as on the way in — a file that is already
    // inconsistent when written can never be read safely.
    if (sa->slots == nullptr && sa->length > 0)
        return fail(Code::kMalformed, "column '%s': %llu slots but null slot array",
                    name, static_cast<unsigned long long>(sa->length));

    for (uint64_t i = 0; i < sa->length; ++i) {
        const DrakenStringSlot* slot = &sa->slots[i];
        if (str_is_inline(slot)) continue;  // payload lives in the slot itself
        const uint32_t offset = slot->ext.arena_offset;
        if (sa->payloads_elided) {
            if (offset != STR_ELIDED_PAYLOAD_OFFSET)
                return fail(Code::kMalformed,
                            "column '%s': payloads_elided is set but slot %llu "
                            "carries arena offset %u, not the elided trap value",
                            name, static_cast<unsigned long long>(i), offset);
        } else {
            const uint64_t end = static_cast<uint64_t>(offset) + str_length(slot);
            if (end > sa->arena_used)
                return fail(Code::kMalformed,
                            "column '%s': slot %llu spans arena bytes [%u, %llu) "
                            "but only %llu bytes are used",
                            name, static_cast<unsigned long long>(i), offset,
                            static_cast<unsigned long long>(end),
                            static_cast<unsigned long long>(sa->arena_used));
        }
    }
    if (sa->payloads_elided && sa->arena_used != 0)
        return fail(Code::kMalformed,
                    "column '%s': payloads_elided is set but arena_used is %llu",
                    name, static_cast<unsigned long long>(sa->arena_used));

    plan->head.string_slot_count      = sa->length;
    plan->head.string_arena_used      = sa->arena_used;
    plan->head.string_arena_cap       = sa->arena_cap;
    plan->head.string_payloads_elided = sa->payloads_elided;

    SKENE_RETURN_IF_ERROR(emit_slot_lanes(ctx, sa->slots, sa->length));

    if (sa->arena_used > 0) {
        if (sa->arena == nullptr)
            return fail(Code::kMalformed,
                        "column '%s': arena_used is %llu but the arena is null",
                        name, static_cast<unsigned long long>(sa->arena_used));
        SKENE_RETURN_IF_ERROR(emit_section(ctx, SectionKind::kStringArena, sa->arena,
                                           static_cast<size_t>(sa->arena_used)));
    }
    return Status::ok();
}

Status write_column_data(const WriteContext& base, NodeCursor& cursor,
                         const CxxColumn& column,
                         const std::string& name, uint32_t field_id,
                         const WriteOptions& options, bool allow_value_order,
                         ColumnPlan* plan) {
    const DrakenVector& v = column.view;
    const VectorOwner*  own = column.own.get();
    const char*         cname = name.c_str();

    // This column's node: its own section list, staging stream and sketch.
    SKENE_RETURN_IF_ERROR(cursor.acquire(cname, &plan->node));
    NodeAccum& node = cursor.nodes[plan->node];
    WriteContext ctx = base;
    ctx.node     = plan->node;
    ctx.index    = false;
    ctx.sections = &node.sections;

    plan->name = name;
    plan->head.field_id    = field_id;
    plan->head.type        = static_cast<uint32_t>(v.type);
    plan->head.vector_flags = v.flags;   // VERBATIM — re-deriving hints is what
                                         // disqualified Parquet.
    plan->head.length      = v.length;
    plan->head.data_length = v.data_length;
    plan->head.value_order = static_cast<uint8_t>(ValueOrder::kAsWritten);

    // Logical type. This is the payload Parquet cannot carry: an IPv4 column is
    // a UINT32 refined by an IPV4 descriptor, and a format that drops the
    // descriptor hands back a bare unsigned integer.
    const LogicalType* lt = (own != nullptr) ? own->logical_type : nullptr;
    if (lt != nullptr) {
        plan->head.logical_present   = 1;
        plan->logical.kind           = static_cast<uint8_t>(lt->kind);
        plan->logical.unit           = static_cast<uint8_t>(lt->unit);
        plan->logical.offset_minutes = lt->offset_minutes;
        plan->logical.precision      = lt->precision;
        plan->logical.scale          = lt->scale;
        plan->logical.dimension      = lt->dimension;
    } else if (type_requires_logical_type(v.type)) {
        return fail(Code::kMalformed,
                    "column '%s': physical type %u requires a LogicalType "
                    "descriptor and has none — the stored bits would be "
                    "uninterpretable",
                    cname, static_cast<unsigned>(v.type));
    }

    // ── Value ordering ──
    //
    // Sorts and deduplicates `data`, rewriting the codes so logical rows are
    // unchanged. Not every column is eligible (see value_order.h); an ineligible
    // one is written kAsWritten, which is honest rather than a claim we cannot
    // back.
    OrderedColumn ordered;
    if (options.read_acceleration && allow_value_order)
        SKENE_RETURN_IF_ERROR(order_column(v, lt, cname, &ordered));

    const uint32_t* codes       = ordered.applied ? ordered.codes.data() : v.selection;
    const uint32_t  data_length = ordered.applied ? ordered.data_length : v.data_length;

    if (ordered.applied) {
        // Flags are REPLACED, not inherited: the layout the input's flags
        // described no longer exists once values move.
        plan->head.vector_flags = ordered.flags;
        plan->head.data_length  = data_length;
        plan->head.value_order  = static_cast<uint8_t>(ValueOrder::kAscending);
    }

    // ── Statistics ──
    //
    // Every statistic here is a property of the MULTISET of values (min, max,
    // null_count, sum), so ordering cannot change any of them — but an ordered
    // column can compute min/max from the ends of its sorted array instead of
    // scanning every row, so this runs after ordering to take that path.
    if (options.read_acceleration) {
        SKENE_RETURN_IF_ERROR(
            compute_statistics(v, lt, cname, &plan->statistics,
                               (ordered.applied && !draken_type_is_string_storage(v.type))
                                   ? ordered.data.get() : nullptr,
                               ordered.applied ? ordered.data_length : 0u));
        plan->has_statistics   = true;

        // v2 NDV: exact when ordering deduplicated (data_length IS the distinct
        // non-null count), the KMV estimate when the sketch measured the column
        // and declined. Never both spellings; never the biased sample verdict.
        if (ordered.applied) {
            plan->statistics.ndv = ordered.data_length;
            plan->statistics.flags |= kStatNdv | kStatNdvExact;
        } else if (ordered.ndv_estimate > 0.0) {
            plan->statistics.ndv =
                static_cast<uint64_t>(ordered.ndv_estimate + 0.5);
            plan->statistics.flags |= kStatNdv;
        }

        // v3 sketch: ONE per column node per FILE, in draken's Vector.hash()
        // family, so it unions with catalog and ANALYZE sketches (FORMAT.md
        // §8.1). Every row group adds its rows; a row group that cannot be
        // hashed (a length-only string chunk) voids the node's sketch, because
        // a union missing a contributor under-counts with nothing to say so.
        // Hashed from the ORIGINAL vector: ordering preserves every row's value.
        if (sketch_supported(v))
            SKENE_RETURN_IF_ERROR(sketch_add_rows(v, &node.sketch));
        else
            node.sketch_void = true;
    }

    SelectionKind selection_kind;
    SKENE_RETURN_IF_ERROR(
        classify_codes(codes, v.length, data_length, cname, &selection_kind));
    plan->head.selection_kind = static_cast<uint8_t>(selection_kind);

    const uint32_t first_section = static_cast<uint32_t>(ctx.sections->size());

    // ── Payload, by family ──
    if (draken_type_is_string_storage(v.type)) {
        if (ordered.applied) {
            plan->head.string_slot_count      = ordered.slot_count;
            plan->head.string_arena_used      = ordered.arena_used;
            plan->head.string_arena_cap       = ordered.arena_used;
            plan->head.string_payloads_elided = 0;  // elided columns are never ordered
            SKENE_RETURN_IF_ERROR(emit_slot_lanes(
                ctx, reinterpret_cast<const DrakenStringSlot*>(ordered.data.get()),
                ordered.slot_count));
            if (ordered.arena_used > 0)
                SKENE_RETURN_IF_ERROR(emit_section(
                    ctx, SectionKind::kStringArena, ordered.arena.get(),
                    static_cast<size_t>(ordered.arena_used)));
        } else {
            SKENE_RETURN_IF_ERROR(write_string_column(ctx, v, cname, plan));
        }
    } else if (v.type == DRAKEN_BOOL) {
        // BOOL is never value-ordered (bit-packed; codes would inflate it ~32x).
        SKENE_RETURN_IF_ERROR(emit_section(
            ctx, SectionKind::kData, v.data,
            (static_cast<size_t>(v.data_length) + 7u) / 8u));
    } else if (v.type == DRAKEN_ARRAY) {
        // Offsets are sized by the LOGICAL row count, not data_length: arrays
        // are stored dense (draken_native.cpp D.13). Never value-ordered.
        SKENE_RETURN_IF_ERROR(emit_section(
            ctx, SectionKind::kData, v.data,
            (static_cast<size_t>(v.length) + 1u) * sizeof(int32_t)));
    } else if (v.type == DRAKEN_NULL) {
        // Self-describing: type == NULL means every row is null. No data, no
        // validity, nothing to write.
    } else {
        const size_t itemsize = draken_type_itemsize(v.type, lt);
        if (itemsize == 0)
            return fail(Code::kUnsupportedType,
                        "column '%s': no fixed item width for physical type %u",
                        cname, static_cast<unsigned>(v.type));
        SKENE_RETURN_IF_ERROR(
            emit_fixed_data(ctx, ordered.applied ? ordered.data.get() : v.data,
                            data_length, itemsize, v.type, ordered.applied));
    }

    // An ABSENT validity section already means "every row is valid", so writing
    // an all-ones bitmap stores a fact the reader would infer anyway. Producers
    // hand these over routinely — every TPC-H column arrives with one, and they
    // were ~400 KB per file of pure redundancy. Dropping them removes the bytes
    // outright, which beats compressing them: no write cost, no read cost, and
    // no dependence on the section clearing the compression size floor.
    //
    // The scan is over the bitmap, not the rows, so it is length/8 bytes.
    if (v.validity != nullptr && v.type != DRAKEN_NULL
            && !bitmap_is_all_set(v.validity, v.length))
        SKENE_RETURN_IF_ERROR(emit_section(
            ctx, SectionKind::kValidity, v.validity,
            (static_cast<size_t>(v.length) + 7u) / 8u));

    // Identity and constant selections store NO section — the reader attaches
    // the shared global. Only genuinely owned codes are written.
    if (selection_kind == SelectionKind::kStored)
        SKENE_RETURN_IF_ERROR(emit_selection(ctx, codes, v.length, data_length));

    // Zone maps on EVERY column of an orderable type — not only ordered ones,
    // and not only dictionary-encoded ones.
    //
    // Built from the ORIGINAL vector and its original selection, which is exact:
    // value ordering rewrites `data` and the codes together but preserves
    // data[selection[i]] for every logical row, so the ordinal at row i is the
    // same before and after. That makes the index independent of ordering
    // instead of a by-product of it.
    //
    // 8k is a small slice, and a chunk skipped is a chunk never decoded. Whether
    // a given column is one anybody filters on is not the writer's judgement to
    // make. Measured cost on TPC-H and ClickBench: 0.09-0.14% of the file.
    if (options.read_acceleration)
        build_zone_map(v, lt, plan);

    // Bloom filter, over the DATA array rather than the rows: on an ordered
    // column that is the deduplicated dictionary, so the filter costs NDV
    // insertions rather than one per row.
    //
    // That substitution is an OPTIMIZATION and nothing downstream may assume it
    // happened. It applies only when value ordering was APPLIED, so a column that
    // declined it arrives here with data_length == its row count — which is why
    // bloom_build establishes the distinct count itself rather than trusting
    // data_length to be one. Sizing on data_length was the bug: every repetitive
    // column that declined ordering got a filter sized for its rows.
    //
    // Built for EVERY eligible column by default. `bloom_columns` narrows that
    // when a caller knows better; empty means all of them, which is the posture
    // that matches zone maps — an equality probe answered from the footer is a
    // column read that never happens.
    const bool wants_bloom =
        options.read_acceleration
        && (options.bloom_columns.empty()
            || std::find(options.bloom_columns.begin(), options.bloom_columns.end(),
                         name) != options.bloom_columns.end());
    if (wants_bloom) {
        DrakenVector filter_view = v;
        if (ordered.applied && !draken_type_is_string_storage(v.type)) {
            filter_view.data        = ordered.data.get();
            filter_view.data_length = ordered.data_length;
        }
        std::vector<uint8_t> body;
        if (bloom_build(filter_view, options.bloom_false_positive_rate, &body))
            plan->index_sections.push_back(
                PendingIndex{SectionKind::kBloom, std::move(body)});
    }

    plan->head.section_index = first_section;
    plan->head.section_count =
        static_cast<uint32_t>(ctx.sections->size()) - first_section;

    // The node's OPTIONAL sections, staged apart from its required ones: the
    // required ones are laid out in the node's DATA run and these in the INDEX
    // region (FORMAT.md §3), and the two slices of the chunk record say which
    // is which.
    {
        WriteContext index_ctx = ctx;
        index_ctx.index = true;
        plan->head.index_section_index = static_cast<uint32_t>(ctx.sections->size());
        for (const PendingIndex& pending : plan->index_sections)
            SKENE_RETURN_IF_ERROR(emit_section(index_ctx, pending.kind,
                                               pending.body.data(), pending.body.size()));
        plan->head.index_section_count =
            static_cast<uint32_t>(ctx.sections->size()) - plan->head.index_section_index;
        plan->index_sections.clear();
    }

    // ── ARRAY child, recursively ──
    if (v.type == DRAKEN_ARRAY) {
        if (own == nullptr || !own->child_owner)
            return fail(Code::kMalformed,
                        "column '%s': ARRAY column has no child owner; its "
                        "elements are unreachable", cname);
        CxxColumn child_column;
        child_column.view = own->child_owner->vec;
        // The child's buffers are owned by the parent's subtree and outlive this
        // call, so the child view is read directly rather than re-owned. An
        // aliasing shared_ptr would let the writer look like it takes ownership
        // of something it must not free.
        child_column.own = std::shared_ptr<VectorOwner>(column.own, own->child_owner.get());

        // ARRAY CHILDREN ARE NEVER VALUE-ORDERED.
        //
        // Ordering a child is CORRECT under the uniform data[selection[i]]
        // contract — the child's logical rows are unchanged and it round-trips.
        // But it produces a DICT-SHAPED array child, and draken has never
        // executed one: ingestion always builds array children dense
        // (draken_native.cpp D.13, "arrays are always stored dense in this
        // implementation"), and the D.13 take/materialize/subscript machinery
        // was written against that. A storage layer is the wrong place to hand
        // the engine a shape it has never run, so this stays off until there is
        // a measured reason to turn it on.
        plan->children.emplace_back();
        SKENE_RETURN_IF_ERROR(write_column_data(base, cursor, child_column,
                                                name + ".element",
                                                field_id, options,
                                                /*allow_value_order=*/false,
                                                &plan->children.back()));
        plan->head.child_count = 1;
    }

    return Status::ok();
}

// ─── Cluster verification ───────────────────────────────────────────────────
//
// A cluster spec is a PROMISE the writer signs on the file's behalf, so the
// writer proves it over the actual rows before writing it down. One comparison
// per row per key, on the write side — where this format spends by charter.
//
// Comparison is by ordinal first (the same dialect the zone maps and manifest
// speak), with a full-bytes tiebreak for the string family, whose ordinals are
// monotonic but NOT injective (first-8-bytes pack). For every other orderable
// type the ordinal is order-faithful, so ordinal equality is order equality.

// One key column's value in the FINAL row of a row group, captured so the seam
// to the next row group can be verified after the morsel is gone.
struct SeamKeyValue {
    bool        captured = false;
    bool        is_null = false;
    int64_t     ordinal = 0;
    bool        has_bytes = false;
    std::string bytes;          // string family only
};

bool cluster_row_is_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

// Payload bytes of one string CODE (not row). Precondition: the column is
// string storage and not payloads-elided — enforced by the eligibility check.
void cluster_string_bytes(const DrakenVector& v, uint32_t code,
                          const uint8_t** out, uint32_t* out_len) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slot = &sa->slots[code];
    *out_len = str_length(slot);
    *out = str_is_inline(slot) ? slot->inl.data
                               : sa->arena + slot->ext.arena_offset;
}

int compare_bytes(const uint8_t* a, uint32_t a_len, const uint8_t* b, uint32_t b_len) {
    const uint32_t common = a_len < b_len ? a_len : b_len;
    const int cmp = common > 0 ? std::memcmp(a, b, common) : 0;
    if (cmp != 0) return cmp < 0 ? -1 : 1;
    if (a_len != b_len) return a_len < b_len ? -1 : 1;
    return 0;
}

// FILE-ORDER comparison of two key values: negative/zero means "a may precede
// b". Encodes the key's direction and draken's single null rule (NULLS FIRST
// ascending, LAST descending — validated against the SortKey up front).
int file_order_compare(bool a_null, bool b_null, int ascending_cmp, bool descending) {
    if (a_null && b_null) return 0;
    if (a_null) return descending ? 1 : -1;
    if (b_null) return descending ? -1 : 1;
    return descending ? -ascending_cmp : ascending_cmp;
}

// Verifies `morsel`'s rows are ordered by `keys`, including the seam from the
// previous row group's final row, and re-captures the seam for the next call.
Status verify_cluster_order(const CxxMorsel& morsel,
                            const std::vector<SortKey>& keys,
                            uint32_t row_group,
                            std::vector<SeamKeyValue>* seam) {
    const uint32_t rows = static_cast<uint32_t>(morsel.num_rows());

    struct KeyColumn {
        const DrakenVector* v;
        const LogicalType*  lt;
        bool                is_string;
    };
    std::vector<KeyColumn> columns(keys.size());

    for (size_t k = 0; k < keys.size(); ++k) {
        const SortKey& key = keys[k];
        if (key.column_ordinal >= morsel.columns.size())
            return fail(Code::kMalformed,
                        "cluster key %zu names column ordinal %u but the morsel "
                        "has %zu columns", k, key.column_ordinal,
                        morsel.columns.size());
        const CxxColumn& column = morsel.columns[key.column_ordinal];
        const DrakenVector& v = column.view;

        if (!type_has_min_max(v.type))
            return fail(Code::kMalformed,
                        "cluster key %zu ('%s') has physical type %u, which has "
                        "no defined order — it cannot be a cluster key", k,
                        morsel.names[key.column_ordinal].c_str(),
                        static_cast<unsigned>(v.type));
        const bool is_string = draken_type_is_string_storage(v.type) != 0;
        if (is_string) {
            const DrakenStringArena* sa =
                static_cast<const DrakenStringArena*>(v.data);
            if (sa == nullptr || sa->payloads_elided)
                return fail(Code::kMalformed,
                            "cluster key %zu ('%s') is a length-only string "
                            "column; its values cannot be compared", k,
                            morsel.names[key.column_ordinal].c_str());
        }
        columns[k] = KeyColumn{
            &v, column.own != nullptr ? column.own->logical_type : nullptr,
            is_string};
    }

    // One key value, read fresh from a row. `ordinal` is meaningless when
    // is_null — a null has no ordinal and must never be compared as one.
    auto read_key = [&](size_t k, uint32_t row, bool* is_null, int64_t* ordinal,
                        const uint8_t** bytes, uint32_t* len) -> Status {
        const KeyColumn& kc = columns[k];
        *is_null = !cluster_row_is_valid(*kc.v, row);
        if (*is_null) return Status::ok();
        const uint32_t code = kc.v->selection[row];
        if (!column_ordinal_at(*kc.v, kc.lt, code, ordinal))
            return fail(Code::kMalformed,
                        "cluster key %zu: no ordinal for row %u", k, row);
        if (kc.is_string) cluster_string_bytes(*kc.v, code, bytes, len);
        return Status::ok();
    };

    if (rows == 0) return Status::ok();  // nothing to order, seam unchanged

    // ── The seam: previous row group's last row vs this one's first ──
    if (!seam->empty() && (*seam)[0].captured) {
        for (size_t k = 0; k < keys.size(); ++k) {
            const SeamKeyValue& prev = (*seam)[k];
            bool cur_null = false; int64_t cur_ord = 0;
            const uint8_t* cur_bytes = nullptr; uint32_t cur_len = 0;
            SKENE_RETURN_IF_ERROR(
                read_key(k, 0, &cur_null, &cur_ord, &cur_bytes, &cur_len));

            int ascending_cmp = 0;
            if (!prev.is_null && !cur_null) {
                ascending_cmp = prev.ordinal < cur_ord ? -1
                              : prev.ordinal > cur_ord ? 1 : 0;
                if (ascending_cmp == 0 && columns[k].is_string)
                    ascending_cmp = compare_bytes(
                        reinterpret_cast<const uint8_t*>(prev.bytes.data()),
                        static_cast<uint32_t>(prev.bytes.size()),
                        cur_bytes, cur_len);
            }
            const int order = file_order_compare(prev.is_null, cur_null,
                                                 ascending_cmp,
                                                 keys[k].descending != 0);
            if (order < 0) break;
            if (order > 0)
                return fail(Code::kMalformed,
                            "cluster_keys declares an order the rows do not "
                            "have: row group %u row 0 sorts before the previous "
                            "row group's last row on key %zu ('%s')", row_group,
                            k, morsel.names[keys[k].column_ordinal].c_str());
        }
    }

    // ── Every adjacent pair within the row group ──
    for (uint32_t row = 1; row < rows; ++row) {
        for (size_t k = 0; k < keys.size(); ++k) {
            bool a_null = false, b_null = false;
            int64_t a_ord = 0, b_ord = 0;
            const uint8_t* a_bytes = nullptr; const uint8_t* b_bytes = nullptr;
            uint32_t a_len = 0, b_len = 0;
            SKENE_RETURN_IF_ERROR(
                read_key(k, row - 1u, &a_null, &a_ord, &a_bytes, &a_len));
            SKENE_RETURN_IF_ERROR(
                read_key(k, row, &b_null, &b_ord, &b_bytes, &b_len));

            int ascending_cmp = 0;
            if (!a_null && !b_null) {
                ascending_cmp = a_ord < b_ord ? -1 : a_ord > b_ord ? 1 : 0;
                if (ascending_cmp == 0 && columns[k].is_string)
                    ascending_cmp = compare_bytes(a_bytes, a_len, b_bytes, b_len);
            }
            const int order = file_order_compare(a_null, b_null, ascending_cmp,
                                                 keys[k].descending != 0);
            if (order < 0) break;   // strictly ordered on this key; later keys free
            if (order > 0)
                return fail(Code::kMalformed,
                            "cluster_keys declares an order the rows do not "
                            "have: row group %u rows %u and %u are out of order "
                            "on key %zu ('%s')", row_group, row - 1u, row, k,
                            morsel.names[keys[k].column_ordinal].c_str());
            // order == 0: tied on this key, the next key decides.
        }
    }

    // ── Capture the final row for the next seam ──
    seam->assign(keys.size(), SeamKeyValue{});
    for (size_t k = 0; k < keys.size(); ++k) {
        SeamKeyValue& capture = (*seam)[k];
        const uint8_t* bytes = nullptr; uint32_t len = 0;
        SKENE_RETURN_IF_ERROR(read_key(k, rows - 1u, &capture.is_null,
                                       &capture.ordinal, &bytes, &len));
        capture.captured = true;
        if (!capture.is_null && columns[k].is_string) {
            capture.has_bytes = true;
            capture.bytes.assign(reinterpret_cast<const char*>(bytes), len);
        }
    }
    return Status::ok();
}

// ─── Schema ─────────────────────────────────────────────────────────────────

// The invariant half of a column: what the FILE says about it, as against what
// one row group says. Captured from the first row group and then ENFORCED on
// every later one — see FileWriter::add_row_group.
struct SchemaNode {
    uint32_t                field_id = 0;
    std::string             name;
    uint32_t                type = 0;
    bool                    logical_present = false;
    LogicalTypeDescriptor   logical{};
    std::vector<SchemaNode> children;
};

SchemaNode schema_from_plan(const ColumnPlan& plan) {
    SchemaNode node;
    node.field_id        = plan.head.field_id;
    node.name            = plan.name;
    node.type            = plan.head.type;
    node.logical_present = plan.head.logical_present != 0;
    node.logical         = plan.logical;
    node.children.reserve(plan.children.size());
    for (const ColumnPlan& child : plan.children)
        node.children.push_back(schema_from_plan(child));
    return node;
}

// Every field a reader would use to decide "this is the same column". A
// divergence here means the footer's schema directory does not describe this
// row group, which a reader cannot detect and would silently mis-type.
Status check_schema_matches(const SchemaNode& expected, const ColumnPlan& plan,
                            uint32_t row_group) {
    if (plan.name != expected.name)
        return fail(Code::kMalformed,
                    "row group %u has column '%s' where the file's schema has "
                    "'%s' — every row group in a file must share one schema",
                    row_group, plan.name.c_str(), expected.name.c_str());
    if (plan.head.type != expected.type)
        return fail(Code::kMalformed,
                    "row group %u: column '%s' is type %u but the file's schema "
                    "says %u", row_group, plan.name.c_str(), plan.head.type,
                    expected.type);
    if ((plan.head.logical_present != 0) != expected.logical_present)
        return fail(Code::kMalformed,
                    "row group %u: column '%s' %s a logical type descriptor but "
                    "the file's schema %s one", row_group, plan.name.c_str(),
                    plan.head.logical_present ? "carries" : "lacks",
                    expected.logical_present ? "has" : "does not");
    if (expected.logical_present
            && std::memcmp(&plan.logical, &expected.logical,
                           sizeof(LogicalTypeDescriptor)) != 0)
        return fail(Code::kMalformed,
                    "row group %u: column '%s' carries a different logical type "
                    "descriptor than the file's schema",
                    row_group, plan.name.c_str());
    if (plan.head.field_id != expected.field_id)
        return fail(Code::kMalformed,
                    "row group %u: column '%s' has field_id %u but the file's "
                    "schema says %u", row_group, plan.name.c_str(),
                    plan.head.field_id, expected.field_id);
    if (plan.children.size() != expected.children.size())
        return fail(Code::kMalformed,
                    "row group %u: column '%s' has %zu children but the file's "
                    "schema says %zu", row_group, plan.name.c_str(),
                    plan.children.size(), expected.children.size());
    for (size_t i = 0; i < plan.children.size(); ++i)
        SKENE_RETURN_IF_ERROR(
            check_schema_matches(expected.children[i], plan.children[i], row_group));
    return Status::ok();
}

// Records one row group's plan into its nodes: the chunk record and the
// statistics slot. Depth first, the same order the node ordinals were issued.
void record_plan(const ColumnPlan& plan, std::deque<NodeAccum>* nodes) {
    NodeAccum& node = (*nodes)[plan.node];
    ChunkRecord record{};
    record.length                 = plan.head.length;
    record.data_length            = plan.head.data_length;
    record.vector_flags           = plan.head.vector_flags;
    record.selection_kind         = plan.head.selection_kind;
    record.value_order            = plan.head.value_order;
    record.string_payloads_elided = plan.head.string_payloads_elided;
    record.section_index          = plan.head.section_index;
    record.section_count          = plan.head.section_count;
    record.index_section_index    = plan.head.index_section_index;
    record.index_section_count    = plan.head.index_section_count;
    record.reserved0              = 0;
    record.string_slot_count      = plan.head.string_slot_count;
    record.string_arena_used      = plan.head.string_arena_used;
    record.string_arena_cap       = plan.head.string_arena_cap;
    record.reserved1              = 0;
    node.chunks.push_back(record);

    StatSlot slot;
    slot.present    = plan.has_statistics;
    slot.statistics = plan.statistics;
    node.stats.push_back(slot);

    for (const ColumnPlan& child : plan.children) record_plan(child, nodes);
}

void write_schema_entry(ByteWriter& w, const SchemaNode& node) {
    SchemaEntryHead head{};
    head.field_id        = node.field_id;
    head.name_bytes      = static_cast<uint32_t>(node.name.size());
    head.type            = node.type;
    head.logical_present = node.logical_present ? 1u : 0u;
    head.reserved0       = 0;
    head.reserved1       = 0;
    head.child_count     = static_cast<uint32_t>(node.children.size());
    w.pod(head);
    w.bytes(node.name.data(), node.name.size());
    if (node.logical_present) w.pod(node.logical);
    for (const SchemaNode& child : node.children) write_schema_entry(w, child);
}

// child_count per node, depth first — the order the nodes were issued in.
void collect_child_counts(const SchemaNode& node, std::vector<uint32_t>* out) {
    out->push_back(static_cast<uint32_t>(node.children.size()));
    for (const SchemaNode& child : node.children) collect_child_counts(child, out);
}

Status validate_cluster_keys(const WriteOptions& options) {
    for (size_t k = 0; k < options.cluster_keys.size(); ++k) {
        const SortKey& key = options.cluster_keys[k];
        if (key.reserved != 0)
            return fail(Code::kMalformed,
                        "cluster key %zu: reserved bytes are %u, not 0", k,
                        key.reserved);
        // draken's single sort null rule: NULLS FIRST ascending, LAST
        // descending (format.h SortKey). A key written under any other rule is
        // a DIFFERENT order wearing the same name, so it is rejected rather
        // than recorded.
        const bool expected_nulls_first = key.descending == 0;
        if ((key.nulls_first != 0) != expected_nulls_first)
            return fail(Code::kMalformed,
                        "cluster key %zu: nulls_first=%u with descending=%u "
                        "violates draken's sort rule (NULLS FIRST ascending, "
                        "LAST descending)", k, key.nulls_first, key.descending);
    }
    return Status::ok();
}

Status validate_options(const WriteOptions& options) {
    SKENE_RETURN_IF_ERROR(validate_cluster_keys(options));
    if (options.block_row_groups == 0)
        return fail(Code::kMalformed,
                    "block_row_groups is 0 — a block holds at least one row group");
    // `codec` and `zstd_level` describe one setting between them, so a
    // combination that means two different things is rejected rather than
    // resolved. A caller who sets a level and gets no zstd — or selects zstd and
    // gets an unspecified level — has a file that is not what they asked for,
    // and nothing downstream would ever tell them.
    switch (options.codec) {
        case SectionCodec::kZstd:
            if (options.zstd_level < 1 || options.zstd_level > 22)
                return fail(Code::kMalformed,
                            "codec is zstd but zstd_level is %d — zstd levels "
                            "run 1 to 22", options.zstd_level);
            return Status::ok();
        case SectionCodec::kNone:
        case SectionCodec::kLz4:
            if (options.zstd_level != 0)
                return fail(Code::kMalformed,
                            "zstd_level is %d but the selected codec is not "
                            "zstd — the level would be ignored",
                            options.zstd_level);
            return Status::ok();
        default:
            return fail(Code::kMalformed, "unknown section codec %u",
                        static_cast<unsigned>(options.codec));
    }
}

uint64_t align_up(uint64_t at) {
    const uint64_t misaligned = at % kSectionAlign;
    return misaligned == 0 ? at : at + (kSectionAlign - misaligned);
}

// Where finish() puts one column node, computed in full before a byte is
// written — directory blocks precede the chunks they describe, so every final
// offset must be known first.
struct NodeLayout {
    uint64_t                 directory_offset = 0;
    uint32_t                 directory_bytes = 0;
    uint64_t                 data_offset = 0;
    uint64_t                 data_bytes = 0;
    uint64_t                 index_offset = 0;
    uint64_t                 index_bytes = 0;
    uint64_t                 directory_checksum = 0;   // set when the block is emitted
    std::vector<uint64_t>    final_offset;   // parallel to NodeAccum::sections
    std::vector<BlockExtent> blocks;
};

}  // namespace

// ─── FileWriter ─────────────────────────────────────────────────────────────

struct FileWriter::State {
    WriteOptions          options;
    bool                  began = false;
    bool                  finished = false;
    // Set by any failed add_row_group. Pass 1 stages sections as it goes, so a
    // row group that failed half way has left sections no chunk record claims;
    // finishing would describe a file other than the one written.
    bool                  failed = false;

    Stage stage;
    Sink  sink;

    std::vector<RowGroupEntry> row_groups;
    uint64_t                   total_rows = 0;

    // Captured from row group 0 and enforced on every later one.
    std::vector<SchemaNode> schema;
    // Depth-first column nodes. A deque, because write_column_data holds a
    // reference to a node while its ARRAY child adds the next one.
    std::deque<NodeAccum>   nodes;
    // The previous row group's final key values, so the cluster order is
    // verified ACROSS row groups, not merely within each.
    std::vector<SeamKeyValue> cluster_seam;
};

FileWriter::FileWriter() : state_(new State()) {}
FileWriter::~FileWriter() = default;

uint32_t FileWriter::row_group_count() const {
    return static_cast<uint32_t>(state_->row_groups.size());
}

uint64_t FileWriter::staged_bytes() const { return state_->stage.staged_bytes(); }

namespace {

Status write_head(Sink* sink) {
    FileHead head{};
    head.magic              = kMagic;
    head.version            = kVersion;
    head.endianness         = static_cast<uint8_t>(Endianness::kLittle);
    head.checksum_algorithm = static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64);
    head.reserved           = 0;
    return sink->write(&head, sizeof(head));
}

}  // namespace

Status FileWriter::begin(const WriteOptions& options, std::vector<uint8_t>* out) {
    if (out == nullptr) return fail(Code::kMalformed, "FileWriter::begin: out is null");
    if (state_->began)
        return fail(Code::kMalformed, "FileWriter::begin called twice");
    SKENE_RETURN_IF_ERROR(validate_options(options));
    if (!options.scratch_path.empty())
        return fail(Code::kMalformed,
                    "scratch_path is set but the output is a buffer — buffer "
                    "output stages in memory; a scratch file is for path output");

    state_->options = options;
    state_->began   = true;
    out->clear();
    state_->stage.open_memory();
    state_->sink.open_memory(out);
    return write_head(&state_->sink);
}

Status FileWriter::begin(const WriteOptions& options, const std::string& path) {
    if (state_->began)
        return fail(Code::kMalformed, "FileWriter::begin called twice");
    SKENE_RETURN_IF_ERROR(validate_options(options));
    if (options.scratch_path.empty())
        return fail(Code::kMalformed,
                    "writing to a path stages sections in a scratch file, and "
                    "scratch_path names none — say where the writer may stage");
    if (options.scratch_path == path)
        return fail(Code::kMalformed,
                    "scratch_path and the output path are both '%s'", path.c_str());

    state_->options = options;
    state_->began   = true;
    SKENE_RETURN_IF_ERROR(state_->stage.open_file(options.scratch_path));
    SKENE_RETURN_IF_ERROR(state_->sink.open_file(path));
    return write_head(&state_->sink);
}

Status FileWriter::add_row_group(const CxxMorsel& morsel) {
    if (!state_->began)
        return fail(Code::kMalformed,
                    "FileWriter::add_row_group before begin()");
    if (state_->finished)
        return fail(Code::kMalformed,
                    "FileWriter::add_row_group after finish()");
    if (state_->failed)
        return fail(Code::kMalformed,
                    "FileWriter::add_row_group after an earlier row group failed; "
                    "this writer's staged state no longer describes a file");

    const WriteOptions& options = state_->options;
    const size_t column_count = morsel.columns.size();
    if (morsel.names.size() != column_count)
        return fail(Code::kMalformed, "%zu columns but %zu names",
                    column_count, morsel.names.size());
    if (!options.field_ids.empty() && options.field_ids.size() != column_count)
        return fail(Code::kMalformed,
                    "%zu field_ids for %zu columns — a partially assigned "
                    "schema is worse than an unassigned one",
                    options.field_ids.size(), column_count);

    const uint32_t index = static_cast<uint32_t>(state_->row_groups.size());

    // Every column holds the row group's rows. A morsel whose columns disagree
    // would write a chunk whose length is not its row group's — a file the
    // reader rejects (FORMAT.md §5.10), so it is refused here, at its source.
    const uint64_t rows = morsel.num_rows();
    for (size_t i = 0; i < column_count; ++i)
        if (morsel.columns[i].view.length != rows)
            return fail(Code::kMalformed,
                        "column '%s' has %u rows but the row group has %llu — every "
                        "column of a row group holds the same rows",
                        morsel.names[i].c_str(), morsel.columns[i].view.length,
                        static_cast<unsigned long long>(rows));

    // Cluster order is proved BEFORE anything is staged: a violation leaves the
    // writer exactly as it was.
    if (!options.cluster_keys.empty())
        SKENE_RETURN_IF_ERROR(verify_cluster_order(morsel, options.cluster_keys,
                                                   index, &state_->cluster_seam));

    // From here a failure leaves staged sections behind: poison the writer.
    Status status = [&]() -> Status {
        NodeCursor cursor{state_->nodes, 0u, index == 0, index};
        WriteContext base;
        base.stage      = &state_->stage;
        base.codec      = options.codec;
        base.zstd_level = options.zstd_level;

        std::vector<ColumnPlan> plans(column_count);
        for (size_t i = 0; i < column_count; ++i) {
            const uint32_t field_id =
                options.field_ids.empty() ? 0u : options.field_ids[i];
            SKENE_RETURN_IF_ERROR(write_column_data(base, cursor, morsel.columns[i],
                                                    morsel.names[i], field_id, options,
                                                    /*allow_value_order=*/true,
                                                    &plans[i]));
        }
        if (cursor.next != state_->nodes.size())
            return fail(Code::kMalformed,
                        "row group %u has %u column nodes but the file's schema "
                        "has %zu — every row group in a file must share one schema",
                        index, cursor.next, state_->nodes.size());

        // ── Schema: capture once, enforce thereafter ──
        if (index == 0) {
            state_->schema.reserve(plans.size());
            for (const ColumnPlan& plan : plans)
                state_->schema.push_back(schema_from_plan(plan));
        } else {
            if (plans.size() != state_->schema.size())
                return fail(Code::kMalformed,
                            "row group %u has %zu columns but the file's schema "
                            "has %zu — every row group in a file must share one "
                            "schema", index, plans.size(), state_->schema.size());
            for (size_t i = 0; i < plans.size(); ++i)
                SKENE_RETURN_IF_ERROR(
                    check_schema_matches(state_->schema[i], plans[i], index));
        }

        for (const ColumnPlan& plan : plans) record_plan(plan, &state_->nodes);

        RowGroupEntry entry{};
        entry.row_count = morsel.num_rows();
        entry.first_row = state_->total_rows;
        state_->total_rows += entry.row_count;
        state_->row_groups.push_back(entry);
        return Status::ok();
    }();
    if (!status.is_ok()) state_->failed = true;
    return status;
}

Status FileWriter::finish() {
    if (!state_->began)
        return fail(Code::kMalformed, "FileWriter::finish before begin()");
    if (state_->finished)
        return fail(Code::kMalformed, "FileWriter::finish called twice");
    if (state_->failed)
        return fail(Code::kMalformed,
                    "FileWriter::finish after a row group failed; refusing to "
                    "write a file its staged state does not describe");
    if (state_->row_groups.empty())
        return fail(Code::kMalformed,
                    "FileWriter::finish with no row groups — a .skene file with "
                    "nothing in it describes no data and would read back as a "
                    "schema-less object");

    const WriteOptions& options = state_->options;
    Sink& sink = state_->sink;
    std::deque<NodeAccum>& nodes = state_->nodes;
    const uint32_t row_groups = static_cast<uint32_t>(state_->row_groups.size());
    const uint32_t G = options.block_row_groups;
    const uint32_t block_count = (row_groups + G - 1u) / G;

    // ── Layout: every final offset, before anything is written ──
    std::vector<NodeLayout> layout(nodes.size());
    uint64_t at = sink.position();   // just past the head
    for (size_t n = 0; n < nodes.size(); ++n) {
        const NodeAccum& node = nodes[n];
        NodeLayout& L = layout[n];
        if (node.chunks.size() != row_groups)
            return fail(Code::kMalformed,
                        "internal: column node %zu has %zu chunk records for %u "
                        "row groups", n, node.chunks.size(), row_groups);
        const uint64_t directory_bytes =
            sizeof(DirectoryBlockHeader)
            + static_cast<uint64_t>(row_groups) * sizeof(ChunkRecord)
            + static_cast<uint64_t>(node.sections.size()) * sizeof(SectionEntry);
        if (directory_bytes > UINT32_MAX)
            return fail(Code::kMalformed,
                        "column node %zu's directory block is %llu bytes, which "
                        "exceeds the 32-bit directory_bytes field", n,
                        static_cast<unsigned long long>(directory_bytes));
        L.directory_offset = at;
        L.directory_bytes  = static_cast<uint32_t>(directory_bytes);
        at += directory_bytes;

        L.final_offset.assign(node.sections.size(), 0);
        L.blocks.assign(block_count, BlockExtent{0, 0});
        uint64_t begin = UINT64_MAX, end = 0;
        for (uint32_t g = 0; g < row_groups; ++g) {
            const ChunkRecord& chunk = node.chunks[g];
            BlockExtent& block = L.blocks[g / G];
            for (uint32_t s = 0; s < chunk.section_count; ++s) {
                const uint32_t i = chunk.section_index + s;
                at = align_up(at);
                L.final_offset[i] = at;
                // A block extent covers the sections that HOLD bytes; a block
                // of empty sections (zero-row chunks) is {0, 0} (FORMAT.md §5.6).
                if (node.sections[i].stored_bytes > 0 && block.bytes == 0)
                    block.offset = at;
                at += node.sections[i].stored_bytes;
                if (node.sections[i].stored_bytes > 0) block.bytes = at - block.offset;
                if (begin == UINT64_MAX) begin = L.final_offset[i];
                end = at;
            }
        }
        if (begin == UINT64_MAX) {
            L.data_offset = L.directory_offset + L.directory_bytes;
            L.data_bytes  = 0;
        } else {
            L.data_offset = begin;
            L.data_bytes  = end - begin;
        }
    }
    for (size_t n = 0; n < nodes.size(); ++n) {
        const NodeAccum& node = nodes[n];
        NodeLayout& L = layout[n];
        uint64_t begin = UINT64_MAX, end = 0;
        for (uint32_t g = 0; g < row_groups; ++g) {
            const ChunkRecord& chunk = node.chunks[g];
            for (uint32_t s = 0; s < chunk.index_section_count; ++s) {
                const uint32_t i = chunk.index_section_index + s;
                at = align_up(at);
                L.final_offset[i] = at;
                at += node.sections[i].stored_bytes;
                if (begin == UINT64_MAX) begin = L.final_offset[i];
                end = at;
            }
        }
        if (begin != UINT64_MAX) {
            L.index_offset = begin;
            L.index_bytes  = end - begin;
        }
    }
    const uint64_t footer_offset = at;

    // ── DATA region: per node, its directory block then its chunks ──
    for (size_t n = 0; n < nodes.size(); ++n) {
        const NodeAccum& node = nodes[n];
        NodeLayout& L = layout[n];

        std::vector<uint8_t> block;
        block.reserve(L.directory_bytes);
        ByteWriter w(&block);
        DirectoryBlockHeader header{};
        header.directory_magic = kDirectoryMagic;
        header.node_ordinal    = static_cast<uint32_t>(n);
        header.chunk_count     = row_groups;
        header.section_count   = static_cast<uint32_t>(node.sections.size());
        w.pod(header);
        for (const ChunkRecord& chunk : node.chunks) w.pod(chunk);
        for (size_t i = 0; i < node.sections.size(); ++i) {
            SectionEntry entry = node.sections[i];
            entry.offset = L.final_offset[i];
            w.pod(entry);
        }
        if (block.size() != L.directory_bytes || sink.position() != L.directory_offset)
            return fail(Code::kMalformed,
                        "internal: column node %zu's directory block does not "
                        "land where the layout put it", n);
        L.directory_checksum = checksum_xxh3_64(block.data(), block.size());
        SKENE_RETURN_IF_ERROR(sink.write(block.data(), block.size()));

        for (const ChunkRecord& chunk : node.chunks) {
            for (uint32_t s = 0; s < chunk.section_count; ++s) {
                const uint32_t i = chunk.section_index + s;
                SKENE_RETURN_IF_ERROR(sink.zeros(
                    static_cast<size_t>(L.final_offset[i] - sink.position())));
                SKENE_RETURN_IF_ERROR(state_->stage.copy_to(
                    static_cast<uint32_t>(n), false, node.sections[i].offset,
                    node.sections[i].stored_bytes, &sink));
            }
        }
        state_->stage.release(static_cast<uint32_t>(n), false);
    }

    // ── INDEX region ──
    for (size_t n = 0; n < nodes.size(); ++n) {
        const NodeAccum& node = nodes[n];
        const NodeLayout& L = layout[n];
        for (const ChunkRecord& chunk : node.chunks) {
            for (uint32_t s = 0; s < chunk.index_section_count; ++s) {
                const uint32_t i = chunk.index_section_index + s;
                SKENE_RETURN_IF_ERROR(sink.zeros(
                    static_cast<size_t>(L.final_offset[i] - sink.position())));
                SKENE_RETURN_IF_ERROR(state_->stage.copy_to(
                    static_cast<uint32_t>(n), true, node.sections[i].offset,
                    node.sections[i].stored_bytes, &sink));
            }
        }
        state_->stage.release(static_cast<uint32_t>(n), true);
    }
    if (sink.position() != footer_offset)
        return fail(Code::kMalformed,
                    "internal: the data and index regions end at %llu but the "
                    "layout put the footer at %llu",
                    static_cast<unsigned long long>(sink.position()),
                    static_cast<unsigned long long>(footer_offset));

    // ── FOOTER ──
    std::vector<uint8_t> footer;
    ByteWriter w(&footer);

    FileFooterHeader fh{};
    fh.footer_magic       = kFileFooterMagic;
    fh.footer_version     = kFileFooterVersion;
    fh.reserved           = 0;
    fh.row_count          = state_->total_rows;
    fh.row_group_count    = row_groups;
    fh.column_count       = static_cast<uint32_t>(state_->schema.size());
    fh.created_at_unix_us = options.created_at_unix_us;
    fh.writer_tag_bytes   = static_cast<uint32_t>(options.writer_tag.size());
    fh.block_row_groups   = G;
    fh.data_region_bytes  = footer_offset - kFileHeadBytes;
    std::memcpy(fh.file_uuid, options.file_uuid, sizeof(fh.file_uuid));
    w.pod(fh);
    w.bytes(options.writer_tag.data(), options.writer_tag.size());

    for (const RowGroupEntry& entry : state_->row_groups) w.pod(entry);
    for (const SchemaNode& node : state_->schema) write_schema_entry(w, node);

    // Cluster spec: written against the SCHEMA's top-level order, so an ordinal
    // past it would be a record no reader could resolve.
    {
        if (options.cluster_keys.size() > UINT16_MAX)
            return fail(Code::kMalformed, "%zu cluster keys exceed the 16-bit "
                        "key count", options.cluster_keys.size());
        for (const SortKey& key : options.cluster_keys)
            if (key.column_ordinal >= state_->schema.size())
                return fail(Code::kMalformed,
                            "cluster key names column ordinal %u but the schema "
                            "has %zu top-level columns", key.column_ordinal,
                            state_->schema.size());
        ClusterSpecHeader spec{};
        spec.key_count = static_cast<uint16_t>(options.cluster_keys.size());
        spec.reserved  = 0;
        w.pod(spec);
        for (const SortKey& key : options.cluster_keys) w.pod(key);
    }

    // Column summaries, depth first. Emitting them in node order IS the nested
    // shape: each head's child_count says its child's summary follows.
    std::vector<uint32_t> child_counts;
    for (const SchemaNode& node : state_->schema) collect_child_counts(node, &child_counts);
    if (child_counts.size() != nodes.size())
        return fail(Code::kMalformed,
                    "internal: the schema describes %zu column nodes but %zu were "
                    "written", child_counts.size(), nodes.size());
    for (size_t n = 0; n < nodes.size(); ++n) {
        const NodeAccum& node = nodes[n];
        const NodeLayout& L = layout[n];
        ColumnSummaryHead head{};
        head.directory_offset   = L.directory_offset;
        head.directory_bytes    = L.directory_bytes;
        head.reserved0          = 0;
        head.directory_checksum = L.directory_checksum;
        head.data_offset        = L.data_offset;
        head.data_bytes         = L.data_bytes;
        head.index_offset       = L.index_offset;
        head.index_bytes        = L.index_bytes;
        head.block_count        = block_count;
        head.child_count        = child_counts[n];
        w.pod(head);
        for (const BlockExtent& block : L.blocks) w.pod(block);

        SketchRecordHeader sketch{};
        const bool tracked = options.read_acceleration && !node.sketch_void
                             && node.sketch.size() > 0;
        if (tracked) {
            sketch.hash_family = kSketchFamilyDrakenVectorHash;
            sketch.k           = static_cast<uint16_t>(kSketchK);
            sketch.count       = static_cast<uint32_t>(node.sketch.size());
        }
        w.pod(sketch);
        if (tracked)
            for (uint64_t hash : node.sketch.hashes()) w.u64(hash);
    }

    // Per-row-group statistics, column-node major then row group (FORMAT.md
    // §5.8) — reachable from the footer alone, which is what keeps row group
    // pruning alive once manifest bounds coarsen to the union over a file.
    for (const NodeAccum& node : nodes) {
        for (const StatSlot& slot : node.stats) {
            if (!slot.present) { w.u32(0); continue; }
            w.u32(static_cast<uint32_t>(sizeof(ColumnStatistics)));
            w.pod(slot.statistics);
        }
    }

    if (footer.size() > UINT32_MAX)
        return fail(Code::kMalformed,
                    "footer is %zu bytes, which exceeds the 32-bit footer_bytes "
                    "field", footer.size());
    SKENE_RETURN_IF_ERROR(sink.write(footer.data(), footer.size()));

    // ── TAIL ──
    FileTail tail{};
    tail.footer_bytes       = static_cast<uint32_t>(footer.size());
    tail.footer_checksum    = checksum_xxh3_64(footer.data(), footer.size());
    tail.version            = kVersion;
    tail.endianness         = static_cast<uint8_t>(Endianness::kLittle);
    tail.checksum_algorithm = static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64);
    tail.reserved           = 0;
    tail.magic              = kMagic;
    SKENE_RETURN_IF_ERROR(sink.write(&tail, sizeof(tail)));

    SKENE_RETURN_IF_ERROR(sink.commit());
    state_->stage.close();
    state_->finished = true;
    return Status::ok();
}

Status write_morsel(const CxxMorsel& morsel, const WriteOptions& options,
                    std::vector<uint8_t>* out) {
    // The one-row-group case IS FileWriter, not a parallel implementation of it.
    FileWriter writer;
    SKENE_RETURN_IF_ERROR(writer.begin(options, out));
    SKENE_RETURN_IF_ERROR(writer.add_row_group(morsel));
    return writer.finish();
}

}  // namespace skene
