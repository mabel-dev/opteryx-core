#include "skene/writer.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <type_traits>

#include "skene/checksum.h"
#include "skene/format.h"
#include "bloom.h"
#include "encoding.h"
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
// Appends to a growable buffer while tracking absolute position, so a section's
// offset is recorded as it is written rather than predicted by a separate sizing
// pass that could drift from the writing pass.
//
// The whole file is assembled in memory. That is right for both current
// callers — job-result parts are size-bounded and spill blocks are chunked at
// the sink's threshold — and a streaming sink can replace this without touching
// the layout, since every offset is absolute and the footer is written last.
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

struct WriteContext {
    ByteWriter*                writer;
    std::vector<SectionEntry>* sections;
    int                        zstd_level = 0;
};

// The checksum covers the STORED bytes, so it is computed after encoding — a
// reader verifies what it is about to decode, not what the writer started from.
void emit_raw(WriteContext& ctx, SectionKind kind, Encoding encoding,
              const void* stored, size_t stored_bytes, size_t plain_bytes) {
    SectionEntry entry;
    entry.kind         = static_cast<uint16_t>(kind);
    entry.encoding     = static_cast<uint16_t>(encoding);
    entry.offset       = ctx.writer->position();
    entry.stored_bytes = stored_bytes;
    entry.plain_bytes  = plain_bytes;
    entry.checksum     = checksum_xxh3_64(stored, stored_bytes);
    ctx.writer->bytes(stored, stored_bytes);
    ctx.sections->push_back(entry);
}

// Compresses a section body when that is smaller, otherwise stores it as-is.
//
// Three gates, each measured rather than assumed (see format.h and
// BENCHMARKS.md):
//
//   encoding   only PLAIN bodies are candidates. A bit-packed or delta body has
//              already had its redundancy removed, so a general compressor over
//              it costs CPU for nothing.
//   kind       only kinds that measurably compress. Bloom bits are random by
//              construction and a PLAIN selection is one bit packing declined,
//              meaning high entropy — attempting either is pure cost.
//   size       only above kCompressMinBytes. Small sections are the large
//              MAJORITY by count and a rounding error by bytes.
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

void emit_encoded(WriteContext& ctx, SectionKind kind, Encoding encoding,
                  const void* stored, size_t stored_bytes, size_t plain_bytes) {
    if (ctx.zstd_level != 0 && encoding == Encoding::kPlain
            && stored_bytes >= kCompressMinBytes
            && kind_is_compressible(static_cast<uint16_t>(kind))) {
        std::vector<uint8_t> packed;
        if (zstd_encode(stored, stored_bytes, ctx.zstd_level, &packed)) {
            emit_raw(ctx, kind, Encoding::kZstd, packed.data(), packed.size(),
                     plain_bytes);
            return;
        }
    }
    emit_raw(ctx, kind, encoding, stored, stored_bytes, plain_bytes);
}

void emit_section(WriteContext& ctx, SectionKind kind, const void* data, size_t bytes) {
    emit_encoded(ctx, kind, Encoding::kPlain, data, bytes, bytes);
}

// Selection codes, bit-packed to the width data_length implies. This is where
// the bytes are: under value ordering every non-degenerate column stores one
// code per row, so a column with <= 256 distinct values drops from 4 bytes per
// row to 1, and <= 16 distinct to a half.
void emit_selection(WriteContext& ctx, const uint32_t* codes, uint32_t length,
                    uint32_t data_length) {
    const size_t plain = static_cast<size_t>(length) * sizeof(uint32_t);
    std::vector<uint8_t> packed;
    if (bitpack_encode_codes(codes, length, data_length, &packed))
        emit_encoded(ctx, SectionKind::kSelection, Encoding::kBitpack,
                     packed.data(), packed.size(), plain);
    else
        emit_section(ctx, SectionKind::kSelection, codes, plain);
}

// Fixed-width data. Delta+bitpack applies ONLY to a value-ordered column, where
// ascending order is established by construction — never assumed from a flag or
// from the data happening to look sorted.
void emit_fixed_data(WriteContext& ctx, const void* data, uint32_t data_length,
                     size_t itemsize, DrakenType type, bool ascending) {
    const size_t plain = static_cast<size_t>(data_length) * itemsize;
    if (ascending && type_supports_delta(type)) {
        std::vector<uint8_t> encoded;
        if (delta_bitpack_encode(data, data_length, itemsize, &encoded)) {
            emit_encoded(ctx, SectionKind::kData, Encoding::kDeltaBitpack,
                         encoded.data(), encoded.size(), plain);
            return;
        }
    }
    emit_section(ctx, SectionKind::kData, data, plain);
}

// Everything the footer needs to describe one column, gathered while its data
// sections are written. ARRAY children nest, so this is a tree.
// An optional section body, held back until every column's required sections
// have been written, so all of them land contiguously in the index region.
struct PendingIndex {
    SectionKind          kind;
    std::vector<uint8_t> body;
};

struct ColumnPlan {
    ColumnEntryHead           head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    ColumnStatistics          statistics{};
    bool                      has_statistics = false;
    std::vector<PendingIndex> index_sections;
    std::vector<ColumnPlan>   children;
};

// Per-chunk code bounds, for skipping byte ranges WITHIN a column.
//
// Only meaningful on a value-ordered column: there, `data` is ascending, so a
// predicate resolves to a contiguous CODE interval and a chunk whose codes miss
// that interval provably contains no matching row. Without ordering the codes
// carry no order at all and the bounds would be noise.
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


// `allow_value_order` is false for ARRAY children — see the call site.
Status write_column_data(WriteContext& ctx, const CxxColumn& column,
                         const std::string& name, uint32_t field_id,
                         const WriteOptions& options, bool allow_value_order,
                         ColumnPlan* plan);

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

    emit_section(ctx, SectionKind::kStringSlots, sa->slots,
                 static_cast<size_t>(sa->length) * sizeof(DrakenStringSlot));

    if (sa->arena_used > 0) {
        if (sa->arena == nullptr)
            return fail(Code::kMalformed,
                        "column '%s': arena_used is %llu but the arena is null",
                        name, static_cast<unsigned long long>(sa->arena_used));
        emit_section(ctx, SectionKind::kStringArena, sa->arena,
                     static_cast<size_t>(sa->arena_used));
    }
    return Status::ok();
}

Status write_column_data(WriteContext& ctx, const CxxColumn& column,
                         const std::string& name, uint32_t field_id,
                         const WriteOptions& options, bool allow_value_order,
                         ColumnPlan* plan) {
    const DrakenVector& v = column.view;
    const VectorOwner*  own = column.own.get();
    const char*         cname = name.c_str();

    plan->name = name;
    plan->head.field_id    = field_id;
    plan->head.name_bytes  = static_cast<uint32_t>(name.size());
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
        plan->head.stats_bytes = static_cast<uint32_t>(sizeof(ColumnStatistics));
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
            emit_section(ctx, SectionKind::kStringSlots, ordered.data.get(),
                         static_cast<size_t>(ordered.slot_count)
                             * sizeof(DrakenStringSlot));
            if (ordered.arena_used > 0)
                emit_section(ctx, SectionKind::kStringArena, ordered.arena.get(),
                             static_cast<size_t>(ordered.arena_used));
        } else {
            SKENE_RETURN_IF_ERROR(write_string_column(ctx, v, cname, plan));
        }
    } else if (v.type == DRAKEN_BOOL) {
        // BOOL is never value-ordered (bit-packed; codes would inflate it ~32x).
        emit_section(ctx, SectionKind::kData, v.data,
                     (static_cast<size_t>(v.data_length) + 7u) / 8u);
    } else if (v.type == DRAKEN_ARRAY) {
        // Offsets are sized by the LOGICAL row count, not data_length: arrays
        // are stored dense (draken_native.cpp D.13). Never value-ordered.
        emit_section(ctx, SectionKind::kData, v.data,
                     (static_cast<size_t>(v.length) + 1u) * sizeof(int32_t));
    } else if (v.type == DRAKEN_NULL) {
        // Self-describing: type == NULL means every row is null. No data, no
        // validity, nothing to write.
    } else {
        const size_t itemsize = draken_type_itemsize(v.type, lt);
        if (itemsize == 0)
            return fail(Code::kUnsupportedType,
                        "column '%s': no fixed item width for physical type %u",
                        cname, static_cast<unsigned>(v.type));
        emit_fixed_data(ctx, ordered.applied ? ordered.data.get() : v.data,
                        data_length, itemsize, v.type, ordered.applied);
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
        emit_section(ctx, SectionKind::kValidity, v.validity,
                     (static_cast<size_t>(v.length) + 7u) / 8u);

    // Identity and constant selections store NO section — the reader attaches
    // the shared global. Only genuinely owned codes are written.
    if (selection_kind == SelectionKind::kStored)
        emit_selection(ctx, codes, v.length, data_length);

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
    // insertions and data_length gives its sizing an exact count.
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
        SKENE_RETURN_IF_ERROR(write_column_data(ctx, child_column, name + ".element",
                                                field_id, options,
                                                /*allow_value_order=*/false,
                                                &plan->children.back()));
        plan->head.child_count = 1;
    }

    return Status::ok();
}

// ─── Footer ─────────────────────────────────────────────────────────────────

void write_column_entry(ByteWriter& w, const ColumnPlan& plan) {
    w.pod(plan.head);
    w.bytes(plan.name.data(), plan.name.size());
    if (plan.head.logical_present) w.pod(plan.logical);
    for (const ColumnPlan& child : plan.children) write_column_entry(w, child);
}

// Writes every column's optional sections, depth first, into one contiguous
// region immediately before the footer — so a pruning reader gets the footer and
// every index in a single range request.
void emit_index_sections(WriteContext& ctx, ColumnPlan* plan) {
    plan->head.index_section_index = static_cast<uint32_t>(ctx.sections->size());
    for (const PendingIndex& pending : plan->index_sections)
        emit_section(ctx, pending.kind, pending.body.data(), pending.body.size());
    plan->head.index_section_count =
        static_cast<uint32_t>(ctx.sections->size()) - plan->head.index_section_index;

    for (ColumnPlan& child : plan->children) emit_index_sections(ctx, &child);
}

void write_statistics(ByteWriter& w, const ColumnPlan& plan) {
    if (plan.has_statistics) w.pod(plan.statistics);
    for (const ColumnPlan& child : plan.children) write_statistics(w, child);
}

uint32_t count_sections(const ColumnPlan& plan) {
    uint32_t total = plan.head.section_count + plan.head.index_section_count;
    for (const ColumnPlan& child : plan.children) total += count_sections(child);
    return total;
}

}  // namespace

Status write_morsel(const CxxMorsel& morsel, const WriteOptions& options,
                    std::vector<uint8_t>* out) {
    if (out == nullptr)
        return fail(Code::kMalformed, "write_morsel: out is null");

    const size_t column_count = morsel.columns.size();
    if (morsel.names.size() != column_count)
        return fail(Code::kMalformed,
                    "write_morsel: %zu columns but %zu names",
                    column_count, morsel.names.size());
    if (!options.field_ids.empty() && options.field_ids.size() != column_count)
        return fail(Code::kMalformed,
                    "write_morsel: %zu field_ids for %zu columns — a partially "
                    "assigned schema is worse than an unassigned one",
                    options.field_ids.size(), column_count);

    out->clear();
    ByteWriter w(out);

    // ── HEAD ──
    FileHead head{};
    head.magic             = kMagic;
    head.version           = kVersion;
    head.endianness        = static_cast<uint8_t>(Endianness::kLittle);
    head.checksum_algorithm = static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64);
    head.reserved          = 0;
    w.pod(head);

    // ── DATA region ──
    std::vector<SectionEntry> sections;
    std::vector<ColumnPlan>   plans(column_count);
    WriteContext ctx{&w, &sections, options.zstd_level};

    for (size_t i = 0; i < column_count; ++i) {
        const uint32_t field_id = options.field_ids.empty() ? 0u : options.field_ids[i];
        SKENE_RETURN_IF_ERROR(write_column_data(ctx, morsel.columns[i],
                                                morsel.names[i], field_id,
                                                options, /*allow_value_order=*/true,
                                                &plans[i]));
    }

    // ── INDEX region ──
    for (ColumnPlan& plan : plans) emit_index_sections(ctx, &plan);

    // ── FOOTER ──
    const uint64_t footer_start = w.position();

    FooterFileHeader fh{};
    fh.row_count           = morsel.num_rows();
    fh.column_count        = static_cast<uint32_t>(column_count);
    fh.section_count       = static_cast<uint32_t>(sections.size());
    fh.created_at_unix_us  = options.created_at_unix_us;
    fh.writer_tag_bytes    = static_cast<uint32_t>(options.writer_tag.size());
    fh.file_flags          = 0;
    std::memcpy(fh.file_uuid, options.file_uuid, sizeof(fh.file_uuid));
    w.pod(fh);
    w.bytes(options.writer_tag.data(), options.writer_tag.size());

    for (const ColumnPlan& plan : plans) write_column_entry(w, plan);
    for (const SectionEntry& entry : sections) w.pod(entry);
    // Statistics blobs, in the SAME depth-first order as the column directory,
    // skipping columns with stats_bytes == 0. Located by order rather than by an
    // offset, so a future longer blob is read prefix-first and the remainder
    // skipped -- which is what lets statistics grow with no version bump.
    for (const ColumnPlan& plan : plans) write_statistics(w, plan);

    const uint64_t footer_bytes = w.position() - footer_start;
    if (footer_bytes > UINT32_MAX)
        return fail(Code::kMalformed, "footer is %llu bytes, which exceeds the "
                    "32-bit footer_bytes field",
                    static_cast<unsigned long long>(footer_bytes));

    // ── TAIL ──
    FileTail tail{};
    tail.footer_bytes      = static_cast<uint32_t>(footer_bytes);
    tail.footer_checksum   = checksum_xxh3_64(out->data() + footer_start,
                                              static_cast<size_t>(footer_bytes));
    tail.version           = kVersion;
    tail.endianness        = static_cast<uint8_t>(Endianness::kLittle);
    tail.checksum_algorithm = static_cast<uint8_t>(ChecksumAlgorithm::kXxh3_64);
    tail.reserved          = 0;
    tail.magic             = kMagic;
    w.pod(tail);

    // Sanity: the section count recorded in the footer must match what the
    // column tree actually claims, or a reader walking either path sees a
    // different file.
    uint32_t claimed = 0;
    for (const ColumnPlan& plan : plans) claimed += count_sections(plan);
    if (claimed != sections.size())
        return fail(Code::kMalformed,
                    "internal: column tree claims %u sections but %zu were "
                    "written", claimed, sections.size());

    return Status::ok();
}

}  // namespace skene
