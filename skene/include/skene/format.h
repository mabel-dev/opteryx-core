#pragma once
// skene/format.h — the .skene on-disk format. SINGLE SOURCE OF TRUTH.
//
// Design: opteryx-core/docs/SKENE_FILE_FORMAT_DESIGN.md
//
// A .skene file holds ONE OR MORE row groups of draken vectors, losslessly —
// including the things Parquet drops: the LogicalType descriptor (so an IPv4
// column reads back typed IPV4, not bare UINT32), DrakenVector.flags verbatim,
// and the dictionary selection RESTORED rather than re-derived.
//
// ─── Layout ──────────────────────────────────────────────────────────────────
//   HEAD             16 bytes, magic first
//   per row group, in order:
//     DATA region    per column, all its sections contiguous (one range GET/column)
//     INDEX region   optional sections; adjacent to the RG FOOTER, one GET takes both
//     RG FOOTER      row group header + column directory + section directory + stats
//   FILE FOOTER      file index: schema + row group directory + per-RG statistics
//   TAIL             24 bytes, fixed, magic last
//
// ─── Why a row group is not a file ───────────────────────────────────────────
// It used to be. That made a ClickBench mirror 396 objects against parquet's 99
// for the same data: ~0.1ms of fixed per-file cost locally (open+mmap, footer
// fetch) is ~40ms of a full scan before a byte of data is read, and on object
// storage each of those is a GET with tens of milliseconds of latency instead of
// a syscall. Packing row groups amortises that.
//
// Three properties make the packing pay rather than cost, and each is a
// constraint on everything downstream:
//
//  1. THE UNIT OF WORK IS (file, row group), NEVER file. A scan that claims
//     whole files coarsens its parallelism by the packing factor and starves.
//     Measured on 16M rows with the claim unit tied to row group size: flat from
//     64k to 256k rows per claim, then 2.4x at 1M and 5.8x at 4M.
//  2. EVERY ROW GROUP IS INDEPENDENTLY ADDRESSABLE. Each carries its own footer
//     at its own offset with its own checksum (RowGroupEntry), so a reader
//     fetches the small FILE FOOTER, prunes, and then reads only the surviving
//     row groups' directories — not all of them.
//  3. PER-ROW-GROUP STATISTICS LIVE IN THE FILE FOOTER. Catalog/manifest bounds
//     necessarily coarsen to the union across a file's row groups; what recovers
//     the lost pruning is the file footer's per-row-group per-column min/max,
//     which is reachable without touching a single row group footer.
//
// ─── Validation order is TOTAL and comes before interpretation ───────────────
//   magic -> version -> endianness -> declared lengths vs real object size
//         -> checksum -> only then read one byte of content.
// This format memcpys buffers and rebuilds absolute pointers. A wrong layout
// interpreted as a right one is memory corruption, not a wrong answer. There is
// no "best effort" read path and there is no fallback.

#include <cstddef>
#include <cstdint>

namespace skene {

// ─── Magic and version ──────────────────────────────────────────────────────

// "SKEN", little-endian. Present at BOTH ends: the head magic rejects an
// unrelated or front-truncated object immediately; the tail magic plus
// footer_len finds the footer in one range request with no linear parse.
inline constexpr uint32_t kMagic = 0x4E454B53u;

// Bumped ONLY by a required-section layout change. Adding an optional section
// kind, a statistic, or an encoding on an OPTIONAL section does not bump it
// (see kSectionOptionalBase). Adding an encoding to a REQUIRED section does.
inline constexpr uint16_t kVersion = 1u;

// Byte order of every multi-byte field and every memcpy'd buffer. A file
// outliving the fleet must make a big-endian reader FAIL LOUD, not read garbage.
enum class Endianness : uint8_t { kLittle = 0, kBig = 1 };

// Identifies the checksum over section bodies and the footer, so it can be
// replaced without a required-section version bump.
enum class ChecksumAlgorithm : uint8_t { kXxh3_64 = 0 };

// ─── Fixed-size head and tail ───────────────────────────────────────────────

#pragma pack(push, 1)

struct FileHead {
    uint32_t magic;              // kMagic
    uint16_t version;            // kVersion
    uint8_t  endianness;         // Endianness
    uint8_t  checksum_algorithm; // ChecksumAlgorithm
    uint64_t reserved;           // 0
};

struct FileTail {
    uint32_t footer_bytes;       // length of the FOOTER region
    uint64_t footer_checksum;    // over exactly footer_bytes of footer
    uint16_t version;            // must equal FileHead::version
    uint8_t  endianness;         // must equal FileHead::endianness
    uint8_t  checksum_algorithm; // must equal FileHead::checksum_algorithm
    uint32_t reserved;           // 0
    uint32_t magic;              // kMagic — LAST four bytes of the file
};

#pragma pack(pop)

inline constexpr size_t kFileHeadBytes = 16u;
inline constexpr size_t kFileTailBytes = 24u;

static_assert(sizeof(FileHead) == kFileHeadBytes, "FileHead layout drift");
static_assert(sizeof(FileTail) == kFileTailBytes, "FileTail layout drift");

// ─── File footer: the file index ────────────────────────────────────────────

// First four bytes of the FILE FOOTER. This is not decoration and not a second
// format check — it is what makes the multi-row-group change fail LOUD against
// the single-row-group files v1 wrote before it.
//
// Those files are framed identically (same head, same tail, same version, a
// footer whose checksum verifies), so framing alone cannot tell them apart: the
// tail points at what used to be a lone row group footer and is now expected to
// be a file index. Parsing one as the other is not a wrong answer, it is a
// FooterFileHeader::row_count read as a magic and a writer tag read as a row
// group directory. The magic stops that at the first field, with a message that
// names the change and says to regenerate.
//
// v1 was still DRAFT and not frozen when this landed (FORMAT.md §1), so the
// layout changed without a version bump — which is exactly why the guard has to
// be in the bytes rather than in the version.
inline constexpr uint32_t kFileFooterMagic = 0x494E4B53u;  // "SKNI"

// Versions the FILE FOOTER's own layout, independently of kVersion. Both are
// 1 today; they are separate fields because the file index and the row group
// layout are separately extensible.
inline constexpr uint16_t kFileFooterVersion = 1u;

#pragma pack(push, 1)

// First record of the FILE FOOTER.
struct FileFooterHeader {
    uint32_t footer_magic;       // kFileFooterMagic
    uint16_t footer_version;     // kFileFooterVersion
    uint16_t reserved;           // 0
    uint64_t row_count;          // TOTAL logical rows, summed over row groups
    uint32_t row_group_count;    // >= 1
    uint32_t column_count;       // top-level schema columns; children nested
    uint8_t  file_uuid[16];      // all-zero means unset
    uint64_t created_at_unix_us; // provenance only, NEVER load-bearing
    uint32_t writer_tag_bytes;   // followed by writer_tag_bytes; provenance only
    uint32_t file_flags;         // 0; reserved
};

// One entry of the row group directory. Everything needed to read a row group
// without parsing any other one: where its bytes are, where its footer is, and
// whether that footer is intact.
//
// footer_checksum is here rather than beside the footer because the file footer
// is the only thing a ranged reader has fetched at the point it decides which
// row group footers to request — a checksum stored next to the bytes it covers
// would have to be fetched with them, and could not be validated against
// anything the reader already trusts.
struct RowGroupEntry {
    uint64_t row_count;       // logical rows in this row group
    uint64_t first_row;       // this row group's first row, in file row order
    uint64_t data_offset;     // absolute; start of its DATA region
    uint64_t data_bytes;      // its DATA + INDEX regions, up to its footer
    uint64_t footer_offset;   // absolute; start of its own footer
    uint64_t footer_checksum; // over exactly footer_bytes at footer_offset
    uint32_t footer_bytes;
    uint32_t reserved;        // 0
};

#pragma pack(pop)

static_assert(sizeof(FileFooterHeader) == 56u, "FileFooterHeader layout drift");
static_assert(sizeof(RowGroupEntry) == 56u, "RowGroupEntry layout drift");

// Smallest possible well-formed file: head + tail + a file footer that at least
// holds its own header. A file with no row groups at all is rejected separately;
// this bound only makes the framing arithmetic safe.
inline constexpr size_t kMinFileBytes =
    kFileHeadBytes + sizeof(FileFooterHeader) + kFileTailBytes;

// ─── Sections ───────────────────────────────────────────────────────────────

// Kind < kSectionOptionalBase  => REQUIRED. An unrecognised one is a hard error:
//   the column cannot be reconstructed without it.
// Kind >= kSectionOptionalBase => OPTIONAL. An unrecognised one is SKIPPED.
//
// What makes skipping safe, and the rule every future section must satisfy:
// an optional section MUST be provably reconstructible from the required
// sections, so ignoring it can only cost speed, never correctness. Anything
// carrying information not otherwise present is a required section, and adding
// one bumps kVersion. There is no third category, and "optional" is never a
// route for sneaking data past an older reader.
inline constexpr uint16_t kSectionOptionalBase = 256u;

enum class SectionKind : uint16_t {
    // Required.
    kData         = 1,  // fixed: data_length * itemsize. BOOL: bit-packed
                        // (data_length+7)/8. ARRAY: (length+1) * int32 offsets.
                        // DRAKEN_NULL: empty.
    kSelection    = 2,  // length uint32 codes; present iff selection_kind==kStored
    kValidity     = 3,  // (length+7)/8 bytes; absent => all rows valid
    kStringSlots  = 4,  // slot_count * 16 bytes, verbatim
    kStringArena  = 5,  // arena_used bytes, verbatim

    // Optional.
    kBloom        = 256,
    kPermutation  = 257,
    kZoneMap      = 258,
};

inline constexpr bool section_is_required(uint16_t kind) noexcept {
    return kind < kSectionOptionalBase;
}

// Encoding of a section body. PLAIN is always legal for every section; the
// others are size optimizations whose decoded output is byte-identical.
// NOTE: an unrecognised encoding on a REQUIRED section is fatal (the column
// cannot be decoded), so the encoding set for required sections is fixed at v1
// and adding one later is a version bump — unlike adding an optional section,
// which is free. All of these are therefore declared now; the writer may emit
// only a subset while v1 is still unreleased.
//
// There is deliberately no AFFINE encoding: identity and all-zero selections are
// expressed by SelectionKind, which stores NO section at all, so an affine
// encoding would have no remaining producer. A selection that is affine with any
// other base/stride is not something any draken op constructs.
//
// There is deliberately no bare DELTA: first-order differences stored at the
// source width are never smaller than the values themselves, so nothing would
// ever produce one. Delta only pays combined with bit packing, which is what
// kDeltaBitpack is.
//
// A GENERAL-PURPOSE CODEC is applied to SOME sections, never to the whole file.
// Compressing the file as a unit would be smaller still, but it would destroy
// the one property the layout exists for: a reader cannot decompress a slice, so
// reading one column by range request would become reading everything.
// Per-section keeps every extent independently fetchable and independently
// decodable.
//
// Two codecs, because they answer different questions and the writer picks per
// POSTURE, not per section. Measured on a ClickBench row group (154.7MB of
// section bytes, 256KB blocks, Apple Silicon, dev/skene_codec_bench.cpp):
//
//   zstd-9   7.34x ratio, 3477 MB/s decode, 219 MB/s encode
//   lz4      4.49x ratio, 8931 MB/s decode, 1822 MB/s encode
//
// zstd's decode rate is essentially LEVEL-INDEPENDENT (3284/3043/3477 MB/s at
// levels 1/3/9), so a low zstd level buys nothing on the read side and costs
// ratio — there is no reason to write one. LZ4 decodes at roughly the rate the
// reader's own uncompressed path runs at (a full-width raw decode of that same
// file measured ~8840 MB/s), so it is close to free on read for ~70% of zstd's
// ratio.
enum class Encoding : uint16_t {
    kPlain        = 0,  // verbatim; stored_bytes == plain_bytes
    kBitpack      = 1,  // uint32 array at a fixed bit width (BitpackHeader)
    kDeltaBitpack = 2,  // ascending 4/8-byte integer array: first value verbatim,
                        // then first-order differences bit-packed
                        // (DeltaBitpackHeader)
    kZstd         = 3,  // zstd frame; plain_bytes is the decoded size
    kLz4          = 4,  // LZ4 BLOCK (not frame); plain_bytes is the decoded size.
                        // The block format carries no length of its own, which is
                        // exactly why the directory's plain_bytes is load-bearing
                        // here: LZ4_decompress_safe is given that size as its
                        // capacity and must produce exactly it.
};

// Below this, a section is stored plain without attempting compression.
//
// Measured on TPC-H: sections under 10 KB are 87% of all sections but hold ~1.2%
// of the recoverable bytes, so the attempt costs far more than it returns. The
// bytes given up are mostly all-valid validity bitmaps, which compress ~99% but
// are tiny to begin with.
inline constexpr uint64_t kCompressMinBytes = 10240u;

// Whether a section kind is worth attempting compression on. Measured on TPC-H
// (BENCHMARKS.md), not assumed:
//
//   STRING_ARENA   0.25x — text keeps nearly all its redundancy after the other
//                          encodings, and is the bulk of a real table
//   STRING_SLOTS   0.43x — 16-byte slots share prefixes and near-sequential
//                          arena offsets
//   VALIDITY       0.00x — an all-valid bitmap is a run of ones
//   DATA                 — candidate: a PLAIN data body is one that bit packing
//                          and delta both declined, which does not by itself
//                          mean incompressible (float columns, notably, are
//                          untested here)
//
// Excluded, each for a reason rather than by omission:
//   SELECTION      a PLAIN selection is one bit packing declined, which happens
//                  when the code space is near-full — high entropy by definition
//   BLOOM          hash bits; measured at 1.00x, incompressible by construction
//   ZONE_MAP       measured 0.49-0.70x but only kilobytes per file
//   PERMUTATION    row ordinals, near-random by nature
inline constexpr bool kind_is_compressible(uint16_t kind) noexcept {
    switch (static_cast<SectionKind>(kind)) {
        case SectionKind::kData:
        case SectionKind::kValidity:
        case SectionKind::kStringSlots:
        case SectionKind::kStringArena:
            return true;
        default:
            return false;
    }
}

#pragma pack(push, 1)

// One entry of the section directory. Absolute offsets, so a section is a
// range request with no further arithmetic.
struct SectionEntry {
    uint16_t kind;           // SectionKind
    uint16_t encoding;       // Encoding
    uint64_t offset;         // absolute, from file start
    uint64_t stored_bytes;   // on disk, post-encoding
    uint64_t plain_bytes;    // after decoding; == stored_bytes when kPlain
    uint64_t checksum;       // over the STORED bytes, not the decoded ones
};

#pragma pack(pop)

static_assert(sizeof(SectionEntry) == 36u, "SectionEntry layout drift");

// ─── Selection ──────────────────────────────────────────────────────────────

// How a column's selection is reconstructed. This is a WRITTEN FACT, not a
// property derived by inspecting the vector on read — which is the whole point.
// The reader reads one byte; it never classifies a shape. A selection_kind that
// contradicts data_length (kConstant with data_length != 1, kIdentity with
// data_length != length) is a DETECTABLE CORRUPTION, checked and rejected,
// never a silent reshape.
enum class SelectionKind : uint8_t {
    kConstant = 0,  // no kSelection section; reader attaches draken_zero_sel
    kIdentity = 1,  // no kSelection section; reader attaches draken_identity_sel
    kStored   = 2,  // kSelection section present; reader owns the decoded codes
};

// ─── Value ordering ─────────────────────────────────────────────────────────

// Whether the column's `data` array was sorted ascending and deduplicated at
// write time (design §7). Optional and per column: ON for result/stored files,
// OFF for spill, which needs no read acceleration.
//
// When kAscending:
//   - data[0] and data[data_length-1] ARE the min and max, exactly.
//   - data_length IS the exact distinct count, not an estimate.
//   - a predicate resolves to a contiguous code interval by binary search.
//
// Dedup keys on the BIT PATTERN, never on engine equality: under draken's float
// order -0.0 == 0.0, so an equality-based dedup would collapse them and a column
// containing -0.0 would read back as 0.0. That is silent corruption on a round
// trip. The two values sort adjacently and both survive.
//
// Types with no defined order are never value-ordered: DRAKEN_VARIANT (no
// collation), DRAKEN_ARRAY (no whole-array comparison), DRAKEN_VECTOR_FP16.
enum class ValueOrder : uint8_t {
    kAsWritten = 0,
    kAscending = 1,
};

// ─── Statistics ─────────────────────────────────────────────────────────────

// Absent means "NOT TRACKED", never "zero" — draken's cardinal statistics rule
// (draken/docs/design/05_statistics.md). Spill files carry stats_bytes == 0
// everywhere by construction.
enum StatFlag : uint32_t {
    kStatMin       = 1u << 0,  // ordinalize() ordinal, int64, over NON-NULL values
    kStatMax       = 1u << 1,
    kStatNullCount = 1u << 2,
    kStatSum       = 1u << 3,  // int128; exact types only, NEVER floats
    kStatRowSorted = 1u << 4,  // mirrors DRAKEN_ROW_SORTED
    kStatRowSortedDescending = 1u << 5,
};

#pragma pack(push, 1)

// Per-column statistics blob. Only the fields whose StatFlag is set are
// meaningful; the rest are zero and MUST NOT be read.
//
// min/max are ordinalize() ordinals (draken/ops/ordinalize.h), the same dialect
// the catalog manifest speaks, so a predicate literal's ordinal compares
// directly against these bounds at plan time. Two properties that are
// load-bearing:
//   - MONOTONIC BUT NOT INJECTIVE. String ordinals pack the first 8 content
//     bytes big-endian and collide on a shared prefix. Pruning is therefore
//     CONSERVATIVE: a file may be read unnecessarily, never skipped wrongly.
//   - ORDINAL_NULL == INT64_MIN sorts nulls first, so the writer must compute
//     min/max over non-null values only, or every nullable column's min becomes
//     INT64_MIN and prunes nothing.
//
// No min/max for DRAKEN_DECIMAL128 (ordinalize.h deliberately has no entry — it
// throws rather than return a lossy int64 proxy), nor for VARIANT / ARRAY /
// VECTOR_FP16, which have no order at all.
struct ColumnStatistics {
    uint32_t flags;       // StatFlag bitmask
    uint32_t reserved;
    int64_t  min_ordinal;
    int64_t  max_ordinal;
    uint64_t null_count;
    int64_t  sum_low;     // int128 accumulator, little-endian halves
    int64_t  sum_high;
};

#pragma pack(pop)

static_assert(sizeof(ColumnStatistics) == 48u, "ColumnStatistics layout drift");

// A signed 128-bit sum cannot overflow at any row count this format can
// address: the worst case is INT64_MIN summed 2^32 times, |2^63 * 2^32| == 2^95,
// far inside 2^127. So there is no overflow flag and none is needed.
static_assert(sizeof(uint32_t) == 4u && sizeof(uint64_t) == 8u, "fixed-width drift");

// ─── Zone map ───────────────────────────────────────────────────────────────

// Intra-column skipping (design §10.2b). One row group means a predicate would
// otherwise read a whole column or none of it. With value ordering a predicate
// resolves to a CODE interval, so per-chunk min/max CODES are enough to skip
// row chunks — and the reader fetches only the surviving byte ranges of the
// selection section. Parquet's page index, without pages.
//
// 8 bytes per chunk: ~1 KB for a million rows.
inline constexpr uint32_t kZoneMapDefaultChunkRows = 8192u;

#pragma pack(push, 1)

struct ZoneMapHeader {
    uint32_t chunk_rows;
    uint32_t chunk_count;
    // Followed by chunk_count * { uint32 min_code; uint32 max_code; }
};

// Value bounds for one chunk of rows, as ordinals from draken's ordinalize
// kernels — the SAME dialect the footer's min/max and the catalog manifest use.
//
// Ordinals, not codes. Codes only carry order when a column is BOTH value-ordered
// AND dictionary-encoded, which meant the sharpest pruning case there is — a
// sorted, unique key column — produced no index at all, because it has an
// identity selection and therefore no codes to bound. Ordinals are defined for
// every column of an orderable type regardless of encoding shape or ordering.
//
// An all-null chunk is written as an EMPTY range (min > max), which correctly
// answers "cannot contain" for any probe. Nulls are excluded from the bounds of
// a mixed chunk: a null never satisfies a comparison, so leaving it out narrows
// the range without ever excluding a row that matches.
struct ZoneMapEntry {
    int64_t min_ordinal;
    int64_t max_ordinal;
};

// Body header of a kBitpack section, followed by ceil(count*bit_width/8) bytes
// of LSB-first packed values.
struct BitpackHeader {
    uint32_t count;      // number of values
    uint8_t  bit_width;  // 0..32; 0 means every value is zero
    uint8_t  pad[3];
};

// Body header of a kDeltaBitpack section, followed by `item_bytes` holding the
// first value verbatim, then ceil((count-1)*bit_width/8) bytes of packed
// differences.
//
// Differences are computed in UNSIGNED arithmetic and wrap deliberately: for an
// ascending signed array, the wrapping unsigned difference is the true magnitude
// of the step regardless of sign (e.g. -5 -> 3 gives 8), and adding it back
// unsigned reconstructs the value exactly. Doing this in signed arithmetic would
// overflow whenever the array spans more than half the type's range.
struct DeltaBitpackHeader {
    uint32_t count;       // number of values
    uint8_t  item_bytes;  // 4 or 8
    uint8_t  bit_width;   // 0..64; 0 means every difference is zero
    uint16_t pad;
};

#pragma pack(pop)

static_assert(sizeof(ZoneMapHeader) == 8u, "ZoneMapHeader layout drift");
static_assert(sizeof(ZoneMapEntry) == 16u, "ZoneMapEntry layout drift");
static_assert(sizeof(BitpackHeader) == 8u, "BitpackHeader layout drift");
static_assert(sizeof(DeltaBitpackHeader) == 8u, "DeltaBitpackHeader layout drift");

// ─── Sort specification (permutation sections) ──────────────────────────────

#pragma pack(push, 1)

// One key of a stored sort order. nulls_first MUST follow draken's single sort
// null-ordering rule (NULLS FIRST ascending, LAST descending — see
// draken/morsels/sort.hpp). A permutation written under a different rule is a
// different order, silently.
struct SortKey {
    uint32_t column_ordinal;
    uint8_t  descending;
    uint8_t  nulls_first;
    uint16_t reserved;
};

struct PermutationHeader {
    uint16_t key_count;
    uint16_t reserved;
    uint32_t length;  // == file row_count
    // Followed by key_count * SortKey, then length * uint32 row ordinals.
};

#pragma pack(pop)

static_assert(sizeof(SortKey) == 8u, "SortKey layout drift");
static_assert(sizeof(PermutationHeader) == 8u, "PermutationHeader layout drift");

// ─── Logical type descriptor ────────────────────────────────────────────────

// The reason this format exists. draken's LogicalType is a borrowed pointer into
// a process-global interned registry and must never be written as one; this is
// its POD projection, re-interned via logical_type_intern() on read.
//
// Mandatory for the parameterized physical types (TIMESTAMP64, TIME32/64,
// DECIMAL, VECTOR_FP16) — a timestamp vector with no descriptor is a hard error,
// not a degraded one. Optional for IPV4, which REFINES an otherwise complete
// UINT32: dropping it degrades an IPv4 column to a well-formed unsigned integer
// column, which is a display and cast regression, never a wrong answer. Carrying
// it is precisely what Parquet cannot do.
#pragma pack(push, 1)

struct LogicalTypeDescriptor {
    uint8_t  kind;            // draken LogicalKind
    uint8_t  unit;            // draken TimestampUnit
    int16_t  offset_minutes;
    uint8_t  precision;
    uint8_t  scale;
    uint16_t reserved;
    uint32_t dimension;
};

#pragma pack(pop)

static_assert(sizeof(LogicalTypeDescriptor) == 12u, "LogicalTypeDescriptor layout drift");

// ─── Column directory ───────────────────────────────────────────────────────

// Fixed-size head of a column directory entry, PER ROW GROUP: one of these per
// column per row group, in that row group's own footer. Its identity/type half
// necessarily repeats the file's SchemaEntryHead; everything else (length,
// data_length, selection_kind, value_order, the section slices, the string arena
// counts) describes this row group only and exists nowhere else.
//
// The variable-size parts (name bytes, the optional LogicalTypeDescriptor, and
// child entries for ARRAY) follow in the footer stream. Every variable-length
// field carries an explicit length and is bounds-checked against the footer
// extent before it is read.
#pragma pack(push, 1)

struct ColumnEntryHead {
    uint32_t field_id;        // stable identity across schema evolution. The
                              // catalog assigns these; the format only
                              // guarantees the slot exists and round-trips.
                              // Matching columns by NAME breaks on rename —
                              // the lesson Parquet and Iceberg both learned late.
    uint32_t name_bytes;      // followed by name_bytes of column identity
    uint32_t type;            // DrakenType, verbatim
    uint8_t  vector_flags;    // DrakenVector.flags, VERBATIM — layout hints must
                              // survive; re-deriving them is what disqualified
                              // Parquet.
    uint8_t  logical_present; // 0/1 — a LogicalTypeDescriptor follows the name
    uint8_t  selection_kind;  // SelectionKind
    uint8_t  value_order;     // ValueOrder
    uint32_t length;          // logical row count
    uint32_t data_length;     // physical value count
    uint32_t child_count;     // 0 except DRAKEN_ARRAY
    uint32_t section_index;   // first REQUIRED-section entry in the directory
    uint32_t section_count;   // how many belong to this column
    uint32_t stats_bytes;     // 0 == no statistics tracked for this column
    uint64_t string_slot_count;  // string family only; 0 otherwise
    uint64_t string_arena_used;  // string family only
    uint64_t string_arena_cap;   // string family only
    uint8_t  string_payloads_elided;  // string family only — see below
    uint8_t  pad[3];
    // Optional sections live in their OWN directory slice, because they live in
    // their own REGION: every column's required sections sit in the data region
    // and its optional ones in the index region next to the footer. A pruning
    // reader therefore fetches the footer and every filter and index in ONE
    // range request, and only then decides which column extents to read. One
    // slice for both would force the two to interleave, and an index scattered
    // through the data region is an index you have to read the data to reach.
    uint32_t index_section_index;
    uint32_t index_section_count;
    uint32_t reserved;
};

#pragma pack(pop)

static_assert(sizeof(ColumnEntryHead) == 80u, "ColumnEntryHead layout drift");

// string_payloads_elided is not a hint and not cosmetic. A length-only column
// has a NULL arena and every long slot stamped STR_ELIDED_PAYLOAD_OFFSET
// (0xFFFFFFFF) as a TRAP value. Losing the flag turns that trap into a 4 GB
// out-of-bounds read. Across a process boundary and a week of object storage,
// writing it correctly is not enough — the reader VERIFIES it:
//
//   elided == 1 => arena_used == 0, no kStringArena section, and EVERY long slot
//                  carries exactly STR_ELIDED_PAYLOAD_OFFSET.
//   elided == 0 => EVERY long slot satisfies arena_offset + length <= arena_used.
//
// Either violation is a hard error. One linear pass over the slots, cheap
// against the arena memcpy it guards.

// ─── Schema directory (FILE FOOTER) ─────────────────────────────────────────

// The part of a column that CANNOT vary between row groups: its identity and its
// type. Everything else about a column — length, data_length, selection kind,
// value order, section extents, string arena counts — is a property of one row
// group and lives in that row group's ColumnEntryHead.
//
// It exists so the FILE FOOTER alone answers "what columns does this file have,
// and what types are they" and gives the per-row-group statistics block a
// defined column order. Without it a reader would have to open a row group
// footer to learn the schema, which is the one thing the file index is for.
//
// The writer PROVES the invariant rather than assuming it: a row group whose
// columns differ from the first one's in name, type, logical descriptor or
// nesting is rejected, because a schema directory that does not describe every
// row group is a lie a reader has no way to detect.
#pragma pack(push, 1)

struct SchemaEntryHead {
    uint32_t field_id;
    uint32_t name_bytes;      // followed by name_bytes of column identity
    uint32_t type;            // DrakenType, verbatim
    uint8_t  logical_present; // 0/1 — a LogicalTypeDescriptor follows the name
    uint8_t  reserved0;       // 0
    uint16_t reserved1;       // 0
    uint32_t child_count;     // 0 except DRAKEN_ARRAY, which has 1
    // Followed by name_bytes, the optional LogicalTypeDescriptor, then children
    // depth first — the same shape and the same order as the column directory.
};

#pragma pack(pop)

static_assert(sizeof(SchemaEntryHead) == 20u, "SchemaEntryHead layout drift");

// ─── Row group header (first record of a ROW GROUP footer) ──────────────────

// Named for what it heads: one row group, not the file. The file-level
// equivalents (total row count, lineage, provenance) live in FileFooterHeader
// and are NOT repeated per row group — the two fields that look duplicated
// (file_uuid, created_at_unix_us) are carried here as well so that a row group
// footer extracted on its own still names the file it came from.
#pragma pack(push, 1)

struct RowGroupFooterHeader {
    uint64_t row_count;         // logical rows in THIS row group
    uint32_t column_count;      // top-level columns; ARRAY children are nested
    uint32_t section_count;
    uint8_t  file_uuid[16];     // lineage and manifest dedup
    uint64_t created_at_unix_us;// provenance only, NEVER load-bearing
    uint32_t writer_tag_bytes;  // followed by writer_tag_bytes; provenance only
    uint32_t file_flags;
};

#pragma pack(pop)

static_assert(sizeof(RowGroupFooterHeader) == 48u,
              "RowGroupFooterHeader layout drift");

// ─── Version support window ─────────────────────────────────────────────────

// A build reads exactly TWO versions: the one it writes, and the one before it.
//
// A .skene file may be long-term storage for a dataset that cannot be
// regenerated, so an unreadable version would be data loss. The answer is not an
// unbounded reader — it is MIGRATION: a migrate entry point reads an N-1 file
// and writes it as N. A file more than one version behind is stepped forward by
// running successively newer RETAINED binaries, one version per hop. (Landing
// with the reader; it cannot exist before there is something to read with.)
//
// Two obligations fall out of that, and both are load-bearing:
//
//  1. The N-1 READER MUST BE RETAINED IN THE SOURCE, not just in a released
//     binary — migrate needs it to read its input. So reader code is versioned
//     from v1 onward (reader_v1.cpp, reader_v2.cpp, …) and dispatched on the
//     file's version. Deleting an old reader without first deleting its version
//     from the migrate chain breaks the chain silently.
//
//  2. THE VERSION MUST BE READABLE BY A BUILD THAT CANNOT READ THE FILE.
//     Otherwise an operator holding an N-3 file cannot tell which retained
//     binary to reach for. probe_version() (probe.h) therefore parses magic and
//     version ONLY, succeeds for every version past present and future, and is
//     frozen forever — the head layout's first 8 bytes can never change.
//
// A version outside [kMinReadVersion, kVersion] fails loud, naming BOTH the
// file's version and the reader's, telling the operator to migrate, and
// interpreting nothing.
inline constexpr uint16_t kMinReadVersion =
    (kVersion > 1u) ? static_cast<uint16_t>(kVersion - 1u) : 1u;

inline constexpr bool version_is_supported(uint16_t v) noexcept {
    return v >= kMinReadVersion && v <= kVersion;
}

// True when `v` is old but migratable by THIS build: exactly kVersion - 1.
inline constexpr bool version_is_migratable(uint16_t v) noexcept {
    return kVersion > 1u && v == static_cast<uint16_t>(kVersion - 1u);
}

}  // namespace skene
