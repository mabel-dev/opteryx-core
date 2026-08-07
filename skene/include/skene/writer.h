#pragma once
// skene/writer.h — serialize a CxxMorsel to a .skene buffer.
//
// The writer only ever emits kVersion. There is deliberately no "write as an
// older version" mode: that would put two writers in one binary and make it
// ambiguous what a file at a given version contains, which is precisely what
// the one-hop migration chain depends on NOT being true.

#include <cstdint>
#include <string>
#include <vector>

#include "skene/status.h"

// draken — imported, never copied (opteryx-core .claude/CLAUDE.md §14).
#include "morsels/cxx_morsel.h"

namespace skene {

struct WriteOptions {
    // Everything that exists to make a LATER READ faster, as one switch:
    //
    //   - value ordering — sort and deduplicate each column's `data`, with
    //     `selection` carrying the original row order, so a predicate becomes a
    //     binary search and `data_length` is the exact distinct count
    //   - statistics — min/max ordinals, null_count, exact 128-bit sum
    //   - zone maps — per-row-chunk code bounds, for skipping within a column
    //
    // They are one flag because they are one idea. Value ordering is itself a
    // statistic — its products ARE the exact NDV and the free min/max — so
    // "ordered but no statistics" describes nothing anyone wants, and a caller
    // that wants no statistics wants no ordering either.
    //
    // Not every column is eligible: types with no defined order, BOOL,
    // length-only string columns, and near-unique columns where ordering would
    // only add a permutation to a column that had none. An ineligible column is
    // written kAsWritten — the file never CLAIMS an ordering that was not
    // performed, because a consumer trusting that flag on unordered data returns
    // wrong answers, not slow ones.
    bool read_acceleration = false;

    // Per-SECTION zstd. 0 disables it; otherwise the zstd level.
    //
    // Level 1 is the right default and is what `for_storage()` uses. Measured
    // across 35 MB of TPC-H sections: zstd-1 reaches 0.30x in 58 ms, zstd-3
    // 0.28x in 66 ms, zstd-9 0.26x in 356 ms. lz4 is faster (44 ms) but only
    // reaches 0.43x, and snappy is beaten by zstd-1 on BOTH axes (0.41x, 66 ms),
    // so neither earns a second codec in the format.
    //
    // Only some section kinds are attempted (kind_is_compressible) and only
    // above kCompressMinBytes — see format.h for the measurements behind both.
    //
    // Applied per section rather than to the whole file, so every column extent
    // stays independently fetchable and decodable — whole-file compression would
    // be smaller but would make reading one column mean reading all of them.
    //
    // Measured on TPC-H (BENCHMARKS.md): bit packing and delta already extract
    // almost everything from numeric columns, but a string arena of near-unique
    // text does not, and comment-style columns dominate real tables. Off, skene
    // is ~3x larger than zstd Parquet there; on, roughly at parity.
    //
    // Off by default: spill wants raw bytes, and a section only pays for the
    // codec when the result is actually smaller.
    int zstd_level = 0;

    // Narrows which columns get a bloom filter. EMPTY MEANS ALL of them, under
    // read_acceleration — this is a restriction, not an opt-in.
    //
    // The default is every eligible column. A filter that answers an equality
    // probe from the footer turns a column read into no read at all, and 8k-chunk
    // pruning plus a bloom is the pair the read path is built on. Whether a
    // particular column is one anyone filters on is not the writer's call to
    // guess — guessing wrong costs a full column read, while carrying a filter
    // nobody probes costs bytes we can measure.
    std::vector<std::string> bloom_columns;
    // 5%, not Parquet's 1% — see kDefaultFalsePositiveRate in bloom.h for why the
    // economics differ. The rate is DELIVERED, not nominal: sizing is calibrated
    // against measured behaviour, so asking for 5% gets 5%.
    double bloom_false_positive_rate = 0.05;

    // Stable per-column identity across schema evolution. Matching columns by
    // NAME breaks on rename — the lesson Parquet and Iceberg both learned late.
    // Assignment is the catalog's business; the format only guarantees the slot
    // exists and round-trips. Empty means "unassigned" and writes 0.
    //
    // When non-empty it MUST have one entry per top-level column, or the write
    // fails: a partially-assigned schema is worse than an unassigned one.
    std::vector<uint32_t> field_ids;

    uint8_t     file_uuid[16] = {};   // all-zero == unset
    uint64_t    created_at_unix_us = 0;
    std::string writer_tag;           // provenance only, never load-bearing

    // Spill: written once, read once, in-process, wall-clock bound. Nothing
    // that trades write time for read time can pay, so the whole bundle is off.
    static WriteOptions for_spill() { return WriteOptions(); }

    // Stored data: read many times, kept for a long time, so read acceleration
    // and compression both pay. Without compression skene is 1.9-3.8x larger
    // than the equivalent ZSTD Parquet on TPC-H; with it, 0.92-1.09x.
    static WriteOptions for_storage() {
        WriteOptions options;
        options.read_acceleration = true;
        options.zstd_level = 1;
        return options;
    }
};

// Serializes `morsel` into `out` (replacing its contents).
//
// Fails loud rather than degrading, on: a type this build cannot materialize, a
// parameterized physical type missing its mandatory LogicalType descriptor, a
// selection code out of range, or an internally inconsistent string arena.
Status write_morsel(const CxxMorsel& morsel, const WriteOptions& options,
                    std::vector<uint8_t>* out);

// ─── Scope of this implementation ───────────────────────────────────────────
//
// IMPLEMENTED: the complete required-section layout — every family
// (fixed-width, BOOL, the string family including length-only columns, ARRAY
// with recursive children, DRAKEN_NULL), all three selection kinds, LogicalType
// round-trip, per-section and footer checksums, head/tail framing, both
// encodings, value ordering, statistics and zone maps.
//
// NOT YET IMPLEMENTED: bloom filters and permutations. Both are additive — new
// optional sections — so neither requires a format change. v1 is not frozen
// until they land.

}  // namespace skene
