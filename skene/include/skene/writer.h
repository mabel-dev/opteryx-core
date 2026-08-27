#pragma once
// skene/writer.h — serialize a CxxMorsel to a .skene buffer.
//
// The writer only ever emits kVersion. There is deliberately no "write as an
// older version" mode: that would put two writers in one binary and make it
// ambiguous what a file at a given version contains, which is precisely what
// the one-hop migration chain depends on NOT being true.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "skene/format.h"   // SectionCodec, SortKey — on-disk enums live there
#include "skene/status.h"

// draken — imported, never copied (opteryx-core .claude/CLAUDE.md §14).
#include "morsels/cxx_morsel.h"

namespace skene {

// SectionCodec moved to format.h with the v2 bump — it is written into every
// SectionEntry now, so it is an on-disk enum, and on-disk enums have one home.
// The measurements that pick between the codecs live with the enum.

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

    // Per-SECTION general-purpose compression. `codec` is the switch; `kNone`
    // means every section is stored plain.
    //
    // Only some section kinds are attempted (kind_is_compressible) and only
    // above kCompressMinBytes — see format.h for the measurements behind both.
    // A section that does not come out SMALLER is stored plain regardless of
    // codec, so selecting one is a request to try, never a guarantee it is used.
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
    // Off by default: kNone is the neutral baseline; every caller with a read
    // or capacity posture sets a codec deliberately (for_storage, for_fast_reads,
    // and the engine's spill buffer, which uses zstd-1 for disk-ceiling headroom).
    SectionCodec codec = SectionCodec::kNone;

    // The zstd level, and ONLY meaningful when `codec == kZstd`. The two fields
    // are checked against each other rather than left to disagree quietly: a
    // level set alongside a non-zstd codec is a caller who thinks compression is
    // configured one way while the writer does something else, so write_morsel
    // rejects it instead of picking a winner (see writer.cpp).
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

    // v2: declares that the file's rows are GLOBALLY ordered by these keys, in
    // file row order across every row group. Empty means unclustered, which is
    // the only honest default.
    //
    // The declaration is VERIFIED, not trusted: add_row_group checks every
    // adjacent row pair (and finish() has already checked the seam between row
    // groups as they arrived) and the write FAILS on the first out-of-order
    // pair. A cluster spec is a promise consumers may act on — zone maps become
    // tight, merge readers may skip sorting — and a false one is silent wrong
    // answers in every one of them. Verification is one comparison per row per
    // key on the write side, which is exactly where this format spends.
    //
    // SortKey.column_ordinal indexes the TOP-LEVEL schema, and nulls_first MUST
    // follow draken's single sort rule (NULLS FIRST ascending, LAST descending)
    // — any other combination is rejected. Key columns must be orderable,
    // non-elided types.
    std::vector<SortKey> cluster_keys;

    // There is deliberately NO alignment switch. The v2 acceptance A/B
    // (2026-08-20, interleaved aligned-vs-packed ClickBench mirrors) measured
    // the two indistinguishable on read time (12353-12401ms vs 12165-12600ms,
    // overlapping ranges) and alignment at 0.06% of the file — so aligned won
    // on the zero-copy option it preserves, and the temporary A/B switch was
    // removed rather than left as a knob nothing should ever set.

    // Spill: written once, read once, in-process. The READ-ACCELERATION bundle
    // is off — nothing that trades write time for read time can pay on a file
    // read exactly once. The CODEC is a separate axis: the engine's spill
    // buffer (opteryx pipeline_buffers.hpp) layers zstd-1 on top of this
    // posture, because spill's binding constraint is the ~9GB disk ceiling and
    // ratio converts directly to queries that survive (architect, 2026-08-27;
    // bakeoff: zstd-1 0.30x in 58ms vs lz4 0.43x in 44ms, decode level-flat).
    // This function stays codec-free as the acceleration-off BASELINE posture
    // (fixtures and tests build on it); it is not a statement that spill wants
    // raw bytes.
    static WriteOptions for_spill() { return WriteOptions(); }

    // Stored data: read many times, kept for a long time, so read acceleration
    // and compression both pay. Without compression skene is 1.9-3.8x larger
    // than the equivalent ZSTD Parquet on TPC-H; with it, 0.92-1.09x.
    //
    // zstd at level 7, not 1 and not 9.
    //
    // Level 1 was ruled out first: zstd decodes at the same rate whatever level
    // produced the bytes (2882 MB/s at level 1, 3078 at level 9 — flat within
    // noise), so a low level gives up ratio and buys nothing on the read side.
    //
    // 9 was the answer while the comparison was only 1-vs-9. Sweeping the whole
    // operational band (dev/codec_matrix_bench.cpp, 2026-08-11) shows the ratio
    // curve is NOT monotonic in level, so a level has to be judged on its WORST
    // shape rather than its average. Ranked against the best ratio available per
    // shape, the worst case of each level is:
    //
    //     L1  0.1%   (int64 1..10: 11.83x where 10296x was available)
    //     L3  0.2%   (str8 compressible: 18.19x vs 10217x)
    //     L4  0.2%   (same cliff)
    //     L7  86%    (no catastrophic shape)
    //     L9  47%    (str8 sequential: 14.29x vs 30.70x)
    //
    // 7 is the only level in the band with no cliff, and it costs about HALF
    // L9's compress time. L9 is dominated on both axes — worse worst case and
    // slower to write. Ratios are bit-identical on ARM and x86, so this ranking
    // is architecture-independent.
    //
    // ⛔ L5/L6 are UNMEASURED. The L4→L7 cliff is zstd's dfast→lazy strategy
    // change, and greedy at 5-6 may capture most of it more cheaply.
    static WriteOptions for_storage() {
        WriteOptions options;
        options.read_acceleration = true;
        options.codec = SectionCodec::kZstd;
        options.zstd_level = 7;
        return options;
    }

    // Read-first data: the same acceleration, LZ4 instead of zstd.
    //
    // Where `for_storage()` trades read latency for bytes, this trades bytes for
    // read latency — 4.49x against 7.34x, but decoding at 8414 MB/s against
    // 3078, which is close to the rate the reader's uncompressed path runs at on
    // the same file. For a working set read far more often than it is written.
    //
    // ⭐ THIS IS ALSO THE LOCAL BENCHMARK POSTURE, and the reason there is no
    // separate "performance" mode. Measured on the TPC-H SF10 mirror, three
    // interleaved rounds, minimum of each (2026-08-11):
    //
    //     none      5823.8ms   7.8 GiB
    //     lz4       6041.3ms   4.0 GiB     <- this posture
    //     zstd-7    7153.0ms   2.7 GiB
    //
    // LZ4 and uncompressed OVERLAP across runs (6041-6232 against 5824-6250), so
    // on a local read they are not distinguishable — but LZ4 is half the size.
    // Uncompressed buys a further 3.7% for double the bytes, which is not a
    // posture, it is a rounding error with a name. The marginal rates say the
    // same thing: none->lz4 removes 18.8 MB per millisecond spent, lz4->zstd-7
    // removes 1.25. LZ4 is the knee.
    //
    // ⛔ Which posture a corpus uses is a statement about where it is READ, not
    // about what it contains. Deployed data is remote and takes for_storage();
    // a local benchmark mirror takes this one. See the Makefile's
    // clickbench-skene / tpch-skene targets, where that split is deliberate and
    // the parquet corpora do NOT follow it.
    static WriteOptions for_fast_reads() {
        WriteOptions options;
        options.read_acceleration = true;
        options.codec = SectionCodec::kLz4;
        return options;
    }
};

// ─── Writing a file ─────────────────────────────────────────────────────────

// A .skene file holds one or more row groups. This is the only writer; the
// single-row-group case is `write_morsel` below, which is a two-line wrapper
// rather than a second implementation, so the two cannot drift.
//
// STREAMING BY ROW GROUP, deliberately. The alternative — take a vector of
// morsels and write them all — would hold every row group resident at once,
// which for a wide schema at the 16-row-group default is the better part of a
// gigabyte of input on top of the output buffer. Here a caller decodes a row
// group, hands it over, and drops it; only the output buffer and the accumulated
// METADATA (a row group directory entry plus one statistics blob per column per
// row group — kilobytes) grow with the row group count.
//
// EVERY ROW GROUP MUST SHARE ONE SCHEMA. The file footer's schema directory
// describes the file, not a row group, so a row group whose columns differ in
// name, type, logical descriptor or nesting is REJECTED by add_row_group rather
// than written into a file whose index does not describe it. This is checked,
// not documented: a reader has no way to detect the lie.
//
// Usage is strictly begin -> add_row_group* -> finish. Calling out of order, or
// finishing with no row groups, is an error and says so.
class FileWriter {
  public:
    FileWriter();
    ~FileWriter();

    FileWriter(const FileWriter&) = delete;
    FileWriter& operator=(const FileWriter&) = delete;

    // Validates `options` and writes the file head into `out` (replacing its
    // contents). `out` must outlive the writer.
    Status begin(const WriteOptions& options, std::vector<uint8_t>* out);

    // Appends one row group. Fails loud rather than degrading, on: a type this
    // build cannot materialize, a parameterized physical type missing its
    // mandatory LogicalType descriptor, a selection code out of range, an
    // internally inconsistent string arena, or a schema that diverges from the
    // first row group's.
    Status add_row_group(const CxxMorsel& morsel);

    // Writes the file footer and the tail. After this the buffer is a complete
    // .skene file and the writer must not be reused.
    Status finish();

    uint32_t row_group_count() const;

  private:
    struct State;
    std::unique_ptr<State> state_;
};

// Serializes `morsel` into `out` (replacing its contents) as a file of exactly
// one row group — the degenerate case of FileWriter, and implemented as one.
Status write_morsel(const CxxMorsel& morsel, const WriteOptions& options,
                    std::vector<uint8_t>* out);

// ─── Scope of this implementation ───────────────────────────────────────────
//
// IMPLEMENTED: the complete v2 required-section layout — every family
// (fixed-width, BOOL, the string family including length-only columns, ARRAY
// with recursive children, DRAKEN_NULL), all three selection kinds, LogicalType
// round-trip, per-section and footer checksums, head/tail framing, every
// encoding plus the codec axis, slot lanes, value ordering, statistics
// (including NDV), zone maps, bloom filters, and the cluster spec.
//
// NOT YET IMPLEMENTED: permutations (kPermutation) — additive, an optional
// section, no format change required when it lands.

}  // namespace skene
