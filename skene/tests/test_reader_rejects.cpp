// Every way a .skene file can be wrong, and the reader refusing it.
//
// This format memcpys buffers and rebuilds absolute pointers from stored
// offsets, so a bad file read "as best we can" is memory corruption, not a wrong
// answer. There is no partial read and no recovery path — these tests exist to
// keep it that way.

#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "footer_probe.h"
#include "harness.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/checksum.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

static std::vector<uint8_t> good_file() {
    auto m = morsel_of({
        {"n", dense_column<int64_t>({1, 2, 3, 4}, DRAKEN_INT64, {true, false, true, true})},
        {"s", string_column({"alpha", "a longer value past twelve bytes", "g", "d"})},
        {"d", dict_column<int64_t>({10, 20}, {0, 1, 1, 0}, DRAKEN_INT64)},
    });
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, WriteOptions::for_spill(), &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  fixture write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// Reads must fail. Returns the status so a caller can check the code/message.
static Status expect_rejected(const std::vector<uint8_t>& bytes, const char* what) {
    CxxMorsel out;
    Status st = read_morsel(bytes.data(), bytes.size(), 0, ReadOptions(), &out);
    ++skene_test::g_checks;
    if (st.is_ok()) {
        skene_test::report(__FILE__, __LINE__, what,
                           "read SUCCEEDED on a file that should have been rejected");
    }
    return st;
}

static void check_message_mentions(const Status& st, const char* needle,
                                   const char* context) {
    ++skene_test::g_checks;
    if (st.message().find(needle) == std::string::npos) {
        skene_test::report(__FILE__, __LINE__, context,
                           "message did not mention '" + std::string(needle) +
                           "': " + st.message());
    }
}

// ─── Not a .skene file at all ───────────────────────────────────────────────

static void test_rejects_foreign_and_empty() {
    std::vector<uint8_t> parquet = {'P','A','R','1'};
    parquet.resize(200, 0);
    parquet[196] = 'P'; parquet[197] = 'A'; parquet[198] = 'R'; parquet[199] = '1';
    Status st = expect_rejected(parquet, "a Parquet file");
    CHECK(st.code() == Code::kNotSkene);

    // Parquet and .skene live in the same buckets under the same manifests, so
    // being handed the wrong one is the most likely mistake in production.
    std::vector<uint8_t> empty;
    CxxMorsel out;
    CHECK(!read_morsel(empty.data(), empty.size(), 0, ReadOptions(), &out).is_ok());

    std::vector<uint8_t> tiny(kMinFileBytes - 1, 0);
    CHECK(!read_morsel(tiny.data(), tiny.size(), 0, ReadOptions(), &out).is_ok());
}

// ─── Version ────────────────────────────────────────────────────────────────

static void test_version_mismatch_names_both_versions_and_the_way_out() {
    auto bytes = good_file();

    // A file from far in the past — outside this build's two-version window.
    std::vector<uint8_t> old_file = bytes;
    const uint16_t ancient = 1u;
    const size_t tail_at = old_file.size() - kFileTailBytes;
    std::memcpy(old_file.data() + 4, &ancient, sizeof(ancient));
    std::memcpy(old_file.data() + tail_at + offsetof(FileTail, version),
                &ancient, sizeof(ancient));

    // At v1 the "ancient" file IS current, so this only asserts once there is a
    // window to fall outside of. Kept unconditional in shape so it starts
    // asserting the moment v2 lands rather than being remembered.
    if (!version_is_supported(ancient)) {
        Status st = expect_rejected(old_file, "a file older than the read window");
        CHECK(st.code() == Code::kUnsupportedVersion);
        check_message_mentions(st, "migrat", "old-version message");
    }

    // A file from the future. No retained OLDER binary can help, so the advice
    // must be "upgrade", not "migrate" — sending an operator down a chain that
    // does not exist wastes the outage.
    std::vector<uint8_t> future = bytes;
    const uint16_t ahead = static_cast<uint16_t>(kVersion + 3u);
    std::memcpy(future.data() + 4, &ahead, sizeof(ahead));
    std::memcpy(future.data() + tail_at + offsetof(FileTail, version),
                &ahead, sizeof(ahead));

    Status st = expect_rejected(future, "a file newer than this build");
    CHECK(st.code() == Code::kUnsupportedVersion);
    check_message_mentions(st, "NEWER", "future-version message");
    check_message_mentions(st, "upgrade", "future-version message");
}

static void test_head_and_tail_must_agree() {
    auto bytes = good_file();
    // Only the head's version is changed. A reader that trusted the tail alone
    // (the cheap remote path) would sail past this, which is exactly why both
    // carry the field.
    const uint16_t wrong = static_cast<uint16_t>(kVersion + 1u);
    std::memcpy(bytes.data() + 4, &wrong, sizeof(wrong));

    Status st = expect_rejected(bytes, "head/tail version disagreement");
    check_message_mentions(st, "disagree", "head/tail mismatch");
}

static void test_rejects_foreign_endianness_and_checksum() {
    auto bytes = good_file();
    const size_t tail_at = bytes.size() - kFileTailBytes;

    std::vector<uint8_t> big_endian = bytes;
    big_endian[6] = static_cast<uint8_t>(Endianness::kBig);
    big_endian[tail_at + offsetof(FileTail, endianness)] =
        static_cast<uint8_t>(Endianness::kBig);
    Status st = expect_rejected(big_endian, "a big-endian file");
    CHECK(st.code() == Code::kWrongEndianness);

    std::vector<uint8_t> other_hash = bytes;
    other_hash[7] = 99;
    other_hash[tail_at + offsetof(FileTail, checksum_algorithm)] = 99;
    st = expect_rejected(other_hash, "an unknown checksum algorithm");
    CHECK(st.code() == Code::kUnknownChecksum);
}

// ─── Truncation ─────────────────────────────────────────────────────────────

static void test_rejects_truncation_at_every_length() {
    const auto bytes = good_file();

    // A truncated object must NEVER be read as a shorter valid one. Walk every
    // prefix: an object-storage read that returns short is not exotic.
    for (size_t n = 0; n < bytes.size(); ++n) {
        std::vector<uint8_t> prefix(bytes.begin(), bytes.begin() + n);
        CxxMorsel out;
        Status st = read_morsel(prefix.data(), prefix.size(), 0, ReadOptions(), &out);
        if (st.is_ok()) {
            skene_test::report(__FILE__, __LINE__, "truncated prefix accepted",
                               "length " + std::to_string(n) + " of " +
                               std::to_string(bytes.size()));
        }
    }
    ++skene_test::g_checks;  // one check for the whole sweep

    // And a file with extra bytes appended: the tail is no longer at the end, so
    // the magic is not where it must be.
    std::vector<uint8_t> extended = bytes;
    extended.push_back(0);
    expect_rejected(extended, "trailing garbage");
}

// ─── Corruption ─────────────────────────────────────────────────────────────

static void test_corrupt_footer_is_caught_before_any_offset_is_followed() {
    auto bytes = good_file();

    // The FOOTER: every summary offset a reader is about to follow lives there.
    {
        size_t footer_at = 0, footer_len = 0;
        CHECK(skene_test::file_footer_extent(bytes, &footer_at, &footer_len));
        std::vector<uint8_t> corrupt = bytes;
        corrupt[footer_at + footer_len / 2] ^= 0xFF;
        Status st = expect_rejected(corrupt, "corrupt footer");
        CHECK(st.code() == Code::kChecksumMismatch);
        check_message_mentions(st, "footer checksum", "corrupt footer");
    }

    // A DIRECTORY BLOCK's section entries — the worst place, since every section
    // offset the decode follows lives there. Caught against the checksum the
    // footer recorded for it.
    {
        skene_test::DirectoryBlock dir;
        CHECK(skene_test::directory_block(bytes, 0, &dir));
        std::vector<uint8_t> corrupt = bytes;
        corrupt[dir.section_at(0) + 8] ^= 0xFF;
        Status st = expect_rejected(corrupt, "corrupt directory block");
        CHECK(st.code() == Code::kChecksumMismatch);
        check_message_mentions(st, "directory block checksum", "corrupt directory block");
    }
}

static void test_corrupt_section_body_is_caught() {
    auto bytes = good_file();
    // v3: the first section body follows column 0's directory block, at the
    // offset the directory records.
    skene_test::DirectoryBlock dir;
    CHECK(skene_test::directory_block(bytes, 0, &dir));
    std::vector<uint8_t> corrupt = bytes;
    corrupt[static_cast<size_t>(dir.sections[0].offset) + 4] ^= 0xFF;

    Status st = expect_rejected(corrupt, "corrupt section body");
    CHECK(st.code() == Code::kChecksumMismatch);
    // The message must say WHICH column, or an operator holding a 200-column
    // file learns nothing actionable.
    check_message_mentions(st, "column", "corrupt section body");
}

// Marks every byte some checksum covers: the head (verified structurally, and
// any flip there changes magic/version/reserved — all checked), the footer, the
// tail, every column node's directory block, and every section's stored bytes.
// What is left is alignment padding — zero bytes belonging to no section and no
// directory block.
static std::vector<bool> covered_map(const std::vector<uint8_t>& bytes) {
    std::vector<bool> covered(bytes.size(), false);
    auto mark = [&](uint64_t at, uint64_t n) {
        for (uint64_t i = at; i < at + n && i < covered.size(); ++i)
            covered[i] = true;
    };
    mark(0, kFileHeadBytes);
    mark(bytes.size() - kFileTailBytes, kFileTailBytes);

    size_t footer_at = 0, footer_len = 0;
    skene_test::file_footer_extent(bytes, &footer_at, &footer_len);
    mark(footer_at, footer_len);

    size_t summaries = 0;
    uint32_t nodes = 0;
    skene_test::summaries_at(bytes, &summaries, &nodes);
    for (uint32_t n = 0; n < nodes; ++n) {
        ColumnSummaryHead head;
        skene_test::DirectoryBlock dir;
        skene_test::column_summary(bytes, n, &head);
        skene_test::directory_block(bytes, n, &dir);
        mark(head.directory_offset, head.directory_bytes);
        for (const SectionEntry& entry : dir.sections) mark(entry.offset, entry.stored_bytes);
    }
    return covered;
}

static void test_corrupt_bit_anywhere_is_always_caught() {
    // Sweep: flip the top bit of every byte. A flip in a COVERED byte must be
    // rejected — an accepted one is a hole in the checksum coverage. A flip in
    // alignment PADDING must be inert: the read still succeeds, because those
    // zero bytes belong to no section and nothing derives anything from them.
    // Both directions are pinned — a padding flip that suddenly REJECTED would
    // mean the reader had started depending on bytes outside every checksum.
    const auto bytes = good_file();
    const auto covered = covered_map(bytes);

    size_t padding_bytes = 0;
    for (bool c : covered) if (!c) ++padding_bytes;

    size_t accepted_covered = 0;
    size_t rejected_padding = 0;
    for (size_t i = 0; i < bytes.size(); ++i) {
        std::vector<uint8_t> corrupt = bytes;
        corrupt[i] ^= 0x80;
        CxxMorsel out;
        const bool ok =
            read_morsel(corrupt.data(), corrupt.size(), 0, ReadOptions(), &out).is_ok();
        if (covered[i] && ok) ++accepted_covered;
        if (!covered[i] && !ok) ++rejected_padding;
    }
    ++skene_test::g_checks;
    if (accepted_covered != 0)
        skene_test::report(__FILE__, __LINE__, "unverified content bytes",
                           std::to_string(accepted_covered) + " of " +
                           std::to_string(bytes.size() - padding_bytes) +
                           " covered-byte corruptions were accepted");
    ++skene_test::g_checks;
    if (rejected_padding != 0)
        skene_test::report(__FILE__, __LINE__,
                           "padding bytes are load-bearing",
                           std::to_string(rejected_padding) + " of " +
                           std::to_string(padding_bytes) +
                           " padding-byte corruptions changed the read");
}

// ─── Structural lies that pass every checksum ───────────────────────────────

// A checksum proves the bytes are the bytes that were written. It does NOT prove
// the writer was sane. These rewrite a field AND repair the checksums, so the
// file is internally "valid" and only the structural checks can catch it.
static void repair_checksums(std::vector<uint8_t>* bytes);

static void test_structurally_impossible_files_are_rejected() {
    // A selection code pointing past the dictionary. Passes every checksum;
    // would become an out-of-bounds read on the very first data[selection[i]].
    //
    // Three distinct values means codes pack at 2 bits, so a corrupted body can
    // decode to 3 — a perfectly well-formed code that is nonetheless out of
    // range for a 3-entry dictionary. That gap between "decodable" and "valid"
    // is exactly what the reader's bounds check exists to close.
    auto m = morsel_of({{"d", dict_column<int64_t>({10, 20, 30}, {0, 1, 2, 1},
                                                   DRAKEN_INT64)}});
    std::vector<uint8_t> bytes;
    CHECK(write_morsel(m, WriteOptions::for_spill(), &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());

    // Find the selection section in the only column's directory block.
    skene_test::DirectoryBlock dir;
    CHECK(skene_test::directory_block(bytes, 0, &dir));

    for (const SectionEntry& entry : dir.sections) {
        if (entry.kind != static_cast<uint16_t>(SectionKind::kSelection)) continue;

        // Corrupt the packed BODY, leaving the header intact, so the section
        // still decodes cleanly and only the bounds check can reject it.
        std::vector<uint8_t> corrupt = bytes;
        for (uint64_t at = entry.offset + sizeof(BitpackHeader);
             at < entry.offset + entry.stored_bytes; ++at)
            corrupt[at] = 0xFF;
        repair_checksums(&corrupt);

        Status st = expect_rejected(corrupt, "selection code past the dictionary");
        check_message_mentions(st, "out of range", "out-of-range selection code");
        break;
    }
}

// A column's section slice — [section_index, section_index + section_count) —
// indexes the section directory. A slice that runs off the end of the directory
// makes the reader index a std::vector past its size, which is an out-of-bounds
// read of whatever the allocator put after it, not a wrong answer. Found by the
// native fuzzer: build_column walked the slice through check_kinds, which was
// the one accessor that did not bound it.
//
// Both files below repair their checksums, so nothing but the structural check
// stands between them and the buffer building.
static void test_section_slice_must_lie_inside_the_directory() {
    auto bytes = good_file();
    skene_test::DirectoryBlock dir;
    CHECK(skene_test::directory_block(bytes, 0, &dir));
    const size_t chunk_at = dir.chunk_at(0);
    const ChunkRecord chunk = dir.chunks[0];
    const uint32_t section_count = dir.header.section_count;

    auto with_chunk = [&](ChunkRecord bad) {
        std::vector<uint8_t> corrupt = bytes;
        std::memcpy(corrupt.data() + chunk_at, &bad, sizeof(bad));
        repair_checksums(&corrupt);
        return corrupt;
    };

    // A count that runs off the end of the directory block's section list.
    {
        ChunkRecord bad = chunk;
        bad.section_count = section_count + 1u;
        Status st = expect_rejected(with_chunk(bad), "section slice past the directory");
        CHECK(st.code() == Code::kMalformed);
        check_message_mentions(st, "names sections past", "over-long section slice");
    }

    // An index near UINT32_MAX: the bound must be computed wide, or the slice
    // end wraps and a nonsense slice reads as if it fit.
    {
        ChunkRecord bad = chunk;
        bad.section_index = 0xFFFFFFF0u;
        bad.section_count = 0x20u;
        Status st = expect_rejected(with_chunk(bad), "section slice that wraps uint32");
        CHECK(st.code() == Code::kMalformed);
        check_message_mentions(st, "names sections past", "wrapping section slice");
    }

    // The per-row-group metadata path attaches the same directory block, so it
    // meets the same check on the same untrusted bytes. read_metadata does NOT:
    // it parses the footer only and never sees a directory block, which is
    // exactly the property that makes it cheap.
    {
        ChunkRecord bad = chunk;
        bad.section_count = section_count + 1u;
        const auto corrupt = with_chunk(bad);
        FileMetadata footer_only;
        CHECK(read_metadata(corrupt.data(), corrupt.size(), &footer_only).is_ok());
        RowGroupMetadata meta;
        Status st = read_row_group_metadata(corrupt.data(), corrupt.size(), 0, &meta);
        ++skene_test::g_checks;
        if (st.is_ok())
            skene_test::report(__FILE__, __LINE__, "metadata on a bad slice",
                               "read_row_group_metadata SUCCEEDED on an "
                               "out-of-range section slice");
        check_message_mentions(st, "names sections past", "metadata section slice");
    }

    // The index slice is bounded the same way.
    {
        ChunkRecord bad = chunk;
        bad.index_section_count = section_count + 1u;
        Status st = expect_rejected(with_chunk(bad), "index slice past the directory");
        CHECK(st.code() == Code::kMalformed);
        check_message_mentions(st, "names sections past", "over-long index slice");
    }
}

// ─── Encodings ──────────────────────────────────────────────────────────────

// Text with real redundancy, so the writer's compression gate actually fires
// (kCompressMinBytes, and only when the result is smaller).
static std::vector<std::string> compressible_text(size_t rows) {
    static const char* fragments[] = {
        "carefully regular accounts sleep against the",
        "furiously bold requests wake quickly among the",
        "slyly ironic deposits nag blithely about the",
        "express packages haggle carefully around the",
    };
    std::vector<std::string> values;
    values.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
        values.push_back(std::string(fragments[i % 4]) + " " + std::to_string(i));
    return values;
}

static std::vector<uint8_t> compressed_file(SectionCodec codec, int level) {
    auto m = morsel_of({{"s", string_column(compressible_text(20000))}});
    WriteOptions options;
    // No read acceleration, for the same reason every fixture in this file goes
    // without it: statistics blobs follow the section directory in the footer
    // (FORMAT.md §5), and walking back from the tail by section_count entries
    // only lands on the directory when there are none. Compression does not
    // depend on acceleration either way.
    options.read_acceleration = false;
    options.codec = codec;
    options.zstd_level = level;
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, options, &bytes);
    if (!st.is_ok()) {
        std::fprintf(stderr, "  fixture write failed: %s\n", st.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

// Runs `mutate` over the first section carrying `codec`, repairs every
// checksum, and requires the read to fail. Repairing the checksums is the whole
// point: it strips away the integrity layer so the CODEC is the only thing left
// standing between a crafted body and the buffers the reader builds from it.
// (v2: the codec is its own SectionEntry field, no longer an Encoding value.)
static void each_section_with_codec(
        const std::vector<uint8_t>& bytes, SectionCodec codec, const char* what,
        void (*mutate)(std::vector<uint8_t>*, SectionEntry*, size_t)) {
    skene_test::DirectoryBlock dir;
    CHECK(skene_test::directory_block(bytes, 0, &dir));

    bool found = false;
    for (uint32_t i = 0; i < dir.header.section_count; ++i) {
        const size_t at = dir.section_at(i);
        SectionEntry entry = dir.sections[i];
        if (entry.codec != static_cast<uint8_t>(codec)) continue;
        found = true;

        std::vector<uint8_t> corrupt = bytes;
        mutate(&corrupt, &entry, at);
        std::memcpy(corrupt.data() + at, &entry, sizeof(entry));
        repair_checksums(&corrupt);
        expect_rejected(corrupt, what);
        break;
    }
    ++skene_test::g_checks;
    if (!found)
        skene_test::report(__FILE__, __LINE__, what,
                           "no section used the encoding under test — the "
                           "fixture compressed nothing, so nothing was checked");
}

// An encoding this build does not implement is FATAL on a required section: the
// column cannot be decoded, and guessing at a body would be memory corruption
// rather than a wrong answer. This is the guard that makes adding an encoding
// (kLz4 was added after kZstd) safe to do at all — an older reader meeting a
// newer file must say so rather than proceed.
static void test_unknown_encoding_is_fatal() {
    auto bytes = good_file();
    skene_test::DirectoryBlock dir;
    CHECK(skene_test::directory_block(bytes, 0, &dir));

    for (uint32_t i = 0; i < dir.header.section_count; ++i) {
        SectionEntry entry = dir.sections[i];
        if (entry.kind != static_cast<uint16_t>(SectionKind::kData)) continue;

        std::vector<uint8_t> corrupt = bytes;
        // 4 is kLz4, so 250 is well past anything this format will assign for
        // a long time — a value from a future version, not a typo. (The field
        // is one byte: a wider literal would wrap to some other value.)
        entry.encoding = 250u;
        std::memcpy(corrupt.data() + dir.section_at(i), &entry, sizeof(entry));
        repair_checksums(&corrupt);

        Status st = expect_rejected(corrupt, "an encoding from the future");
        CHECK(st.code() == Code::kUnsupportedEncoding);
        check_message_mentions(st, "does not implement", "unknown encoding");
        return;
    }
    skene_test::report(__FILE__, __LINE__, "unknown encoding",
                       "fixture had no data section to retag");
}

// LZ4's block format carries no length of its own, so the directory's
// `plain_bytes` is what bounds the decode. Everything below attacks that pair
// with the checksums repaired, which is the only way to reach the decoder.
static void test_lz4_sections_reject_malformed_bodies() {
    const auto bytes = compressed_file(SectionCodec::kLz4, 0);

    // A truncated body. LZ4_decompress_safe must refuse rather than decode as
    // far as it can and leave the rest of the destination buffer as it found it.
    each_section_with_codec(
        bytes, SectionCodec::kLz4, "a truncated lz4 body",
        [](std::vector<uint8_t>* file, SectionEntry* entry, size_t) {
            (void)file;
            entry->stored_bytes /= 2u;
        });

    // A body whose declared decoded size is larger than it really decodes to.
    // Accepting this would hand back a buffer whose tail was never written.
    // v2: the codec's decode capacity is encoded_bytes; plain_bytes moves with
    // it so the kPlain encoded==plain invariant cannot reject the file before
    // the codec ever runs — the CODEC must be what stands.
    each_section_with_codec(
        bytes, SectionCodec::kLz4, "an lz4 body that decodes short",
        [](std::vector<uint8_t>* file, SectionEntry* entry, size_t) {
            (void)file;
            entry->encoded_bytes += 4096u;
            entry->plain_bytes   += 4096u;
        });

    // A declared decoded size past the codec's int-sized ceiling belongs in
    // test_encoding, against lz4_decode directly, NOT here: a section declaring
    // one is never reached through this path, because the reader materializes a
    // string section on its declared plain_bytes BEFORE checking that size
    // against the column's shape, so an absurd value is a terminal allocation
    // rather than a rejection. That is a pre-existing reader hazard independent
    // of any codec (see the note raised with the architect) — pinning it here
    // would only be pinning the wrong layer.

    // Random bytes where a block should be. LZ4 has no header to reject this
    // on, so only the safe decoder's own bounds checking stands here.
    each_section_with_codec(
        bytes, SectionCodec::kLz4, "an lz4 body that is not a block at all",
        [](std::vector<uint8_t>* file, SectionEntry* entry, size_t) {
            uint64_t state = 0x9E3779B97F4A7C15ull;
            for (uint64_t at = entry->offset;
                 at < entry->offset + entry->stored_bytes; ++at) {
                state ^= state << 13; state ^= state >> 7; state ^= state << 17;
                (*file)[at] = static_cast<uint8_t>(state);
            }
        });
}

// Recomputes everything a structural rule is not under test for — each
// section's checksum, each column's block extents, each directory block's
// checksum in its summary, and the footer's checksum — so a deliberately
// altered file is byte-consistent and only the rule under test can reject it.
//
// Skipping any level would let a test pass for the wrong reason: the read would
// fail on a checksum (or a block extent that no longer matches its chunks)
// instead of on the rule it was written to exercise.
static void repair_checksums(std::vector<uint8_t>* bytes) {
    extern uint64_t skene_test_checksum(const void*, size_t);
    size_t summaries = 0;
    uint32_t nodes = 0;
    if (!skene_test::summaries_at(*bytes, &summaries, &nodes)) return;
    FileFooterHeader ffh;
    skene_test::file_footer_header(*bytes, &ffh);
    const uint32_t G = ffh.block_row_groups == 0 ? 1u : ffh.block_row_groups;

    for (uint32_t n = 0; n < nodes; ++n) {
        skene_test::DirectoryBlock dir;
        if (!skene_test::directory_block(*bytes, n, &dir)) return;
        for (uint32_t i = 0; i < dir.header.section_count; ++i) {
            SectionEntry entry = dir.sections[i];
            if (entry.offset + entry.stored_bytes > bytes->size()) continue;
            entry.checksum = skene_test_checksum(bytes->data() + entry.offset,
                                                 entry.stored_bytes);
            std::memcpy(bytes->data() + dir.section_at(i), &entry, sizeof(entry));
            dir.sections[i] = entry;
        }

        // Block extents, re-derived from the chunks exactly as the writer does.
        ColumnSummaryHead head;
        size_t head_at = 0;
        skene_test::column_summary(*bytes, n, &head, &head_at);
        std::vector<BlockExtent> blocks(head.block_count, BlockExtent{0, 0});
        for (uint32_t g = 0; g < dir.chunks.size(); ++g) {
            const ChunkRecord& c = dir.chunks[g];
            if (g / G >= blocks.size()) break;
            BlockExtent& block = blocks[g / G];
            for (uint32_t s = 0; s < c.section_count; ++s) {
                const uint64_t index = static_cast<uint64_t>(c.section_index) + s;
                if (index >= dir.sections.size()) break;
                const SectionEntry& e = dir.sections[index];
                if (block.bytes == 0) block.offset = e.offset;
                if (e.offset + e.stored_bytes > block.offset)
                    block.bytes = std::max<uint64_t>(block.bytes,
                                                     e.offset + e.stored_bytes - block.offset);
            }
        }
        for (uint32_t b = 0; b < blocks.size(); ++b)
            std::memcpy(bytes->data() + head_at + sizeof(ColumnSummaryHead)
                            + b * sizeof(BlockExtent),
                        &blocks[b], sizeof(BlockExtent));

        head.directory_checksum = skene_test_checksum(
            bytes->data() + head.directory_offset, head.directory_bytes);
        std::memcpy(bytes->data() + head_at, &head, sizeof(head));
    }
    skene_test::reseal_footer(bytes);
}

int main() {
    test_rejects_foreign_and_empty();
    test_version_mismatch_names_both_versions_and_the_way_out();
    test_head_and_tail_must_agree();
    test_rejects_foreign_endianness_and_checksum();
    test_rejects_truncation_at_every_length();
    test_corrupt_footer_is_caught_before_any_offset_is_followed();
    test_corrupt_section_body_is_caught();
    test_corrupt_bit_anywhere_is_always_caught();
    test_structurally_impossible_files_are_rejected();
    test_section_slice_must_lie_inside_the_directory();
    test_unknown_encoding_is_fatal();
    test_lz4_sections_reject_malformed_bodies();
    return skene_test::summary("test_reader_rejects");
}

uint64_t skene_test_checksum(const void* data, size_t bytes) {
    return skene::checksum_xxh3_64(data, bytes);
}
