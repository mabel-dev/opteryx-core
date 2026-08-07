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
    Status st = read_morsel(bytes.data(), bytes.size(), ReadOptions(), &out);
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
    CHECK(!read_morsel(empty.data(), empty.size(), ReadOptions(), &out).is_ok());

    std::vector<uint8_t> tiny(kMinFileBytes - 1, 0);
    CHECK(!read_morsel(tiny.data(), tiny.size(), ReadOptions(), &out).is_ok());
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
        Status st = read_morsel(prefix.data(), prefix.size(), ReadOptions(), &out);
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
    const size_t tail_at = bytes.size() - kFileTailBytes;
    FileTail tail;
    std::memcpy(&tail, bytes.data() + tail_at, sizeof(tail));
    const size_t footer_at = tail_at - tail.footer_bytes;

    // Flip a byte inside the section directory — the worst place, since every
    // offset a reader is about to follow lives there.
    std::vector<uint8_t> corrupt = bytes;
    corrupt[footer_at + tail.footer_bytes - 8] ^= 0xFF;
    Status st = expect_rejected(corrupt, "corrupt footer");
    CHECK(st.code() == Code::kChecksumMismatch);
    check_message_mentions(st, "footer checksum", "corrupt footer");
}

static void test_corrupt_section_body_is_caught() {
    auto bytes = good_file();
    // Byte 40 is inside the first column's data, well past the head.
    std::vector<uint8_t> corrupt = bytes;
    corrupt[kFileHeadBytes + 4] ^= 0xFF;

    Status st = expect_rejected(corrupt, "corrupt section body");
    CHECK(st.code() == Code::kChecksumMismatch);
    // The message must say WHICH column, or an operator holding a 200-column
    // file learns nothing actionable.
    check_message_mentions(st, "column", "corrupt section body");
}

static void test_corrupt_bit_anywhere_is_always_caught() {
    // Sweep: flipping the top bit of every byte in the file must be rejected.
    // Any accepted flip is a hole in the checksum coverage — a region of the
    // file nothing verifies.
    const auto bytes = good_file();
    size_t accepted = 0;
    for (size_t i = 0; i < bytes.size(); ++i) {
        std::vector<uint8_t> corrupt = bytes;
        corrupt[i] ^= 0x80;
        CxxMorsel out;
        if (read_morsel(corrupt.data(), corrupt.size(), ReadOptions(), &out).is_ok())
            ++accepted;
    }
    ++skene_test::g_checks;
    if (accepted != 0) {
        skene_test::report(__FILE__, __LINE__, "unverified bytes in the file",
                           std::to_string(accepted) + " of " +
                           std::to_string(bytes.size()) +
                           " single-bit corruptions were accepted");
    }
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

    // Find the selection section: it is the last section of the only column.
    const size_t tail_at = bytes.size() - kFileTailBytes;
    FileTail tail;
    std::memcpy(&tail, bytes.data() + tail_at, sizeof(tail));
    const size_t footer_at = tail_at - tail.footer_bytes;

    FooterFileHeader fh;
    std::memcpy(&fh, bytes.data() + footer_at, sizeof(fh));
    const size_t sections_at =
        tail_at - static_cast<size_t>(fh.section_count) * sizeof(SectionEntry);

    for (uint32_t i = 0; i < fh.section_count; ++i) {
        SectionEntry entry;
        std::memcpy(&entry, bytes.data() + sections_at + i * sizeof(SectionEntry),
                    sizeof(entry));
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

// Recomputes every section checksum and the footer checksum, so a deliberately
// altered file is byte-consistent and only structural validation can reject it.
static void repair_checksums(std::vector<uint8_t>* bytes) {
    extern uint64_t skene_test_checksum(const void*, size_t);
    const size_t tail_at = bytes->size() - kFileTailBytes;
    FileTail tail;
    std::memcpy(&tail, bytes->data() + tail_at, sizeof(tail));
    const size_t footer_at = tail_at - tail.footer_bytes;

    FooterFileHeader fh;
    std::memcpy(&fh, bytes->data() + footer_at, sizeof(fh));
    const size_t sections_at =
        tail_at - static_cast<size_t>(fh.section_count) * sizeof(SectionEntry);

    for (uint32_t i = 0; i < fh.section_count; ++i) {
        const size_t at = sections_at + i * sizeof(SectionEntry);
        SectionEntry entry;
        std::memcpy(&entry, bytes->data() + at, sizeof(entry));
        entry.checksum = skene_test_checksum(bytes->data() + entry.offset,
                                             entry.stored_bytes);
        std::memcpy(bytes->data() + at, &entry, sizeof(entry));
    }

    tail.footer_checksum =
        skene_test_checksum(bytes->data() + footer_at, tail.footer_bytes);
    std::memcpy(bytes->data() + tail_at, &tail, sizeof(tail));
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
    return skene_test::summary("test_reader_rejects");
}

uint64_t skene_test_checksum(const void* data, size_t bytes) {
    return skene::checksum_xxh3_64(data, bytes);
}
