// Is FSST worth carrying for the string arena?
//
// The arena is where skene's bytes are — 24 MB of the 35 MB measured across
// TPC-H, compressing 0.25x under zstd-1. zstd is the incumbent, so FSST has to
// beat it on something that matters, not merely compress.
//
// What FSST offers that zstd cannot: each string decompresses INDEPENDENTLY
// from a shared ~2 KB symbol table. A zstd frame must be decoded whole before
// any string in it can be read, so an arena stays compressed only on disk. An
// FSST arena could stay compressed in MEMORY and decode per value on access.
//
// This measures both sides honestly:
//   - size, INCLUDING the symbol table and the per-slot bookkeeping FSST needs
//     (a compressed length, since compressed strings are not the logical length)
//   - compress and decompress throughput against zstd-1 on the same bytes
//   - correctness: every string is decompressed and compared before any number
//     is reported, because a compressor that loses bytes has no size result
//
// It reads real .skene files through skene's own reader, so the string
// boundaries are the actual slot boundaries rather than a guess about them.
//
// NOT part of libskene.a. See bench/README.md.

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/reader.h"

#include "fsst.h"
#include "zstd.h"

using namespace skene;
using Clock = std::chrono::steady_clock;

namespace {

double ms_since(Clock::time_point started) {
    return std::chrono::duration<double, std::milli>(Clock::now() - started).count();
}

struct Tally {
    uint64_t columns = 0;
    uint64_t strings = 0;
    uint64_t plain = 0;
    uint64_t fsst = 0;          // compressed bytes only
    uint64_t symbol_tables = 0; // exported symbol table bytes
    uint64_t zstd = 0;
    uint64_t stacked = 0;       // fsst, then zstd-1 over the compressed bytes
    double   fsst_compress_ms = 0, fsst_decompress_ms = 0;
    double   bounded_decompress_ms = 0;   // decoding WITHOUT a stored extent
    double   zstd_compress_ms = 0, zstd_decompress_ms = 0;
};

// One column's arena, measured. Returns false only on a correctness failure —
// "FSST did not help here" is a number, not an error.
bool measure_column(const std::string& file, const std::string& name,
                    const DrakenVector& v, Tally* tally) {
    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(v.data);
    if (arena == nullptr || arena->payloads_elided) return true;

    // The batch is the SLOTS, not the rows: a dict or value-ordered column has
    // already deduplicated, and compressing rows would re-inflate that work.
    std::vector<const unsigned char*> pointers;
    std::vector<size_t> lengths;
    uint64_t plain = 0;
    for (uint32_t i = 0; i < v.data_length; ++i) {
        const DrakenStringSlot* slot = &arena->slots[i];
        if (str_is_inline(slot)) continue;   // inline bytes never reach the arena
        pointers.push_back(str_data(slot, arena->arena));
        lengths.push_back(str_length(slot));
        plain += str_length(slot);
    }
    if (pointers.empty() || plain == 0) return true;

    const size_t n = pointers.size();

    // ── FSST ──
    auto started = Clock::now();
    fsst_encoder_t* encoder = fsst_create(n, lengths.data(), pointers.data(), 0);

    unsigned char exported[FSST_MAXHEADER];
    const unsigned int symbol_table_bytes = fsst_export(encoder, exported);

    // "Conservative space" per the API contract: 7 + 2x input.
    std::vector<unsigned char> compressed(7 + 2 * plain + 16);
    std::vector<size_t> out_lengths(n);
    std::vector<unsigned char*> out_pointers(n);
    const size_t produced =
        fsst_compress(encoder, n, lengths.data(), pointers.data(),
                      compressed.size(), compressed.data(),
                      out_lengths.data(), out_pointers.data());
    const double compress_ms = ms_since(started);
    fsst_destroy(encoder);

    if (produced != n) {
        std::fprintf(stderr,
                     "%s: column '%s': fsst_compress took %zu of %zu strings — the "
                     "output buffer was sized by the API's own conservative bound, "
                     "so a short count is a real failure, not a tuning problem\n",
                     file.c_str(), name.c_str(), produced, n);
        return false;
    }

    uint64_t fsst_bytes = 0;
    for (size_t i = 0; i < n; ++i) fsst_bytes += out_lengths[i];

    // ── Correctness before size ──
    fsst_decoder_t decoder;
    if (fsst_import(&decoder, exported) == 0) {
        std::fprintf(stderr, "%s: column '%s': fsst_import rejected the table this "
                             "run just exported\n", file.c_str(), name.c_str());
        return false;
    }

    std::vector<unsigned char> scratch(65536);
    started = Clock::now();
    for (size_t i = 0; i < n; ++i) {
        if (scratch.size() < lengths[i]) scratch.resize(lengths[i]);
        // Decompressed INDEPENDENTLY, one string at a time, touching nothing but
        // this string's bytes and the shared table. That is the whole point.
        const size_t got = fsst_decompress(&decoder, out_lengths[i], out_pointers[i],
                                           scratch.size(), scratch.data());
        if (got != lengths[i] || std::memcmp(scratch.data(), pointers[i], got) != 0) {
            std::fprintf(stderr,
                         "%s: column '%s': string %zu did not survive the round trip "
                         "(%zu bytes in, %zu out)\n",
                         file.c_str(), name.c_str(), i, lengths[i], got);
            return false;
        }
    }
    const double decompress_ms = ms_since(started);

    // ── Decoding from (offset, logical length) alone ──
    //
    // The question this answers: can a slot as it stands TODAY read an FSST
    // arena? It carries the decoded length and an offset; it does NOT carry the
    // stored extent, and the two are unrelated.
    //
    // The only way to supply `lenIn` without storing it is an upper bound. FSST's
    // worst case is one escape per byte, so 2n+7 bounds it, clamped to the end of
    // the arena. The decoder clamps its WRITES to `size`, so the output stays
    // correct — but its final loop runs `while (posIn < lenIn)`, so it keeps
    // decoding to the end of whatever input it was handed and returns a posOut
    // past `size`. The bound is therefore load-bearing for SPEED, and the return
    // value becomes unusable.
    const uint64_t compressed_total =
        static_cast<uint64_t>(out_pointers[n - 1] - compressed.data()) + out_lengths[n - 1];
    started = Clock::now();
    for (size_t i = 0; i < n; ++i) {
        const uint64_t offset = static_cast<uint64_t>(out_pointers[i] - compressed.data());
        const uint64_t remaining = compressed_total - offset;
        const uint64_t bound = 2 * lengths[i] + 7;
        const size_t len_in = static_cast<size_t>(bound < remaining ? bound : remaining);

        if (scratch.size() < lengths[i] + 32) scratch.resize(lengths[i] + 32);
        fsst_decompress(&decoder, len_in, compressed.data() + offset,
                        lengths[i], scratch.data());
        if (std::memcmp(scratch.data(), pointers[i], lengths[i]) != 0) {
            std::fprintf(stderr,
                         "%s: column '%s': string %zu decoded WRONG from (offset, "
                         "logical length) alone\n", file.c_str(), name.c_str(), i);
            return false;
        }
    }
    const double bounded_ms = ms_since(started);

    // ── FSST then zstd-1, the question that actually decides it ──
    //
    // If stacking lands near zstd alone, FSST costs nothing on disk and the
    // per-string decode comes free. If FSST has already spent the redundancy
    // zstd would have claimed, it does not.
    uint64_t stacked_bytes = 0;
    {
        std::vector<unsigned char> sbuf(ZSTD_compressBound(fsst_bytes));
        const size_t got = ZSTD_compress(sbuf.data(), sbuf.size(), compressed.data(),
                                         fsst_bytes, 1);
        if (ZSTD_isError(got)) {
            std::fprintf(stderr, "%s: column '%s': zstd over fsst failed: %s\n",
                         file.c_str(), name.c_str(), ZSTD_getErrorName(got));
            return false;
        }
        stacked_bytes = got + symbol_table_bytes;
    }

    // ── zstd-1 on the same bytes, for the comparison that decides it ──
    std::vector<unsigned char> flat;
    flat.reserve(plain);
    for (size_t i = 0; i < n; ++i)
        flat.insert(flat.end(), pointers[i], pointers[i] + lengths[i]);

    std::vector<unsigned char> zbuf(ZSTD_compressBound(flat.size()));
    started = Clock::now();
    const size_t zproduced =
        ZSTD_compress(zbuf.data(), zbuf.size(), flat.data(), flat.size(), 1);
    const double zstd_compress_ms = ms_since(started);
    if (ZSTD_isError(zproduced)) {
        std::fprintf(stderr, "%s: column '%s': zstd failed: %s\n", file.c_str(),
                     name.c_str(), ZSTD_getErrorName(zproduced));
        return false;
    }

    std::vector<unsigned char> zout(flat.size());
    started = Clock::now();
    // Whole frame, because that is the only way zstd can be read: there is no
    // per-string decode to compare against.
    const size_t zgot = ZSTD_decompress(zout.data(), zout.size(), zbuf.data(), zproduced);
    const double zstd_decompress_ms = ms_since(started);
    if (ZSTD_isError(zgot) || zgot != flat.size()) {
        std::fprintf(stderr, "%s: column '%s': zstd round trip failed\n",
                     file.c_str(), name.c_str());
        return false;
    }

    std::printf("  %-16s %9zu %11" PRIu64 " %6.2fx %6.2fx %6.2fx\n",
                name.c_str(), n, plain,
                static_cast<double>(fsst_bytes + symbol_table_bytes) / static_cast<double>(plain),
                static_cast<double>(zproduced) / static_cast<double>(plain),
                static_cast<double>(stacked_bytes) / static_cast<double>(plain));

    tally->columns += 1;
    tally->strings += n;
    tally->plain += plain;
    tally->fsst += fsst_bytes;
    tally->symbol_tables += symbol_table_bytes;
    tally->zstd += zproduced;
    tally->stacked += stacked_bytes;
    tally->fsst_compress_ms += compress_ms;
    tally->fsst_decompress_ms += decompress_ms;
    tally->bounded_decompress_ms += bounded_ms;
    tally->zstd_compress_ms += zstd_compress_ms;
    tally->zstd_decompress_ms += zstd_decompress_ms;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: fsst_arena <file.skene> [more.skene ...]\n");
        return 1;
    }

    Tally tally;
    for (int a = 1; a < argc; ++a) {
        std::vector<uint8_t> bytes;
        Status st = read_file(argv[a], &bytes);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        CxxMorsel morsel;
        st = read_morsel(bytes.data(), bytes.size(), 0, &morsel);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        bool printed_header = false;
        for (size_t c = 0; c < morsel.columns.size(); ++c) {
            const DrakenVector& v = morsel.columns[c].view;
            if (!draken_type_is_string_storage(v.type)) continue;
            if (!printed_header) {
                std::printf("%s\n  %-16s %9s %11s %6s %6s %7s\n", argv[a],
                            "column", "strings", "plain", "fsst", "zstd-1",
                            "fsst+z1");
                printed_header = true;
            }
            if (!measure_column(argv[a], morsel.names[c], v, &tally)) return 1;
        }
    }

    if (tally.plain == 0) {
        std::fprintf(stderr, "no out-of-line string payload found in these files\n");
        return 1;
    }

    const double mb = static_cast<double>(tally.plain) / (1024.0 * 1024.0);
    const uint64_t fsst_total = tally.fsst + tally.symbol_tables;

    std::printf("\n%" PRIu64 " columns, %" PRIu64 " strings, %.1f MB of arena\n",
                tally.columns, tally.strings, mb);
    std::printf("  %-8s %12s %8s %10s %10s %12s\n",
                "codec", "bytes", "ratio", "compress", "decompress", "random access");
    std::printf("  %-8s %12" PRIu64 " %7.2fx %8.0f MB/s %6.0f MB/s %12s\n",
                "fsst", fsst_total,
                static_cast<double>(fsst_total) / static_cast<double>(tally.plain),
                mb / (tally.fsst_compress_ms / 1000.0),
                mb / (tally.fsst_decompress_ms / 1000.0), "per string");
    std::printf("  %-8s %12" PRIu64 " %7.2fx %8.0f MB/s %6.0f MB/s %12s\n",
                "zstd-1", tally.zstd,
                static_cast<double>(tally.zstd) / static_cast<double>(tally.plain),
                mb / (tally.zstd_compress_ms / 1000.0),
                mb / (tally.zstd_decompress_ms / 1000.0), "whole section");

    std::printf("  %-8s %12s %8s %8s %6.0f MB/s %12s\n",
                "fsst*", "-", "-", "-",
                mb / (tally.bounded_decompress_ms / 1000.0), "per string");
    std::printf("     * decoded from (offset, logical length) only — no stored extent\n");
    std::printf("  %-8s %12" PRIu64 " %7.2fx %8s %10s %12s\n",
                "fsst+z1", tally.stacked,
                static_cast<double>(tally.stacked) / static_cast<double>(tally.plain),
                "-", "-", "whole section");

    // FSST needs a compressed length per slot, because the compressed bytes are
    // not the logical length the slot records. The dead `hash32` field is 4 bytes
    // wide and always zero (E37), so it fits with NO growth — but that is a
    // draken ABI decision, so the cost is shown both ways.
    const uint64_t bookkeeping = tally.strings * 4u;
    std::printf("\nper-slot compressed length: %" PRIu64 " bytes (%" PRIu64 " slots x 4)\n",
                bookkeeping, tally.strings);
    std::printf("  into the dead hash32 field: %12" PRIu64 "  %.2fx\n",
                fsst_total, static_cast<double>(fsst_total) / static_cast<double>(tally.plain));
    std::printf("  as a new array:             %12" PRIu64 "  %.2fx\n",
                fsst_total + bookkeeping,
                static_cast<double>(fsst_total + bookkeeping) / static_cast<double>(tally.plain));
    return 0;
}
