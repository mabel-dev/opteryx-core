// Which sections are worth compressing, and with what?
//
// Walks a RAW .skene file's section directory and tries every available codec on
// every section body, aggregating by section kind. That answers three questions
// with measurements instead of intuition:
//
//   - which section kinds actually compress (so the rest can skip the attempt)
//   - which codec gives the best size/speed trade
//   - how small a section has to be before the attempt stops being worth it
//
// Reads the footer with its own parser rather than through skene's reader: the
// reader hands back columns, and this needs the per-SECTION bodies.
//
// NOT part of libskene.a. See bench/README.md.

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/format.h"

#include "zstd.h"
#include "lz4.h"
#include "snappy.h"

using namespace skene;
using Clock = std::chrono::steady_clock;

namespace {

const char* kind_name(uint16_t kind) {
    switch (static_cast<SectionKind>(kind)) {
        case SectionKind::kData:        return "DATA";
        case SectionKind::kSelection:   return "SELECTION";
        case SectionKind::kValidity:    return "VALIDITY";
        case SectionKind::kStringSlots: return "STRING_SLOTS";
        case SectionKind::kStringArena: return "STRING_ARENA";
        case SectionKind::kBloom:       return "BLOOM";
        case SectionKind::kPermutation: return "PERMUTATION";
        case SectionKind::kZoneMap:     return "ZONE_MAP";
        default:                        return "other";
    }
}

// Minimal footer walk — only the section directory is needed.
bool read_sections(const std::vector<uint8_t>& file,
                   std::vector<SectionEntry>* out) {
    if (file.size() < kMinFileBytes) return false;
    FileTail tail;
    std::memcpy(&tail, file.data() + file.size() - kFileTailBytes, sizeof(tail));
    if (tail.magic != kMagic) return false;

    const size_t footer_end = file.size() - kFileTailBytes;
    const size_t footer_at  = footer_end - tail.footer_bytes;

    FooterFileHeader header;
    std::memcpy(&header, file.data() + footer_at, sizeof(header));

    // The section directory sits immediately before the statistics blobs, and
    // every entry is fixed width, so it can be found from the end without
    // walking the variable-length column directory.
    const size_t stats_bytes = 0;  // written only when statistics are enabled
    const size_t sections_at = footer_end - stats_bytes
        - static_cast<size_t>(header.section_count) * sizeof(SectionEntry);

    out->resize(header.section_count);
    for (uint32_t i = 0; i < header.section_count; ++i)
        std::memcpy(&(*out)[i], file.data() + sections_at + i * sizeof(SectionEntry),
                    sizeof(SectionEntry));
    return true;
}

struct Codec {
    const char* name;
    // Returns compressed size, or 0 when the codec declined/failed.
    size_t (*compress)(const uint8_t* src, size_t n, std::vector<uint8_t>& scratch);
};

size_t do_zstd(const uint8_t* src, size_t n, std::vector<uint8_t>& scratch, int level) {
    const size_t bound = ZSTD_compressBound(n);
    if (scratch.size() < bound) scratch.resize(bound);
    const size_t produced = ZSTD_compress(scratch.data(), bound, src, n, level);
    return ZSTD_isError(produced) ? 0 : produced;
}

size_t zstd1(const uint8_t* s, size_t n, std::vector<uint8_t>& b) { return do_zstd(s,n,b,1); }
size_t zstd3(const uint8_t* s, size_t n, std::vector<uint8_t>& b) { return do_zstd(s,n,b,3); }
size_t zstd9(const uint8_t* s, size_t n, std::vector<uint8_t>& b) { return do_zstd(s,n,b,9); }

size_t lz4_fast(const uint8_t* src, size_t n, std::vector<uint8_t>& scratch) {
    if (n > LZ4_MAX_INPUT_SIZE) return 0;
    const int bound = LZ4_compressBound(static_cast<int>(n));
    if (scratch.size() < static_cast<size_t>(bound)) scratch.resize(bound);
    const int produced = LZ4_compress_default(
        reinterpret_cast<const char*>(src),
        reinterpret_cast<char*>(scratch.data()), static_cast<int>(n), bound);
    return produced <= 0 ? 0 : static_cast<size_t>(produced);
}

size_t snappy_codec(const uint8_t* src, size_t n, std::vector<uint8_t>& scratch) {
    const size_t bound = snappy::MaxCompressedLength(n);
    if (scratch.size() < bound) scratch.resize(bound);
    size_t produced = 0;
    snappy::RawCompress(reinterpret_cast<const char*>(src), n,
                        reinterpret_cast<char*>(scratch.data()), &produced);
    return produced;
}

struct Totals {
    uint64_t sections = 0;
    uint64_t plain = 0;
    std::map<std::string, uint64_t> compressed;
    std::map<std::string, double>   milliseconds;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: compression_bakeoff <raw.skene> [more.skene ...]\n");
        return 1;
    }

    const Codec codecs[] = {
        {"zstd-1", zstd1}, {"zstd-3", zstd3}, {"zstd-9", zstd9},
        {"lz4",    lz4_fast}, {"snappy", snappy_codec},
    };

    std::map<std::string, Totals> by_kind;
    Totals overall;
    std::vector<uint8_t> scratch;

    // Size buckets, to find where the attempt stops paying.
    struct Bucket { const char* label; uint64_t low, high; uint64_t plain = 0, best = 0, count = 0; };
    Bucket buckets[] = {
        {"<1K",      0,        1024},
        {"1K-10K",   1024,     10240},
        {"10K-100K", 10240,    102400},
        {"100K-1M",  102400,   1048576},
        {">1M",      1048576,  UINT64_MAX},
    };

    for (int a = 1; a < argc; ++a) {
        std::vector<uint8_t> file;
        Status st = read_file(argv[a], &file);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        std::vector<SectionEntry> sections;
        if (!read_sections(file, &sections)) {
            std::fprintf(stderr, "%s: could not read the section directory\n", argv[a]);
            return 1;
        }

        for (const SectionEntry& section : sections) {
            if (section.encoding != static_cast<uint16_t>(Encoding::kPlain)) continue;
            if (section.offset + section.stored_bytes > file.size()) continue;

            const uint8_t* body = file.data() + section.offset;
            const size_t   n    = static_cast<size_t>(section.stored_bytes);
            if (n == 0) continue;

            Totals& totals = by_kind[kind_name(section.kind)];
            totals.sections += 1;
            totals.plain += n;
            overall.sections += 1;
            overall.plain += n;

            size_t best = n;
            for (const Codec& codec : codecs) {
                const auto started = Clock::now();
                const size_t produced = codec.compress(body, n, scratch);
                const double took =
                    std::chrono::duration<double, std::milli>(Clock::now() - started).count();
                const size_t kept = (produced > 0 && produced < n) ? produced : n;
                totals.compressed[codec.name] += kept;
                totals.milliseconds[codec.name] += took;
                overall.compressed[codec.name] += kept;
                overall.milliseconds[codec.name] += took;
                if (kept < best) best = kept;
            }

            for (Bucket& bucket : buckets) {
                if (n >= bucket.low && n < bucket.high) {
                    bucket.plain += n; bucket.best += best; bucket.count += 1;
                    break;
                }
            }
        }
    }

    std::printf("%-14s %8s %13s", "section kind", "count", "plain");
    for (const Codec& codec : codecs) std::printf(" %16s", codec.name);
    std::printf("\n");

    auto row = [&](const char* label, const Totals& totals) {
        std::printf("%-14s %8" PRIu64 " %13" PRIu64, label, totals.sections, totals.plain);
        for (const Codec& codec : codecs) {
            const auto found = totals.compressed.find(codec.name);
            if (found == totals.compressed.end()) { std::printf(" %16s", "-"); continue; }
            const double ratio = totals.plain > 0
                ? static_cast<double>(found->second) / static_cast<double>(totals.plain) : 1.0;
            std::printf(" %9.2fx%6.0fms", ratio, totals.milliseconds.at(codec.name));
        }
        std::printf("\n");
    };

    for (const auto& entry : by_kind) row(entry.first.c_str(), entry.second);
    std::printf("%-14s\n", "");
    row("ALL", overall);

    std::printf("\n%-10s %8s %13s %13s %s\n", "size", "count", "plain", "best", "saving");
    for (const Bucket& bucket : buckets) {
        if (bucket.count == 0) continue;
        std::printf("%-10s %8" PRIu64 " %13" PRIu64 " %13" PRIu64 " %5.1f%%\n",
                    bucket.label, bucket.count, bucket.plain, bucket.best,
                    bucket.plain > 0
                        ? 100.0 * (1.0 - static_cast<double>(bucket.best)
                                          / static_cast<double>(bucket.plain))
                        : 0.0);
    }
    return 0;
}
