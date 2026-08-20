// skene v2 section inventory — the v2 counterpart of dev/skene_section_census.cpp
// (which understands v1 only and predates the codec axis).
//
// Walks the REAL section directories of a v2 .skene file (file footer -> row
// group directory -> per-RG footer -> column directory -> section directory,
// mirroring reader_v2.cpp's parse order), sums bytes by (kind, encoding, codec),
// and measures what LZ4 would still recover from the bodies stored WITHOUT a
// codec — the residual the writer's gates declined.
//
// Build (from the repo root):
//
//   c++ -std=c++17 -O2 -DNDEBUG -o /tmp/skene_v2_inventory \
//     dev/skene_v2_inventory.cpp third_party/lz4/lz4.c \
//     -I third_party/lz4 -I skene/include
//
// Dev tooling only — never imported by production code (repo rules §5).

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "lz4.h"
#include "skene/format.h"

using namespace skene;

namespace {

struct Key {
    uint16_t kind;
    uint8_t  encoding;
    uint8_t  codec;
    bool operator<(const Key& o) const {
        if (kind != o.kind) return kind < o.kind;
        if (encoding != o.encoding) return encoding < o.encoding;
        return codec < o.codec;
    }
};

struct Tally {
    uint64_t count = 0;
    uint64_t stored = 0;
    uint64_t plain = 0;
    uint64_t lz4_recoverable = 0;   // codec-none bodies only, 85% floor
};

const char* kind_name(uint16_t kind) {
    switch (static_cast<SectionKind>(kind)) {
        case SectionKind::kData:        return "DATA";
        case SectionKind::kSelection:   return "SELECTION";
        case SectionKind::kValidity:    return "VALIDITY";
        case SectionKind::kStringSlots: return "STR_SLOTS_V1";
        case SectionKind::kStringArena: return "ARENA";
        case SectionKind::kSlotLane0:   return "LANE0_LEN";
        case SectionKind::kSlotLane1:   return "LANE1_PFX";
        case SectionKind::kSlotLane2:   return "LANE2_H32";
        case SectionKind::kSlotLane3:   return "LANE3_OFF";
        case SectionKind::kBloom:       return "BLOOM";
        case SectionKind::kPermutation: return "PERMUTATION";
        case SectionKind::kZoneMap:     return "ZONE_MAP";
    }
    return "?";
}

const char* enc_name(uint8_t e) {
    switch (static_cast<Encoding>(e)) {
        case Encoding::kPlain:        return "PLAIN";
        case Encoding::kBitpack:      return "BITPACK";
        case Encoding::kDeltaBitpack: return "DELTA";
        default:                      return "?";
    }
}

const char* codec_name(uint8_t c) {
    switch (static_cast<SectionCodec>(c)) {
        case SectionCodec::kNone: return "-";
        case SectionCodec::kZstd: return "zstd";
        case SectionCodec::kLz4:  return "lz4";
    }
    return "?";
}

// Skips one column entry (head + name + logical + children), collecting the
// section slice bounds; returns false on truncation.
bool walk_column(const uint8_t* footer, size_t footer_len, size_t* at) {
    if (*at + sizeof(ColumnEntryHead) > footer_len) return false;
    ColumnEntryHead head;
    std::memcpy(&head, footer + *at, sizeof(head));
    *at += sizeof(head) + head.name_bytes
         + (head.logical_present ? sizeof(LogicalTypeDescriptor) : 0);
    if (*at > footer_len) return false;
    for (uint32_t c = 0; c < head.child_count; ++c)
        if (!walk_column(footer, footer_len, at)) return false;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: skene_v2_inventory <file.skene>\n");
        return 1;
    }

    const int fd = ::open(argv[1], O_RDONLY);
    if (fd < 0) { std::perror(argv[1]); return 1; }
    struct stat st{};
    ::fstat(fd, &st);
    const size_t file_bytes = static_cast<size_t>(st.st_size);
    const uint8_t* file = static_cast<const uint8_t*>(
        ::mmap(nullptr, file_bytes, PROT_READ, MAP_PRIVATE, fd, 0));
    ::close(fd);
    if (file == MAP_FAILED) { std::perror("mmap"); return 1; }

    FileTail tail;
    std::memcpy(&tail, file + file_bytes - kFileTailBytes, sizeof(tail));
    if (tail.magic != kMagic) { std::fprintf(stderr, "not skene\n"); return 1; }
    if (tail.version != 2) {
        std::fprintf(stderr, "%s: format version %u, this tool understands 2\n",
                     argv[1], tail.version);
        return 1;
    }

    const size_t footer_at = file_bytes - kFileTailBytes - tail.footer_bytes;
    FileFooterHeader fh;
    std::memcpy(&fh, file + footer_at, sizeof(fh));
    const size_t rg_dir = footer_at + sizeof(fh) + fh.writer_tag_bytes;

    std::map<Key, Tally> tallies;
    uint64_t total_stored = 0;
    uint64_t padding = 0;
    uint64_t last_end = kFileHeadBytes;

    for (uint32_t g = 0; g < fh.row_group_count; ++g) {
        RowGroupEntry entry;
        std::memcpy(&entry, file + rg_dir + g * sizeof(RowGroupEntry), sizeof(entry));

        const uint8_t* rgf = file + entry.footer_offset;
        RowGroupFooterHeader rh;
        std::memcpy(&rh, rgf, sizeof(rh));
        size_t at = sizeof(rh) + rh.writer_tag_bytes;
        for (uint32_t c = 0; c < rh.column_count; ++c)
            if (!walk_column(rgf, entry.footer_bytes, &at)) {
                std::fprintf(stderr, "rg %u: truncated column directory\n", g);
                return 1;
            }

        std::vector<uint64_t> starts;
        for (uint32_t s = 0; s < rh.section_count; ++s) {
            SectionEntry e;
            std::memcpy(&e, rgf + at + s * sizeof(SectionEntry), sizeof(e));
            Tally& t = tallies[Key{e.kind, e.encoding, e.codec}];
            ++t.count;
            t.stored += e.stored_bytes;
            t.plain  += e.plain_bytes;
            total_stored += e.stored_bytes;

            if (e.codec == 0 && e.stored_bytes >= kCompressMinBytes) {
                const int bound = LZ4_compressBound(static_cast<int>(e.stored_bytes));
                std::vector<char> out(static_cast<size_t>(bound));
                const int packed = LZ4_compress_default(
                    reinterpret_cast<const char*>(file + e.offset), out.data(),
                    static_cast<int>(e.stored_bytes), bound);
                if (packed > 0
                        && static_cast<uint64_t>(packed) * 100u
                               <= e.stored_bytes * kStackFloorPercent)
                    t.lz4_recoverable += e.stored_bytes - static_cast<uint64_t>(packed);
            }
        }
    }

    // Padding: gaps between the end of one section and the start of the next,
    // within data+index regions.
    {
        std::vector<std::pair<uint64_t, uint64_t>> spans;
        for (uint32_t g = 0; g < fh.row_group_count; ++g) {
            RowGroupEntry entry;
            std::memcpy(&entry, file + rg_dir + g * sizeof(RowGroupEntry), sizeof(entry));
            const uint8_t* rgf = file + entry.footer_offset;
            RowGroupFooterHeader rh;
            std::memcpy(&rh, rgf, sizeof(rh));
            size_t at = sizeof(rh) + rh.writer_tag_bytes;
            for (uint32_t c = 0; c < rh.column_count; ++c)
                walk_column(rgf, entry.footer_bytes, &at);
            uint64_t covered = 0;
            for (uint32_t s = 0; s < rh.section_count; ++s) {
                SectionEntry e;
                std::memcpy(&e, rgf + at + s * sizeof(SectionEntry), sizeof(e));
                covered += e.stored_bytes;
            }
            padding += entry.data_bytes - covered;
        }
        (void)last_end;
    }

    std::printf("skene v2 inventory — %s\n", argv[1]);
    std::printf("  %llu row groups, %llu rows, %u columns, %.1f MB on disk\n",
                static_cast<unsigned long long>(fh.row_group_count),
                static_cast<unsigned long long>(fh.row_count), fh.column_count,
                file_bytes / 1048576.0);
    std::printf("  alignment padding: %.2f MB (%.3f%%)\n\n",
                padding / 1048576.0, 100.0 * padding / file_bytes);

    std::printf("  %-13s %-8s %-5s %8s %11s %11s %7s %12s\n", "kind", "encoding",
                "codec", "count", "stored MB", "plain MB", "ratio", "residual MB");
    uint64_t residual = 0;
    for (const auto& [key, t] : tallies) {
        std::printf("  %-13s %-8s %-5s %8llu %11.1f %11.1f %6.2fx %12.1f\n",
                    kind_name(key.kind), enc_name(key.encoding),
                    codec_name(key.codec),
                    static_cast<unsigned long long>(t.count),
                    t.stored / 1048576.0, t.plain / 1048576.0,
                    t.stored > 0 ? static_cast<double>(t.plain) / t.stored : 0.0,
                    t.lz4_recoverable / 1048576.0);
        residual += t.lz4_recoverable;
    }
    std::printf("\n  section bytes %.1f MB; lz4 residual at %llu%% floor: %.1f MB "
                "(%.1f%% of file)\n",
                total_stored / 1048576.0,
                static_cast<unsigned long long>(kStackFloorPercent),
                residual / 1048576.0, 100.0 * residual / file_bytes);

    ::munmap(const_cast<uint8_t*>(file), file_bytes);
    return 0;
}
