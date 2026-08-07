// How much of a real file can actually be pruned before decoding?
//
// The intended read path is: catalog prunes files on min/max, survivors' footers
// are read, and BRIN-style zone maps prune to 8k blocks WITHOUT touching column
// data. That path is only as good as its coverage — a column with no zone map
// cannot be pruned at all, and no amount of predicate cleverness recovers it.
//
// So this counts, per column, what the footer actually carries: statistics, a
// zone map, a bloom filter, and whether the column is value-ordered (which is
// what makes a zone map's CODE bounds mean anything).
//
// Reads footers only — no column data is touched, which is also a check that the
// metadata path really is independent of the data region.
//
// NOT part of libskene.a. See bench/README.md.

#include <cinttypes>
#include <cstdio>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/reader.h"

using namespace skene;

namespace {

struct Tally {
    uint64_t files = 0;
    uint64_t columns = 0;
    uint64_t with_statistics = 0;
    uint64_t with_zone_map = 0;
    uint64_t with_bloom = 0;
    uint64_t value_ordered = 0;
    uint64_t rows = 0;
    uint64_t zone_chunks = 0;
    uint64_t zone_bytes = 0;
    uint64_t bloom_bytes = 0;
    uint64_t file_bytes = 0;
};

void walk(const ColumnMetadata& column, Tally* tally, bool verbose) {
    tally->columns += 1;
    if (column.has_statistics) tally->with_statistics += 1;
    if (column.zone_map.present()) {
        tally->with_zone_map += 1;
        tally->zone_chunks += column.zone_map.chunks.size();
        tally->zone_bytes += sizeof(ZoneMapHeader)
                           + column.zone_map.chunks.size() * sizeof(ZoneMapEntry);
    }
    if (!column.bloom.empty()) {
        tally->with_bloom += 1;
        tally->bloom_bytes += column.bloom.size();
    }
    if (column.value_order == ValueOrder::kAscending) tally->value_ordered += 1;

    if (verbose)
        std::printf("  %-20s ndv=%8u  bloom=%9zu  bits/ndv=%5.1f  zone=%-5s ord=%s\n",
                    column.name.c_str(), column.data_length, column.bloom.size(),
                    column.data_length > 0
                        ? 8.0 * static_cast<double>(column.bloom.size())
                              / static_cast<double>(column.data_length) : 0.0,
                    column.zone_map.present()
                        ? std::to_string(column.zone_map.chunks.size()).c_str() : "no",
                    column.value_order == ValueOrder::kAscending ? "yes" : "no");

    for (const ColumnMetadata& child : column.children) walk(child, tally, verbose);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: index_coverage [-v] <file.skene> [more...]\n");
        return 1;
    }

    int first = 1;
    bool verbose = false;
    if (std::string(argv[1]) == "-v") { verbose = true; first = 2; }

    Tally tally;
    for (int a = first; a < argc; ++a) {
        std::vector<uint8_t> bytes;
        Status st = read_file(argv[a], &bytes);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        FileMetadata meta;
        st = read_metadata(bytes.data(), bytes.size(), &meta);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        if (verbose) std::printf("%s  (%" PRIu64 " rows)\n", argv[a], meta.row_count);
        tally.files += 1;
        tally.rows += meta.row_count;
        tally.file_bytes += bytes.size();
        for (const ColumnMetadata& column : meta.columns) walk(column, &tally, verbose);
    }

    const double pct = tally.columns > 0 ? 100.0 / static_cast<double>(tally.columns) : 0.0;
    std::printf("\n%" PRIu64 " files, %" PRIu64 " rows, %" PRIu64 " columns\n",
                tally.files, tally.rows, tally.columns);
    std::printf("  statistics    %6" PRIu64 "  %5.1f%%\n", tally.with_statistics,
                tally.with_statistics * pct);
    std::printf("  zone maps     %6" PRIu64 "  %5.1f%%   (%" PRIu64 " chunks total)\n",
                tally.with_zone_map, tally.with_zone_map * pct, tally.zone_chunks);
    std::printf("  bloom filters %6" PRIu64 "  %5.1f%%\n", tally.with_bloom,
                tally.with_bloom * pct);
    std::printf("  value-ordered %6" PRIu64 "  %5.1f%%\n", tally.value_ordered,
                tally.value_ordered * pct);

    const double of_file = tally.file_bytes > 0
        ? 100.0 / static_cast<double>(tally.file_bytes) : 0.0;
    std::printf("\nindex cost against %" PRIu64 " bytes of file\n", tally.file_bytes);
    std::printf("  zone maps     %12" PRIu64 " bytes  %5.2f%%\n",
                tally.zone_bytes, tally.zone_bytes * of_file);
    std::printf("  bloom filters %12" PRIu64 " bytes  %5.2f%%\n",
                tally.bloom_bytes, tally.bloom_bytes * of_file);
    return 0;
}
