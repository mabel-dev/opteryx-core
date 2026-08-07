// What does the bloom filter add that statistics and zone maps do not?
//
// A bloom is only worth its bytes for values the cheaper structures cannot
// already reject. Those come in three tiers, each strictly cheaper than the next:
//
//   statistics   one min/max per column. Rejects anything outside the range.
//   zone map     min/max per 8k chunk. Rejects in-range values that fall in no
//                chunk's span — which needs the data to be clustered.
//   bloom        rejects in-range, in-span values that are genuinely absent.
//
// The bloom's MARGINAL contribution is the only thing that justifies it, and it
// should differ sharply by type. A numeric ordinal is exact, so a numeric zone
// map answers equality precisely. A STRING ordinal truncates — draken packs the
// leading bytes into an int64 — so many distinct strings collapse onto one
// ordinal and a string zone map can only ever prune by prefix. If that is right,
// blooms earn their keep on strings and are near-redundant on numerics.
//
// Probes are real values taken from ANOTHER row group of the same table, which is
// the actual workload: a predicate value that exists in the table but may not be
// in this file. Ground truth comes from scanning the file, so "absent" is a fact
// rather than an assumption.
//
// NOT part of libskene.a. See bench/README.md.

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/reader.h"

#include "statistics.h"

#include "core/string_slot.h"

using namespace skene;

namespace {

bool is_string(DrakenType type) {
    return type == DRAKEN_VARCHAR || type == DRAKEN_NVARCHAR || type == DRAKEN_VARBINARY;
}

// A value's raw bytes — the same bytes the bloom hashes and equality compares.
bool value_bytes(const DrakenVector& v, uint32_t row, std::string* out) {
    const uint32_t code = v.selection[row];
    if (is_string(v.type)) {
        const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(v.data);
        if (arena == nullptr || arena->payloads_elided) return false;
        const DrakenStringSlot* slot = &arena->slots[code];
        out->assign(reinterpret_cast<const char*>(str_data(slot, arena->arena)),
                    str_length(slot));
        return true;
    }
    const size_t width = draken_type_fixed_itemsize(v.type);
    if (width == 0 || v.type == DRAKEN_BOOL) return false;
    out->assign(static_cast<const char*>(v.data) + static_cast<size_t>(code) * width,
                width);
    return true;
}

bool row_is_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

struct Bucket {
    uint64_t columns = 0, probes = 0;
    uint64_t by_stats = 0, by_zone = 0, by_bloom = 0, bloom_only = 0, unpruned = 0;
    uint64_t bloom_bytes = 0;
};

void account(Bucket* b, bool stats_rejects, bool zone_rejects, bool bloom_rejects) {
    b->probes += 1;
    if (stats_rejects) b->by_stats += 1;
    if (zone_rejects)  b->by_zone += 1;
    if (bloom_rejects) b->by_bloom += 1;
    // The number that decides the policy: rejected ONLY by the bloom.
    if (bloom_rejects && !stats_rejects && !zone_rejects) b->bloom_only += 1;
    if (!bloom_rejects && !stats_rejects && !zone_rejects) b->unpruned += 1;
}

void report(const char* label, const Bucket& b) {
    if (b.probes == 0) { std::printf("  %-10s no probes\n", label); return; }
    const double pct = 100.0 / static_cast<double>(b.probes);
    std::printf("  %-10s %3" PRIu64 " cols %8" PRIu64 " probes | stats %5.1f%%  "
                "zone %5.1f%%  bloom %5.1f%% | BLOOM-ONLY %5.1f%%  none %5.1f%% | "
                "%" PRIu64 " bloom bytes\n",
                label, b.columns, b.probes, b.by_stats * pct, b.by_zone * pct,
                b.by_bloom * pct, b.bloom_only * pct, b.unpruned * pct, b.bloom_bytes);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: prune_value <target.skene> <probe-source.skene>\n");
        return 1;
    }

    std::vector<uint8_t> target_bytes, probe_bytes;
    if (!read_file(argv[1], &target_bytes).is_ok()) return 1;
    if (!read_file(argv[2], &probe_bytes).is_ok()) return 1;

    CxxMorsel target, probes;
    FileMetadata meta;
    if (!read_morsel(target_bytes.data(), target_bytes.size(), &target).is_ok()) return 1;
    if (!read_morsel(probe_bytes.data(), probe_bytes.size(), &probes).is_ok()) return 1;
    if (!read_metadata(target_bytes.data(), target_bytes.size(), &meta).is_ok()) return 1;

    Bucket strings, numerics;
    std::printf("%s  probed with values from %s\n", argv[1], argv[2]);

    for (size_t c = 0; c < target.columns.size(); ++c) {
        const DrakenVector& tv = target.columns[c].view;
        const ColumnMetadata& cm = meta.columns[c];

        // Match by name: column order is not guaranteed to agree across files.
        size_t p = probes.names.size();
        for (size_t i = 0; i < probes.names.size(); ++i)
            if (probes.names[i] == target.names[c]) { p = i; break; }
        if (p == probes.names.size()) continue;
        const DrakenVector& pv = probes.columns[p].view;
        if (tv.type != pv.type) continue;

        // Ground truth: everything actually in the target column.
        std::set<std::string> present;
        std::string scratch;
        bool usable = true;
        for (uint32_t row = 0; row < tv.length && usable; ++row) {
            if (!row_is_valid(tv, row)) continue;
            if (!value_bytes(tv, row, &scratch)) { usable = false; break; }
            present.insert(scratch);
        }
        if (!usable || present.empty()) continue;

        Bucket* bucket = is_string(tv.type) ? &strings : &numerics;
        bucket->columns += 1;
        bucket->bloom_bytes += cm.bloom.size();

        std::set<std::string> tried;
        for (uint32_t row = 0; row < pv.length && tried.size() < 4000; ++row) {
            if (!row_is_valid(pv, row)) continue;
            if (!value_bytes(pv, row, &scratch)) break;
            if (!tried.insert(scratch).second) continue;
            if (present.count(scratch) != 0) continue;   // present: nothing to prune

            int64_t ordinal = 0;
            const bool has_ordinal =
                column_ordinal_at(pv, nullptr, pv.selection[row], &ordinal);

            const bool stats_rejects =
                has_ordinal && cm.has_statistics
                && (ordinal < cm.statistics.min_ordinal
                    || ordinal > cm.statistics.max_ordinal);

            bool zone_rejects = false;
            if (has_ordinal && cm.zone_map.present()) {
                zone_rejects = true;
                for (size_t chunk = 0; chunk < cm.zone_map.chunks.size(); ++chunk)
                    if (cm.zone_map.chunk_may_contain(chunk, ordinal, ordinal)) {
                        zone_rejects = false;
                        break;
                    }
            }

            bool may = true;
            if (!cm.bloom.empty())
                bloom_may_contain(cm, scratch.data(),
                                  static_cast<uint32_t>(scratch.size()), &may);
            account(bucket, stats_rejects, zone_rejects, !may);
        }
    }

    report("STRING", strings);
    report("NUMERIC", numerics);
    return 0;
}
