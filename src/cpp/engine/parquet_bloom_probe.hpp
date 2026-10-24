#pragma once
// Remote bloom membership for the native parquet scan
// (docs/PARQUET_WRITE_FOR_READ_SPEED.md §3.3b).
//
// Planning decides the work: for each REMOTE work item that survived min/max
// pruning, which bloom filters to test and with which PLAIN-encoded needles
// (pool_reader.pyx, `_remote_bloom_probes`). Execution does it: the scan issues
// every file's bloom bytes in ONE concurrent range round at make_global — in
// the same round as the first fetch blocks, never ahead of them — and, once
// the bytes land, excludes every not-yet-submitted work item whose bloom proves
// its row group holds no match. Units already submitted are read as before; a
// bloom that lands late only means fewer exclusions, never a different answer.
//
// Blooms already read by this process come from the process bloom cache
// (rugo/src/parquet/bloom_bytes_cache.hpp) and only the misses are fetched; the
// fetch fills the cache. A round with no miss issues no GET and has landed
// before the scan's first walk.
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io_pipeline.hpp"
#include "bloom_bytes_cache.hpp"

// One bloom test planned for one work item.
struct ParquetBloomProbe {
    int32_t item = -1;        // index into the scan's work_items
    int64_t offset = -1;      // the bloom filter (header + bitset) in the file
    int64_t length = 0;
    // PLAIN-encoded literals of one Eq / InList predicate on one column. The
    // row group is excluded when the bloom proves EVERY needle absent; a work
    // item with several probes (several predicate columns) is excluded when
    // ANY of them does — the predicates are a conjunction.
    std::vector<std::string> needles;
};

// One scan's bloom read: per probe, either its cached bytes or the coalesced
// run it was fetched in.
struct ParquetBloomRound {
    std::shared_ptr<rugo::ParquetIOPipeline::RangeFetch> fetch;
    std::vector<rugo::ParquetIOPipeline::RangeRequest> runs;
    std::vector<rugo::BloomBytesCache::Bytes> cached;   // per probe: cache hit, else nullptr
    std::vector<size_t> probe_run;   // per probe: index into `runs` (fetched probes only)
    bool applied = false;
};

// Answer what the process bloom cache holds, coalesce each file's remaining
// probe ranges with the pipeline's own coalescer and issue them all as one
// asynchronous round. Returns at once.
inline std::unique_ptr<ParquetBloomRound> start_bloom_round(
        rugo::ParquetIOPipeline& pipe, const std::vector<ParquetBloomProbe>& probes,
        const std::vector<std::pair<std::string, int>>& work_items) {
    auto round = std::make_unique<ParquetBloomRound>();
    rugo::BloomBytesCache& cache = rugo::bloom_bytes_cache();
    round->cached.resize(probes.size());
    uint64_t hits = 0;
    std::unordered_map<std::string, std::vector<std::pair<int64_t, int64_t>>> per_file;
    std::vector<std::string> file_order;
    for (size_t i = 0; i < probes.size(); ++i) {
        const ParquetBloomProbe& p = probes[i];
        if (p.item < 0 || static_cast<size_t>(p.item) >= work_items.size())
            throw std::logic_error("bloom probe names no work item");
        if (p.offset < 0 || p.length <= 0 || p.needles.empty())
            throw std::logic_error("bloom probe without a sized bloom or a needle");
        const std::string& path = work_items[static_cast<size_t>(p.item)].first;
        round->cached[i] = cache.lookup(path, p.offset, p.length);
        if (round->cached[i]) {
            ++hits;
            continue;
        }
        auto it = per_file.find(path);
        if (it == per_file.end()) {
            it = per_file.emplace(path, std::vector<std::pair<int64_t, int64_t>>{}).first;
            file_order.push_back(path);
        }
        it->second.emplace_back(p.offset, p.offset + p.length);
    }
    // Runs per file, in file order; remember where each file's runs start.
    std::unordered_map<std::string, std::pair<size_t, size_t>> file_runs;  // [first, last)
    for (const std::string& path : file_order) {
        const size_t first = round->runs.size();
        for (const auto& r : pipe.coalesce_ranges(per_file[path]))
            round->runs.push_back(rugo::ParquetIOPipeline::RangeRequest{path, r.first, r.second});
        file_runs.emplace(path, std::make_pair(first, round->runs.size()));
    }
    round->probe_run.assign(probes.size(), SIZE_MAX);
    std::vector<rugo::ParquetIOPipeline::BloomSlice> slices;
    for (size_t i = 0; i < probes.size(); ++i) {
        if (round->cached[i]) continue;
        const ParquetBloomProbe& p = probes[i];
        const auto& span = file_runs[work_items[static_cast<size_t>(p.item)].first];
        size_t r = span.first;
        while (r < span.second && !(round->runs[r].start <= p.offset &&
                                    p.offset + p.length <= round->runs[r].end))
            ++r;
        if (r == span.second) throw std::logic_error("bloom probe outside every coalesced run");
        round->probe_run[i] = r;
        slices.push_back(rugo::ParquetIOPipeline::BloomSlice{r, p.offset, p.length});
    }
    pipe.note_bloom_cache(hits, probes.size() - hits, cache.bytes());
    round->fetch = pipe.fetch_ranges_async(round->runs, slices);
    return round;
}

// Once `round.fetch` reports success: per work item, 1 = some predicate's bloom
// proved its row group holds no match. A bloom that fails to parse proves
// nothing and keeps the row group — the plan-time probe's contract
// (`_bloom_proves_absent`); a FETCH failure is the caller's error to raise.
inline std::vector<uint8_t> bloom_excluded_items(const ParquetBloomRound& round,
                                                 const std::vector<ParquetBloomProbe>& probes,
                                                 size_t n_work_items) {
    std::vector<uint8_t> excluded(n_work_items, 0);
    for (size_t i = 0; i < probes.size(); ++i) {
        const ParquetBloomProbe& p = probes[i];
        if (excluded[static_cast<size_t>(p.item)]) continue;
        const uint8_t* data;
        if (round.cached[i]) {
            data = round.cached[i]->data();
        } else {
            const auto& run = round.runs[round.probe_run[i]];
            data = round.fetch->bufs[round.probe_run[i]].data() + (p.offset - run.start);
        }
        bool all_absent = true;
        for (const std::string& needle : p.needles) {
            bool maybe = true;
            try {
                maybe = TestBloomFilterBytes(data, static_cast<size_t>(p.length), needle);
            } catch (const std::exception&) {
                maybe = true;
            }
            if (maybe) { all_absent = false; break; }
        }
        if (all_absent) excluded[static_cast<size_t>(p.item)] = 1;
    }
    return excluded;
}
