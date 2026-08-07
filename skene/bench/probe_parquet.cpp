// What does rugo's decoder ACTUALLY hand back for a given Parquet file?
//
// Written to answer a specific question with evidence rather than inference: a
// column's handoff form (DirectKind) is decided at DECODE time, not by anything
// visible in the file's metadata, so reading the footer cannot tell you. Any
// column classified DK_POOL comes back as a serialized IPC blob rather than a
// buffer, and reconstructing it needs the IPC deserializer — whose C++ half
// covers only a few tags. So DK_POOL is exactly the set a standalone converter
// cannot handle, and this prints which columns land there.
//
// NOT part of libskene.a. See bench/README.md.

#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "core/alloc.h"

#include "parquet/io_pipeline.hpp"
#include "parquet/metadata.hpp"

static const char* kind_name(int kind) {
    switch (kind) {
        case rugo::DK_POOL:          return "POOL";
        case rugo::DK_INT64:         return "INT64";
        case rugo::DK_FLOAT32:       return "FLOAT32";
        case rugo::DK_FLOAT64:       return "FLOAT64";
        case rugo::DK_BOOL:          return "BOOL";
        case rugo::DK_DECIMAL128:    return "DECIMAL128";
        case rugo::DK_VARCHAR:       return "VARCHAR";
        case rugo::DK_VARCHAR_DICT:  return "VARCHAR_DICT";
        case rugo::DK_INT64_DICT:    return "INT64_DICT";
        case rugo::DK_FLOAT64_DICT:  return "FLOAT64_DICT";
        case rugo::DK_FLOAT32_DICT:  return "FLOAT32_DICT";
        case rugo::DK_UINT8:         return "UINT8";
        case rugo::DK_UINT16:        return "UINT16";
        case rugo::DK_UINT32:        return "UINT32";
        case rugo::DK_UINT64:        return "UINT64";
        case rugo::DK_INT8:          return "INT8";
        case rugo::DK_INT16:         return "INT16";
        case rugo::DK_INT32:         return "INT32";
        case rugo::DK_INT8_DICT:     return "INT8_DICT";
        case rugo::DK_INT16_DICT:    return "INT16_DICT";
        case rugo::DK_INT32_DICT:    return "INT32_DICT";
        case rugo::DK_UINT8_DICT:    return "UINT8_DICT";
        case rugo::DK_UINT16_DICT:   return "UINT16_DICT";
        case rugo::DK_UINT32_DICT:   return "UINT32_DICT";
        case rugo::DK_UINT64_DICT:   return "UINT64_DICT";
        default:                     return "other";
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: probe_parquet <file.parquet> [row_group]\n");
        return 1;
    }
    const std::string path = argv[1];
    const int row_group = (argc > 2) ? std::atoi(argv[2]) : 0;

    const FileStats stats = ReadParquetMetadataC(path.c_str());
    if (stats.row_groups.empty()) {
        std::fprintf(stderr, "no row groups in %s\n", path.c_str());
        return 1;
    }
    std::printf("%s\n  %lld rows, %zu row groups, %zu columns\n\n", path.c_str(),
                static_cast<long long>(stats.num_rows), stats.row_groups.size(),
                stats.row_groups.empty() ? 0 : stats.row_groups[0].columns.size());

    rugo::ParquetIOPipeline pipeline(4);

    // Without a pool sink every column classifies DK_POOL by construction
    // (io_pipeline.hpp: `pool_sink_.draken_alloc ? direct_kind_for(..) : DK_POOL`),
    // so the allocators must be wired or the answer is meaningless.
    // A minimal stand-in for opteryx's MemoryPool. DK_POOL columns serialize an
    // IPC blob into it, so without one they fail rather than classify — and the
    // question here is which columns take that path, not whether they can be
    // stored.
    struct Pool {
        std::mutex mutex;
        std::vector<std::vector<uint8_t>> blobs;
    } pool;

    rugo::PoolSink sink;
    sink.ctx = &pool;
    sink.draken_alloc = [](size_t n) -> void* { return draken_malloc(n); };
    sink.draken_free  = [](void* p) { draken_free(p); };
    sink.reserve = [](void* ctx, int64_t size, void** out_ptr) -> int64_t {
        Pool* p = static_cast<Pool*>(ctx);
        std::lock_guard<std::mutex> lock(p->mutex);
        p->blobs.emplace_back(static_cast<size_t>(size));
        *out_ptr = p->blobs.back().data();
        return static_cast<int64_t>(p->blobs.size()) - 1;
    };
    sink.finalize = [](void*, int64_t, int64_t) {};
    pipeline.set_pool_sink(sink);

    std::vector<std::string> wanted;
    for (const ColumnStats& c : stats.row_groups[row_group].columns)
        wanted.push_back(c.name);

    pipeline.submit_row_group(path, row_group, wanted,
                              stats.row_groups[row_group].columns);

    rugo::MorselRef result;
    if (!pipeline.wait_and_get_result(result)) {
        std::fprintf(stderr, "no result from the pipeline\n");
        return 1;
    }
    if (!result.success) {
        std::fprintf(stderr, "decode failed: %s\n", result.error.c_str());
        return 1;
    }

    // The source file's codec decides what the size comparison actually means:
    // skene stores raw bytes, so comparing it against a compressed Parquet is a
    // different question from comparing it against an uncompressed one.
    std::printf("codec per column: ");
    for (const ColumnStats& c : stats.row_groups[row_group].columns)
        std::printf("%s ", CompressionCodecToString(c.codec));
    std::printf("\n\n");

    std::printf("%-20s %-14s %-10s %s\n", "column", "handoff", "rows", "note");
    size_t pool_columns = 0;
    for (size_t i = 0; i < result.columns.size(); ++i) {
        const rugo::ColumnOut& c = result.columns[i];
        const char* note = "";
        char detail[96] = "";
        if (c.direct_kind == rugo::DK_POOL) {
            ++pool_columns;
            // The IPC blob's first byte is its type tag. opteryx's C++
            // deserializer covers tags 1-5 and 12; everything else falls back to
            // Cython, so the tag decides whether a standalone converter can read
            // this column at all.
            std::lock_guard<std::mutex> lock(pool.mutex);
            if (c.ref_id >= 0 && static_cast<size_t>(c.ref_id) < pool.blobs.size()
                    && !pool.blobs[c.ref_id].empty()) {
                const uint8_t tag = pool.blobs[c.ref_id][0];
                const bool cpp_handles = (tag >= 1 && tag <= 5) || tag == 12;
                std::snprintf(detail, sizeof(detail), "IPC tag %u  %s", tag,
                              cpp_handles ? "<-- C++ deserializer HANDLES this"
                                          : "<-- Cython-only tag");
            } else {
                std::snprintf(detail, sizeof(detail), "no blob");
            }
            note = detail;
        }
        std::printf("%-20s %-14s %-10u %s\n",
                    i < result.column_names.size() ? result.column_names[i].c_str() : "?",
                    kind_name(c.direct_kind), c.length, note);
    }

    std::printf("\n%zu of %zu columns are DK_POOL\n", pool_columns, result.columns.size());
    if (pool_columns == 0)
        std::printf("Every column is a direct buffer — a standalone converter can read this file.\n");
    return 0;
}
