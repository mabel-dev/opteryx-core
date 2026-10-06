// vector_index_build.hpp — build ONE data file's vector index (docs/VECTOR_INDEX_DESIGN.md
// §5.2, §10). Native end to end: no Python, and called with the GIL released.
//
// Input: a parquet data file, its indexed text column, the file's deleted ordinals
// (decoded at plan time, as for every scan), the registered `draken_embed` kernel and the
// IVF parameters. Output: ONE index file (vector_index_file.hpp) streamed to the caller's
// OutputStream - blocks, footer, tail, in that order - and the sizes the catalog commit
// records.
//
// Two passes over the text column, through rugo's ParquetIOPipeline and the native scan's
// own string decoder (NativeScanColumnBuilder) — one decoder, shared with the scan:
//
//   pass 1  ivf_plan picks K and a seeded sample from the CANDIDATE ordinals (every
//           physical row not deleted). Only the sampled rows are decoded (a row mask per
//           row group) and embedded; ivf_train runs k-means on them.
//   pass 2  every row group in order, deleted rows masked out: embed, ivf_assign, and hand
//           the row to a ClusterStream. Each block it emits — one cluster's rows, at most
//           `flush_rows` of them — is one block of the index file.
//
// Memory is bounded by K x flush_rows embedded rows plus the row groups in flight, not by
// the file. Sampled rows are embedded twice (in pass 1 and again in pass 2, ~64 x K rows,
// about 3% of a 5.8M-row file): the stored vector of every row is the pass-2 one.
//
// Ordinals are PHYSICAL positions in the data file, numbered before deletes (the address
// space of delete vectors and MERGE). Rows that are null, deleted, or whose embedding has
// no defined cosine (zero or non-finite) are not indexed.
//
// Deterministic: the same file, deletes, model and parameters give the same files, for any
// number of embed threads — row groups are processed in file order and embedded in fixed
// batches whatever the thread count.

#pragma once

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io_pipeline.hpp"                    // rugo::ParquetIOPipeline, MorselRef
#include "filesystem.hpp"                     // rugo::FetchParquetFooter (local or remote)
#include "metadata.hpp"                       // rugo FileStats / ReadParquetMetadataFromBuffer
#include "memory_pool.hpp"                    // opteryx::MemoryPool
#include "pool_sink_adapter.hpp"              // wire_pool_sink
#include "engine/native_parquet_scan_source.hpp"  // NativeScanColumnBuilder
#include "core/alloc.h"                       // draken_free
#include "morsels/cxx_morsel.h"               // CxxMorsel, CxxColumn
#include "ops/ann/fp16_cosine_ivf.h"          // ivf_plan / ivf_train / ivf_assign / ClusterStream
#include "ops/kernels/kernel_context.h"       // vector_dim_ctx
#include "ops/string_gather.h"                // str_slice
#include "ops/vec_result.h"                   // VecResult
#include "engine/vector_index_file.hpp"      // VectorIndexFileWriter (and skene::OutputStream)
#include "engine/gcs_resumable_body.hpp"      // GcsResumableBody

namespace opteryx::engine {

using EmbedFn = VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs);

struct VectorIndexBuildSpec {
    // The parquet data file: a local path, or a gs:// object read with `auth_header` (a
    // bearer token minted once by the caller; it is not refreshed during the build).
    std::string           data_path;
    std::string           auth_header;         // Authorization for remote reads; empty = none
    int64_t               data_bytes = -1;     // its size (the manifest's); -1 = stat it (local)
    std::string           column;              // the indexed text column
    std::vector<uint32_t> deleted;             // ascending physical ordinals
    EmbedFn               embed = nullptr;     // the registered draken_embed kernel
    uint32_t              dims = 0;            // its declared width
    draken::ann::IvfParams ivf;                // clusters 0 = sqrt(rows to index)
    uint32_t              flush_rows = 512;    // rows per index-file block, at most
    // Rows per embedding call. ONE, measured (2026-10-02, MiniLM, NVD text): batching pads
    // every row to the batch's longest, and the working set of a call grows with
    // batch x sequence^2 — at 64 rows x 12 threads it reached +10 GiB. Batch 1 was the
    // fastest on both targets (M5: 1207 vs 417 rows/s at 64; i5-8500: 356 vs 134).
    uint32_t              embed_batch = 1;
    uint32_t              embed_threads = 1;
    uint32_t              decode_workers = 2;
    // Pass 2 writes a progress line to stderr at most this often (rows embedded of the
    // total, rate, time left): a build of millions of rows runs for hours and was otherwise
    // silent until it ended. 0 = silent.
    uint32_t              progress_seconds = 60;
};

struct VectorIndexBuildResult {
    bool                  empty = true;        // no indexable row: no file was written
    uint64_t              file_bytes = 0;      // the index file, whole
    uint64_t              footer_bytes = 0;    // its footer (recorded, so a search opens it in one read)
    uint64_t              rows_indexed = 0;
    uint32_t              clusters = 0;
    uint32_t              blocks = 0;
    // The billed size (§5.5): the format is its own decoded form, so this is the file.
    uint64_t              logical_bytes = 0;
};

namespace vib_detail {

// A kernel's VecResult, freed per vec_result.h's ownership contract.
struct OwnedVecResult {
    VecResult r;
    explicit OwnedVecResult(VecResult v) : r(v) {}
    OwnedVecResult(const OwnedVecResult&) = delete;
    OwnedVecResult& operator=(const OwnedVecResult&) = delete;
    ~OwnedVecResult() {
        if (r.data == nullptr) return;
        draken_free(r.data);
        if (r.validity != nullptr && r.validity_embedded == 0u) draken_free(r.validity);
        if (r.owns_selection) draken_free(const_cast<uint32_t*>(r.selection));
        if (r.arena != nullptr) draken_free(r.arena);
    }
    DrakenVector view() const {
        DrakenVector v{};
        v.data = r.data;
        v.selection = r.selection;
        v.data_length = r.data_length;
        v.length = r.length;
        v.validity = r.validity;
        v.type = r.type;
        v.flags = r.flags;
        return v;
    }
};

inline bool bit(const uint8_t* bits, uint32_t i) { return (bits[i >> 3] >> (i & 7u)) & 1u; }

// Embed rows [0, n) of `text` into out (n x dims fp16) and ok (1 = has an embedding), in
// fixed batches of `embed_batch` rows spread over `threads` threads. Each batch is sliced into a
// compact string vector of its own: the kernel embeds every PHYSICAL value of its operand,
// so it must never see the whole row group.
inline bool embed_rows(const VectorIndexBuildSpec& spec, const DrakenVector& text, uint32_t n,
                       uint16_t* out, uint8_t* ok, std::string* err) {
    const uint32_t batch = spec.embed_batch;
    const uint32_t batches = (n + batch - 1u) / batch;
    const uint32_t threads = std::max(1u, std::min(spec.embed_threads, batches));
    std::vector<std::string> errors(threads);
    auto work = [&](uint32_t t) {
        vector_dim_ctx ctx{spec.dims};
        for (uint32_t b = t; b < batches; b += threads) {
            const uint32_t start = b * batch;
            const uint32_t count = std::min(batch, n - start);
            OwnedVecResult slice(draken::ops::str_slice(text, start, count));
            if (slice.r.data == nullptr) {
                errors[t] = slice.r.error_msg ? slice.r.error_msg : "string slice failed";
                return;
            }
            const DrakenVector sv = slice.view();
            const DrakenVector* args[1] = {&sv};
            OwnedVecResult emb(spec.embed(&ctx, args, 1u));
            if (emb.r.data == nullptr) {
                errors[t] = emb.r.error_msg ? emb.r.error_msg : "embedding failed";
                return;
            }
            if (emb.r.type != DRAKEN_VECTOR_FP16 || emb.r.vec_dimension != spec.dims ||
                emb.r.length != count) {
                errors[t] = "the embedding kernel returned the wrong type, width or row count";
                return;
            }
            const uint16_t* data = static_cast<const uint16_t*>(emb.r.data);
            for (uint32_t i = 0; i < count; ++i) {
                const bool valid = emb.r.validity == nullptr || bit(emb.r.validity, i);
                ok[start + i] = valid ? 1u : 0u;
                uint16_t* dst = out + static_cast<size_t>(start + i) * spec.dims;
                if (valid)
                    std::memcpy(dst, data + static_cast<size_t>(emb.r.selection[i]) * spec.dims,
                                spec.dims * sizeof(uint16_t));
                else
                    std::memset(dst, 0, spec.dims * sizeof(uint16_t));
            }
        }
    };
    if (threads == 1u) {
        work(0);
    } else {
        std::vector<std::thread> pool;
        for (uint32_t t = 0; t < threads; ++t) pool.emplace_back(work, t);
        for (auto& th : pool) th.join();
    }
    for (const auto& e : errors)
        if (!e.empty()) { *err = "vector index build: " + e; return false; }
    return true;
}

// Decodes one text column of one data file, row group by row group, through the pipeline.
struct TextReader {
    std::string                         path;
    std::vector<std::string>            names;
    std::vector<ColumnStats>      chunk;      // per row group, the column's chunk
    std::unique_ptr<MemoryPool>         pool;
    std::unique_ptr<rugo::ParquetIOPipeline> pipeline;
    std::vector<uint8_t>                varchar_flag{1};
    std::vector<int>                    string_type{DRAKEN_VARCHAR};
    NativeScanColumnBuilder             builder{};

    TextReader(const std::string& p, const std::string& auth_header, const std::string& column,
               std::vector<ColumnStats> c, uint32_t workers)
        : path(p), names{column}, chunk(std::move(c)) {
        pool = std::make_unique<MemoryPool>(int64_t{64} << 20, "vector index build", true);
        pipeline = std::make_unique<rugo::ParquetIOPipeline>(static_cast<int>(workers), 64u);
        if (!auth_header.empty()) pipeline->set_auth_header(auth_header);
        wire_pool_sink(pipeline.get(), pool.get());
        builder.pool = pool.get();
        builder.varchar_columns = &varchar_flag;
        builder.string_types = &string_type;
    }

    void submit(uint32_t rg, const std::vector<uint8_t>& mask) {
        if (mask.empty())
            pipeline->submit_row_group(path, static_cast<int>(rg), names, {chunk[rg]});
        else
            pipeline->submit_row_group(path, static_cast<int>(rg), names, {chunk[rg]}, mask);
    }

    // The next finished row group (any order): its index and its decoded text column.
    bool next(uint32_t* rg, CxxColumn* text, std::string* err) {
        rugo::MorselRef result;
        if (!pipeline->wait_and_get_result(result)) {
            *err = "vector index build: the parquet pipeline drained with row groups missing";
            return false;
        }
        if (!result.success) {
            *err = "vector index build: " + (result.error.empty() ? std::string("decode failed") : result.error);
            return false;
        }
        if (result.columns.size() != 1u) {
            *err = "vector index build: the decoded row group does not hold exactly the text column";
            return false;
        }
        ErrCtx ec;
        if (!builder.build_column(result, 0, *text, ec)) {
            *err = std::string("vector index build: ") + (ec.msg ? ec.msg : "cannot decode the text column");
            return false;
        }
        *rg = static_cast<uint32_t>(result.rg_idx);
        return true;
    }
};

}  // namespace vib_detail

// The write phase every index build shares — the embedding build here and compaction's
// carry (vector_index_carry.hpp): rows arrive one at a time with their physical ordinal,
// each is assigned to its nearest trained centroid and handed to a ClusterStream, whose
// blocks (one cluster's rows, at most flush_rows) become the index file's blocks, streamed
// to the caller's OutputStream. `finish` writes the footer and the sizes the commit records.
class IvfIndexWriter {
  public:
    IvfIndexWriter(const draken::ann::IvfCentroids& trained, uint32_t dims, uint32_t flush_rows)
        : trained_(trained), dims_(dims), stream_(trained.clusters, dims, flush_rows) {}

    void begin(skene::OutputStream* out) {
        file_ = std::make_unique<VectorIndexFileWriter>(out, trained_.centroids.data(), trained_.clusters, dims_);
    }

    // One searchable row (the caller has checked ann_row_searchable).
    bool add(const uint16_t* row, uint32_t ordinal, std::string* err) {
        stream_.add(draken::ann::ivf_assign(trained_, row), ordinal, row, emitter());
        ++indexed_;
        if (!emit_err_.empty()) { *err = emit_err_; return false; }
        return true;
    }

    uint64_t indexed() const noexcept { return indexed_; }

    // Flush, write the footer, report the sizes. With no row added, nothing is written and
    // `out->empty` is set: the caller abandons the stream.
    bool finish(VectorIndexBuildResult* out, std::string* err) {
        if (indexed_ == 0u) { out->empty = true; return true; }
        stream_.finish(emitter());
        if (!emit_err_.empty()) { *err = emit_err_; return false; }
        VectorIndexFileSizes sizes;
        if (!file_->finish(&sizes, err)) return false;
        out->empty = false;
        out->file_bytes = sizes.file_bytes;
        out->footer_bytes = sizes.footer_bytes;
        out->rows_indexed = sizes.rows;
        out->clusters = sizes.clusters;
        out->blocks = sizes.blocks;
        out->logical_bytes = sizes.file_bytes;
        return true;
    }

  private:
    struct Emitter {
        IvfIndexWriter* self;
        void operator()(uint32_t c, const uint32_t* ordinals, const uint16_t* vectors, uint32_t n) const {
            if (self->emit_err_.empty()) self->file_->add_block(c, ordinals, vectors, n, &self->emit_err_);
        }
    };
    Emitter emitter() { return Emitter{this}; }

    const draken::ann::IvfCentroids&         trained_;
    uint32_t                                 dims_;
    draken::ann::ClusterStream               stream_;
    std::unique_ptr<VectorIndexFileWriter>   file_;
    uint64_t                                 indexed_ = 0;
    std::string                              emit_err_;
};

// Build the index of one data file. Returns false with `err` set on any failure; nothing
// has then been promised — the caller abandons the stream.
inline bool build_vector_index_file(const VectorIndexBuildSpec& spec, skene::OutputStream* body,
                                    VectorIndexBuildResult* out, std::string* err) {
    using namespace vib_detail;
    if (spec.embed == nullptr || spec.dims == 0u) {
        *err = "vector index build: no embedding kernel";
        return false;
    }
    if (spec.flush_rows == 0u || spec.embed_batch == 0u || spec.embed_threads == 0u ||
        spec.decode_workers == 0u) {
        *err = "vector index build: flush_rows, embed_batch, embed_threads and decode_workers "
               "must all be >= 1";
        return false;
    }

    // ── The footer: row groups, and the text column's chunk in each ──
    FileStats fs;
    try {
        // The manifest records every file's size, so a remote file's is given, never HEADed.
        if (spec.data_path.find("://") != std::string::npos && spec.data_bytes <= 0) {
            *err = "vector index build: a remote data file needs its size (data_bytes)";
            return false;
        }
        if (spec.data_path.rfind("gs://", 0) == 0 && spec.auth_header.empty()) {
            *err = "vector index build: " + spec.data_path + " is a gs:// object and no Authorization header was given";
            return false;
        }
        std::vector<rugo::ParquetFooterResult> footers =
            rugo::FetchParquetFootersMany({spec.data_path}, {spec.data_bytes}, spec.auth_header);
        const rugo::ParquetFooterResult& footer = footers[0];
        fs = ReadParquetMetadataFromBuffer(footer.envelope.data(), footer.envelope.size());
    } catch (const std::exception& e) {
        *err = std::string("vector index build: cannot read the footer of ") + spec.data_path + ": " + e.what();
        return false;
    }
    const uint32_t row_groups = static_cast<uint32_t>(fs.row_groups.size());
    std::vector<ColumnStats> chunk;
    std::vector<uint32_t> first_row(row_groups + 1u, 0u);
    int64_t nulls = 0;
    for (uint32_t g = 0; g < row_groups; ++g) {
        const auto& rg = fs.row_groups[g];
        const ColumnStats* found = nullptr;
        for (const auto& c : rg.columns)
            if (c.name == spec.column) { found = &c; break; }
        if (found == nullptr) {
            *err = "vector index build: " + spec.data_path + " has no column '" + spec.column + "'";
            return false;
        }
        if (found->physical_type != "byte_array") {
            *err = "vector index build: column '" + spec.column + "' of " + spec.data_path +
                   " is " + found->physical_type + ", not text";
            return false;
        }
        chunk.push_back(*found);
        nulls += found->null_count > 0 ? found->null_count : 0;
        if (rg.num_rows < 0 || static_cast<uint64_t>(first_row[g]) + rg.num_rows > UINT32_MAX) {
            *err = "vector index build: " + spec.data_path + " has more rows than a uint32 ordinal addresses";
            return false;
        }
        first_row[g + 1u] = first_row[g] + static_cast<uint32_t>(rg.num_rows);
    }
    const uint32_t total = first_row[row_groups];
    for (size_t i = 0; i < spec.deleted.size(); ++i)
        if (spec.deleted[i] >= total || (i > 0 && spec.deleted[i] <= spec.deleted[i - 1])) {
            *err = "vector index build: deleted ordinals must be ascending, unique and inside the file";
            return false;
        }

    // Per row group: 1 = keep, for the not-deleted rows (empty = keep every row).
    auto keep_mask = [&](uint32_t g) {
        std::vector<uint8_t> mask;
        auto lo = std::lower_bound(spec.deleted.begin(), spec.deleted.end(), first_row[g]);
        auto hi = std::lower_bound(spec.deleted.begin(), spec.deleted.end(), first_row[g + 1u]);
        if (lo == hi) return mask;
        mask.assign(first_row[g + 1u] - first_row[g], 1u);
        for (auto it = lo; it != hi; ++it) mask[*it - first_row[g]] = 0u;
        return mask;
    };

    // ── Pass 1: plan, decode + embed the sample, train ──
    std::vector<uint32_t> candidates;
    candidates.reserve(total - spec.deleted.size());
    for (uint32_t r = 0, d = 0; r < total; ++r) {
        if (d < spec.deleted.size() && spec.deleted[d] == r) { ++d; continue; }
        candidates.push_back(r);
    }
    draken::ann::IvfParams params = spec.ivf;
    if (params.clusters == 0u) {
        // K from the rows that will be indexed: the footer's null count stands in for the
        // rows whose text is null (deleted nulls make it a slight over-count).
        const int64_t rows = std::max<int64_t>(1, static_cast<int64_t>(candidates.size()) - nulls);
        params.clusters = static_cast<uint32_t>(std::lround(std::sqrt(static_cast<double>(rows))));
    }
    draken::ann::IvfSamplePlan plan;
    try {
        plan = draken::ann::ivf_plan(candidates, params);
    } catch (const std::exception& e) {
        *err = std::string("vector index build: ") + e.what();
        return false;
    }
    std::vector<uint32_t>().swap(candidates);
    if (plan.clusters == 0u) { out->empty = true; return true; }

    std::unordered_map<uint32_t, uint32_t> sample_at;   // ordinal -> position in the plan
    sample_at.reserve(plan.sample.size());
    for (uint32_t i = 0; i < plan.sample.size(); ++i) sample_at.emplace(plan.sample[i], i);
    const uint32_t m = static_cast<uint32_t>(plan.sample.size());
    std::vector<uint16_t> sample_vectors(static_cast<size_t>(m) * spec.dims, 0u);
    std::vector<uint8_t>  sample_valid((m + 7u) / 8u, 0u);
    {
        std::vector<std::vector<uint32_t>> by_group(row_groups);
        for (uint32_t ordinal : plan.sample) {
            const uint32_t g = static_cast<uint32_t>(
                std::upper_bound(first_row.begin(), first_row.end(), ordinal) - first_row.begin()) - 1u;
            by_group[g].push_back(ordinal);
        }
        TextReader reader(spec.data_path, spec.auth_header, spec.column, chunk, spec.decode_workers);
        uint32_t pending = 0;
        for (uint32_t g = 0; g < row_groups; ++g) {
            if (by_group[g].empty()) continue;
            std::sort(by_group[g].begin(), by_group[g].end());
            std::vector<uint8_t> mask(first_row[g + 1u] - first_row[g], 0u);
            for (uint32_t ordinal : by_group[g]) mask[ordinal - first_row[g]] = 1u;
            reader.submit(g, mask);
            ++pending;
        }
        for (; pending > 0; --pending) {
            uint32_t g = 0;
            CxxColumn text;
            if (!reader.next(&g, &text, err)) return false;
            const auto& ordinals = by_group[g];
            if (text.view.length != ordinals.size()) {
                *err = "vector index build: the sampled decode returned the wrong row count";
                return false;
            }
            const uint32_t n = text.view.length;
            std::vector<uint16_t> vecs(static_cast<size_t>(n) * spec.dims);
            std::vector<uint8_t> ok(n);
            if (!embed_rows(spec, text.view, n, vecs.data(), ok.data(), err)) return false;
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t at = sample_at.at(ordinals[i]);
                std::memcpy(sample_vectors.data() + static_cast<size_t>(at) * spec.dims,
                            vecs.data() + static_cast<size_t>(i) * spec.dims, spec.dims * 2u);
                if (ok[i]) sample_valid[at >> 3] |= static_cast<uint8_t>(1u << (at & 7u));
            }
        }
    }
    std::vector<uint32_t> identity(m);
    for (uint32_t i = 0; i < m; ++i) identity[i] = i;
    const draken::ann::Fp16Column sample{sample_vectors.data(), identity.data(), sample_valid.data(),
                                         m, spec.dims};
    draken::ann::IvfCentroids trained;
    try {
        trained = draken::ann::ivf_train(sample, plan.clusters, params);
    } catch (const std::exception& e) {
        *err = std::string("vector index build: ") + e.what();
        return false;
    }
    std::vector<uint16_t>().swap(sample_vectors);
    if (trained.clusters == 0u) { out->empty = true; return true; }

    // ── Pass 2: every row group, in file order ──
    IvfIndexWriter files(trained, spec.dims, spec.flush_rows);
    files.begin(body);

    TextReader reader(spec.data_path, spec.auth_header, spec.column, chunk, spec.decode_workers);
    std::vector<std::vector<uint8_t>> masks(row_groups);
    const uint32_t window = std::max(2u, spec.decode_workers * 2u);
    uint32_t submitted = 0;
    auto top_up = [&](uint32_t upto) {
        for (; submitted < row_groups && submitted < upto; ++submitted) {
            masks[submitted] = keep_mask(submitted);
            reader.submit(submitted, masks[submitted]);
        }
    };
    top_up(window);
    std::unordered_map<uint32_t, CxxColumn> arrived;   // decoded ahead of their turn
    // Progress: every row not deleted is embedded once in this pass.
    const uint64_t to_embed = static_cast<uint64_t>(first_row[row_groups]) - spec.deleted.size();
    uint64_t embedded = 0;
    const auto pass_start = std::chrono::steady_clock::now();
    auto last_report = pass_start;
    auto report = [&](const char* state) {
        const double secs =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - pass_start).count();
        const double rate = secs > 0.0 ? static_cast<double>(embedded) / secs : 0.0;
        const double left = rate > 0.0 ? static_cast<double>(to_embed - embedded) / rate : 0.0;
        std::fprintf(stderr,
                     "vector index build %s (%s): %s %llu / %llu rows (%.1f%%), %.0f rows/s, "
                     "%.0f min left\n",
                     spec.data_path.c_str(), spec.column.c_str(), state,
                     static_cast<unsigned long long>(embedded),
                     static_cast<unsigned long long>(to_embed),
                     to_embed ? 100.0 * static_cast<double>(embedded) / static_cast<double>(to_embed)
                              : 100.0,
                     rate, left / 60.0);
        std::fflush(stderr);
    };
    for (uint32_t g = 0; g < row_groups; ++g) {
        while (arrived.find(g) == arrived.end()) {
            uint32_t got = 0;
            CxxColumn text;
            if (!reader.next(&got, &text, err)) return false;
            arrived.emplace(got, std::move(text));
        }
        CxxColumn text = std::move(arrived.at(g));
        arrived.erase(g);
        top_up(g + 1u + window);

        // The physical ordinals of the decoded rows: the row group's kept rows, in order.
        std::vector<uint32_t> ordinals;
        ordinals.reserve(first_row[g + 1u] - first_row[g]);
        for (uint32_t r = first_row[g]; r < first_row[g + 1u]; ++r)
            if (masks[g].empty() || masks[g][r - first_row[g]]) ordinals.push_back(r);
        std::vector<uint8_t>().swap(masks[g]);
        if (text.view.length != ordinals.size()) {
            *err = "vector index build: a row group decoded to the wrong row count";
            return false;
        }
        const uint32_t n = text.view.length;
        std::vector<uint16_t> vecs(static_cast<size_t>(n) * spec.dims);
        std::vector<uint8_t> ok(n);
        if (!embed_rows(spec, text.view, n, vecs.data(), ok.data(), err)) return false;
        for (uint32_t i = 0; i < n; ++i) {
            const uint16_t* row = vecs.data() + static_cast<size_t>(i) * spec.dims;
            if (!ok[i] || !draken::ann::ann_row_searchable(row, spec.dims)) continue;
            if (!files.add(row, ordinals[i], err)) return false;
        }
        embedded += n;
        if (spec.progress_seconds != 0u) {
            const auto now = std::chrono::steady_clock::now();
            if (now - last_report >= std::chrono::seconds(spec.progress_seconds)) {
                last_report = now;
                report("embedding");
            }
        }
    }
    if (spec.progress_seconds != 0u) report("embedded");
    return files.finish(out, err);
}

// ── Local files (development and local catalogs) ──

// Streams an index file to `<path>.partial`; `commit` renames it into place. A stream that
// is not committed leaves nothing behind.
struct LocalIndexStream final : skene::OutputStream {
    std::FILE*  f = nullptr;
    std::string path;
    std::string partial;
    explicit LocalIndexStream(const std::string& p) : path(p), partial(p + ".partial") {
        f = std::fopen(partial.c_str(), "wb");
    }
    ~LocalIndexStream() { discard(); }
    bool ok() const { return f != nullptr; }
    skene::Status write(const void* data, size_t n) override {
        if (std::fwrite(data, 1, n, f) != n)
            return skene::Status(skene::Code::kMalformed, "cannot write " + partial + ": " + std::strerror(errno));
        return skene::Status::ok();
    }
    bool commit(std::string* err) {
        const bool closed = std::fclose(f) == 0;
        f = nullptr;
        if (!closed || std::rename(partial.c_str(), path.c_str()) != 0) {
            *err = "cannot write " + path + ": " + std::strerror(errno);
            discard();
            return false;
        }
        partial.clear();
        return true;
    }
    void discard() {
        if (f != nullptr) { std::fclose(f); f = nullptr; }
        if (!partial.empty()) { std::remove(partial.c_str()); partial.clear(); }
    }
};

// Build one data file's index into the local file `path`. On failure, or when the file has
// nothing to index (`out->empty`), no file exists afterwards.
inline bool build_vector_index_file_local(const VectorIndexBuildSpec& spec, const std::string& path,
                                          VectorIndexBuildResult* out, std::string* err) {
    LocalIndexStream file(path);
    if (!file.ok()) { *err = "vector index build: cannot create " + file.partial; return false; }
    if (!build_vector_index_file(spec, &file, out, err)) return false;
    if (out->empty) return true;
    return file.commit(err);
}

// ── GCS: the file into an open resumable session ──

// Build one data file's index, streaming the whole file into the resumable upload session
// `session_uri` (opened by the control plane, which holds the credentials). On success the
// session is finished and the object exists. On failure, or when there is nothing to index,
// the session is left unfinished and never becomes an object.
inline bool build_vector_index_file_to_session(const VectorIndexBuildSpec& spec,
                                               const std::string& session_uri, size_t chunk_bytes,
                                               VectorIndexBuildResult* out, std::string* err) {
    GcsResumableBody body(session_uri, chunk_bytes);
    if (!body.valid(err)) { *err = "vector index build: " + *err; return false; }
    if (!build_vector_index_file(spec, &body, out, err)) return false;
    if (out->empty) return true;
    skene::Status st = body.finish();
    if (!st.is_ok()) { *err = "vector index build: " + st.message(); return false; }
    if (body.committed() != out->file_bytes) {
        *err = "vector index build: the session holds a different number of bytes than were written";
        return false;
    }
    return true;
}

}  // namespace opteryx::engine
