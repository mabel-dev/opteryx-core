// vector_index_carry.hpp — compaction's vector carry (docs/VECTOR_INDEX_DESIGN.md §5.6).
//
// Compaction never embeds (D-14). When it rewrites indexed files, each output's index is
// built from the vectors its inputs' index files already hold: the compaction writer records,
// for every output row, the (input file, input ordinal) it came from, and this
// re-clusters those vectors into the output's own index file. No model.
//
// Native end to end, called with the GIL released. Inputs are read through VectorIndexFile
// (local, gs:// with a bearer header, or a presigned URL) block by block in large parallel
// range reads, never held whole:
//
//   pass 0  every input's ordinals: which output rows have a vector (the carry candidates),
//           and the INVARIANT — every indexed input row that is not deleted was written to
//           exactly one output (a vector left behind is a row the compaction lost, and fails
//           it). Output rows with no vector are rows the inputs did not index (null text, no
//           defined cosine): they stay unindexed, as they were.
//   pass 1  the sampled candidates' vectors; train each output's centroids.
//   pass 2  per output, the inputs it draws from: every carried vector, under its OUTPUT
//           ordinal, through IvfIndexWriter — the write phase the embedding build uses.
//
// Each pass reads its inputs' bodies whole (a block holds its ordinals and vectors
// together); the carry is maintenance, and bytes are cheap next to round trips.
// Deterministic: inputs are read in their given order and blocks in file order.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/vector_index_file.hpp"
#include "engine/vector_index_build.hpp"   // IvfIndexWriter, VectorIndexBuildResult, LocalIndexStream

namespace opteryx::engine {

struct CarryInput {
    std::string           path;            // the input's index file: local path, gs:// or presigned URL
    uint64_t              file_bytes = 0;
    uint64_t              footer_bytes = 0;
    std::string           auth_header;     // Authorization for its remote reads; empty = none
    std::vector<uint32_t> deleted;         // ascending: its deleted ordinals at plan time
};

struct CarryOutput {
    // One entry per output row, in output order (the output ordinal is the position): the
    // input (an index into CarrySpec::inputs) and that input's physical ordinal.
    std::vector<uint32_t> src_file;
    std::vector<uint32_t> src_ordinal;
};

struct CarrySpec {
    std::vector<CarryInput> inputs;
    uint32_t                dims = 0;
    draken::ann::IvfParams  ivf;              // clusters 0 = sqrt(carried rows), per output
    uint32_t                flush_rows = 512;
};

namespace carry_detail {

constexpr uint64_t kUnmapped = ~uint64_t{0};

inline bool open_input(const CarryInput& in, uint32_t dims, VectorIndexFile* file, std::string* err) {
    if (!file->open(in.path, in.file_bytes, in.footer_bytes, in.auth_header, err)) return false;
    if (file->dims() != dims) {
        *err = "vector index carry: " + in.path + " holds " + std::to_string(file->dims()) +
               "-dimensional vectors, the index is " + std::to_string(dims);
        return false;
    }
    return true;
}

}  // namespace carry_detail

// Re-cluster each output's carried vectors. `outs[j]` receives output j's index file; a
// result with `empty` set carried no vector and the caller abandons that stream. Returns
// false with `err` on any failure, including a broken carry invariant.
inline bool carry_vector_index(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                               const std::vector<skene::OutputStream*>& outs,
                               std::vector<VectorIndexBuildResult>* results, std::string* err) {
    using namespace carry_detail;
    const uint32_t n_in = static_cast<uint32_t>(spec.inputs.size());
    const uint32_t n_out = static_cast<uint32_t>(outputs.size());
    if (spec.dims == 0u || spec.flush_rows == 0u) { *err = "vector index carry: dims and flush_rows must be >= 1"; return false; }
    if (outs.size() != n_out) { *err = "vector index carry: one output stream per output"; return false; }
    results->assign(n_out, VectorIndexBuildResult());

    // ── The inverse of the writer's mapping: (input, ordinal) -> (output, output ordinal) ──
    std::vector<std::vector<uint64_t>> where(n_in);
    std::vector<std::vector<uint8_t>>  feeds(n_out, std::vector<uint8_t>(n_in, 0u));
    for (uint32_t j = 0; j < n_out; ++j) {
        const CarryOutput& o = outputs[j];
        if (o.src_file.size() != o.src_ordinal.size()) {
            *err = "vector index carry: an output's mapping arrays differ in length";
            return false;
        }
        for (size_t r = 0; r < o.src_file.size(); ++r) {
            const uint32_t k = o.src_file[r], ord = o.src_ordinal[r];
            if (k >= n_in) { *err = "vector index carry: a row names an input that was not given"; return false; }
            auto& w = where[k];
            if (ord >= w.size()) w.resize(static_cast<size_t>(ord) + 1u, kUnmapped);
            if (w[ord] != kUnmapped) {
                *err = "vector index carry: input row " + std::to_string(ord) + " of " + spec.inputs[k].path +
                       " was written twice";
                return false;
            }
            w[ord] = (static_cast<uint64_t>(j) << 32) | static_cast<uint32_t>(r);
            feeds[j][k] = 1u;
        }
    }
    auto mapped = [&](uint32_t k, uint32_t ord) {
        return ord < where[k].size() ? where[k][ord] : kUnmapped;
    };

    // ── Pass 0: the candidates, and the invariant ──
    std::vector<std::vector<uint32_t>> candidates(n_out);
    for (uint32_t k = 0; k < n_in; ++k) {
        const CarryInput& in = spec.inputs[k];
        VectorIndexFile file;
        if (!open_input(in, spec.dims, &file, err)) return false;
        VectorIndexReadStats stats;
        std::string lost;
        bool ok = file.for_each_block([&](const VectorIndexBlock& b) {
            if (!lost.empty()) return;
            for (uint32_t i = 0; i < b.rows; ++i) {
                const uint32_t ord = b.ordinals[i];
                const uint64_t at = mapped(k, ord);
                if (at == kUnmapped) {
                    if (!std::binary_search(in.deleted.begin(), in.deleted.end(), ord)) {
                        lost = "vector index carry: indexed row " + std::to_string(ord) + " of " + in.path +
                               " is live but was not written by the compaction; refusing to lose it";
                        return;
                    }
                    continue;
                }
                candidates[at >> 32].push_back(static_cast<uint32_t>(at));
            }
        }, &stats, err);
        if (!ok) return false;
        if (!lost.empty()) { *err = lost; return false; }
    }

    // ── Pass 1: the samples; train ──
    std::vector<draken::ann::IvfSamplePlan> plans(n_out);
    std::vector<std::unordered_map<uint32_t, uint32_t>> sample_at(n_out);
    std::vector<std::vector<uint16_t>> samples(n_out);
    for (uint32_t j = 0; j < n_out; ++j) {
        std::sort(candidates[j].begin(), candidates[j].end());
        if (std::adjacent_find(candidates[j].begin(), candidates[j].end()) != candidates[j].end()) {
            *err = "vector index carry: an input index file holds one ordinal twice";
            return false;
        }
        try {
            plans[j] = draken::ann::ivf_plan(candidates[j], spec.ivf);
        } catch (const std::exception& e) {
            *err = std::string("vector index carry: ") + e.what();
            return false;
        }
        std::vector<uint32_t>().swap(candidates[j]);
        for (uint32_t i = 0; i < plans[j].sample.size(); ++i) sample_at[j].emplace(plans[j].sample[i], i);
        samples[j].assign(plans[j].sample.size() * static_cast<size_t>(spec.dims), 0u);
    }
    for (uint32_t k = 0; k < n_in; ++k) {
        bool wanted = false;
        for (uint32_t j = 0; j < n_out; ++j) wanted |= feeds[j][k] && !plans[j].sample.empty();
        if (!wanted) continue;
        VectorIndexFile file;
        if (!open_input(spec.inputs[k], spec.dims, &file, err)) return false;
        VectorIndexReadStats stats;
        bool ok = file.for_each_block([&](const VectorIndexBlock& b) {
            for (uint32_t i = 0; i < b.rows; ++i) {
                const uint64_t at = mapped(k, b.ordinals[i]);
                if (at == kUnmapped) continue;
                const uint32_t j = static_cast<uint32_t>(at >> 32);
                auto s = sample_at[j].find(static_cast<uint32_t>(at));
                if (s == sample_at[j].end()) continue;
                std::memcpy(samples[j].data() + static_cast<size_t>(s->second) * spec.dims,
                            b.vectors + static_cast<size_t>(i) * spec.dims, spec.dims * 2u);
            }
        }, &stats, err);
        if (!ok) return false;
    }
    std::vector<draken::ann::IvfCentroids> trained(n_out);
    for (uint32_t j = 0; j < n_out; ++j) {
        const uint32_t m = static_cast<uint32_t>(plans[j].sample.size());
        if (m == 0u) continue;
        std::vector<uint32_t> identity(m);
        for (uint32_t i = 0; i < m; ++i) identity[i] = i;
        const draken::ann::Fp16Column sample{samples[j].data(), identity.data(), nullptr, m, spec.dims};
        try {
            trained[j] = draken::ann::ivf_train(sample, plans[j].clusters, spec.ivf);
        } catch (const std::exception& e) {
            *err = std::string("vector index carry: ") + e.what();
            return false;
        }
        std::vector<uint16_t>().swap(samples[j]);
    }

    // ── Pass 2: per output, every carried vector under its output ordinal ──
    for (uint32_t j = 0; j < n_out; ++j) {
        VectorIndexBuildResult& out = (*results)[j];
        if (trained[j].clusters == 0u) { out.empty = true; continue; }
        IvfIndexWriter files(trained[j], spec.dims, spec.flush_rows);
        files.begin(outs[j]);
        for (uint32_t k = 0; k < n_in; ++k) {
            if (!feeds[j][k]) continue;
            VectorIndexFile file;
            if (!open_input(spec.inputs[k], spec.dims, &file, err)) return false;
            VectorIndexReadStats stats;
            std::string failed;
            bool ok = file.for_each_block([&](const VectorIndexBlock& b) {
                if (!failed.empty()) return;
                for (uint32_t i = 0; i < b.rows; ++i) {
                    const uint64_t at = mapped(k, b.ordinals[i]);
                    if (at == kUnmapped || static_cast<uint32_t>(at >> 32) != j) continue;
                    const uint16_t* row = b.vectors + static_cast<size_t>(i) * spec.dims;
                    if (!draken::ann::ann_row_searchable(row, spec.dims)) {
                        failed = "vector index carry: " + spec.inputs[k].path + " holds a vector with no defined cosine";
                        return;
                    }
                    if (!files.add(row, static_cast<uint32_t>(at), &failed)) return;
                }
            }, &stats, err);
            if (!ok) return false;
            if (!failed.empty()) { *err = failed; return false; }
        }
        if (!files.finish(&out, err)) return false;
    }
    return true;
}

// ── Output targets: local files, or GCS resumable sessions ──

// Carry into local files: output j's index at `paths[j]`. An output that carried nothing
// gets no file. On failure no output file is left.
inline bool carry_vector_index_local(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                                     const std::vector<std::string>& paths,
                                     std::vector<VectorIndexBuildResult>* results, std::string* err) {
    if (paths.size() != outputs.size()) { *err = "vector index carry: one path per output"; return false; }
    std::vector<std::unique_ptr<LocalIndexStream>> owned;
    std::vector<skene::OutputStream*> outs;
    for (const auto& path : paths) {
        owned.push_back(std::make_unique<LocalIndexStream>(path));
        if (!owned.back()->ok()) { *err = "vector index carry: cannot create " + owned.back()->partial; return false; }
        outs.push_back(owned.back().get());
    }
    if (!carry_vector_index(spec, outputs, outs, results, err)) return false;
    for (size_t j = 0; j < outputs.size(); ++j) {
        if ((*results)[j].empty) continue;
        if (!owned[j]->commit(err)) {
            for (size_t i = 0; i < j; ++i) std::remove(paths[i].c_str());
            return false;
        }
    }
    return true;
}

// Carry into GCS: output j's index file streams into the open resumable session
// `sessions[j]`, finished here. A session whose output carried nothing, or any session after
// a failure, is left unfinished.
inline bool carry_vector_index_to_sessions(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                                           const std::vector<std::string>& sessions, size_t chunk_bytes,
                                           std::vector<VectorIndexBuildResult>* results, std::string* err) {
    if (sessions.size() != outputs.size()) { *err = "vector index carry: one session per output"; return false; }
    std::vector<std::unique_ptr<GcsResumableBody>> owned;
    std::vector<skene::OutputStream*> outs;
    for (const auto& uri : sessions) {
        owned.push_back(std::make_unique<GcsResumableBody>(uri, chunk_bytes));
        if (!owned.back()->valid(err)) { *err = "vector index carry: " + *err; return false; }
        outs.push_back(owned.back().get());
    }
    if (!carry_vector_index(spec, outputs, outs, results, err)) return false;
    for (size_t j = 0; j < outputs.size(); ++j) {
        const VectorIndexBuildResult& r = (*results)[j];
        if (r.empty) continue;
        skene::Status st = owned[j]->finish();
        if (!st.is_ok()) { *err = "vector index carry: " + st.message(); return false; }
        if (owned[j]->committed() != r.file_bytes) {
            *err = "vector index carry: a session holds a different number of bytes than were written";
            return false;
        }
    }
    return true;
}

}  // namespace opteryx::engine
