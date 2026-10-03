// vector_index_carry.hpp — compaction's vector carry (docs/VECTOR_INDEX_DESIGN.md §5.6).
//
// Compaction never embeds (D-14). When it rewrites indexed files, each output's index is
// built from the vectors its inputs' index files already hold: the compaction writer records,
// for every output row, the (input file, input ordinal) it came from, and this
// re-clusters those vectors into the output's own vectors and centroids files. No model.
//
// Native end to end, called with the GIL released. Inputs are read row group by row group
// through SkeneRangedFile (local, gs:// with a bearer header, or a presigned URL), never whole:
//
//   pass 0  every input's `ordinal` column: which output rows have a vector (the carry
//           candidates), and the INVARIANT — every indexed input row that is not deleted
//           was written to exactly one output (a vector left behind is a row the compaction
//           lost, and fails it). Output rows with no vector are rows the inputs did not
//           index (null text, no defined cosine): they stay unindexed, as they were.
//   pass 1  the sampled candidates' vectors; train each output's centroids.
//   pass 2  per output, the inputs it draws from: every carried vector, under its OUTPUT
//           ordinal, through IvfFilesWriter — the write phase the embedding build uses.
//
// Deterministic: inputs are read in their given order and row groups in file order.

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/skene_ranged_file.hpp"
#include "engine/vector_index_build.hpp"   // IvfFilesWriter, VectorIndexBuildResult, LocalBodyStream

namespace opteryx::engine {

struct CarryInput {
    std::string           vectors;         // the input's vectors file: local path, gs:// or presigned URL
    uint64_t              vectors_bytes = 0;
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

// The decoded columns of one vectors-file row group.
struct VectorRows {
    const uint16_t* embedding = nullptr;
    const uint32_t* emb_sel = nullptr;
    const uint32_t* ordinal = nullptr;
    const uint32_t* ord_sel = nullptr;
    uint32_t        rows = 0;
};

inline bool column_of(const CxxMorsel& m, const char* name, const DrakenVector** out) {
    for (size_t i = 0; i < m.names.size(); ++i)
        if (m.names[i] == name) { *out = &m.columns[i].view; return true; }
    return false;
}

inline bool rows_of(const CxxMorsel& m, uint32_t dims, bool with_embedding, VectorRows* out, std::string* err) {
    const DrakenVector* ord = nullptr;
    if (!column_of(m, "ordinal", &ord) || ord->type != DRAKEN_UINT32) {
        *err = "vector index carry: an input vectors file has no UINT32 `ordinal` column";
        return false;
    }
    out->ordinal = static_cast<const uint32_t*>(ord->data);
    out->ord_sel = ord->selection;
    out->rows = ord->length;
    if (!with_embedding) return true;
    const DrakenVector* emb = nullptr;
    if (!column_of(m, "embedding", &emb) || emb->type != DRAKEN_VECTOR_FP16 || emb->length != ord->length) {
        *err = "vector index carry: an input vectors file has no fp16 `embedding` column";
        return false;
    }
    out->embedding = static_cast<const uint16_t*>(emb->data);
    out->emb_sel = emb->selection;
    (void)dims;
    return true;
}

}  // namespace carry_detail

// Re-cluster each output's carried vectors. `bodies[j]` receives output j's vectors body
// (its prefix lands in results[j].vectors_prefix); a result with `empty` set carried no
// vector and the caller abandons that body. Returns false with `err` on any failure,
// including a broken carry invariant.
inline bool carry_vector_index(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                               const std::vector<skene::OutputStream*>& bodies,
                               std::vector<VectorIndexBuildResult>* results, std::string* err) {
    using namespace carry_detail;
    const uint32_t n_in = static_cast<uint32_t>(spec.inputs.size());
    const uint32_t n_out = static_cast<uint32_t>(outputs.size());
    if (spec.dims == 0u || spec.flush_rows == 0u) { *err = "vector index carry: dims and flush_rows must be >= 1"; return false; }
    if (bodies.size() != n_out) { *err = "vector index carry: one body per output"; return false; }
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
                *err = "vector index carry: input row " + std::to_string(ord) + " of " + spec.inputs[k].vectors +
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
        SkeneRangedFile file;
        if (!file.open(in.vectors, in.vectors_bytes, {"ordinal"}, in.auth_header, err)) return false;
        for (uint32_t g = 0; g < file.row_groups(); ++g) {
            SkeneRangedFile::RowGroup rg;
            if (!file.read(g, &rg, err)) return false;
            VectorRows v;
            if (!rows_of(rg.morsel, spec.dims, false, &v, err)) return false;
            for (uint32_t i = 0; i < v.rows; ++i) {
                const uint32_t ord = v.ordinal[v.ord_sel[i]];
                const uint64_t at = mapped(k, ord);
                if (at == kUnmapped) {
                    if (!std::binary_search(in.deleted.begin(), in.deleted.end(), ord)) {
                        *err = "vector index carry: indexed row " + std::to_string(ord) + " of " + in.vectors +
                               " is live but was not written by the compaction; refusing to lose it";
                        return false;
                    }
                    continue;
                }
                candidates[at >> 32].push_back(static_cast<uint32_t>(at));
            }
        }
    }

    // ── Pass 1: the samples; train ──
    std::vector<draken::ann::IvfSamplePlan> plans(n_out);
    std::vector<std::unordered_map<uint32_t, uint32_t>> sample_at(n_out);
    std::vector<std::vector<uint16_t>> samples(n_out);
    for (uint32_t j = 0; j < n_out; ++j) {
        std::sort(candidates[j].begin(), candidates[j].end());
        if (std::adjacent_find(candidates[j].begin(), candidates[j].end()) != candidates[j].end()) {
            *err = "vector index carry: an input vectors file holds one ordinal twice";
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
        SkeneRangedFile file;
        if (!file.open(spec.inputs[k].vectors, spec.inputs[k].vectors_bytes, {"embedding", "ordinal"}, spec.inputs[k].auth_header, err)) return false;
        for (uint32_t g = 0; g < file.row_groups(); ++g) {
            SkeneRangedFile::RowGroup rg;
            if (!file.read(g, &rg, err)) return false;
            VectorRows v;
            if (!rows_of(rg.morsel, spec.dims, true, &v, err)) return false;
            for (uint32_t i = 0; i < v.rows; ++i) {
                const uint64_t at = mapped(k, v.ordinal[v.ord_sel[i]]);
                if (at == kUnmapped) continue;
                const uint32_t j = static_cast<uint32_t>(at >> 32);
                auto s = sample_at[j].find(static_cast<uint32_t>(at));
                if (s == sample_at[j].end()) continue;
                std::memcpy(samples[j].data() + static_cast<size_t>(s->second) * spec.dims,
                            v.embedding + static_cast<size_t>(v.emb_sel[i]) * spec.dims, spec.dims * 2u);
            }
        }
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
        IvfFilesWriter files(trained[j], spec.dims, spec.flush_rows);
        if (!files.begin(bodies[j], &out, err)) return false;
        for (uint32_t k = 0; k < n_in; ++k) {
            if (!feeds[j][k]) continue;
            SkeneRangedFile file;
            if (!file.open(spec.inputs[k].vectors, spec.inputs[k].vectors_bytes, {"embedding", "ordinal"}, spec.inputs[k].auth_header, err)) return false;
            for (uint32_t g = 0; g < file.row_groups(); ++g) {
                SkeneRangedFile::RowGroup rg;
                if (!file.read(g, &rg, err)) return false;
                VectorRows v;
                if (!rows_of(rg.morsel, spec.dims, true, &v, err)) return false;
                for (uint32_t i = 0; i < v.rows; ++i) {
                    const uint64_t at = mapped(k, v.ordinal[v.ord_sel[i]]);
                    if (at == kUnmapped || static_cast<uint32_t>(at >> 32) != j) continue;
                    const uint16_t* row = v.embedding + static_cast<size_t>(v.emb_sel[i]) * spec.dims;
                    if (!draken::ann::ann_row_searchable(row, spec.dims)) {
                        *err = "vector index carry: " + spec.inputs[k].vectors + " holds a vector with no defined cosine";
                        return false;
                    }
                    if (!files.add(row, static_cast<uint32_t>(at), err)) return false;
                }
            }
        }
        if (!files.finish(&out, err)) return false;
    }
    return true;
}

// ── Output targets: local files, or GCS resumable sessions ──

// Carry into local files: output j's vectors and centroids at `vectors[j]` / `centroids[j]`.
// An output that carried nothing gets neither file. On failure no output file is left.
inline bool carry_vector_index_local(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                                     const std::vector<std::string>& vectors,
                                     const std::vector<std::string>& centroids,
                                     std::vector<VectorIndexBuildResult>* results, std::string* err) {
    if (vectors.size() != outputs.size() || centroids.size() != outputs.size()) {
        *err = "vector index carry: one vectors and one centroids path per output";
        return false;
    }
    std::vector<std::unique_ptr<LocalBodyStream>> owned;
    std::vector<skene::OutputStream*> bodies;
    for (const auto& path : vectors) {
        owned.push_back(std::make_unique<LocalBodyStream>(path));
        if (!owned.back()->ok()) { *err = "vector index carry: cannot create " + owned.back()->partial; return false; }
        bodies.push_back(owned.back().get());
    }
    if (!carry_vector_index(spec, outputs, bodies, results, err)) return false;
    for (size_t j = 0; j < outputs.size(); ++j) {
        const VectorIndexBuildResult& r = (*results)[j];
        if (r.empty) continue;
        bool good = assemble_local(*owned[j], r.vectors_prefix, vectors[j], err) &&
                    write_local_file(centroids[j], r.centroids, err);
        if (!good) {
            for (size_t i = 0; i <= j; ++i) { std::remove(vectors[i].c_str()); std::remove(centroids[i].c_str()); }
            return false;
        }
    }
    return true;
}

// Carry into GCS: output j's vectors BODY streams into the open resumable session
// `sessions[j]`; the caller uploads each prefix and centroids file and composes. A session
// whose output carried nothing, or any session after a failure, is left unfinished.
inline bool carry_vector_index_to_sessions(const CarrySpec& spec, const std::vector<CarryOutput>& outputs,
                                           const std::vector<std::string>& sessions, size_t chunk_bytes,
                                           std::vector<VectorIndexBuildResult>* results, std::string* err) {
    if (sessions.size() != outputs.size()) { *err = "vector index carry: one session per output"; return false; }
    std::vector<std::unique_ptr<GcsResumableBody>> owned;
    std::vector<skene::OutputStream*> bodies;
    for (const auto& uri : sessions) {
        owned.push_back(std::make_unique<GcsResumableBody>(uri, chunk_bytes));
        if (!owned.back()->valid(err)) { *err = "vector index carry: " + *err; return false; }
        bodies.push_back(owned.back().get());
    }
    if (!carry_vector_index(spec, outputs, bodies, results, err)) return false;
    for (size_t j = 0; j < outputs.size(); ++j) {
        const VectorIndexBuildResult& r = (*results)[j];
        if (r.empty) continue;
        skene::Status st = owned[j]->finish();
        if (!st.is_ok()) { *err = "vector index carry: " + st.message(); return false; }
        if (owned[j]->committed() != r.vectors_body_bytes) {
            *err = "vector index carry: a session holds a different number of bytes than were written";
            return false;
        }
    }
    return true;
}

}  // namespace opteryx::engine
