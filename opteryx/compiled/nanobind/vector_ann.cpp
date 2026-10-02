// vector_ann.cpp — Python surface over draken/ops/ann (fp16 cosine IVF-flat + exact top-k).
//
// Test and measurement surface only (docs/VECTOR_INDEX_DESIGN.md Stage B): the engine's
// index scan calls draken::ann directly from C++. Bitmaps are 1-bit-per-row `bytes`
// (bit i of byte i>>3), indexed by row ordinal — the same layout as DrakenVector validity.
// uint32 arrays travel as little-endian `bytes`. The GIL is released for builds and searches.
//
//   ann_ivf_build(vectors, excluded, clusters, iterations, sample_per_cluster, threads, seed)
//       -> (centroids_fp16: bytes, order_u32: bytes, offsets_u32: bytes)
//   ann_ivf_search(vectors, centroids, order, offsets, query, k, nprobe, excluded, admitted)
//       -> (ordinals, distances)
//   ann_exact_topk(vectors, query, k, excluded, admitted) -> (ordinals, distances)
//   ann_ivf_stream(vectors, excluded, clusters, iterations, sample_per_cluster, threads, seed,
//                  flush_rows) -> (centroids_fp16: bytes, [(cluster, ordinals_u32: bytes)])
//       the streaming build: plan over the CANDIDATE rows (valid, not excluded — what a
//       build knows before embedding), train on the sample, assign row by row through a
//       ClusterStream. The blocks are what become the vectors file's row groups.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/buffers.h"
#include "core/draken_bridge.h"
#include "fp16/fp16.h"
#include "ops/ann/fp16_cosine_ivf.h"

namespace nb = nanobind;

namespace {

draken::ann::Fp16Column ann_column(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    PyObject* raw = PyObject_GetAttrString(obj.ptr(), "logical_type_dimension");
    if (!raw) throw nb::python_error();
    nb::object dim_obj = nb::steal<nb::object>(raw);
    if (dim_obj.is_none())
        throw nb::type_error((std::string(fn) + ": expected a VECTOR_FP16 Vector with its dimension").c_str());
    const long dim = PyLong_AsLong(dim_obj.ptr());
    if (dim == -1L && PyErr_Occurred()) throw nb::python_error();
    return draken::ann::Fp16Column::of(*dv, static_cast<uint32_t>(dim));
}

// nullptr for None; otherwise a bitmap that must cover every ordinal.
const uint8_t* ann_bitmap(nb::object obj, uint32_t rows, const char* what) {
    if (obj.is_none()) return nullptr;
    if (!nb::isinstance<nb::bytes>(obj)) throw nb::type_error((std::string(what) + " must be bytes or None").c_str());
    nb::bytes b = nb::borrow<nb::bytes>(obj);
    if (b.size() < (static_cast<size_t>(rows) + 7u) / 8u)
        throw std::invalid_argument(std::string(what) + " is shorter than one bit per row");
    return reinterpret_cast<const uint8_t*>(b.c_str());
}

std::vector<uint16_t> ann_query(const std::vector<double>& query, uint32_t dims) {
    if (query.size() != dims)
        throw std::invalid_argument("query length " + std::to_string(query.size()) +
                                    " does not match the vector dimension " + std::to_string(dims));
    std::vector<uint16_t> q(dims);
    for (uint32_t i = 0; i < dims; ++i) q[i] = fp16_ieee_from_fp32_value(static_cast<float>(query[i]));
    return q;
}

std::vector<uint32_t> u32_of(nb::bytes b, const char* what) {
    if (b.size() % sizeof(uint32_t) != 0u)
        throw std::invalid_argument(std::string(what) + " is not a whole number of uint32");
    std::vector<uint32_t> out(b.size() / sizeof(uint32_t));
    std::memcpy(out.data(), b.c_str(), b.size());
    return out;
}

template <typename T>
nb::bytes bytes_of(const std::vector<T>& v) {
    return nb::bytes(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(T));
}

std::pair<std::vector<uint32_t>, std::vector<double>> ann_split(const std::vector<draken::ann::AnnHit>& hits) {
    std::pair<std::vector<uint32_t>, std::vector<double>> out;
    out.first.reserve(hits.size());
    out.second.reserve(hits.size());
    for (const auto& h : hits) { out.first.push_back(h.ordinal); out.second.push_back(h.distance); }
    return out;
}

}  // namespace

void register_vector_ann(nb::module_& m) {
    m.def("ann_ivf_build",
        [](nb::object vectors, nb::object excluded, uint32_t clusters, uint32_t iterations,
           uint32_t sample_per_cluster, uint32_t threads, uint64_t seed) {
            const auto column = ann_column(vectors, "ann_ivf_build");
            const uint8_t* ex = ann_bitmap(excluded, column.rows, "excluded");
            draken::ann::IvfParams params;
            params.clusters = clusters;
            params.iterations = iterations;
            params.sample_per_cluster = sample_per_cluster;
            params.threads = threads;
            params.seed = seed;
            draken::ann::IvfModel model;
            {
                nb::gil_scoped_release release;
                model = draken::ann::ivf_build(column, ex, params);
            }
            return std::make_tuple(bytes_of(model.centroids), bytes_of(model.order), bytes_of(model.offsets));
        },
        nb::arg("vectors"), nb::arg("excluded").none(), nb::arg("clusters") = 0,
        nb::arg("iterations") = 8, nb::arg("sample_per_cluster") = 64, nb::arg("threads") = 1,
        nb::arg("seed") = 0x5EEDC0DEull);

    m.def("ann_ivf_stream",
        [](nb::object vectors, nb::object excluded, uint32_t clusters, uint32_t iterations,
           uint32_t sample_per_cluster, uint32_t threads, uint64_t seed, uint32_t flush_rows) {
            const auto column = ann_column(vectors, "ann_ivf_stream");
            const uint8_t* ex = ann_bitmap(excluded, column.rows, "excluded");
            draken::ann::IvfParams params;
            params.clusters = clusters;
            params.iterations = iterations;
            params.sample_per_cluster = sample_per_cluster;
            params.threads = threads;
            params.seed = seed;
            draken::ann::IvfCentroids trained;
            std::vector<std::pair<uint32_t, std::vector<uint32_t>>> blocks;
            {
                nb::gil_scoped_release release;
                std::vector<uint32_t> candidates;
                for (uint32_t r = 0; r < column.rows; ++r)
                    if (column.valid(r) && (ex == nullptr || !draken::ann::ann_bit(ex, r)))
                        candidates.push_back(r);
                const auto plan = draken::ann::ivf_plan(candidates, params);
                std::vector<uint32_t> sel(plan.sample.size());
                for (size_t i = 0; i < sel.size(); ++i) sel[i] = column.selection[plan.sample[i]];
                const draken::ann::Fp16Column sample{column.data, sel.data(), nullptr,
                                                     static_cast<uint32_t>(sel.size()), column.dims};
                trained = draken::ann::ivf_train(sample, plan.clusters, params);
                if (trained.clusters > 0u) {
                    auto emit = [&](uint32_t c, const uint32_t* ords, const uint16_t*, uint32_t n) {
                        blocks.emplace_back(c, std::vector<uint32_t>(ords, ords + n));
                    };
                    draken::ann::ClusterStream stream(trained.clusters, column.dims, flush_rows);
                    for (uint32_t r : candidates) {
                        if (!draken::ann::ann_row_searchable(column.row(r), column.dims)) continue;
                        stream.add(draken::ann::ivf_assign(trained, column.row(r)), r, column.row(r), emit);
                    }
                    stream.finish(emit);
                }
            }
            std::vector<std::pair<uint32_t, nb::bytes>> out;
            for (const auto& [c, ords] : blocks) out.emplace_back(c, bytes_of(ords));
            return std::make_pair(bytes_of(trained.centroids), out);
        },
        nb::arg("vectors"), nb::arg("excluded").none(), nb::arg("clusters"), nb::arg("iterations"),
        nb::arg("sample_per_cluster"), nb::arg("threads"), nb::arg("seed"), nb::arg("flush_rows"));

    m.def("ann_ivf_search",
        [](nb::object vectors, nb::bytes centroids, nb::bytes order, nb::bytes offsets,
           std::vector<double> query, uint32_t k, uint32_t nprobe, nb::object excluded,
           nb::object admitted) {
            const auto column = ann_column(vectors, "ann_ivf_search");
            const uint8_t* ex = ann_bitmap(excluded, column.rows, "excluded");
            const uint8_t* adm = ann_bitmap(admitted, column.rows, "admitted");
            const auto q = ann_query(query, column.dims);
            draken::ann::IvfModel model;
            model.dims = column.dims;
            model.order = u32_of(order, "order");
            model.offsets = u32_of(offsets, "offsets");
            if (model.offsets.empty() || centroids.size() % (static_cast<size_t>(column.dims) * 2u) != 0u)
                throw std::invalid_argument("centroids / offsets do not describe a model of this dimension");
            model.clusters = static_cast<uint32_t>(model.offsets.size() - 1u);
            if (centroids.size() != static_cast<size_t>(model.clusters) * column.dims * 2u ||
                model.offsets.back() != model.order.size())
                throw std::invalid_argument("centroids, order and offsets are inconsistent");
            for (uint32_t r : model.order)
                if (r >= column.rows) throw std::invalid_argument("order references a row past the vectors");
            model.centroids.resize(static_cast<size_t>(model.clusters) * column.dims);
            std::memcpy(model.centroids.data(), centroids.c_str(), centroids.size());
            std::vector<draken::ann::AnnHit> hits;
            {
                nb::gil_scoped_release release;
                hits = draken::ann::ivf_search(model, column, q.data(), k, nprobe, ex, adm);
            }
            return ann_split(hits);
        },
        nb::arg("vectors"), nb::arg("centroids"), nb::arg("order"), nb::arg("offsets"),
        nb::arg("query"), nb::arg("k"), nb::arg("nprobe"), nb::arg("excluded").none() = nb::none(),
        nb::arg("admitted").none() = nb::none());

    m.def("ann_exact_topk",
        [](nb::object vectors, std::vector<double> query, uint32_t k, nb::object excluded,
           nb::object admitted) {
            const auto column = ann_column(vectors, "ann_exact_topk");
            const uint8_t* ex = ann_bitmap(excluded, column.rows, "excluded");
            const uint8_t* adm = ann_bitmap(admitted, column.rows, "admitted");
            const auto q = ann_query(query, column.dims);
            std::vector<draken::ann::AnnHit> hits;
            {
                nb::gil_scoped_release release;
                hits = draken::ann::exact_topk(column, q.data(), k, ex, adm);
            }
            return ann_split(hits);
        },
        nb::arg("vectors"), nb::arg("query"), nb::arg("k"), nb::arg("excluded").none() = nb::none(),
        nb::arg("admitted").none() = nb::none());
}
