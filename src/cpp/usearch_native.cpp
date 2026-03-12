#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <usearch/index_dense.hpp>

namespace nb = nanobind;

namespace {

using DenseIndex = unum::usearch::index_dense_gt<std::int64_t>;
using unum::usearch::index_limits_t;
using unum::usearch::metric_kind_t;
using unum::usearch::metric_punned_t;
using unum::usearch::scalar_kind_t;

struct BufferView {
    Py_buffer view {};
    bool acquired = false;

    ~BufferView() {
        if (acquired) {
            PyBuffer_Release(&view);
        }
    }

    BufferView(const BufferView&) = delete;
    BufferView& operator=(const BufferView&) = delete;
    BufferView() = default;
};

void acquire_buffer(nb::handle obj, int flags, BufferView& out, const char* err_msg) {
    if (PyObject_GetBuffer(obj.ptr(), &out.view, flags) != 0) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, err_msg);
        }
        throw nb::python_error();
    }
    out.acquired = true;
}

void require_c_contiguous(const Py_buffer& view, const char* name) {
    if (!PyBuffer_IsContiguous(&view, 'C')) {
        const std::string message = std::string(name) + " must be C-contiguous";
        throw nb::value_error(message.c_str());
    }
}

template <typename T>
const T* require_1d_buffer_untyped(nb::handle obj, BufferView& out, const char* name, std::size_t& count) {
    acquire_buffer(obj, PyBUF_ND, out, "object does not support buffer protocol");
    require_c_contiguous(out.view, name);
    if (out.view.ndim != 1) {
        const std::string message = std::string(name) + " must be rank 1";
        throw nb::value_error(message.c_str());
    }
    if (static_cast<std::size_t>(out.view.itemsize) != sizeof(T)) {
        const std::string message = std::string(name) + " has incorrect item size";
        throw nb::value_error(message.c_str());
    }
    count = static_cast<std::size_t>(out.view.shape[0]);
    return static_cast<const T*>(out.view.buf);
}

const float* require_1d_float32_buffer(nb::handle obj, BufferView& out, const char* name, std::size_t& count) {
    acquire_buffer(obj, PyBUF_FORMAT | PyBUF_ND, out, "object does not support buffer protocol");
    require_c_contiguous(out.view, name);
    if (out.view.ndim != 1) {
        const std::string message = std::string(name) + " must be rank 1";
        throw nb::value_error(message.c_str());
    }
    if (static_cast<std::size_t>(out.view.itemsize) != sizeof(float)) {
        const std::string message = std::string(name) + " must contain float32 values";
        throw nb::value_error(message.c_str());
    }
    if (!out.view.format || std::string(out.view.format) != "f") {
        const std::string message = std::string(name) + " must contain float32 values";
        throw nb::value_error(message.c_str());
    }
    count = static_cast<std::size_t>(out.view.shape[0]);
    return static_cast<const float*>(out.view.buf);
}

const float* require_2d_float32_buffer(nb::handle obj, BufferView& out, std::size_t& rows, std::size_t& dims) {
    acquire_buffer(obj, PyBUF_FORMAT | PyBUF_ND, out, "object does not support buffer protocol");
    require_c_contiguous(out.view, "vectors");
    if (out.view.ndim != 2) {
        throw nb::value_error("vectors must be rank 2");
    }
    if (static_cast<std::size_t>(out.view.itemsize) != sizeof(float)) {
        throw nb::value_error("vectors must contain float32 values");
    }
    if (!out.view.format || std::string(out.view.format) != "f") {
        throw nb::value_error("vectors must contain float32 values");
    }
    rows = static_cast<std::size_t>(out.view.shape[0]);
    dims = static_cast<std::size_t>(out.view.shape[1]);
    return static_cast<const float*>(out.view.buf);
}

metric_kind_t parse_metric(std::string const& metric_name) {
    if (metric_name == "cos") {
        return metric_kind_t::cos_k;
    }
    if (metric_name == "l2sq") {
        return metric_kind_t::l2sq_k;
    }
    if (metric_name == "ip") {
        return metric_kind_t::ip_k;
    }
    throw nb::value_error("unsupported metric; expected one of: cos, l2sq, ip");
}

template <typename result_at>
void raise_if_error(result_at& result) {
    if (!result) {
        char const* message = result.error.release();
        throw std::runtime_error(message ? message : "USearch operation failed");
    }
}

class UsearchIndex {
  public:
    UsearchIndex(
        std::size_t dimensions,
        std::size_t capacity = 0,
        std::string metric_name = "cos",
        std::size_t expansion_add = 0,
        std::size_t expansion_search = 0
    )
        : dimensions_(dimensions) {

        if (dimensions_ == 0) {
            throw nb::value_error("dimensions must be positive");
        }

        metric_kind_t metric_kind = parse_metric(metric_name);
        metric_punned_t metric(dimensions_, metric_kind, scalar_kind_t::f32_k);
        unum::usearch::index_dense_config_t config;
        config.expansion_add = expansion_add;
        config.expansion_search = expansion_search;

        auto result = DenseIndex::make(metric, config);
        raise_if_error(result);
        index_ = std::move(result.index);

        if (capacity > 0) {
            reserve(capacity);
        }
    }

    void reserve(std::size_t capacity) {
        index_.reserve(index_limits_t {capacity, 1});
    }

    void add(std::int64_t key, nb::handle vector_obj) {
        BufferView vector_view;
        std::size_t dims = 0;
        const float* vector = require_1d_float32_buffer(vector_obj, vector_view, "vector", dims);
        if (dims != dimensions_) {
            throw nb::value_error("vector dimension does not match index dimensions");
        }
        auto result = index_.add(key, vector);
        raise_if_error(result);
    }

    void add_batch(nb::handle row_ids_obj, nb::handle vectors_obj) {
        BufferView row_ids_view;
        BufferView vectors_view;

        std::size_t row_count = 0;
        std::size_t vectors_rows = 0;
        std::size_t dims = 0;
        const auto* row_ids = require_1d_buffer_untyped<std::int64_t>(row_ids_obj, row_ids_view, "row_ids", row_count);
        const float* vectors = require_2d_float32_buffer(vectors_obj, vectors_view, vectors_rows, dims);

        if (row_count != vectors_rows) {
            throw nb::value_error("row_ids and vectors must have the same number of rows");
        }
        if (dims != dimensions_) {
            throw nb::value_error("vectors dimension does not match index dimensions");
        }

        for (std::size_t row_idx = 0; row_idx != row_count; ++row_idx) {
            auto result = index_.add(row_ids[row_idx], vectors + (row_idx * dims));
            raise_if_error(result);
        }
    }

    std::pair<std::vector<std::int64_t>, std::vector<float>> search(
        nb::handle query_vector_obj,
        std::size_t k,
        bool exact = false
    ) const {
        BufferView query_view;
        std::size_t dims = 0;
        const float* query = require_1d_float32_buffer(query_vector_obj, query_view, "query_vector", dims);

        if (dims != dimensions_) {
            throw nb::value_error("query_vector dimension does not match index dimensions");
        }
        if (k == 0) {
            return {};
        }

        auto result = index_.search(query, k, DenseIndex::any_thread(), exact);
        raise_if_error(result);

        std::size_t found = result.size();
        std::vector<std::int64_t> keys(found);
        std::vector<float> distances(found);
        result.dump_to(keys.data(), distances.data(), found);

        std::vector<std::pair<std::int64_t, float>> pairs;
        pairs.reserve(found);
        for (std::size_t i = 0; i != found; ++i) {
            pairs.emplace_back(keys[i], distances[i]);
        }
        std::sort(
            pairs.begin(),
            pairs.end(),
            [](std::pair<std::int64_t, float> const& left, std::pair<std::int64_t, float> const& right) {
                if (left.second != right.second) {
                    return left.second < right.second;
                }
                return left.first < right.first;
            }
        );
        for (std::size_t i = 0; i != found; ++i) {
            keys[i] = pairs[i].first;
            distances[i] = pairs[i].second;
        }

        return {std::move(keys), std::move(distances)};
    }

    std::size_t size() const { return index_.size(); }
    std::size_t capacity() const { return index_.capacity(); }
    std::size_t dimensions() const { return dimensions_; }
    std::size_t memory_usage() const { return index_.memory_usage(); }

  private:
    DenseIndex index_;
    std::size_t dimensions_ = 0;
};

}  // namespace

NB_MODULE(usearch_native, m) {
    nb::class_<UsearchIndex>(m, "UsearchIndex")
        .def(
            nb::init<std::size_t, std::size_t, std::string, std::size_t, std::size_t>(),
            nb::arg("dimensions"),
            nb::arg("capacity") = 0,
            nb::arg("metric") = "cos",
            nb::arg("expansion_add") = 0,
            nb::arg("expansion_search") = 0
        )
        .def("reserve", &UsearchIndex::reserve, nb::arg("capacity"))
        .def("add", &UsearchIndex::add, nb::arg("key"), nb::arg("vector"))
        .def("add_batch", &UsearchIndex::add_batch, nb::arg("row_ids"), nb::arg("vectors"))
        .def("search", &UsearchIndex::search, nb::arg("query_vector"), nb::arg("k"), nb::arg("exact") = false)
        .def("size", &UsearchIndex::size)
        .def("capacity", &UsearchIndex::capacity)
        .def("dimensions", &UsearchIndex::dimensions)
        .def("memory_usage", &UsearchIndex::memory_usage);
}
