#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/vector.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace {

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

std::size_t require_rank(const Py_buffer& view, int expected_ndim, const char* name) {
    if (view.ndim != expected_ndim) {
        const std::string message = std::string(name) + " has incorrect rank";
        throw nb::value_error(message.c_str());
    }
    return static_cast<std::size_t>(view.shape[0]);
}

template <typename T>
const T* require_1d_buffer(
    nb::handle obj,
    BufferView& out,
    const char* name,
    const char* format = nullptr
) {
    acquire_buffer(obj, PyBUF_FORMAT | PyBUF_ND, out, "object does not support buffer protocol");
    require_c_contiguous(out.view, name);
    require_rank(out.view, 1, name);
    if (static_cast<std::size_t>(out.view.itemsize) != sizeof(T)) {
        const std::string message = std::string(name) + " has incorrect item size";
        throw nb::value_error(message.c_str());
    }
    if (format && (!out.view.format || std::string(out.view.format) != format)) {
        const std::string message = std::string(name) + " has incorrect dtype";
        throw nb::value_error(message.c_str());
    }
    return static_cast<const T*>(out.view.buf);
}

template <typename T>
const T* require_1d_buffer_untyped(nb::handle obj, BufferView& out, const char* name) {
    acquire_buffer(obj, PyBUF_ND, out, "object does not support buffer protocol");
    require_c_contiguous(out.view, name);
    require_rank(out.view, 1, name);
    if (static_cast<std::size_t>(out.view.itemsize) != sizeof(T)) {
        const std::string message = std::string(name) + " has incorrect item size";
        throw nb::value_error(message.c_str());
    }
    return static_cast<const T*>(out.view.buf);
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

double l2_norm(const float* values, std::size_t length) {
    double sum = 0.0;
    for (std::size_t i = 0; i < length; ++i) {
        const double value = static_cast<double>(values[i]);
        sum += value * value;
    }
    return std::sqrt(sum);
}

struct SearchResult {
    std::int64_t row_id;
    float score;
};

std::vector<float> score_cosine(nb::handle query_vector_obj, nb::handle vectors_obj) {
    BufferView query_view;
    BufferView vectors_view;

    const float* query = require_1d_buffer<float>(query_vector_obj, query_view, "query_vector", "f");

    std::size_t row_count = 0;
    std::size_t dims = 0;
    const float* vectors = require_2d_float32_buffer(vectors_obj, vectors_view, row_count, dims);

    const std::size_t query_dims = static_cast<std::size_t>(query_view.view.shape[0]);
    if (query_dims != dims) {
        throw nb::value_error("query_vector dimension does not match vectors");
    }

    const double query_norm = l2_norm(query, dims);
    if (query_norm == 0.0) {
        throw nb::value_error("query_vector norm must be non-zero");
    }

    std::vector<float> scores(row_count, -std::numeric_limits<float>::infinity());
    for (std::size_t row_idx = 0; row_idx < row_count; ++row_idx) {
        const float* row = vectors + (row_idx * dims);
        double dot = 0.0;
        double row_sq_sum = 0.0;
        for (std::size_t dim_idx = 0; dim_idx < dims; ++dim_idx) {
            const double row_value = static_cast<double>(row[dim_idx]);
            const double query_value = static_cast<double>(query[dim_idx]);
            dot += row_value * query_value;
            row_sq_sum += row_value * row_value;
        }

        const double row_norm = std::sqrt(row_sq_sum);
        if (row_norm != 0.0) {
            scores[row_idx] = static_cast<float>(dot / (query_norm * row_norm));
        }
    }

    return scores;
}

std::pair<std::vector<std::int64_t>, std::vector<float>> exact_search_cosine(
    nb::handle query_vector_obj,
    nb::handle row_ids_obj,
    nb::handle vectors_obj,
    std::size_t k
) {
    BufferView query_view;
    BufferView row_ids_view;
    BufferView vectors_view;

    const float* query = require_1d_buffer<float>(query_vector_obj, query_view, "query_vector", "f");
    const auto* row_ids = require_1d_buffer_untyped<std::int64_t>(row_ids_obj, row_ids_view, "row_ids");

    std::size_t row_count = 0;
    std::size_t dims = 0;
    const float* vectors = require_2d_float32_buffer(vectors_obj, vectors_view, row_count, dims);

    const std::size_t query_dims = static_cast<std::size_t>(query_view.view.shape[0]);
    const std::size_t row_id_count = static_cast<std::size_t>(row_ids_view.view.shape[0]);

    if (row_count != row_id_count) {
        throw nb::value_error("row_ids and vectors must have the same number of rows");
    }
    if (query_dims != dims) {
        throw nb::value_error("query_vector dimension does not match vectors");
    }
    if (k == 0 || row_count == 0) {
        return {};
    }

    if (k > row_count) {
        k = row_count;
    }

    const double query_norm = l2_norm(query, dims);
    if (query_norm == 0.0) {
        throw nb::value_error("query_vector norm must be non-zero");
    }

    std::vector<SearchResult> results;
    results.reserve(row_count);

    for (std::size_t row_idx = 0; row_idx < row_count; ++row_idx) {
        const float* row = vectors + (row_idx * dims);
        double dot = 0.0;
        double row_sq_sum = 0.0;
        for (std::size_t dim_idx = 0; dim_idx < dims; ++dim_idx) {
            const double row_value = static_cast<double>(row[dim_idx]);
            const double query_value = static_cast<double>(query[dim_idx]);
            dot += row_value * query_value;
            row_sq_sum += row_value * row_value;
        }

        const double row_norm = std::sqrt(row_sq_sum);
        float score = -std::numeric_limits<float>::infinity();
        if (row_norm != 0.0) {
            score = static_cast<float>(dot / (query_norm * row_norm));
        }
        results.push_back(SearchResult {.row_id = row_ids[row_idx], .score = score});
    }

    const auto cmp = [](const SearchResult& left, const SearchResult& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        return left.row_id < right.row_id;
    };

    std::partial_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(k), results.end(), cmp);
    results.resize(k);

    std::vector<std::int64_t> out_row_ids;
    std::vector<float> out_scores;
    out_row_ids.reserve(k);
    out_scores.reserve(k);
    for (const SearchResult& result : results) {
        out_row_ids.push_back(result.row_id);
        out_scores.push_back(result.score);
    }

    return {std::move(out_row_ids), std::move(out_scores)};
}

}  // namespace

NB_MODULE(vector_search, m) {
    m.def(
        "score_cosine",
        &score_cosine,
        nb::arg("query_vector"),
        nb::arg("vectors"),
        "Row-aligned cosine scores for a dense float32 matrix"
    );
    m.def(
        "exact_search_cosine",
        &exact_search_cosine,
        nb::arg("query_vector"),
        nb::arg("row_ids"),
        nb::arg("vectors"),
        nb::arg("k"),
        "Exact cosine top-k search over a dense float32 matrix"
    );
}
