#include <Python.h>
#include <nanobind/nanobind.h>
#include <stdint.h>

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

inline void acquire_buffer(nb::handle obj, int flags, BufferView& out, const char* err_msg) {
    if (PyObject_GetBuffer(obj.ptr(), &out.view, flags) != 0) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, err_msg);
        }
        throw nb::python_error();
    }
    out.acquired = true;
}

inline size_t validate_offsets_length(Py_ssize_t len_bytes) {
    if (len_bytes % static_cast<Py_ssize_t>(sizeof(int32_t)) != 0) {
        throw nb::value_error("offsets buffer has invalid length");
    }

    size_t num_offsets = static_cast<size_t>(len_bytes) / sizeof(int32_t);
    if (num_offsets < 2) {
        throw nb::value_error("offsets must have length >= 2");
    }

    return num_offsets;
}

inline void compute_lengths(const int32_t* in, uint32_t* out, size_t n_out) {
    for (size_t i = 0; i < n_out; ++i) {
        out[i] = static_cast<uint32_t>(in[i + 1] - in[i]);
    }
}

nb::bytearray offsets_to_lengths(nb::object offsets_obj) {
    BufferView view_in;
    acquire_buffer(offsets_obj, PyBUF_SIMPLE, view_in, "object does not support buffer protocol");

    size_t num_offsets = validate_offsets_length(view_in.view.len);
    size_t res_len = num_offsets - 1;

    PyObject* out_obj = PyByteArray_FromStringAndSize(nullptr, static_cast<Py_ssize_t>(res_len * sizeof(uint32_t)));
    if (!out_obj) {
        throw nb::python_error();
    }

    nb::bytearray out = nb::steal<nb::bytearray>(out_obj);
    auto* in = static_cast<const int32_t*>(view_in.view.buf);
    auto* outp = reinterpret_cast<uint32_t*>(PyByteArray_AsString(out.ptr()));
    compute_lengths(in, outp, res_len);

    return out;
}

nb::object offsets_to_lengths_into(nb::object offsets_obj, nb::object out_obj) {
    BufferView view_in;
    BufferView view_out;

    acquire_buffer(offsets_obj, PyBUF_SIMPLE, view_in, "offsets object does not support buffer protocol");
    acquire_buffer(out_obj, PyBUF_WRITABLE, view_out, "output object is not writable or does not support buffer protocol");

    size_t num_offsets = validate_offsets_length(view_in.view.len);
    size_t res_len = num_offsets - 1;
    size_t expected_out_len = res_len * sizeof(uint32_t);

    if (static_cast<size_t>(view_out.view.len) != expected_out_len) {
        throw nb::value_error("output buffer has incorrect size");
    }

    auto* in = static_cast<const int32_t*>(view_in.view.buf);
    auto* outp = static_cast<uint32_t*>(view_out.view.buf);
    compute_lengths(in, outp, res_len);

    return out_obj;
}

} // namespace

NB_MODULE(list_length, m) {
    m.def(
        "offsets_to_lengths",
        &offsets_to_lengths,
        "Convert int32 offsets -> uint32 lengths (returns bytearray)"
    );
    m.def(
        "offsets_to_lengths_into",
        &offsets_to_lengths_into,
        "Convert int32 offsets -> uint32 lengths into a provided writable buffer"
    );
}
