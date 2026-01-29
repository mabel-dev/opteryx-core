#include <Python.h>
#include <vector>
#include <string>
#include "simd_search.h"
#include "simd_string_ops.h"

// count_char(bytes, int) -> int
static PyObject* py_count_char(PyObject* self, PyObject* args) {
    PyObject* obj;
    int target;
    if (!PyArg_ParseTuple(args, "Oi", &obj, &target))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) != 0)
        return NULL;

    const char* data = (const char*) view.buf;
    size_t len = (size_t) view.len;
    size_t res = 0;
    // Dispatch to generic avx_count/neon_count through avx_count wrapper
    res = avx_count(data, len, (char)target);

    PyBuffer_Release(&view);
    return PyLong_FromSize_t(res);
}

// find_char(bytes, int) -> int
static PyObject* py_find_char(PyObject* self, PyObject* args) {
    PyObject* obj;
    int target;
    if (!PyArg_ParseTuple(args, "Oi", &obj, &target))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) != 0)
        return NULL;

    const char* data = (const char*) view.buf;
    size_t len = (size_t) view.len;
    int idx = avx_search(data, len, (char)target);

    PyBuffer_Release(&view);
    return PyLong_FromLong((long) idx);
}

// find_all_char(bytes, int) -> list[int]
static PyObject* py_find_all_char(PyObject* self, PyObject* args) {
    PyObject* obj;
    int target;
    if (!PyArg_ParseTuple(args, "Oi", &obj, &target))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) != 0)
        return NULL;

    const char* data = (const char*) view.buf;
    size_t len = (size_t) view.len;

    std::vector<size_t> results = simd_find_all(data, len, (char)target);

    PyObject* py_list = PyList_New(results.size());
    if (!py_list) {
        PyBuffer_Release(&view);
        return NULL;
    }

    for (size_t i = 0; i < results.size(); ++i) {
        PyObject* v = PyLong_FromSize_t(results[i]);
        if (!v) {
            Py_DECREF(py_list);
            PyBuffer_Release(&view);
            return NULL;
        }
        PyList_SET_ITEM(py_list, i, v); // steals reference
    }

    PyBuffer_Release(&view);
    return py_list;
}

// to_upper(bytes) -> bytes
static PyObject* py_to_upper(PyObject* self, PyObject* args) {
    PyObject* obj;
    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) != 0)
        return NULL;

    const char* data = (const char*) view.buf;
    Py_ssize_t len = view.len;

    PyObject* out = PyByteArray_FromStringAndSize(NULL, len);
    if (!out) {
        PyBuffer_Release(&view);
        return NULL;
    }

    char* out_ptr = PyByteArray_AsString(out);
    if (len > 0)
        memcpy(out_ptr, data, len);

    simd_to_upper(out_ptr, (size_t) len);

    // Convert to immutable bytes
    PyObject* res = PyBytes_FromStringAndSize(out_ptr, len);
    Py_DECREF(out);
    PyBuffer_Release(&view);
    return res;
}

// to_lower(bytes) -> bytes
static PyObject* py_to_lower(PyObject* self, PyObject* args) {
    PyObject* obj;
    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) != 0)
        return NULL;

    const char* data = (const char*) view.buf;
    Py_ssize_t len = view.len;

    PyObject* out = PyByteArray_FromStringAndSize(NULL, len);
    if (!out) {
        PyBuffer_Release(&view);
        return NULL;
    }

    char* out_ptr = PyByteArray_AsString(out);
    if (len > 0)
        memcpy(out_ptr, data, len);

    simd_to_lower(out_ptr, (size_t) len);

    PyObject* res = PyBytes_FromStringAndSize(out_ptr, len);
    Py_DECREF(out);
    PyBuffer_Release(&view);
    return res;
}

static PyMethodDef SimdStringsMethods[] = {
    {"count_char", py_count_char, METH_VARARGS, "Count occurrences of a byte"},
    {"find_char", py_find_char, METH_VARARGS, "Find first index of a byte (or -1)"},
    {"find_all_char", py_find_all_char, METH_VARARGS, "Return list of indices for byte occurrences"},
    {"to_upper", py_to_upper, METH_VARARGS, "Uppercase ASCII bytes via SIMD"},
    {"to_lower", py_to_lower, METH_VARARGS, "Lowercase ASCII bytes via SIMD"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef simdstringsmodule = {
    PyModuleDef_HEAD_INIT,
    "simd_strings",
    "SIMD string helpers (C++ backing)",
    -1,
    SimdStringsMethods
};

PyMODINIT_FUNC PyInit_simd_strings(void) {
    return PyModule_Create(&simdstringsmodule);
}
