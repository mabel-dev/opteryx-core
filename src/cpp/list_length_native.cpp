#include <Python.h>
#include <stdint.h>

// offsets_to_lengths using the Python buffer protocol and returning a
// bytearray of uint32 values (native endianness). This removes the
// dependency on NumPy for this hot path while keeping the API flexible
// (accepts any object that supports the buffer protocol, e.g., NumPy
// arrays or PyArrow buffers).
static PyObject * offsets_to_lengths(PyObject *self, PyObject *args) {
    PyObject *offsets_obj = NULL;
    if (!PyArg_ParseTuple(args, "O", &offsets_obj))
        return NULL;

    Py_buffer view;
    if (PyObject_GetBuffer(offsets_obj, &view, PyBUF_SIMPLE) != 0) {
        PyErr_SetString(PyExc_TypeError, "object does not support buffer protocol");
        return NULL;
    }

    // offsets must be int32, so buffer length must be multiple of 4
    if (view.len % sizeof(int32_t) != 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offsets buffer has invalid length");
        return NULL;
    }

    size_t num_offsets = view.len / sizeof(int32_t);
    if (num_offsets < 2) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offsets must have length >= 2");
        return NULL;
    }

    size_t res_len = num_offsets - 1;
    // Create a bytearray of res_len * 4 bytes to hold uint32_t results
    PyObject *out = PyByteArray_FromStringAndSize(NULL, res_len * sizeof(uint32_t));
    if (!out) {
        PyBuffer_Release(&view);
        return NULL;
    }

    int32_t *in = (int32_t *) view.buf;
    uint32_t *outp = (uint32_t *) PyByteArray_AsString(out);

    for (size_t i = 0; i < res_len; ++i) {
        outp[i] = (uint32_t) (in[i + 1] - in[i]);
    }

    PyBuffer_Release(&view);
    return out;
}

static PyObject * offsets_to_lengths_into(PyObject *self, PyObject *args) {
    PyObject *offsets_obj = NULL;
    PyObject *out_obj = NULL;
    if (!PyArg_ParseTuple(args, "OO", &offsets_obj, &out_obj))
        return NULL;

    Py_buffer view_in;
    if (PyObject_GetBuffer(offsets_obj, &view_in, PyBUF_SIMPLE) != 0) {
        PyErr_SetString(PyExc_TypeError, "offsets object does not support buffer protocol");
        return NULL;
    }

    Py_buffer view_out;
    if (PyObject_GetBuffer(out_obj, &view_out, PyBUF_WRITABLE) != 0) {
        PyBuffer_Release(&view_in);
        PyErr_SetString(PyExc_TypeError, "output object is not writable or does not support buffer protocol");
        return NULL;
    }

    if (view_in.len % sizeof(int32_t) != 0) {
        PyBuffer_Release(&view_in);
        PyBuffer_Release(&view_out);
        PyErr_SetString(PyExc_ValueError, "offsets buffer has invalid length");
        return NULL;
    }

    size_t num_offsets = view_in.len / sizeof(int32_t);
    if (num_offsets < 2) {
        PyBuffer_Release(&view_in);
        PyBuffer_Release(&view_out);
        PyErr_SetString(PyExc_ValueError, "offsets must have length >= 2");
        return NULL;
    }

    size_t res_len = num_offsets - 1;
    if ((size_t)view_out.len != res_len * sizeof(uint32_t)) {
        Py_ssize_t in_len = view_in.len;
        Py_ssize_t out_len = view_out.len;
        PyBuffer_Release(&view_in);
        PyBuffer_Release(&view_out);
        PyErr_Format(PyExc_ValueError, "output buffer has incorrect size (in_len=%zd, out_len=%zd, expected_out=%zu)", in_len, out_len, res_len * sizeof(uint32_t));
        return NULL;
    }

    int32_t *in = (int32_t *) view_in.buf;
    uint32_t *outp = (uint32_t *) view_out.buf;

    for (size_t i = 0; i < res_len; ++i) {
        outp[i] = (uint32_t) (in[i + 1] - in[i]);
    }

    PyBuffer_Release(&view_in);
    PyBuffer_Release(&view_out);

    Py_INCREF(out_obj);
    return out_obj;
}

static PyMethodDef ListLengthMethods[] = {
    {"offsets_to_lengths", (PyCFunction) offsets_to_lengths, METH_VARARGS, "Convert int32 offsets -> uint32 lengths (returns bytearray)"},
    {"offsets_to_lengths_into", (PyCFunction) offsets_to_lengths_into, METH_VARARGS, "Convert int32 offsets -> uint32 lengths into a provided writable buffer"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef list_length_module = {
    PyModuleDef_HEAD_INIT,
    "list_length",
    NULL,
    -1,
    ListLengthMethods
};

PyMODINIT_FUNC PyInit_list_length(void) {
    return PyModule_Create(&list_length_module);
}
