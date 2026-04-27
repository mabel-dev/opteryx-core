#include "yyjson_wrapper.hpp"
#include "yyjson.h"
#include <cstring>
#include <cmath>

// Helper to convert yyjson_val to PyObject*
static PyObject* value_to_pyobject(yyjson_val* val, bool parse_objects) {
    if (yyjson_is_null(val)) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (yyjson_is_bool(val)) {
        return PyBool_FromLong(yyjson_get_bool(val) ? 1 : 0);
    }

    if (yyjson_is_uint(val)) {
        unsigned long long u = yyjson_get_uint(val);
        return PyLong_FromUnsignedLongLong(u);
    }

    if (yyjson_is_sint(val)) {
        long long s = yyjson_get_sint(val);
        return PyLong_FromLongLong(s);
    }

    if (yyjson_is_real(val)) {
        double d = yyjson_get_real(val);
        // Try to coerce to int if it's a whole number
        long long ll = (long long)d;
        if ((double)ll == d && d >= -9223372036854775808.0 && d < 9223372036854775808.0) {
            return PyLong_FromLongLong(ll);
        }
        return PyFloat_FromDouble(d);
    }

    if (yyjson_is_str(val)) {
        const char* str = yyjson_get_str(val);
        size_t len = yyjson_get_len(val);
        return PyUnicode_FromStringAndSize(str, len);
    }

    if (yyjson_is_arr(val)) {
        PyObject* list = PyList_New(0);
        if (!list) {
            PyErr_NoMemory();
            return nullptr;
        }

        yyjson_arr_iter iter = yyjson_arr_iter_with(val);
        yyjson_val* item = yyjson_arr_iter_next(&iter);
        while (item != nullptr) {
            PyObject* py_item = value_to_pyobject(item, parse_objects);
            if (!py_item) {
                Py_DECREF(list);
                return nullptr;
            }
            if (PyList_Append(list, py_item) == -1) {
                Py_DECREF(py_item);
                Py_DECREF(list);
                return nullptr;
            }
            Py_DECREF(py_item);
            item = yyjson_arr_iter_next(&iter);
        }
        return list;
    }

    if (yyjson_is_obj(val)) {
        PyObject* dict = PyDict_New();
        if (!dict) {
            PyErr_NoMemory();
            return nullptr;
        }

        yyjson_obj_iter iter = yyjson_obj_iter_with(val);
        yyjson_val* key_val = yyjson_obj_iter_next(&iter);
        while (key_val != nullptr) {
            const char* key = yyjson_get_str(key_val);
            size_t key_len = yyjson_get_len(key_val);
            yyjson_val* obj_val = yyjson_obj_iter_get_val(key_val);
            PyObject* py_val = value_to_pyobject(obj_val, parse_objects);
            if (!py_val) {
                Py_DECREF(dict);
                return nullptr;
            }
            // Use key_len to safely set the dict item
            int ret = PyDict_SetItemString(dict, key, py_val);
            Py_DECREF(py_val);
            if (ret == -1) {
                Py_DECREF(dict);
                return nullptr;
            }
            key_val = yyjson_obj_iter_next(&iter);
        }
        return dict;
    }

    PyErr_SetString(PyExc_RuntimeError, "yyjson wrapper: unknown json type");
    return nullptr;
}

extern "C" PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bool parse_objects) {
    // Parse JSON
    yyjson_read_err err;
    yyjson_doc* doc = yyjson_read_opts((char*)data, len, 0, nullptr, &err);

    if (doc == nullptr) {
        PyErr_SetString(PyExc_RuntimeError, "yyjson parse error");
        return nullptr;
    }

    yyjson_val* root = yyjson_doc_get_root(doc);
    if (root == nullptr) {
        yyjson_doc_free(doc);
        PyErr_SetString(PyExc_RuntimeError, "yyjson: failed to get root");
        return nullptr;
    }

    PyObject* result = value_to_pyobject(root, parse_objects);
    yyjson_doc_free(doc);
    return result;
}
