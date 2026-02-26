#pragma once

#include <vector>
#include <cstdint>
#include <Python.h>
#include "../../third_party/eyalz800/zpp_bits/zpp_bits.h"

namespace opteryx {
namespace aggregations {

/**
 * Serialize a Python list of key components to binary using zpp_bits.
 * Single-pass, zero-copy serialization for use in aggregation hot paths.
 * 
 * Handles: None, bool, int, float, str, bytes
 * Returns bytes object suitable for use as dict key.
 */
inline std::vector<uint8_t> serialize_key_components_zpp(PyObject* py_list) {
    std::vector<uint8_t> result;
    auto [in, out] = zpp::bits::in_out(result);
    
    Py_ssize_t list_size = PyList_Size(py_list);
    if (list_size < 0) {
        throw std::runtime_error("Failed to get list size");
    }
    
    // Write component count
    out(static_cast<uint32_t>(list_size)).or_throw();
    
    for (Py_ssize_t i = 0; i < list_size; ++i) {
        PyObject* item = PyList_GetItem(py_list, i);
        if (!item) {
            throw std::runtime_error("Failed to get list item");
        }
        
        // Type dispatch and serialization
        if (item == Py_None) {
            uint8_t type_tag = 0;  // NULL
            out(type_tag).or_throw();
        } 
        else if (PyBool_Check(item)) {
            uint8_t type_tag = 3;  // BOOL
            bool value = (item == Py_True);
            out(type_tag, value).or_throw();
        }
        else if (PyLong_Check(item)) {
            uint8_t type_tag = 1;  // INT64
            long long value = PyLong_AsLongLong(item);
            if (value == -1 && PyErr_Occurred()) {
                throw std::runtime_error("Failed to convert int to int64");
            }
            out(type_tag, static_cast<int64_t>(value)).or_throw();
        }
        else if (PyFloat_Check(item)) {
            uint8_t type_tag = 2;  // FLOAT64
            double value = PyFloat_AsDouble(item);
            if (value == -1.0 && PyErr_Occurred()) {
                throw std::runtime_error("Failed to convert float to double");
            }
            out(type_tag, value).or_throw();
        }
        else if (PyUnicode_Check(item)) {
            uint8_t type_tag = 4;  // BYTES (UTF-8 string)
            const char* utf8_str = PyUnicode_AsUTF8(item);
            if (!utf8_str) {
                throw std::runtime_error("Failed to convert string to UTF-8");
            }
            std::string str(utf8_str);
            out(type_tag, zpp::bits::sized<std::uint32_t>(str)).or_throw();
        }
        else if (PyBytes_Check(item)) {
            uint8_t type_tag = 4;  // BYTES
            char* bytes_ptr = nullptr;
            Py_ssize_t bytes_len = 0;
            if (PyBytes_AsStringAndSize(item, &bytes_ptr, &bytes_len) < 0) {
                throw std::runtime_error("Failed to get bytes data");
            }
            std::vector<uint8_t> bytes_vec(
                reinterpret_cast<uint8_t*>(bytes_ptr),
                reinterpret_cast<uint8_t*>(bytes_ptr) + bytes_len
            );
            out(type_tag, zpp::bits::sized<std::uint32_t>(bytes_vec)).or_throw();
        }
        else {
            // Fallback: convert to string
            uint8_t type_tag = 4;  // BYTES
            PyObject* str_obj = PyObject_Str(item);
            if (!str_obj) {
                throw std::runtime_error("Failed to convert object to string");
            }
            const char* utf8_str = PyUnicode_AsUTF8(str_obj);
            Py_DECREF(str_obj);
            if (!utf8_str) {
                throw std::runtime_error("Failed to convert fallback string to UTF-8");
            }
            std::string str(utf8_str);
            out(type_tag, zpp::bits::sized<std::uint32_t>(str)).or_throw();
        }
    }
    
    return result;
}

} // namespace aggregations
} // namespace opteryx
