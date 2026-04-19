# cython: language_level=3, c_string_type=unicode, c_string_encoding=utf8
# distutils: language=c

from cpython.bytes cimport PyBytes_AsStringAndSize

from cyyjson cimport *

cdef bytes str_as_bytes(s):
    if isinstance(s, unicode):
        return (<unicode>s).encode('utf-8')
    return s


cdef object val_to_python(yyjson_val* val):
    """Convert a yyjson value to a Python object."""
    cdef const char* s
    cdef size_t length

    if yyjson_is_null(val):
        return None
    elif yyjson_is_bool(val):
        return bool(yyjson_get_bool(val))
    elif yyjson_is_uint(val):
        return int(yyjson_get_uint(val))
    elif yyjson_is_sint(val):
        return int(yyjson_get_sint(val))
    elif yyjson_is_real(val):
        return float(yyjson_get_real(val))
    elif yyjson_is_str(val):
        s = yyjson_get_str(val)
        length = yyjson_get_len(val)
        return s[:length].decode('utf-8')
    elif yyjson_is_arr(val):
        return arr_to_list(val)
    elif yyjson_is_obj(val):
        return obj_to_dict(val)
    else:
        raise TypeError(f"Unknown yyjson value type")


cdef object arr_to_list(yyjson_val* arr):
    """Convert a yyjson array to a Python list."""
    cdef list result = []
    cdef yyjson_arr_iter iter
    cdef yyjson_val* item

    iter = yyjson_arr_iter_with(arr)
    item = yyjson_arr_iter_next(&iter)
    while item != NULL:
        result.append(val_to_python(item))
        item = yyjson_arr_iter_next(&iter)
    return result


cdef dict obj_to_dict(yyjson_val* obj):
    """Convert a yyjson object to a Python dict."""
    cdef dict result = {}
    cdef yyjson_obj_iter iter
    cdef yyjson_val* key_val
    cdef const char* key_c
    cdef size_t key_len

    iter = yyjson_obj_iter_with(obj)
    key_val = yyjson_obj_iter_next(&iter)
    while key_val != NULL:
        key_c = yyjson_get_str(key_val)
        key_len = yyjson_get_len(key_val)
        key_str = key_c[:key_len].decode('utf-8')
        result[key_str] = val_to_python(yyjson_obj_iter_get_val(key_val))
        key_val = yyjson_obj_iter_next(&iter)
    return result


cdef class Parser:
    """yyjson JSON parser wrapper."""

    def parse(self, data, recursive=True):
        """Parse JSON bytes/str to Python object.

        Args:
            data: JSON bytes or string
            recursive: Ignored (kept for API compatibility)

        Returns:
            Parsed Python object

        Raises:
            ValueError: On JSON parse error
        """
        cdef bytes json_bytes = str_as_bytes(data)
        cdef char* json_data
        cdef size_t json_len
        cdef yyjson_doc* doc
        cdef yyjson_val* root
        cdef yyjson_read_err err
        cdef object result

        PyBytes_AsStringAndSize(json_bytes, &json_data, <ssize_t*>&json_len)

        # Parse with standard flags
        doc = yyjson_read_opts(json_data, json_len, 0, NULL, &err)

        if doc == NULL:
            raise ValueError(f"yyjson parse error")

        try:
            root = yyjson_doc_get_root(doc)
            if root == NULL:
                raise ValueError("yyjson: failed to get root value")
            result = val_to_python(root)
        finally:
            yyjson_doc_free(doc)

        return result

    def dump(self, obj):
        """Serialize Python object to JSON bytes.

        Args:
            obj: Python object to serialize

        Returns:
            JSON bytes
        """
        # For now, fall back to stdlib json for serialization
        # yyjson's mutable document API is more complex
        import json
        return json.dumps(obj, separators=(',', ':')).encode('utf-8')
