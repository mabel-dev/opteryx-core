# cython: language_level=3, c_string_type=unicode, c_string_encoding=utf8
# distutils: language=c

from cpython.bytes cimport PyBytes_AsStringAndSize

from .cyyjson cimport *

cdef bytes str_as_bytes(s):
    # Accept both bytes and str; avoid Python 2 'unicode' name
    if isinstance(s, str):
        return s.encode('utf-8')
    return s



# Lazy wrappers: avoid converting whole document to Python objects until requested

cdef object val_to_python(yyjson_val* val):
    """Eager conversion helper used by as_py() when needed."""
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
        raise TypeError("Unknown yyjson value type")


cdef object arr_to_list(yyjson_val* arr):
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


cdef class YYVal:
    cdef yyjson_val* _val
    cdef yyjson_doc* _doc

    def __cinit__(self, yyjson_val* v, yyjson_doc* d):
        self._val = v
        self._doc = d

    def as_py(self):
        return val_to_python(self._val)

    def as_list(self):
        if not yyjson_is_arr(self._val):
            raise TypeError("Not an array")
        return arr_to_list(self._val)

    def as_dict(self):
        if not yyjson_is_obj(self._val):
            raise TypeError("Not an object")
        return obj_to_dict(self._val)

    def get(self, key):
        """Get object member by key (returns YYVal or None)."""
        if not yyjson_is_obj(self._val):
            raise TypeError("Not an object")
        cdef yyjson_obj_iter iter = yyjson_obj_iter_with(self._val)
        cdef yyjson_val* key_val = yyjson_obj_iter_next(&iter)
        cdef const char* key_c
        cdef size_t key_len
        cdef object key_str
        while key_val != NULL:
            key_c = yyjson_get_str(key_val)
            key_len = yyjson_get_len(key_val)
            key_str = key_c[:key_len].decode('utf-8')
            if key_str == key:
                return YYVal(yyjson_obj_iter_get_val(key_val), self._doc)
            key_val = yyjson_obj_iter_next(&iter)
        return None

    def __getitem__(self, idx):
        if yyjson_is_arr(self._val):
            cdef yyjson_arr_iter iter = yyjson_arr_iter_with(self._val)
            cdef yyjson_val* item = yyjson_arr_iter_next(&iter)
            cdef Py_ssize_t i = 0
            while item != NULL:
                if i == idx:
                    return YYVal(item, self._doc)
                i += 1
                item = yyjson_arr_iter_next(&iter)
            raise IndexError('index out of range')
        elif yyjson_is_obj(self._val):
            return self.get(idx)
        else:
            raise TypeError('value is not subscriptable')


cdef class YYDoc:
    cdef yyjson_doc* _doc
    cdef yyjson_val* _root

    def __cinit__(self, yyjson_doc* d):
        self._doc = d
        self._root = yyjson_doc_get_root(d)

    def __dealloc__(self):
        if self._doc != NULL:
            yyjson_doc_free(self._doc)
            self._doc = NULL

    def root(self):
        return YYVal(self._root, self._doc)

    def get(self, key):
        return self.root().get(key)

    def as_py(self):
        return val_to_python(self._root)

    def dumps(self):
        # Not implemented: avoid silent fallback to stdlib json.
        raise NotImplementedError("YYDoc.dumps is not implemented. Use a yyjson-based writer or convert to Python objects and serialize with json.dumps")


cdef class Parser:
    """yyjson JSON parser wrapper returning lazy YYDoc objects."""

    def parse(self, data, recursive=True):
        cdef bytes json_bytes = str_as_bytes(data)
        cdef char* json_data
        cdef size_t json_len
        cdef yyjson_doc* doc
        cdef yyjson_read_err err

        PyBytes_AsStringAndSize(json_bytes, &json_data, <ssize_t*>&json_len)

        doc = yyjson_read_opts(json_data, json_len, 0, NULL, &err)
        if doc == NULL:
            raise ValueError('json parse error')

        return YYDoc(doc)

    def dump(self, obj):
        if isinstance(obj, YYDoc):
            return obj.dumps()
