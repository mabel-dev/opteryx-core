# cython: language_level=3, c_string_type=unicode, c_string_encoding=utf8
# distutils: language=c

from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8
from libc.stdlib cimport free

# ----------------------------------------------------------------------
# yyjson C API declarations
# ----------------------------------------------------------------------
cdef extern from "yyjson.h":
    ctypedef struct yyjson_val
    ctypedef struct yyjson_doc
    ctypedef struct yyjson_alc
    ctypedef struct yyjson_read_err:
        size_t pos
        const char* msg
        int code
    ctypedef struct yyjson_write_err:
        size_t pos
        const char* msg
        int code

    yyjson_doc* yyjson_read_opts(char* dat, size_t len, unsigned int flags,
                                 const yyjson_alc* alc, yyjson_read_err* err)
    char* yyjson_write(const yyjson_doc* doc, unsigned int flags, size_t* len)
    void yyjson_doc_free(yyjson_doc* doc)
    yyjson_val* yyjson_doc_get_root(yyjson_doc* doc)

    bint yyjson_is_null(yyjson_val* val)
    bint yyjson_is_bool(yyjson_val* val)
    bint yyjson_is_uint(yyjson_val* val)
    bint yyjson_is_sint(yyjson_val* val)
    bint yyjson_is_real(yyjson_val* val)
    bint yyjson_is_str(yyjson_val* val)
    bint yyjson_is_arr(yyjson_val* val)
    bint yyjson_is_obj(yyjson_val* val)

    bint yyjson_get_bool(yyjson_val* val)
    unsigned long long yyjson_get_uint(yyjson_val* val)
    long long yyjson_get_sint(yyjson_val* val)
    double yyjson_get_real(yyjson_val* val)
    const char* yyjson_get_str(yyjson_val* val)
    size_t yyjson_get_len(yyjson_val* val)

    size_t yyjson_arr_size(yyjson_val* arr)
    yyjson_val* yyjson_arr_get(yyjson_val* arr, size_t idx)

    size_t yyjson_obj_size(yyjson_val* obj)
    yyjson_val* yyjson_obj_get(yyjson_val* obj, const char* key)

    yyjson_val* yyjson_doc_ptr_get(yyjson_doc* doc, const char* ptr)
    yyjson_val* yyjson_ptr_get(yyjson_val* val, const char* ptr)

    ctypedef struct yyjson_obj_iter
    yyjson_obj_iter yyjson_obj_iter_with(yyjson_val* obj)
    yyjson_val* yyjson_obj_iter_next(yyjson_obj_iter* iter)
    yyjson_val* yyjson_obj_iter_get_val(yyjson_val* key)


# ----------------------------------------------------------------------
cdef inline str _decode_str(const char* s, size_t length):
    return PyUnicode_DecodeUTF8(s, length, NULL)


# ----------------------------------------------------------------------
# Forward declarations
# ----------------------------------------------------------------------
cdef class YYDoc
cdef class YYVal


# ----------------------------------------------------------------------
# YYVal – lazy wrapper around yyjson_val*
# ----------------------------------------------------------------------
cdef class YYVal:
    cdef yyjson_val* _val
    cdef YYDoc _doc

    # No __cinit__ – we create instances via __new__ and assign manually

    @property
    def doc(self):
        return self._doc

    # Type checks
    def is_null(self): return yyjson_is_null(self._val)
    def is_bool(self): return yyjson_is_bool(self._val)
    def is_int(self):  return yyjson_is_uint(self._val) or yyjson_is_sint(self._val)
    def is_uint(self): return yyjson_is_uint(self._val)
    def is_sint(self): return yyjson_is_sint(self._val)
    def is_real(self): return yyjson_is_real(self._val)
    def is_str(self):  return yyjson_is_str(self._val)
    def is_arr(self):  return yyjson_is_arr(self._val)
    def is_obj(self):  return yyjson_is_obj(self._val)

    # Full conversion to Python objects
    def as_py(self):
        return self._to_py(self._val)

    cdef object _to_py(self, yyjson_val* val):
        cdef list result_list
        cdef dict result_dict
        cdef size_t i, n
        cdef yyjson_val* item
        cdef yyjson_val* key
        cdef yyjson_val* child
        cdef yyjson_obj_iter it
        cdef const char* key_c
        cdef size_t key_len

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
            return _decode_str(yyjson_get_str(val), yyjson_get_len(val))
        elif yyjson_is_arr(val):
            result_list = []
            n = yyjson_arr_size(val)
            for i in range(n):
                result_list.append(self._to_py(yyjson_arr_get(val, i)))
            return result_list
        elif yyjson_is_obj(val):
            result_dict = {}
            it = yyjson_obj_iter_with(val)
            key = yyjson_obj_iter_next(&it)
            while key != NULL:
                key_c = yyjson_get_str(key)
                key_len = yyjson_get_len(key)
                child = yyjson_obj_iter_get_val(key)
                result_dict[_decode_str(key_c, key_len)] = self._to_py(child)
                key = yyjson_obj_iter_next(&it)
            return result_dict
        else:
            raise TypeError("Unknown JSON type")

    # Length
    def __len__(self):
        if yyjson_is_arr(self._val):
            return yyjson_arr_size(self._val)
        elif yyjson_is_obj(self._val):
            return yyjson_obj_size(self._val)
        raise TypeError(f"'{type(self).__name__}' has no length")

    # Indexing / key access
    def __getitem__(self, key):
        cdef size_t idx
        cdef yyjson_val* item
        cdef YYVal result

        if yyjson_is_arr(self._val):
            if not isinstance(key, int):
                raise TypeError("array indices must be integers")
            idx = <size_t>key
            item = yyjson_arr_get(self._val, idx)
            if item == NULL:
                raise IndexError("array index out of range")
            result = YYVal.__new__(YYVal)
            result._val = item
            result._doc = self._doc
            return result
        elif yyjson_is_obj(self._val):
            return self.get(key)
        else:
            raise TypeError(f"'{type(self).__name__}' is not subscriptable")

    def get(self, str key):
        cdef bytes bkey = key.encode()
        cdef yyjson_val* member = yyjson_obj_get(self._val, <const char*>bkey)
        if member == NULL:
            return None
        cdef YYVal result = YYVal.__new__(YYVal)
        result._val = member
        result._doc = self._doc
        return result

    # JSON Pointer
    def at_pointer(self, str pointer):
        cdef bytes bptr
        if pointer.startswith('$.'):
            bptr = self._jsonptr_from_dot(pointer).encode()
        else:
            bptr = pointer.encode()
        cdef yyjson_val* val = yyjson_doc_ptr_get(self._doc._doc, <const char*>bptr)
        if val == NULL:
            return None
        cdef YYVal result = YYVal.__new__(YYVal)
        result._val = val
        result._doc = self._doc
        return result

    cdef str _jsonptr_from_dot(self, str dot_path):
        if dot_path.startswith('$.'):
            path = dot_path[2:]
        else:
            path = dot_path
        parts = []
        i = 0
        n = len(path)
        while i < n:
            if path[i] == '[':
                j = path.find(']', i)
                if j == -1:
                    raise ValueError("unmatched '[' in JSON Pointer")
                idx = path[i+1:j]
                if not idx.isdigit():
                    raise ValueError("array index must be an integer")
                parts.append(idx)
                i = j + 1
                if i < n and path[i] == '.':
                    i += 1
            else:
                start = i
                while i < n and path[i] not in '.[':
                    i += 1
                parts.append(path[start:i])
        return '/' + '/'.join(parts)

    def dumps(self, int pretty=0, int indent=2):
        return self._doc.dumps(pretty, indent)


# ----------------------------------------------------------------------
# YYDoc – owns the yyjson_doc* and its root
# ----------------------------------------------------------------------
cdef class YYDoc:
    cdef yyjson_doc* _doc
    cdef yyjson_val* _root

    # No __cinit__ – we create instances via __new__ and assign

    def __dealloc__(self):
        if self._doc != NULL:
            yyjson_doc_free(self._doc)

    @property
    def root(self):
        cdef YYVal result = YYVal.__new__(YYVal)
        result._val = self._root
        result._doc = self
        return result

    def get(self, str key):
        return self.root.get(key)

    def __getitem__(self, key):
        return self.root[key]

    def at_pointer(self, str pointer):
        return self.root.at_pointer(pointer)

    def dumps(self, int pretty=0, int indent=2):
        cdef unsigned int flags = 1 if pretty else 0
        cdef size_t json_len = 0
        cdef char* json = NULL
        cdef object out = None
        json = yyjson_write(self._doc, flags, &json_len)
        if json == NULL:
            raise MemoryError("Serialisation error")
        try:
            out = PyBytes_FromStringAndSize(json, json_len)
            return out.decode('utf-8')
        finally:
            free(json)


# ----------------------------------------------------------------------
# Parser – creates YYDoc from JSON input
# ----------------------------------------------------------------------
cdef class Parser:
    def parse(self, object data, unsigned int flags=0):
        cdef bytes bdata
        if isinstance(data, str):
            bdata = data.encode('utf-8')
        elif isinstance(data, bytes):
            bdata = data
        else:
            raise TypeError("data must be bytes or str")

        cdef char* json_data = bdata
        cdef size_t json_len = len(bdata)
        cdef yyjson_read_err err
        cdef yyjson_doc* doc = yyjson_read_opts(json_data, json_len, flags, NULL, &err)
        if doc == NULL:
            raise ValueError(f"JSON parse error at position {err.pos}: {err.msg.decode() if err.msg else 'unknown'}")
        cdef YYDoc result = YYDoc.__new__(YYDoc)
        result._doc = doc
        result._root = yyjson_doc_get_root(doc)
        return result

    def loads(self, object data, unsigned int flags=0):
        return self.parse(data, flags)

    def dumps(self, object obj, object default_handler=None, unsigned int options=0):
        """Serialize a Python object to JSON bytes using yyjson's mutable API.

        Args:
            obj: The Python object to serialize
            default_handler: Optional callable(obj) for non-serializable objects
            options: yyjson write flags (e.g., YYJSON_WRITE_PRETTY)

        Returns:
            JSON as bytes (the caller decodes to str if needed)
        """
        cdef yyjson_mut_doc* doc
        cdef yyjson_mut_val* root
        cdef size_t json_len
        cdef char* json
        cdef object result

        doc = yyjson_mut_doc_new(NULL)
        if doc == NULL:
            raise MemoryError("Failed to create yyjson document")

        try:
            root = self._py_to_mut(obj, doc, default_handler)
            if root == NULL:
                raise TypeError(f"Cannot serialize object of type {type(obj).__name__}")

            yyjson_mut_doc_set_root(doc, root)

            json_len = 0
            json = yyjson_mut_write(doc, options, &json_len)
            if json == NULL:
                raise MemoryError("Serialisation error")

            try:
                result = PyBytes_FromStringAndSize(json, json_len)
                return result
            finally:
                free(json)
        finally:
            yyjson_mut_doc_free(doc)

    cdef yyjson_mut_val* _py_to_mut(self, object obj, yyjson_mut_doc* doc,
                                     object default_handler):
        """Recursively convert Python object to yyjson_mut_val."""
        cdef yyjson_mut_val* result
        cdef yyjson_mut_val* key_val
        cdef bytes key_bytes
        cdef bytes value_bytes
        cdef char* str_ptr
        cdef size_t str_len

        if obj is None:
            return yyjson_mut_null(doc)
        elif isinstance(obj, bool):
            return yyjson_mut_true(doc) if obj else yyjson_mut_false(doc)
        elif isinstance(obj, int):
            if obj >= 0:
                return yyjson_mut_uint(doc, <unsigned long long>obj)
            else:
                return yyjson_mut_sint(doc, <long long>obj)
        elif isinstance(obj, float):
            return yyjson_mut_real(doc, <double>obj)
        elif isinstance(obj, str):
            value_bytes = obj.encode('utf-8')
            PyBytes_AsStringAndSize(value_bytes, &str_ptr, <Py_ssize_t*>&str_len)
            return yyjson_mut_strncpy(doc, str_ptr, str_len)
        elif isinstance(obj, bytes):
            PyBytes_AsStringAndSize(obj, &str_ptr, <Py_ssize_t*>&str_len)
            return yyjson_mut_strncpy(doc, str_ptr, str_len)
        elif isinstance(obj, dict):
            result = yyjson_mut_obj(doc)
            if result == NULL:
                return NULL
            for py_key, py_val in obj.items():
                key_bytes = str(py_key).encode('utf-8')
                PyBytes_AsStringAndSize(key_bytes, &str_ptr, <Py_ssize_t*>&str_len)
                # Create the key as a yyjson_mut_val with copy
                key_val = yyjson_mut_strncpy(doc, str_ptr, str_len)
                if key_val == NULL:
                    return NULL
                # Create the value
                mut_val = self._py_to_mut(py_val, doc, default_handler)
                if mut_val == NULL:
                    return NULL
                # Add key-value pair
                if not yyjson_mut_obj_add(result, key_val, mut_val):
                    return NULL
            return result
        elif isinstance(obj, (list, tuple)):
            result = yyjson_mut_arr(doc)
            if result == NULL:
                return NULL
            for item in obj:
                mut_val = self._py_to_mut(item, doc, default_handler)
                if mut_val == NULL:
                    return NULL
                if not yyjson_mut_arr_add_val(result, mut_val):
                    return NULL
            return result
        elif default_handler is not None:
            try:
                return self._py_to_mut(default_handler(obj), doc, default_handler)
            except (TypeError, ValueError):
                return NULL
        else:
            return NULL
