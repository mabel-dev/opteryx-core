"""Cython wrapper for reading JSONL files."""

# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

from libc.stdint cimport uint8_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from cpython.buffer cimport PyBUF_CONTIG_RO, PyObject_GetBuffer, PyBuffer_Release, Py_buffer
from cpython.ref cimport PyObject
from cpython.exc cimport PyErr_Occurred, PyErr_Clear

# Typed-vector cimports removed as part of E.31 migration (same gap registry as E.28):
#   E.28-gap-1: Integer64Vector dense constructor + ptr.data write access
#   E.28-gap-2: Float64Vector dense constructor + ptr.data write access
#   E.28-gap-3: StringVectorBuilder (constructors, append_bytes, append_null, finish)
#   E.31-gap-1: BoolVector dense constructor + ptr.data write access
#   E.31-gap-2: array_from_sequence — Python-list-to-ArrayVector helper; no new-draken equivalent
from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel



# Internal fast array parser (no runtime deps). Parses a JSON array encoded
# in UTF-8 bytes into Python lists. Objects found inside arrays are returned
# as raw bytes; strings are unescaped to Python str; numbers become int/float;
# null -> None, true/false -> bool.


# (removed unused fast whitespace skip function)


def _parse_array_from_bytes(bytes b):
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t n = len(b)

    def parse_value():
        nonlocal i
        # skip whitespace
        while i < n and b[i] in (32,9,10,13):
            i += 1
        if i >= n:
            raise ValueError('unexpected end')
        c = b[i]
        # string
        if c == 34:  # '"'
            i += 1
            chars = []
            while i < n:
                ch = b[i]
                if ch == 34:
                    i += 1
                    return ''.join(chars)
                if ch == 92:  # backslash
                    i += 1
                    if i >= n:
                        raise ValueError('unterminated escape')
                    esc = b[i]
                    i += 1
                    if esc == 34: chars.append('"')
                    elif esc == 92: chars.append('\\')
                    elif esc == 47: chars.append('/')
                    elif esc == 98: chars.append('\b')
                    elif esc == 102: chars.append('\f')
                    elif esc == 110: chars.append('\n')
                    elif esc == 114: chars.append('\r')
                    elif esc == 116: chars.append('\t')
                    elif esc == 117:
                        # unicode escape \uXXXX
                        if i + 4 <= n:
                            hex_s = b[i:i+4].decode('ascii')
                            chars.append(chr(int(hex_s, 16)))
                            i += 4
                        else:
                            raise ValueError('invalid unicode escape')
                    else:
                        # unknown escape, keep char
                        chars.append(chr(esc))
                else:
                    # append raw utf-8 byte; accumulate then decode at end
                    # to support multi-byte UTF-8 sequences, collect bytes
                    start = i
                    # collect consecutive non-escape non-quote bytes
                    while i < n and b[i] != 34 and b[i] != 92:
                        i += 1
                    # decode slice
                    chars.append(b[start:i].decode('utf-8'))
            raise ValueError('unterminated string')

        # null
        if c == 110 and i + 4 <= n and b[i:i+4] == b'null':
            i += 4
            return None

        # true/false
        if c == 116 and i + 4 <= n and b[i:i+4] == b'true':
            i += 4
            return True
        if c == 102 and i + 5 <= n and b[i:i+5] == b'false':
            i += 5
            return False

        # number
        if c == 45 or (48 <= c <= 57):
            start = i
            if c == 45:
                i += 1
            while i < n and 48 <= b[i] <= 57:
                i += 1
            if i < n and b[i] == 46:
                i += 1
                while i < n and 48 <= b[i] <= 57:
                    i += 1
            if i < n and (b[i] == 101 or b[i] == 69):
                i += 1
                if i < n and (b[i] == 43 or b[i] == 45):
                    i += 1
                while i < n and 48 <= b[i] <= 57:
                    i += 1
                return float(b[start:i].decode('ascii'))
            s = b[start:i].decode('ascii')
            if '.' in s or 'e' in s or 'E' in s:
                return float(s)
            return int(s)

        # array
        if c == 91:  # '['
            # parse nested array
            i += 1
            res = []
            # skip whitespace
            while i < n and b[i] in (32,9,10,13): i += 1
            if i < n and b[i] == 93:
                i += 1
                return res
            while True:
                val = parse_value()
                res.append(val)
                while i < n and b[i] in (32,9,10,13): i += 1
                if i >= n:
                    raise ValueError('unterminated array')
                if b[i] == 44:
                    i += 1
                    continue
                elif b[i] == 93:
                    i += 1
                    break
                else:
                    raise ValueError('invalid array separator')
            return res

        # object: return raw bytes slice for object
        if c == 123:  # '{'
            start = i
            depth = 0
            while i < n:
                ch = b[i]
                if ch == 34:
                    # skip string
                    i += 1
                    while i < n:
                        if b[i] == 92:
                            i += 2
                        elif b[i] == 34:
                            i += 1
                            break
                        else:
                            i += 1
                    continue
                if ch == 123:
                    depth += 1
                elif ch == 125:
                    depth -= 1
                    if depth == 0:
                        i += 1
                        return b[start:i]
                i += 1
            raise ValueError('unterminated object')

        raise ValueError('unexpected token at %d' % i)

    # top-level: expect '['
    while i < n and b[i] in (32,9,10,13): i += 1
    if i >= n or b[i] != 91:
        raise ValueError('not an array')
    return parse_value()

cdef extern from "decode.hpp":
    cdef enum JsonType:
        pass
    cdef cppclass ColumnSchema:
        string name
        JsonType type
        bint nullable
        JsonType element_type
    cdef cppclass JsonlColumn:
        vector[int64_t] int_values
        vector[double] double_values
        vector[string] string_values
        vector[uint8_t] boolean_values
        vector[uint8_t] null_mask
        string type
        bint success
    cdef cppclass JsonlTable:
        vector[JsonlColumn] columns
        vector[string] column_names
        size_t num_rows
        bint success
    vector[ColumnSchema] GetJsonlSchema(const uint8_t* data, size_t size, size_t sample_size) nogil except +
    JsonlTable ReadJsonl(const uint8_t* data, size_t size, const vector[string]& column_names) nogil except +
    JsonlTable ReadJsonl(const uint8_t* data, size_t size) nogil except +
    PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bint parse_objects)

def get_jsonl_schema(data, sample_size=25):
    """
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

    Infer the schema of a JSONL dataset from a sample of the data.

    Parameters
    ----------
    data : bytes or object supporting the buffer protocol
        The JSONL data to analyze.
    sample_size : int, optional
        The number of rows to sample for schema inference (default: 25).

    Returns
    -------
    list of dict
        A list of dictionaries, each describing a column with keys:
        - 'name': str, the column name
        - 'type': str, the inferred type ('null', 'boolean', 'int64', 'double', 'bytes')
        - 'nullable': bool, whether the column can contain null values
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef bint have_view = False
    if isinstance(data, bytes):
        data_bytes = <bytes>data
        data_ptr = <const uint8_t*>(<char*>data_bytes)
        data_size = len(data_bytes)
    else:
        if PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO) == -1:
            raise TypeError("object does not support contiguous buffer interface")
        have_view = True
        data_ptr = <const uint8_t*>view.buf
        data_size = <size_t>view.len
    cdef size_t sample_size_c = <size_t>sample_size
    cdef vector[ColumnSchema] schema
    with nogil:
        schema = GetJsonlSchema(data_ptr, data_size, sample_size_c)
    if have_view:
        PyBuffer_Release(&view)
    result = []
    cdef size_t i
    cdef int type_val
    for i in range(schema.size()):
        col = schema[i]
        type_val = <int>col.type
        type_str = "string"
        # JsonType enum: Null=0, Boolean=1, Integer=2, Double=3, String=4, Array=5, Object=6
        if type_val == 0:
            type_str = "null"
        elif type_val == 1:
            type_str = "boolean"
        elif type_val == 2:
            type_str = "int64"
        elif type_val == 3:
            type_str = "double"
        elif type_val == 4:
            type_str = "string"
        elif type_val == 5:
            # array: include element type if available
            elem_val = <int>col.element_type
            if elem_val == 2:
                type_str = "array<int64>"
            elif elem_val == 3:
                type_str = "array<double>"
            elif elem_val == 4:
                type_str = "array<bytes>"
            else:
                type_str = "array"
        elif type_val == 6:
            type_str = "object"
        result.append({
            'name': col.name.decode('utf-8'),
            'type': type_str,
            'nullable': col.nullable
        })
    return result


def _has_string_leaf(x):
    """Check if any leaf in a nested list is a string (recursive check)."""
    if isinstance(x, list):
        for y in x:
            if _has_string_leaf(y):
                return True
        return False
    return isinstance(x, str)


def _convert_str_leaves_to_bytes(obj):
    """Convert all string leaves in a nested list to bytes (in-place)."""
    if isinstance(obj, list):
        for idx in range(len(obj)):
            v = obj[idx]
            if isinstance(v, str):
                obj[idx] = v.encode('utf-8')
            elif isinstance(v, list):
                _convert_str_leaves_to_bytes(v)



cdef Vector _build_int64_vector(JsonlColumn* col, size_t n):
    raise NotImplementedError(
        "rugo migration gap: Integer64Vector dense constructor + ptr.data write access "
        "has no new-draken equivalent; tracked as E.28-gap-1."
    )


cdef Vector _build_float64_vector(JsonlColumn* col, size_t n):
    raise NotImplementedError(
        "rugo migration gap: Float64Vector dense constructor + ptr.data write access "
        "has no new-draken equivalent; tracked as E.28-gap-2."
    )


cdef Vector _build_bool_vector(JsonlColumn* col, size_t n):
    raise NotImplementedError(
        "rugo migration gap: BoolVector dense constructor + ptr.data write access "
        "has no new-draken equivalent; tracked as E.31-gap-1."
    )


cdef Vector _build_string_vector(JsonlColumn* col, size_t n):
    raise NotImplementedError(
        "rugo migration gap: StringVectorBuilder has no new-draken equivalent; "
        "tracked as E.28-gap-3."
    )


cdef Vector _build_array_vector(
        JsonlColumn* col, size_t n, string col_type,
        bint parse_objects):
    raise NotImplementedError(
        "rugo migration gap: array_from_sequence (Python-list-to-ArrayVector) "
        "has no new-draken equivalent; tracked as E.31-gap-2."
    )


def read_jsonl(data, columns=None, parse_arrays=True, parse_objects=True):
    """
    Reads a JSONL (JSON Lines) dataset and returns its contents in a columnar format.

    Parameters
    ----------
    data : bytes or object supporting buffer protocol
        The JSONL data to read. Can be a bytes object or any object supporting the buffer protocol.
    columns : list of str, optional
        List of column names to read. If None, all columns are read.

    Returns
    -------
    dict
        A dictionary with the following keys:
            - 'success': bool, True if reading was successful.
            - 'column_names': list of str, names of the columns.
            - 'num_rows': int, number of rows in the dataset.
            - 'columns': list, each element is a list of values for a column (with None for nulls), or None if the column failed to read.
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef bint have_view = False
    if isinstance(data, bytes):
        data_bytes = <bytes>data
        data_ptr = <const uint8_t*>(<char*>data_bytes)
        data_size = len(data_bytes)
    else:
        if PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO) == -1:
            raise TypeError("object does not support contiguous buffer interface")
        have_view = True
        data_ptr = <const uint8_t*>view.buf
        data_size = <size_t>view.len
    cdef vector[string] column_names_vec
    cdef JsonlTable table
    if columns is None:
        with nogil:
            table = ReadJsonl(data_ptr, data_size)
    else:
        for col_name in columns:
            column_names_vec.push_back(col_name.encode('utf-8'))
        with nogil:
            table = ReadJsonl(data_ptr, data_size, column_names_vec)
    if have_view:
        PyBuffer_Release(&view)
    if not table.success:
        return {
            'success': False,
            'column_names': [],
            'num_rows': 0,
            'columns': []
        }
    py_column_names = []
    cdef size_t i
    for i in range(table.column_names.size()):
        py_column_names.append(table.column_names[i].decode('utf-8'))
    cdef size_t n = table.num_rows
    cdef JsonlColumn* col
    draken_columns = []
    for i in range(table.columns.size()):
        col = &table.columns[i]
        if not col.success:
            draken_columns.append(None)
            continue
        if col.type == b'int64':
            draken_columns.append(_build_int64_vector(col, n))
        elif col.type == b'double':
            draken_columns.append(_build_float64_vector(col, n))
        elif col.type == b'string' or col.type == b'bytes' or col.type == b'object':
            draken_columns.append(_build_string_vector(col, n))
        elif col.type == b'boolean':
            draken_columns.append(_build_bool_vector(col, n))
        elif col.type.size() >= 5 and col.type.substr(0, 5) == b'array':
            if parse_arrays:
                draken_columns.append(_build_array_vector(col, n, col.type, parse_objects))
            else:
                draken_columns.append(_build_string_vector(col, n))
        else:
            draken_columns.append(None)

    return {
        'success': True,
        'column_names': py_column_names,
        'num_rows': table.num_rows,
        'columns': draken_columns
    }
