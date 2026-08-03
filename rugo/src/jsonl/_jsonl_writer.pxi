
# distutils: language = c++
#
# Native JSONL writer: Morsel -> bytes (one JSON object per line).
# No pyarrow; all value formatting is in C++ (draken/interop/value_format.hpp).

from libc.stdint cimport uint8_t, uint32_t, int32_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libcpp.string cimport string
from libcpp.vector cimport vector

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING

from draken.core.buffers cimport DrakenVector, DRAKEN_ARRAY, DRAKEN_VECTOR_FP16
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector

cdef extern from "interop/value_format.hpp" namespace "rugo_text":
    void json_string(string& out, const char* s, size_t n)

cdef extern from "_text_render.hpp" namespace "rugo_text":
    # Returns the rendered bytes as one buffer per worker (parallel render).
    vector[string] jsonl_write(const DrakenVector** dvs, const DrakenVector** childs,
                               const ColumnDesc* descs,
                               const string* prefixes, size_t ncols, size_t nrows) nogil


def write_jsonl(Morsel morsel not None):
    """Serialize a Morsel to JSONL bytes (one JSON object per row)."""
    cdef Py_ssize_t ncols = morsel._num_columns()
    cdef Py_ssize_t nrows = morsel.num_rows
    cdef list names = morsel._col_names

    cdef list vecs = []        # keep Vector refs alive
    cdef list child_vecs = []
    cdef const DrakenVector** dvs = <const DrakenVector**>malloc(ncols * sizeof(void*))
    cdef const DrakenVector** child_dvs = <const DrakenVector**>malloc(ncols * sizeof(void*))
    # One descriptor per column. Zero-filled == the C++ struct's own defaults
    # (kind NONE, unit s, no scale/dimension) — malloc does not run them.
    cdef ColumnDesc* descs = <ColumnDesc*>malloc(ncols * sizeof(ColumnDesc))
    cdef vector[string] prefixes   # pre-escaped  "name":

    cdef Vector v, cv
    cdef const DrakenVector* dv
    cdef Py_ssize_t c, i
    cdef object nm
    cdef string namebuf
    cdef bytes nb_name
    cdef vector[string] chunks
    cdef size_t total = 0, off = 0, k
    cdef bytes result
    cdef char* dst

    try:
        memset(descs, 0, ncols * sizeof(ColumnDesc))
        for c in range(ncols):
            v = morsel._get_column(c)
            vecs.append(v)
            dv = v.unified()
            dvs[c] = dv
            child_dvs[c] = NULL
            _fill_logical_desc(&descs[c].column, v._nb)
            if dv.type == DRAKEN_VECTOR_FP16 and descs[c].column.dim == 0:
                raise ValueError(
                    "write_jsonl: VECTOR_FP16 column %r missing logical-type "
                    "descriptor (dimension)" % (names[c],))
            if dv.type == DRAKEN_ARRAY and v._nb.array_child_type is not None:
                cv = Vector(v._nb.array_child)
                child_vecs.append(cv)
                child_dvs[c] = cv.unified()
                _fill_logical_desc(&descs[c].child, cv._nb)
            nm = names[c]
            nb_name = nm if isinstance(nm, bytes) else str(nm).encode("utf-8")
            namebuf = string()
            json_string(namebuf, <const char*>nb_name, len(nb_name))
            namebuf.push_back(b':')
            prefixes.push_back(namebuf)

        with nogil:
            chunks = jsonl_write(dvs, child_dvs, descs,
                                 prefixes.data(), <size_t>ncols, <size_t>nrows)
        for k in range(chunks.size()):
            total += chunks[k].size()
        # Allocate the result bytes once and concatenate the per-worker chunks
        # straight into it — the full output is never staged in a std::string.
        result = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>total)
        dst = PyBytes_AS_STRING(result)
        with nogil:
            for k in range(chunks.size()):
                memcpy(dst + off, chunks[k].data(), chunks[k].size())
                off += chunks[k].size()
        return result
    finally:
        free(dvs); free(child_dvs); free(descs)
