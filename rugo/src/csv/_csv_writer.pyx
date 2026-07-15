# cython: language_level=3
# distutils: language = c++
#
# Native CSV writer: Morsel -> bytes (RFC 4180). No pyarrow; all value
# formatting is in C++ (draken/interop/value_format.hpp).

from libc.stdint cimport uint8_t, uint32_t, int32_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libcpp.string cimport string
from libcpp.vector cimport vector

from cpython.bytes cimport PyBytes_FromStringAndSize

from draken.core.buffers cimport DrakenVector, DRAKEN_ARRAY
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector

cdef extern from "_text_render.hpp" namespace "rugo_text":
    string csv_write(const DrakenVector** dvs, const DrakenVector** childs,
                     const int* units, const int* scales,
                     const int* cunits, const int* cscales,
                     const string* names, size_t ncols, size_t nrows,
                     char delim, bint header)


cdef inline int _unit_code(object u):
    if u == "s": return 0
    if u == "ms": return 1
    if u == "ns": return 3
    return 2  # us / default


def write_csv(Morsel morsel not None, str delimiter=",", bint header=True):
    """Serialize a Morsel to CSV bytes (RFC 4180).

    delimiter: single-character field separator (default ',').
    header: write a header row of column names (default True).
    Nulls are empty fields; ARRAY columns render as a JSON array (quoted).
    """
    if len(delimiter) != 1:
        raise ValueError("write_csv: delimiter must be a single character")
    cdef char delim = <char>(delimiter.encode("utf-8")[0])

    cdef Py_ssize_t ncols = morsel._num_columns()
    cdef Py_ssize_t nrows = morsel.num_rows
    cdef list names = morsel._col_names

    cdef list vecs = []
    cdef list child_vecs = []
    cdef const DrakenVector** dvs = <const DrakenVector**>malloc(ncols * sizeof(void*))
    cdef const DrakenVector** child_dvs = <const DrakenVector**>malloc(ncols * sizeof(void*))
    cdef int* units = <int*>malloc(ncols * sizeof(int))
    cdef int* scales = <int*>malloc(ncols * sizeof(int))
    cdef int* cunits = <int*>malloc(ncols * sizeof(int))
    cdef int* cscales = <int*>malloc(ncols * sizeof(int))

    cdef Vector v, cv
    cdef const DrakenVector* dv
    cdef Py_ssize_t c
    cdef object nm, u, sc
    cdef string out
    cdef bytes nb_name
    cdef vector[string] cnames

    try:
        for c in range(ncols):
            v = morsel._get_column(c)
            vecs.append(v)
            dv = v.unified()
            dvs[c] = dv
            child_dvs[c] = NULL
            units[c] = 0; scales[c] = 0; cunits[c] = 0; cscales[c] = 0
            u = v._nb.logical_type_unit
            if u is not None:
                units[c] = _unit_code(u)
            sc = v._nb.logical_type_scale
            if sc is not None:
                scales[c] = <int>sc
            if dv.type == DRAKEN_ARRAY and v._nb.array_child_type is not None:
                cv = Vector(v._nb.array_child)
                child_vecs.append(cv)
                child_dvs[c] = cv.unified()
                u = cv._nb.logical_type_unit
                if u is not None:
                    cunits[c] = _unit_code(u)
                sc = cv._nb.logical_type_scale
                if sc is not None:
                    cscales[c] = <int>sc
            nm = names[c]
            nb_name = nm if isinstance(nm, bytes) else str(nm).encode("utf-8")
            cnames.push_back(string(<const char*>nb_name, len(nb_name)))

        out = csv_write(dvs, child_dvs, units, scales, cunits, cscales,
                        cnames.data(), <size_t>ncols, <size_t>nrows, delim, header)
        return PyBytes_FromStringAndSize(out.data(), out.size())
    finally:
        free(dvs); free(child_dvs)
        free(units); free(scales); free(cunits); free(cscales)
