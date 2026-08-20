
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

from draken.core.buffers cimport DrakenVector, DRAKEN_ARRAY, DRAKEN_VECTOR_FP16
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector

cdef extern from "_text_render.hpp" namespace "rugo_text":
    # for_excel raises std::invalid_argument (-> ValueError) on a morsel Excel
    # would silently mangle, hence `except +`.
    string csv_write(const DrakenVector** dvs,
                     const ColumnDesc* descs,
                     const string* names, size_t ncols, size_t nrows,
                     char delim, bint header, bint for_excel) except +


def write_csv(Morsel morsel not None, str delimiter=",", bint header=True,
              bint for_excel=False):
    """Serialize a Morsel to CSV bytes (RFC 4180).

    delimiter: single-character field separator (default ',').
    header: write a header row of column names (default True).
    for_excel: check the morsel against the limits of the Excel grid it is
    destined for (default False -- a CSV file itself has no such limits).
    Excel enforces them by truncating the cell and dropping the off-sheet rows
    and columns without saying so, so this raises ValueError instead:
      * more than 1,048,576 lines (the header row counts),
      * more than 16,384 columns,
      * any cell -- or column name -- over 32,767 characters.
    The row count is per-morsel; a caller concatenating several morsels into
    one file must add up the rows itself.
    Nulls are empty fields; ARRAY and VECTOR_FP16 columns render as a JSON
    array (quoted) -- VECTOR_FP16 has no wire type here any more than in
    Parquet, so it renders as an array of floats.
    """
    if len(delimiter) != 1:
        raise ValueError("write_csv: delimiter must be a single character")
    cdef char delim = <char>(delimiter.encode("utf-8")[0])

    cdef Py_ssize_t ncols = morsel._num_columns()
    cdef Py_ssize_t nrows = morsel.num_rows
    cdef list names = morsel._col_names

    cdef list vecs = []
    cdef list child_vecs = []  # keep every ARRAY-level Vector alive (see _fill_array_levels)
    cdef const DrakenVector** dvs = <const DrakenVector**>malloc(ncols * sizeof(void*))
    # One descriptor per column, properly default-constructed (unlike malloc,
    # vector[T].resize() runs each ColumnDesc's constructor — required now
    # that it owns a std::vector<ArrayLevel> member).
    cdef vector[ColumnDesc] descs
    descs.resize(ncols)

    cdef Vector v
    cdef const DrakenVector* dv
    cdef Py_ssize_t c
    cdef object nm
    cdef string out
    cdef bytes nb_name
    cdef vector[string] cnames

    try:
        for c in range(ncols):
            v = morsel._get_column(c)
            vecs.append(v)
            dv = v.unified()
            dvs[c] = dv
            _fill_logical_desc(&descs[c].column, v._nb)
            if dv.type == DRAKEN_VECTOR_FP16 and descs[c].column.dim == 0:
                raise ValueError(
                    "write_csv: VECTOR_FP16 column %r missing logical-type "
                    "descriptor (dimension)" % (names[c],))
            if dv.type == DRAKEN_ARRAY:
                _fill_array_levels(&descs[c], v, child_vecs)
            nm = names[c]
            nb_name = nm if isinstance(nm, bytes) else str(nm).encode("utf-8")
            cnames.push_back(string(<const char*>nb_name, len(nb_name)))

        out = csv_write(dvs, descs.data(),
                        cnames.data(), <size_t>ncols, <size_t>nrows, delim, header,
                        for_excel)
        return PyBytes_FromStringAndSize(out.data(), out.size())
    finally:
        free(dvs)
