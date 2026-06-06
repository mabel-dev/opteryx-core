# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int64_t
from libc.stdlib cimport malloc, free
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.utility cimport move


cdef extern from "core/csv_parse_context.hpp" namespace "rugo::_csv":
    struct CsvPredicate:
        string column
        uint8_t op
        string value

    struct CsvParseContext:
        uint8_t delimiter
        bint    has_header
        vector[string] projected_columns
        vector[CsvPredicate] predicates
        size_t max_threads
        void rebuild_lut()


cdef extern from "core/csv_row_map.hpp" namespace "rugo::_csv":
    struct CsvFieldSpan:
        uint32_t start
        uint16_t length
        bint     was_quoted
        bint     has_escape

    struct CsvRowMap:
        uint32_t num_rows
        uint32_t num_cols
        vector[string] column_names
        vector[uint32_t] request_cols
        vector[vector[CsvFieldSpan]] column_spans

    size_t parse_csv_header(
        const uint8_t* data,
        size_t         length,
        const CsvParseContext& ctx,
        vector[string]& column_names_out,
        uint32_t&       num_cols_out
    ) nogil

    CsvRowMap extract_spans_threaded(
        const uint8_t*          data,
        size_t                  length,
        size_t                  header_offset,
        const CsvParseContext&  ctx,
        uint32_t                num_cols,
        const vector[uint32_t]& request_ordinals,
        size_t                  max_threads
    ) nogil

    vector[uint8_t] evaluate_predicates(
        const uint8_t*         buffer,
        const CsvRowMap&       row_map,
        const CsvParseContext& ctx
    ) nogil


cdef extern from "core/csv_column_builder.hpp" namespace "rugo::_csv":
    cppclass ParsedCsvColumn:
        pass

    vector[ParsedCsvColumn] parse_all_csv_columns(
        const uint8_t*          buffer,
        const CsvRowMap&        row_map,
        const vector[size_t]&   column_indices,
        const vector[uint8_t]&  survivor_bitmap,
        size_t                  max_threads
    ) nogil

    object wrap_csv_column(ParsedCsvColumn& pc)


# Op code mapping: 0=EQ 1=NE 2=LT 3=LE 4=GT 5=GE (shared with JSONL)
cdef int _parse_op(str op) except -1:
    if   op == '==': return 0
    elif op == '!=': return 1
    elif op == '<':  return 2
    elif op == '<=': return 3
    elif op == '>':  return 4
    elif op == '>=': return 5
    raise ValueError(f"Unknown predicate operator: {op!r}")


def read_csv(
    data,
    columns=None,
    predicates=None,
    delimiter=',',
    has_header=True,
    use_threads=True,
):
    """
    Read CSV data into Draken vectors with projection and predicate pushdown.

    Parameters
    ----------
    data : bytes, buffer-like, or str
        Raw CSV bytes or a file path string.
    columns : list[str] | None
        Column names to extract (None = all columns).
    predicates : list[tuple] | None
        Filter predicates as (column, op, value) tuples.
        op must be one of: '==' '!=' '<' '<=' '>' '>='
    delimiter : str
        Single-character field separator (default ','; use '\\t' for TSV).
    has_header : bool
        True if the first row is a header row (default True).
        False: columns are named col_0, col_1, …
    use_threads : bool
        Enable parallel span extraction (default True).

    Returns
    -------
    dict with keys:
        'success'      : bool
        'column_names' : list[str]
        'num_rows'     : int
        'columns'      : list of Draken Vector objects
    """
    cdef CsvParseContext  ctx
    cdef CsvPredicate     pred_cpp
    cdef vector[string]   column_names_cpp
    cdef uint32_t         num_cols = 0
    cdef size_t           header_offset = 0
    cdef size_t           n_threads
    cdef vector[uint32_t] request_ordinals
    cdef vector[size_t]   column_indices
    cdef CsvRowMap        row_map
    cdef vector[uint8_t]  survivor_bitmap
    cdef vector[ParsedCsvColumn] parsed_cols
    cdef const uint8_t*   buf_ptr
    cdef size_t           buf_len
    cdef uint32_t         n_survivors = 0
    cdef uint32_t         r, byte_idx, bit_idx
    cdef size_t           i
    cdef uint32_t         col_ord

    cdef dict result = {
        'success': False,
        'column_names': [],
        'num_rows': 0,
        'columns': [],
    }

    # ---- Build CsvParseContext ----
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("delimiter must be a single character")
    ctx.delimiter  = ord(delimiter)
    ctx.has_header = bool(has_header)
    ctx.rebuild_lut()

    if columns:
        for col in columns:
            ctx.projected_columns.push_back(col.encode('utf-8'))

    if predicates:
        for col, op, val in predicates:
            pred_cpp.column = col.encode('utf-8')
            pred_cpp.op     = <uint8_t>_parse_op(op)
            pred_cpp.value  = str(val).encode('utf-8')
            ctx.predicates.push_back(pred_cpp)

    # ---- Load buffer ----
    if isinstance(data, str):
        with open(data, 'rb') as f:
            buf = f.read()
    elif isinstance(data, bytes):
        buf = data
    else:
        buf = bytes(data)

    if len(buf) == 0:
        result['success'] = True
        return result

    buf_ptr = <const uint8_t*>buf
    buf_len = len(buf)

    # ---- Phase 1: header parse ----
    with nogil:
        header_offset = parse_csv_header(
            buf_ptr, buf_len, ctx, column_names_cpp, num_cols
        )

    if num_cols == 0:
        result['success'] = True
        return result

    # ---- Resolve request_ordinals ----
    # Build name → ordinal dict (Python, under GIL)
    name_to_ord = {}
    for i in range(column_names_cpp.size()):
        name_to_ord[column_names_cpp[i].decode('utf-8')] = <int>i

    predicate_col_names = set()
    if predicates:
        for col, op, val in predicates:
            predicate_col_names.add(col)

    if columns:
        proj_names = list(columns)
    else:
        proj_names = [column_names_cpp[i].decode('utf-8')
                      for i in range(<int>column_names_cpp.size())]

    # Collect all needed ordinals (projection ∪ predicate columns), sorted
    ord_set = set()
    for name in proj_names:
        if name in name_to_ord:
            ord_set.add(name_to_ord[name])
    for name in predicate_col_names:
        if name in name_to_ord:
            ord_set.add(name_to_ord[name])

    for o in sorted(ord_set):
        request_ordinals.push_back(<uint32_t>o)

    if request_ordinals.empty():
        result['success'] = True
        result['column_names'] = proj_names
        result['num_rows'] = 0
        return result

    # ---- Phase 2: span extraction ----
    n_threads = 0 if use_threads else 1  # pre-compute before nogil
    with nogil:
        row_map = extract_spans_threaded(
            buf_ptr, buf_len, header_offset, ctx,
            num_cols, request_ordinals, n_threads
        )

    # Populate row_map.column_names aligned to request_ordinals
    # (evaluate_predicates matches predicates by column name)
    row_map.column_names.clear()
    for i in range(request_ordinals.size()):
        row_map.column_names.push_back(column_names_cpp[request_ordinals[i]])

    if row_map.num_rows == 0:
        result['success'] = True
        result['column_names'] = proj_names
        result['num_rows'] = 0
        return result

    # ---- Phase 3: predicate evaluation ----
    if not ctx.predicates.empty():
        with nogil:
            survivor_bitmap = evaluate_predicates(buf_ptr, row_map, ctx)

    # Count survivors (typed C loop avoids Python object truth-testing)
    if survivor_bitmap.empty():
        n_survivors = row_map.num_rows
    else:
        n_survivors = 0
        for r in range(row_map.num_rows):
            byte_idx = r >> 3
            bit_idx  = r & 7
            if (survivor_bitmap[byte_idx] >> bit_idx) & 1:
                n_survivors += 1

    if n_survivors == 0:
        result['success'] = True
        result['column_names'] = proj_names
        result['num_rows'] = 0
        return result

    # ---- Phase 4: column build ----
    # Map ordinal → span index within row_map.column_spans
    ord_to_span_idx = {}
    for i in range(request_ordinals.size()):
        ord_to_span_idx[<int>request_ordinals[i]] = <int>i

    output_col_names = []
    for name in proj_names:
        if name in name_to_ord:
            col_ord = <uint32_t>name_to_ord[name]
            if <int>col_ord in ord_to_span_idx:
                column_indices.push_back(<size_t>ord_to_span_idx[<int>col_ord])
                output_col_names.append(name)

    with nogil:
        parsed_cols = parse_all_csv_columns(
            buf_ptr, row_map, column_indices, survivor_bitmap, 0
        )

    # Wrap under GIL
    draken_vectors = []
    for i in range(parsed_cols.size()):
        draken_vectors.append(wrap_csv_column(parsed_cols[i]))

    result['success']      = True
    result['column_names'] = output_col_names
    result['num_rows']     = <int>n_survivors
    result['columns']      = draken_vectors
    return result
