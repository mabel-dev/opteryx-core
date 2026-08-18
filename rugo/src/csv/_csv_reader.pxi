









from libc.stdint cimport uint8_t, int16_t, uint32_t, int64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap

from draken.core.buffers cimport DrakenType


cdef extern from "declared_type.hpp" namespace "rugo":
    # explicit_schema's type vocabulary. Validation goes through the SAME parser the
    # C++ builder uses, so a name that validates here is guaranteed to resolve there.
    cdef struct DeclaredType:
        DrakenType type
        uint8_t    logical_kind
        uint8_t    unit
        int16_t    offset_minutes
        uint8_t    precision
        uint8_t    scale

    bint parse_declared_type(const string& name, DeclaredType* out) nogil
    const char* declared_type_vocabulary() nogil


cdef extern from "core/csv_parse_context.hpp" namespace "rugo::_csv":
    struct CsvPredicate:
        string column
        uint8_t op
        string value

    struct CsvParseContext:
        uint8_t delimiter
        bint    has_header
        uint32_t sniff_sample_size
        bint    ignore_errors
        vector[string] projected_columns
        vector[CsvPredicate] predicates
        cmap[string, string] explicit_schema
        size_t max_threads
        void rebuild_lut()


cdef extern from "core/csv_row_map.hpp" namespace "rugo::_csv":
    size_t parse_csv_header(
        const uint8_t* data,
        size_t         length,
        const CsvParseContext& ctx,
        vector[string]& column_names_out,
        uint32_t&       num_cols_out
    ) nogil


cdef extern from "core/csv_column_builder.hpp" namespace "rugo::_csv":
    cppclass ParsedCsvColumn:
        pass

    struct StreamResult:
        vector[ParsedCsvColumn] columns
        uint32_t                num_rows

    # except + : commit_row throws std::runtime_error -- translated by Cython to a
    # Python RuntimeError -- on a post-sniff type mismatch (ignore_errors=false) and
    # on ANY value that does not fit an explicit_schema-declared type (where
    # ignore_errors does not apply at all). See csv_column_builder.cpp.
    StreamResult build_columns_streaming(
        const uint8_t*          buffer,
        size_t                  length,
        size_t                  header_offset,
        const vector[string]&   column_names,
        uint32_t                num_cols,
        const vector[uint32_t]& request_ordinals,
        const vector[size_t]&   proj_indices,
        const CsvParseContext&  ctx,
        size_t                  max_threads
    ) except + nogil

    object wrap_csv_column(ParsedCsvColumn& pc)


# Op code mapping: 0=EQ 1=NE 2=LT 3=LE 4=GT 5=GE
cdef int _csv_parse_op(str op) except -1:
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
    infer_sample_size=128,
    fail_on_error=True,
    explicit_schema=None,
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
    infer_sample_size : int
        Non-null values per projected column sampled to sniff its type
        (INT64 -> FLOAT64 -> VARCHAR widening). Default 128.
    fail_on_error : bool
        True (default): a value past the sample window that doesn't parse as
        the sniffed type raises RuntimeError, naming the column and value.
        False: that value is treated as NULL instead.

        This governs SNIFFED columns only. A column declared in explicit_schema
        is never softened by it — see below.
    explicit_schema : dict[str, str] | None
        Declared column types, keyed by column name. The type is a
        PLATFORM-CANONICAL type name — the same string a stored schema holds —
        so a caller that already knows the destination schema passes it straight
        through with no translation table:

            INT8 INT16 INT32 INT64 · UINT8 UINT16 UINT32 UINT64 · FLOAT32 FLOAT64
            BOOL · VARCHAR · DATE · TIMESTAMP[s|ms|us|ns] · DECIMAL(p, s) · IPV4

        matched case-insensitively, with the usual SQL aliases (INTEGER, BIGINT,
        TINYINT, SMALLINT, DOUBLE, FLOAT, REAL, STRING, TEXT, BOOLEAN).

        A declared column skips type sniffing entirely and is parsed STRICTLY as
        that type: no sample window, no widening, no VARCHAR fallback. A value
        that does not fit raises RuntimeError naming the column, the value and
        the declared type — `fail_on_error=False` does NOT apply, because it
        exists to soften a GUESS made from a sample and a declared type is not a
        guess.

        Text forms go through draken's own parsers, so a value read here means
        exactly what the equivalent CAST would make it mean. IPV4 is dotted-quad
        ONLY (a bare integer, inet_aton shorthand "10.1", and leading-zero forms
        "010.1.1.1" all raise); DATE and TIMESTAMP are ISO-8601 text only, and
        converting to a declared unit is exact-or-refuse.

    Returns
    -------
    dict with keys:
        'success'      : bool
        'column_names' : list[str]
        'num_rows'     : int
        'columns'      : list of Draken Vector objects
    """
    cdef CsvParseContext    ctx
    cdef CsvPredicate       pred_cpp
    cdef vector[string]     column_names_cpp
    cdef uint32_t           num_cols = 0
    cdef size_t             header_offset = 0
    cdef size_t             n_threads
    cdef vector[uint32_t]   request_ordinals
    cdef vector[size_t]     proj_indices
    cdef StreamResult       stream_result
    cdef const uint8_t*     buf_ptr
    cdef size_t             buf_len
    cdef size_t             i
    cdef uint32_t           col_ord
    cdef DeclaredType       probe_type

    cdef dict result = {
        'success': False,
        'column_names': [],
        'num_rows': 0,
        'columns': [],
    }

    # ---- Build CsvParseContext ----
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("delimiter must be a single character")
    if not isinstance(infer_sample_size, int) or isinstance(infer_sample_size, bool) or infer_sample_size <= 0:
        raise ValueError("infer_sample_size must be a positive integer")
    ctx.delimiter          = ord(delimiter)
    ctx.has_header         = bool(has_header)
    ctx.sniff_sample_size  = <uint32_t>infer_sample_size
    ctx.ignore_errors      = not bool(fail_on_error)
    ctx.rebuild_lut()

    if explicit_schema:
        for col, declared_type in explicit_schema.items():
            if not isinstance(declared_type, str):
                raise ValueError(
                    f"read_csv: explicit_schema[{col!r}] = {declared_type!r} is not a "
                    f"type name; expected a string such as 'IPV4' or 'DECIMAL(18, 2)'"
                )
            # Validated EAGERLY, through the same parser that will do the work, so a
            # bad type name fails before any bytes are read rather than part-way
            # through a large file.
            declared_bytes = declared_type.encode('utf-8')
            if not parse_declared_type(declared_bytes, &probe_type):
                raise ValueError(
                    f"read_csv: explicit_schema[{col!r}] = {declared_type!r} is not a "
                    f"supported type; supported types are "
                    f"{declared_type_vocabulary().decode('utf-8')}"
                )
            ctx.explicit_schema[col.encode('utf-8')] = declared_bytes

    if columns:
        for col in columns:
            ctx.projected_columns.push_back(col.encode('utf-8'))

    if predicates:
        for col, op, val in predicates:
            pred_cpp.column = col.encode('utf-8')
            pred_cpp.op     = <uint8_t>_csv_parse_op(op)
            # val is bytes for every real Opteryx-pushed VARCHAR literal (its VARCHAR
            # storage is byte-based, not str) -- str(b'foo') == "b'foo'", the Python repr,
            # not the string's own bytes. Same bug/fix as rugo/src/jsonl/_jsonl_reader.pxi's
            # predicate-value encoding. CSV never produces a BOOL column (see
            # opteryx/planner/binder/dataset.py's _CSV_SUPPORTED_TYPES), so no bool case here.
            pred_cpp.value  = val if isinstance(val, bytes) else str(val).encode('utf-8')
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

    # ---- Resolve column names and ordinals ----
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

    # Build sorted request_ordinals (projected ∪ predicate columns)
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

    # Build proj_indices: for each projected name, its index in request_ordinals
    ord_to_req_idx = {}
    for i in range(request_ordinals.size()):
        ord_to_req_idx[<int>request_ordinals[i]] = <int>i

    output_col_names = []
    for name in proj_names:
        if name in name_to_ord:
            col_ord = <uint32_t>name_to_ord[name]
            if <int>col_ord in ord_to_req_idx:
                proj_indices.push_back(<size_t>ord_to_req_idx[<int>col_ord])
                output_col_names.append(name)

    if proj_indices.empty():
        result['success'] = True
        result['column_names'] = output_col_names
        result['num_rows'] = 0
        return result

    # ---- Phase 2: streaming build (split-find + sniff + parallel scan) ----
    n_threads = 0 if use_threads else 1
    with nogil:
        stream_result = build_columns_streaming(
            buf_ptr, buf_len, header_offset,
            column_names_cpp, num_cols,
            request_ordinals, proj_indices,
            ctx, n_threads
        )

    if stream_result.num_rows == 0:
        result['success']      = True
        result['column_names'] = output_col_names
        result['num_rows']     = 0
        return result

    # ---- Wrap columns under GIL ----
    draken_vectors = []
    for i in range(stream_result.columns.size()):
        draken_vectors.append(wrap_csv_column(stream_result.columns[i]))

    result['success']      = True
    result['column_names'] = output_col_names
    result['num_rows']     = <int>stream_result.num_rows
    result['columns']      = draken_vectors
    return result
