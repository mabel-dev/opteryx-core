"""
rugo.jsonl — unified read/write facade for JSONL.

    from rugo import jsonl

    with jsonl.read_jsonl("events.jsonl", columns=["id"], predicates=[("status", "==", "ok")]) as reader:
        for morsel in reader:
            ...

    meta = jsonl.read_metadata("events.jsonl")
    print(meta.num_rows, meta.schema_columns)

    data = jsonl.write_jsonl(morsel)
"""

from typing import Optional, Sequence, Tuple, Union

from rugo.rugo_native import get_jsonl_schema as _get_schema
from rugo.rugo_native import read_jsonl as _read_jsonl
from rugo.rugo_native import write_jsonl as _write_jsonl

__all__ = ["read_jsonl", "read_metadata", "write_jsonl"]

Source = Union[str, bytes, bytearray, memoryview]
Predicate = Tuple[str, str, object]


class JsonlMetadata:
    __slots__ = ("num_rows", "schema_columns")

    def __init__(self, num_rows: int, schema_columns: list):
        self.num_rows = num_rows
        self.schema_columns = schema_columns  # list of {"name": str, "type": str, "nullable": bool}

    def __repr__(self):
        return f"JsonlMetadata(num_rows={self.num_rows}, columns={[c['name'] for c in self.schema_columns]})"


class _JsonlReader:
    """Context-managed reader that yields a single Morsel of all matching rows."""

    def __init__(self, source, columns, predicates, explicit_schema,
                 infer_schema, infer_sample_size, parse_arrays, parse_objects,
                 fail_on_error, use_threads):
        self._source = source
        self._columns = columns
        self._predicates = predicates
        self._explicit_schema = explicit_schema
        self._infer_schema = infer_schema
        self._infer_sample_size = infer_sample_size
        self._parse_arrays = parse_arrays
        self._parse_objects = parse_objects
        self._fail_on_error = fail_on_error
        self._use_threads = use_threads

    def __enter__(self) -> "_JsonlReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        result = _read_jsonl(
            self._source,
            columns=self._columns,
            predicates=self._predicates,
            explicit_schema=self._explicit_schema,
            infer_schema=self._infer_schema,
            infer_sample_size=self._infer_sample_size,
            parse_arrays=self._parse_arrays,
            parse_objects=self._parse_objects,
            fail_on_error=self._fail_on_error,
            use_threads=self._use_threads,
        )
        if not result["success"]:
            # The native reader raises directly (OSError, NotImplementedError, ...)
            # on genuine failures -- it never reaches this line to report one via
            # the result dict. `success=False` here only ever means "zero rows
            # survived parsing/predicate filtering" (see read_jsonl's C
            # implementation: `result['success']` is set True only inside the
            # `total_rows > 0` branch, and is otherwise left at its False default
            # with no error populated). Treating that as an error broke predicate
            # pushdown for any chunk a predicate filters down to zero rows -- a
            # legitimate, common outcome, not a failure. Yield nothing instead.
            return
        from draken.morsels.morsel import Morsel
        yield Morsel.from_vectors(result["column_names"], result["columns"])


def read_jsonl(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[Predicate]] = None,
    explicit_schema=None,
    infer_schema: bool = True,
    infer_sample_size: int = 5,
    parse_arrays: bool = True,
    parse_objects: bool = True,
    fail_on_error: bool = True,
    use_threads: bool = True,
) -> _JsonlReader:
    """Open a JSONL file or buffer for reading.

    Returns a context manager that yields one Morsel of the (projected, filtered) result.
    predicates: list of (column, op, value); op in ==, !=, <, <=, >, >=.

    explicit_schema: optional {column_name: type} dict. The type is a PLATFORM-CANONICAL
        type name — the same string a stored schema holds — so a caller that already knows
        the destination schema can pass it straight through with no translation table:

            INT8 INT16 INT32 INT64 · UINT8 UINT16 UINT32 UINT64 · FLOAT32 FLOAT64 · BOOL
            VARCHAR · DATE · TIMESTAMP[s|ms|us|ns] · DECIMAL(p, s) · IPV4

        matched case-insensitively, with the usual SQL aliases (INTEGER, BIGINT, TINYINT,
        SMALLINT, DOUBLE, FLOAT, REAL, STRING, TEXT, BOOLEAN). The four original names —
        "int64", "double", "boolean", "string" — keep working unchanged through that table.

        A named column is parsed STRICTLY as that type — no speculative inference, no
        widening, no fallback — and raises ValueError naming the column, row and value if
        anything doesn't fit. Declared columns are always reported back in the returned
        schema dict (see read_metadata / get_jsonl_schema), independent of infer_schema.

        Text forms go through draken's own parsers, so a value read here means exactly what
        the equivalent CAST would make it mean. Two consequences worth knowing:
          · IPV4 is DOTTED-QUAD ONLY. A bare integer raises, as do inet_aton shorthand
            ("10.1") and leading-zero/octal forms ("010.1.1.1") — a reader and an access
            rule disagreeing about which address a value denotes is a security bug.
          · TIMESTAMP and DATE are ISO-8601 TEXT ONLY; an epoch integer raises. Converting
            to a declared unit is exact-or-refuse, so TIMESTAMP[s] rejects a value carrying
            sub-second precision rather than truncating it.
    infer_schema: whether non-declared columns appear in the returned schema dict at all
        (the underlying Draken vectors are always typed the same way regardless — this
        only gates the reported metadata). Declared (explicit_schema) columns are always
        reported.
    infer_sample_size: how many leading records are consulted to decide BOTH which columns
        exist and what type each one is. Must be a positive integer; defaults to 5.

        Columns: the relation's column set is the UNION of the keys across these records,
        in first-seen order. NDJSON is not required to be homogeneous, so a key that is
        absent from record 0 but present in record 3 is still a real column at the default
        of 5. A key that first appears only AFTER this window is not a column at all, and
        its values are unreachable — raise infer_sample_size to see it.

        Types: caps how many leading rows of a non-declared column are consulted
        for its type hint (the first non-null value in that window). Whatever hint is
        picked, the WHOLE column is still validated against it and falls back to VARCHAR
        on any mismatch, so no value is ever misparsed or lost — but if the sample window
        contains no non-null value at all (e.g. a column that's null for its first N rows
        then numeric), no hint is ever formed and the column reports/renders as VARCHAR
        even though a larger sample would have typed it as int64/double/boolean.
    fail_on_error: when True, a malformed line (one that never opens a record, an object/
        key abandoned by an unexpected newline, or a truncated/unterminated array or
        object) raises ValueError naming the 1-based line number, byte offset, and a
        snippet. When False (or a malformed line occurs elsewhere in a larger class of
        inputs this detector doesn't cover — e.g. a record truncated at end-of-file with
        no closing brace and no trailing newline), the malformed line is silently
        dropped, matching this reader's original lenient behaviour.
    parse_objects: when True, a column whose sampled value is a JSON object is returned
        as a VARIANT vector (same raw-JSON-text storage as VARCHAR, just tagged
        differently) instead of VARCHAR. When False, objects are returned as VARCHAR raw
        JSON text.
    parse_arrays: when True, a column whose sampled value is a JSON array is materialized
        as an ARRAY vector, PROVIDED every element across every row is a uniform scalar
        kind (all-int/double, all-boolean, all-string, or all-null/empty; ints widen to
        double). Nested containers (an array inside an array) or a genuine mix of scalar
        kinds (e.g. [1, "a", true]) are out of scope: that column falls back to raw JSON
        text (VARCHAR, same as parse_arrays=False) and raises a RuntimeWarning naming the
        column. When False, arrays are always returned as VARCHAR raw JSON text.
    """
    return _JsonlReader(
        source, columns, predicates, explicit_schema,
        infer_schema, infer_sample_size, parse_arrays, parse_objects,
        fail_on_error, use_threads,
    )


def read_metadata(source: Source) -> JsonlMetadata:
    """Return JsonlMetadata (num_rows, schema_columns) for a JSONL file or buffer.

    Reads the full file to count rows; infers schema from a sample.
    """
    result = _read_jsonl(source, columns=None, predicates=None)
    if not result["success"]:
        raise RuntimeError(result.get("error", "JSONL metadata read failed"))
    schema_columns = [
        {"name": name, "type": result["schema"].get(name, "object"), "nullable": True}
        for name in result["column_names"]
    ]
    return JsonlMetadata(num_rows=result["num_rows"], schema_columns=schema_columns)


def write_jsonl(morsel) -> bytes:
    """Serialize a Morsel to JSONL bytes (one JSON object per row)."""
    return _write_jsonl(morsel)
