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


def _load(source: Source) -> bytes:
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    raise TypeError("source must be a filename (str) or bytes/bytearray/memoryview")


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
            raise RuntimeError(result.get("error", "JSONL read failed"))
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
    data = _load(source)
    result = _read_jsonl(data, columns=None, predicates=None)
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
