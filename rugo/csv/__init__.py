"""
rugo.csv — unified read/write facade for CSV/TSV.

    from rugo import csv

    with csv.read_csv("data.csv", columns=["name"], predicates=[("age", ">", 30)]) as reader:
        for morsel in reader:
            ...

    meta = csv.read_metadata("data.csv")
    print(meta.num_rows, meta.schema_columns)

    data = csv.write_csv(morsel)
"""

from typing import Optional, Sequence, Tuple, Union

from rugo.rugo_native import read_csv as _read_csv
from rugo.rugo_native import write_csv as _write_csv

__all__ = ["read_csv", "read_metadata", "write_csv"]

Source = Union[str, bytes, bytearray, memoryview]
Predicate = Tuple[str, str, object]


def _load(source: Source) -> bytes:
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    raise TypeError("source must be a filename (str) or bytes/bytearray/memoryview")


class CsvMetadata:
    __slots__ = ("num_rows", "schema_columns")

    def __init__(self, num_rows: int, schema_columns: list):
        self.num_rows = num_rows
        self.schema_columns = schema_columns  # list of {"name": str, "type": str, "nullable": bool}

    def __repr__(self):
        return f"CsvMetadata(num_rows={self.num_rows}, columns={[c['name'] for c in self.schema_columns]})"


class _CsvReader:
    """Context-managed reader that yields a single Morsel of all matching rows."""

    def __init__(self, source, columns, predicates, delimiter, has_header, use_threads):
        self._source = source
        self._columns = columns
        self._predicates = predicates
        self._delimiter = delimiter
        self._has_header = has_header
        self._use_threads = use_threads
        self._result = None

    def __enter__(self) -> "_CsvReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        result = _read_csv(
            self._source,
            columns=self._columns,
            predicates=self._predicates,
            delimiter=self._delimiter,
            has_header=self._has_header,
            use_threads=self._use_threads,
        )
        if not result["success"]:
            raise RuntimeError("CSV read failed")
        from draken.morsels.morsel import Morsel
        yield Morsel.from_vectors(result["column_names"], result["columns"])


def read_csv(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[Predicate]] = None,
    delimiter: str = ",",
    has_header: bool = True,
    use_threads: bool = True,
) -> _CsvReader:
    """Open a CSV file or buffer for reading.

    Returns a context manager that yields one Morsel of the (projected, filtered) result.
    predicates: list of (column, op, value); op in ==, !=, <, <=, >, >=.
    """
    return _CsvReader(source, columns, predicates, delimiter, has_header, use_threads)


def read_metadata(source: Source) -> CsvMetadata:
    """Return CsvMetadata (num_rows, schema_columns) for a CSV file or buffer.

    Scans the full file to count rows; infers schema from the first few rows.
    """
    result = _read_csv(_load(source), columns=None, predicates=None)
    if not result["success"]:
        raise RuntimeError("CSV metadata read failed")
    schema_columns = [
        {"name": name, "type": "string", "nullable": True}
        for name in result["column_names"]
    ]
    return CsvMetadata(num_rows=result["num_rows"], schema_columns=schema_columns)


def write_csv(morsel, delimiter: str = ",", header: bool = True) -> bytes:
    """Serialize a Morsel to RFC 4180 CSV bytes."""
    return _write_csv(morsel, delimiter=delimiter, header=header)
