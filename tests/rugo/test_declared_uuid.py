"""CSV / JSONL `explicit_schema` UUID columns (docs/UUID_TYPE_DESIGN.md, ruling U12).

A column DECLARED UUID parses strictly (36-char 8-4-4-4-12, either case) into
DRAKEN_UUID; anything else is a declared-type mismatch that raises. Undeclared,
a uuid-shaped string stays VARCHAR — there is no inference. A pushed predicate on a
declared UUID column is refused: the readers compare a field's raw text, and UUID
text is case-insensitive, so evaluating it would answer `= 'ffff...'` with no rows
against a stored 'FFFF...'.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo import csv, jsonl

A = "550e8400-e29b-41d4-a716-446655440000"
C = "FFFFFFFF-0000-0000-0000-000000000001"
WANT = [uuid.UUID(A), None, uuid.UUID(C)]

CSV = f"id,n\n{A},1\n,2\n{C},3\n".encode()
JSONL = f'{{"id":"{A}","n":1}}\n{{"id":null,"n":2}}\n{{"id":"{C}","n":3}}\n'.encode()


def _columns(reader):
    out = {}
    with reader as r:
        for morsel in r:
            for name in morsel.column_names:
                out.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return out


def _read(fmt, data, **kwargs):
    return _columns((csv.read_csv if fmt == "csv" else jsonl.read_jsonl)(data, **kwargs))


@pytest.mark.parametrize("fmt,data", [("csv", CSV), ("jsonl", JSONL)])
def test_declared_uuid_reads_as_uuid(fmt, data):
    got = _read(fmt, data, explicit_schema={"id": "UUID"})
    assert got["id"] == WANT
    assert got["n"] == [1, 2, 3]


@pytest.mark.parametrize("bad", ["nope", "{" + A + "}", A.replace("-", ""), "urn:uuid:" + A])
def test_declared_uuid_is_strict(bad):
    with pytest.raises(ValueError, match="UUID"):
        _read("jsonl", f'{{"id":"{bad}"}}\n'.encode(), explicit_schema={"id": "UUID"})
    # The CSV reader raises its declared-type mismatches as RuntimeError.
    with pytest.raises(RuntimeError, match="not a valid UUID"):
        _read("csv", f"id\n{bad}\n".encode(), explicit_schema={"id": "UUID"})


def test_a_json_number_is_not_a_uuid():
    with pytest.raises(ValueError, match="UUID"):
        _read("jsonl", b'{"id":123}\n', explicit_schema={"id": "UUID"})


def test_undeclared_uuid_text_stays_varchar():
    assert _read("jsonl", JSONL)["id"][0] == A
    assert _read("csv", CSV)["id"][0] == A


# Pushed predicates compare the parsed 16 bytes: case never matters, and byte order
# is the UUID order. A (550e...) < C (ffff...); row 2 is NULL and matches nothing.
@pytest.mark.parametrize("fmt,data", [("csv", CSV), ("jsonl", JSONL)])
@pytest.mark.parametrize("op,literal,want", [
    ("==", C.lower(), [3]),
    ("==", C, [3]),
    ("==", A.upper(), [1]),
    ("!=", C.lower(), [1]),
    ("<", C.lower(), [1]),
    ("<=", A.upper(), [1]),
    (">", A, [3]),
    (">=", A, [1, 3]),
])
def test_pushed_predicate_compares_uuid_bytes(fmt, data, op, literal, want):
    got = _read(fmt, data, explicit_schema={"id": "UUID"}, predicates=[("id", op, literal)])
    assert got.get("n", []) == want


def test_jsonl_in_list_on_a_uuid_column():
    got = _read("jsonl", JSONL, explicit_schema={"id": "UUID"},
                predicates=[("id", "in", [C.lower(), "00000000-0000-0000-0000-000000000000"])])
    assert got["n"] == [3]


@pytest.mark.parametrize("fmt,data", [("csv", CSV), ("jsonl", JSONL)])
def test_a_predicate_literal_that_is_not_a_uuid_raises(fmt, data):
    with pytest.raises(ValueError, match="is not a UUID"):
        _read(fmt, data, explicit_schema={"id": "UUID"}, predicates=[("id", "==", "nope")])
    with pytest.raises(ValueError, match="UUID"):
        _read(fmt, data, explicit_schema={"id": "UUID"}, predicates=[("id", "==", 5)])
