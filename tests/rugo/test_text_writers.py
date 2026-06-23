# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the native CSV / JSONL writers (rugo.csv.write_csv,
rugo.jsonl.write_jsonl). Python's csv/json modules are the oracle.
"""

import csv as _csv
import io
import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl


def _morsel(sql):
    return list(opteryx.session().execute_to_morsels(sql))[0]


def _vec_morsel(name, nb):
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    return Morsel.from_vectors([name], [Vector(nb)])


# ---------------- JSONL ----------------

def test_jsonl_scalars_and_escaping():
    m = _morsel(
        "SELECT * FROM (VALUES "
        "(1, 1.5, true, 'a \"q\" b'),"
        "(9000000000, -3.0, false, 'x'),"
        "(NULL, NULL, NULL, NULL)) AS t(i, d, b, s)"
    )
    rows = [json.loads(l) for l in write_jsonl(m).decode().splitlines()]
    assert rows[0] == {"i": 1, "d": 1.5, "b": True, "s": 'a "q" b'}
    assert rows[1] == {"i": 9000000000, "d": -3.0, "b": False, "s": "x"}
    assert rows[2] == {"i": None, "d": None, "b": None, "s": None}


def test_jsonl_arrays():
    import draken.draken_native as dn

    m = _vec_morsel("a", dn.vector_array_from_sequence([[1, 2, 3], [], None, [4, None, 6]]))
    rows = [json.loads(l) for l in write_jsonl(m).decode().splitlines()]
    assert [r["a"] for r in rows] == [[1, 2, 3], [], None, [4, None, 6]]


def test_jsonl_decimal():
    m = _morsel("SELECT CAST(v AS DECIMAL(10,2)) AS d FROM (VALUES (12.5),(-1.5),(NULL)) AS t(v)")
    rows = [json.loads(l) for l in write_jsonl(m).decode().splitlines()]
    assert [r["d"] for r in rows] == [12.5, -1.5, None]


def test_jsonl_date_timestamp_are_rfc3339():
    """Date is ISO-8601 full-date; timestamp is RFC 3339 (T separator, Z zone)."""
    import datetime

    m = _morsel(
        "SELECT CAST(d AS DATE) AS dt, CAST(CAST(d AS DATE) AS TIMESTAMP) AS ts "
        "FROM (VALUES ('2020-01-01')) AS t(d)"
    )
    row = json.loads(write_jsonl(m).decode().splitlines()[0])
    assert row["dt"] == "2020-01-01"
    assert row["ts"] == "2020-01-01T00:00:00Z"
    # both parse with the stdlib ISO parser
    assert datetime.date.fromisoformat(row["dt"]) == datetime.date(2020, 1, 1)
    assert datetime.datetime.fromisoformat(row["ts"]).tzinfo is not None


# ---------------- CSV ----------------

def test_csv_quoting_and_nulls():
    m = _morsel(
        "SELECT * FROM (VALUES "
        "(1, 'plain'),"
        "(2, 'has,comma'),"
        "(3, 'has\"quote'),"
        "(NULL, NULL)) AS t(i, s)"
    )
    rows = list(_csv.reader(io.StringIO(write_csv(m).decode())))
    assert rows[0] == ["i", "s"]                  # header
    assert rows[1] == ["1", "plain"]
    assert rows[2] == ["2", "has,comma"]          # comma field round-trips
    assert rows[3] == ["3", 'has"quote']          # doubled quote round-trips
    assert rows[4] == ["", ""]                    # nulls -> empty fields


def test_csv_no_header_and_delimiter():
    m = _morsel("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s)")
    out = write_csv(m, delimiter="\t", header=False).decode()
    assert out == "1\ta\n2\tb\n"


def test_csv_array_renders_as_json():
    import draken.draken_native as dn

    m = _vec_morsel("a", dn.vector_array_from_sequence([[1, 2, 3], None]))
    out = write_csv(m, header=False).decode()
    assert out == '"[1,2,3]"\n\n'  # array quoted; null list -> empty field/line
    rows = list(_csv.reader(io.StringIO(out)))
    assert rows[0] == ["[1,2,3]"]


def test_csv_roundtrips_through_rugo_reader():
    from rugo.csv import read_csv

    m = _morsel("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(i, s)")
    result = read_csv(write_csv(m))
    assert result["column_names"] == ["i", "s"]


if __name__ == "__main__":
    test_jsonl_scalars_and_escaping()
    test_jsonl_arrays()
    test_jsonl_decimal()
    test_jsonl_date_timestamp_are_rfc3339()
    test_csv_quoting_and_nulls()
    test_csv_no_header_and_delimiter()
    test_csv_array_renders_as_json()
    test_csv_roundtrips_through_rugo_reader()
    print("✅ okay")
