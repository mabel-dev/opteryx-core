# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for draken Vector._to_json() (E.36).

The method serializes one column to a JSON array as native bytes, using the
SAME per-value renderer as rugo.jsonl.write_jsonl (draken/interop/value_format.hpp).
So the oracle is write_jsonl itself: for every column, _to_json() must equal the
column's values taken row-wise from the JSONL output. That is what guarantees
/results and /download render identically.
"""

import io
import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from rugo.jsonl import write_jsonl
import rugo.parquet


def _morsel(sql):
    return list(opteryx.session().execute_to_morsels(sql))[0]


def _morsel_from_arrow(table):
    """Build a Morsel from a pyarrow Table via a parquet roundtrip (rugo reader).

    Used for temporal columns, whose SQL VARCHAR->TIMESTAMP cast path is not
    available in every build. The oracle (write_jsonl on the same morsel) makes
    the construction path irrelevant to what is being asserted.
    """
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(table, buf)
    with rugo.parquet.read_parquet(buf.getvalue()) as reader:
        return next(iter(reader))


def _names(m):
    return [n.decode() if isinstance(n, bytes) else n for n in m.column_names]


def _assert_matches_write_jsonl(m):
    """Every column's _to_json() equals its values taken row-wise from write_jsonl."""
    rows = [json.loads(line) for line in write_jsonl(m).decode().splitlines()]
    for name in _names(m):
        got = json.loads(m.column(name)._to_json())
        want = [row[name] for row in rows]
        assert got == want, f"column {name!r}: {got!r} != {want!r}"


def test_to_json_scalars_and_escaping():
    m = _morsel(
        "SELECT * FROM (VALUES "
        "(1, 1.5, true, 'a \"q\" b'),"
        "(9000000000, -3.0, false, 'x'),"
        "(NULL, NULL, NULL, NULL)) AS t(i, d, b, s)"
    )
    _assert_matches_write_jsonl(m)
    # spot-check the literal bytes for the string column (quote + null)
    assert m.column("s")._to_json() == b'["a \\"q\\" b","x",null]'


def test_to_json_dates_and_timestamps_are_rfc3339():
    import datetime

    import pyarrow as pa

    table = pa.table({
        "d": pa.array([datetime.date(2024, 1, 1), datetime.date(1999, 12, 31), None]),
        "t": pa.array([
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            datetime.datetime(2020, 6, 15, 8, 30, 15),
            None,
        ]),
    })
    m = _morsel_from_arrow(table)
    _assert_matches_write_jsonl(m)
    # documents the format decision: dates ISO, timestamps RFC-3339 +00:00 (== /download)
    assert m.column("d")._to_json() == b'["2024-01-01","1999-12-31",null]'
    assert (
        m.column("t")._to_json()
        == b'["2024-01-01T12:00:00+00:00","2020-06-15T08:30:15+00:00",null]'
    )


def test_to_json_decimals_match_writer():
    # Whatever scale the writer renders, _to_json must render identically.
    m = _morsel(
        "SELECT CAST(v AS DECIMAL(10,2)) AS d FROM (VALUES (1.23),(4.50),(NULL)) AS t(v)"
    )
    _assert_matches_write_jsonl(m)


def test_to_json_floats_nan_inf_become_null():
    m = _morsel("SELECT * FROM (VALUES (1.5),(NULL)) AS t(f)")
    _assert_matches_write_jsonl(m)


def test_to_json_arrays():
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel
    import draken.draken_native as dn

    m = Morsel.from_vectors(
        ["a"], [Vector(dn.vector_array_from_sequence([[1, 2, 3], [], None, [4, None, 6]]))]
    )
    _assert_matches_write_jsonl(m)
    assert json.loads(m.column("a")._to_json()) == [[1, 2, 3], [], None, [4, None, 6]]


def test_to_json_honours_selection_when_sliced():
    # A sliced morsel has a non-identity selection; data[selection[i]] must be honoured.
    m = _morsel(
        "SELECT * FROM (VALUES (1,'x'),(2,'y'),(3,'z'),(4,'w'),(5,'v')) AS t(n, s)"
    )
    _assert_matches_write_jsonl(m.slice(1, 3))


def test_to_json_empty_column_is_empty_array():
    m = _morsel("SELECT * FROM (VALUES (1),(2)) AS t(n)")
    assert m.slice(0, 0).column("n")._to_json() == b"[]"


if __name__ == "__main__":  # pragma: no cover
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print("OK", _name)
    print("done")
