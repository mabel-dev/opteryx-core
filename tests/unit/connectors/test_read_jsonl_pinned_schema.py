# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""READ_JSONL pins the bind-time schema onto every chunk of every file.

The schema is resolved once from the first record-bearing chunk of the first
matched file and passed to rugo as `explicit_schema` for every decode after
that (opteryx/connectors/jsonl_io, JsonlReadNode). Before this, every chunk
was re-inferred from its own 5-row sample, so a column that happened to be
null for the first rows of a later chunk (or file) drifted to VARCHAR and the
whole query failed on a "type mismatch" that was never in the data.

A glob over several small files is the cheapest way to reach a second decode
with a different first-row shape; within one file the same logic runs per
64MB chunk.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetReadError


def _values(sql):
    """Every value the query produced, flattened, in output order."""
    morsels = list(opteryx.session().execute_to_morsels(sql))
    return [v for m in morsels for n in m.column_names for v in m.column(n).to_pylist()]


def _write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    return path


def test_a_column_null_for_the_first_rows_of_a_later_file_keeps_its_bound_type(tmp_path):
    """The exact drift that used to fail: file 2's sample window has no non-null
    `a`, so on its own it would have inferred VARCHAR; pinned, it reads INT64."""
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", ('{"a": null, "b": "y"}\n' * 5) + '{"a": 7, "b": "z"}\n')
    assert _values(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl') ORDER BY b, a") == [
        1, None, None, None, None, None, 7
    ]


def test_a_sparse_key_is_not_drift(tmp_path):
    """The key is present on SOME record of the later file; the rows that lack it
    read as NULL, which is what a missing key means in NDJSON."""
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"b": "y"}\n{"a": 2, "b": "z"}\n')
    assert _values(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl') ORDER BY b") == [1, None, 2]


def test_a_file_missing_a_bound_column_entirely_fails_loud_naming_both(tmp_path):
    """Column drift across a glob stays a loud failure (the decision the older
    per-chunk name check recorded): a file where a bound key appears on NO record
    is a different file than the schema came from, not a column of NULLs."""
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"b": "y"}\n')
    with pytest.raises(DatasetReadError) as err:
        _values(f"SELECT a, b FROM READ_JSONL('{tmp_path}/*.jsonl')")
    message = str(err.value)
    assert "b.jsonl" in message and "['a']" in message and "absent from every record" in message


def test_a_file_with_none_of_the_bound_columns_fails_loud_naming_the_file(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1, "b": "x"}\n')
    _write(tmp_path, "b.jsonl", '{"c": 1}\n{"d": 2}\n')
    with pytest.raises(DatasetReadError) as err:
        _values(f"SELECT a, b FROM READ_JSONL('{tmp_path}/*.jsonl')")
    message = str(err.value)
    assert "b.jsonl" in message and "['a', 'b']" in message


def test_a_value_that_does_not_fit_the_bound_type_fails_loud_naming_it(tmp_path):
    """Pinning is a contract, not a guess: a string in a column bound INT64 is a
    data error that names the file, column, row and value."""
    _write(tmp_path, "a.jsonl", '{"a": 1}\n')
    _write(tmp_path, "b.jsonl", '{"a": 2}\n{"a": "oops"}\n')
    with pytest.raises(DatasetReadError) as err:
        _values(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl')")
    message = str(err.value)
    assert "b.jsonl" in message
    assert "column 'a'" in message and "row 1" in message and "oops" in message


def test_structured_columns_pin_too(tmp_path):
    """VARIANT and ARRAY<T> are what made pinning possible: the bound spelling
    ARRAY<INT64> / VARIANT round-trips through rugo's declared vocabulary."""
    _write(tmp_path, "a.jsonl", '{"v": {"k": 1}, "l": [1, 2]}\n')
    _write(tmp_path, "b.jsonl", '{"v": null, "l": null}\n{"v": {"k": 2}, "l": [3]}\n')
    values = _values(f"SELECT l FROM READ_JSONL('{tmp_path}/*.jsonl')")
    assert sorted(values, key=lambda x: (x is None, x or [])) == [[1, 2], [3], None]


def test_count_star_over_a_glob_needs_no_pinning(tmp_path):
    _write(tmp_path, "a.jsonl", '{"a": 1}\n')
    _write(tmp_path, "b.jsonl", '{"z": 1}\n{"z": 2}\n')
    assert _values(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*.jsonl')") == [3]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
