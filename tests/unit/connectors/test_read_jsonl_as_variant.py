"""READ_JSONL(path, as_variant => 'data'): each record whole, as one VARIANT column.

JSONBench's legal load (docs/parquet_variant_design.md, D5): the table is one `data VARIANT`
column holding the whole line, which the Parquet writer then shreds. Splitting top-level
keys into columns is flattening, which the benchmark does not allow.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import duckdb
import pytest

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError
from rugo import parquet as rp
from rugo.rugo_native import prepare_jsonl_context

LINES = [
    '{"kind":"commit","data":5,"n":{"a":[1,2]}}',
    '{"kind":"x","t":"\\u00e9","d":1.5e3}',
    "{}",
    '{"kind":"commit","n":{"a":[]},"z":null}',
]


def _rows(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return rows


@pytest.fixture
def jsonl(tmp_path):
    # Blank lines and whitespace around a record are not records.
    path = tmp_path / "w.jsonl"
    path.write_text(LINES[0] + "\n\n  " + LINES[1] + "  \n" + LINES[2] + "\n" + LINES[3] + "\n")
    return str(path)


def test_each_record_is_one_value(jsonl):
    assert _rows(f"SELECT * FROM READ_JSONL('{jsonl}', as_variant => 'data')") == [(l,) for l in LINES]


def test_paths_read_the_record_not_a_key_of_that_name(jsonl):
    # The first record has a top-level key named "data": `data` is still the whole record.
    rows = _rows(
        f"SELECT data->>'kind', data->'n', data->>'data' "
        f"FROM READ_JSONL('{jsonl}', as_variant => 'data')"
    )
    assert rows == [("commit", '{"a":[1,2]}', "5"), ("x", None, None), (None, None, None),
                    ("commit", '{"a":[]}', None)]


def test_a_filter_on_a_path(jsonl):
    rows = _rows(
        f"SELECT data->>'kind' FROM READ_JSONL('{jsonl}', as_variant => 'data') "
        f"WHERE data->>'kind' = 'commit'"
    )
    assert rows == [("commit",), ("commit",)]


def test_count_reads_no_column(jsonl):
    assert _rows(f"SELECT COUNT(*) FROM READ_JSONL('{jsonl}', as_variant => 'data')") == [(4,)]


@pytest.mark.parametrize("bad", ['{"a":1', "[1,2]", "5", '{"a":1} {"b":2}', '{"a":}'])
def test_a_line_that_is_not_one_object_fails(tmp_path, caplog, bad):
    path = tmp_path / "bad.jsonl"
    path.write_text(LINES[0] + "\n" + bad + "\n" + LINES[2] + "\n")
    # The engine reports a read failure redacted; the cause is in the log.
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data')")
    assert "Malformed JSONL at line 2" in caplog.text
    assert _rows(
        f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data', ignore_errors => true)"
    ) == [(LINES[0],), (LINES[2],)]


def _nested(arrays, inner=""):
    # The record's object is depth 0; its member `a` opens `arrays` arrays, the k-th at depth
    # k, around `inner` (a scalar there sits at depth arrays + 1). A string full of brackets,
    # opening with an escaped quote, sits beside it: brackets inside strings nest nothing.
    return '{"s":"\\"' + "[{" * 1500 + '","a":' + "[" * arrays + inner + "]" * arrays + "}"


# A VARIANT holds values to depth 1024 (rugo/src/variant_limit.hpp).
FITS = [_nested(1024), _nested(1023, "0")]
TOO_DEEP = [_nested(1025), _nested(1024, "0")]


@pytest.mark.parametrize("record", FITS)
def test_a_record_at_the_depth_limit_is_read_and_written(tmp_path, record):
    path = tmp_path / "deep.jsonl"
    path.write_text(LINES[0] + "\n" + record + "\n")
    morsels = list(opteryx.session().execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data')"
    ))
    assert [v for m in morsels for v in m.column("data").to_pylist()] == [LINES[0], record]
    # The writer holds the same limit: what the reader accepts, it writes.
    for m in morsels:
        rp.write_parquet(m)


@pytest.mark.parametrize("record", TOO_DEEP)
def test_a_record_deeper_than_a_variant_holds(tmp_path, caplog, record):
    path = tmp_path / "deep.jsonl"
    path.write_text(LINES[0] + "\n" + record + "\n" + LINES[2] + "\n")
    with pytest.raises(DatasetReadError):
        _rows(f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data')")
    assert "line 2" in caplog.text and "nested deeper than 1024 levels" in caplog.text
    # With ignore_errors it is skipped, and the query says so.
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data', ignore_errors => true)"
    ):
        rows.extend(morsel.column("data").to_pylist())
    assert rows == [LINES[0], LINES[2]]
    assert any("skipped 1 record(s) nested deeper than 1024" in m for m in session.messages)


def test_no_warning_when_nothing_is_skipped(jsonl):
    session = opteryx.session()
    for _ in session.execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{jsonl}', as_variant => 'data', ignore_errors => true)"
    ):
        pass
    assert not any("nested deeper" in m for m in session.messages)


def test_a_buffer_read_in_many_windows(tmp_path):
    # The map builder indexes 256 KB windows: every record keeps its own span across them.
    path = tmp_path / "big.jsonl"
    lines = [json.dumps({"i": i, "pad": "x" * (i % 97), "o": {"k": [i]}}, separators=(",", ":"))
             for i in range(80_000)]
    path.write_text("\n".join(lines) + "\n")
    assert os.path.getsize(path) > 16 * (256 << 10)
    rows = _rows(f"SELECT data FROM READ_JSONL('{path}', as_variant => 'data')")
    assert sorted(r[0] for r in rows) == sorted(lines)


@pytest.mark.parametrize(
    "options, message",
    [
        ("as_variant => 1", "non-empty string literal"),
        ("as_variant => ''", "non-empty string literal"),
        ("as_variant => 'data', infer_schema => false", "'infer_schema' cannot be used"),
        ("as_variant => 'data', infer_sample_size => 9", "'infer_sample_size' cannot be used"),
    ],
)
def test_refused_options(jsonl, options, message):
    with pytest.raises(InvalidFunctionParameterError, match=message):
        _rows(f"SELECT * FROM READ_JSONL('{jsonl}', {options})")


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"columns": ["data", "kind"]}, "only column"),
        ({"columns": ["data"], "predicates": [("data", "==", "x")]}, "no predicates"),
        ({"columns": ["data"], "explicit_schema": {"data": "VARCHAR"}}, "only schema"),
        ({"whole_record": "data->>'a'"}, "plain name"),
        ({"whole_record": ""}, "non-empty"),
    ],
)
def test_the_context_refuses_what_a_whole_record_cannot_mean(kwargs, message):
    kwargs.setdefault("whole_record", "data")
    with pytest.raises(ValueError, match=message):
        prepare_jsonl_context(**kwargs)


def test_written_as_a_shredded_parquet_variant(jsonl, tmp_path):
    # The JSONBench load: the whole-record column is written as a Parquet VARIANT, shredded,
    # and another reader returns every record as it was.
    morsels = list(opteryx.session().execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{jsonl}', as_variant => 'data')"
    ))
    assert len(morsels) == 1
    path = str(tmp_path / "w.parquet")
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsels[0]))
    names = [r[0] for r in duckdb.sql(f"SELECT name FROM parquet_schema('{path}')").fetchall()]
    assert "typed_value" in names and "kind" in names
    read = [json.loads(t) for (t,) in duckdb.sql(f"SELECT data::JSON::VARCHAR FROM '{path}'").fetchall()]
    assert read == [json.loads(l) for l in LINES]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
