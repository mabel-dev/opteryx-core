"""Zero-projection reads over READ_CSV / READ_JSONL -- `SELECT COUNT(*)` and a
projection of only constants, neither of which reads any column.

Regression: both readers validate every decoded file against the bind-time schema
(rugo sniffs each file independently, so a glob's files can genuinely disagree).
That check compared the file's decoded column NAMES against the pushed-down
projection -- but rugo answers a `columns=[]` request with the file's FULL column
set, so a query projecting nothing compared 8 real columns against `[]` and failed:

    DatasetReadError READ_JSONL('events.jsonl'): this file's columns ['Company', ...]
    do not match the expected [] from the bind-time schema

It fired on a single non-glob file too, so `SELECT COUNT(*) FROM READ_CSV(one_file)`
was unrunnable. READ_PARQUET was unaffected (a bare COUNT(*) over a manifest is
answered by StatisticsOnlyResponseStrategy and never reaches a scan).

The fix must not merely silence the check: the zero-column morsel it now emits has
to carry its row count in `zero_col_rows`, or COUNT(*) quietly returns 0 -- a wrong
answer where there used to be a loud error. Hence the exact-value assertions here
and the cross-format parity against READ_PARQUET.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetReadError

ROWS = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 82},
    {"id": 3, "name": "Carol", "score": 50},
    {"id": 4, "name": "Dan", "score": 61},
]

CSV_TEXT = "id,name,score\n" + "".join(f"{r['id']},{r['name']},{r['score']}\n" for r in ROWS)
JSONL_TEXT = "".join(
    '{"id": %d, "name": "%s", "score": %d}\n' % (r["id"], r["name"], r["score"]) for r in ROWS
)


def _values(sql):
    """Every value the query produced, flattened -- the output column of an
    aggregate is named by identity, so this avoids naming it."""
    morsels = list(opteryx.session().execute_to_morsels(sql))
    return [v for m in morsels for n in m.column_names for v in m.column(n).to_pylist()]


def _rows(sql):
    return sum(m.num_rows for m in opteryx.session().execute_to_morsels(sql))


@pytest.fixture
def sources(tmp_path):
    """The same four rows as CSV, JSONL and Parquet, for cross-format parity."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    csv_file = tmp_path / "events.csv"
    csv_file.write_text(CSV_TEXT)
    jsonl_file = tmp_path / "events.jsonl"
    jsonl_file.write_text(JSONL_TEXT)
    parquet_file = tmp_path / "events.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([r["id"] for r in ROWS], type=pa.int64()),
                "name": pa.array([r["name"] for r in ROWS]),
                "score": pa.array([r["score"] for r in ROWS], type=pa.int64()),
            }
        ),
        parquet_file,
    )
    return {"CSV": csv_file, "JSONL": jsonl_file, "PARQUET": parquet_file}


@pytest.mark.parametrize("fmt", ["CSV", "JSONL"])
def test_count_star_over_a_single_file(sources, fmt):
    """The reported bug: a zero-projection read of ONE file, no glob involved."""
    assert _values(f"SELECT COUNT(*) FROM READ_{fmt}('{sources[fmt]}')") == [len(ROWS)]


@pytest.mark.parametrize("fmt", ["CSV", "JSONL"])
def test_count_star_matches_parquet(sources, fmt):
    """Cross-format parity -- the count must equal the format that never broke.
    Guards the silent-zero failure mode: a zero-column morsel that lost its row
    count still 'succeeds', it just answers 0."""
    parquet = _values(f"SELECT COUNT(*) FROM READ_PARQUET('{sources['PARQUET']}')")
    assert _values(f"SELECT COUNT(*) FROM READ_{fmt}('{sources[fmt]}')") == parquet == [len(ROWS)]


@pytest.mark.parametrize("fmt", ["CSV", "JSONL"])
def test_count_star_with_a_predicate(sources, fmt):
    """A zero projection whose predicate still needs a column read and filtered."""
    sql = f"SELECT COUNT(*) FROM READ_{fmt}('{sources[fmt]}') WHERE score > 60"
    parquet_sql = f"SELECT COUNT(*) FROM READ_PARQUET('{sources['PARQUET']}') WHERE score > 60"
    assert _values(sql) == _values(parquet_sql) == [3]


@pytest.mark.parametrize("fmt", ["CSV", "JSONL"])
def test_constant_only_projection(sources, fmt):
    """`SELECT <literal>` also projects no columns -- one row out per row in,
    which only holds if the zero-column morsel carried its row count."""
    values = _values(f"SELECT 3.14 FROM READ_{fmt}('{sources[fmt]}')")
    assert values == [3.14] * len(ROWS)


@pytest.mark.parametrize("fmt", ["CSV", "JSONL"])
def test_projecting_a_column_still_works(sources, fmt):
    """The non-empty projection path is untouched."""
    sql = f"SELECT COUNT(name) FROM READ_{fmt}('{sources[fmt]}')"
    assert _values(sql) == [len(ROWS)]


# ---- the drift check the empty-projection branch must NOT have disarmed --------


@pytest.fixture
def divergent_glob(tmp_path):
    """Two files with genuinely different columns: `b` in one, `c` in the other."""
    (tmp_path / "f1.csv").write_text("a,b\n1,x\n2,y\n")
    (tmp_path / "f2.csv").write_text("a,c\n3,z\n4,w\n")
    (tmp_path / "f1.jsonl").write_text('{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n')
    (tmp_path / "f2.jsonl").write_text('{"a": 3, "c": "z"}\n{"a": 4, "c": "w"}\n')
    return tmp_path


@pytest.mark.parametrize("fmt,ext", [("CSV", "csv"), ("JSONL", "jsonl")])
def test_column_drift_across_a_glob_still_fails(divergent_glob, fmt, ext):
    """Projecting a column across files whose columns disagree must still fail
    loud, naming the offending file.

    Two wordings are both the drift check doing its job: the file's decoded names
    disagreeing with the expectation, and (JSONL's per-file probe) the projected
    column being absent from the file altogether."""
    with pytest.raises(DatasetReadError) as exc:
        _rows(f"SELECT b FROM READ_{fmt}('{divergent_glob}/*.{ext}')")
    message = str(exc.value)
    assert "do not match the expected" in message or "were found in this file" in message
    assert "f2." + ext in message  # the offending file is named

    with pytest.raises(DatasetReadError):
        _rows(f"SELECT * FROM READ_{fmt}('{divergent_glob}/*.{ext}')")


@pytest.mark.parametrize("fmt,ext", [("CSV", "csv"), ("JSONL", "jsonl")])
def test_count_star_across_a_divergent_glob_counts_rows(divergent_glob, fmt, ext):
    """Counting rows does not read any column, so files whose columns disagree
    cannot change the answer -- 4 rows is correct, not a missed drift error.
    (READ_PARQUET counts the same glob shape without complaint too.)"""
    assert _values(f"SELECT COUNT(*) FROM READ_{fmt}('{divergent_glob}/*.{ext}')") == [4]


@pytest.fixture
def type_drift_glob(tmp_path):
    """Same column NAMES across both files, incompatible types for `b`."""
    (tmp_path / "f1.jsonl").write_text('{"a": 1, "b": 10}\n{"a": 2, "b": 20}\n')
    (tmp_path / "f2.jsonl").write_text('{"a": 3, "b": "ten"}\n{"a": 4, "b": "twenty"}\n')
    return tmp_path


def test_type_drift_across_a_glob_still_fails(type_drift_glob):
    """Name-level agreement is not enough -- the per-column type check must still
    fire for a projected column."""
    with pytest.raises(DatasetReadError) as exc:
        _rows(f"SELECT b FROM READ_JSONL('{type_drift_glob}/*.jsonl')")
    assert "at bind time" in str(exc.value)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
