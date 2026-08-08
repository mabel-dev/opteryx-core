"""Record-less inputs to READ_JSONL / READ_CSV -- a zero-byte file, and a file
holding nothing but blank/whitespace lines.

Regression: an input file with no records carries no schema to infer. rugo's
JSONL reader yields NO morsel at all for one, and the bind-time sample read did
`next(iter(reader))` with no default, so a bare StopIteration escaped the
binder's generator:

    RuntimeError: generator raised StopIteration      (PEP 479)

`SELECT AVG(overall) FROM READ_JSONL('empty.jsonl')` was therefore unrunnable.
The same bare `next()` sat in the filesystem connector's JSONL schema inference
(a JSONL *dataset* whose first blob is empty) and in `read_csv_file`.

An empty file is a legitimately EMPTY RELATION, not a read failure. It binds
with zero columns, so `SELECT *` returns no rows and `COUNT(*)` returns 0;
naming a column fails loud with ColumnNotFoundError, which is honest -- the
relation genuinely has no such column -- rather than a leaked StopIteration.

Two things the fix must not do, both asserted below:

  * bind zero columns off an empty FIRST file in a glob. With no columns bound,
    both readers' zero-column branch deliberately suppresses the per-file
    schema-drift check, so the glob would silently return column-less rows for
    every other matched file. The schema source is the first matched file that
    actually holds a record; record-less files are skipped.
  * treat "rugo returned no columns" as schema drift. rugo.csv collapses
    record-less, every-row-filtered-by-predicates, and genuinely-absent-columns
    into the same zero-column morsel -- so the drift check used to fail an
    ordinary `WHERE col = <no match>` over a CSV instead of returning zero rows.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import DatasetReadError

# A zero-byte file, and files whose only content is blank/whitespace lines.
EMPTY = ""
WHITESPACE_JSONL = "   \n\n  \n"


def _values(sql):
    """Every value the query produced, flattened -- an aggregate's output column
    is named by identity, so this avoids naming it."""
    morsels = list(opteryx.session().execute_to_morsels(sql))
    return [v for m in morsels for n in m.column_names for v in m.column(n).to_pylist()]


def _rows(sql):
    return sum(m.num_rows for m in opteryx.session().execute_to_morsels(sql))


# ---------------------------------------------------------------------------
# JSONL -- the reported failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("content", [EMPTY, WHITESPACE_JSONL])
def test_jsonl_record_less_file_is_an_empty_relation(tmp_path, content):
    """`SELECT *` over a record-less JSONL file returns no rows, and no error."""
    path = tmp_path / "empty.jsonl"
    path.write_text(content)

    assert _rows(f"SELECT * FROM READ_JSONL('{path}')") == 0


@pytest.mark.parametrize("content", [EMPTY, WHITESPACE_JSONL])
def test_jsonl_record_less_file_counts_zero(tmp_path, content):
    """COUNT(*) is 0, not an error and not a silently-dropped morsel."""
    path = tmp_path / "empty.jsonl"
    path.write_text(content)

    assert _values(f"SELECT COUNT(*) FROM READ_JSONL('{path}')") == [0]


@pytest.mark.parametrize("content", [EMPTY, WHITESPACE_JSONL])
def test_jsonl_record_less_file_naming_a_column_fails_loud(tmp_path, content):
    """The exact query from the bug report. A record-less file has no columns,
    so naming one is a ColumnNotFoundError -- an actionable error naming the
    column, never `RuntimeError: generator raised StopIteration` and never a
    segfault."""
    path = tmp_path / "empty.jsonl"
    path.write_text(content)

    sql = f"SELECT AVG(overall) FROM READ_JSONL('{path}', infer_sample_size => 1000)"
    with pytest.raises(ColumnNotFoundError) as exc:
        _rows(sql)
    assert "overall" in str(exc.value)


def test_jsonl_record_less_file_with_a_predicate(tmp_path):
    """A pushed-down predicate over a record-less file is still zero rows --
    the predicate path must not resurrect the schema-drift error."""
    path = tmp_path / "empty.jsonl"
    path.write_text('{"a": 1}\n')
    empty = tmp_path / "a_empty.jsonl"
    empty.write_text(WHITESPACE_JSONL)

    assert _rows(f"SELECT a FROM READ_JSONL('{tmp_path}/*.jsonl') WHERE a = 99") == 0


# ---------------------------------------------------------------------------
# CSV -- the same hole, the same shape
# ---------------------------------------------------------------------------


def test_csv_empty_file_is_an_empty_relation(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text(EMPTY)

    assert _rows(f"SELECT * FROM READ_CSV('{path}')") == 0
    assert _values(f"SELECT COUNT(*) FROM READ_CSV('{path}')") == [0]


def test_csv_header_only_file_is_an_empty_relation(tmp_path):
    """rugo.csv drops the header along with the (absent) data rows, so a
    header-only file reports zero columns -- indistinguishable from a zero-byte
    file, and treated the same way."""
    path = tmp_path / "header.csv"
    path.write_text("a,b,c\n")

    assert _rows(f"SELECT * FROM READ_CSV('{path}')") == 0
    assert _values(f"SELECT COUNT(*) FROM READ_CSV('{path}')") == [0]


def test_csv_empty_file_naming_a_column_fails_loud(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text(EMPTY)

    with pytest.raises(ColumnNotFoundError) as exc:
        _rows(f"SELECT AVG(overall) FROM READ_CSV('{path}')")
    assert "overall" in str(exc.value)


def test_csv_whitespace_only_file_does_not_crash(tmp_path):
    """CSV has no blank-line-is-nothing rule: rugo reads `"   \\n\\n"` as a
    one-column header named `"   "` with one (null) data row. That is rugo's
    parser contract, not something this layer overrides -- what matters here is
    that it completes rather than raising. Pinned so a change in rugo's CSV
    parsing surfaces as a decision to make, not a silent behaviour shift."""
    path = tmp_path / "ws.csv"
    path.write_text("   \n\n")

    assert _rows(f"SELECT * FROM READ_CSV('{path}')") == 1


# ---------------------------------------------------------------------------
# Globs -- an empty file must not define (or erase) the schema
# ---------------------------------------------------------------------------


def test_jsonl_glob_skips_an_empty_first_file_for_the_schema(tmp_path):
    """`a_empty.jsonl` sorts first, so it used to be the schema source. Binding
    zero columns off it would suppress the drift check and silently return
    column-less rows for `b_data.jsonl`."""
    (tmp_path / "a_empty.jsonl").write_text(EMPTY)
    (tmp_path / "b_data.jsonl").write_text('{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n')

    assert _values(f"SELECT a, b FROM READ_JSONL('{tmp_path}/*.jsonl')") == [1, 3, 2, 4]
    assert _values(f"SELECT SUM(a) FROM READ_JSONL('{tmp_path}/*.jsonl')") == [4]
    assert _values(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*.jsonl')") == [2]


def test_csv_glob_skips_an_empty_first_file_for_the_schema(tmp_path):
    (tmp_path / "a_empty.csv").write_text(EMPTY)
    (tmp_path / "b_data.csv").write_text("a,b\n1,2\n3,4\n")

    assert _values(f"SELECT a, b FROM READ_CSV('{tmp_path}/*.csv')") == [1, 3, 2, 4]
    assert _values(f"SELECT SUM(a) FROM READ_CSV('{tmp_path}/*.csv')") == [4]
    assert _values(f"SELECT COUNT(*) FROM READ_CSV('{tmp_path}/*.csv')") == [2]


def test_jsonl_glob_with_a_record_less_file_after_the_schema_source(tmp_path):
    """The mirror case: the empty file sorts LAST, so the schema comes from the
    real file and the empty one is met at execution time. It contributes no
    rows; it is not schema drift."""
    (tmp_path / "a_data.jsonl").write_text('{"a": 1, "b": 2}\n')
    (tmp_path / "z_empty.jsonl").write_text(WHITESPACE_JSONL)

    assert _values(f"SELECT a, b FROM READ_JSONL('{tmp_path}/*.jsonl')") == [1, 2]


def test_csv_glob_with_an_empty_file_after_the_schema_source(tmp_path):
    (tmp_path / "a_data.csv").write_text("a,b\n1,2\n")
    (tmp_path / "z_empty.csv").write_text(EMPTY)

    assert _values(f"SELECT a, b FROM READ_CSV('{tmp_path}/*.csv')") == [1, 2]


def test_jsonl_glob_of_only_record_less_files(tmp_path):
    """No matched file holds a record -- the relation is empty, with no columns."""
    (tmp_path / "a.jsonl").write_text(EMPTY)
    (tmp_path / "b.jsonl").write_text(WHITESPACE_JSONL)

    assert _rows(f"SELECT * FROM READ_JSONL('{tmp_path}/*.jsonl')") == 0
    assert _values(f"SELECT COUNT(*) FROM READ_JSONL('{tmp_path}/*.jsonl')") == [0]


def test_csv_glob_of_only_empty_files(tmp_path):
    (tmp_path / "a.csv").write_text(EMPTY)
    (tmp_path / "b.csv").write_text(EMPTY)

    assert _rows(f"SELECT * FROM READ_CSV('{tmp_path}/*.csv')") == 0
    assert _values(f"SELECT COUNT(*) FROM READ_CSV('{tmp_path}/*.csv')") == [0]


# ---------------------------------------------------------------------------
# JSONL *datasets* -- the filesystem connector's own schema inference, a second
# copy of the same bare `next()` on a completely different code path
# ---------------------------------------------------------------------------


def test_jsonl_dataset_with_an_empty_first_blob(tmp_path):
    """`SELECT * FROM '<dir>'` over a JSONL dataset: the empty blob sorts first,
    so it used to be the schema source. Binding zero columns off it returns NO
    ROWS AT ALL while `b_data.jsonl` holds two -- a silent wrong answer, which is
    worse than the StopIteration crash it replaced."""
    (tmp_path / "a_empty.jsonl").write_text(EMPTY)
    (tmp_path / "b_data.jsonl").write_text('{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n')

    assert _values(f"SELECT a, b FROM '{tmp_path}'") == [1, 3, 2, 4]
    assert _values(f"SELECT SUM(a) FROM '{tmp_path}'") == [4]


def test_jsonl_dataset_of_only_record_less_blobs(tmp_path):
    (tmp_path / "a.jsonl").write_text(EMPTY)
    (tmp_path / "b.jsonl").write_text(WHITESPACE_JSONL)

    assert _rows(f"SELECT * FROM '{tmp_path}'") == 0
    assert _values(f"SELECT COUNT(*) FROM '{tmp_path}'") == [0]


# ---------------------------------------------------------------------------
# The zero-column morsel must not become a licence to skip drift detection
# ---------------------------------------------------------------------------


def test_genuine_column_drift_across_a_glob_still_fails(tmp_path):
    """A file that HAS records but not the expected columns is still a loud
    failure -- distinguishing "no records" from "wrong columns" must not have
    softened the drift check into silence."""
    (tmp_path / "a_data.jsonl").write_text('{"a": 1, "b": 2}\n')
    (tmp_path / "b_other.jsonl").write_text('{"x": 9, "y": 8}\n')

    with pytest.raises(DatasetReadError):
        _rows(f"SELECT a, b FROM READ_JSONL('{tmp_path}/*.jsonl')")


def test_csv_genuine_column_drift_across_a_glob_still_fails(tmp_path):
    (tmp_path / "a_data.csv").write_text("a,b\n1,2\n")
    (tmp_path / "b_other.csv").write_text("x,y\n9,8\n")

    with pytest.raises(DatasetReadError):
        _rows(f"SELECT a, b FROM READ_CSV('{tmp_path}/*.csv')")


def test_csv_predicate_matching_no_row_returns_zero_rows(tmp_path):
    """rugo.csv answers a predicate that matches nothing with a ZERO-COLUMN
    morsel, which the drift check read as "this file's columns [] do not match".
    An ordinary filtered CSV query was therefore unrunnable when it selected
    nothing."""
    path = tmp_path / "d.csv"
    path.write_text("a,b\n1,2\n3,4\n")

    assert _rows(f"SELECT * FROM READ_CSV('{path}') WHERE a = 99") == 0
    assert _values(f"SELECT COUNT(*) FROM READ_CSV('{path}') WHERE a = 99") == [0]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
