"""SQL-surface tests for READ_CSV(path, ...) -- the binder branch in
opteryx.planner.binder.dataset and the CsvReadNode physical operator.

Coverage: basic read, separator/has_header_row options, projection + predicate
pushdown, glob support, unrecognized/malformed options, and the
ignore_errors/infer_sample_size options added alongside the rugo type-mismatch
fix (see tests/rugo/test_csv_reader.py for the native-layer coverage of that
fix itself).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidFunctionParameterError


def _run(sql):
    session = opteryx.session()
    return list(session.execute_to_morsels(sql))


def _rows(morsels):
    return sum(m.num_rows for m in morsels)


def test_basic_read_all_columns(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,score\n1,Alice,95\n2,Bob,82\n3,Carol,77\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{csv_file}')")
    assert _rows(morsels) == 3
    names = {n.decode() if isinstance(n, bytes) else n for m in morsels for n in m.column_names}
    assert names == {"id", "name", "score"}


def test_projection_and_predicate_pushdown(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,score\n1,Alice,95\n2,Bob,82\n3,Carol,50\n")

    morsels = _run(f"SELECT name FROM READ_CSV('{csv_file}') WHERE score > 60")
    names = [v for m in morsels for v in m.column(b"name").to_pylist()]
    assert sorted(names) == ["Alice", "Bob"]


def test_has_header_row_false_uses_positional_names(tmp_path):
    csv_file = tmp_path / "noheader.csv"
    csv_file.write_text("1,Alice,95\n2,Bob,82\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{csv_file}', has_header_row=>false)")
    names = {n.decode() if isinstance(n, bytes) else n for m in morsels for n in m.column_names}
    assert names == {"col_0", "col_1", "col_2"}
    assert _rows(morsels) == 2


def test_separator_option(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id;name;score\n1;Alice;95\n2;Bob;82\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{csv_file}', separator=>';')")
    assert _rows(morsels) == 2


def test_glob_reads_every_matched_file(tmp_path):
    (tmp_path / "a.csv").write_text("id,val\n1,10\n2,20\n")
    (tmp_path / "b.csv").write_text("id,val\n3,30\n4,40\n")

    morsels = _run(f"SELECT * FROM READ_CSV('{tmp_path}/*.csv')")
    assert _rows(morsels) == 4


def test_unrecognized_option_rejected(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a\n1\n")

    with pytest.raises(InvalidFunctionParameterError):
        _run(f"SELECT * FROM READ_CSV('{csv_file}', bogus_option=>1)")


def test_separator_must_be_single_character(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a\n1\n")

    with pytest.raises(InvalidFunctionParameterError):
        _run(f"SELECT * FROM READ_CSV('{csv_file}', separator=>'::')")


def test_infer_sample_size_must_be_positive(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a\n1\n")

    with pytest.raises(InvalidFunctionParameterError):
        _run(f"SELECT * FROM READ_CSV('{csv_file}', infer_sample_size=>0)")


def test_type_mismatch_past_sample_window_fails_loud(tmp_path):
    csv_file = tmp_path / "mismatch.csv"
    rows = "\n".join(f"{i},{i}" for i in range(10))
    csv_file.write_text(f"id,val\n{rows}\nX,notanumber\n")

    with pytest.raises(DatasetReadError):
        _run(f"SELECT * FROM READ_CSV('{csv_file}', infer_sample_size=>3)")


def test_ignore_errors_nulls_mismatched_value_instead_of_failing(tmp_path):
    csv_file = tmp_path / "mismatch.csv"
    rows = "\n".join(f"{i},{i}" for i in range(10))
    csv_file.write_text(f"id,val\n{rows}\n10,notanumber\n")

    morsels = _run(
        f"SELECT val FROM READ_CSV('{csv_file}', infer_sample_size=>3, ignore_errors=>true)"
    )
    values = [v for m in morsels for v in m.column(b"val").to_pylist()]
    assert values[-1] is None
    assert values[:-1] == list(range(10))


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
