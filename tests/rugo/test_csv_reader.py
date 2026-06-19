"""
Tests for rugo.csv.read_csv

Coverage:
  - basic read (all columns)
  - projection pushdown
  - predicate pushdown (eq, lt, gt)
  - projection + predicate combined
  - quoted fields: embedded delimiter, embedded newline
  - escape styles: \"  and  ""
  - CRLF line endings
  - has_header=False  (col_0, col_1 ... names)
  - TSV (tab delimiter)
  - empty field → null
  - type inference: int64, float64, VARCHAR
  - mixed types → VARCHAR
  - no trailing newline
  - empty input
  - predicate on projected-out column still filters
  - all-null column validity bitmap
"""

import pytest

import draken  # noqa: F401 — must precede rugo.csv to resolve draken symbols
from rugo.csv import read_csv

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_list(draken_vec):
    """Convert a DrakenVector to a Python list (None for nulls)."""
    return draken_vec.to_pylist()


# ---------------------------------------------------------------------------
# Basic read
# ---------------------------------------------------------------------------


def test_basic_all_columns():
    csv = b"id,name,value\n1,alice,3.14\n2,bob,2.71\n3,charlie,1.41\n"
    r = read_csv(csv)
    assert r["success"]
    assert r["column_names"] == ["id", "name", "value"]
    assert r["num_rows"] == 3
    assert len(r["columns"]) == 3
    assert _to_list(r["columns"][0]) == [1, 2, 3]
    assert _to_list(r["columns"][1]) == ["alice", "bob", "charlie"]


def test_basic_no_trailing_newline():
    csv = b"id,name\n10,foo\n20,bar"
    r = read_csv(csv)
    assert r["success"]
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [10, 20]


def test_empty_input():
    r = read_csv(b"")
    assert r["success"]
    assert r["num_rows"] == 0
    assert r["columns"] == []


def test_header_only_no_data():
    r = read_csv(b"id,name,value\n")
    assert r["success"]
    assert r["num_rows"] == 0


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------


def test_projection_single_column():
    csv = b"a,b,c\n1,2,3\n4,5,6\n"
    r = read_csv(csv, columns=["b"])
    assert r["column_names"] == ["b"]
    assert len(r["columns"]) == 1
    assert _to_list(r["columns"][0]) == [2, 5]


def test_projection_reorder():
    csv = b"x,y,z\n10,20,30\n40,50,60\n"
    r = read_csv(csv, columns=["z", "x"])
    assert r["column_names"] == ["z", "x"]
    assert _to_list(r["columns"][0]) == [30, 60]
    assert _to_list(r["columns"][1]) == [10, 40]


def test_projection_unknown_column_ignored():
    csv = b"a,b\n1,2\n"
    r = read_csv(csv, columns=["a", "does_not_exist"])
    # only known columns returned
    assert r["column_names"] == ["a"]
    assert r["num_rows"] == 1


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------


def test_predicate_eq():
    csv = b"id,name\n1,alice\n2,bob\n3,charlie\n"
    r = read_csv(csv, predicates=[("id", "==", 2)])
    assert r["num_rows"] == 1
    assert _to_list(r["columns"][0]) == [2]


def test_predicate_lt():
    csv = b"v\n10\n20\n30\n40\n"
    r = read_csv(csv, predicates=[("v", "<", 25)])
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [10, 20]


def test_predicate_gt():
    csv = b"v\n10\n20\n30\n40\n"
    r = read_csv(csv, predicates=[("v", ">", 25)])
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [30, 40]


def test_predicate_no_survivors():
    csv = b"v\n1\n2\n3\n"
    r = read_csv(csv, predicates=[("v", ">", 100)])
    assert r["success"]
    assert r["num_rows"] == 0


def test_predicate_on_non_projected_column():
    """Predicate column not in projection still filters rows correctly."""
    csv = b"id,score\n1,50\n2,80\n3,30\n"
    r = read_csv(csv, columns=["id"], predicates=[("score", ">", 60)])
    assert r["column_names"] == ["id"]
    assert r["num_rows"] == 1
    assert _to_list(r["columns"][0]) == [2]


# ---------------------------------------------------------------------------
# Quoted fields
# ---------------------------------------------------------------------------


def test_quoted_field_with_embedded_delimiter():
    csv = b'id,name\n1,"smith, john"\n2,doe\n'
    r = read_csv(csv)
    assert r["num_rows"] == 2
    names = _to_list(r["columns"][1])
    assert names[0] == "smith, john"
    assert names[1] == "doe"


def test_quoted_field_with_embedded_newline():
    csv = b'id,note\n1,"line one\nline two"\n2,plain\n'
    r = read_csv(csv)
    assert r["num_rows"] == 2
    notes = _to_list(r["columns"][1])
    assert notes[0] == "line one\nline two"
    assert notes[1] == "plain"


def test_backslash_escape_in_quoted():
    # \" inside a quoted field → "
    csv = b'id,val\n1,"say \\"hello\\""\n'
    r = read_csv(csv)
    assert r["num_rows"] == 1
    assert _to_list(r["columns"][1])[0] == 'say "hello"'


def test_doubled_quote_escape():
    # "" inside a quoted field → "
    csv = b'id,val\n1,"say ""hello"""\n'
    r = read_csv(csv)
    assert r["num_rows"] == 1
    assert _to_list(r["columns"][1])[0] == 'say "hello"'


# ---------------------------------------------------------------------------
# CRLF
# ---------------------------------------------------------------------------


def test_crlf_endings():
    csv = b"a,b\r\n1,2\r\n3,4\r\n"
    r = read_csv(csv)
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [1, 3]
    assert _to_list(r["columns"][1]) == [2, 4]


# ---------------------------------------------------------------------------
# has_header=False
# ---------------------------------------------------------------------------


def test_no_header():
    csv = b"1,alice,3.14\n2,bob,2.71\n"
    r = read_csv(csv, has_header=False)
    assert r["success"]
    assert r["column_names"] == ["col_0", "col_1", "col_2"]
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [1, 2]


def test_no_header_projection():
    csv = b"1,alice,3.14\n2,bob,2.71\n"
    r = read_csv(csv, columns=["col_2", "col_0"], has_header=False)
    assert r["column_names"] == ["col_2", "col_0"]
    # 3.14 and 2.71 are both valid floats → inferred as float64
    assert _to_list(r["columns"][0]) == pytest.approx([3.14, 2.71])
    assert _to_list(r["columns"][1]) == [1, 2]


# ---------------------------------------------------------------------------
# Delimiter variants
# ---------------------------------------------------------------------------


def test_tab_delimiter():
    tsv = b"id\tname\tval\n1\talice\t10\n2\tbob\t20\n"
    r = read_csv(tsv, delimiter="\t")
    assert r["column_names"] == ["id", "name", "val"]
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == [1, 2]


def test_semicolon_delimiter():
    csv = b"a;b\n1;2\n3;4\n"
    r = read_csv(csv, delimiter=";")
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][1]) == [2, 4]


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------


def test_empty_unquoted_field_is_null():
    csv = b"a,b,c\n1,,3\n"
    r = read_csv(csv)
    assert r["num_rows"] == 1
    b_col = _to_list(r["columns"][1])
    assert b_col[0] is None


def test_empty_quoted_field_is_empty_string():
    csv = b'a,b\n1,""\n'
    r = read_csv(csv)
    b_col = _to_list(r["columns"][1])
    assert b_col[0] == ""


def test_all_null_column():
    csv = b"a,b\n1,\n2,\n3,\n"
    r = read_csv(csv)
    assert r["num_rows"] == 3
    b_col = _to_list(r["columns"][1])
    assert all(v is None for v in b_col)


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


def test_infers_int64():
    csv = b"n\n1\n-2\n999\n"
    r = read_csv(csv)
    col = _to_list(r["columns"][0])
    assert col == [1, -2, 999]


def test_infers_float64():
    csv = b"f\n1.5\n2.0\n-3.14\n"
    r = read_csv(csv)
    col = _to_list(r["columns"][0])
    assert col == pytest.approx([1.5, 2.0, -3.14])


def test_mixed_int_float_falls_back_to_float():
    csv = b"n\n1\n2.5\n3\n"
    r = read_csv(csv)
    col = _to_list(r["columns"][0])
    assert col == pytest.approx([1.0, 2.5, 3.0])


def test_mixed_type_falls_back_to_varchar():
    csv = b"v\n1\nhello\n3.14\n"
    r = read_csv(csv)
    col = _to_list(r["columns"][0])
    assert col == ["1", "hello", "3.14"]


def test_varchar_column():
    csv = b"s\nalpha\nbeta\ngamma\n"
    r = read_csv(csv)
    col = _to_list(r["columns"][0])
    assert col == ["alpha", "beta", "gamma"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_single_column_single_row():
    r = read_csv(b"x\n42\n")
    assert r["num_rows"] == 1
    assert _to_list(r["columns"][0]) == [42]


def test_single_row_many_columns():
    header = ",".join(f"c{i}" for i in range(20))
    values = ",".join(str(i) for i in range(20))
    csv = (header + "\n" + values + "\n").encode()
    r = read_csv(csv)
    assert r["num_rows"] == 1
    assert len(r["columns"]) == 20


def test_many_rows_no_threading():
    rows = "id,val\n" + "".join(f"{i},{i * 2}\n" for i in range(1000))
    r = read_csv(rows.encode(), use_threads=False)
    assert r["num_rows"] == 1000
    ids = _to_list(r["columns"][0])
    assert ids[0] == 0
    assert ids[999] == 999


def test_many_rows_with_threading():
    rows = "id,val\n" + "".join(f"{i},{i * 2}\n" for i in range(1000))
    r = read_csv(rows.encode(), use_threads=True)
    assert r["num_rows"] == 1000


def test_returns_dict_with_expected_keys():
    r = read_csv(b"a\n1\n")
    assert "success" in r
    assert "column_names" in r
    assert "num_rows" in r
    assert "columns" in r
