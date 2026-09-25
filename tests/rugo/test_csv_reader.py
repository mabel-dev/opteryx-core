"""
Tests for rugo.csv.read_csv

Coverage:
  - basic read (all columns)
  - projection pushdown
  - predicate pushdown (eq, lt, gt)
  - projection + predicate combined
  - quoted fields: embedded delimiter, embedded newline
  - RFC 4180 quoting: "" is the only escape, backslash is literal
  - fields longer than 64 KiB are not truncated
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
from rugo.rugo_native import read_csv

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
    # No header means no columns: refused rather than returned as a zero-column result.
    with pytest.raises(ValueError, match="input is empty"):
        read_csv(b"")


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


def test_projection_all_unknown_columns_raises():
    with pytest.raises(ValueError, match="none of the requested columns"):
        read_csv(b"a,b\n1,2\n", columns=["x", "y"])


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


def test_backslash_is_literal_in_quoted():
    # RFC 4180: backslash is not an escape. "C:\" is a complete field whose
    # value is C:\ — the quote after the backslash CLOSES the field.
    csv = b'a,b,c\n1,"C:\\",2\n3,"x",4\n'
    r = read_csv(csv)
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][1]) == ["C:\\", "x"]
    assert _to_list(r["columns"][2]) == [2, 4]


def test_backslash_before_ordinary_byte_is_literal():
    # \0 inside a quoted field is two literal bytes, not an escape sequence.
    csv = b'a,b,c\n1,"back\\0slash",2\n'
    for threads in (False, True):
        r = read_csv(csv, use_threads=threads)
        assert r["num_rows"] == 1
        assert _to_list(r["columns"][1]) == ["back\\0slash"]
        assert _to_list(r["columns"][2]) == [2]


def test_backslash_is_literal_in_quoted_header():
    r = read_csv(b'"a\\","b"\n1,2\n')
    assert r["column_names"] == ["a\\", "b"]
    assert r["num_rows"] == 1


def test_backslash_is_literal_first_row_count_no_header():
    r = read_csv(b'"x\\",1\n"y",2\n', has_header=False)
    assert r["column_names"] == ["col_0", "col_1"]
    assert _to_list(r["columns"][0]) == ["x\\", "y"]


def test_backslash_before_doubled_quote():
    # \"" inside a quoted field is a literal backslash then an escaped quote.
    csv = b'id,val\n1,"a\\""b"\n'
    r = read_csv(csv)
    assert _to_list(r["columns"][1]) == ['a\\"b']


def test_backslash_quote_threaded_matches_python_csv():
    # Drives the parallel split FSM: quoted fields ending in a backslash, with
    # embedded newlines, across many chunks. Oracle is the stdlib csv module
    # (RFC 4180 defaults: doublequote=True, escapechar=None).
    import csv as _csv
    import io

    rows = [["id", "path", "n"]]
    for i in range(20000):
        rows.append([str(i), f"C:\\dir{i}\\" if i % 3 else f'x"\n\\{i}', str(i * 2)])
    buf = io.StringIO()
    _csv.writer(buf, lineterminator="\n").writerows(rows)
    data = buf.getvalue().encode()
    expected = list(_csv.reader(io.StringIO(buf.getvalue())))[1:]
    for threads in (False, True):
        r = read_csv(data, use_threads=threads)
        assert r["num_rows"] == len(expected)
        assert _to_list(r["columns"][1]) == [row[1] for row in expected]
        assert _to_list(r["columns"][2]) == [int(row[2]) for row in expected]


def test_mid_field_quote_is_literal():
    # A quote only opens a quoted field as the field's first byte.
    r = read_csv(b'a,b\nab"c,d\n1,2\n')
    assert r["num_rows"] == 2
    assert _to_list(r["columns"][0]) == ['ab"c', "1"]
    assert _to_list(r["columns"][1]) == ["d", "2"]


def test_mid_field_quote_no_header_column_count():
    r = read_csv(b'x"y,2,3\n', has_header=False)
    assert r["column_names"] == ["col_0", "col_1", "col_2"]
    assert _to_list(r["columns"][0]) == ['x"y']


def test_mid_field_quote_threaded_matches_python_csv():
    # A stray mid-field quote must not flip the split FSM into "quoted", or a
    # later real quoted field with an embedded newline gets split mid-field.
    import csv as _csv
    import io

    lines = ["id,a,b"]
    for i in range(20000):
        if i % 2:
            lines.append(f'{i},5" pipe,"multi\nline {i}"')
        else:
            lines.append(f'{i},plain,"x,{i}"')
    text = "\n".join(lines) + "\n"
    expected = list(_csv.reader(io.StringIO(text)))[1:]
    for threads in (False, True):
        r = read_csv(text.encode(), use_threads=threads)
        assert r["num_rows"] == len(expected)
        assert _to_list(r["columns"][1]) == [row[1] for row in expected]
        assert _to_list(r["columns"][2]) == [row[2] for row in expected]


def test_long_unquoted_field_not_truncated():
    v = "z" * 70000
    r = read_csv(f"a,b\n{v},1\n".encode())
    assert _to_list(r["columns"][0]) == [v]


def test_long_quoted_escaped_field_not_truncated():
    # Exercises the unescape path (sniff + build) past the old uint16 limit.
    v = '"' + "z" * 70000 + '"'
    raw = v.replace('"', '""')
    r = read_csv(f'a,b\n"{raw}",1\n'.encode())
    assert _to_list(r["columns"][0]) == [v]
    assert _to_list(r["columns"][1]) == [1]


def test_long_escaped_field_under_predicate():
    # Predicate-only column goes through the shared predicate scratch.
    v = '"' + "q" * 70000
    raw = v.replace('"', '""')
    csv = f'a,b\n"{raw}",1\n"x",2\n'.encode()
    r = read_csv(csv, columns=["b"], predicates=[("a", "==", v)])
    assert _to_list(r["columns"][0]) == [1]


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


# ---------------------------------------------------------------------------
# Type-mismatch past the sniff sample window (infer_sample_size / fail_on_error)
#
# sniff_csv_column_types only samples the first `infer_sample_size` non-null
# values per column; a later value that doesn't fit that sniffed type used to
# be silently coerced to 0 (int64/float64 columns) instead of erroring or
# nulling. Fixed to fail loud by default, or null the value under
# fail_on_error=False.
# ---------------------------------------------------------------------------


def _late_mismatch_csv(good_rows=500, bad_value="notanumber"):
    header = "id,val\n"
    body = "\n".join(f"{i},{i}" for i in range(good_rows))
    return (header + body + f"\n{good_rows},{bad_value}\n").encode()


def test_late_int_mismatch_raises_by_default():
    csv = _late_mismatch_csv()
    with pytest.raises(RuntimeError, match="val"):
        read_csv(csv, infer_sample_size=3)


def test_late_int_mismatch_names_column_and_value_in_error():
    csv = _late_mismatch_csv(bad_value="notanumber")
    with pytest.raises(RuntimeError, match="notanumber"):
        read_csv(csv, infer_sample_size=3)


def test_late_int_mismatch_nulled_when_fail_on_error_false():
    csv = _late_mismatch_csv(good_rows=10)
    r = read_csv(csv, infer_sample_size=3, fail_on_error=False)
    assert r["success"]
    val_col = _to_list(r["columns"][1])
    assert val_col[-1] is None
    assert val_col[:-1] == list(range(10))


def test_late_float_mismatch_raises_by_default():
    header = "id,val\n"
    body = "\n".join(f"{i},{i}.5" for i in range(20))
    csv = (header + body + "\n20,notafloat\n").encode()
    with pytest.raises(RuntimeError, match="val"):
        read_csv(csv, infer_sample_size=3)


def test_mismatch_within_sample_window_widens_to_varchar_not_error():
    # A mismatching value seen DURING sniffing (not past the window) just
    # widens the column to VARCHAR -- this is pre-existing, correct behavior,
    # not the bug this fix addresses.
    csv = b"id,val\n1,10\n2,notanumber\n3,30\n"
    r = read_csv(csv, infer_sample_size=128)
    assert r["success"]
    assert _to_list(r["columns"][1]) == ["10", "notanumber", "30"]


def test_infer_sample_size_is_respected():
    # With a large sample window, the same file that fails with a small window
    # succeeds because the offending value falls inside the sniff sample.
    csv = _late_mismatch_csv(good_rows=5, bad_value="6")
    r = read_csv(csv, infer_sample_size=128)
    assert r["success"]
    assert _to_list(r["columns"][1])[-1] == 6


def test_invalid_infer_sample_size_rejected():
    with pytest.raises(ValueError):
        read_csv(b"a\n1\n", infer_sample_size=0)
    with pytest.raises(ValueError):
        read_csv(b"a\n1\n", infer_sample_size=-1)


def test_late_mismatch_with_threading_still_raises():
    # Exercises the multi-threaded build path (default use_threads=True with
    # enough rows to split across threads) -- a mismatch discovered on a
    # worker thread must still propagate as a Python exception, and must not
    # crash/hang from other still-running pool threads referencing freed
    # stack state.
    csv = _late_mismatch_csv(good_rows=5000, bad_value="notanumber")
    with pytest.raises(RuntimeError, match="val"):
        read_csv(csv, infer_sample_size=3, use_threads=True)
