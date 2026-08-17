"""JSONL array-column materialization (column_builder.cpp's parse_array_column).

Covers the walker that decodes array elements during ingest: element typing and the
widening ladder, null handling at both row and element level, string escape decoding
and the inline/arena boundary, and the two ways a column leaves scope — elements that
are nested or of mixed kinds, and array text that is not valid JSON.

The strict-rejection cases matter more than they look: array text reaching the builder
has only been bracket-balanced by the structural scanner, never validated. Whether the
walker accepts a given byte sequence decides the column's TYPE (ARRAY vs raw VARCHAR
text), so each accept/reject below is a type-stability test, not a parser nicety.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.draken_native import DrakenType
from rugo.rugo_native import read_jsonl


BS = chr(92)  # a single backslash: JSON escapes below are DATA, not Python escapes


def read_col(*rows, column="a", parse_arrays=True):
    data = ("\n".join(rows) + "\n").encode()
    res = read_jsonl(data, parse_arrays=parse_arrays)
    return res["columns"][res["column_names"].index(column)]


def read_col_bytes(payload, column="a"):
    res = read_jsonl(payload, parse_arrays=True)
    return res["columns"][res["column_names"].index(column)]


def assert_array(vec, child_type, values):
    assert vec.type == DrakenType.ARRAY
    assert vec.array_child_type == child_type
    assert vec.to_pylist() == values


def assert_fell_back(vec):
    """The column left array scope and is raw JSON text."""
    assert vec.type == DrakenType.VARCHAR


# --------------------------------------------------------------------------
# element types and the widening ladder
# --------------------------------------------------------------------------

def test_int_elements():
    assert_array(read_col('{"a":[1,2,3]}', '{"a":[4]}'), DrakenType.INT64, [[1, 2, 3], [4]])


def test_bool_elements():
    assert_array(read_col('{"a":[true,false]}'), DrakenType.BOOL, [[True, False]])


def test_string_elements():
    assert_array(read_col('{"a":["a","bb"]}'), DrakenType.VARCHAR, [["a", "bb"]])


def test_float_elements():
    assert_array(read_col('{"a":[1.5,2.25]}'), DrakenType.FLOAT64, [[1.5, 2.25]])


def test_exponent_without_decimal_point_is_float():
    assert_array(read_col('{"a":[1e2]}'), DrakenType.FLOAT64, [[100.0]])


def test_int_widens_to_float_within_a_row():
    assert_array(read_col('{"a":[1,2.5]}'), DrakenType.FLOAT64, [[1.0, 2.5]])


def test_int_widens_to_float_across_rows():
    # The widening decision is column-scoped: a late float retypes earlier int rows.
    assert_array(
        read_col('{"a":[1]}', '{"a":[2]}', '{"a":[3.5]}'),
        DrakenType.FLOAT64,
        [[1.0], [2.0], [3.5]],
    )


def test_empty_arrays_are_not_null():
    vec = read_col('{"a":[]}', '{"a":[1]}', '{"a":[]}')
    assert_array(vec, DrakenType.INT64, [[], [1], []])
    assert vec.null_count() == 0


# --------------------------------------------------------------------------
# integer width: values past int64 must not wrap to a negative
# --------------------------------------------------------------------------

def test_value_past_int64_becomes_uint64():
    assert_array(
        read_col('{"a":[18446744073709551615]}'),
        DrakenType.UINT64,
        [[18446744073709551615]],
    )


def test_int64_max_stays_int64():
    assert_array(
        read_col('{"a":[9223372036854775807]}'),
        DrakenType.INT64,
        [[9223372036854775807]],
    )


def test_int64_min_stays_int64():
    assert_array(
        read_col('{"a":[-9223372036854775808]}'),
        DrakenType.INT64,
        [[-9223372036854775808]],
    )


def test_uint64_promotes_the_whole_column():
    assert_array(
        read_col('{"a":[1]}', '{"a":[18446744073709551615]}'),
        DrakenType.UINT64,
        [[1], [18446744073709551615]],
    )


def test_uint64_mixed_with_negative_widens_to_float():
    # No single integer width holds both, so the ladder's next rung is FLOAT64.
    assert_array(
        read_col('{"a":[-1,18446744073709551615]}'),
        DrakenType.FLOAT64,
        [[-1.0, 1.8446744073709552e19]],
    )


def test_magnitude_past_uint64_becomes_float():
    assert_array(read_col('{"a":[99999999999999999999]}'), DrakenType.FLOAT64, [[1e20]])


def test_negative_magnitude_past_int64_becomes_float():
    vec = read_col('{"a":[-9223372036854775809]}')
    assert vec.array_child_type == DrakenType.FLOAT64


# --------------------------------------------------------------------------
# floats: ingestion canonicalisation
# --------------------------------------------------------------------------

def test_negative_zero_is_canonicalised():
    # fp_canon applies at every ingestion point, array elements included, so the same
    # literal cannot mean different things inside and outside an array.
    (value,) = read_col('{"a":[-0.0]}').to_pylist()[0]
    assert value == 0.0
    assert str(value) == "0.0"


def test_exponent_underflow_is_zero():
    assert_array(read_col('{"a":[1e-400]}'), DrakenType.FLOAT64, [[0.0]])


def test_exponent_overflow_leaves_array_scope():
    # A magnitude past DBL_MAX is a parse failure, not an infinity.
    with pytest.warns(RuntimeWarning):
        vec = read_col('{"a":[1e400]}')
    assert_fell_back(vec)


# --------------------------------------------------------------------------
# nulls
# --------------------------------------------------------------------------

def test_row_level_null():
    vec = read_col('{"a":[1]}', '{"a":null}', '{"a":[2]}')
    assert vec.to_pylist() == [[1], None, [2]]
    assert vec.null_count() == 1


def test_element_level_nulls_are_not_row_nulls():
    vec = read_col('{"a":[1,null,3]}', '{"a":[null]}')
    assert vec.to_pylist() == [[1, None, 3], [None]]
    assert vec.null_count() == 0


def test_absent_column_is_a_null_row():
    vec = read_col('{"a":[1]}', '{"b":2}', '{"a":[3]}')
    assert vec.to_pylist() == [[1], None, [3]]


def test_all_null_elements_with_no_scalar_kind():
    vec = read_col('{"a":[null,null]}')
    assert vec.type == DrakenType.ARRAY
    assert vec.to_pylist() == [[None, None]]


def test_null_elements_then_typed_row():
    assert_array(read_col('{"a":[null]}', '{"a":[1]}'), DrakenType.INT64, [[None], [1]])


# --------------------------------------------------------------------------
# strings: escape decoding and the inline/arena boundary
# --------------------------------------------------------------------------

def test_simple_escapes_are_decoded():
    assert_array(
        read_col(r'{"a":["a\"b","c\\d","e\nf","g\th"]}'),
        DrakenType.VARCHAR,
        [['a"b', "c\\d", "e\nf", "g\th"]],
    )


def test_solidus_and_control_escapes():
    assert_array(
        read_col(r'{"a":["\/","\b","\f","\r"]}'),
        DrakenType.VARCHAR,
        [["/", "\b", "\f", "\r"]],
    )


def test_unicode_escape_is_decoded():
    assert_array(
        read_col('{"a":["' + BS + 'u0041' + BS + 'u00e9' + BS + 'u4e2d"]}'),
        DrakenType.VARCHAR,
        [["A\u00e9\u4e2d"]],
    )


def test_surrogate_pair_is_decoded():
    assert_array(
        read_col('{"a":["' + BS + 'ud83d' + BS + 'ude00"]}'),
        DrakenType.VARCHAR,
        [["\U0001f600"]],
    )


def test_escaped_nul_is_string_content():
    assert_array(
        read_col('{"a":["a' + BS + 'u0000b"]}'),
        DrakenType.VARCHAR,
        [["a\x00b"]],
    )


@pytest.mark.parametrize("text", ["123456789012", "1234567890123", "", "x"])
def test_inline_and_extern_slot_boundary(text):
    # STR_INLINE_MAX is 12: 12 bytes is an inline slot, 13 goes to the arena.
    assert_array(read_col('{"a":["%s"]}' % text), DrakenType.VARCHAR, [[text]])


def test_escape_shrinks_a_long_span_below_the_inline_boundary():
    # 12 source bytes, 2 decoded: sizing the arena on the RAW span would both
    # over-allocate and put this element on the wrong side of the boundary.
    assert_array(
        read_col('{"a":["' + BS + 'u0041' + BS + 'u0041"]}'),
        DrakenType.VARCHAR,
        [["AA"]],
    )


def test_escape_grows_past_the_inline_boundary():
    assert_array(
        read_col('{"a":["' + (BS + 'u4e2d') * 5 + '"]}'),
        DrakenType.VARCHAR,
        [["\u4e2d" * 5]],
    )


def test_mixed_inline_and_arena_elements():
    assert_array(
        read_col('{"a":["a","%s","bb"]}' % ("z" * 40)),
        DrakenType.VARCHAR,
        [["a", "z" * 40, "bb"]],
    )


def test_structural_characters_inside_strings():
    assert_array(
        read_col(r'{"a":["a]b","c[d","e,f","{\"k\":1}"]}'),
        DrakenType.VARCHAR,
        [["a]b", "c[d", "e,f", '{"k":1}']],
    )


def test_string_elements_with_nulls_interleaved():
    assert_array(
        read_col('{"a":["a",null,"%s"]}' % ("q" * 30)),
        DrakenType.VARCHAR,
        [["a", None, "q" * 30]],
    )


# --------------------------------------------------------------------------
# out of scope: nested elements and mixed kinds fall back to raw JSON text
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "row",
    [
        '{"a":[[1,2]]}',
        '{"a":[{"k":1}]}',
        '{"a":[[]]}',
        '{"a":[1,"b"]}',
        '{"a":[true,1]}',
        '{"a":[true,"b"]}',
    ],
)
def test_out_of_scope_falls_back_to_text(row):
    with pytest.warns(RuntimeWarning, match="nested or of mixed scalar"):
        vec = read_col(row)
    assert_fell_back(vec)


def test_mixed_kinds_across_rows_falls_back():
    with pytest.warns(RuntimeWarning, match="nested or of mixed scalar"):
        vec = read_col('{"a":[1]}', '{"a":["b"]}')
    assert_fell_back(vec)


def test_parse_arrays_disabled_returns_text_without_warning():
    vec = read_col('{"a":[1,2]}', parse_arrays=False)
    assert_fell_back(vec)
    assert vec.to_pylist() == ["[1,2]"]


def test_non_array_value_in_an_array_column_falls_back():
    with pytest.warns(RuntimeWarning):
        vec = read_col('{"a":[1]}', '{"a":"text"}')
    assert_fell_back(vec)


# --------------------------------------------------------------------------
# malformed array text: bracket-balanced but not valid JSON
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "array_text",
    [
        "[1,,2]",       # empty slot
        "[1,2,]",       # trailing comma
        "[,1]",         # leading comma
        "[abc]",        # bare word
        "[01]",         # leading zero
        "[+1]",         # leading plus
        "[.5]",         # no integer part
        "[1.]",         # no fraction digits
        "[1e]",         # no exponent digits
        "[1e+]",        # no exponent digits after sign
        "[NaN]",        # not JSON
        "[Infinity]",   # not JSON
        "[True]",       # wrong case
        "['a']",        # single quotes
        "[1 2]",        # missing comma
        "[0x10]",       # hex
        r'["a\qb"]',    # unknown escape
        r'["a\u00zz"]', # bad hex in \u
        r'["\u12"]',    # truncated \u
        r'["\ud83d"]',  # lone high surrogate
        r'["\udc00"]',  # lone low surrogate
        '["a\tb"]',     # unescaped control character
    ],
)
def test_malformed_array_text_falls_back(array_text):
    with pytest.warns(RuntimeWarning):
        vec = read_col('{"a":%s}' % array_text)
    assert_fell_back(vec)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"a":["\xc3"]}\n',              # truncated 2-byte sequence
        b'{"a":["\xe4\xb8"]}\n',          # truncated 3-byte sequence
        b'{"a":["\x80"]}\n',              # lone continuation byte
        b'{"a":["\xc0\x80"]}\n',          # overlong
        b'{"a":["\xed\xa0\x80"]}\n',      # UTF-8-encoded surrogate half
        b'{"a":["\xf4\x90\x80\x80"]}\n',  # past U+10FFFF
        b'{"a":["a\x00b"]}\n',            # raw NUL is a control character
    ],
)
def test_invalid_utf8_in_a_string_element_falls_back(payload):
    with pytest.warns(RuntimeWarning):
        vec = read_col_bytes(payload)
    assert_fell_back(vec)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"a":["\xc3\xa9"]}\n',
        b'{"a":["\xe4\xb8\xad"]}\n',
        b'{"a":["\xf0\x9f\x98\x80"]}\n',
        b'{"a":["\xf4\x8f\xbf\xbf"]}\n',  # U+10FFFF, the last legal codepoint
    ],
)
def test_valid_utf8_in_a_string_element_materializes(payload):
    vec = read_col_bytes(payload)
    assert vec.type == DrakenType.ARRAY
    assert vec.array_child_type == DrakenType.VARCHAR


def test_a_later_malformed_row_takes_the_whole_column_out_of_scope():
    with pytest.warns(RuntimeWarning):
        vec = read_col('{"a":[1]}', '{"a":[1,,2]}')
    assert_fell_back(vec)


def test_whitespace_inside_arrays_is_accepted():
    assert_array(read_col('{"a":[ 1 , 2 ,  3 ]}'), DrakenType.INT64, [[1, 2, 3]])
    assert_array(read_col('{"a":[   ]}', '{"a":[1]}'), DrakenType.INT64, [[], [1]])


# --------------------------------------------------------------------------
# scale — crosses the row-parallel threshold in parse_all_columns
# --------------------------------------------------------------------------

def test_many_rows_int_arrays():
    rows = ['{"a":[%d,%d]}' % (i, -i) for i in range(20_000)]
    vec = read_col(*rows)
    assert vec.array_child_type == DrakenType.INT64
    assert vec.length == 20_000
    assert vec.to_pylist()[-1] == [19_999, -19_999]


def test_many_rows_string_arrays_with_escapes():
    rows = [r'{"a":["v%d","w\"%d"]}' % (i, i) for i in range(20_000)]
    vec = read_col(*rows)
    assert vec.array_child_type == DrakenType.VARCHAR
    assert vec.to_pylist()[-1] == ["v19999", 'w"19999']


def test_many_rows_widening_on_the_last_row():
    rows = ['{"a":[1]}'] * 19_999 + ['{"a":[2.5]}']
    vec = read_col(*rows)
    assert vec.array_child_type == DrakenType.FLOAT64
    assert vec.to_pylist()[0] == [1.0]
    assert vec.to_pylist()[-1] == [2.5]


def test_many_rows_with_interleaved_row_nulls():
    rows = ['{"a":[%d]}' % i if i % 3 else '{"a":null}' for i in range(20_000)]
    vec = read_col(*rows)
    assert vec.null_count() == len(range(0, 20_000, 3))
    assert vec.to_pylist()[0] is None
    assert vec.to_pylist()[1] == [1]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
