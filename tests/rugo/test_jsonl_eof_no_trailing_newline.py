"""The last JSONL record must survive a missing trailing newline.

A bare scalar (true/false/null/number) emits no structural marker of its own, so the
document-map FSA is still in EXPECT_VALUE when the record's '}' arrives and used that
'}' purely as the scalar's terminator -- leaving the record open, waiting on a LATER
delimiter to bank it. With a trailing newline that delimiter existed; at end-of-buffer
it did not, so the final record reached MapBuilder::finish() with committed spans and
was reported as malformed ("Malformed JSONL at line N").

A string/object/array value never hit this (its own closing quote/bracket terminates
it, leaving the '}' free to close the record), which is why the defect only showed on
records whose LAST value was unquoted. Every case below is asserted both with and
without the trailing newline -- the two must agree.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl


LAST_VALUE_SHAPES = [
    (b'{"ok":true}', "true"),
    (b'{"ok":false}', "false"),
    (b'{"s":null}', "null"),
    (b'{"n":1}', "integer"),
    (b'{"f":1.5}', "float"),
    (b'{"n":-1e3}', "exponent"),
    (b'{"t":"a"}', "string"),
    (b'{"o":{"x":1}}', "object"),
    (b'{"a":[1,2]}', "array"),
    (b'{"a":1,"b":true}', "scalar after scalar"),
    (b'{"a":true,"b":"x"}', "string after scalar"),
    (b'{"a": 1 }', "whitespace padded"),
]


@pytest.mark.parametrize("line,shape", LAST_VALUE_SHAPES, ids=[s for _, s in LAST_VALUE_SHAPES])
def test_final_record_survives_missing_newline(line, shape):
    bare = read_jsonl(line)
    terminated = read_jsonl(line + b"\n")

    assert bare["num_rows"] == 1, shape
    assert terminated["num_rows"] == 1, shape
    assert bare["malformed_count"] == 0, shape
    assert bare["column_names"] == terminated["column_names"], shape
    for name in bare["column_names"]:
        i = bare["column_names"].index(name)
        assert bare["columns"][i].to_pylist() == terminated["columns"][i].to_pylist(), shape


def test_multi_record_last_line_unterminated():
    data = b'{"a":1}\n{"a":2}\n{"a":3}'
    res = read_jsonl(data)
    assert res["num_rows"] == 3
    assert res["columns"][0].to_pylist() == [1, 2, 3]
    assert read_jsonl(data + b"\n")["columns"][0].to_pylist() == [1, 2, 3]


def test_truncated_record_is_still_rejected():
    """The fix must not turn a genuinely unterminated record into a row."""
    for truncated in (b'{"a":1}\n{"a":2', b'{"a":1}\n{"a":"x', b'{"a":1}\n{"a"'):
        assert read_jsonl(truncated)["num_rows"] == 1, truncated


# ---------------------------------------------------------------------------
# A record left open at a newline is truncated, never a row.
#
# Both halves of this were load-bearing for the EOF defect above and only became
# reachable-on-malformed-input once it was fixed:
#   * EXPECT_SEPARATOR + newline used to PUSH_RECORD, so a brace-less fragment
#     banked as a complete row with malformed_count 0;
#   * ABANDON_RECORD flagged the line but left its field-spans in the arena, so
#     they were swept into the NEXT record when that one banked -- the following
#     row then reported the DISCARDED line's value. A silent wrong answer.
# ---------------------------------------------------------------------------

TRUNCATED = [
    (b'{"a":1,\n{"a":2}\n', [2], "open after comma"),
    (b'{"a":"x",\n{"a":"y"}\n', ["y"], "open after comma, string"),
    (b'{"a":"x"\n{"a":"y"}\n', ["y"], "no closing brace, string value"),
    (b'{"a":{"b":1}\n{"a":{"b":2}}\n', ['{"b":2}'], "no closing brace, container value"),
    (b'{"a"\n{"a":2}\n', [2], "key with no colon"),
]


@pytest.mark.parametrize("data,expected,shape", TRUNCATED, ids=[s for _, _, s in TRUNCATED])
def test_truncated_line_is_dropped_not_merged(data, expected, shape):
    res = read_jsonl(data, fail_on_error=False)
    assert res["malformed_count"] == 1, shape
    assert res["num_rows"] == len(expected), shape
    assert res["columns"][0].to_pylist() == expected, shape


def test_truncated_line_alone_yields_no_rows():
    for data in (b'{"a":"x"\n', b'{"a":{"b":1}\n', b'{"a":1,\n'):
        res = read_jsonl(data, fail_on_error=False)
        assert res["num_rows"] == 0, data
        assert res["malformed_count"] == 1, data


def test_truncated_line_raises_under_fail_on_error():
    with pytest.raises(ValueError, match="Malformed JSONL"):
        read_jsonl(b'{"a":"x"\n{"a":"y"}\n')


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
