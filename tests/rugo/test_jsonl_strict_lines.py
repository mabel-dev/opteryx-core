"""A JSONL line is accepted only when its top level is well-formed JSON (RFC 8259).

The document map parses each record strictly at its top level: `{`, members `"key": value`
separated by `,`, `}` — whitespace between tokens and nothing else — and every scalar must
be one JSON token (`true`, `false`, `null`, or a JSON number; `NaN`/`Infinity`, which
Python's json.dumps writes by default, are not JSON). Container interiors and string
contents are not validated at this layer.

With a projection the reader stops parsing a record once every wanted column is resolved
(early exit, ruled 2026-10-05; rugo/src/jsonl/core/interpreter.hpp, build_columns). The rest
of the line is only checked for brackets and strings: it must close the record exactly
once, at the line's last non-whitespace byte, outside a string. So past the wanted columns
member grammar and scalar tokens are NOT validated, and a projected read can accept a line
the full read rejects (LONG_RELAXED). Everything else is still rejected whatever the
projection (LONG_STILL_REJECTED).

The INVALID / VALID lines below are shorter than the reader's first 64-byte index step, so
every one is indexed whole and judged by the full rules under every projection; the LONG_*
lines are long enough for the early exit to take the rest of the line.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

PROJECTIONS = [None, ["n"], ["obj->>'b'"], ["n", "obj"]]

GOOD_BEFORE = b'{"n":1,"obj":{"b":"x"}}\n'
GOOD_AFTER = b'{"n":2,"obj":{"b":"y"}}\n'

INVALID = [
    b'{"n":5,"s""x","obj":{"b":"z"}}',            # missing colon
    b'{"n":5,"s":"x" "t":"y","obj":{"b":"z"}}',  # missing comma between members
    b'{"n":5,"s":"x"zz,"obj":{"b":"z"}}',        # junk after a string value
    b'{"n":5,"obj":{"b":"z"}zz}',                # junk after a container value
    b'{"n":hello,"obj":{"b":"z"}}',               # bare word scalar
    b'{"n":NaN,"obj":{"b":"z"}}',                 # not JSON
    b'{"n":Infinity,"obj":{"b":"z"}}',            # not JSON
    b'{"n":-Infinity,"obj":{"b":"z"}}',           # not JSON
    b'{"n":,"obj":{"b":"z"}}',                    # empty value
    b'{"n":5,"obj":{"b":"z"},}',                  # trailing comma
    b'{"n":05,"obj":{"b":"z"}}',                  # leading zero
    b'{"n":5.,"obj":{"b":"z"}}',                  # fraction without digits
    b'{"n":.5,"obj":{"b":"z"}}',                  # no integer part
    b'{"n":+5,"obj":{"b":"z"}}',                  # leading plus
    b'{"n":5e,"obj":{"b":"z"}}',                  # exponent without digits
    b'{"n":5 6,"obj":{"b":"z"}}',                 # two tokens
    b'{"n":tru,"obj":{"b":"z"}}',                 # truncated literal
    b'{"n":nulll,"obj":{"b":"z"}}',               # overlong literal
    b'{n:5,"obj":{"b":"z"}}',                     # unquoted key
    b'{"n":5,,"obj":{"b":"z"}}',                  # double comma
    b'{"n"::5,"obj":{"b":"z"}}',                  # double colon
    b'{"n":5,"obj":{"b":"z"}',                    # unclosed record
    b'{,"n":5}',                                  # leading comma
]

VALID = [
    b'{}',
    b' { } ',
    b'{ "n" : 7 , "obj" : { "b" : "q" } }',
    b'{"n":-0,"obj":{"b":"q"}}',
    b'{"n":1e5,"obj":{"b":"q"}}',
    b'{"n":1E+5,"obj":{"b":"q"}}',
    b'{"n":-1.5e-3,"obj":{"b":"q"}}',
    b'{"n":true,"obj":{"b":"q"}}',
    b'{"n":null,"obj":{"b":"q"}}',
    b'{"n":0,"obj":{},"a":[],"s":""}',
    b'{"n":7,"s":"a,b:c{d}[e]\\"","obj":{"b":"q"}}',
]


@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
@pytest.mark.parametrize("line", INVALID, ids=lambda b: b.decode())
def test_invalid_top_level_is_one_malformed_line(line, columns):
    data = GOOD_BEFORE + line + b"\n" + GOOD_AFTER
    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == 2
    assert res["malformed_count"] == 1
    with pytest.raises(ValueError, match="Malformed JSONL at line 2"):
        read_jsonl(data, columns=columns, fail_on_error=True)


@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
@pytest.mark.parametrize("line", VALID, ids=lambda b: b.decode())
def test_valid_top_level_is_accepted(line, columns):
    data = GOOD_BEFORE + line + b"\n" + GOOD_AFTER
    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == 3
    assert res["malformed_count"] == 0


PAD = b'"pad":"' + b"x" * 200 + b'"'

# Malformed only AFTER the wanted column `n`: a projection on `n` stops before the fault.
LONG_RELAXED = [
    b'{"n":5,"s""x",' + PAD + b"}",             # missing colon
    b'{"n":5,"s":tru,' + PAD + b"}",            # truncated literal
    b'{"n":5,"s":"x" "t":"y",' + PAD + b"}",    # missing comma between members
    b'{"n":5,"s":"x"zz,' + PAD + b"}",          # junk after a string value
    b'{"n":5,' + PAD + b",}",                   # trailing comma
]

# Malformed after `n` too, but in what the tail check still sees.
LONG_STILL_REJECTED = [
    b'{"n":5,' + PAD,                           # record never closed
    b'{"n":5,"obj":{"b":"z",' + PAD + b"}",     # nested object never closed
    b'{"n":5,' + PAD + b',"s":"ab}',            # cut inside a string ending in '}'
    b'{"n":5,' + PAD + b'}{"n":6}',             # two objects on one line
    b'{"n":5,' + PAD + b"}}",                   # extra closing brace
    b'{"n":5,' + PAD + b"} zz",                 # junk after the record
    b'{"n":5,' + PAD + b',"a":[1,2}',           # mismatched bracket leaves the record open
]


@pytest.mark.parametrize("line", LONG_RELAXED, ids=lambda b: b[:24].decode())
def test_projection_past_wanted_columns_skips_grammar(line):
    data = GOOD_BEFORE + line + b"\n" + GOOD_AFTER
    full = read_jsonl(data, fail_on_error=False)
    assert full["num_rows"] == 2
    assert full["malformed_count"] == 1
    projected = read_jsonl(data, columns=["n"], fail_on_error=True)
    assert projected["columns"][0].to_pylist() == [1, 5, 2]


@pytest.mark.parametrize("columns", PROJECTIONS, ids=str)
@pytest.mark.parametrize("line", LONG_STILL_REJECTED, ids=lambda b: b[-12:].decode())
def test_projection_tail_check_still_rejects(line, columns):
    data = GOOD_BEFORE + line + b"\n" + GOOD_AFTER
    res = read_jsonl(data, columns=columns, fail_on_error=False)
    assert res["num_rows"] == 2
    assert res["malformed_count"] == 1
    with pytest.raises(ValueError, match="Malformed JSONL at line 2"):
        read_jsonl(data, columns=columns, fail_on_error=True)


# A nested projection stops INSIDE the object: the rest of the object and of the line is
# the tail check's.
def test_nested_projection_stops_inside_the_object():
    tail_bad = b'{"obj":{"b":"z","c":tru},' + PAD + b"}"    # bad scalar after the wanted sub-key
    unclosed = b'{"obj":{"b":"z",' + PAD + b"}"             # the object closes, the record does not
    data = GOOD_BEFORE + tail_bad + b"\n" + unclosed + b"\n" + GOOD_AFTER
    res = read_jsonl(data, columns=["obj->>'b'"], fail_on_error=False)
    assert res["columns"][0].to_pylist() == ["x", "z", "y"]
    assert res["malformed_count"] == 1


# A key repeated in one record: its FIRST occurrence is the value — as yyjson's object
# lookup (draken's `->` / `->>`) reads it — whatever the projection, and a repeat never
# hides a later column. For a nested column, the first container with that key decides.
@pytest.mark.parametrize("columns", [None, ["a", "b"], ["b", "a"], ["a"], ["b"]], ids=str)
def test_repeated_key_first_occurrence_wins(columns):
    res = read_jsonl(b'{"a":1,"a":2,"b":3}\n{"a":4,"b":5,"b":6}\n', columns=columns,
                     fail_on_error=True)
    names = res["column_names"]
    values = {n: res["columns"][i].to_pylist() for i, n in enumerate(names)}
    if "a" in names:
        assert values["a"] == [1, 4]
    if "b" in names:
        assert values["b"] == [3, 5]


def test_repeated_container_key_first_decides_nested():
    data = b'{"o":{"b":"x"},"o":{"b":"y"}}\n{"o":{"z":1},"o":{"b":"w"}}\n'
    res = read_jsonl(data, columns=["o->>'b'"], fail_on_error=True)
    assert res["columns"][0].to_pylist() == ["x", None]
