"""Nested JSONL columns: `key->>'sub'` (text) and `key->'sub'` (JSON), one level deep.

The record walk reads the sub-value straight out of the container, so the container is
never materialised. The rendering must be byte-identical to draken's `->` / `->>` over
the same path (draken/ops/json_extract.h: yyjson read NUMBER_AS_RAW, write flags 0) —
every expectation below was checked against Opteryx's `->` / `->>` on the same rows when
this was built (2026-10-01):

  string   `->>`: decoded UTF-8        `->`: re-escaped JSON string
  object / array: canonical minified JSON (both operators)
  number: the source token             true / false: verbatim
  absent / JSON null / not an object: NULL

Only the VALUE read is validated (architect ruling 2026-10-01); malformed content
elsewhere in the container is not looked at.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

ROWS = [
    r'{"id":1,"commit":{"x":"plain","y":1}}',
    r'{"id":2,"commit":{"x":"esc \"q\" \\ \/ \n\t","y":1.10}}',
    r'{"id":3,"commit":{"x":"é中😀","y":1e3}}',
    r'{"id":4,"commit":{"x":"null","y":-0}}',
    r'{"id":5,"commit":{"x":null,"y":true}}',
    r'{"id":6,"commit":{"y":false}}',
    r'{"id":7,"commit":{"x": { "a" : 1 , "b":[ 1 , "s\/" , {"c":null} ] },"y":0}}',
    r'{"id":8,"commit":{"x":[ ],"y":{ }}}',
    r'{"id":9,"commit":{"record":{"x":"deep"},"y":12345678901234567890}}',
    r'{"id":10,"commit":{"x":"ctl\u0001\u001fend","y":"5"}}',
    r'{"id":11,"commit":{"y":2,"x":"last"}}',
    r'{"id":13,"commit":{"x":{"k":"é \u0008"},"y":-1.5E-7}}',
    r'{"id":14,"commit":{}}',
]
DATA = ("\n".join(ROWS) + "\n").encode()

TEXT_X = "commit->>'x'"
JSON_X = "commit->'x'"
TEXT_Y = "commit->>'y'"
JSON_Y = "commit->'y'"


def _read(data, **kwargs):
    result = read_jsonl(data, fail_on_error=True, **kwargs)
    return {n: v.to_pylist() for n, v in zip(result["column_names"], result["columns"])}, result


def _by_id(columns, name):
    return dict(zip(columns["id"], columns[name]))


def test_text_and_json_rendering_matches_draken():
    cols, _ = _read(DATA, columns=["id", TEXT_X, JSON_X, TEXT_Y, JSON_Y])
    tx, jx, ty, jy = (_by_id(cols, n) for n in (TEXT_X, JSON_X, TEXT_Y, JSON_Y))

    assert tx[1] == "plain" and jx[1] == '"plain"'
    assert tx[2] == 'esc "q" \\ / \n\t'
    assert jx[2] == '"esc \\"q\\" \\\\ / \\n\\t"'          # `/` is not re-escaped
    assert tx[3] == "é中😀" and jx[3] == '"é中😀"'          # UTF-8 written verbatim
    assert tx[4] == "null" and jx[4] == '"null"'            # a STRING "null" is not NULL
    assert tx[5] is None and jx[5] is None                  # JSON null
    assert tx[6] is None                                    # absent
    assert tx[7] == jx[7] == '{"a":1,"b":[1,"s/",{"c":null}]}'  # minified, \/ decoded
    assert tx[8] == "[]" and ty[8] == "{}"
    assert tx[9] is None                                    # depth 2 never matches
    assert jx[10] == '"ctl\\u0001\\u001Fend"'               # uppercase \u00XX
    assert tx[11] == "last"                                 # last member, '}' terminated
    assert tx[13] == '{"k":"é \\b"}'
    assert tx[14] is None

    # Numbers keep their source token; literals are verbatim; `->` of a string re-quotes.
    assert [ty[i] for i in (1, 2, 3, 4, 5, 6, 9, 13)] == [
        "1", "1.10", "1e3", "-0", "true", "false", "12345678901234567890", "-1.5E-7"
    ]
    assert ty[10] == "5" and jy[10] == '"5"'


def test_nested_and_top_level_columns_on_one_key():
    data = b'{"c":{"x":"a","y":2},"d":1}\n{"c":{"y":3},"d":2}\n'
    cols, _ = _read(data, columns=["c", "c->>'x'", "c->>'y'", "d"])
    assert cols == {
        "c": ['{"x":"a","y":2}', '{"y":3}'],
        "c->>'x'": ["a", None],
        "c->>'y'": ["2", "3"],
        "d": [1, 2],
    }


def test_key_whose_value_is_not_an_object_is_null():
    cols, _ = _read(b'{"c":[1,2]}\n{"c":"str"}\n{"c":5}\n', columns=["c->>'x'"])
    assert cols == {"c->>'x'": [None, None, None]}


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ((TEXT_X, "==", "null"), [4]),                     # string "null", not JSON null
        ((TEXT_Y, "==", "5"), [10]),
        ((TEXT_Y, "==", "1.10"), [2]),                     # numbers compare as their text
        ((TEXT_Y, "!=", "0"), [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 13]),
        ((TEXT_X, "<", "m"), [2, 8, 10, 11]),
        ((TEXT_X, "in", ["plain", "last", "null"]), [1, 4, 11]),
        ((TEXT_X, "not in", ["plain"]), [2, 3, 4, 7, 8, 10, 11, 13]),
        ((TEXT_X, "is null", None), [5, 6, 9, 14]),
        ((TEXT_X, "is not null", None), [1, 2, 3, 4, 7, 8, 10, 11, 13]),
        ((TEXT_X, "==", '{"a":1,"b":[1,"s/",{"c":null}]}'), [7]),
    ],
)
def test_text_predicates_compare_the_rendered_text(predicate, expected):
    cols, result = _read(DATA, columns=["id"], predicates=[predicate])
    assert (sorted(cols["id"]) if result["success"] else []) == expected


@pytest.mark.parametrize(
    "line, shown",
    [
        (b'{"c":{"x":12x3}}', "12x3"),
        (b'{"c":{"x":nope}}', "nope"),
        (b'{"c":{"x":{"a":01}}}', '{"a":01}'),
        (b'{"c":{"x":"bad \\q"}}', "bad \\q"),
    ],
)
def test_an_invalid_value_fails_loud(line, shown):
    with pytest.raises(RuntimeError, match="is not valid JSON") as err:
        read_jsonl(line + b"\n", columns=["c->>'x'"], fail_on_error=True)
    assert shown in str(err.value)


def test_malformed_content_off_the_path_is_not_validated():
    cols, _ = _read(b'{"c":{"x":"ok","z":[1,,2]}}\n', columns=["c->>'x'"])
    assert cols == {"c->>'x'": ["ok"]}


def test_first_bad_row_is_reported_under_parallel_build():
    rows = [b'{"c":{"x":%d}}' % i for i in range(300_000)]
    rows[200_000] = b'{"c":{"x":2x}}'
    rows[70_001] = b'{"c":{"x":1x}}'
    with pytest.raises(RuntimeError, match="row 70001"):
        read_jsonl(b"\n".join(rows) + b"\n", columns=["c->>'x'"], fail_on_error=True)


def test_large_input_every_row_through_the_threaded_walk():
    n = 400_000
    lines = [
        ('{"id":%d,"c":{"k":"v%d","n":%d,"o":{"z":[%d]}}}' % (i, i % 7, i, i)).encode()
        if i % 5 else ('{"id":%d,"c":{"n":%d}}' % (i, i)).encode()
        for i in range(n)
    ]
    cols, _ = _read(b"\n".join(lines) + b"\n", columns=["id", "c->>'k'", "c->>'n'", "c->'o'"])
    assert len(cols["id"]) == n
    for row, i in enumerate(cols["id"]):
        k, num, obj = cols["c->>'k'"][row], cols["c->>'n'"][row], cols["c->'o'"][row]
        assert num == str(i)
        if i % 5:
            assert (k, obj) == ("v%d" % (i % 7), '{"z":[%d]}' % i)
        else:
            assert (k, obj) == (None, None)


def test_declared_types_are_fixed_by_the_operator():
    cols, _ = _read(
        b'{"c":{"x":"a","y":{"k":1}}}\n',
        columns=["c->>'x'", "c->'y'"],
        explicit_schema={"c->>'x'": "NVARCHAR", "c->'y'": "VARIANT"},
    )
    assert cols == {"c->>'x'": ["a"], "c->'y'": ['{"k":1}']}
    with pytest.raises(ValueError, match="nested `->>` column is NVARCHAR"):
        read_jsonl(b'{"c":{"x":"a"}}\n', columns=["c->>'x'"], explicit_schema={"c->>'x'": "VARCHAR"})


def test_sub_key_absent_from_the_whole_chunk_is_reported_absent():
    cols, result = _read(
        b'{"c":{"y":1}}\n', columns=["c->>'x'"], explicit_schema={"c->>'x'": "NVARCHAR"}
    )
    assert cols == {"c->>'x'": [None]}
    assert result["absent_columns"] == ["c->>'x'"]


def test_refusals():
    with pytest.raises(ValueError, match="predicate on `->` column"):
        read_jsonl(b'{"c":{"x":"a"}}\n', columns=["c->'x'"], predicates=[("c->'x'", "==", "a")])
    with pytest.raises(ValueError, match="non-empty key and sub-key"):
        read_jsonl(b'{"c":{}}\n', columns=["c->>''"])


def test_sub_keys_compare_decoded_like_yyjson():
    # yyjson (draken's `->>`) looks keys up DECODED, so an escaped spelling is the same key.
    data = (
        b'{"c":{"col\\u006cection":"v1"}}\n'
        b'{"c":{"x\\"y":"q"}}\n'
        b'{"c":{"a\\/b":"slash"}}\n'
    )
    cols, _ = _read(data, columns=["c->>'collection'", "c->>'x\"y'", "c->>'a/b'"])
    assert cols == {
        "c->>'collection'": ["v1", None, None],
        "c->>'x\"y'": [None, "q", None],
        "c->>'a/b'": [None, None, "slash"],
    }


# --- intern_nested_text (EXPERIMENT 2026-10-02): a repetitive `->>` column is built
# Dict-shaped in the scan. Values and NULLs must be identical to the dense build.


def _low_card_lines(n):
    # Every rendering kind, repeated: plain, escaped (and an escape-alias of a plain value,
    # which must intern to the SAME entry), non-ASCII, the string "null", JSON null, absent,
    # numbers, literals, containers.
    shapes = [
        r'"app.bsky.feed.like"', r'"app.bsky.feed.post"', r'"app.bsky.feed.post"',
        r'"esc \"q\" \n"', r'"é中😀"', r'"null"', "null", None, "12", "1.10", "true",
        r'{"a":[1, "s\/"]}', "[ ]", r'"a-much-longer-than-twelve-bytes-value"', r'""',
    ]
    out = []
    for i in range(n):
        s = shapes[(i * 7) % len(shapes)]
        out.append(('{"id":%d,"c":{%s"y":1}}' % (i, "" if s is None else '"x":%s,' % s)).encode())
    return b"\n".join(out) + b"\n"


@pytest.mark.parametrize("n", [300, 3_000, 300_000])
def test_interned_text_matches_dense(n):
    data = _low_card_lines(n)
    dense = read_jsonl(data, columns=["c->>'x'"], fail_on_error=True)["columns"][0]
    interned = read_jsonl(data, columns=["c->>'x'"], fail_on_error=True, intern_nested_text=True)["columns"][0]
    assert interned.to_pylist() == dense.to_pylist()
    assert dense.is_dense
    # 12 distinct rendered values (15 shapes, less JSON null and absent, less the escape
    # alias, which shares "app.bsky.feed.post"'s entry).
    assert interned.data_length == 12 and interned.is_dict


def test_interned_text_high_cardinality_falls_back_dense():
    data = b"".join(b'{"c":{"x":"v%d"}}\n' % i for i in range(50_000))
    col = read_jsonl(data, columns=["c->>'x'"], intern_nested_text=True)["columns"][0]
    assert col.is_dense
    assert col.to_pylist() == ["v%d" % i for i in range(50_000)]


def test_interned_text_reports_the_first_bad_row():
    rows = [b'{"c":{"x":"v%d"}}' % (i % 5) for i in range(300_000)]
    rows[200_000] = b'{"c":{"x":2x}}'
    rows[70_001] = b'{"c":{"x":1x}}'
    with pytest.raises(RuntimeError, match="row 70001"):
        read_jsonl(b"\n".join(rows) + b"\n", columns=["c->>'x'"], fail_on_error=True,
                   intern_nested_text=True)
