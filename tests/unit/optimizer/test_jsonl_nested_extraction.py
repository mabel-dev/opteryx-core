"""JsonlNestedExtractionStrategy: `col ->> 'k'` / `col -> 'k'` over a READ_JSONL object
column is read by the scan (rugo's nested walk) instead of materialising the object and
re-parsing it per row.

The strategy may only ever change HOW a value is read, never WHAT it is: every query
here runs with the strategy on and off and must return the same rows. A spy on the rugo
call asserts what was pushed — and, for the shapes that must not be, that it wasn't.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
import rugo.rugo_native as rugo_native
from opteryx import config

ROWS = [
    r'{"id":1,"c":{"col":"post","op":"create","x":"plain","n":1},"s":"{\"k\":1}"}',
    r'{"id":2,"c":{"col":"like","op":"create","x":"esc \"q\" \/ \n","n":1.10},"s":"{\"k\":2}"}',
    r'{"id":3,"c":{"col":"post","op":"delete","x":"é","n":1e3},"s":"{}"}',
    r'{"id":4,"c":{"col":"post","op":"create","x":"null","n":-0},"s":"{}"}',
    r'{"id":5,"c":{"col":"repost","op":"update","x":null,"n":true},"s":"{}"}',
    r'{"id":6,"c":{"op":"create","n":false},"s":"{}"}',
    r'{"id":7,"c":{"col":"like","x":{ "a" : 1 , "b":[1,"s"] },"n":0,"o":{"z":"deep"}},"s":"{}"}',
    r'{"id":8,"c":["post","create"],"s":"{}"}',
    r'{"id":9,"c":{"col":"post","op":"create","a.b":"dotted","0":"zero"},"s":"{}"}',
    r'{"id":10,"c":{"col":"post","op":"create","x":"5","n":"5"},"s":"{}"}',
]


@pytest.fixture
def data(tmp_path):
    path = tmp_path / "nested.jsonl"
    path.write_text("\n".join(ROWS) + "\n")
    return f"READ_JSONL('{path}')"


def _run(sql, pushed):
    """Rows of `sql` (sorted by repr) with the strategy on/off, plus what rugo was asked."""
    original_flag = config.features.disable_jsonl_nested_extraction
    # The native JSONL scan's plan-time ParseContext is built by this one call
    # (compiler._compile_jsonl_scan), with exactly the columns/predicates rugo decodes.
    original_prepare = rugo_native.prepare_jsonl_context
    asked = {"columns": set(), "predicates": set()}

    def spy(columns=None, predicates=None, **kwargs):
        asked["columns"].update(columns or [])
        asked["predicates"].update(p[0] for p in predicates or [])
        return original_prepare(columns=columns, predicates=predicates, **kwargs)

    config.features.disable_jsonl_nested_extraction = not pushed
    rugo_native.prepare_jsonl_context = spy
    try:
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(name).to_pylist() for name in morsel.column_names]))
    finally:
        config.features.disable_jsonl_nested_extraction = original_flag
        rugo_native.prepare_jsonl_context = original_prepare
    return sorted(rows, key=repr), asked


def _same_both_ways(sql):
    on, asked = _run(sql, pushed=True)
    off, _ = _run(sql, pushed=False)
    assert on == off
    return on, asked


def test_projection_is_read_by_the_scan(data):
    rows, asked = _same_both_ways(
        f"SELECT id, c ->> 'x' AS tx, c -> 'x' AS jx, c ->> 'n' AS tn, c -> 'n' AS jn FROM {data}"
    )
    assert {"c->>'x'", "c->'x'", "c->>'n'", "c->'n'"} <= asked["columns"]
    assert "c" not in asked["columns"]
    by_id = {row[0]: row[1:] for row in rows}
    assert by_id[4][0] == "null"              # the string "null", not NULL
    assert by_id[5][0] is None                # JSON null
    assert by_id[8] == (None, None, None, None)  # an array has no keys
    assert by_id[2][3] == "1.10"              # numbers keep their source token


def test_filters_on_pushed_columns_push_into_the_reader(data):
    rows, asked = _same_both_ways(
        f"SELECT c ->> 'col' AS col, COUNT(*) AS n FROM {data} "
        "WHERE c ->> 'op' = 'create' GROUP BY col ORDER BY n DESC"
    )
    assert "c->>'op'" in asked["predicates"]
    assert "c" not in asked["columns"]
    assert dict(rows) == {"post": 4, "like": 1, None: 1}  # id 9's key is escaped: still `col`


@pytest.mark.parametrize(
    "condition",
    [
        "c ->> 'x' <> 'plain'",
        "c ->> 'x' < 'm'",
        "c ->> 'x' IN ('plain', 'null', '5')",
        "c ->> 'x' IS NULL",
        "c ->> 'x' IS NOT NULL",
        "c ->> 'n' = '5'",   # the JSON string "5" and nothing else
        "c ->> 'n' = '1.10'",
    ],
)
def test_comparisons_mean_the_same_text(data, condition):
    _same_both_ways(f"SELECT id FROM {data} WHERE {condition}")


def test_one_expression_bound_in_select_and_where(data):
    _same_both_ways(f"SELECT c ->> 'col' AS col FROM {data} WHERE c ->> 'col' IN ('post', 'like')")


def test_raw_object_still_read_when_also_used_whole(data):
    rows, asked = _same_both_ways(f"SELECT id, c, c ->> 'col' AS col FROM {data}")
    assert {"c", "c->>'col'"} <= asked["columns"]


@pytest.mark.parametrize("path", ["a.b", "0", "$.col", "/col", ""])
def test_paths_that_are_not_one_object_key_are_not_pushed(data, path):
    rows, asked = _same_both_ways(f"SELECT id, c ->> '{path}' AS v FROM {data}")
    assert "c" in asked["columns"]
    assert not any("->" in column for column in asked["columns"])


def test_json_text_in_a_varchar_column_is_not_pushed(data):
    # `s` is a STRING holding JSON text: rugo sees a string, not an object.
    rows, asked = _same_both_ways(f"SELECT id, s ->> 'k' AS k FROM {data}")
    assert "s" in asked["columns"]


def test_chained_extraction(data):
    _same_both_ways(f"SELECT id, (c -> 'o') ->> 'z' AS z FROM {data}")


def test_subquery_and_order_and_aggregates(data):
    _same_both_ways(
        f"SELECT col FROM (SELECT c ->> 'col' AS col FROM {data}) AS t WHERE col = 'post'"
    )
    _same_both_ways(f"SELECT id FROM {data} ORDER BY c ->> 'x', id LIMIT 4")
    _same_both_ways(f"SELECT MAX(c ->> 'x'), MIN(c ->> 'col'), COUNT(c ->> 'op') FROM {data}")


def test_count_distinct_star_keeps_every_column(data):
    # `COUNT(DISTINCT *)` dedups on every column in scope — `c` (VARIANT) included, which
    # the engine refuses as a key. Pushed or not, the answer must be that same refusal: had
    # the strategy dropped `c` from the aggregate's columns, the query would silently run.
    from opteryx.exceptions import VariantKeyError

    sql = f"SELECT c ->> 'col' AS col, COUNT(DISTINCT *) FROM {data} GROUP BY col"
    for pushed in (True, False):
        with pytest.raises(VariantKeyError):
            _run(sql, pushed=pushed)
