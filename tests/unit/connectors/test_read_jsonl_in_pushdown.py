"""READ_JSONL pushes `column IN (...)` / `column NOT IN (...)` into rugo's scan as an
('in' | 'not in', members) predicate rather than leaving a Filter above it.

Pushing may only change WHERE the predicate runs, never WHAT it keeps: every query runs
with the IN-list pushdown on and declined, and must return the same rows. A spy on the
plan-time rugo call asserts what was pushed.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
import rugo.rugo_native as rugo_native
from opteryx.connectors import jsonl_io

ROWS = [
    r'{"id":1,"s":"a","n":1,"f":1.5,"b":true,"c":{"col":"post"}}',
    r'{"id":2,"s":"b","n":2,"f":2.5,"b":false,"c":{"col":"like"}}',
    r'{"id":3,"s":"c","n":3,"f":3.5,"b":true,"c":{"col":"repost"}}',
    r'{"id":4,"s":null,"n":null,"f":null,"b":null,"c":{"col":null}}',
    r'{"id":5,"c":{}}',
    r'{"id":6,"s":"a","n":1,"f":1.5,"b":false,"c":{"col":"post","x":1}}',
    r'{"id":7,"s":"é","n":-7,"f":-0.0,"b":true,"c":{"col":"é"}}',
]


@pytest.fixture
def data(tmp_path):
    path = tmp_path / "in.jsonl"
    path.write_text("\n".join(ROWS) + "\n")
    return f"READ_JSONL('{path}')"


def _run(sql, push_in):
    original_can_push = jsonl_io.JsonlPredicatePushable.can_push
    original_prepare = rugo_native.prepare_jsonl_context
    asked = []

    def declining_can_push(self, operator, types=None):
        if operator.condition.value in ("InList", "NotInList"):
            return False
        return original_can_push(self, operator, types)

    def spy(columns=None, predicates=None, **kwargs):
        asked.extend(predicates or [])
        return original_prepare(columns=columns, predicates=predicates, **kwargs)

    if not push_in:
        jsonl_io.JsonlPredicatePushable.can_push = declining_can_push
    rugo_native.prepare_jsonl_context = spy
    try:
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(name).to_pylist() for name in morsel.column_names]))
    finally:
        jsonl_io.JsonlPredicatePushable.can_push = original_can_push
        rugo_native.prepare_jsonl_context = original_prepare
    return sorted(rows, key=repr), asked


@pytest.mark.parametrize(
    "condition, column, op, ids",
    [
        ("s IN ('a', 'c')", "s", "in", {1, 3, 6}),
        ("s NOT IN ('a', 'c')", "s", "not in", {2, 7}),
        ("s IN ('é', 'zz')", "s", "in", {7}),
        ("n IN (1, 3)", "n", "in", {1, 3, 6}),
        ("n NOT IN (1, 3)", "n", "not in", {2, 7}),
        ("c ->> 'col' IN ('post', 'repost')", "c->>'col'", "in", {1, 3, 6}),
        ("c ->> 'col' NOT IN ('post', 'repost')", "c->>'col'", "not in", {2, 7}),
        ("c ->> 'col' IN ('nope', 'none')", "c->>'col'", "in", set()),
    ],
)
def test_in_list_is_pushed_and_keeps_the_same_rows(data, condition, column, op, ids):
    sql = f"SELECT id FROM {data} WHERE {condition}"
    pushed_rows, asked = _run(sql, push_in=True)
    declined_rows, _ = _run(sql, push_in=False)
    assert pushed_rows == declined_rows
    assert {row[0] for row in pushed_rows} == ids
    assert [p[:2] for p in asked] == [(column, op)]


def test_in_list_pushes_beside_other_predicates(data):
    sql = f"SELECT id FROM {data} WHERE c ->> 'col' IN ('post', 'like') AND n > 1"
    pushed_rows, asked = _run(sql, push_in=True)
    declined_rows, _ = _run(sql, push_in=False)
    assert pushed_rows == declined_rows == [(2,)]
    assert sorted(p[:2] for p in asked) == [("c->>'col'", "in"), ("n", ">")]
