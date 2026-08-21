"""
Unit tests for the estimation harness's structural operator key (runner.py).

Pure logic over synthetic plan records — no engine, no queries, no dataset.
The key's whole value is the four promises made in the runner docstring, and
each of them is a test here: an unchanged plan keys identically, an edit in
one branch does not disturb another, a genuinely new operator gets a key no
baseline has (so compare mode reports it ADDED rather than pairing it with a
neighbour), and fabricated `_UNKNOWN_ROW_COUNT` estimates are flagged where
they are made and everywhere they propagate.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.performance.estimation.runner import _UNKNOWN_ROW_COUNT  # noqa: E402
from tests.performance.estimation.runner import _plan_identity  # noqa: E402
from tests.performance.estimation.runner import _score_query  # noqa: E402


def _op(nid, operator, config=None, est=None, actual=0):
    return {
        "nid": nid,
        "operator": operator,
        "type": operator,
        "config": config,
        "est_rows": est,
        "est_rows_kind": "estimate" if est is not None else None,
        "actual_rows": actual,
    }


def _edge(producer, consumer, leg=None):
    edge = {"from": producer, "to": consumer}
    if leg:
        edge["leg"] = leg
    return edge


def _keys(operators, edges):
    return {nid: info["key"] for nid, info in _plan_identity(operators, edges).items()}


# A two-branch join plan: SORT <- JOIN <- (SCAN orders | SCAN lineitem)
def _base_plan():
    operators = [
        _op("a", "SORT"),
        _op("b", "HASH JOIN"),
        _op("c", "TABLE SCAN", config="db.orders"),
        _op("d", "TABLE SCAN", config="db.lineitem"),
    ]
    edges = [_edge("b", "a"), _edge("c", "b", "left"), _edge("d", "b", "right")]
    return operators, edges


def test_key_is_stable_for_an_unchanged_plan():
    operators, edges = _base_plan()
    # Same plan, freshly generated nids (the real worker regenerates them every
    # run) — the key must not carry any of that per-run identity through.
    renamed_ops = [dict(op, nid=op["nid"].upper()) for op in operators]
    renamed_edges = [
        {**e, "from": e["from"].upper(), "to": e["to"].upper()} for e in edges
    ]
    first = _keys(operators, edges)
    second = _keys(renamed_ops, renamed_edges)
    assert sorted(first.values()) == sorted(second.values())


def test_key_does_not_move_when_a_measured_value_moves():
    operators, edges = _base_plan()
    before = _keys(operators, edges)
    moved = [dict(op, est_rows=(op["est_rows"] or 0) + 12345, actual_rows=99) for op in operators]
    assert _keys(moved, edges) == before


def test_an_edit_in_one_branch_leaves_the_other_branch_alone():
    operators, edges = _base_plan()
    before = _keys(operators, edges)

    # Insert a FILTER above the orders scan only. The lineitem branch is
    # untouched; the join's relation multiset is unchanged, so it keys the same.
    operators = operators + [_op("f", "FILTER")]
    edges = [e for e in edges if not (e["from"] == "c" and e["to"] == "b")]
    edges += [_edge("c", "f"), _edge("f", "b", "left")]
    after = _keys(operators, edges)

    assert after["d"] == before["d"], "untouched branch's scan re-keyed"
    assert after["b"] == before["b"], "join over the same relations re-keyed"
    assert after["a"] == before["a"], "operator above the edit re-keyed"


def test_a_new_operator_is_a_new_key_never_a_silent_repairing():
    operators, edges = _base_plan()
    before = set(_keys(operators, edges).values())

    operators = operators + [_op("f", "FILTER")]
    edges = [e for e in edges if not (e["from"] == "c" and e["to"] == "b")]
    edges += [_edge("c", "f"), _edge("f", "b", "left")]
    after = set(_keys(operators, edges).values())

    assert before <= after, "an existing operator lost its key to the new one"
    assert len(after - before) == 1, "the new operator did not surface as exactly one added key"


def test_a_stacked_chain_is_separated_by_rank_not_by_depth():
    operators = [
        _op("top", "FILTER"),
        _op("mid", "FILTER"),
        _op("scan", "TABLE SCAN", config="db.orders"),
    ]
    edges = [_edge("scan", "mid"), _edge("mid", "top")]
    before = _keys(operators, edges)
    assert before["top"] != before["mid"]
    assert before["mid"].endswith("#0.0")
    assert before["top"].endswith("#1.0")

    # Insert an unrelated PROJECT between them. Depth-from-root would shift for
    # `top`; rank must not, because no same-role operator was added.
    operators = operators + [_op("proj", "PROJECT")]
    edges = [_edge("scan", "mid"), _edge("mid", "proj"), _edge("proj", "top")]
    after = _keys(operators, edges)
    assert after["top"] == before["top"]
    assert after["mid"] == before["mid"]


def test_interchangeable_siblings_get_distinct_deterministic_keys():
    # A self-join: both scans are the same operator over the same relation at
    # the same rank. They can only be told apart by the ordinal, which is the
    # documented limit — but they must still be two keys, not one.
    operators = [
        _op("j", "HASH JOIN"),
        _op("l", "TABLE SCAN", config="db.orders"),
        _op("r", "TABLE SCAN", config="db.orders"),
    ]
    edges = [_edge("l", "j", "left"), _edge("r", "j", "right")]
    keys = _keys(operators, edges)
    assert len({keys["l"], keys["r"]}) == 2
    assert keys == _keys(operators, edges), "ordinal assignment is not deterministic"
    # The join sees the relation twice — a multiset, not a set.
    assert "db.orders*2" in keys["j"]


def test_standin_estimates_are_flagged_where_made_and_where_inherited():
    operators = [
        _op("sort", "SORT", est=_UNKNOWN_ROW_COUNT * 3, actual=10),
        _op("join", "HASH JOIN", est=_UNKNOWN_ROW_COUNT * 3, actual=10),
        _op("bad", "TABLE SCAN", config="db.unsized", est=_UNKNOWN_ROW_COUNT, actual=10),
        _op("good", "TABLE SCAN", config="db.orders", est=1500, actual=1500),
    ]
    edges = [
        _edge("bad", "join", "left"),
        _edge("good", "join", "right"),
        _edge("join", "sort"),
    ]
    taint = {nid: info["stand_in"] for nid, info in _plan_identity(operators, edges).items()}
    assert taint["bad"] == "direct"
    assert taint["join"] == "direct"  # the multiple is itself a stand-in value
    assert taint["sort"] == "direct"
    assert taint["good"] is None


def test_standins_are_excluded_from_the_ex_standin_geomean_only():
    operators = [
        _op("bad", "TABLE SCAN", config="db.unsized", est=_UNKNOWN_ROW_COUNT, actual=10),
        _op("good", "TABLE SCAN", config="db.orders", est=1500, actual=1500),
    ]
    entry = _score_query({"status": "ok", "operators": operators, "edges": []})
    assert entry["standin_operators"] == 1
    flagged = {op["key"]: op.get("stand_in") for op in entry["operators"]}
    assert sum(1 for v in flagged.values() if v == "direct") == 1
    # Both operators are still SCORED — the exclusion lives in the summary, so
    # the raw q-error record stays complete and auditable.
    assert entry["pairs"] == 2


def test_scored_operators_are_ordered_by_key_not_by_measurement():
    operators, edges = _base_plan()
    entry = _score_query({"status": "ok", "operators": operators, "edges": edges})
    keys = [op["key"] for op in entry["operators"]]
    assert keys == sorted(keys)
    assert all(key is not None for key in keys)


if __name__ == "__main__":  # pragma: no cover
    import traceback

    failures = 0
    for name, fn in sorted(list(globals().items())):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  ok    {name}")
        except Exception:
            failures += 1
            print(f"  FAIL  {name}")
            traceback.print_exc()
    print("all passed" if not failures else f"{failures} failed")
    sys.exit(1 if failures else 0)
