"""
Optimizer rewrites must build new expression nodes, never edit the ones they are given.

An expression node can be reachable from more than one place: an aggregate shared
by the Aggregate step and HAVING, a conjunct also held by the pre-optimization
plan, a column object shared by a node and its copy. A rewrite that edited its
input in place changed the expression for every other holder - a LIKE became
LIKE ANY (or FALSE) somewhere else, an equality came to name a column that did
not exist where it sat, a NOT was flipped twice. Each rewrite below was fixed to
allocate; this pins that, by running real queries through the real optimizer,
wrapping each rewrite, and comparing a snapshot of every field of its input
before and after the call.

The wrappers also record whether each rewrite actually FIRED on these queries,
and the test requires that it did - a snapshot of a rewrite that never ran
proves nothing.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

import opteryx
from opteryx.compiled.structures.expressions import is_expression
from opteryx.models import LogicalColumn
from opteryx.types.schema import ConstantColumn
import opteryx.planner.optimizer.strategies.cross_join_filter_pushdown as cross_join
import opteryx.planner.optimizer.strategies.join_key_materialization as join_keys
import opteryx.planner.optimizer.strategies.predicate_ordering as ordering
import opteryx.planner.optimizer.strategies.predicate_rewriter as rewriter
import opteryx.planner.optimizer.strategies.project_fusion as fusion
import opteryx.planner.optimizer.strategies.split_conjunctive_predicates as splitting

TABLE = "testdata.fuzzing.mixed"


def _fields(obj):
    """Every declared field of a typed expression, read through its properties."""
    names = set()
    for cls in type(obj).__mro__:
        for name, member in vars(cls).items():
            if not name.startswith("_") and type(member).__name__ in ("getset_descriptor", "property"):
                names.add(name)
    return {name: getattr(obj, name) for name in sorted(names)}


def _snapshot(*roots):
    """Every field of every expression object reachable from `roots`."""
    snap = {}

    def rep(value):
        if is_expression(value):
            return ("node", id(value))
        if isinstance(value, (list, tuple)):
            return tuple(rep(item) for item in value)
        return repr(value)[:200]

    def walk(obj):
        if not is_expression(obj) or id(obj) in snap:
            return
        if isinstance(obj, LogicalColumn):
            snap[id(obj)] = ("column", obj.current_name)
            return
        fields = _fields(obj)
        column = fields.pop("schema_column", None)
        snap[id(obj)] = (
            tuple(sorted((k, rep(v)) for k, v in fields.items())),
            None
            if column is None
            else (
                id(column),
                repr(column.column_type),
                repr(column.aliases),
                repr(column.value)[:200] if isinstance(column, ConstantColumn) else None,
            ),
        )
        for value in fields.values():
            if is_expression(value):
                walk(value)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    walk(item)

    for root in roots:
        walk(root)
    assert snap or not roots, "the snapshot saw none of its roots - the check is blind"
    return snap


# (module, function name, positions of the arguments that are the INPUT tree,
#  query that makes it fire)
_REWRITES = [
    (rewriter, "rewrite_ored_like_to_any", (0,), f"SELECT row_id FROM {TABLE} WHERE s_low LIKE 'a%' OR s_low LIKE '%b'"),
    (rewriter, "rewrite_ored_eq_to_inlist", (0,), f"SELECT (i_group = 1 OR i_group = 3) AS p FROM {TABLE}"),
    (rewriter, "rewrite_ored_any_eq_to_contains", (0,), f"SELECT row_id FROM {TABLE} WHERE 'alpha' = ANY(arr_str) OR 'beta' = ANY(arr_str)"),
    (rewriter, "rewrite_cnf_like_to_any", (0,), f"SELECT row_id FROM {TABLE} WHERE s_low LIKE 'a%' OR s_low LIKE '%b' OR s_low LIKE '%c%'"),
    (rewriter, "rewrite_cnf_eq_to_inlist", (0,), f"SELECT row_id FROM {TABLE} WHERE i_group = 1 OR i_group = 3 OR i_group = 5"),
    (rewriter, "rewrite_cnf_any_eq_to_contains", (0,), f"SELECT row_id FROM {TABLE} WHERE 'alpha' = ANY(arr_str) OR 'beta' = ANY(arr_str) OR 'gamma' = ANY(arr_str)"),
    # Patched where it is CALLED: splitting imports it under its own name, and the
    # two below are reached through the rewriter's `dispatcher` dict.
    (splitting, "rewrite_anded_not_like_to_all", (0,), f"SELECT row_id FROM {TABLE} WHERE s_low NOT LIKE 'a%' AND s_low NOT LIKE '%z' AND row_id > 3"),
    (rewriter, "_rewrite_rlike_to_dfa", (0,), f"SELECT row_id FROM {TABLE} WHERE s_low RLIKE '^a'"),
    (rewriter.dispatcher, "rewrite_in_to_eq", (0,), f"SELECT row_id FROM {TABLE} WHERE i_group IN (3)"),
    (rewriter.dispatcher, "reorder_interval_calc", (0,), f"SELECT row_id FROM {TABLE} WHERE ts_value - ts_null > INTERVAL '1' DAY"),
    (rewriter, "rewrite_int_vs_fractional_const", (0,), f"SELECT row_id FROM {TABLE} WHERE i_null != 4.5"),
    (rewriter, "rewrite_unsatisfiable_case_fold", (0,), f"SELECT row_id FROM {TABLE} WHERE UPPER(s_null) = 'Ab'"),
    (ordering, "rewrite_anded_any_eq_to_contains_all", (0,), f"SELECT row_id FROM {TABLE} WHERE 'alpha' = ANY(arr_str) AND 'beta' = ANY(arr_str)"),
    (fusion, "_substitute_column", (0,), f"SELECT UPPER(v) AS u FROM (SELECT s_low || s_high AS v FROM {TABLE}) AS s"),
    (cross_join, "_hoist_arithmetic_join_key", (3,), f"SELECT a.row_id FROM {TABLE} AS a, {TABLE} AS b WHERE a.row_id = b.row_id - 53"),
]


@pytest.mark.parametrize(
    "module, name, input_positions, statement", _REWRITES, ids=[r[1] for r in _REWRITES]
)
def test_rewrite_leaves_its_input_untouched(monkeypatch, module, name, input_positions, statement):
    original = module[name] if isinstance(module, dict) else getattr(module, name)
    calls = []

    def wrapped(*args, **kwargs):
        inputs = [args[position] for position in input_positions]
        roots = [item for value in inputs for item in (value if isinstance(value, list) else [value])]
        before = _snapshot(*roots)
        result = original(*args, **kwargs)
        # A rewrite that returns its input unchanged did not fire; one that
        # returns anything else did - either way the input must be as it was.
        fired = not any(result is value for value in inputs) and result is not None
        calls.append((fired, before == _snapshot(*roots)))
        return result

    if isinstance(module, dict):
        monkeypatch.setitem(module, name, wrapped)
    else:
        monkeypatch.setattr(module, name, wrapped)
    for _ in opteryx.session().execute_to_morsels(statement):
        pass

    assert any(fired for fired, _ in calls), f"{name} never fired on this query - the check is blind"
    assert all(unchanged for _, unchanged in calls), f"{name} modified its input in place"


def test_join_key_materialization_leaves_on_conjuncts_untouched(monkeypatch):
    strategy = join_keys.JoinKeyMaterializationStrategy
    original = strategy._materialize_keys
    calls = []

    def wrapped(self, plan, join_id):
        on = plan[join_id].on
        conjuncts = join_keys.split_and_conditions(on)
        before = _snapshot(*conjuncts)
        original(self, plan, join_id)
        calls.append((plan[join_id].on is not on, before == _snapshot(*conjuncts)))

    monkeypatch.setattr(strategy, "_materialize_keys", wrapped)
    statement = f"SELECT a.row_id FROM {TABLE} AS a INNER JOIN {TABLE} AS b ON a.row_id = b.i_group * 2"
    for _ in opteryx.session().execute_to_morsels(statement):
        pass

    assert any(rewrote for rewrote, _ in calls), "no ON conjunct was materialized - the check is blind"
    assert all(unchanged for _, unchanged in calls), "an ON conjunct was modified in place"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
