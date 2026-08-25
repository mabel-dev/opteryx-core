# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
MERGE INTO — desugaring to a join plus a per-row action.

MERGE is not a new kind of write. It is one join whose every output row carries
an action, feeding the two mutation primitives the catalog already has: mark a
row deleted, append a row. An UPDATE is a delete plus an append; the only reason
it cannot be spelled that way in SQL is that its new values are computed from
the row it replaces.

    MERGE INTO tgt n USING src t ON <on>            SELECT <blend>, $merge_action,
      WHEN MATCHED AND p THEN UPDATE SET c = ...      n.$file, n.$ordinal
      WHEN NOT MATCHED THEN INSERT ...        →     FROM src t LEFT JOIN tgt n ON <on>

**Source on the LEFT.** That single choice is what makes classification
unambiguous. The left row is always present, so `(<on>) IS TRUE` means matched,
and no presence-marker column is needed. Written the other way round — target
left, or FULL OUTER — a target row with a NULL join key and a source row with a
NULL join key are indistinguishable and need opposite treatment, and the marker
that would tell them apart cannot be built (the dialect refuses
`SELECT *, TRUE AS m FROM …`, and a general USING source has no knowable column
list).

Untouched target rows never enter the plan at all: they are not emitted, not
read past the join, and not rewritten. That is what merge-on-read means, and it
is why a feed that republishes mostly-unchanged rows costs almost nothing.

**Arm order is semantics.** Within a population, the first arm whose condition
holds wins, so the CASE chain must preserve declaration order exactly. The two
populations (matched / not matched) may be emitted in either block order because
no row satisfies both guards — every not-matched branch is guarded by
`NOT ((<on>) IS TRUE)` and a matched row fails that guard.

The action codes below are read by MergeNode, which is the only consumer.

This module also owns **UPDATE** and **DELETE**. They are MERGE with a
degenerate source - no join, one constant action - and they share the action
codes, the row-address columns, the binder visitor and the sink. They live here
so that sharing is visible rather than something two modules have to keep in
step. See the UPDATE/DELETE block at the foot of the file.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError

# Action codes carried in `$merge_action`. MergeNode is the only reader.
#   NOOP   — nothing at all: no delete position, no appended row. An unmatched
#            source row no arm claimed, or a matched row whose every arm
#            condition was false (the IS DISTINCT FROM guard's whole purpose).
#   INSERT — append only.
#   UPDATE — delete position AND append.
#   DELETE — delete position only.
MERGE_NOOP = 0
MERGE_INSERT = 1
MERGE_UPDATE = 2
MERGE_DELETE = 3

# Control columns the sink reads and then drops. Named so they cannot collide
# with a target column: `$` is the engine-internal prefix and is unspellable in
# user SQL by convention.
MERGE_ACTION_COLUMN = "$merge_action"
MERGE_FILE_COLUMN = "$merge_file"
MERGE_ORDINAL_COLUMN = "$merge_ordinal"


# ── AST constructors ────────────────────────────────────────────────────────
# The Merge AST's own sub-expressions (the ON condition, each arm's predicate,
# each assignment's value) are embedded VERBATIM into the query built here —
# never re-rendered to SQL and re-parsed. A round trip through text is where
# quoting and precedence bugs live, and there is nothing to gain from it when
# the parsed form is already in hand.


def _ident(relation: str, column: str) -> dict:
    return {"CompoundIdentifier": [{"value": relation}, {"value": column}]}


def _null() -> dict:
    return {"Value": {"value": "Null"}}


def _int(value: int) -> dict:
    return {"Value": {"value": {"Number": [str(value), False]}}}


def _nested(expr: dict) -> dict:
    return {"Nested": expr}


def _not(expr: dict) -> dict:
    return {"UnaryOp": {"op": "Not", "expr": expr}}


def _and(left: dict, right: dict) -> dict:
    return {"BinaryOp": {"left": left, "op": "And", "right": right}}


def _is_true(expr: dict) -> dict:
    return {"IsTrue": _nested(expr)}


def _case(conditions: List[Tuple[dict, dict]], else_result: dict) -> dict:
    return {
        "Case": {
            "operand": None,
            "conditions": [{"condition": c, "result": r} for c, r in conditions],
            "else_result": else_result,
        }
    }


def _aliased(expr: dict, alias: str) -> dict:
    return {"ExprWithAlias": {"expr": expr, "alias": {"value": alias}}}


# ── Clause inspection ───────────────────────────────────────────────────────


def _relation_alias(table_factor: dict, role: str) -> str:
    """The alias a MERGE side was given, or a refusal naming what to add.

    v1 requires both sides to be aliased. Deriving one would mean rewriting
    every reference in the ON condition and the arms to match, and a
    fully-qualified reference to an un-aliased dotted relation
    (`ws.col.ds.column`) has no spelling the binder resolves anyway. Refusing
    with the fix in the message beats silently binding to the wrong thing.
    """
    table = table_factor.get("Table")
    if table is None:
        raise UnsupportedSyntaxError(
            f"**MERGE INTO** requires a table as its {role}. A sub-query source is "
            "not supported yet; write it to a table first."
        )
    alias = table.get("alias")
    if not alias or not alias.get("name", {}).get("value"):
        name = ".".join(p["Identifier"]["value"] for p in table["name"])
        example = "n" if role == "target" else "s"
        raise UnsupportedSyntaxError(
            f"**MERGE INTO** requires an alias on its {role}. Write "
            f"`{name} AS {example}` and qualify the {role}'s columns with it."
        )
    return alias["name"]["value"]


def _relation_name(table_factor: dict) -> str:
    return ".".join(p["Identifier"]["value"] for p in table_factor["Table"]["name"])


def _insert_assignments(action: dict) -> Dict[str, dict]:
    """`{target column: value expression}` for a NOT MATCHED arm's INSERT."""
    insert = action["Insert"]
    columns = [c[0]["Identifier"]["value"] for c in insert.get("columns") or []]
    kind = insert.get("kind") or {}
    values = kind.get("Values")
    if values is None:
        raise UnsupportedSyntaxError(
            "**MERGE INTO**'s **INSERT** arm must use **VALUES**. "
            "`INSERT ROW` and `INSERT *` are not supported."
        )
    rows = values.get("rows") or []
    if len(rows) != 1:
        raise UnsupportedSyntaxError(
            "**MERGE INTO**'s **INSERT** arm takes exactly one **VALUES** row."
        )
    contents = rows[0]["content"]
    if not columns:
        raise UnsupportedSyntaxError(
            "**MERGE INTO**'s **INSERT** arm must name its columns. Write "
            "`INSERT (a, b) VALUES (...)` so the values cannot silently bind to "
            "the wrong columns when the target's schema changes."
        )
    if len(columns) != len(contents):
        raise UnsupportedSyntaxError(
            f"**MERGE INTO**'s **INSERT** arm names {len(columns)} column(s) but "
            f"supplies {len(contents)} value(s)."
        )
    return dict(zip(columns, contents))


def _update_assignments(action: dict, owner: str = "**MERGE INTO**'s **UPDATE** arm") -> Dict[str, dict]:
    """`{target column: value expression}` for a SET list.

    Shared by MERGE's MATCHED arm and the UPDATE statement: the two spell SET
    identically, so one reader means they cannot drift. `owner` names whichever
    is being read, so the refusal points at the SQL the user actually wrote.
    """
    update = action["Update"]
    if update.get("update_predicate") or update.get("delete_predicate"):
        raise UnsupportedSyntaxError(
            "**MERGE INTO** does not support **WHERE** on an **UPDATE** arm. "
            "Put the condition on the arm itself: `WHEN MATCHED AND <cond> THEN`."
        )
    out: Dict[str, dict] = {}
    for assignment in update.get("assignments") or []:
        target = assignment.get("target", {}).get("ColumnName")
        if target is None or len(target) != 1:
            raise UnsupportedSyntaxError(
                f"{owner}'s **SET** targets a single unqualified "
                "column name on the left of each assignment."
            )
        column = target[0]["Identifier"]["value"]
        if column in out:
            raise UnsupportedSyntaxError(
                f"{owner}'s **SET** assigns `{column}` more than once."
            )
        out[column] = assignment["value"]
    if not out:
        raise UnsupportedSyntaxError(f"{owner} assigns nothing.")
    return out


class _Arm:
    """One WHEN clause, reduced to what the chains need."""

    __slots__ = ("matched", "predicate", "action_code", "assignments")

    def __init__(self, matched: bool, predicate: Optional[dict], action_code: int, assignments):
        self.matched = matched
        self.predicate = predicate
        self.action_code = action_code
        self.assignments = assignments  # {column: expr}, empty for DELETE


def _read_arms(clauses: List[dict]) -> List[_Arm]:
    arms: List[_Arm] = []
    for clause in clauses:
        kind = clause.get("clause_kind")
        if kind == "NotMatchedBySource":
            raise UnsupportedSyntaxError(
                "**MERGE INTO** does not support **WHEN NOT MATCHED BY SOURCE**. "
                "It acts on target rows the source never mentioned, which this "
                "implementation does not read."
            )
        if kind not in ("Matched", "NotMatched"):
            raise UnsupportedSyntaxError(f"Unsupported **MERGE** clause: {kind}")
        matched = kind == "Matched"
        action = clause["action"]
        if "Insert" in action:
            if matched:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN MATCHED** arm cannot **INSERT** — the "
                    "row already exists. Use **UPDATE** or **DELETE**."
                )
            arms.append(_Arm(False, clause.get("predicate"), MERGE_INSERT, _insert_assignments(action)))
        elif "Update" in action:
            if not matched:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN NOT MATCHED** arm cannot **UPDATE** — "
                    "there is no row to update. Use **INSERT**."
                )
            arms.append(_Arm(True, clause.get("predicate"), MERGE_UPDATE, _update_assignments(action)))
        elif "Delete" in action:
            if not matched:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN NOT MATCHED** arm cannot **DELETE** — "
                    "there is no row to delete."
                )
            arms.append(_Arm(True, clause.get("predicate"), MERGE_DELETE, {}))
        else:
            raise UnsupportedSyntaxError(f"Unsupported **MERGE** action: {sorted(action)}")
    if not arms:
        raise UnsupportedSyntaxError("**MERGE INTO** needs at least one **WHEN** clause.")
    return arms


# ── Chain building ──────────────────────────────────────────────────────────


def _chain(arms, unmatched_guard, per_arm_result, unmatched_default, matched_default):
    """The one CASE chain shape both the action code and every blended column use.

    Not-matched arms first, each guarded; then a catch-all for an unmatched row
    no arm claimed; then the matched arms in declaration order, which need no
    guard because every unmatched row has already been caught above; then the
    matched catch-all as ELSE.
    """
    conditions = []
    for arm in arms:
        if arm.matched:
            continue
        guard = unmatched_guard
        if arm.predicate is not None:
            guard = _and(unmatched_guard, _nested(arm.predicate))
        conditions.append((guard, per_arm_result(arm)))
    conditions.append((unmatched_guard, unmatched_default))
    for arm in arms:
        if not arm.matched:
            continue
        if arm.predicate is None:
            # An unconditional matched arm ends the chain: nothing after it can
            # ever be reached, and emitting the rest would be dead branches.
            return _case(conditions, per_arm_result(arm))
        conditions.append((_nested(arm.predicate), per_arm_result(arm)))
    return _case(conditions, matched_default)


def plan_merge(statement, **kwargs):
    """Build the logical plan for MERGE INTO."""
    # Imported here rather than at module scope: logical_planner imports this
    # module, so a top-level import back into it would be circular.
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.constants.row_identity import ROW_IDENTITY_FILE
    from opteryx.constants.row_identity import ROW_IDENTITY_ORDINAL
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.logical_planner.logical_planner import plan_query
    from opteryx.utils import random_string

    merge = statement["Merge"]

    target_factor = merge["table"]
    source_factor = merge["source"]
    target_alias = _relation_alias(target_factor, "target")
    source_alias = _relation_alias(source_factor, "source")
    if target_alias == source_alias:
        raise UnsupportedSyntaxError(
            f"**MERGE INTO**'s target and source share the alias `{target_alias}`; "
            "a column reference could mean either. Give them different aliases."
        )
    target_name = _relation_name(target_factor)

    on_expr = merge["on"]
    arms = _read_arms(merge["clauses"])

    # The target's columns, in schema order — the shape the merged rows must
    # have. Read here rather than at bind time because the projection cannot be
    # built without it; the logical planner already resolves connectors this way
    # for REFRESH MATERIALIZED VIEW.
    connector = connector_factory(target_name, telemetry=kwargs.get("telemetry"))
    if not isinstance(connector, Writable):
        raise ReadOnlyConnectorError(f"connector for {target_name} does not support MERGE")
    target_columns = list(connector.relation_column_names(target_name))
    if not target_columns:
        raise UnsupportedSyntaxError(f"**MERGE INTO** target {target_name} has no columns.")

    unmatched = _not(_is_true(on_expr))

    # ── the action code ──────────────────────────────────────────────────────
    action_expr = _chain(
        arms,
        unmatched_guard=unmatched,
        per_arm_result=lambda arm: _int(arm.action_code),
        unmatched_default=_int(MERGE_NOOP),
        matched_default=_int(MERGE_NOOP),
    )

    # ── one blended column per target column ─────────────────────────────────
    # A DELETE arm keeps a branch here even though its row is dropped: without
    # one, a row that took the DELETE arm would fall through to a LATER arm's
    # value, which is correct only by accident. Keeping the branch means arm
    # order in this chain is the same arm order as in the action chain.
    projection = []
    for column in target_columns:
        old_value = _ident(target_alias, column)

        def result_for(arm, _column=column, _old=old_value):
            if arm.matched:
                return arm.assignments.get(_column, _old)
            return arm.assignments.get(_column, _null())

        projection.append(
            _aliased(
                _chain(
                    arms,
                    unmatched_guard=unmatched,
                    per_arm_result=result_for,
                    # An unmatched row no arm claimed is dropped by the sink, so
                    # its values are never read; NULL says that honestly rather
                    # than reaching for the target row, which does not exist.
                    unmatched_default=_null(),
                    matched_default=old_value,
                ),
                column,
            )
        )

    projection.append(_aliased(action_expr, MERGE_ACTION_COLUMN))
    projection.append(_aliased(_ident(target_alias, ROW_IDENTITY_FILE), MERGE_FILE_COLUMN))
    projection.append(_aliased(_ident(target_alias, ROW_IDENTITY_ORDINAL), MERGE_ORDINAL_COLUMN))

    select = {
        "Select": {
            "distinct": None,
            "top": None,
            "top_before_distinct": False,
            "projection": projection,
            "exclude": None,
            "into": None,
            "from": [
                {
                    "relation": source_factor,
                    "joins": [
                        {
                            "relation": target_factor,
                            "global": False,
                            "join_operator": {"LeftOuter": {"On": on_expr}},
                        }
                    ],
                }
            ],
            "lateral_views": [],
            "prewhere": None,
            # No WHERE: MergeNode drops the NOOP rows. Repeating the action
            # chain here to filter them would double the work of evaluating
            # every arm predicate, and the sink is masking per morsel anyway.
            "selection": None,
            "connect_by": [],
            "group_by": {"Expressions": [[], []]},
            "cluster_by": [],
            "distribute_by": [],
            "sort_by": [],
            "having": None,
            "named_window": [],
            "qualify": None,
            "window_before_qualify": False,
            "value_table_mode": None,
            "flavor": "Standard",
            "optimizer_hints": [],
            "select_modifiers": None,
        }
    }
    query = {
        "Query": {
            "with": None,
            "body": select,
            "order_by": None,
            "limit_clause": None,
            "fetch": None,
            "locks": [],
            "for_clause": None,
            "settings": None,
            "format_clause": None,
            "pipe_operators": [],
        }
    }

    plan = plan_query(query)
    exit_node_id = plan.get_exit_points()[0]

    # Ask the TARGET scan for row identity. Only that scan: the source's rows
    # are never addressed, and a second scan emitting `$ordinal` would force
    # single-pass on a side that has no use for it.
    stamped = False
    for _nid, node in plan.nodes(data=True):
        if node.node_type == LogicalPlanStepType.Scan and node.alias == target_alias:
            node.emit_row_identity = True
            stamped = True
    if not stamped:  # pragma: no cover - the join above always plans a target Scan
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            f"plan_merge: no Scan for target alias {target_alias} to address rows through"
        )

    merge_step = LogicalPlanNode(node_type=LogicalPlanStepType.Merge)
    merge_step.relation_name = target_name
    merge_step.target_column_names = tuple(target_columns)
    merge_step.source_tail_id = exit_node_id
    merge_step.target_alias = target_alias

    merge_id = random_string()
    plan.add_node(merge_id, merge_step)
    plan.add_edge(exit_node_id, merge_id)

    return plan


# ── UPDATE and DELETE ───────────────────────────────────────────────────────
#
# Both are MERGE with a degenerate source: there is no second relation, so
# there is no join and every scanned row that survives the WHERE is "matched"
# by construction. What is left is exactly what MERGE already does — a target
# scan carrying its row address, a per-row action, and the same sink.
#
#   DELETE FROM t WHERE p   →  SELECT 3 AS $merge_action, $file, $ordinal
#                              FROM t WHERE p
#
#   UPDATE t SET c = e      →  SELECT <every target column, c replaced by e>,
#         WHERE p              2 AS $merge_action, $file, $ordinal
#                              FROM t WHERE p
#
# DELETE projects NO payload columns at all. The sink splits by position, so
# an empty `target_column_names` puts the three control columns at 0, 1, 2 and
# the append stream is empty for every row — a DELETE never writes a data file,
# and the scan never reads a column the predicate did not ask for.
#
# Neither statement needs a cardinality check. Without a join no target row can
# be emitted twice, so the address set can only ever see each address once. The
# check still runs — it is one set insert either way — and would fire if that
# ever stopped being true.
#
# Row identity is referenced UNQUALIFIED (`$file`, not `t.$file`). An UPDATE or
# DELETE has exactly one relation in its FROM, and only a scan the planner
# stamped emits these columns at all, so the unqualified spelling is
# unambiguous — and unlike a qualified one it works for a dotted relation name
# the user did not alias, where `ws.col.ds.$file` has no spelling the binder
# resolves.


def _identifier(name: str) -> dict:
    return {"Identifier": {"value": name}}


def _select_over(projection: List[dict], relation: dict, selection, hints) -> dict:
    """One relation, no joins, an optional WHERE — the shape both statements need."""
    return {
        "Query": {
            "with": None,
            "body": {
                "Select": {
                    "distinct": None,
                    "top": None,
                    "top_before_distinct": False,
                    "projection": projection,
                    "exclude": None,
                    "into": None,
                    "from": [{"relation": relation, "joins": []}],
                    "lateral_views": [],
                    "prewhere": None,
                    "selection": selection,
                    "connect_by": [],
                    "group_by": {"Expressions": [[], []]},
                    "cluster_by": [],
                    "distribute_by": [],
                    "sort_by": [],
                    "having": None,
                    "named_window": [],
                    "qualify": None,
                    "window_before_qualify": False,
                    "value_table_mode": None,
                    "flavor": "Standard",
                    "optimizer_hints": hints or [],
                    "select_modifiers": None,
                }
            },
            "order_by": None,
            "limit_clause": None,
            "fetch": None,
            "locks": [],
            "for_clause": None,
            "settings": None,
            "format_clause": None,
            "pipe_operators": [],
        }
    }


def _target_alias(table_factor: dict, relation_name: str) -> str:
    """The alias the Scan for this relation will be keyed by.

    Mirrors `create_node_relation`, which falls back to the relation name when
    the statement gave no alias. Read here so the stamping search below looks
    for the same key the planner will actually produce.
    """
    alias = table_factor["Table"].get("alias")
    if alias and alias.get("name", {}).get("value"):
        return alias["name"]["value"]
    return relation_name


def _reject_row_returning(statement: dict, keyword: str) -> None:
    """Refuse the clauses that would make the statement return or order rows.

    RETURNING and OUTPUT need the written rows back as a result set; this sink
    produces a row count and nothing else. ORDER BY and LIMIT bound WHICH rows
    are acted on, and the engine has no ordered, bounded delete. Accepting any
    of them and ignoring it would silently act on the wrong rows.
    """
    if statement.get("returning"):
        raise UnsupportedSyntaxError(
            f"**{keyword}** does not support **RETURNING**. The statement reports "
            "how many rows it changed; it cannot return them."
        )
    if statement.get("output"):
        raise UnsupportedSyntaxError(f"**{keyword}** does not support **OUTPUT**.")
    if statement.get("order_by"):
        raise UnsupportedSyntaxError(
            f"**{keyword}** does not support **ORDER BY**. Rows are acted on as a "
            "set, not in an order."
        )
    if statement.get("limit"):
        raise UnsupportedSyntaxError(
            f"**{keyword}** does not support **LIMIT**. Narrow the **WHERE** "
            "condition so it names exactly the rows to act on."
        )


def _target_table_factor(relation: dict, keyword: str) -> dict:
    if relation.get("Table") is None:
        raise UnsupportedSyntaxError(
            f"**{keyword}** requires a table as its target. A sub-query, a "
            "function or a VALUES list has no rows to address."
        )
    return relation


def _writable_target(relation_name: str, keyword: str, telemetry):
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError

    connector = connector_factory(relation_name, telemetry=telemetry)
    if not isinstance(connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {relation_name} does not support {keyword}"
        )
    return connector


def _stamp_target_scan(plan, relation_name: str, alias: str, keyword: str) -> None:
    """Ask the target's Scan — and only it — for row identity.

    An UPDATE or DELETE has exactly one relation in its FROM, and a sub-query in
    the WHERE is not expanded into the logical plan here (it is lowered after
    binding), so this plan holds exactly one Scan. Stamping a scan that is not
    the one being written would address rows through the wrong relation, so the
    count is asserted rather than assumed: anything else means the plan shape
    changed underneath this, and picking one would be a guess.
    """
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType

    candidates = [
        node
        for _nid, node in plan.nodes(data=True)
        if node.node_type == LogicalPlanStepType.Scan
        and node.relation == relation_name
        and node.alias == alias
    ]
    if len(candidates) != 1:  # pragma: no cover - one relation, one Scan
        raise InvalidInternalStateError(
            f"{keyword}: expected exactly one Scan of {relation_name} to address "
            f"rows through, found {len(candidates)}"
        )
    candidates[0].emit_row_identity = True


def _sink_node(relation_name: str, target_columns, alias: str, keyword: str):
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType

    step = LogicalPlanNode(node_type=LogicalPlanStepType.Merge)
    step.relation_name = relation_name
    step.target_column_names = tuple(target_columns)
    step.target_alias = alias
    step.statement_name = keyword
    return step


def _attach_sink(plan, step):
    from opteryx.utils import random_string

    exit_node_id = plan.get_exit_points()[0]
    step.source_tail_id = exit_node_id
    node_id = random_string()
    plan.add_node(node_id, step)
    plan.add_edge(exit_node_id, node_id)
    return plan


def plan_delete(statement, **kwargs):
    """Build the logical plan for DELETE FROM.

    A DELETE reads nothing but the columns its predicate needs plus the row
    address, and writes no data file at all — every acted-on row contributes a
    delete position and nothing else.
    """
    from opteryx.constants.row_identity import ROW_IDENTITY_FILE
    from opteryx.constants.row_identity import ROW_IDENTITY_ORDINAL
    from opteryx.planner.logical_planner.logical_planner import plan_query

    delete = statement["Delete"]
    keyword = "DELETE FROM"

    if delete.get("tables"):
        raise UnsupportedSyntaxError(
            "**DELETE FROM** deletes from one relation. The multi-table "
            "`DELETE a, b FROM ...` form is not supported."
        )
    if delete.get("using"):
        raise UnsupportedSyntaxError(
            "**DELETE FROM** does not support **USING**. Write the condition as a "
            "sub-query in the **WHERE** clause, or use **MERGE INTO**."
        )
    _reject_row_returning(delete, keyword)

    from_clause = delete.get("from") or {}
    relations = from_clause.get("WithFromKeyword")
    if relations is None:
        relations = from_clause.get("WithoutKeyword")
    if not relations:
        raise UnsupportedSyntaxError("**DELETE FROM** needs a relation to delete from.")
    if len(relations) != 1 or relations[0].get("joins"):
        raise UnsupportedSyntaxError(
            "**DELETE FROM** deletes from one relation and cannot join. Write the "
            "condition as a sub-query in the **WHERE** clause, or use **MERGE INTO**."
        )

    table_factor = _target_table_factor(relations[0]["relation"], keyword)
    relation_name = _relation_name(table_factor)
    alias = _target_alias(table_factor, relation_name)

    _writable_target(relation_name, keyword, kwargs.get("telemetry"))

    # No payload columns: a deleted row is never re-written, so there is nothing
    # to blend and nothing to read. The three control columns are the whole
    # projection, and the scan reads only what the predicate asked for.
    projection = [
        _aliased(_int(MERGE_DELETE), MERGE_ACTION_COLUMN),
        _aliased(_identifier(ROW_IDENTITY_FILE), MERGE_FILE_COLUMN),
        _aliased(_identifier(ROW_IDENTITY_ORDINAL), MERGE_ORDINAL_COLUMN),
    ]

    plan = plan_query(
        _select_over(
            projection, table_factor, delete.get("selection"), delete.get("optimizer_hints")
        )
    )
    _stamp_target_scan(plan, relation_name, alias, keyword)
    return _attach_sink(plan, _sink_node(relation_name, (), alias, keyword))


def plan_update(statement, **kwargs):
    """Build the logical plan for UPDATE.

    An updated row is retired and a replacement appended, exactly as MERGE's
    UPDATE arm does. The replacement is the old row with the SET columns
    substituted, so the scan projects every target column — that is inherent to
    rebuilding a whole row from a partial SET list, not a missed pushdown.
    """
    from opteryx.constants.row_identity import ROW_IDENTITY_FILE
    from opteryx.constants.row_identity import ROW_IDENTITY_ORDINAL
    from opteryx.exceptions import ColumnNotFoundError
    from opteryx.planner.logical_planner.logical_planner import plan_query

    update = statement["Update"]
    keyword = "UPDATE"

    if update.get("from"):
        raise UnsupportedSyntaxError(
            "**UPDATE** does not support **FROM**. An update that reads a second "
            "relation is a **MERGE INTO**."
        )
    if update.get("or"):
        raise UnsupportedSyntaxError(
            "**UPDATE** does not support an **OR** conflict clause "
            "(`UPDATE OR REPLACE`, `UPDATE OR IGNORE`)."
        )
    _reject_row_returning(update, keyword)

    table = update["table"]
    if table.get("joins"):
        raise UnsupportedSyntaxError(
            "**UPDATE** updates one relation and cannot join. An update that "
            "reads a second relation is a **MERGE INTO**."
        )
    table_factor = _target_table_factor(table["relation"], keyword)
    relation_name = _relation_name(table_factor)
    alias = _target_alias(table_factor, relation_name)

    connector = _writable_target(relation_name, keyword, kwargs.get("telemetry"))
    target_columns = list(connector.relation_column_names(relation_name))
    if not target_columns:
        raise UnsupportedSyntaxError(f"**UPDATE** target {relation_name} has no columns.")

    # `_update_assignments` is MERGE's SET reader, and the arm and the statement
    # spell SET identically - one reader, so the two cannot drift.
    assignments = _update_assignments({"Update": update}, owner="**UPDATE**")

    # Resolve each assigned name against the target's schema. Column names are
    # not case sensitive, so the SET list's spelling is matched case-insensitively
    # and the SCHEMA's spelling is what the projection is keyed by. An
    # unresolvable name is refused here: silently dropping it would report a
    # successful update that did not make the change it was asked for.
    by_folded = {name.lower(): name for name in target_columns}
    resolved: Dict[str, dict] = {}
    for assigned, value in assignments.items():
        canonical = by_folded.get(assigned.lower())
        if canonical is None:
            raise ColumnNotFoundError(column=assigned, dataset=relation_name)
        if canonical in resolved:
            raise UnsupportedSyntaxError(
                f"**UPDATE**'s **SET** assigns `{canonical}` more than once."
            )
        resolved[canonical] = value

    projection = [
        _aliased(resolved.get(column) or _identifier(column), column)
        for column in target_columns
    ]
    projection.append(_aliased(_int(MERGE_UPDATE), MERGE_ACTION_COLUMN))
    projection.append(_aliased(_identifier(ROW_IDENTITY_FILE), MERGE_FILE_COLUMN))
    projection.append(_aliased(_identifier(ROW_IDENTITY_ORDINAL), MERGE_ORDINAL_COLUMN))

    plan = plan_query(
        _select_over(
            projection, table_factor, update.get("selection"), update.get("optimizer_hints")
        )
    )
    _stamp_target_scan(plan, relation_name, alias, keyword)
    return _attach_sink(plan, _sink_node(relation_name, target_columns, alias, keyword))
