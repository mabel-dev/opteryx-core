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

**Source on the LEFT.** The left row is always present, so `(<on>) IS TRUE`
means matched and the two populations need nothing else to tell them apart.

**`n.$file` is the presence marker.** The target's row address is NULL exactly
when the outer join invented the target row, so `n.$file IS NOT NULL` says "a
real target row is here" — and it says it without knowing one column name of
either side. That is what makes `WHEN NOT MATCHED BY SOURCE` expressible: a
target row with a NULL join key and a source row with a NULL join key are
otherwise indistinguishable and need opposite treatment, and no marker built out
of the data could tell them apart (the dialect refuses `SELECT *, TRUE AS m
FROM …`, and a general USING source has no knowable column list). The address
column is not data — the scan synthesizes it — so it is immune to that. The sink
already discriminates on exactly this (native_merge_sink.hpp).

    WHEN NOT MATCHED BY SOURCE  →  FROM src t FULL OUTER JOIN tgt n ON <on>

**Three populations, three guards**, mutually exclusive by construction:

    matched                  (<on>) IS TRUE
    not matched              NOT ((<on>) IS TRUE) AND n.$file IS NULL
    not matched by source    NOT ((<on>) IS TRUE) AND n.$file IS NOT NULL

The join only widens to FULL OUTER when a NOT MATCHED BY SOURCE arm is present;
without one there are no target-only rows to classify, and both the join and the
CASE chains are exactly what they were before the arm existed.

⚠️ **A NOT MATCHED BY SOURCE arm gives up the cost property below.** It acts on
target rows the source never mentioned, so every target row must be read and
classified — that is inherent to what the arm MEANS, not an implementation
choice. Without such an arm:

Untouched target rows never enter the plan at all: they are not emitted, not
read past the join, and not rewritten. That is what merge-on-read means, and it
is why a feed that republishes mostly-unchanged rows costs almost nothing.

**Arm order is semantics.** Within a population, the first arm whose condition
holds wins, so the CASE chain must preserve declaration order exactly. The
populations may be emitted in any block order because no row satisfies two
guards, and each population's block ends in a catch-all — so by the time the
chain reaches the last block, every row still falling through belongs to it.

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

# Which of the join's three populations a WHEN clause acts on. Not the SQL
# keyword and not an index into anything - a row belongs to exactly one, and the
# guard that selects it is built once per population rather than per arm.
POP_NOT_MATCHED = 0            # a source row no target row matched
POP_NOT_MATCHED_BY_SOURCE = 1  # a target row no source row mentioned
POP_MATCHED = 2                # a source row and the target row it matched


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


def _is_null(expr: dict) -> dict:
    return {"IsNull": expr}


def _is_not_null(expr: dict) -> dict:
    return {"IsNotNull": expr}


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

    Both a named relation and a derived table (a sub-query, a VALUES list) are
    read here; they carry their alias in different places, and that is the only
    difference this function cares about. Nothing else in the desugar asks the
    SOURCE what it is called - `plan_merge` hands the factor straight to the
    join, where ordinary relation planning takes it - so the alias is the whole
    of what a sub-query source needed.

    Both sides must be aliased. Deriving one would mean rewriting every
    reference in the ON condition and the arms to match, and a fully-qualified
    reference to an un-aliased dotted relation (`ws.col.ds.column`) has no
    spelling the binder resolves anyway. For a derived table the rule is not a
    convenience at all: there is no relation name to fall back on. Refusing with
    the fix in the message beats silently binding to the wrong thing.
    """
    table = table_factor.get("Table")
    derived = table_factor.get("Derived")
    if table is not None:
        alias = table.get("alias")
        written = ".".join(p["Identifier"]["value"] for p in table["name"])
    elif derived is not None:
        alias = derived.get("alias")
        written = "(<sub-query>)"
    else:
        raise UnsupportedSyntaxError(
            f"**MERGE INTO** requires a table or a sub-query as its {role}."
        )
    if not alias or not alias.get("name", {}).get("value"):
        example = "n" if role == "target" else "s"
        raise UnsupportedSyntaxError(
            f"**MERGE INTO** requires an alias on its {role}. Write "
            f"`{written} AS {example}` and qualify the {role}'s columns with it."
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



def _resolve_assignments(
    assignments: Dict[str, dict],
    target_columns: List[str],
    relation_name: str,
    owner: str,
) -> Dict[str, dict]:
    """Re-key a SET or INSERT column list by the target's own spelling.

    Two things go wrong without this, and both are silent. The blend chain looks
    each target column up in the assignment dict by its SCHEMA name, so an
    assignment whose key does not match a schema name exactly is never read:
    a column the target does not have, and a column spelled in a different case
    from the schema, are both quietly dropped. The statement then reports success
    for a change it did not make.

    Column names are not case sensitive anywhere else in the engine, so the
    match is folded and the SCHEMA's spelling is what comes back.
    """
    from opteryx.exceptions import ColumnNotFoundError

    by_folded = {name.lower(): name for name in target_columns}
    resolved: Dict[str, dict] = {}
    for assigned, value in assignments.items():
        canonical = by_folded.get(assigned.lower())
        if canonical is None:
            raise ColumnNotFoundError(column=assigned, dataset=relation_name)
        if canonical in resolved:
            raise UnsupportedSyntaxError(
                f"{owner} names `{canonical}` more than once."
            )
        resolved[canonical] = value
    return resolved


class _Arm:
    """One WHEN clause, reduced to what the chains need."""

    __slots__ = ("population", "predicate", "action_code", "assignments")

    def __init__(self, population: int, predicate: Optional[dict], action_code: int, assignments):
        self.population = population
        self.predicate = predicate
        self.action_code = action_code
        self.assignments = assignments  # {column: expr}, empty for DELETE

    @property
    def has_target_row(self) -> bool:
        """Whether a row this arm acts on has a target row behind it.

        Both MATCHED and NOT MATCHED BY SOURCE do: the difference between them is
        whether a SOURCE row is also present, which changes which arms may READ
        the source, not where the blended values come from. So the two behave
        identically everywhere the old row's values are the fallback.
        """
        return self.population != POP_NOT_MATCHED


_CLAUSE_POPULATION = {
    "Matched": POP_MATCHED,
    "NotMatched": POP_NOT_MATCHED,
    "NotMatchedBySource": POP_NOT_MATCHED_BY_SOURCE,
}


def _read_arms(clauses: List[dict]) -> List[_Arm]:
    arms: List[_Arm] = []
    for clause in clauses:
        kind = clause.get("clause_kind")
        population = _CLAUSE_POPULATION.get(kind)
        if population is None:
            raise UnsupportedSyntaxError(f"Unsupported **MERGE** clause: {kind}")
        action = clause["action"]
        if "Insert" in action:
            if population == POP_MATCHED:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN MATCHED** arm cannot **INSERT** — the "
                    "row already exists. Use **UPDATE** or **DELETE**."
                )
            # NOT MATCHED BY SOURCE + INSERT needs no arm here: the grammar
            # refuses it outright ("INSERT is not allowed in a NOT MATCHED BY
            # SOURCE merge clause"), so a second check would be unreachable.
            arms.append(
                _Arm(population, clause.get("predicate"), MERGE_INSERT, _insert_assignments(action))
            )
        elif "Update" in action:
            if population == POP_NOT_MATCHED:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN NOT MATCHED** arm cannot **UPDATE** — "
                    "there is no row to update. Use **INSERT**."
                )
            arms.append(
                _Arm(population, clause.get("predicate"), MERGE_UPDATE, _update_assignments(action))
            )
        elif "Delete" in action:
            if population == POP_NOT_MATCHED:
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN NOT MATCHED** arm cannot **DELETE** — "
                    "there is no row to delete."
                )
            arms.append(_Arm(population, clause.get("predicate"), MERGE_DELETE, {}))
        else:
            raise UnsupportedSyntaxError(f"Unsupported **MERGE** action: {sorted(action)}")
    if not arms:
        raise UnsupportedSyntaxError("**MERGE INTO** needs at least one **WHEN** clause.")
    return arms


# ── Chain building ──────────────────────────────────────────────────────────


def _references_relation(expr, alias: str) -> bool:
    """Whether `expr` qualifies any column with `alias`.

    Walks the raw AST rather than the built expression: this runs before
    anything is bound, and a qualified reference is a `CompoundIdentifier` whose
    first part is the relation. Column and relation names are not case sensitive
    anywhere else in the engine, so the comparison is folded.

    Qualified references ONLY. An unqualified name is resolved by the binder
    against both sides, and reproducing that resolution here — without the
    source's column list, which a sub-query source does not have until it is
    planned — would be guessing. So this proves a reference IS to the named
    relation; it never proves one is not.
    """
    folded = alias.lower()
    stack = [expr]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            parts = node.get("CompoundIdentifier")
            if isinstance(parts, list) and len(parts) > 1:
                head = parts[0].get("value")
                if isinstance(head, str) and head.lower() == folded:
                    return True
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return False


def _reject_source_references(arms: List[_Arm], source_alias: str) -> None:
    """A NOT MATCHED BY SOURCE arm may not read the source.

    Its rows are target rows no source row mentioned, so every source column on
    them is NULL — a predicate reading one is never true and an assignment
    reading one writes NULL over real data. Both are silent; refusing is not.
    """
    for arm in arms:
        if arm.population != POP_NOT_MATCHED_BY_SOURCE:
            continue
        expressions = list(arm.assignments.values())
        if arm.predicate is not None:
            expressions.append(arm.predicate)
        for expression in expressions:
            if _references_relation(expression, source_alias):
                raise UnsupportedSyntaxError(
                    "**MERGE INTO**'s **WHEN NOT MATCHED BY SOURCE** arm cannot read "
                    f"`{source_alias}`. It acts on target rows the source never "
                    "mentioned, so every column of the source is NULL there."
                )


def _chain(arms, groups, per_arm_result):
    """The one CASE chain shape both the action code and every blended column use.

    `groups` is `[(population, guard, default)]` in emission order. Each block
    emits that population's arms in declaration order — each `AND`ed with the
    population's guard — and then the guard alone as a catch-all for a row of
    that population no arm claimed. Because every block ends in a catch-all, a
    row still falling through has been excluded from every population emitted so
    far; the LAST group therefore needs no guard at all and supplies the ELSE.

    Passing `guard=None` marks that last group. It must be last, and there must
    be exactly one — a guarded final group would leave rows with no branch, which
    a CASE answers with NULL rather than an error.
    """
    conditions = []
    for index, (population, guard, default) in enumerate(groups):
        last = index == len(groups) - 1
        if last != (guard is None):  # pragma: no cover - caller-built, fixed shapes
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                "merge chain: exactly the last population block is the unguarded ELSE"
            )
        for arm in arms:
            if arm.population != population:
                continue
            if guard is None:
                if arm.predicate is None:
                    # An unconditional arm in the ELSE block ends the chain:
                    # nothing after it can ever be reached, and emitting the rest
                    # would be dead branches.
                    return _case(conditions, per_arm_result(arm))
                conditions.append((_nested(arm.predicate), per_arm_result(arm)))
                continue
            test = guard if arm.predicate is None else _and(guard, _nested(arm.predicate))
            conditions.append((test, per_arm_result(arm)))
        if guard is None:
            return _case(conditions, default)
        conditions.append((guard, default))
    from opteryx.exceptions import InvalidInternalStateError  # pragma: no cover

    raise InvalidInternalStateError("merge chain: no population blocks")  # pragma: no cover


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

    # The target must name a relation: MERGE addresses target rows by file and
    # ordinal, and a derived table has no rows to address. The SOURCE is under
    # no such rule — it is only read.
    target_factor = _target_table_factor(merge["table"], "MERGE INTO")
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

    # Re-key every arm's assignments by the target's own spelling BEFORE the
    # blend chain is built - it looks columns up by schema name, so an
    # unresolvable or differently-cased key would simply never be read.
    for arm in arms:
        if not arm.assignments:
            continue
        owner = (
            "**MERGE INTO**'s **INSERT** arm"
            if arm.action_code == MERGE_INSERT
            else "**MERGE INTO**'s **UPDATE SET**"
        )
        arm.assignments = _resolve_assignments(
            arm.assignments, target_columns, target_name, owner
        )

    # ── the population guards ────────────────────────────────────────────────
    # Without a NOT MATCHED BY SOURCE arm there are no target-only rows to tell
    # apart, so the guards, the chains and the join are exactly what they were
    # before the arm existed - `$file` is not read and the join stays LEFT OUTER.
    by_source = any(arm.population == POP_NOT_MATCHED_BY_SOURCE for arm in arms)
    unmatched = _not(_is_true(on_expr))
    not_matched_guard = unmatched
    by_source_guard = None
    if by_source:
        _reject_source_references(arms, source_alias)
        # `$file` is NULL exactly when the outer join invented the target row.
        # It is what separates a source row that matched nothing from a target
        # row that matched nothing, and it needs no column name from either side
        # - see the module docstring, and the same test in the sink.
        not_matched_guard = _and(unmatched, _is_null(_ident(target_alias, ROW_IDENTITY_FILE)))
        by_source_guard = _and(unmatched, _is_not_null(_ident(target_alias, ROW_IDENTITY_FILE)))

    def _groups(not_matched_default, by_source_default, matched_default):
        """The population blocks in emission order, MATCHED last.

        MATCHED is last because it is the population every other guard excludes,
        so it can be emitted unguarded as the ELSE. The BY SOURCE block is absent
        entirely when no arm declared one, rather than present and dead.
        """
        blocks = [(POP_NOT_MATCHED, not_matched_guard, not_matched_default)]
        if by_source:
            blocks.append((POP_NOT_MATCHED_BY_SOURCE, by_source_guard, by_source_default))
        blocks.append((POP_MATCHED, None, matched_default))
        return blocks

    # ── the action code ──────────────────────────────────────────────────────
    action_expr = _chain(
        arms,
        _groups(_int(MERGE_NOOP), _int(MERGE_NOOP), _int(MERGE_NOOP)),
        lambda arm: _int(arm.action_code),
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
            # A NOT MATCHED BY SOURCE row has a target row behind it exactly as a
            # MATCHED row does, so the old value is the fallback for both. Only a
            # NOT MATCHED row has none.
            if arm.has_target_row:
                return arm.assignments.get(_column, _old)
            return arm.assignments.get(_column, _null())

        projection.append(
            _aliased(
                _chain(
                    arms,
                    # A row no arm claimed is dropped by the sink, so its values
                    # are never read. For a NOT MATCHED row NULL says that
                    # honestly rather than reaching for a target row that does
                    # not exist; for a NOT MATCHED BY SOURCE row the target row
                    # is right there, so the old value is what is honest.
                    _groups(_null(), old_value, old_value),
                    result_for,
                ),
                column,
            )
        )

    projection.append(_aliased(action_expr, MERGE_ACTION_COLUMN))
    projection.append(_aliased(_ident(target_alias, ROW_IDENTITY_FILE), MERGE_FILE_COLUMN))
    projection.append(_aliased(_ident(target_alias, ROW_IDENTITY_ORDINAL), MERGE_ORDINAL_COLUMN))

    # Held as a local because `create_node_relation` stamps the id of the Scan it
    # builds back onto this dict, and that id is how the target scan is found
    # again below.
    # FULL OUTER only when a NOT MATCHED BY SOURCE arm asked for the target rows
    # the source never mentioned. That widening is what costs the merge-on-read
    # property (see the module docstring); a statement without such an arm does
    # not pay it.
    target_join = {
        "relation": target_factor,
        "global": False,
        "join_operator": (
            {"FullOuter": {"On": on_expr}} if by_source else {"LeftOuter": {"On": on_expr}}
        ),
    }

    select = {
        "Select": {
            "distinct": None,
            "top": None,
            "top_before_distinct": False,
            "projection": projection,
            "exclude": None,
            "into": None,
            "from": [{"relation": source_factor, "joins": [target_join]}],
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
    #
    # Found by the id `create_node_relation` stamped back onto the join entry,
    # not by searching the plan for a scan wearing the target's alias. A source
    # is an arbitrary relation expression, so the plan can hold scans this
    # module never wrote — including one reading the target itself under the
    # target's own alias — and a search would stamp rows that are not the ones
    # being written.
    target_scan = plan[target_join.get("step_id")]
    if (
        target_scan is None or target_scan.node_type != LogicalPlanStepType.Scan
    ):  # pragma: no cover - the join above always plans a target Scan
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            f"plan_merge: no Scan for target {target_name} to address rows through"
        )
    target_scan.emit_row_identity = True
    # What the statement is CALLED, carried so a refusal to address rows names
    # the statement the reader wrote. UPDATE and DELETE desugar through this
    # same sink, so "MERGE" is not a safe assumption there.
    target_scan.row_identity_statement = "MERGE INTO"

    merge_step = LogicalPlanNode(node_type=LogicalPlanStepType.Merge)
    merge_step.relation_name = target_name
    merge_step.target_column_names = tuple(target_columns)
    merge_step.source_tail_id = exit_node_id
    merge_step.target_alias = target_alias
    merge_step.statement_name = "MERGE INTO"
    merge_step.operation = "merge"

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
    candidates[0].row_identity_statement = keyword


def _sink_node(relation_name: str, target_columns, alias: str, keyword: str, operation: str):
    """The Merge sink node. `keyword` is what the statement is CALLED in a
    message; `operation` is what the catalog records it AS in the snapshot log
    and the audit trail. Two fields rather than one derived from the other: the
    catalog's vocabulary is its own, and deriving it by taking the first word of
    a message would make a wording change a silent history change."""
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode
    from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType

    step = LogicalPlanNode(node_type=LogicalPlanStepType.Merge)
    step.relation_name = relation_name
    step.target_column_names = tuple(target_columns)
    step.target_alias = alias
    step.statement_name = keyword
    step.operation = operation
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
    return _attach_sink(plan, _sink_node(relation_name, (), alias, keyword, "delete"))


def plan_update(statement, **kwargs):
    """Build the logical plan for UPDATE.

    An updated row is retired and a replacement appended, exactly as MERGE's
    UPDATE arm does. The replacement is the old row with the SET columns
    substituted, so the scan projects every target column — that is inherent to
    rebuilding a whole row from a partial SET list, not a missed pushdown.
    """
    from opteryx.constants.row_identity import ROW_IDENTITY_FILE
    from opteryx.constants.row_identity import ROW_IDENTITY_ORDINAL
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

    resolved = _resolve_assignments(
        assignments, target_columns, relation_name, "**UPDATE**'s **SET**"
    )

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
    return _attach_sink(
        plan, _sink_node(relation_name, target_columns, alias, keyword, "update")
    )
