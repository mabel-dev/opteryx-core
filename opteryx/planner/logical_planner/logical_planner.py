# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Converts the AST to a logical query plan.

The plan does not try to be efficient or clever, at this point it is only trying to be correct.
"""

import copy
import fnmatch
import time
from enum import Enum, auto
from typing import List, Optional, Tuple

from opteryx.exceptions import (
    InvalidInternalStateError,
    UnnamedColumnError,
    UnsupportedSyntaxError,
    compose,
    md_code,
    md_column,
    md_syntax,
)
from opteryx.expression import NodeType, format_expression, get_all_nodes_of_type
from opteryx.models import LogicalColumn, Node
from opteryx.operators.window.helpers import FRAME_BOUND_KIND
from opteryx.operators.window.helpers import FRAME_UNITS
from opteryx.operators.window.helpers import FRAMED_AGGREGATE_FUNCTIONS
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import logical_planner_builders
from opteryx.planner.logical_planner.logical_planner_rewriter import decompose_aggregates
from opteryx.third_party.travers import Graph
from opteryx.types import logical_type as _plt
from opteryx.types.logical_type import ColumnType, LogicalCategory
from opteryx.types.vectors.vector_types import (
    get_vector_source_identifier,
    node_is_vector_query_expression,
)
from opteryx.utils import dnf, random_string


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  # appending relations (UNION/UNION ALL)
    Intersect = auto()  # set intersection (INTERSECT/INTERSECT ALL)
    Except = auto()  # set difference (EXCEPT/EXCEPT ALL)
    Explain = auto()  # EXPLAIN
    Difference = auto()  # relation interection
    Join = auto()  # all joins
    Unnest = auto()  # UNNEST
    #    Containment = auto() # IN (maybe also EXISTS?)
    AggregateAndGroup = auto()  # group by
    Aggregate = auto()
    Scan = auto()  # read a dataset
    Show = auto()  # show a variable
    ShowColumns = auto()  # SHOW COLUMNS
    ShowManifest = auto()  # SHOW MANIFEST FOR <table>
    ShowSnapshots = auto()  # SHOW SNAPSHOTS FOR <table>
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()
    Exit = auto()
    HeapSort = auto()

    # Runtime cardinality guard on an uncorrelated scalar subquery: >1 row is
    # SQL's cardinality violation, 0 rows yields NULL. Inserted by
    # decorrelate_subquery (post-bind) at the subquery's exit whenever plan
    # structure cannot prove the exactly-one-row property statically.
    ScalarSubqueryGuard = auto()

    Subquery = auto()
    # Leaf reference to a multiply-referenced CTE whose body executes ONCE (the
    # body plan lives in `plan.shared_ctes`, keyed by this node's `cte_key`).
    # Inserted by the Relation Resolver when a CTE is referenced >= 2 times;
    # single-reference CTEs are still spliced inline. Each reference mints its
    # own column identities at bind time (`cte_column_map`: ref identity ->
    # body output identity), which is what keeps a CTE self-join two distinct
    # relations over one materialized result.
    MaterializedCteRef = auto()
    Window = auto()  # OVER (PARTITION BY ...) — rewritten to join by plan rewriter
    FramedWindow = auto()  # SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE BETWEEN ...) — native sink, never rewritten
    FunctionDataset = auto()  # Unnest, GenerateSeries, values + Fake
    DependentJoin = auto()  # Correlated subquery awaiting decorrelation

    CreateView = auto()
    AlterView = auto()
    DropView = auto()
    Analyze = auto()
    Comment = auto()  # COMMENT ON VIEW/TABLE

    CreateRelation = auto()
    DropRelation = auto()
    TruncateRelation = auto()
    AlterRelation = auto()
    RenameRelation = auto()
    CreateTag = auto()
    DropTag = auto()
    AddColumn = auto()
    DropColumn = auto()
    RenameColumn = auto()
    AlterColumnType = auto()
    OptimizeRelation = auto()
    Insert = auto()
    Merge = auto()

    CreateCollection = auto()
    DropCollection = auto()
    AlterWorkspace = auto()
    DropWorkspace = auto()

    DropTrigger = auto()
    AlterMaterializedViewOwner = auto()
    AlterMaterializedViewSuspended = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def copy(self, memo=None) -> "Node":
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        parent_copy = super().copy(memo)
        new_node = LogicalPlanNode(**parent_copy.properties)
        new_node.uuid = parent_copy.uuid
        # super().copy() registered id(self) -> the plain-Node intermediate;
        # replace it with the correctly-typed LogicalPlanNode so any other
        # reference to this same node (via the shared memo) lands here too.
        memo[id(self)] = new_node
        return new_node

    def __str__(self):  # pragma: no cover
        try:
            from opteryx.planner.logical_planner.logical_planner_renderers import _render_registry

            render_fn = _render_registry.get(self.node_type)
            if render_fn:
                return render_fn(self)
        except Exception as err:
            import warnings

            warnings.warn(f"Problem drawing logical plan - {err}")
        return self.node_type.name


def _set_operation_leg_columns(leg_plan: Graph) -> Optional[list]:
    """The columns one leg of a set operation projects — its own EXIT node's columns.

    Every leg plan ends in an EXIT whose `columns` are that leg's declared projection:
    a plain SELECT sets it in inner_query_planner, and a NESTED set operation recurses
    through plan_query, which sets its own. So this is the leg's true output list at
    any nesting depth, syntactically, before any schema is fetched.

    "The first Project node in the leg's graph" is NOT the same thing, and was the bug
    this replaces: `SELECT id FROM (SELECT id, name FROM $planets) AS x UNION ALL
    SELECT id FROM $planets` has two Project nodes in its left leg, and graph order put
    the SUBQUERY's (id, name) first — so the union declared two columns for a leg that
    projects one, and the query died at compile time with "a UNION leg narrower than
    the union schema", blaming the leg for the union's own declaration.

    Returns None when the leg has no single locatable exit, or an exit with no columns.
    """
    exit_points = leg_plan.get_exit_points()
    if len(exit_points) != 1:
        return None
    return leg_plan[exit_points[0]].columns or None


def _set_operation_leg_arity(leg_plan: Graph) -> Optional[int]:
    """How many columns one leg of a set operation projects, or None when only the
    binder can say.

    None means "not knowable here", never "no columns": a wildcard stands for however
    many columns the relation turns out to have, which is the binder's business
    (`_validate_set_operation_types` counts those from the bound schemas). A leg with
    no locatable single exit is unknown for the same reason. Callers must treat None as
    "do not check", never as zero.
    """
    columns = _set_operation_leg_columns(leg_plan)
    if columns is None:
        return None
    if any(column.node_type == NodeType.WILDCARD for column in columns):
        return None
    return len(columns)


def get_subplan_schemas(sub_plan: Graph) -> List[str]:
    """
    Collects all schema aliases used within a given sub-plan.

    This function traverses the sub-plan graph to collect aliases, including those from subqueries.
    Aliases define the schemas used at exit and entry points of the sub-plan.

    Parameters:
        sub_plan: Graph
            The sub-plan object representing a branch of the logical plan.

    Returns:
        List[str]:
            A sorted list of unique schema aliases found within the sub-plan.
    """

    def collect_aliases(node: dict) -> List[str]:
        """
        Recursively traverse the graph to collect schema aliases.

        Parameters:
            node: dict
                The current node in the graph.

        Returns:
            List[str]:
                A list of unique schema aliases collected from the current node and its children.
        """
        current_node = sub_plan[node["name"]]

        # Start with the alias of the current node, if it exists
        aliases = [current_node.alias] if current_node.alias else []

        # If this node is a subquery, stop traversal here
        if current_node.node_type == LogicalPlanStepType.Subquery:
            return aliases

        # A nested set-op (this branch is itself `a UNION b`, e.g. a 3+-leg chained
        # UNION ALL) only keeps its LEFT side's schema entries alive in
        # context.schemas once bound -- visit_union/visit_intersect/visit_except
        # (opteryx/planner/binder/set_ops.py) explicitly pop the right side's
        # entries once folded into the left. Descending into both children here
        # would collect the right side's leaf aliases too, and the outer set-op
        # would later try to resolve those against context.schemas after they're
        # gone (KeyError). current_node.left_relation_names was already computed
        # by this exact function for the inner set-op (set inside plan_query
        # right after inner_query_planner returns it), so reuse it instead of
        # re-deriving from raw graph children -- correct by induction at any
        # nesting depth.
        if current_node.node_type in (
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Intersect,
            LogicalPlanStepType.Except,
        ):
            return aliases + list(current_node.left_relation_names or [])

        # Recursively collect aliases from children
        for child in node.get("children", []):
            aliases.extend(collect_aliases(child))

        return aliases

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    aliases = collect_aliases(root_node)

    # Return sorted list of unique aliases
    return sorted(set(aliases))


def get_subplan_reads(sub_plan: Graph) -> List[str]:
    def collect_reads(node: dict) -> List[str]:
        current_node = sub_plan[node["name"]]

        # If this node is a subquery, stop traversal here
        if current_node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
        ):
            return [current_node.uuid]

        readers = []
        # Recursively collect aliases from children
        for child in node.get("children", []):
            readers.extend(collect_reads(child))

        return readers

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    readers = collect_reads(root_node)

    # Return sorted list of unique aliases
    return sorted(set(readers))


"""
CLAUSE PLANNERS
"""


def _query_body(branch):
    """Find the Query a statement runs, unwrapping the statements that wrap one.

    A statement that wraps a query carries the WITH clause on the INNER query, not on
    itself. Looking for `with` on the outer node found nothing, so the CTEs were silently
    dropped and the query failed with "dataset '<cte name>' could not be found":

        EXPLAIN WITH c AS (...) SELECT * FROM c     -- query is under Explain.statement
        INSERT INTO t WITH c AS (...) SELECT * FROM c   -- query is under Insert.source
    """
    if "Explain" in branch:
        branch = branch["Explain"]["statement"]
    elif "Insert" in branch:
        branch = branch["Insert"]["source"]
    return branch.get("Query", branch)


def _apply_column_aliases(column_aliases: list, columns: list, relation: str) -> None:
    """Apply a relation's column-alias list — the `(a, b)` in `WITH t(a, b) AS (...)`.

    The names rename the relation's OUTPUT columns positionally.

    Applied HERE, at the point the CTE's plan is built, and not later at the splice: the
    Relation Resolver copies a CTE's plan per reference, and `LogicalColumn.copy()` takes
    no memo, so after a copy the head's view of the projection is a set of DISTINCT
    objects from the Project node's. Renaming those renamed nothing the query could see.
    At this point `columns` is still the very list the Project node holds.
    """
    from opteryx.expression import NodeType

    if not column_aliases:
        return

    columns = columns or []

    # `WITH t(a, b) AS (SELECT * FROM ...)` — a wildcard body has no projection list to
    # line the names up against until binding resolves the schema. Refuse, rather than
    # drop the names on the floor, which is what used to happen.
    if not columns or any(column.node_type == NodeType.WILDCARD for column in columns):
        raise UnsupportedSyntaxError(
            f"Relation '{relation}' declares column aliases over a wildcard projection. "
            "Name the columns in the body instead of using `SELECT *`."
        )

    if len(column_aliases) != len(columns):
        raise UnsupportedSyntaxError(
            f"Relation '{relation}' declares {len(column_aliases)} column alias(es) "
            f"but its body produces {len(columns)} column(s)."
        )

    for column, alias in zip(columns, column_aliases):
        column.alias = alias


def extract_ctes(branch):
    ctes = {}
    with_clause = _query_body(branch).get("with")
    if with_clause:
        if with_clause.get("recursive"):
            # A recursive CTE needs a fixpoint operator in the execution engine — the
            # plan graph must stay acyclic, so the loop has to live inside an operator.
            # That does not exist yet. Until it does, say so: previously the self-
            # reference was re-expanded forever and hung the planner.
            raise UnsupportedSyntaxError(
                "**WITH RECURSIVE** is not supported. Rewrite the query without recursion."
            )
        for _ast in with_clause["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            # Plan the whole Query node, not just its `body`. ORDER BY / LIMIT / OFFSET
            # are siblings of `body` in the AST and it is plan_query that hoists them
            # into it. Planning `body` alone silently discarded them, so the LIMIT in
            # `WITH c AS (SELECT ... LIMIT 3)` was dropped and the CTE returned every
            # row — while the identical inline derived table honoured it. This is the
            # same entry point the derived-table path uses (create_node_relation), so
            # the two forms now converge on the same logical plan.
            logical_plan = plan_query(_ast["query"])
            # CTEs don't have an exit node. Its columns ARE the CTE's output
            # projection — the same list object the Project node holds — so read them
            # before it goes. The node left at the head is whatever the body ends with,
            # and for a body with ORDER BY or LIMIT that is an Order/Limit node, which
            # carries no columns of its own.
            plan_head = logical_plan.get_exit_points()[0]
            output_columns = logical_plan[plan_head].columns
            logical_plan.remove_node(plan_head, True)

            # `WITH t(a, b) AS (...)` renames the CTE's output columns. The names were
            # previously parsed and dropped, so the rename silently did nothing and the
            # body's own column names leaked out.
            column_aliases = [
                col["name"]["value"] for col in (_ast.get("alias").get("columns") or [])
            ]
            _apply_column_aliases(column_aliases, output_columns, alias)

            ctes[alias] = logical_plan
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return logical_planner_builders.build(clause[0])
    return [logical_planner_builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["Identifier"]["value"]
    return [token["Identifier"]["value"] for token in clause]


def extract_simple_filter(filters, identifier: str = "Name"):
    if "Like" in filters:
        left = Node(NodeType.IDENTIFIER, value=identifier)
        right = Node(NodeType.LITERAL, type=_plt.VARCHAR, value=filters["Like"])
        root = Node(
            NodeType.COMPARISON_OPERATOR,
            value="ILike",  # we're case insensitive for SHOW filters
            left=left,
            right=right,
        )
        return root
    if "Where" in filters:
        root = logical_planner_builders.build(filters["Where"])
        return root


def _is_vector_order_expression(node: Node) -> bool:
    source_identifier = (
        _get_vector_order_source_identifier(node.parameters[0])
        if (node.node_type == NodeType.FUNCTION and len(node.parameters) == 2)
        else None
    )
    return (
        node.node_type == NodeType.FUNCTION
        and node.value in ("COSINE_SIMILARITY", "COSINE_DISTANCE")
        and len(node.parameters) == 2
        and source_identifier is not None
        and node_is_vector_query_expression(node.parameters[1])
    )


def _get_vector_order_source_identifier(node: Node):
    source_identifier = get_vector_source_identifier(node)
    if source_identifier is not None:
        return source_identifier
    if node.node_type == NodeType.IDENTIFIER:
        return node
    if (
        node.node_type == NodeType.CAST
        and getattr(node, "value", None) in {"VECTOR", "TRY_VECTOR"}
        and getattr(node, "left", None) is not None
        and node.left.node_type == NodeType.IDENTIFIER
    ):
        return node.left
    return None


def _table_name(branch):
    keys = ("Table", "Derived")
    for key in keys:
        if key in branch["relation"]:
            break
    if branch["relation"][key]["alias"]:
        return branch["relation"][key]["alias"]["name"]["value"]
    return ".".join(part["Identifier"]["value"] for part in branch["relation"][key]["name"])


def _strip_outer_nesting(node):
    """Drop redundant `NESTED` wrappers from the OUTERMOST node of a clause expression.

    Parentheses parse to a `NESTED` wrapper, and that wrapper is part of the
    expression's rendering — which IS its identity (the binder resolves an
    expression by looking its rendering up in the schemas). So `(id + 1)` and
    `id + 1` were two different columns, and `SELECT (id + 1) AS u ... GROUP BY
    id + 1` planned an aggregate emitting one identity under a projection asking
    for the other; the compiler then tried to recompute the projection from `id`,
    which the aggregate no longer carries.

    Strip only at the TOP. Inside an expression the wrapper is load-bearing:
    `BINARY_OPERATOR` renders without parentheses of its own, so `(id + 2) * 3`
    and `id + (2 * 3)` are told apart by nothing else, and collapsing them onto
    one identity is a wrong answer, not a naming wrinkle. At the top of a clause
    expression there is no enclosing operator, so the parentheses cannot be
    disambiguating anything and dropping them is provably meaning-preserving.

    This must happen BEFORE binding. Doing it afterwards — as the optimizer does
    for ORDER BY and GROUP BY in constant_folding.py — is too late: both nodes
    have already been minted their own separate identities, which is the divergence
    itself.

    The alias rides down onto the centre, because the binder names a projection
    from the outermost node.

    Applied at every clause top: SELECT, WHERE, GROUP BY, HAVING, QUALIFY,
    ORDER BY, DISTINCT ON, JOIN ON / ASOF, and a window's PARTITION BY and
    ORDER BY. Uniformly, not only where a break was observed — a clause that
    tolerates the wrapper today does so by accident of what its consumers happen
    to look through, and each one that does not was its own distinct failure:
    ORDER BY raised the compiler KeyError, a window's PARTITION BY raised
    `'NoneType' object has no attribute 'lower'`, and `JOIN ... ON (a = b)`
    reported "INNER JOIN has no valid conditions, did you mean CROSS JOIN?".
    """
    if not isinstance(node, Node):
        return node
    while node.node_type == NodeType.NESTED and node.centre is not None:
        centre = node.centre
        if node.alias and not centre.alias:
            centre.alias = node.alias
        node = centre
    return node


def _expand_grouping_elements(elements: list):
    """Lower a GROUP BY list containing grouping constructs into `(keys, grouping_sets)`.

    `keys` is the flat, de-duplicated list of key expressions in first-appearance order —
    exactly the shape a plain GROUP BY produces, so everything downstream of here treats
    it identically. `grouping_sets` is a list of index tuples into `keys`, one per set;
    a key whose index is absent from a set is NULL for that set's rows.

    The GROUP BY list is a sequence of grouping ELEMENTS combined by CROSS PRODUCT — the
    standard's rule, and the reason `GROUP BY a, ROLLUP(b, c)` means
    `(a,b,c), (a,b), (a)` rather than anything simpler. A plain expression is the element
    that denotes exactly one set (itself), which is why a GROUP BY with no construct in
    it falls out of the same code as a single set over all its keys.

    De-duplication is by rendered expression, the same identity the binder resolves on,
    so `ROLLUP(a, a)` collapses to one key column with the sets still distinct by depth.
    """
    key_positions: dict = {}
    keys: list = []

    def _position(expr):
        rendered = format_expression(expr).lower()
        if rendered not in key_positions:
            key_positions[rendered] = len(keys)
            keys.append(expr)
        return key_positions[rendered]

    # Each element contributes its list of alternative sets; the running product is the
    # concatenation of one alternative from each element, in element order.
    combined: list = [[]]
    for element in elements:
        if isinstance(element, logical_planner_builders.GroupingConstruct):
            alternatives = element.grouping_sets()
        else:
            alternatives = [[element]]
        combined = [prefix + alternative for prefix in combined for alternative in alternatives]

    # Positions, not expressions: the aggregate masks key COLUMNS, and two spellings that
    # render the same are one column. Repeats WITHIN a set collapse — grouping by `a`
    # twice is grouping by `a` once — but DUPLICATE SETS are kept. `ROLLUP(a, a)` denotes
    # `(a,a), (a), ()`, whose first two sets group identically yet are still two sets, and
    # the standard says each set contributes its own rows. Collapsing them dropped three
    # rows of a seven-row answer.
    grouping_sets = [
        tuple(dict.fromkeys(_position(expr) for expr in one_set)) for one_set in combined
    ]

    return keys, grouping_sets


def _validate_where_clause_expression(
    node: Node,
    clause_label: str = "WHERE clause",
    example_prefix: str = "WHERE ",
    *,
    under_not: bool = False,
) -> None:
    """Validate that a WHERE clause (or JOIN ON condition) is a boolean VALUE
    EXPRESSION.

    RULING 2026-08-10 (architect). "Is this boolean?" is a TYPE question, and it is
    NOT answered here — `binder/filter.py::visit_filter` answers it against real bound
    types and names the offender ("condition `arr_int[0]` returns INTEGER instead of
    BOOLEAN"). This function answers only the SHAPE question: is this the kind of
    expression that can carry a predicate at all? Splitting it that way is what makes
    the answer independent of which node kind produced the boolean — before the split,
    `WHERE IIF(c, TRUE, FALSE)` was admitted and the identical `WHERE (CASE WHEN c THEN
    TRUE ELSE FALSE END)` was refused, which is the one answer that cannot be explained
    to a user.

    Admitted: every VALUE EXPRESSION — comparisons, IS-forms, AND/OR/XOR/NOT,
    functions, binary/unary operators, CASE, CAST, and `->`/`->>`/`[i]` extraction.
    A CASE is admitted on its own merits: used as a switch it is a multi-branch form
    with no comparison spelling, and the optimizer already treats single-branch CASE
    and IIF as one expression (predicate_rewriter's CASE -> IIF rewrite).

    Refused, each for its OWN reason and with its own message:

    - Bare LITERAL — a bare-literal conjunct (`ON a.x = b.y AND FALSE`) has no column
      to key a join on and no column for a Filter step to reference, so predicate
      pushdown has nowhere to route it and would silently drop it between planning and
      execution. Also very nearly always a typo.
    - Bare IDENTIFIER — `WHERE some_column` is nearly always a typo for a comparison.
      This is a deliberate dialect deviation from SQL's boolean-column rule, and it is
      about TYPOS, not about booleans: it is why admitting CASE alongside it is
      consistent rather than contradictory (a CASE is not a plausible slip).
    - AGGREGATOR / SUBQUERY — not value expressions in this position. An aggregate
      belongs in HAVING; a bare subquery needs an operator to say how to compare it.

    `under_not` only picks the remedy shown for a bare column: under a NOT the clause
    is not bare, so suggesting `WHERE col = value` would misdirect.
    """
    if node is None:
        return

    node_type = node.node_type

    # Allowed: comparison operators and boolean functions
    if node_type == NodeType.COMPARISON_OPERATOR:
        return
    if node_type == NodeType.FUNCTION:
        # Function result validity checked at evaluation time
        return

    # Allowed: IS TRUE/FALSE/NULL
    if node_type == NodeType.UNARY_OPERATOR and node.value in (
        "IsTrue",
        "IsNotTrue",
        "IsFalse",
        "IsNotFalse",
        "IsNull",
        "IsNotNull",
    ):
        return

    # Allowed: logical operators applied to valid expressions
    if node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        _validate_where_clause_expression(node.left, clause_label, example_prefix)
        _validate_where_clause_expression(node.right, clause_label, example_prefix)
        return

    # NOT is allowed if applied to a valid boolean expression
    if node_type == NodeType.NOT:
        _validate_where_clause_expression(
            node.centre, clause_label, example_prefix, under_not=True
        )
        return

    # Allowed: nested expressions
    if node_type == NodeType.NESTED:
        _validate_where_clause_expression(
            node.centre, clause_label, example_prefix, under_not=under_not
        )
        return

    # Binary/unary operators that might return boolean (like LIKE)
    if node_type == NodeType.BINARY_OPERATOR:
        # These are validated at evaluation time
        return
    if node_type == NodeType.UNARY_OPERATOR:
        # Unary operators like NOT on columns (validated at evaluation)
        return

    # Allowed: the remaining VALUE EXPRESSIONS. None of these is a plausible typo and
    # each has a bound type, so the boolean question is the binder's to answer — see
    # the RULING above. CASE reaches execution as a c-native draken_if_then_else chain
    # (compiler._rewrite_case), whose BOOLEAN result is flagged BC_RESULT_WRAP_AS_BOOL
    # and so satisfies the filter's bool-mask contract.
    if node_type in (NodeType.CASE, NodeType.CAST, NodeType.EXTRACTION_OPERATOR):
        return

    # Disallowed: bare literals
    if node_type == NodeType.LITERAL:
        raise UnsupportedSyntaxError(
            compose(
                f"{clause_label} cannot be a bare literal ({md_code(node.value)})",
                f"Compare something instead, for example "
                f"{md_code(f'{example_prefix}column = value')} or "
                f"{md_code(f'{example_prefix}column IS TRUE')}",
            )
        )

    # Disallowed: bare identifiers (column names without comparison)
    if node_type == NodeType.IDENTIFIER:
        # Under a NOT the clause is not bare, so `WHERE col = value` is not the
        # remedy — it would misdirect the reader to a shape they did not write.
        if under_not:
            raise UnsupportedSyntaxError(
                compose(
                    f"{clause_label} cannot filter on a bare column name "
                    f"({md_column(node.value)}) — negating it does not make it a test",
                    f"Test it explicitly, for example "
                    f"{md_code(f'{example_prefix}{node.value} IS FALSE')} or "
                    f"{md_code(f'{example_prefix}{node.value} != value')}",
                )
            )
        raise UnsupportedSyntaxError(
            compose(
                f"{clause_label} cannot be a bare column name ({md_column(node.value)})",
                f"Say what to compare it to, for example "
                f"{md_code(f'{example_prefix}{node.value} = value')} or "
                f"{md_code(f'{example_prefix}{node.value} IS TRUE')}",
            )
        )

    # Disallowed: an aggregate is not a row-level test — it belongs in HAVING.
    if node_type == NodeType.AGGREGATOR:
        raise UnsupportedSyntaxError(
            compose(
                f"{clause_label} cannot contain the aggregate {md_code(node.value)}",
                f"{clause_label} tests one row at a time, so it cannot see a group. "
                f"Filter groups with {md_code('HAVING')} instead",
            )
        )

    # WILDCARD gets no arm here: the parser refuses `*` in a WHERE/ON position
    # before this function is reached (every spelling — bare, qualified, under a
    # NOT, inside an AND — is a QueryParseError), so an arm for it would be dead.

    # Disallowed: a subquery in this position needs an operator to say how to compare.
    if node_type == NodeType.SUBQUERY:
        raise UnsupportedSyntaxError(
            compose(
                f"{clause_label} cannot be a bare subquery",
                f"Say how to compare it, for example "
                f"{md_code(f'{example_prefix}column IN (SELECT ...)')} or "
                f"{md_code(f'{example_prefix}EXISTS (SELECT ...)')}",
            )
        )

    # Any other node type in WHERE/ON is unsupported. Reaching here means a node kind
    # that is not a value expression at all — name the rule, not the node.
    raise UnsupportedSyntaxError(
        compose(
            f"{clause_label} must be an expression that is true or false for each row",
            f"{md_code(format_expression(node))} is not a value that can be tested. Use "
            f"a comparison, an {md_code('IS')} test, or an expression that returns a "
            f"boolean",
        )
    )


def _find_base_scan(plan: LogicalPlan) -> "LogicalPlanNode":
    """Refuse a window with no base table, or with more than one, while the clause that
    wrote it is still in hand.

    Validation only — the returned Scan is discarded. The rewriter copies the whole
    sub-plan below the Window node instead (window_to_join._rewrite_one_window), which is
    the window's real input, WHERE included.

    This runs at LOGICAL PLANNING time, before the Relation Resolver expands CTEs and
    views, so it counts a CTE reference as the one Scan it is at this point. The
    post-resolution rule is `window_to_join._source_relation`, and it is the narrower
    one in one direction and the wider one in the other: a derived table whose body joins
    two tables is already inlined here and so is refused, while the same body written as
    a CTE reaches the rewriter as a single Subquery relation and is allowed.
    """
    scans = [node for _, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Scan]
    if not scans:
        raise UnsupportedSyntaxError(
            "Window functions require a base table — cannot be used without a **FROM** clause. Add a **FROM** clause naming the relation the window should run over."
        )
    if len(scans) > 1:
        raise UnsupportedSyntaxError(
            "Window functions over multiple joined tables are not yet supported. Compute the window in a subquery over a single relation, then join to that result."
        )
    return scans[0]


def _expression_children(tree) -> list:
    """Every child expression of `tree`, in the shape the expression walkers agree on.

    `get_all_nodes_of_type`, `_replace_node` and the walks below all have to reach the
    same set of children, or a node one of them can find is a node another silently
    walks past. Held in one place so they cannot drift.
    """
    children: list = []
    if tree.parameters:
        children.extend(p for p in tree.parameters if isinstance(p, (Node, LogicalColumn)))
    for _side in ("left", "centre", "right"):
        _child = getattr(tree, _side, None)
        if isinstance(_child, (Node, LogicalColumn)):
            children.append(_child)
    if tree.node_type == NodeType.CASE:
        if tree.conditions:
            children.extend(c for c in tree.conditions if isinstance(c, (Node, LogicalColumn)))
        if tree.results:
            children.extend(r for r in tree.results if isinstance(r, (Node, LogicalColumn)))
        if isinstance(tree.else_result, (Node, LogicalColumn)):
            children.append(tree.else_result)
    return children


def _enclosing_aggregator(tree, target, nearest=None):
    """The aggregate or window call `target` is written INSIDE, or None.

    Two shapes are forbidden by the standard and both arrive here, told apart by
    whether the returned node carries an `over`:

    * `SUM(COUNT(*) OVER ())` - an aggregate's argument cannot contain a window;
    * `SUM(COUNT(*) OVER ()) OVER ()` - window calls cannot be nested.

    Both were made reachable by the window hoist: the inner window is lifted out
    and the enclosing call is left over its output column, which is a plan the
    engine can build. They are refused rather than planned.

    Ancestry, not co-occurrence, is what separates these from the window-BESIDE-
    aggregate case (`SELECT COUNT(*), COUNT(*) OVER ()`), which is a different
    arrangement with the opposite remedy - so this walks for a strict ancestor
    rather than asking whether both are present.

    The NEAREST enclosing call is returned, not the outermost: it is the call that
    directly holds the window and therefore the one that cannot hold it. In
    `SUM(SUM(COUNT(*) OVER ()) OVER ()) OVER ()` the middle window is the honest
    complaint, and it renders without having to splice two levels at once.
    Matching is by IDENTITY, for the reason `_replace_node` matches that way.

    Ranking functions need no special handling: they take no arguments, so one can
    never BE an ancestor - which is also why the outer half of an argument nesting
    is always an aggregate window.

    Call this before the hoist mutates anything. The aggregate-window branch clears
    `over` on the nodes it processes, which would make an already-hoisted window
    look like a plain aggregate to a later window in the same expression.
    """
    if tree is None:
        return None
    if tree is target:
        return nearest
    if tree.node_type == NodeType.AGGREGATOR:
        nearest = tree

    for _child in _expression_children(tree):
        found = _enclosing_aggregator(_child, target, nearest)
        if found is not None:
            return found
    return None


def _rendered_window(window) -> str:
    """A window's display form, rendered from its own OVER spec.

    The loop below renders the same thing from the spec nodes it has already built
    (`_window_display_name`); this builds them itself, because the refusals that
    need it fire BEFORE the loop reaches the window - refusing on the pristine tree
    is the whole point (see `_enclosing_aggregator`). Only refusal paths call it, so
    the duplicate build costs nothing on a statement that runs.

    A malformed spec is rendered, not validated: a nested `ROW_NUMBER() OVER ()` is
    named as written and refused for the nesting, which is the structural problem -
    adding the ORDER BY it is missing would not make it legal.

    Windows nested inside this one are rendered too, recursively. `format_expression`
    sees no OVER anywhere - the spec is the parser's dict, not a child node - so
    without this a doubly-nested window rendered as `SUM(COUNT(*)) OVER ()`, which is
    not what the caller wrote and is not even the same statement. Substituting mutates
    the subtree, which is sound only because every caller raises.
    """
    if window.alias:
        return window.alias
    for _nested in get_all_nodes_of_type(window, select_nodes=(NodeType.AGGREGATOR,)):
        if _nested is window or getattr(_nested, "over", None) is None:
            continue
        _nested_display = _rendered_window(_nested)
        _nested_ref = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=_nested_display)
        _nested_ref.query_column = _nested_display
        _replace_node(window, _nested, _nested_ref)
    _partition_by, _order_by = _window_spec_nodes(window.over)
    return _window_display_name(window, _partition_by, _order_by)


def _refuse_nested_window(tree, window) -> None:
    """Refuse a window written inside an aggregate's or another window's argument.

    Both halves are named as the CALLER wrote them. That is the whole reason this
    refuses here instead of letting the window-beside-aggregate guard downstream
    catch it: by then the window has been hoisted, and `format_expression` renders
    the enclosing call around the minted `$win_<random>` join key - a column the
    caller never wrote, different on every execution. `format_expression` has no
    OVER to render either (the spec lives on the parser's dict), so the window is
    swapped for a column carrying its display form and the enclosing call rendered
    around that. The swap mutates in place, which is sound ONLY because this raises
    on the next statement: nothing downstream ever sees the tree.
    """
    _enclosing = _enclosing_aggregator(tree, window)
    if _enclosing is None:
        return

    _window_display = _rendered_window(window)
    _display_ref = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column=_window_display)
    _display_ref.query_column = _window_display
    _replace_node(_enclosing, window, _display_ref)

    if getattr(_enclosing, "over", None) is None:
        raise UnsupportedSyntaxError(
            compose(
                f"Window function {md_code(_window_display)} cannot appear inside the "
                f"aggregate {md_code(format_expression(_enclosing))}",
                "The window produces one value per row and the aggregate collapses "
                "them, so the window cannot be computed over the aggregated result",
                "Compute the window in a subquery and aggregate its result",
            )
        )

    # Window inside a window. Chaining the two across a subquery is the rewrite, and it
    # is advised unconditionally: it used to be offered only when the inner window was a
    # ranking one, because an aggregate window over a subquery computing an aggregate
    # window died with "an aggregate Window node was left below a window chain" — that
    # was a plan-rewrite ordering defect, since fixed (window_to_join
    # `_innermost_chain_first`), and all four combinations run.
    raise UnsupportedSyntaxError(
        compose(
            f"Window function {md_code(_window_display)} cannot appear inside the window "
            f"function {md_code(_rendered_window(_enclosing))}",
            "Window functions cannot be nested — each is computed over the rows of its "
            "own window, so neither can be the input to the other",
            "Compute the inner window in a subquery and apply the outer window to its result",
        )
    )


def _refuse_window_in_window_spec(spec_nodes: list, clause: str) -> None:
    """Refuse a window function written in a PARTITION BY or a window's ORDER BY.

    The other half of "window functions cannot be nested", and it is invisible to
    `_refuse_nested_window`: the spec is not part of the expression tree, it is the
    parser's `over` dict hanging off the node, so a window in it is reached by no
    walk of the projection. It arrived at the binder as a bare function call and was
    reported as a MISSING COLUMN — "Column *COUNT* cannot be found" for
    `OVER (PARTITION BY COUNT(*) OVER ())`, which names something the caller did not
    write and points them at a table that was never the problem.

    Called with the spec nodes already built, which is the first point at which the
    nested window exists as a node; `build` carries its `over` through, so the same
    test identifies it.
    """
    for _spec_node in spec_nodes:
        for _node in get_all_nodes_of_type(_spec_node, select_nodes=(NodeType.AGGREGATOR,)):
            if getattr(_node, "over", None) is None:
                continue
            raise UnsupportedSyntaxError(
                compose(
                    f"Window function {md_code(_rendered_window(_node))} cannot appear in "
                    f"the {md_syntax(clause)} of an {md_syntax('over')} (...) clause",
                    "Window functions cannot be nested — the window spec is computed "
                    "before the window it defines",
                    f"Compute the inner window in a subquery and {clause.lower()} its result",
                )
            )


def _refuse_window_in_having(having) -> None:
    """Refuse a window function written in HAVING.

    Standard SQL does not allow one there, and the reason is the evaluation order it
    fixes: HAVING filters GROUPS, and window functions are computed AFTER grouping and
    its filter. A window in HAVING asks to filter on a value that does not exist yet, so
    there is no semantics to implement — refusing is the fix, not a gap.

    It had no guard because the hoist walks the PROJECTION (and the windows QUALIFY
    borrows into it), and HAVING is neither. Its aggregates are collected straight into
    `_aggregates` further down, and nothing on that path reads `over` — so the spec was
    DISCARDED and the window silently became a plain aggregate:
    `SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 100` computed `COUNT(*)`,
    compared 9 > 100, and returned no rows. No error, and an answer that looks right.
    A GROUP BY beside it did not help — that combination is refused everywhere else, but
    the guard for it never saw HAVING either.

    Called with the HAVING tree freshly built, which is while `over` is still on the node.

    A ranking function is caught whether or not it carries an OVER: it is window-only
    wherever it is written, and a bare `RANK()` in HAVING was reported as
    "the aggregate function ROW_NUMBER is not supported", which names it as the one thing
    it is not. It is rendered as WRITTEN — with its spec if it has one, bare if it does
    not — rather than being given an `OVER ()` the caller did not type.
    """
    for _node in get_all_nodes_of_type(having, select_nodes=(NodeType.AGGREGATOR,)):
        _is_window = getattr(_node, "over", None) is not None
        if not _is_window and _node.value not in _RANKING_FUNCTIONS:
            continue
        _display = _rendered_window(_node) if _is_window else format_expression(_node)
        raise UnsupportedSyntaxError(
            compose(
                f"Window function {md_code(_display)} cannot appear in {md_syntax('having')}",
                f"{md_syntax('having')} filters groups, and window functions are computed "
                "after grouping — so the window's value does not exist yet when the filter "
                "runs",
                f"Filter on a window function's output with {md_syntax('qualify')}",
            )
        )


def _refuse_window_group_key(key, window_outputs: set, position: int = 0) -> None:
    """Refuse a window function used as a GROUP BY key.

    Standard SQL does not allow one, for the same evaluation-order reason HAVING does
    not (`_refuse_window_in_having` above): grouping happens FIRST and window functions
    are computed over the grouped result, so a window's value does not exist yet when
    the grouping runs. There is no semantics to implement — refusing is the fix, not a
    gap. DuckDB refuses it too, as "GROUP BY clause cannot contain window functions".

    The message it replaces named the wrong thing entirely. `GROUP BY <window>` fell
    through to the window-over-grouped-result lowering, which rebased the window's own
    ORDER BY over the grouped rows, found that column was not a group key, and reported
    the STANDARD's other rule:

        SELECT NTILE(4) OVER (ORDER BY gravity) AS decile, COUNT(*)
        FROM $planets GROUP BY decile
        -> Column 'gravity' must appear in the `GROUP BY` clause ...

    `gravity` is not what the caller got wrong, adding it to the GROUP BY does not help,
    and the advice to wrap it in `MIN()` produces a different query. This is the same
    defect ORDER BY had before the hoist covered it (see the note at the ORDER BY hoist,
    which was refused with "Column 'name' must appear in the `GROUP BY` clause" for the
    same reason); GROUP BY was the remaining clause without a guard.

    Two shapes reach here, and both are refused:

    * a window written DIRECTLY in GROUP BY, which is still a raw AGGREGATOR carrying
      its `over` — GROUP BY is not run through `_hoist_windows`, so nothing has
      rewritten it. Without this it reached the compiler as "a GROUP BY key the engine
      could not resolve here", which names nothing.
    * a key that RESOLVED to a window's output, by alias (`GROUP BY decile`) or by
      position (`GROUP BY 1`). The hoist has already replaced the window with an
      identifier reference to its minted output column, so the test is the one
      `_group_by_all_keys` uses for the same question — deliberately the same set and
      the same walk, so the two cannot decide it differently. Walking every identifier
      also catches an EXPRESSION over a window output (`GROUP BY decile + 1`), which is
      no more groupable than the output itself.

    `position` is the 1-based SELECT position when the key came from one, so the message
    can name what the caller actually wrote rather than an alias they never typed.
    """
    def _refuse(display: str, is_reference: bool = False) -> None:
        # A position is named the way the aggregate refusal beside this one names it —
        # `GROUP BY 1` is a spelling the caller can see in their own query, whereas the
        # expression it resolved to may be one they never wrote.
        # Four sentences, written out rather than assembled from fragments. An ALIAS
        # is not itself a window function, it NAMES one, and a position is named the
        # way the aggregate refusal beside this one names it — `GROUP BY 1` is a
        # spelling the caller can see in their own query, whereas the expression it
        # resolved to may be one they never wrote.
        if position and is_reference:
            _what = (
                f"{md_syntax('group by')} position {position} refers to "
                f"{md_code(display)}, a window function's output, in the "
                f"{md_syntax('select')} list"
            )
        elif position:
            _what = (
                f"{md_syntax('group by')} position {position} refers to the window "
                f"function {md_code(display)} in the {md_syntax('select')} list"
            )
        elif is_reference:
            _what = (
                f"{md_code(display)} is a window function's output and cannot be a "
                f"{md_syntax('group by')} key"
            )
        else:
            _what = (
                f"Window function {md_code(display)} cannot be a "
                f"{md_syntax('group by')} key"
            )
        raise UnsupportedSyntaxError(
            compose(
                _what,
                f"{md_syntax('group by')} groups the rows FIRST and a window function is "
                "computed over the grouped result — so the window's value does not exist "
                "yet when the grouping runs",
                f"Compute the window in a subquery and {md_syntax('group by')} its result",
            )
        )

    for _node in get_all_nodes_of_type(key, select_nodes=(NodeType.AGGREGATOR,)):
        _is_window = getattr(_node, "over", None) is not None
        if not _is_window and _node.value not in _RANKING_FUNCTIONS:
            continue
        # Rendered as WRITTEN — with its spec if it has one, bare if it does not —
        # rather than being given an `OVER ()` the caller did not type.
        _refuse(_rendered_window(_node) if _is_window else format_expression(_node))

    if not window_outputs:
        return

    for _identifier in get_all_nodes_of_type(key, select_nodes=(NodeType.IDENTIFIER,)):
        if (_identifier.source_column or "") not in window_outputs:
            continue
        # The name the caller WROTE. `source_column` is the minted `$win_` alias for an
        # unaliased window — random per execution, and a column nobody typed.
        _refuse(_identifier.query_column or _identifier.source_column, is_reference=True)


def _replace_node(tree, target, replacement):
    """Swap `target` for `replacement` inside an expression tree, by IDENTITY.

    Matching on identity rather than on value is deliberate: a query may filter on
    two window functions that render identically, and a value-based match would
    rewrite whichever came first for both. The walk mirrors
    `get_all_nodes_of_type` — parameters, left/centre/right, and CASE's
    conditions/results/else_result — so any shape that walker can reach, this one
    can rewrite.
    """
    if tree is None:
        return None
    if tree is target:
        return replacement

    if tree.parameters:
        tree.parameters = [
            _replace_node(param, target, replacement)
            if isinstance(param, (Node, LogicalColumn))
            else param
            for param in tree.parameters
        ]
    for _side in ("left", "centre", "right"):
        _child = getattr(tree, _side, None)
        if isinstance(_child, (Node, LogicalColumn)):
            setattr(tree, _side, _replace_node(_child, target, replacement))
    if tree.node_type == NodeType.CASE:
        if tree.conditions:
            tree.conditions = [_replace_node(c, target, replacement) for c in tree.conditions]
        if tree.results:
            tree.results = [_replace_node(r, target, replacement) for r in tree.results]
        if isinstance(tree.else_result, (Node, LogicalColumn)):
            tree.else_result = _replace_node(tree.else_result, target, replacement)
    return tree


_FRAME_UNITS_NAMES = {_v: _k for _k, _v in FRAME_UNITS.items()}
_FRAME_BOUND_NAMES = {_v: _k for _k, _v in FRAME_BOUND_KIND.items()}


def _render_frame_bound(kind_code: int, offset: int) -> str:
    _name = _FRAME_BOUND_NAMES[kind_code]
    if _name == "UNBOUNDED_PRECEDING":
        return "UNBOUNDED PRECEDING"
    if _name == "PRECEDING":
        return f"{offset} PRECEDING"
    if _name == "CURRENT_ROW":
        return "CURRENT ROW"
    if _name == "FOLLOWING":
        return f"{offset} FOLLOWING"
    return "UNBOUNDED FOLLOWING"


def _render_frame_clause(frame: tuple) -> str:
    _units, _start_kind, _start_offset, _end_kind, _end_offset = frame
    return (
        f"{_FRAME_UNITS_NAMES[_units]} BETWEEN {_render_frame_bound(_start_kind, _start_offset)} "
        f"AND {_render_frame_bound(_end_kind, _end_offset)}"
    )


def _window_display_name(
    function_node, partition_by: list, window_order_by: list, frame: Optional[tuple] = None
) -> str:
    """The user-facing name of an unaliased window function — what it renders to.

    Every unaliased projection expression is named by its rendering (see the binder's
    `query_column = alias or format_expression(...)`), and a window function is no
    different. It is rendered HERE rather than in `format_expression` because the OVER
    clause only exists as the parser's dict on the node; by the time the plan is built
    the spec has been lifted onto the Window node and `over` is cleared. The spec is
    part of what the column IS — two windows over the same aggregate but different
    partitions (or, for a framed aggregate, different FRAMES) are two different
    columns — so it is rendered too, and is also the dedup key `_hoist_windows` mints
    against: leaving the frame out of it would fold `SUM(x) OVER (... ROWS BETWEEN
    UNBOUNDED PRECEDING AND CURRENT ROW)` and the same aggregate with a different
    frame onto the SAME minted column.

    `partition_by` and `window_order_by` are the already-built nodes, not the parser's
    branches; `window_order_by` is a list of (expression, ascending).
    """
    _parts = []
    if partition_by:
        _parts.append("PARTITION BY " + ", ".join(format_expression(pb) for pb in partition_by))
    if window_order_by:
        _parts.append(
            "ORDER BY "
            + ", ".join(
                format_expression(_col) + ("" if _ascending else " DESC")
                for _col, _ascending in window_order_by
            )
        )
    if frame is not None:
        _parts.append(_render_frame_clause(frame))
    return f"{format_expression(function_node)} OVER ({' '.join(_parts)})"


# The window-function registry (opteryx/operators/window/helpers.py) is the
# source of truth for which functions route down the ranking-window branch.
_RANKING_FUNCTIONS = tuple(WINDOW_FUNCTIONS)


def _window_spec_nodes(over: Optional[dict]) -> Tuple[list, list]:
    """An OVER clause's PARTITION BY and its ORDER BY, built into expression nodes.

    The WINDOW's own ORDER BY is a different thing from the statement-level ORDER BY,
    and the two are built from the same builders — so they are built in one place and
    handed back named apart, rather than by a loop variable that can shadow the other.
    """
    _over = over or {}
    _partition_by = [
        _strip_outer_nesting(logical_planner_builders.build(pb))
        for pb in _over.get("partition_by", [])
    ]
    _window_order_by = [
        (
            _strip_outer_nesting(logical_planner_builders.build(item["expr"])),
            True if item["options"]["asc"] is None else item["options"]["asc"],
        )
        for item in _over.get("order_by", [])
    ]
    return _partition_by, _window_order_by


_FRAME_BOUND_RANK = {
    "UNBOUNDED_PRECEDING": 0,
    "PRECEDING": 1,
    "CURRENT_ROW": 2,
    "FOLLOWING": 3,
    "UNBOUNDED_FOLLOWING": 4,
}


def _frame_offset_literal(expr_dict) -> int:
    """A window FRAME's PRECEDING/FOLLOWING offset — a non-negative integer literal,
    the same requirement LAG/LEAD's row offset already enforces (no column reference:
    the frame shape must be known before any row is read)."""
    node = logical_planner_builders.build(expr_dict)
    offset = node.value if node.node_type == NodeType.LITERAL else None
    if offset is None or isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise UnsupportedSyntaxError(
            "A window **FRAME**'s **PRECEDING**/**FOLLOWING** offset must be a non-negative integer literal."
        )
    return offset


def _parse_frame_bound(bound) -> Tuple[str, int]:
    """One frame bound (the parser's `start_bound`/`end_bound`) to (kind name, offset)."""
    if bound == "CurrentRow":
        return "CURRENT_ROW", 0
    if isinstance(bound, dict):
        if "Preceding" in bound:
            value = bound["Preceding"]
            return ("UNBOUNDED_PRECEDING", 0) if value is None else ("PRECEDING", _frame_offset_literal(value))
        if "Following" in bound:
            value = bound["Following"]
            return ("UNBOUNDED_FOLLOWING", 0) if value is None else ("FOLLOWING", _frame_offset_literal(value))
    raise InvalidInternalStateError(f"unrecognised window frame bound: {bound!r}")


def _build_window_frame(over: dict, has_order_by: bool) -> Optional[tuple]:
    """A window's FrameSpec as (units, start_kind, start_offset, end_kind, end_offset)
    — engine kind codes (FRAME_UNITS / FRAME_BOUND_KIND, native_window_frame.hpp's
    mirror) — or None when the window has no ORDER BY and therefore no per-row
    ordering to frame: it stays on the whole-partition broadcast-join path
    (window_to_join.py), unchanged from before this function existed.

    A frame clause REQUIRES an ORDER BY — the same rule Postgres/DuckDB enforce for
    an explicit ROWS/RANGE clause, and there is no real query this refuses: a frame
    with no ordering has no "current row" to be relative to. An ORDER BY with NO
    explicit frame gets the standard's default frame, RANGE UNBOUNDED PRECEDING AND
    CURRENT ROW — the shape a bare `SUM(x) OVER (ORDER BY d)` means.
    """
    frame = over.get("window_frame")
    if not has_order_by:
        if frame is not None:
            raise UnsupportedSyntaxError(
                "A window **FRAME** (**ROWS**/**RANGE BETWEEN** ...) requires an **ORDER BY** in its **OVER** (...) clause."
            )
        return None
    if frame is None:
        units, start_kind, start_offset, end_kind, end_offset = (
            "RANGE",
            "UNBOUNDED_PRECEDING",
            0,
            "CURRENT_ROW",
            0,
        )
    else:
        units_name = frame.get("units")
        if units_name not in ("Rows", "Range"):
            raise UnsupportedSyntaxError(
                f"Window **FRAME** unit {md_code(str(units_name))} is not supported. Use **ROWS** or **RANGE**."
            )
        units = "ROWS" if units_name == "Rows" else "RANGE"
        start_kind, start_offset = _parse_frame_bound(frame["start_bound"])
        end_bound = frame.get("end_bound")
        end_kind, end_offset = ("CURRENT_ROW", 0) if end_bound is None else _parse_frame_bound(end_bound)
        if units == "RANGE" and (
            start_kind in ("PRECEDING", "FOLLOWING") or end_kind in ("PRECEDING", "FOLLOWING")
        ):
            raise UnsupportedSyntaxError(
                "**RANGE** frames with a numeric **PRECEDING**/**FOLLOWING** offset are not "
                "supported — only **UNBOUNDED PRECEDING**, **CURRENT ROW** and **UNBOUNDED "
                "FOLLOWING**. Use **ROWS** instead."
            )
        start_rank = _FRAME_BOUND_RANK[start_kind]
        end_rank = _FRAME_BOUND_RANK[end_kind]
        inverted = start_rank > end_rank
        if not inverted and start_rank == end_rank == _FRAME_BOUND_RANK["PRECEDING"]:
            inverted = start_offset < end_offset
        if not inverted and start_rank == end_rank == _FRAME_BOUND_RANK["FOLLOWING"]:
            inverted = start_offset > end_offset
        if inverted:
            raise UnsupportedSyntaxError(
                "A window **FRAME**'s start bound cannot come after its end bound."
            )
    return (
        FRAME_UNITS[units],
        FRAME_BOUND_KIND[start_kind],
        start_offset,
        FRAME_BOUND_KIND[end_kind],
        end_offset,
    )


def _hoist_windows(
    item,
    window_specs: list,
    ranking_specs: list,
    minted: dict,
    newly_minted: list,
):
    """Lift every window function out of `item`, leaving a reference to its output.

    A window function does not have to BE the item — it can sit inside a larger
    expression (`COUNT(*) OVER (PARTITION BY gravity) + 0`). Every window in the item's
    tree is hoisted out and replaced by a reference to the Window node's output, leaving
    the residual expression to be computed ABOVE the window; a top-level window is the
    degenerate case of the same rewrite, where the window IS the tree and `_replace_node`
    returns the reference.

    Testing only the item itself was a SILENT WRONG ANSWER: a nested window kept `over`
    set, fell through to the aggregate walk, and was computed as a plain aggregate with
    its OVER spec discarded, so `COUNT(*) OVER (PARTITION BY gravity) + 0` collapsed nine
    rows to one global count.

    A ranking function is a candidate even with NO over clause, so the missing-OVER
    refusal fires wherever it is written; a nested `ROW_NUMBER() + 1` used to die with an
    internal IndexError instead.

    `minted` maps a window's canonical rendering to the (internal alias, display name)
    already minted for it, so the SAME window written twice is computed once and every
    reference reads the one column. The case that needs it is a projection window
    repeated in the statement-level ORDER BY: two spellings of one column, which without
    this became two Window outputs and rode a duplicate through the Project.

    Newly minted internal aliases are appended to `newly_minted`. A caller hoisting from a
    clause whose windows are NOT in the SELECT list (QUALIFY, ORDER BY) uses them to keep
    those columns out of a wildcard's expansion.

    Returns the rewritten item.
    """
    _windows = [
        _node
        for _node in get_all_nodes_of_type(item, select_nodes=(NodeType.AGGREGATOR,))
        if getattr(_node, "over", None) is not None or _node.value in _RANKING_FUNCTIONS
    ]
    if not _windows:
        return item

    # A window written inside another call — an aggregate's argument or another window's
    # — is refused BEFORE anything below mutates. The loop clears `over` on each aggregate
    # window it processes and splices it out of the tree; after either, a second window in
    # the same expression can no longer see what it was written inside, and an
    # already-hoisted window reads as a plain aggregate.
    #
    # A clause that borrows its windows into the projection one at a time (QUALIFY) must
    # ALSO run this over its own predicate, where the enclosing call still is — by the
    # time a borrowed window arrives here it is a bare node with its context left behind.
    for _window in _windows:
        _refuse_nested_window(item, _window)

    # (reference, minted alias) per hoisted window. The references are built carrying the
    # window's DISPLAY form and re-pointed at their minted aliases once the residual
    # expression has been rendered — see the naming note at the end of this function.
    _hoisted: list = []
    for _window in _windows:
        _over = getattr(_window, "over", None)
        _user_alias = _window.alias
        _is_ranking = _window.value in _RANKING_FUNCTIONS
        if _is_ranking:
            if _over is None:
                raise UnsupportedSyntaxError(
                    f"{_window.value}() is a window function and requires an **OVER** (...) clause. Add one, for example `ROW_NUMBER() OVER (PARTITION BY column)`."
                )
            if not _over.get("order_by"):
                raise UnsupportedSyntaxError(
                    f"{_window.value}() requires an **ORDER BY** in its **OVER** (...) clause. Add one, for example `OVER (ORDER BY column)`."
                )
            if _over.get("window_frame") is not None:
                raise UnsupportedSyntaxError(
                    "Window frame specifications (**ROWS**/**RANGE BETWEEN**) are not supported "
                    f"for {_window.value}() — ranking and navigation window functions are always "
                    "computed over the whole ordered partition."
                )
        elif (_over.get("order_by") or _over.get("window_frame") is not None) and (
            _window.value not in FRAMED_AGGREGATE_FUNCTIONS
        ):
            raise UnsupportedSyntaxError(
                f"{_window.value}() cannot be used with a window **ORDER BY** or **FRAME** — only "
                f"{', '.join(sorted(FRAMED_AGGREGATE_FUNCTIONS))} support a running/framed window. "
                "Use **PARTITION BY** only, or compute the running aggregate in a subquery."
            )
        _partition_by, _window_order_by = _window_spec_nodes(_over)
        # A window in the SPEC is invisible to `_refuse_nested_window` — the spec is the
        # parser's dict, not part of the expression tree — so it is tested here, where it
        # has just become nodes.
        _refuse_window_in_window_spec(_partition_by, "PARTITION BY")
        _refuse_window_in_window_spec([_col for _col, _asc in _window_order_by], "ORDER BY")

        # A framed aggregate window's FrameSpec — see `_build_window_frame`. None
        # (including for every ranking/navigation window) means "no ORDER BY, no
        # frame": the whole-partition broadcast-join path, unchanged.
        _frame = None if _is_ranking else _build_window_frame(_over, bool(_window_order_by))

        # Rendered before anything is mutated — the aggregate path clears `over`, and the
        # spec is part of what the column IS. Deliberately alias-independent, because this
        # is also the dedup key: `w OVER (...)` written in ORDER BY is the same column as
        # `w OVER (...) AS x` written in the SELECT list.
        _canonical = _window_display_name(_window, _partition_by, _window_order_by, _frame)
        _already = minted.get(_canonical)
        if _already is None:
            # Two different names for two different jobs: `_win_alias` is the INTERNAL
            # identity — it names the Window node's output column and the references to it
            # — while `_win_display` is what the caller sees. Collapsing them put the
            # minted `$win_...` in the result (and it is random per execution, so callers
            # could not even rely on it).
            _win_alias = _user_alias or f"$win_{random_string(6)}"
            _win_display = _user_alias or _canonical
            if _is_ranking:
                # `parameters` is empty for the ranking three; LAG/LEAD carry
                # (argument expression[, offset literal]) — arity and offset were
                # validated by the builder.
                ranking_specs.append(
                    (_window.value, _partition_by, _window_order_by, _win_alias,
                     list(_window.parameters or []))
                )
            else:
                # `_win_alias` names the aggregate inside the CTE the window rewrite builds
                # (unframed) or the FramedWindow node's output (framed), and the reference
                # to it, so it must be minted and must stay on the window node; only the
                # OUTER reference carries the display name.
                _window.alias = _win_alias
                _window.query_column = _win_alias
                _window.over = None  # clear so it acts as a plain aggregate inside the CTE
                window_specs.append((_window, _partition_by, _window_order_by, _frame))
            minted[_canonical] = (_win_alias, _win_display)
            newly_minted.append(_win_alias)
        else:
            _win_alias, _win_display = _already

        _ref = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=_win_display,
            alias=_user_alias,
        )
        _ref.query_column = _win_display
        item = _replace_node(item, _window, _ref)
        _hoisted.append((_ref, _win_alias))

    # Naming: an unaliased item is named by its rendering, and the binder honours a
    # `query_column` set here (see `binder.py`). A reference left in place of a hoisted
    # window renders as its MINTED alias, which is random per execution — so the residual
    # expression is rendered while the references still carry the window's display form,
    # and that rendering is pinned as the item's name BEFORE they are re-pointed at the
    # aliases they must resolve against. Order matters: render, then re-point. A top-level
    # window skips the render — the reference already carries the display name as its
    # query_column.
    if item.query_column is None:
        item.query_column = format_expression(item)
    for _ref, _minted_alias in _hoisted:
        _ref.source_column = _minted_alias
    return item


# The relation the aggregate step is presented as when a window is computed OVER the
# grouped result. Minted, never typed.
GROUPED_AGGREGATE_ALIAS_PREFIX = "$grouped-"


def _outermost_aggregates(tree) -> list:
    """Every aggregate call in `tree` that is NOT written inside another aggregate.

    `get_all_nodes_of_type` returns the nested ones too, and for the aggregate step
    below the grouped result they are the wrong answer: the inner call of
    `SUM(SUM(x))` is the group-level aggregate and the outer one is the window over
    those group results, so registering both would compute `SUM(x)` twice and give
    the second copy no meaning.
    """
    if tree is None or not isinstance(tree, (Node, LogicalColumn)):
        return []
    if tree.node_type == NodeType.AGGREGATOR:
        return [tree]
    found: list = []
    for _child in _expression_children(tree):
        found.extend(_outermost_aggregates(_child))
    return found


def _grouped_output_name(node) -> str:
    """The name the grouped result exposes `node` under.

    The grouped rows become a RELATION (a Subquery node, see the lowering in
    `inner_query_planner`), so every group key and every aggregate needs a name the
    expressions above the grouping can address it by.

    That name is the SCHEMA column's, which is not the caller's alias: `visit_project`
    records an `AS` as an extra alias on the schema column rather than renaming it, and
    the boundary (`visit_subquery`) publishes `schema_column.name` and drops the
    aliases. So `SUM(x) AS revenue` is addressed across the boundary as `SUM(x)`, and
    the alias is re-applied ABOVE it — the reference the caller's expression is rewritten
    to carries `AS revenue`, so the output column is still named `revenue`.
    """
    if node.node_type == NodeType.IDENTIFIER:
        return node.source_column
    return format_expression(node)


def _grouped_reference_keys(node) -> tuple:
    """The spellings an expression above the grouping may use to name `node`.

    An identifier is indexed by its bare column name as well as by its rendering,
    because the two are not the same string once an alias is involved: `i_class AS c`
    RENDERS as `c` (an identifier renders as its `current_name`) while the GROUP BY
    that named it wrote `i_class`. Indexing only one of them left the other spelling
    unmatched and the column unresolvable above the boundary.
    """
    _keys = [format_expression(node).lower()]
    if node.node_type == NodeType.IDENTIFIER and node.source_column:
        _keys.append(node.source_column.lower())
    return tuple(_keys)


def _refuse_ungrouped_column(node) -> None:
    """The standard's rule, for a column read above a grouping that did not group by it.

    Reached from the window-over-grouped-result lowering, where the offending column is
    most often written inside the WINDOW rather than in the SELECT list —
    `SUM(mass) OVER ()` beside `SUM(mass)` reads a raw `mass` at a level where only the
    group keys and the aggregates exist. DuckDB and PostgreSQL both refuse it in the same
    terms, and this is the wording Opteryx already uses for the un-windowed spelling.

    Named by `query_column`, the display form every identifier carries from the builders:
    `source_column` has already lost any qualifier the caller wrote.
    """
    from opteryx.exceptions import SqlError

    _column = node.query_column or node.source_column
    raise SqlError(
        f"Column '{_column}' must appear in the `GROUP BY` clause or must be part of an "
        f"aggregate function. Either add it to the `GROUP BY` list, or add an "
        f"aggregation such as `MIN({_column})`."
    )


def _rebase_over_aggregate(tree, names: dict, skipped: set, passthrough: set, memo: dict):
    """Re-point an expression written over the PRE-aggregation rows at the grouped rows.

    Everything above the grouping — the SELECT list, HAVING, QUALIFY, ORDER BY, and a
    window's own PARTITION BY / ORDER BY / arguments — is written in terms of the
    source columns, but it runs over the grouped result, which is a relation exposing
    one column per group key and one per aggregate. Each group key and each aggregate
    is swapped for a reference to that column; everything else is left alone and
    rebuilt above it, so `SUM(x) * 100 / SUM(SUM(x)) OVER (...)` keeps its arithmetic
    and only its operands move.

    References are UNQUALIFIED on purpose. The window rewrite copies the window's
    source sub-plan and renames every relation in it (`window_to_join.rename_relations`)
    but does NOT remap the window aggregate's own operands — a qualified reference
    inside one therefore names a relation that no longer exists in the copy. An
    unqualified one resolves by name in both scopes.

    `skipped` holds the ids of the hoisted window nodes themselves: QUALIFY still
    carries the original node in its predicate (it is re-pointed at the window's output
    later, by identity), and it is the window, not a group-level aggregate.

    `passthrough` holds the lower-cased names that resolve ABOVE the grouping rather
    than against it — a window's output column, and a SELECT alias an ORDER BY names.
    Every OTHER unmatched identifier is a column read at a level where it does not
    exist, which is the standard's GROUP BY rule and is refused as such.

    `memo` maps `id(node)` to what that node was rewritten to, so a node reachable from
    two places — a HAVING aggregate is both inside the condition and in the Project's
    pass-through list — is rewritten to the SAME reference object in both, as it was
    the same object before.
    """
    if tree is None or not isinstance(tree, (Node, LogicalColumn)):
        return tree
    if id(tree) in skipped:
        return tree
    _cached = memo.get(id(tree))
    if _cached is not None:
        return _cached

    _name = None
    if tree.node_type == NodeType.IDENTIFIER and tree.source_column:
        _name = names.get(tree.source_column.lower())
    if _name is None:
        _name = names.get(format_expression(tree).lower())

    if _name is not None:
        _reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=_name,
            alias=tree.alias,
            span=tree.span,
        )
        # The name the CALLER sees is the expression they wrote, not the grouped
        # relation's internal column name — `SELECT SUM(x)` answers `SUM(x)` whether or
        # not a window made the grouping a relation.
        _reference.query_column = tree.query_column or format_expression(tree)
        memo[id(tree)] = _reference
        return _reference

    if tree.node_type == NodeType.AGGREGATOR:
        # Every aggregate above the grouping is registered as one of its outputs before
        # this runs. One that is not means the collection missed a clause, which is a
        # planning fault and not something the caller can act on.
        raise InvalidInternalStateError(
            f"aggregate `{format_expression(tree)}` is not an output of the grouping it "
            "is computed by"
        )

    if tree.node_type == NodeType.IDENTIFIER:
        # Not a group key. A window's output and a SELECT alias both resolve above the
        # grouping and are left where they are; anything else is a column read at a
        # level where only the group keys and the aggregates exist.
        if (tree.source_column or "").lower() in passthrough:
            return tree
        _refuse_ungrouped_column(tree)

    if tree.parameters:
        tree.parameters = [
            _rebase_over_aggregate(_parameter, names, skipped, passthrough, memo)
            if isinstance(_parameter, (Node, LogicalColumn))
            else _parameter
            for _parameter in tree.parameters
        ]
    for _side in ("left", "centre", "right"):
        _child = getattr(tree, _side, None)
        if isinstance(_child, (Node, LogicalColumn)):
            setattr(tree, _side, _rebase_over_aggregate(_child, names, skipped, passthrough, memo))
    if tree.node_type == NodeType.CASE:
        if tree.conditions:
            tree.conditions = [
                _rebase_over_aggregate(_condition, names, skipped, passthrough, memo)
                for _condition in tree.conditions
            ]
        if tree.results:
            tree.results = [
                _rebase_over_aggregate(_result, names, skipped, passthrough, memo)
                for _result in tree.results
            ]
        if isinstance(tree.else_result, (Node, LogicalColumn)):
            tree.else_result = _rebase_over_aggregate(
                tree.else_result, names, skipped, passthrough, memo
            )
    memo[id(tree)] = tree
    return tree


def _group_by_all_keys(projection: list, window_outputs: set) -> list:
    """The keys `GROUP BY ALL` stands for — every projection expression that is neither
    an aggregate nor a window's output.

    A window is computed AFTER the grouping, so its value cannot be a grouping key. The
    hoist has already replaced each window with an identifier reference to its output
    column, and an identifier is exactly what this collects — so without the second test
    `SELECT k, COUNT(*) OVER () FROM t GROUP BY ALL` grouped by a column that does not
    exist until after the grouping.
    """
    _keys = []
    for _column in projection:
        if get_all_nodes_of_type(_column, select_nodes=(NodeType.AGGREGATOR,)):
            continue
        if window_outputs and any(
            (_identifier.source_column or "") in window_outputs
            for _identifier in get_all_nodes_of_type(_column, select_nodes=(NodeType.IDENTIFIER,))
        ):
            continue
        _keys.append(_column)
    return _keys


def inner_query_planner(ast_branch: dict) -> LogicalPlan:
    if "Query" in ast_branch:
        # Sometimes we get a full query plan here (e.g. when queries in set
        # functions are in parenthesis)
        return plan_query(ast_branch)

    # Handle nested SetOperations (chained UNION/INTERSECT/EXCEPT)
    if "SetOperation" in ast_branch:
        # Recursively call plan_query to handle the nested set operation
        return plan_query({"Query": {"body": ast_branch}})

    inner_plan = LogicalPlan()
    step_id = None

    # TOP used?
    if ast_branch["Select"].get("top") is not None:
        raise UnsupportedSyntaxError(
            "**SELECT** TOP to limit number of returned records not supported, use **LIMIT** instead."
        )

    # from
    _relations = ast_branch["Select"].get("from", [])

    # Process first relation if any
    if len(_relations) > 0:
        step_id, sub_plan = create_node_relation(_relations[0])
        inner_plan += sub_plan

        # If there are multiple relations, build sequential binary implicit cross joins
        # This converts FROM A, B, C into A CROSS JOIN B CROSS JOIN C
        if len(_relations) > 1:
            for i in range(1, len(_relations)):
                # Process the next relation
                right_step_id, right_sub_plan = create_node_relation(_relations[i])

                # Get relation names BEFORE adding right_sub_plan to inner_plan
                left_relation_names = get_subplan_schemas(inner_plan)
                left_readers = get_subplan_reads(inner_plan)
                right_relation_names = get_subplan_schemas(right_sub_plan)
                right_readers = get_subplan_reads(right_sub_plan)

                # Create binary cross join node
                join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
                join_step.type = "cross join"
                join_step.implied_join = True
                join_step.left_relation_names = left_relation_names
                join_step.left_readers = left_readers
                join_step.right_relation_names = right_relation_names
                join_step.right_readers = right_readers

                # For compatibility with binder's fallback extraction (if left_relation_names not set)
                # Don't set readers to avoid triggering the >2 relations check in the binder
                join_step.relation_names = [left_relation_names, right_relation_names]

                # Add the right sub_plan to inner_plan
                inner_plan += right_sub_plan

                # Add join node and wire it
                join_step_id = random_string()
                inner_plan.add_node(join_step_id, join_step)
                inner_plan.add_edge(step_id, join_step_id, "left")
                inner_plan.add_edge(right_step_id, join_step_id, "right")

                # Update step_id for next iteration
                step_id = join_step_id

    # If there's no relations, use $no_table
    if len(_relations) == 0:
        step_id, sub_plan = create_node_relation(
            {
                "relation": {
                    "Table": {
                        "name": [{"Identifier": {"value": "$no_table"}}],
                        "args": None,
                        "alias": None,
                        "with_hints": [],
                    }
                }
            }
        )
        inner_plan += sub_plan

    # selection
    _selection = _strip_outer_nesting(
        logical_planner_builders.build(ast_branch["Select"].get("selection"))
    )
    if _selection:
        if len(_relations) == 0:
            raise UnsupportedSyntaxError("Statement has a **WHERE** clause but no **FROM** clause.")
        _validate_where_clause_expression(_selection)
        selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        selection_step.condition = _selection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, selection_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # groups
    _projection = [
        _strip_outer_nesting(p)
        for p in (logical_planner_builders.build(ast_branch["Select"].get("projection")) or [])
    ]
    if len(_projection) > 1 and any(
        p.node_type == NodeType.WILDCARD for p in _projection if p.value is None
    ):
        from opteryx.exceptions import SqlError

        raise SqlError("`SELECT *` cannot coexist with additional columns. List the columns you want explicitly, or use `SELECT *` on its own.")

    if len(_projection) > 1 and any(p.node_type == NodeType.WILDCARD for p in _projection[1:]):
        from opteryx.exceptions import SqlError

        raise SqlError(
            "Qualified wild cards (`table.*`) must be the first column when used with additional columns. Move it to the start of the projection."
        )

    # A SELECT-list EXISTS / IN is NOT refused here. Which shapes are supported
    # cannot be decided before binding — the answer turns on whether the subquery
    # correlates, and on what it correlates BY, neither of which is knowable until
    # every name has been resolved. DecorrelateSubqueryStrategy decides it on the
    # bound plan (an existence join for the shapes it can lower, one explicit
    # refusal for the rest), exactly as it already did for a SELECT-list scalar
    # subquery.

    # Detect window functions (AGGREGATOR nodes with an OVER clause) before aggregate extraction.
    # Replace each window function in _projection with a plain column reference to its output alias,
    # so the regular aggregate path does not see them. Window logical nodes are inserted here so
    # they sit between the scan/filter chain and the project, ready for the plan rewriter.
    # aggregate windows: (agg_node, partition_by_nodes, window_order_by_pairs|None, frame|None).
    # order_by/frame are None for a whole-partition window (window_to_join broadcasts it);
    # set for a framed aggregate (FramedWindow node, native_window_frame.hpp).
    _window_specs: list = []
    # ranking windows: (kind, partition_by_nodes, order_by_pairs, win_alias)
    _ranking_specs: list = []
    # QUALIFY is filtering on a window function's OUTPUT, so its window functions
    # have to be computed before the filter can run. They ride into `_projection`
    # here so the detection loop below treats them exactly like a window function
    # the user selected — same alias minting, same Window node, same grouping by
    # partition spec. The loop replaces each with an identifier reference, and
    # `_qualify` is then re-pointed at those references so the Filter reads the
    # computed column rather than re-evaluating the window.
    #
    # This mirrors HAVING (see `_having_passthrough` below): a clause may name
    # something the SELECT list does not, the extra columns ride through the
    # Project as pass-throughs, and the Exit node prunes them back to the SELECT
    # list so they never reach the caller.
    #
    # Until this existed, `ast_branch["Select"]["qualify"]` was read by nothing:
    # the clause parsed, bound, and then vanished, so QUALIFY silently returned
    # the unfiltered relation.
    _qualify = _strip_outer_nesting(
        logical_planner_builders.build(ast_branch["Select"].get("qualify"))
    )
    _qualify_window_slots: list = []  # (index into _projection, original node)
    # The minted names of window columns a clause OTHER than the SELECT list needed —
    # QUALIFY's and ORDER BY's. Removing them from `_projection` is enough for a
    # projection that NAMES its columns, but a wildcard is expanded from the schemas in
    # scope at bind time — and the Window node's output relation is one of them. These
    # names travel to the Project and Exit nodes so their wildcard expansion can skip a
    # column no reader asked for and could not name if they wanted to (it is random per
    # execution). Only what each clause NEWLY minted goes in: a window that dedups onto
    # one the caller also SELECTED must not hide the selected column.
    _hidden_window_columns: list = []
    if _qualify is not None:
        _qualify_windows = [
            _node
            for _node in get_all_nodes_of_type(_qualify, select_nodes=(NodeType.AGGREGATOR,))
            if getattr(_node, "over", None) is not None
        ]
        if not _qualify_windows:
            raise UnsupportedSyntaxError(
                "**QUALIFY** filters on a window function, but this one contains none. "
                "Use **WHERE** to filter on plain columns, or **HAVING** to filter a grouped "
                "result."
            )
        _projection_length_without_qualify = len(_projection)
        for _window_function in _qualify_windows:
            # Ancestry has to be read from the PREDICATE, not from the projection slot
            # below: only the window itself is borrowed into `_projection`, so a
            # `QUALIFY SUM(COUNT(*) OVER ()) > 1` arrives at the loop as a bare window
            # with the aggregate left behind here. Nothing saw that aggregate — it was
            # never collected into `_aggregates` either — and the statement planned,
            # then died in the engine with a raw KeyError naming a `$derived_` column.
            _refuse_nested_window(_qualify, _window_function)
            _qualify_window_slots.append((len(_projection), _window_function))
            _projection.append(_window_function)
    _qualify_slot_indices = {_slot for _slot, _ in _qualify_window_slots}
    # One shared `minted` map for the whole statement: the SAME window written twice — in
    # the SELECT list and again in QUALIFY or ORDER BY — is ONE column, computed once,
    # with every reference reading it. Without it the second spelling minted a second
    # Window output and rode a duplicate through the Project.
    _minted: dict = {}
    for _i, proj_col in enumerate(_projection):
        # Only the aliases THIS clause newly minted are hidden. A QUALIFY window that
        # dedups onto a window the caller also SELECTED must not hide the selected one.
        _newly_minted: list = []
        _projection[_i] = _hoist_windows(
            proj_col,
            _window_specs,
            _ranking_specs,
            _minted,
            _newly_minted,
        )
        if _i in _qualify_slot_indices:
            _hidden_window_columns.extend(_newly_minted)
    # Collect aggregates in projection (SELECT) order. get_all_nodes_of_type uses a
    # LIFO stack, so passing the whole projection list scrambles cross-column order;
    # for the ungrouped Aggregate operator that order leaks straight to the output
    # columns. Walking each projection column in turn preserves left-to-right order
    # (the binder dedups by identity afterwards).
    _aggregates = []
    for _proj_col in _projection:
        _aggregates.extend(get_all_nodes_of_type(_proj_col, select_nodes=(NodeType.AGGREGATOR,)))
    _aggregates, _projection = decompose_aggregates(_aggregates, _projection)

    # ORDER BY is BUILT here, ahead of the aggregate step below, for the same reason
    # HAVING is (see the note on the next block): `GROUP BY k ORDER BY COUNT(*) DESC`
    # sorts on an aggregate that is never selected, so that aggregate has to reach
    # `_aggregates` before the aggregate step is constructed or nothing computes it.
    # Only the BUILD moves up — the "which ORDER BY columns aren't in the projection"
    # reconciliation still runs further down, next to the Project it feeds, and it
    # already hoists an unselected aggregate as a pass-through column once the value
    # actually exists.
    _order_by = ast_branch.get("order_by")
    _order_by_columns_not_in_projection: list = []
    _order_by_columns: list = []
    if _order_by and _order_by.get("kind") and _order_by["kind"].get("Expressions"):
        _order_by = [
            (
                _strip_outer_nesting(logical_planner_builders.build(item["expr"])),
                True if item["options"]["asc"] is None else item["options"]["asc"],
            )
            for item in _order_by["kind"]["Expressions"]
        ]
        # Resolve positional ORDER BY (SQL-92): an integer literal refers to the
        # 1-based position in the SELECT list. Replace it with the projection
        # expression so downstream stages see a normal column reference.
        # Any other literal (string, float, NULL, ...) is rejected.
        #
        # A bare `SELECT *` is the one case this cannot do here: `_projection` is a
        # single WILDCARD placeholder, not the columns it will actually expand to —
        # that count is only known once the source schema is bound. Validating the
        # position against `len(_projection)` in that case rejects every position but
        # 1, which is exactly the TPC-DS shape `SELECT * FROM v ... ORDER BY expr, 1,
        # 2, ..., 9`. The literal is left in place (flagged) and resolved positionally
        # against the bound schema instead, in `binder/order.py`.
        _is_bare_wildcard_projection = (
            len(_projection) == 1
            and _projection[0].node_type == NodeType.WILDCARD
            and _projection[0].except_columns is None
            and _projection[0].value is None
        )
        rewritten = []
        for expr, ascending in _order_by:
            if expr.node_type == NodeType.LITERAL:
                _expr_cat = expr.type.category if isinstance(expr.type, ColumnType) else expr.type
                if _expr_cat != LogicalCategory.INTEGER:
                    raise UnsupportedSyntaxError("Cannot **ORDER BY** constant values. Order by a column, or by an expression over one.")
                position = int(expr.value)
                if position < 1:
                    raise UnsupportedSyntaxError(
                        f"**ORDER BY** position {position} is out of range — **SELECT** has {len(_projection)} column(s). Positions count the **SELECT** columns and start at 1."
                    )
                if _is_bare_wildcard_projection:
                    expr.is_wildcard_order_position = True
                elif position > len(_projection):
                    raise UnsupportedSyntaxError(
                        f"**ORDER BY** position {position} is out of range — **SELECT** has {len(_projection)} column(s). Positions count the **SELECT** columns and start at 1."
                    )
                else:
                    expr = _projection[position - 1]
            rewritten.append((expr, ascending))
        _order_by = rewritten

        # A window in ORDER BY is LEGAL SQL — windows are computed before the sort, so
        # ordering on one is well defined — and it is hoisted here exactly as QUALIFY's
        # are: the Window node computes the column below the Order step, and the ORDER BY
        # is re-pointed at it.
        #
        # Until this ran, an ORDER BY window fell through to the aggregate walk below and
        # was collected as a PLAIN aggregate with its OVER discarded. That made the
        # statement look like an aggregate query, so
        # `SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)` was refused
        # with "Column 'name' must appear in the `GROUP BY` clause" — naming a column the
        # caller had no way to act on, for a rule that was never the problem.
        #
        # The shared `_minted` map is what makes the same window written in BOTH the
        # SELECT list and ORDER BY one column rather than two, and it is keyed on the
        # canonical rendering rather than the alias, so `w OVER (...) AS x` in the SELECT
        # list and a verbatim `w OVER (...)` in ORDER BY meet. That case used to produce
        # the most misleading message of the lot: the beside-aggregate refusal, naming the
        # ALIAS as the window and the window as the aggregate.
        #
        # Newly minted columns ride to the Project as pass-throughs — the reconciliation
        # further down finds them exactly as it finds any other ORDER BY column the
        # projection does not name — and are hidden at the Exit so they never reach the
        # caller. A window that dedups onto a SELECTED one mints nothing, so it is neither
        # passed through nor hidden; it is already in the projection.
        _hoisted_order_by = []
        for _expr, _ascending in _order_by:
            _newly_minted: list = []
            _expr = _hoist_windows(
                _expr,
                _window_specs,
                _ranking_specs,
                _minted,
                _newly_minted,
            )
            _hidden_window_columns.extend(_newly_minted)
            _hoisted_order_by.append((_expr, _ascending))
        _order_by = _hoisted_order_by
        _order_by_columns = [exp[0] for exp in _order_by]

    # An aggregate in ORDER BY that the SELECT list does not name is still an
    # aggregate the aggregate step must produce (`GROUP BY k ORDER BY MIN(x)`).
    # The pass-through hoist further down already carries the VALUE up to the Order
    # node; without this it would carry a value nothing ever computed, and the
    # binder would fail on the pruned operand column instead.
    for _order_column in _order_by_columns:
        for _aggregate in get_all_nodes_of_type(
            _order_column, select_nodes=(NodeType.AGGREGATOR,)
        ):
            _aggregates.append(_aggregate)

    # HAVING is BUILT here, not at its position in the plan further down, because a
    # HAVING clause may reference aggregates and group keys that never appear in the
    # SELECT list — `GROUP BY k HAVING SUM(x) > 1` with neither SUM(x) nor k selected
    # is valid SQL-92 and is exactly what canonical TPC-H Q18 does. Two things follow:
    #
    #   1. those aggregates must join `_aggregates` so the aggregate step below
    #      actually COMPUTES them (otherwise nothing produces the value, and the
    #      binder prunes the operand column out of the schema entirely), and
    #   2. the expressions must ride through the Project as pass-through columns,
    #      because the HAVING Filter sits ABOVE the Project. The Exit node prunes back
    #      to the SELECT list, so they never reach the output row.
    #
    # Collected AFTER decompose_aggregates: decomposition rewrites the projection list
    # only, so a decomposed HAVING aggregate would leave the condition tree referencing
    # an expression that nothing computes. Undecomposed aggregates over expressions are
    # supported by the aggregate operator directly.
    _having = _strip_outer_nesting(
        logical_planner_builders.build(ast_branch["Select"].get("having"))
    )
    _having_passthrough: list = []
    if _having:
        # Before the aggregates are collected (below, once the group keys are resolved) —
        # that walk cannot tell a window from a plain aggregate, and appending one to
        # `_aggregates` is what silently threw its OVER spec away.
        _refuse_window_in_having(_having)

    _groups = logical_planner_builders.build(ast_branch["Select"].get("group_by"))[0]

    # GROUP BY ROLLUP(...) — lower the construct to the flat key list plus an explicit
    # list of GROUPING SETS over it, before any of the rewriting below runs. The flat
    # list then takes exactly the same path a plain GROUP BY does (nesting strip,
    # positional/alias resolution, binding), and `_grouping_sets` indexes into it
    # positionally — every rewrite below replaces list members in place, so the indices
    # stay valid.
    #
    # The sets are the representation the aggregate carries all the way to the native
    # compiler. It is deliberately the general form (an arbitrary list of key subsets),
    # not a rollup prefix-chain: CUBE and GROUPING SETS are the same object with a
    # different set list, so adding them is a builder change, not a redesign.
    _grouping_sets = None
    if isinstance(_groups, list) and any(
        isinstance(g, logical_planner_builders.GroupingConstruct) for g in _groups
    ):
        _groups, _grouping_sets = _expand_grouping_elements(_groups)

    if isinstance(_groups, list):
        # Both sides of the match have to be stripped, or the projection and the
        # group key still disagree — `SELECT (id + 1) ... GROUP BY id + 1` is the
        # case where only one side carries the wrapper.
        _groups = [_strip_outer_nesting(g) for g in _groups]

    # Resolve positional and aliased GROUP BY into the actual projection
    # expression, mirroring the ORDER BY resolution below:
    #
    #  * a bare positive integer is a 1-based position into the SELECT list
    #    (`GROUP BY 1` = the 1st output column). This is the de-facto convention
    #    across the analytical-engine cohort — Dremio, Trino, DuckDB, ClickHouse
    #    (ClickHouse flipped its default TO positional in 22.7). It is not in
    #    SQL-92 (ordinals there are an ORDER BY feature), but the whole peer set
    #    adopted it, so Opteryx matches. A position that lands on an aggregate is
    #    rejected, same as those engines ("aggregates are not allowed in GROUP BY").
    #  * a bare identifier matching an output alias refers to that projection
    #    expression (Postgres/DuckDB precedence — the output alias wins over a
    #    same-named source column).
    #
    # The substitution must happen HERE, before the binder's schema-pruning
    # (opteryx/planner/binder/aggregate.py) runs — otherwise a computed
    # expression's (e.g. CASE) inner columns get pruned from the schema as unused
    # and a later ColumnNotFoundError follows.
    # `_minted` is complete by here — the projection and ORDER BY hoists above are the
    # only writers — so the window outputs are known, which the GROUP BY resolution
    # immediately below needs (a key may resolve to one) as well as the HAVING
    # resolution and the windowed-grouping branch further down.
    _window_output_aliases = {_alias for _alias, _display in _minted.values()}

    if isinstance(_groups, list) and _groups:
        _rewritten_groups = []
        for _group_expr in _groups:
            if _group_expr.node_type == NodeType.LITERAL:
                _expr_cat = (
                    _group_expr.type.category
                    if isinstance(_group_expr.type, ColumnType)
                    else _group_expr.type
                )
                if _expr_cat == LogicalCategory.INTEGER:
                    _position = int(_group_expr.value)
                    if _position < 1 or _position > len(_projection):
                        raise UnsupportedSyntaxError(
                            f"**GROUP BY** position {_position} is out of range — **SELECT** has {len(_projection)} column(s). Positions count the **SELECT** columns and start at 1."
                        )
                    _target = _projection[_position - 1]
                    # A window is tested BEFORE the aggregate: the hoist has already
                    # replaced a window with an identifier reference, but a FRAMED or
                    # whole-partition aggregate window is still an AGGREGATOR node here,
                    # and reporting that as "an aggregate in the SELECT list" would name
                    # the wrong rule for it.
                    _refuse_window_group_key(_target, _window_output_aliases, _position)
                    if get_all_nodes_of_type(_target, select_nodes=(NodeType.AGGREGATOR,)):
                        raise UnsupportedSyntaxError(
                            f"**GROUP BY** position {_position} refers to an aggregate in the **SELECT** "
                            "list — aggregates cannot appear in **GROUP BY**."
                        )
                    _group_expr = _target
            elif _group_expr.node_type == NodeType.IDENTIFIER:
                _alias_match = next(
                    (
                        p
                        for p in _projection
                        if p.alias and p.alias.lower() == (_group_expr.source_column or "").lower()
                    ),
                    None,
                )
                if _alias_match is not None:
                    _group_expr = _alias_match
            # On the RESOLVED key, so one call covers all three spellings: a window
            # written directly in GROUP BY (unchanged by the arms above), one reached
            # through an output alias, and one reached by position (already refused
            # above, with its position named).
            _refuse_window_group_key(_group_expr, _window_output_aliases)
            _rewritten_groups.append(_group_expr)
        _groups = _rewritten_groups


    # ---- HAVING, resolved against the group keys ------------------------------------
    #
    # Deferred to HERE, below the GROUP BY resolution, because a HAVING sub-expression
    # has to be matched against the FINAL key list — positions and aliases resolved —
    # and that list did not exist at the point HAVING was built.
    #
    # `GROUP BY UPPER(name) HAVING UPPER(name) > 'A'` is standard SQL: HAVING may name a
    # grouping EXPRESSION, not just an alias for one. It was refused, because the
    # pass-through walk below saw only the bare identifier `name` at the leaf and asked
    # the Project ABOVE the aggregate to carry it — a column the aggregate legitimately
    # does not emit. The compiler then failed with "projecting a column the engine could
    # not resolve here", naming nothing. Worse, `GROUP BY UPPER(name) HAVING name > 'A'`
    # — which is NOT legal, `name` surviving neither grouping nor aggregation — produced
    # the identical message, so the legal query and the illegal one were indistinguishable.
    #
    # An identifier inside a grouping expression is consumed by the GROUPING, exactly as
    # one inside an aggregate is consumed by the AGGREGATE, and the existing
    # `_aggregate_operands` skip is the precedent this mirrors. What rides through the
    # Project is the KEY EXPRESSION, which the aggregate does emit — never its leaves.
    if _having:
        _projection_aliases = {p.alias.lower() for p in _projection if p.alias}
        _seen_expressions = {format_expression(p).lower() for p in _projection}
        _seen_expressions.update(
            p.qualified_name.lower() for p in _projection if p.qualified_name
        )

        _having_aggregates = get_all_nodes_of_type(
            _having, select_nodes=(NodeType.AGGREGATOR,)
        )

        # Identifiers INSIDE an aggregate are pre-aggregation operands (SUM(x) consumes
        # raw x) — hoisting them past the aggregate step is meaningless and they must be
        # skipped. Only bare identifiers (group keys) are pass-through candidates.
        _aggregate_operands = {
            id(identifier)
            for aggregate in _having_aggregates
            for identifier in get_all_nodes_of_type(
                aggregate, select_nodes=(NodeType.IDENTIFIER,)
            )
        }

        for _aggregate in _having_aggregates:
            # The binder dedups the aggregate list by schema_column.identity, so an
            # aggregate already named in the SELECT is not computed twice.
            _aggregates.append(_aggregate)
            _key = format_expression(_aggregate).lower()
            if _key not in _seen_expressions:
                _seen_expressions.add(_key)
                _having_passthrough.append(_aggregate)

        # The grouping keys, by rendering. `format_expression` is the expression identity
        # this function already matches on (`_seen_expressions`, and the pass-through
        # dedup further down) — one convention, not a second one invented here.
        #
        # GROUP BY ALL stands for the projection expressions that are neither aggregates
        # nor window outputs, and HAVING may name one of those the same way. Derived here
        # for MATCHING only — `_groups` stays WILDCARD and is resolved in its own place
        # below, so there is still one resolution of ALL, not two.
        if _groups == NodeType.WILDCARD:
            _explicit_groups = _group_by_all_keys(_projection, _window_output_aliases)
        elif isinstance(_groups, list):
            _explicit_groups = list(_groups)
        else:
            _explicit_groups = []
        # Resolve to the PROJECTION's node where the SELECT list carries the same
        # expression, and to the GROUP BY's node otherwise.
        #
        # `GROUP BY UPPER(name)` and `SELECT UPPER(name) AS u` are two AST objects for
        # one expression, and they bind to two different identities. Which one HAVING
        # must share depends on where its Filter ends up: pushed BELOW the aggregate
        # (legal for a key, and what predicate_pushdown does with a simple compare) the
        # group key's identity is what the stream carries; left ABOVE the Project — a
        # CASE, which pushdown does not move — only the projection's is. Binding to the
        # group key and being left above the Project asked the compiler to recompute
        # `UPPER(name)` from a `name` the aggregate had dropped, and it failed with
        # "references a column the stream does not carry".
        #
        # Preferring the projection is safe for both: a projected key is emitted by the
        # Project, and pushdown simply declines to move a predicate it cannot resolve
        # below. An unprojected key keeps the GROUP BY's node and rides through as a
        # pass-through column below.
        _projection_by_rendering = {}
        for _column in _projection:
            _projection_by_rendering.setdefault(format_expression(_column).lower(), _column)
        _group_key_renderings = {}
        for _key in _explicit_groups:
            _rendering = format_expression(_key).lower()
            _group_key_renderings[_rendering] = _projection_by_rendering.get(_rendering, _key)

        # Largest-first, and SUBSTITUTING: the whole `UPPER(name)` matches before its
        # leaf `name` is reached, and the matched subtree is REPLACED by the group key's
        # own Node - the same object the GROUP BY (and, where it is selected, the
        # projection) holds. Sharing the object is what makes it resolve: HAVING's copy
        # of the expression is a separate AST node that nothing else binds, so leaving it
        # in place bound nothing and failed with "IDENTIFIER node missing schema_column".
        # One object reachable from two clauses is the same rule `_rebase_memo` keeps for
        # everything computed above the grouping.
        _matched_keys: list = []

        def _substitute_group_keys(_node):
            if _node is None or not isinstance(_node, (Node, LogicalColumn)):
                return _node
            _rendering = format_expression(_node).lower()
            _key_node = _group_key_renderings.get(_rendering)
            if _key_node is not None:
                _matched_keys.append((_rendering, _key_node))
                return _key_node
            # The child set is `get_all_nodes_of_type`'s, exactly: parameters, CASE's
            # conditions/results/else_result, and left/right/centre. A walker that knows
            # a smaller set silently stops matching inside whatever it skips — a CASE
            # branch here would have left its `UPPER(name)` unresolved and then had the
            # leaf rejected as ungrouped, which is a WRONG error on a legal query.
            if _node.parameters:
                _node.parameters = [
                    _substitute_group_keys(_child) if isinstance(_child, (Node, LogicalColumn))
                    else _child
                    for _child in _node.parameters
                ]
            if _node.node_type == NodeType.CASE:
                if _node.conditions:
                    _node.conditions = [
                        _substitute_group_keys(_c) if isinstance(_c, (Node, LogicalColumn)) else _c
                        for _c in _node.conditions
                    ]
                if _node.results:
                    _node.results = [
                        _substitute_group_keys(_r) if isinstance(_r, (Node, LogicalColumn)) else _r
                        for _r in _node.results
                    ]
                if isinstance(_node.else_result, (Node, LogicalColumn)):
                    _node.else_result = _substitute_group_keys(_node.else_result)
            for _attr in ("left", "right", "centre"):
                _child = getattr(_node, _attr, None)
                if _child is not None:
                    setattr(_node, _attr, _substitute_group_keys(_child))
            return _node

        _having = _substitute_group_keys(_having)

        # Everything the substituted keys consume. Collected AFTER the rewrite, off the
        # keys themselves, so the leaf-identifier walk below cannot mistake a grouping
        # operand for an ungrouped column.
        _grouped_operands = {
            id(_inner)
            for _rendering, _key_node in _matched_keys
            for _inner in get_all_nodes_of_type(_key_node, select_nodes=(NodeType.IDENTIFIER,))
        }
        _grouped_operands.update(id(_key_node) for _rendering, _key_node in _matched_keys)

        # A matched key that the SELECT list does not already carry rides through the
        # Project as the KEY, so the Filter above can read the column the aggregate
        # emitted. `GROUP BY UPPER(name) HAVING UPPER(name) > 'A'` with no `UPPER(name)`
        # selected is the case this exists for.
        for _rendering, _key_node in _matched_keys:
            if _rendering not in _seen_expressions:
                _seen_expressions.add(_rendering)
                _having_passthrough.append(_key_node)

        for _identifier in get_all_nodes_of_type(_having, select_nodes=(NodeType.IDENTIFIER,)):
            if id(_identifier) in _aggregate_operands or id(_identifier) in _grouped_operands:
                continue
            # A bare identifier naming a SELECT alias (`SUM(q) AS x ... HAVING x > 300`)
            # resolves against the Project's own output — the Project creates it, so it
            # must not be hoisted from below it.
            if (_identifier.source_column or "").lower() in _projection_aliases:
                continue
            # Under an EXPLICIT GROUP BY, an identifier left over here survives neither
            # the grouping nor an aggregate, so there is no value for it in a grouped
            # row - `GROUP BY UPPER(name) HAVING name > 'A'`. That is a bind error, and
            # naming the column is the whole point: it used to be hoisted into the
            # Project and reported by the compiler as an unresolvable column, in the same
            # words the LEGAL form above got.
            if _explicit_groups:
                _display = format_expression(_identifier)
                raise UnsupportedSyntaxError(
                    f"Column {md_code(_display)} in **HAVING** is not grouped and is not "
                    "aggregated, so a grouped row has no single value for it. Add it to "
                    f"**GROUP BY**, wrap it in an aggregate, or filter on it in **WHERE** "
                    "(which runs before the grouping)."
                )
            _key = format_expression(_identifier).lower()
            if _key not in _seen_expressions:
                _seen_expressions.add(_key)
                _having_passthrough.append(_identifier)

    # ---- a window computed OVER the grouped result -----------------------------------
    #
    # `SELECT k, SUM(x), SUM(SUM(x)) OVER (PARTITION BY k) ... GROUP BY k` is standard
    # SQL and the common analytics idiom: GROUP BY collapses the rows, the aggregates are
    # computed per group, and the window then runs over the GROUPED rows — an aggregate
    # nested inside a window's argument being the group-level aggregate, and the window's
    # own call the aggregate over those group results.
    #
    # Both arrangements were refused, and for a real reason: the Window step is planned
    # UNDER the aggregate step, so the window would have been computed over the rows the
    # aggregate collapses and could never see the aggregated result. A bare aggregate
    # with no GROUP BY is the same wall (it is still one group), and so is a window
    # borrowed into the projection by QUALIFY or by ORDER BY.
    #
    # The fix is the plan shape the refusals used to ADVISE the caller to write by hand:
    # aggregate first, present the grouped rows as a relation, and run the windows over
    # it —
    #
    #     <source> -> Aggregate[AndGroup] -> Project -> Subquery -> Window(s) -> ...
    #
    # The Subquery is not decoration. `window_to_join` copies the sub-plan below the
    # Window node as the window's input and needs it to expose exactly ONE relation name
    # (`_source_relation`) to rebuild the outer leg of its join as a qualified wildcard.
    # An aggregate over a join exposes none, so the boundary is what makes the grouped
    # rows addressable — and it is also what lets a window run over a multi-table join
    # at all, which `_find_base_scan` refuses in every other arrangement.
    #
    # Everything above the grouping is then re-pointed at that relation's columns
    # (`_rebase_over_aggregate`), because past the boundary the source columns are gone
    # and only the group keys and the aggregates have names.
    if (_window_specs or _ranking_specs) and (
        (_groups is not None and _groups != []) or _aggregates
    ):
        if _groups is not None and _groups != []:
            if any(p.node_type == NodeType.WILDCARD for p in _projection):
                raise UnsupportedSyntaxError(
                    "`SELECT *` cannot be used with **GROUP BY** — did you mean `GROUP BY ALL`?"
                )
            if _groups == NodeType.WILDCARD:
                _groups = _group_by_all_keys(_projection, _window_output_aliases)
        _grouped_keys = list(_groups) if isinstance(_groups, list) else []

        # The hoisted window nodes themselves. They are aggregates by node type, and
        # QUALIFY still holds the original object in its predicate, so they have to be
        # told apart from the group-level aggregates rather than walked into. A ranking
        # window is here too — it is not in `_window_specs`, but `RANK()` is an
        # AGGREGATOR node and collecting it as a group-level aggregate reported it as
        # "the aggregate function RANK is not supported".
        _hoisted_windows = {id(_agg_node) for _agg_node, _partition_by, _wob, _frame in _window_specs}
        _hoisted_windows.update(id(_original) for _slot, _original in _qualify_window_slots)

        # The aggregates the grouping must compute, gathered from every clause that can
        # hold one. `_aggregates` already carries the SELECT list's, HAVING's and ORDER
        # BY's; what it cannot carry is an aggregate written inside a WINDOW, because the
        # hoist spliced the window out of the projection before the collection walk ran —
        # so `SUM(SUM(x)) OVER (...)`'s inner SUM was computed by nothing at all.
        _window_operands: list = []
        for _agg_node, _partition_by, _wob, _frame in _window_specs:
            _window_operands.extend(_agg_node.parameters or [])
            _window_operands.extend(_partition_by)
            if _wob:
                _window_operands.extend(_column for _column, _ascending in _wob)
        for _kind, _partition_by, _window_order_by, _win_alias, _params in _ranking_specs:
            _window_operands.extend(_partition_by)
            _window_operands.extend(_column for _column, _ascending in _window_order_by)
            _window_operands.extend(_params or [])
        for _operand in _window_operands:
            _aggregates.extend(_outermost_aggregates(_operand))
        # A plain aggregate in QUALIFY (`QUALIFY RANK() OVER (...) = 1 AND SUM(x) > 5`).
        # Its Filter sits above the windows but BELOW the Project, so it reads the
        # grouped relation directly and needs no pass-through — only computing.
        for _aggregate in _outermost_aggregates(_qualify):
            if id(_aggregate) not in _hoisted_windows:
                _aggregates.append(_aggregate)

        # One aggregate, however many times it is written. `SUM(x) AS revenue` in the
        # SELECT list, a bare `SUM(x)` inside a window's argument and a third in HAVING
        # are ONE grouped column that all three read. Deduped on the RENDERING, which is
        # also the name the column crosses the boundary under — the aliases are re-applied
        # above it, on the references, so which spelling wins here does not name anything.
        _grouped_aggregates: list = []
        _seen_renderings: set = set()
        for _aggregate in _aggregates:
            _rendering = format_expression(_aggregate).lower()
            if _rendering not in _seen_renderings:
                _seen_renderings.add(_rendering)
                _grouped_aggregates.append(_aggregate)

        # The columns the grouped relation exposes, and the spellings that reach each.
        # A literal group key is dropped: it forms one group and names no column (the
        # binder strips it from `groups` for the same reason).
        _grouped_columns: list = [
            _key for _key in _grouped_keys if _key.node_type != NodeType.LITERAL
        ] + _grouped_aggregates
        _grouped_names: dict = {}
        _grouped_taken: set = set()
        for _column in _grouped_columns:
            _output_name = _grouped_output_name(_column)
            if _output_name in _grouped_taken:
                raise UnsupportedSyntaxError(
                    compose(
                        f"The grouped result has two columns named {md_code(_output_name)}",
                        "A window is computed over the grouped rows, which makes them a "
                        "relation — and a relation cannot hold two columns of one name",
                        f"Name one of them differently with {md_syntax('as')}",
                    )
                )
            _grouped_taken.add(_output_name)
            for _key in _grouped_reference_keys(_column):
                _grouped_names[_key] = _output_name

        # A SELECT alias standing for a group key or an aggregate reaches the grouped
        # relation too. HAVING is planned BELOW the boundary (see the note where its
        # Filter is emitted), which is below the Project that creates the alias, so
        # `SUM(q) AS x ... HAVING x > 300` has to resolve `x` to the column `SUM(q)`
        # rather than to a name that does not exist yet. Registered only where the
        # projection IS one of the grouped columns, and never over an existing key — a
        # real column of that name outranks an alias of it.
        for _column in _projection:
            if not _column.alias:
                continue
            _alias_key = _column.alias.lower()
            if _alias_key in _grouped_names:
                continue
            _target = None
            if _column.node_type == NodeType.IDENTIFIER and _column.source_column:
                _target = _grouped_names.get(_column.source_column.lower())
            if _target is None:
                _target = _grouped_names.get(format_expression(_column).lower())
            if _target is not None:
                _grouped_names[_alias_key] = _target

        # Names that resolve ABOVE the grouping rather than against it: a window's
        # output column, and a SELECT alias the statement's ORDER BY names (the Project
        # creates it, and the Order node sits above the Project).
        _above_grouping = {_alias.lower() for _alias in _window_output_aliases}
        _above_grouping.update(p.alias.lower() for p in _projection if p.alias)

        # Re-point everything computed ABOVE the grouping. One memo across the whole
        # statement, so a node reachable from two clauses stays one object.
        _rebase_memo: dict = {}

        def _rebase(_node):
            return _rebase_over_aggregate(
                _node, _grouped_names, _hoisted_windows, _above_grouping, _rebase_memo
            )

        for _index, _column in enumerate(_projection):
            _projection[_index] = _rebase(_column)
        _having = _rebase(_having)
        _qualify = _rebase(_qualify)
        if isinstance(_order_by, list):
            _order_by = [
                (_rebase(_expression), _ascending) for _expression, _ascending in _order_by
            ]
            _order_by_columns = [_item[0] for _item in _order_by]
        for _index, (_agg_node, _partition_by, _wob, _frame) in enumerate(_window_specs):
            _agg_node.parameters = [
                _rebase(_parameter) if isinstance(_parameter, (Node, LogicalColumn)) else _parameter
                for _parameter in (_agg_node.parameters or [])
            ]
            _rebased_wob = (
                [(_rebase(_column), _ascending) for _column, _ascending in _wob] if _wob else _wob
            )
            _window_specs[_index] = (
                _agg_node, [_rebase(_pb) for _pb in _partition_by], _rebased_wob, _frame
            )
        for _index, (
            _kind,
            _partition_by,
            _window_order_by,
            _win_alias,
            _params,
        ) in enumerate(_ranking_specs):
            _ranking_specs[_index] = (
                _kind,
                [_rebase(_pb) for _pb in _partition_by],
                [(_rebase(_column), _ascending) for _column, _ascending in _window_order_by],
                _win_alias,
                [_rebase(_parameter) for _parameter in (_params or [])],
            )

        if _grouped_keys:
            _grouped_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
            _grouped_step.groups = _grouped_keys
            _grouped_step.aggregates = _grouped_aggregates
            _grouped_step.projection = list(_grouped_columns)
            # None = one set over every key (a plain GROUP BY) — see the note on the
            # ungrouped-window-free path below.
            _grouped_step.grouping_sets = _grouping_sets
        else:
            # No GROUP BY: one group holding every row, and the UNGROUPED node is the one
            # that promises exactly one row out for any input. `AggregateAndGroup` over an
            # empty key list makes no such promise, and the window above reads that one
            # row as its whole partition.
            _grouped_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
            _grouped_step.groups = []
            _grouped_step.aggregates = _grouped_aggregates
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, _grouped_step)
        inner_plan.add_edge(previous_step_id, step_id)

        # A Project between the aggregate and the boundary. The aggregate's outputs live
        # in `$derived` until a Project renames them to `$project`, and the Subquery's
        # `visit_exit` pops `$derived` — without this the aggregate columns never appear
        # in the boundary's schema at all (the same requirement `window_to_join` has when
        # it builds its own aggregate CTE).
        _grouped_project = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        _grouped_project.columns = list(_grouped_columns)
        _grouped_project.passthrough_columns = []
        _grouped_project.except_columns = None
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, _grouped_project)
        inner_plan.add_edge(previous_step_id, step_id)

        if _having:
            # HAVING is planned INSIDE the boundary, below the windows — SQL evaluates it
            # before window functions, so the window runs over the groups that SURVIVED
            # it. Left in its usual place above the Project (which is above the windows)
            # it filtered nothing the window had not already counted:
            # `GROUP BY k HAVING SUM(id) > 4` with `SUM(SUM(id)) OVER ()` answered the
            # total over ALL groups, 45 where DuckDB and the standard say 35.
            #
            # It reads the Project's output, which is the same scope it reads today, and
            # its aggregates and group keys are all grouped columns — so unlike the
            # un-windowed path it needs no pass-through columns above.
            _having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
            _having_step.condition = _having
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, _having_step)
            inner_plan.add_edge(previous_step_id, step_id)
            _having = None
            _having_passthrough = []

        _grouped_relation = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
        _grouped_relation.alias = f"{GROUPED_AGGREGATE_ALIAS_PREFIX}{random_string(6)}"
        _grouped_relation.columns = [Node(node_type=NodeType.WILDCARD)]
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, _grouped_relation)
        inner_plan.add_edge(previous_step_id, step_id)

        # The grouping has been planned. Emptied so the aggregate steps further down do
        # not plan it a second time, above the windows this time.
        _groups = []
        _aggregates = []
        _grouped_window_source = True
    else:
        _grouped_window_source = False

    if _window_specs:
        if not _grouped_window_source:
            # Refuse a window with no base table, or with more than one, while the clause
            # that wrote it is still in hand. The scan itself is NOT captured: the rewriter
            # copies the whole sub-plan below the Window node instead, which is both the
            # window's real input (WHERE included) and the post-resolution shape of it.
            #
            # Skipped over a grouped result: the boundary above IS the single relation
            # this is checking for, however many tables the grouping read.
            _find_base_scan(inner_plan)

        # Whole-partition aggregate windows (no ORDER BY/FRAME) still take the
        # broadcast-join lowering (window_to_join.py, unchanged). A framed one
        # (ORDER BY and/or FRAME present) needs a genuinely ordered, per-row
        # computation instead — see FramedWindowSink — and is built below.
        _unframed_specs = [
            (_agg_node, _partition_by)
            for _agg_node, _partition_by, _wob, _frame in _window_specs
            if _frame is None
        ]
        _framed_specs = [
            (_agg_node, _partition_by, _wob, _frame)
            for _agg_node, _partition_by, _wob, _frame in _window_specs
            if _frame is not None
        ]

        if _unframed_specs:
            # Group by distinct partition spec; same partition → one Window node
            # (shared CTE).
            _by_partition: dict = {}
            for _agg_node, _partition_by in _unframed_specs:
                _key = tuple(
                    getattr(pb, "source_column", None)
                    or getattr(pb, "value", None)
                    or format_expression(pb)
                    for pb in _partition_by
                )
                if _key not in _by_partition:
                    _by_partition[_key] = (_partition_by, [])
                _by_partition[_key][1].append(_agg_node)
            for _key, (_partition_by, _agg_nodes) in _by_partition.items():
                _window_step = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
                _window_step.aggregates = _agg_nodes
                _window_step.partition_by = _partition_by
                previous_step_id, step_id = step_id, random_string()
                inner_plan.add_node(step_id, _window_step)
                inner_plan.add_edge(previous_step_id, step_id)

        if _framed_specs:
            from opteryx.types.schema import SchemaColumn, mint_column_identity

            # Group by distinct PARTITION BY + window ORDER BY: functions that share
            # both need only one sorted pass (FramedWindowSink computes every one of
            # them from it), even when their FRAMES differ — the frame is per
            # function, the sort is per (partition, order).
            _by_fspec: dict = {}
            for _agg_node, _partition_by, _wob, _frame in _framed_specs:
                _pkey = tuple(format_expression(pb) for pb in _partition_by)
                _okey = tuple((format_expression(c), bool(a)) for c, a in _wob)
                _spec_key = (_pkey, _okey)
                if _spec_key not in _by_fspec:
                    _by_fspec[_spec_key] = (_partition_by, _wob, [])
                _by_fspec[_spec_key][2].append((_agg_node, _frame))
            for _spec_key, (_partition_by, _wob, _outs) in _by_fspec.items():
                _win_rel = f"$framedwindow-{random_string(6)}"
                # INT64 is a placeholder: the true output type depends on the
                # aggregate AND its argument's type (SUM(int) is INT64, SUM(float)
                # is FLOAT64, AVG is always FLOAT64, SUM/MIN/MAX(DECIMAL128) stays
                # DECIMAL128, ...) and is not resolved until binding — the framed
                # window binder overwrites `column_type` there, mirroring how the
                # ranking window binder overwrites LAG/LEAD's placeholder.
                _outputs = [
                    (
                        _agg_node.value,
                        SchemaColumn(
                            name=_agg_node.alias,
                            column_type=_plt.INT64,
                            identity=mint_column_identity(_win_rel, _agg_node.alias),
                        ),
                        list(_agg_node.parameters or []),
                        _frame,
                    )
                    for _agg_node, _frame in _outs
                ]
                _win_step = LogicalPlanNode(node_type=LogicalPlanStepType.FramedWindow)
                _win_step.partition_by = _partition_by
                _win_step.order_by = _wob
                _win_step.outputs = _outputs
                _win_step.output_relation = _win_rel
                _win_step.columns = []
                previous_step_id, step_id = step_id, random_string()
                inner_plan.add_node(step_id, _win_step)
                inner_plan.add_edge(previous_step_id, step_id)

    if _ranking_specs:
        from opteryx.types.schema import SchemaColumn, mint_column_identity

        # Group ranking functions that share the same PARTITION BY + ORDER BY into a
        # single Window node (one sort serves all of them).
        # `_window_order_by` here is the WINDOW's ORDER BY, deliberately NOT named
        # `_order_by`: the statement-level ORDER BY is built above this point, and a
        # loop variable of that name would silently overwrite it for everything
        # downstream (it did — `RANK() OVER(ORDER BY id)` lost the statement's ORDER BY).
        _by_spec: dict = {}
        for _kind, _partition_by, _window_order_by, _win_alias, _params in _ranking_specs:
            _pkey = tuple(format_expression(pb) for pb in _partition_by)
            _okey = tuple((format_expression(c), bool(a)) for c, a in _window_order_by)
            _spec_key = (_pkey, _okey)
            if _spec_key not in _by_spec:
                _by_spec[_spec_key] = (_partition_by, _window_order_by, [])
            _by_spec[_spec_key][2].append((_kind, _win_alias, _params))
        for _spec_key, (_partition_by, _window_order_by, _outs) in _by_spec.items():
            _win_rel = f"$window-{random_string(6)}"
            # INT64 is the true type for the ranking functions. For LAG/LEAD it is a
            # placeholder: the output's type is the ARGUMENT's, which is not resolved
            # until binding — the window binder overwrites `column_type` there.
            _outputs = [
                (
                    _kind,
                    SchemaColumn(
                        name=_win_alias,
                        column_type=_plt.INT64,
                        identity=mint_column_identity(_win_rel, _win_alias),
                    ),
                    _params,
                )
                for _kind, _win_alias, _params in _outs
            ]
            _win_step = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
            _win_step.partition_by = _partition_by
            _win_step.order_by = _window_order_by
            _win_step.outputs = _outputs
            _win_step.output_relation = _win_rel
            _win_step.columns = []
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, _win_step)
            inner_plan.add_edge(previous_step_id, step_id)

    if _qualify is not None:
        # The detection loop replaced each of QUALIFY's window functions with an
        # identifier reference to the Window node's output. Point the predicate at
        # those references, by object IDENTITY — the same node object sits in both
        # `_projection` and the predicate tree, so a value-based match could
        # rewrite the wrong one when a query filters on two windows that render
        # identically.
        for _slot, _original in _qualify_window_slots:
            _qualify = _replace_node(_qualify, _original, _projection[_slot])

        qualify_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        qualify_step.condition = _qualify
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, qualify_step)
        inner_plan.add_edge(previous_step_id, step_id)

        # Drop the window columns QUALIFY borrowed. They were only ever in
        # `_projection` to make the detection loop build a Window node for them;
        # the Filter above reads the Window's output directly, and the Project is
        # ABOVE the Filter, so removing them here keeps them out of the caller's
        # result without keeping them out of the filter's input. Leaving them in
        # returned an extra column nobody asked for — `SELECT name ... QUALIFY
        # ROW_NUMBER() OVER (...) = 1` answered two columns.
        #
        # Removing them only covers a projection that NAMES its columns. A wildcard
        # names none of them and is expanded at bind time from the relations in
        # scope — which include the Window node's output relation (a ranking window)
        # or the aggregate CTE the window-to-join rewrite builds — so `SELECT *`
        # picked the minted column straight back up. The names to skip were recorded
        # during the hoist, from what each clause NEWLY minted: reading them back off
        # the slots here would hide a window the caller also SELECTED, in the case
        # where QUALIFY's window dedups onto that one.
        del _projection[_projection_length_without_qualify:]

    if _groups is not None and _groups != []:
        if any(p.node_type == NodeType.WILDCARD for p in _projection):
            raise UnsupportedSyntaxError(
                "`SELECT *` cannot be used with **GROUP BY** — did you mean `GROUP BY ALL`?"
            )
        # WILDCARD is used to represent GROUP BY ALL, we group by all columns in the projection
        # which aren't aggregates
        if _groups == NodeType.WILDCARD:
            _groups = _group_by_all_keys(_projection, _window_output_aliases)

        group_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
        group_step.groups = _groups
        group_step.aggregates = _aggregates
        group_step.projection = _projection
        # None = one set over every key (a plain GROUP BY). Otherwise the explicit set
        # list, as index tuples into `groups` — see `_expand_grouping_elements`.
        group_step.grouping_sets = _grouping_sets
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, group_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)
    # aggregates
    elif len(_aggregates) > 0:
        aggregate_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
        aggregate_step.groups = _groups
        aggregate_step.aggregates = _aggregates

        known_columns = {
            hash(n) for n in get_all_nodes_of_type(_groups + _aggregates, (NodeType.IDENTIFIER,))
        }
        project_columns = [
            n
            for n in get_all_nodes_of_type(_projection, (NodeType.IDENTIFIER,))
            if hash(n) not in known_columns
        ]

        if len(project_columns) > 0:
            from opteryx.exceptions import SqlError

            # The name the caller WROTE, not the internal one. `source_column` is the
            # bare identity — for a hoisted window reference it is the minted `$win_`
            # join key (random per execution, and a column nobody typed), and for a
            # qualified column it has already lost its qualifier. `query_column` is the
            # display form every identifier carries from the builders, and is the only
            # spelling safe to put in front of a caller.
            _offending = project_columns.pop()
            column = _offending.query_column or _offending.source_column
            error = f"Column '{column}' must appear in the `GROUP BY` clause or must be part of an aggregate function. Either add it to the `GROUP BY` list, or add an aggregation such as `MIN({column})`."
            raise SqlError(error)

        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, aggregate_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # projection
    project_step = None
    if not (
        len(_projection) == 1
        and _projection[0].node_type == NodeType.WILDCARD
        and _projection[0].except_columns is None
        and _projection[0].value is None
    ):
        for column in _projection:
            if (
                column.node_type == NodeType.LITERAL
                # A typed NULL is never a parenthesised value list — `(a, b)` always
                # carries values. `CAST(NULL AS ARRAY<E>)` folds to a NULL literal that
                # must keep ARRAY<E> (nothing downstream could recover the element type
                # otherwise), which gives it category ARRAY and would trip this refusal.
                # Exempted narrowly by absence of a value, NOT by widening the category
                # test: an array literal that HAS values still cannot be projected (its
                # materialization is broken three separate ways), and this guard is what
                # keeps that a clean error instead of a TypeError or an empty array.
                and column.value is not None
                and column.type is not None
                and column.type.category
                in (
                    LogicalCategory.ARRAY,
                    LogicalCategory.VECTOR,
                )
            ):
                if ast_branch["Select"].get("distinct"):
                    raise UnsupportedSyntaxError(
                        "A value list cannot be projected in the **SELECT** clause — did you mean **DISTINCT ON**(cols) cols **FROM** ?"
                    )
                # Names BOTH spellings: the message used to say "parenthesised"
                # at a caller who had typed `SELECT ['a','b']`, sending them to
                # look for parentheses they had not used.
                raise UnsupportedSyntaxError(
                    "A literal list cannot be projected in the **SELECT** clause, in either "
                    "`['a', 'b']` or `('a', 'b')` form. Use `UNNEST(('a', 'b'))` in the "
                    "**FROM** clause to build a relation from literals."
                )

        # ORDER BY needing to be able to order by columns not in the projection
        # whilst being able to order by aliases created by the projection means
        # we need to do specific checks
        if _order_by_columns:
            # Collect qualified names and aliases from projection columns
            projection_qualified_names = {
                proj_col.qualified_name for proj_col in _projection if proj_col.qualified_name
            }.union({f".{proj_col.alias}" for proj_col in _projection if proj_col.alias})

            # Compare projection and ORDER BY identifiers case-insensitively
            projection_qualified_names_lower = {n.lower() for n in projection_qualified_names}

            # Collect expression columns from projection (lowercased)
            projection_expressions_lower = {
                format_expression(proj_col).lower()
                for proj_col in _projection
                if proj_col.node_type != NodeType.IDENTIFIER
            }

            # Collect source column names from projection (lowercased)
            projection_source_columns_lower = {
                f".{proj_col.source_column}".lower()
                for proj_col in _projection
                if getattr(proj_col, "source_column", None)
            }

            # Remove columns from ORDER BY that are directly in the projection, aliased, or have the same expression
            _order_by_columns_not_in_projection = [
                ord_col
                for ord_col in _order_by_columns
                if (
                    (ord_col.qualified_name or "").lower() not in projection_qualified_names_lower
                    and f".{(ord_col.source_column or '')}".lower()
                    not in projection_qualified_names_lower
                    and f".{(ord_col.source_column or '')}".lower()
                    not in projection_source_columns_lower
                    and format_expression(ord_col).lower() not in projection_expressions_lower
                )
            ]

            # Remove columns from ORDER BY that match the source of a wildcard in the projection
            if _projection[0].except_columns is None:
                for proj_col in [pc for pc in _projection if pc.node_type == NodeType.WILDCARD]:
                    _order_by_columns_not_in_projection = [
                        ord_col
                        for ord_col in _order_by_columns_not_in_projection
                        if (ord_col.source or "").lower() != (proj_col.value[0] or "").lower()
                    ]

            for ord_col in _order_by_columns:
                if not _is_vector_order_expression(ord_col):
                    continue
                _order_by_columns_not_in_projection = [
                    candidate
                    for candidate in _order_by_columns_not_in_projection
                    if candidate is not ord_col
                ]
                source_column = _get_vector_order_source_identifier(ord_col.parameters[0])
                if source_column is None:
                    continue
                source_identity = getattr(source_column.schema_column, "identity", None)
                existing_projection_identities = {
                    getattr(col.schema_column, "identity", None)
                    for col in list(_projection) + list(_order_by_columns_not_in_projection)
                    if getattr(col, "schema_column", None) is not None
                }
                if source_identity in existing_projection_identities:
                    continue
                _order_by_columns_not_in_projection.append(source_column)

        project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        project_step.columns = _projection
        project_step.passthrough_columns = _order_by_columns_not_in_projection
        project_step.except_columns = _projection[0].except_columns
        project_step.hidden_columns = _hidden_window_columns
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, project_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # EXCEPT with ORDER BY creates complex situations
    if project_step and project_step.except_columns and _order_by_columns_not_in_projection:
        if any(
            col.source_column in {c.source_column for c in project_step.except_columns}
            for col in project_step.passthrough_columns
        ):
            raise UnsupportedSyntaxError(
                "Cannot **ORDER BY** columns excluded by the **EXCEPT** clause in the projection."
            )
        project_step.passthrough_columns = []

    # HAVING expressions absent from the SELECT list ride through the Project so the
    # Filter above it can read them. Appended AFTER the EXCEPT/ORDER BY reconciliation
    # above — that block clears the ORDER BY pass-throughs, which must not take the
    # HAVING ones with it (the query is unexecutable without them).
    #
    # Deduped against the ORDER BY pass-throughs already present: `HAVING SUM(x) > 1
    # ORDER BY SUM(x)` hoists the same expression from both clauses, and emitting it
    # twice trips the binder's duplicate-output check (AmbiguousIdentifierError).
    if project_step is not None and _having_passthrough:
        _existing = list(project_step.passthrough_columns or [])
        _existing_keys = {format_expression(c).lower() for c in _existing}
        for _column in _having_passthrough:
            if format_expression(_column).lower() not in _existing_keys:
                _existing_keys.add(format_expression(_column).lower())
                _existing.append(_column)
        project_step.passthrough_columns = _existing

    # having
    if _having:
        having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        having_step.condition = _having
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, having_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # distinct
    if ast_branch["Select"].get("distinct"):
        distinct_step = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
        if isinstance(ast_branch["Select"]["distinct"], dict):
            distinct_step.on = [
                _strip_outer_nesting(c)
                for c in logical_planner_builders.build(ast_branch["Select"]["distinct"]["On"])
            ]
        elif project_step is not None and project_step.passthrough_columns:
            # the ORDER BY value is ambiguous once rows collapse into a DISTINCT
            # group - the column must appear in the SELECT list so the ordering
            # value is well-defined per output row.
            raise UnsupportedSyntaxError(
                "With **SELECT DISTINCT**, everything in **ORDER BY** must also appear "
                "in the **SELECT** list - otherwise the rows being ordered are not the "
                "rows being returned. Add the expression to the **SELECT** list, or drop "
                "**DISTINCT**."
            )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, distinct_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # order
    if _order_by:
        order_step = LogicalPlanNode(node_type=LogicalPlanStepType.Order)
        order_step.order_by = _order_by
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, order_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # limit/offset
    _limit = ast_branch.get("limit")
    _offset = ast_branch.get("offset")
    if _limit or _offset:
        limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
        limit_step.limit = None if _limit is None else logical_planner_builders.build(_limit).value
        limit_step.offset = (
            None if _offset is None else logical_planner_builders.build(_offset).value
        )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, limit_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # add the exit node
    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = _projection
    exit_node.hidden_columns = _hidden_window_columns
    previous_step_id, step_id = step_id, random_string()
    inner_plan.add_node(step_id, exit_node)
    if previous_step_id is not None:
        inner_plan.add_edge(previous_step_id, step_id)

    return inner_plan


"""
STATEMENT PLANNERS
"""


def process_join_tree(join: dict) -> LogicalPlanNode:
    """
    Processes a join tree from the AST and returns a LogicalPlanNode representing the join.
    """

    def extract_join_type(join: dict) -> str:
        """
        Extracts the type of the join from the AST node representing the join.
        """
        join_operator = join["join_operator"]

        if join_operator == {"Join": "Natural"}:
            return "natural join"
        elif join_operator == "CrossJoin":
            return "cross join"
        elif join_operator == {"Join": "None"}:
            return "cross join"

        join_operator = next(iter(join["join_operator"]))

        return {
            "Anti": "left anti",  # ANTI JOIN is a LEFT ANTI JOIN
            "AsOf": "asof",
            "FullOuter": "full outer",
            "Join": "inner",
            "Inner": "inner",
            "LeftAnti": "left anti",
            "Left": "left outer",
            "LeftOuter": "left outer",
            "LeftSemi": "left semi",
            "RightAnti": "right anti",  # not supported
            "Right": "right outer",
            "RightOuter": "right outer",
            "RightSemi": "right semi",  # not supported
            "Semi": "left semi",  # SEMI JOIN is a LEFT SEMI JOIN
            "CrossJoin": "cross join",  # should never match, here for completeness
            "Natural": "natural join",  # should never match, here for completeness
        }.get(join_operator)

    def extract_join_condition(join: dict) -> Tuple[Optional[str], Optional[List[str]]]:
        """
        Extracts the join's limiting condition from the AST node representing the join.
        """
        join_operator = join["join_operator"]
        if not isinstance(join_operator, dict):
            return None, None

        join_on = None
        join_using = None

        join_operator = next(iter(join_operator))
        join_condition = next(iter(join["join_operator"][join_operator]))
        if join_condition == "On":
            join_on = _strip_outer_nesting(
                logical_planner_builders.build(
                    join["join_operator"][join_operator][join_condition]
                )
            )
            # A conjunct with no column reference at all (a bare literal like
            # `AND FALSE`, anywhere in the AND-tree) has no join key to extract
            # and no column for a Filter step to carry it on — the optimizer's
            # predicate-pushdown machinery has nowhere to route it and silently
            # drops it between planning and execution. Reject at plan time
            # instead, mirroring the same rule already enforced for WHERE.
            _validate_where_clause_expression(
                join_on, clause_label="JOIN condition", example_prefix="ON "
            )
        if join_condition == "Using":
            join_using = [
                logical_planner_builders.build(identifier[0])
                for identifier in join["join_operator"][join_operator][join_condition]
            ]

        return join_on, join_using

    def create_unnest_node(join: dict, join_step: Node, function: str = "UNNEST") -> Node:
        """
        Extracts information for an UNNEST dataset from the AST node representing the join.

        Shared by UNNEST (expand an ARRAY column) and CIDR_UNNEST (expand a CIDR
        block into one row per address). Both are the same PLAN shape — one value
        per parent row fanning out to many rows — and differ only in the expansion
        rule, so they share this node type rather than adding a second one. A
        distinct LogicalPlanStepType would need a matching `visit_` in the binder
        or it would silently pass through unbound, and the three optimizer
        strategies that treat Unnest as a barrier want identical treatment for
        both. `unnest_function` is what the binder and compiler branch on.
        """
        if join_step.type != "cross join":
            raise UnsupportedSyntaxError(f"**JOIN** on {function} only supported for CROSS joins. Write it as a **CROSS JOIN**.")
        unnest_column = logical_planner_builders.build(join["relation"]["Table"]["args"]["args"][0])
        if join["relation"]["Table"].get("alias") is None:
            raise UnnamedColumnError(
                f"Column created by {function} has no name, use AS to name the column."
            )
        unnest_alias = join["relation"]["Table"]["alias"]["name"]["value"]

        # if we're a UNNEST JOIN, we're a different node type
        join_step.node_type = LogicalPlanStepType.Unnest
        join_step.unnest_column = unnest_column
        join_step.unnest_alias = unnest_alias
        join_step.unnest_function = function
        join_step.alias = f"$unnest-{random_string(6)}"

        # return the updated node
        return join_step

    join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)

    join_step.type = extract_join_type(join)

    if join_step.type in ("right semi", "right anti"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} **JOIN** not supported, use LEFT variations only."
        )

    if join_step.type == "asof":
        asof_payload = join["join_operator"]["AsOf"]
        join_step.asof_condition = _strip_outer_nesting(
            logical_planner_builders.build(asof_payload["match_condition"])
        )
        constraint = asof_payload.get("constraint", "None")
        if isinstance(constraint, dict) and "On" in constraint:
            join_step.on = _strip_outer_nesting(
                logical_planner_builders.build(constraint["On"])
            )
        elif isinstance(constraint, dict) and "Using" in constraint:
            join_step.using = [logical_planner_builders.build(i[0]) for i in constraint["Using"]]
    else:
        join_step.on, join_step.using = extract_join_condition(join)

    if not join_step.on and not join_step.using and join_step.type in ("left outer", "right outer"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} **JOIN** must have an ON or **USING** clause. Add `ON left.key = right.key`, or `USING (key)` when both sides name the column the same way."
        )

    # JOIN UNNEST needs to be handled differently
    if "Table" in join.get("relation", {}):
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in join["relation"]["Table"]["name"]
        )
        if relation_name.upper() in ("UNNEST", "CIDR_UNNEST"):
            join_step = create_unnest_node(join, join_step, relation_name.upper())

    return join_step


def create_node_relation(relation: dict):
    sub_plan = LogicalPlan()
    root_node = None

    relation_name = None
    if "Table" in relation["relation"]:
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in relation["relation"]["Table"]["name"]
        )

    if "Derived" in relation["relation"]:
        if relation["relation"]["Derived"]["subquery"]:
            subquery = relation["relation"]["Derived"]
            if "Values" not in subquery["subquery"]["body"]:
                # SUBQUERY nodes wrap other queries and the result is available as a relation in
                # the parent query.
                subquery_step = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
                if subquery["alias"] is None:
                    subquery_step.alias = f"$subquery-{random_string(6)}"
                else:
                    subquery_step.alias = subquery["alias"]["name"]["value"]
                step_id = random_string()
                sub_plan.add_node(step_id, subquery_step)

                subquery_plan = plan_query(subquery["subquery"])
                exit_node = subquery_plan.get_exit_points()[0]
                subquery_step.columns = subquery_plan[exit_node].columns
                subquery_plan.remove_node(exit_node, heal=True)

                sub_plan += subquery_plan
                subquery_entry_id = subquery_plan.get_exit_points()[0]
                sub_plan.add_edge(subquery_entry_id, step_id)

                root_node = step_id
                relation["step_id"] = step_id
            else:
                # VALUES nodes are where the relation is defined within the SQL statement.
                # e.g. SELECT * FROM (VALUES(1),(2)) AS numbers (number)
                #
                # We have the name of the relation (alias), the column names (columns) and the
                # values in each row (values)
                values_step = LogicalPlanNode(
                    node_type=LogicalPlanStepType.FunctionDataset, function="VALUES"
                )
                values_step.alias = subquery["alias"]["name"]["value"]
                values_step.columns = tuple(
                    col["name"]["value"] for col in subquery["alias"]["columns"]
                )
                values_step.values = [
                    tuple(logical_planner_builders.build(value) for value in row["content"])
                    for row in subquery["subquery"]["body"]["Values"]["rows"]
                ]
                step_id = random_string()
                sub_plan.add_node(step_id, values_step)
                root_node = step_id
        else:  # pragma: no cover
            raise NotImplementedError(relation["relation"]["Derived"])

    elif relation["relation"]["Table"]["args"]:
        # If we have args, we're a function dataset (like UNNEST)
        function = relation["relation"]["Table"]
        function_name = relation_name.upper()

        # READ_JSONL/READ_PARQUET/READ_CSV derive their column names from the file's
        # schema at bind time (not yet implemented), so unlike UNNEST/VALUES/
        # GENERATE_SERIES they don't require an AS alias(columns) clause to name
        # their output columns.
        requires_column_alias = function_name not in ("READ_JSONL", "READ_PARQUET", "READ_CSV")

        if function["alias"] is None and requires_column_alias:
            from opteryx.exceptions import UnnamedColumnError

            raise UnnamedColumnError(
                f"Column or Relation created by {function_name} has no name, use AS to give it a name."
            )

        function_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function=function_name
        )
        if function_name == "UNNEST":
            function_step.alias = f"$unnest-{random_string(6)}"
            function_step.relation = function_step.alias
            function_step.unnest_target = function["alias"]["name"]["value"]
        elif function["alias"] is not None:
            function_step.alias = function["alias"]["name"]["value"]

        args = []
        named_args = {}
        for arg in function["args"]["args"]:
            if "Named" in arg:
                named = arg["Named"]
                named_args[named["name"]["value"]] = logical_planner_builders.build(named["arg"])
            else:
                args.append(logical_planner_builders.build(arg))
        function_step.args = args
        function_step.named_args = named_args
        if function["alias"] is not None:
            function_step.columns = tuple(
                col["name"]["value"] for col in function["alias"]["columns"]
            )

        step_id = random_string()
        sub_plan.add_node(step_id, function_step)
        root_node = step_id
        relation["step_id"] = step_id
    else:
        # SCAN nodes are where we read relations; these can be from memory, disk or a remote
        # system. This has many physical implementations but at this point all we have is the
        # name/location of the relation (relation), what the relation is called inside the
        # query (alias) and if there are any hints (hints)
        from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        table = relation["relation"]["Table"]
        from_step.relation = relation_name
        from_step.alias = (
            from_step.relation if table["alias"] is None else table["alias"]["name"]["value"]
        )
        from_step.hints = [hint["Identifier"]["value"] for hint in table["with_hints"]]

        # Extract and validate AT / VERSION clause if present
        version_clause = table.get("version")
        if version_clause is not None:
            # Three forms, three destinations, decided once: a TAG names a
            # snapshot the catalog resolves by name, VERSION AS OF names one
            # directly, and everything else is a point in time. The tag test
            # comes first because its clause arrives as `AT(TAG => ...)` - the
            # rewriter's carrier for a name the VERSION AS OF grammar cannot
            # hold - and would otherwise fall through to the timestamp arm and
            # be reported as a bad date.
            if logical_planner_builders.is_tag_clause(version_clause):
                from_step.version_tag = logical_planner_builders.extract_timetravel_tag(
                    version_clause
                )
            elif logical_planner_builders.is_version_as_of_clause(version_clause):
                from_step.version = logical_planner_builders.extract_timetravel_version(
                    version_clause
                )
            else:
                from_step.at_date = logical_planner_builders.extract_timetravel_timestamp(
                    version_clause
                )

        step_id = random_string()
        sub_plan.add_node(step_id, from_step)

        root_node = step_id
        relation["step_id"] = step_id

    # joins
    _joins = relation.get("joins", [])
    for join in _joins:
        # this is the convention: select * from LEFT join RIGHT

        join_step = process_join_tree(join)

        if join_step.node_type == LogicalPlanStepType.Unnest:
            # UNNEST joins don't have a LEFT and RIGHT side
            join_step_id = random_string()
            sub_plan.add_node(join_step_id, join_step)
            sub_plan.add_edge(root_node, join_step_id, "left")
            root_node = join_step_id
            continue

        right_node_id, right_plan = create_node_relation(join)

        # add the left and right relation names - we sometimes need these later
        join_step.left_relation_names = get_subplan_schemas(sub_plan)
        join_step.left_readers = get_subplan_reads(sub_plan)
        join_step.right_relation_names = get_subplan_schemas(right_plan)
        join_step.right_readers = get_subplan_reads(right_plan)

        # add the right side of the join
        sub_plan += right_plan

        join_step_id = random_string()
        sub_plan.add_node(join_step_id, join_step)

        # add the from table as the left side of the join
        sub_plan.add_edge(root_node, join_step_id, "left")
        sub_plan.add_edge(right_node_id, join_step_id, "right")

        root_node = join_step_id

    return root_node, sub_plan


def plan_explain(statement, **kwargs) -> LogicalPlan:
    plan = LogicalPlan()
    explain_node = LogicalPlanNode(node_type=LogicalPlanStepType.Explain)
    explain_node.analyze = statement["Explain"]["analyze"]
    explain_format = statement["Explain"].get("format")

    if explain_format is None:
        explain_node.format = "TEXT"
    else:
        explain_node.format = explain_format.get("Keyword", "TEXT").upper()
    # GRAPHVIZ is the carrier token, not a request: the parser has no MERMAID
    # keyword, so sql_rewriter.rewrite_explain sends MERMAID through as GRAPHVIZ
    # (and rejects a genuine FORMAT GRAPHVIZ before parsing). This maps it back.
    if explain_node.format == "GRAPHVIZ":
        explain_node.format = "MERMAID"

    explain_id = random_string()
    plan.add_node(explain_id, explain_node)

    # The explained statement is not necessarily a SELECT. Dispatch it through
    # the same builder table the top level uses rather than assuming plan_query:
    # `EXPLAIN INSERT ...` and `EXPLAIN MERGE ...` have no query `body` to read
    # and used to die on a raw KeyError deep inside plan_query, naming nothing.
    inner = statement["Explain"]["statement"]
    inner_root = next(iter(inner))
    if explain_node.analyze and inner_root != "Query":
        # ANALYZE gets its numbers by RUNNING the statement. For anything that
        # writes, that would make an EXPLAIN mutate the table - a surprise no
        # plan-inspection command should be able to spring. Refused rather than
        # silently executed, and rather than silently reporting nothing.
        raise UnsupportedSyntaxError(
            f"**EXPLAIN ANALYZE** cannot be used on a **{inner_root.upper()}** "
            "statement: ANALYZE measures a statement by running it, and this one "
            "writes. Use **EXPLAIN** without **ANALYZE** to see the plan."
        )
    builder = QUERY_BUILDERS.get(inner_root)
    if builder is None:
        raise UnsupportedSyntaxError(
            f"**EXPLAIN** does not support **{inner_root.upper()}** statements."
        )
    sub_plan = builder(inner)
    sub_plan_id = sub_plan.get_exit_points()[0]
    plan += sub_plan
    plan.add_edge(sub_plan_id, explain_id)

    return plan


def plan_query(statement: dict) -> LogicalPlan:
    """ """

    root_node = statement
    if "Query" in root_node:
        root_node = root_node["Query"]

    # set operations (UNION, INTERSECT, EXCEPT)
    if "SetOperation" in root_node["body"]:
        set_operation = root_node["body"]["SetOperation"]

        op_type = set_operation["op"]
        if op_type == "Union":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
        elif op_type == "Intersect":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Intersect)
        elif op_type == "Except":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Except)
        else:
            raise UnsupportedSyntaxError(f"Unsupported SET operator '{op_type}'. **UNION**, **UNION ALL**, **EXCEPT** and **INTERSECT** are the supported forms.")

        set_op_node.modifier = (
            None if set_operation["set_quantifier"] == "None" else set_operation["set_quantifier"]
        )
        step_id = random_string()
        plan = LogicalPlan()
        plan.add_node(step_id, set_op_node)
        head_nid = step_id

        left_plan = inner_query_planner(set_operation["left"])
        from opteryx.planner.relation_resolver import UNION_ALIAS_PREFIX
        from opteryx.planner.relation_resolver import rename_relations

        left_plan = rename_relations(left_plan, prefix=UNION_ALIAS_PREFIX)
        plan += left_plan
        subquery_entry_id = left_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        right_plan = inner_query_planner(set_operation["right"])

        # Both sides must present the same number of columns. The binder checks this
        # too (binder/set_ops.py `_validate_set_operation_types`), but only UNION ever
        # reaches it: plan_rewriter rewrites non-wildcard INTERSECT/EXCEPT into semi-
        # and anti-joins BEFORE binding, and that rewrite builds its ON condition from
        # the LEFT side's column names alone — so a wider right side simply had its
        # extra columns ignored, and `SELECT id FROM $planets INTERSECT SELECT id, name
        # FROM $planets` answered 9 rows instead of refusing.
        #
        # Checking here rather than in the two rewrite strategies keeps ONE rule for
        # all three operators, and puts it where the node still exists whatever runs
        # later. The binder's check is not redundant: it is the only one that can count
        # a WILDCARD leg, which is exactly what this cannot (see
        # `_set_operation_leg_arity` — None is "ask the binder", not zero).
        left_arity = _set_operation_leg_arity(left_plan)
        right_arity = _set_operation_leg_arity(right_plan)
        if left_arity is not None and right_arity is not None and left_arity != right_arity:
            # Same class and wording as the binder's check, so one rule reads as one
            # rule no matter which phase catches it.
            raise ValueError(
                f"{op_type.upper()}: column count mismatch — left has {left_arity}, right has {right_arity}"
            )

        right_plan = rename_relations(right_plan, prefix=UNION_ALIAS_PREFIX)
        plan += right_plan
        subquery_entry_id = right_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        # UNION ALL
        if set_op_node.modifier != "All":
            distinct = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
            head_nid, step_id = step_id, random_string()
            plan.add_node(step_id, distinct)
            plan.add_edge(head_nid, step_id)

        # limit/offset
        if root_node.get("limit_clause"):
            _limit = root_node["limit_clause"].get("LimitOffset", {}).get("limit")
            _offset = root_node["limit_clause"].get("LimitOffset", {}).get("offset")

            if _offset:
                _offset = _offset.get("value")
            if _limit or _offset:
                limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
                limit_step.limit = (
                    None if _limit is None else logical_planner_builders.build(_limit).value
                )
                limit_step.offset = (
                    None if _offset is None else logical_planner_builders.build(_offset).value
                )
                head_nid, step_id = step_id, random_string()
                plan.add_node(step_id, limit_step)
                if head_nid is not None:
                    plan.add_edge(head_nid, step_id)

        # add the exit node
        exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
        # A set operation's output shape is its LEFT leg's — taken from that leg's own
        # EXIT node, which is what the leg declares it projects. This used to take the
        # first Project node found in the leg's GRAPH, which is a different node as
        # soon as the leg contains a subquery: `SELECT id FROM (SELECT id, name FROM
        # $planets) AS x UNION ALL ...` picked the subquery's two columns for a leg
        # that projects one, and the query died at compile time with "a UNION leg
        # narrower than the union schema" — the union's own declaration blamed on the
        # leg. See `_set_operation_leg_columns`.
        #
        # A BARE wildcard: `value` MUST be None. A non-None `value` marks a QUALIFIED
        # wildcard (`rel.*`) and binder.visit_exit then expands only columns whose
        # origin matches `value[0]` — `(None,)` matches no relation, so the EXIT bound
        # to zero columns and the set operation failed with that same misleading error.
        # Reached when the left leg declares no columns at all.
        columns = _set_operation_leg_columns(left_plan) or [LogicalPlanNode(NodeType.WILDCARD)]
        exit_node.columns = columns
        head_nid, step_id = step_id, random_string()
        plan.add_node(step_id, exit_node)
        if head_nid is not None:
            plan.add_edge(head_nid, step_id)

        set_op_node.columns = columns
        set_op_node.left_relation_names = get_subplan_schemas(left_plan)
        set_op_node.right_relation_names = get_subplan_schemas(right_plan)

        return plan

    # we do some minor AST rewriting
    if root_node.get("limit_clause"):
        root_node["body"]["limit"] = root_node["limit_clause"].get("LimitOffset", {}).get("limit")
        root_node["body"]["offset"] = root_node["limit_clause"].get("LimitOffset", {}).get("offset")
    root_node["body"]["order_by"] = root_node.get("order_by", None)

    planned_query = inner_query_planner(root_node["body"])

    # DEBUG: print("LOGICAL PLAN")
    # DEBUG: print(planned_query.draw())

    return planned_query


def plan_set_variable(statement, **kwargs):
    root_node = "SingleAssignment"
    statement = statement["Set"]
    plan = LogicalPlan()
    set_step = LogicalPlanNode(
        node_type=LogicalPlanStepType.Set,
        variable=extract_variable(statement[root_node]["variable"]),
        value=extract_value(statement[root_node]["values"]),
    )
    plan.add_node(random_string(), set_step)
    return plan


def plan_show_columns(statement, **kwargs):
    root_node = "ShowColumns"
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    table = statement[root_node]["show_options"]["show_in"]["parent_name"]
    from_step.relation = ".".join(part["Identifier"]["value"] for part in table)
    from_step.alias = from_step.relation
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowColumns)
    show_step.extended = statement[root_node]["extended"]
    show_step.full = statement[root_node]["full"]
    show_step.relation = from_step.relation
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    _filter = statement[root_node]["show_options"].get("filter_position")
    if _filter:
        _filter = _filter["Suffix"]
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.condition = extract_simple_filter(_filter, "name")
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, filter_node)
        plan.add_edge(previous_step_id, step_id)
        raise UnsupportedSyntaxError(
            compose(
                f"{md_syntax('SHOW COLUMNS')} cannot be filtered with a "
                f"{md_syntax('WHERE')} clause",
                f"List the columns with {md_syntax('SHOW COLUMNS FROM')} and the table "
                f"name, then filter the result by wrapping it as a subquery",
            )
        )

    return plan


def _plan_virtual_dataset_scan(relation: str, internal_relation: bool) -> LogicalPlan:
    """`SELECT * FROM <virtual dataset>`, built by the planner rather than typed.

    Shared by the `SHOW` forms that are just a wildcard read of a virtual dataset,
    so `SHOW VARIABLES` and `SHOW USER` cannot drift into producing different plan
    shapes for the same job.
    """
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = relation
    from_step.alias = relation
    # For a relation in INTERNAL_ONLY_DATASETS, binder.visit_scan rejects the scan
    # unless this flag marks it as planner-built rather than user-typed.
    from_step.internal_relation = internal_relation
    step_id = random_string()
    plan.add_node(step_id, from_step)

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    # A BARE wildcard: `value` must be None. A non-None `value` marks a QUALIFIED
    # wildcard (`rel.*`) and binder.visit_exit then expands only columns whose
    # origin matches `value[0]` — so `(None,)` silently expands to nothing.
    exit_node.columns = [LogicalPlanNode(NodeType.WILDCARD)]
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, exit_node)
    plan.add_edge(previous_step_id, step_id)

    return plan


def _plan_show_manifest(table_name: str) -> LogicalPlan:
    """`SHOW MANIFEST FOR <table>` — Scan (bound for permission/manifest loading
    only, never read) -> ShowManifest (materializes the already-bound Manifest).

    No Exit node: this mirrors plan_show_columns exactly, not
    _plan_virtual_dataset_scan — ShowManifest is a non-pipeline special op
    answered directly from binder-attached metadata (serial_engine.py calls
    `head_node(None)`), not a normal Scan of real row data run through the
    native pipeline. An Exit node here would make Exit the plan's head, which
    is not a special op, and misroute the whole query into the native
    compiler, which has no operator for ShowManifestNode.

    `SHOW` has no WHERE/column-list grammar in the first place, so there is
    no filter/projection this builder needs to guard against.
    """
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = table_name
    from_step.alias = table_name
    from_step.hints = []
    # binder.visit_scan reads this to (a) additionally require the owner-only
    # MANIFEST permission beside the normal READ gate and (b) never compile a
    # real file scan for this Scan — the bound Manifest IS the answer.
    from_step.for_manifest_only = True
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowManifest)
    show_step.relation = table_name
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    return plan


def _plan_show_snapshots(table_name: str) -> LogicalPlan:
    """`SHOW SNAPSHOTS FOR <table>` — Scan (bound for the commit history only,
    never read) -> ShowSnapshots.

    Same plan shape and the same no-Exit-node reasoning as _plan_show_manifest
    above: a special op answered from what the binder attached, kept off the
    native compiler, which has no operator for ShowSnapshotsNode.

    It differs from SHOW MANIFEST in what the Scan below is asked for. The
    Manifest is already loaded by an ordinary bind, so that statement consumes
    state the Scan would have produced anyway; a relation's snapshot history is
    NOT, and `for_snapshots_only` makes the connector fetch it (a second catalog
    round trip) while skipping the manifest read the Scan would otherwise do.
    """
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = table_name
    from_step.alias = table_name
    from_step.hints = []
    # binder.visit_scan reads this to (a) load the commit history in place of the
    # Manifest and (b) never compile a real file scan for this Scan — the history
    # IS the answer. Unlike for_manifest_only it adds no permission beyond READ:
    # a snapshot row is commit metadata about a relation the caller can already
    # read, and exposes no file paths or storage layout.
    from_step.for_snapshots_only = True
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowSnapshots)
    show_step.relation = table_name
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    return plan


def _plan_show_triggers(table_name: str) -> LogicalPlan:
    """`SHOW TRIGGERS FOR <table>` — desugars to
    `SELECT * FROM <workspace>.information_schema.triggers
     WHERE event_object_table = '<collection.table>'`.

    Not a virtual dataset like SHOW USER/GRANTS: those read only session
    variables, while triggers live in the workspace's catalog, which is only
    reachable through the workspace's connector — i.e. through an
    information_schema scan. The table name supplies the workspace, which is
    why the bare `SHOW TRIGGERS` form is rejected in plan_show_variables (the
    planner has no session default workspace to scan).

    The filter lands on `event_object_table`, a pushable key column of the
    triggers reader, so the predicate-pushdown pass turns it into skipped
    catalog round trips rather than a post-hoc row filter.
    """
    workspace, _, relative = table_name.partition(".")
    if not relative:
        raise UnsupportedSyntaxError(
            "`SHOW TRIGGERS FOR <table>` requires a workspace-qualified table "
            "name, e.g. `SHOW TRIGGERS FOR opteryx.test.pypi`."
        )
    relation = f"{workspace}.information_schema.triggers"

    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = relation
    from_step.alias = relation
    from_step.hints = []
    step_id = random_string()
    plan.add_node(step_id, from_step)

    filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    filter_node.condition = build_expression_tree(
        relation, [("event_object_table", "Eq", relative)]
    )
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, filter_node)
    plan.add_edge(previous_step_id, step_id)

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = [LogicalPlanNode(NodeType.WILDCARD)]
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, exit_node)
    plan.add_edge(previous_step_id, step_id)

    return plan


def plan_show_variables(statement, **kwargs):
    """SHOW VARIABLES, SHOW USER, SHOW GRANTS, SHOW MANIFEST FOR — planned from
    the parser's generic `ShowVariable` catch-all.

    The parser folds every bare `SHOW <words>` form it does not recognise as
    its own statement into a single `ShowVariable` node carrying the trailing
    words: `SHOW VARIABLES` gives an empty list, `SHOW USER` gives ["USER"],
    `SHOW TIME ZONE` gives ["TIME", "ZONE"], `SHOW MANIFEST FOR a.b.c` gives
    ["MANIFEST", "FOR", "a", "b", "c"] (the parser's `parse_identifiers` drops
    the `.` tokens between identifiers, not just whitespace, so a dotted name
    arrives as separate words to rejoin — see sqlparser's `parse_identifiers`).
    So this one builder is the front door for every form the parser did not
    recognise, and it has to name the ones that are ours.

    `SHOW VARIABLES LIKE '<pattern>'` parses to ["LIKE"] with the pattern
    DISCARDED by the parser, so it cannot be honoured here — it is rejected
    rather than silently answered with the unfiltered list, which would be a
    wrong answer wearing the shape of a right one.
    """
    root_node = "ShowVariable"

    parts = statement[root_node]["variable"]
    words = [part["value"].upper() for part in parts]
    if not words:
        # `$variables` is INTERNAL_ONLY_DATASETS: SHOW VARIABLES is its only surface.
        return _plan_virtual_dataset_scan("$variables", internal_relation=True)
    if words == ["USER"]:
        # `$user` is INTERNAL_ONLY_DATASETS on the same rule as `$variables`:
        # SHOW USER is its only surface.
        return _plan_virtual_dataset_scan("$user", internal_relation=True)
    if words == ["GRANTS"]:
        # `$grants` is INTERNAL_ONLY_DATASETS on the same rule: SHOW GRANTS is
        # its only surface. It reports the session's own policies and confers
        # nothing — Opteryx has no GRANT/REVOKE.
        return _plan_virtual_dataset_scan("$grants", internal_relation=True)
    if words == ["LIKE"]:
        # The parser discards the pattern, so we cannot apply it.
        raise UnsupportedSyntaxError(
            "Opteryx does not support `SHOW VARIABLES LIKE`; use `SHOW VARIABLES`."
        )
    if words[0] == "MANIFEST":
        if len(words) < 3 or words[1] != "FOR":
            raise UnsupportedSyntaxError(
                "`SHOW MANIFEST FOR <table>` requires a table name, e.g. "
                "`SHOW MANIFEST FOR opteryx.test.pypi`."
            )
        # Original case preserved for the table's identifier segments -- only
        # the MANIFEST/FOR control words above were matched via the uppercased
        # `words` list; catalog/schema/table names are case-sensitive.
        table_name = ".".join(part["value"] for part in parts[2:])
        return _plan_show_manifest(table_name)
    if words[0] == "SNAPSHOTS":
        if len(words) < 3 or words[1] != "FOR":
            # Bare SHOW SNAPSHOTS has nothing to enumerate for the same reason
            # bare SHOW TRIGGERS does not: a commit history belongs to one
            # relation, and the planner has no session default workspace to
            # sweep for relations to list one for.
            raise UnsupportedSyntaxError(
                "`SHOW SNAPSHOTS FOR <table>` requires a table name, e.g. "
                "`SHOW SNAPSHOTS FOR opteryx.test.pypi`."
            )
        # Original case preserved, as for SHOW MANIFEST FOR above.
        table_name = ".".join(part["value"] for part in parts[2:])
        return _plan_show_snapshots(table_name)
    if words[0] == "TRIGGERS":
        if len(words) < 3 or words[1] != "FOR":
            # Bare SHOW TRIGGERS cannot be answered: triggers live in a
            # workspace's catalog and the planner has no session default
            # workspace, so there is nothing to enumerate. The FOR form names
            # the workspace via the table.
            raise UnsupportedSyntaxError(
                "`SHOW TRIGGERS` requires a table: `SHOW TRIGGERS FOR <table>`. "
                "To list every trigger in a workspace, query "
                "`SELECT * FROM <workspace>.information_schema.triggers`."
            )
        # Original case preserved, as for SHOW MANIFEST FOR above.
        table_name = ".".join(part["value"] for part in parts[2:])
        return _plan_show_triggers(table_name)
    raise UnsupportedSyntaxError(
        f"Opteryx does not support 'SHOW {' '.join(words)}'; "
        "supported forms are `SHOW VARIABLES`, `SHOW USER`, `SHOW GRANTS`, "
        "`SHOW TRIGGERS FOR <table>`, `SHOW MANIFEST FOR <table>`, and "
        "`SHOW SNAPSHOTS FOR <table>`."
    )


def plan_show_create_query(statement, **kwargs):
    root_node = "ShowCreate"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.Show)
    show_step.object_type = statement[root_node]["obj_type"].upper()
    if show_step.object_type != "VIEW":
        # Rejected here, by name, rather than at execution time: a table has no
        # stored CREATE statement to show (its schema is the catalog's, not a
        # statement we kept), so no amount of planning makes this answerable.
        raise UnsupportedSyntaxError(
            f"Opteryx does not support '**SHOW CREATE** {show_step.object_type}'; "
            "only `SHOW CREATE VIEW <view>` is supported."
        )
    show_step.object_name = extract_variable(statement[root_node]["obj_name"])
    if isinstance(show_step.object_name, list):
        show_step.object_name = ".".join(show_step.object_name)
    plan.add_node(random_string(), show_step)
    return plan


def plan_create_view(statement, **kwargs):
    """
    Create a logical plan for CREATE VIEW statement.

    CREATE VIEW view_name AS query

    Note: The query is stored as text, not planned. It will be planned
    when the view is referenced in a query.
    """
    root_node = "CreateView"
    plan = LogicalPlan()

    create_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateView)

    # Extract view name
    view_name_parts = statement[root_node]["name"]
    create_view_node.view_name = extract_variable(view_name_parts)
    if isinstance(create_view_node.view_name, list):
        create_view_node.view_name = ".".join(create_view_node.view_name)

    # Extract OR REPLACE flag
    create_view_node.or_replace = statement[root_node].get("or_replace", False)

    # CREATE MATERIALIZED VIEW is not a view at all: it is CTAS plus
    # registration. The SELECT executes now and its result is written as a
    # backing table; the defining query is stashed on the Insert node so the
    # insert operator can register the MV (defining SQL, source tables,
    # refresh triggers) at the end of its catalog mutation. Registration is
    # deliberately NOT a second plan node - the serial engine assumes
    # InsertNode is the plan head.
    if statement[root_node].get("materialized", False):
        if statement[root_node].get("if_not_exists", False):
            raise UnsupportedSyntaxError(
                "**CREATE MATERIALIZED VIEW** does not support IF NOT **EXISTS**; "
                "use **CREATE OR REPLACE** MATERIALIZED VIEW."
            )
        if statement[root_node].get("columns"):
            raise UnsupportedSyntaxError(
                "**CREATE MATERIALIZED VIEW** cannot specify column definitions; "
                "the columns come from the **SELECT**. Name them with **AS** in the **SELECT** instead."
            )
        return _plan_ctas(
            relation_name=create_view_node.view_name,
            if_not_exists=False,
            query_ast=statement[root_node]["query"],
            or_replace=create_view_node.or_replace,
            is_materialized_view=True,
        )

    # Extract columns (if specified)
    columns = statement[root_node].get("columns")
    if columns:
        cols = []
        for col in columns:
            # Accept several AST shapes for an identifier
            try:
                if isinstance(col, dict) and "Identifier" in col:
                    cols.append(col["Identifier"]["value"])
                elif isinstance(col, dict) and "name" in col:
                    name = col["name"]
                    if isinstance(name, dict) and "Identifier" in name:
                        cols.append(name["Identifier"]["value"])
                    elif isinstance(name, str):
                        cols.append(name)
                    else:
                        # fallback to the first string value in the dict
                        for v in col.values():
                            if isinstance(v, str):
                                cols.append(v)
                                break
                elif isinstance(col, str):
                    cols.append(col)
                else:
                    # generic fallback
                    cols.append(str(col))
            except Exception:
                raise KeyError("Unexpected column AST format in CREATE VIEW")
        create_view_node.columns = cols
    else:
        create_view_node.columns = None

    # Store the query as the AST - we'll need it to reconstruct the SQL or re-parse later
    create_view_node.query = statement[root_node]["query"]

    # Add the CreateView node
    plan.add_node(random_string(), create_view_node)

    return plan


def plan_alter_view(statement, **kwargs):
    """
    Create a logical plan for ALTER VIEW statement (UpdateView).

    ALTER VIEW view_name AS query

    Note: The query is stored as text, not planned. It will be planned
    when the view is referenced in a query.
    """
    root_node = "AlterView"
    plan = LogicalPlan()

    alter_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterView)

    # Extract view name
    view_name_parts = statement[root_node]["name"]
    alter_view_node.view_name = extract_variable(view_name_parts)
    if isinstance(alter_view_node.view_name, list):
        alter_view_node.view_name = ".".join(alter_view_node.view_name)

    # Extract columns (if specified)
    columns = statement[root_node].get("columns")
    if columns:
        cols = []
        for col in columns:
            try:
                if isinstance(col, dict) and "Identifier" in col:
                    cols.append(col["Identifier"]["value"])
                elif isinstance(col, dict) and "name" in col:
                    name = col["name"]
                    if isinstance(name, dict) and "Identifier" in name:
                        cols.append(name["Identifier"]["value"])
                    elif isinstance(name, str):
                        cols.append(name)
                    else:
                        for v in col.values():
                            if isinstance(v, str):
                                cols.append(v)
                                break
                elif isinstance(col, str):
                    cols.append(col)
                else:
                    cols.append(str(col))
            except Exception:
                raise KeyError("Unexpected column AST format in ALTER VIEW")
        alter_view_node.columns = cols
    else:
        alter_view_node.columns = None

    # Store the query as the AST - we'll need it to reconstruct the SQL or re-parse later
    alter_view_node.query = statement[root_node]["query"]

    # Add the AlterView node
    plan.add_node(random_string(), alter_view_node)

    return plan


def plan_alter_table(statement, **kwargs):
    """
    Create a logical plan for ALTER TABLE statement.

    ALTER TABLE [IF EXISTS] table_name CLUSTER BY (column [, column ...])
    ALTER TABLE [IF EXISTS] table_name RENAME TO new_table_name
    ALTER TABLE [IF EXISTS] table_name ADD COLUMN [IF NOT EXISTS] name type [DEFAULT <literal>]
    ALTER TABLE [IF EXISTS] table_name DROP COLUMN [IF EXISTS] name
    ALTER TABLE [IF EXISTS] table_name RENAME COLUMN old_name TO new_name
    ALTER TABLE [IF EXISTS] table_name ALTER COLUMN name TYPE type
    ALTER TABLE [IF EXISTS] table_name CREATE TAG name [AS OF VERSION id|CURRENT|PREVIOUS]
    ALTER TABLE [IF EXISTS] table_name DROP TAG name
    """
    root_node = "AlterTable"
    plan = LogicalPlan()

    alter_statement = statement[root_node]

    # Extract table name
    relation_name_parts = alter_statement["name"]
    relation_name = extract_variable(relation_name_parts)
    if isinstance(relation_name, list):
        relation_name = ".".join(relation_name)

    if_exists = alter_statement.get("if_exists", False)

    operations = alter_statement.get("operations") or []
    if len(operations) != 1:
        raise UnsupportedSyntaxError(
            "Opteryx only supports a single **ALTER TABLE** operation per statement. Split the changes into one statement each."
        )
    operation = operations[0]

    if "ClusterBy" in operation:
        cluster_columns = []
        for expr in operation["ClusterBy"]["exprs"]:
            if "Identifier" not in expr:
                raise UnsupportedSyntaxError(
                    "CLUSTER BY only supports column names, not expressions. Cluster on a column; if you need the expression, store it as a column first."
                )
            cluster_columns.append(expr["Identifier"]["value"])

        alter_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterRelation)
        alter_relation_node.relation_name = relation_name
        alter_relation_node.cluster_columns = cluster_columns
        alter_relation_node.if_exists = if_exists

        plan.add_node(random_string(), alter_relation_node)
        return plan

    if "RenameTable" in operation:
        new_name = extract_variable(operation["RenameTable"]["table_name"]["To"])
        if isinstance(new_name, list):
            new_name = ".".join(new_name)

        # A rename may move the relation between collections but never between
        # workspaces: the two would live in different catalogs, and moving data
        # across them is a copy, not a rename. Rejected here rather than left
        # for a connector to discover half way through.
        if relation_name.split(".", 1)[0] != new_name.split(".", 1)[0]:
            raise UnsupportedSyntaxError(
                f"RENAME TO cannot move a relation between workspaces "
                f"({relation_name} -> {new_name}); the workspace must be unchanged. Rename it within its own workspace."
            )
        if relation_name == new_name:
            raise UnsupportedSyntaxError(
                f"RENAME TO target is the same as the source ({relation_name}). Choose a different name for the target."
            )

        rename_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.RenameRelation)
        rename_relation_node.relation_name = relation_name
        rename_relation_node.new_relation_name = new_name
        rename_relation_node.if_exists = if_exists

        plan.add_node(random_string(), rename_relation_node)
        return plan

    if "AddColumn" in operation:
        from opteryx.exceptions import SqlError as _SqlError
        from opteryx.planner.logical_planner.logical_planner_builders import (
            build as build_expression,
        )
        from opteryx.planner.logical_planner.logical_planner_builders import (
            column_type_from_ast,
        )

        add_op = operation["AddColumn"]
        if add_op.get("column_position") is not None:
            raise UnsupportedSyntaxError(
                "**ALTER TABLE ... ADD COLUMN ... FIRST/AFTER** is not supported. A new column is always appended."
            )

        column_def = add_op["column_def"]
        column_name = column_def["name"]["value"]

        try:
            column_type = column_type_from_ast(column_def)
        except (_SqlError, ValueError) as err:
            raise UnsupportedSyntaxError(
                f"unsupported column type in **ALTER TABLE ... ADD COLUMN** for '{column_name}': {err}"
            ) from err

        col_nullable = True
        default_value = None
        for opt in column_def.get("options", []) or []:
            option = opt.get("option")
            if option == "NotNull":
                col_nullable = False
            elif option == "Null":
                continue
            elif isinstance(option, dict) and "Default" in option:
                default_expr = build_expression(option["Default"])
                if default_expr is None or default_expr.node_type != NodeType.LITERAL:
                    raise UnsupportedSyntaxError(
                        "**ALTER TABLE ... ADD COLUMN ... DEFAULT** only supports literal "
                        "values, not expressions - a default that isn't a constant would "
                        "need evaluating once per existing row, which this statement never does."
                    )
                default_value = default_expr.value
            else:
                raise UnsupportedSyntaxError(
                    f"**ALTER TABLE ... ADD COLUMN** does not support column option: {option}"
                )

        add_column_node = LogicalPlanNode(node_type=LogicalPlanStepType.AddColumn)
        add_column_node.relation_name = relation_name
        add_column_node.if_exists = if_exists
        add_column_node.column_name = column_name
        add_column_node.column_type = column_type
        add_column_node.nullable = col_nullable
        add_column_node.default = default_value
        add_column_node.if_not_exists = add_op.get("if_not_exists", False)

        plan.add_node(random_string(), add_column_node)
        return plan

    if "DropColumn" in operation:
        drop_op = operation["DropColumn"]
        drop_behavior = drop_op.get("drop_behavior")
        if drop_behavior is not None:
            raise UnsupportedSyntaxError(
                f"**ALTER TABLE ... DROP COLUMN ... {drop_behavior.upper()}** is not supported."
            )

        # The dialect's grammar only accepts one column per DROP COLUMN today, but
        # the AST always carries a list (`column_names`) - guard rather than assume,
        # so a future grammar change that allows `DROP COLUMN a, b` fails loud here
        # instead of silently dropping only the first name.
        column_names = drop_op.get("column_names") or []
        if len(column_names) != 1:
            raise UnsupportedSyntaxError(
                "**ALTER TABLE ... DROP COLUMN** supports a single column name. Split "
                "multiple drops into one statement each."
            )

        drop_column_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropColumn)
        drop_column_node.relation_name = relation_name
        drop_column_node.if_exists = if_exists
        drop_column_node.column_name = column_names[0]["value"]
        drop_column_node.column_if_exists = drop_op.get("if_exists", False)

        plan.add_node(random_string(), drop_column_node)
        return plan

    if "RenameColumn" in operation:
        rename_op = operation["RenameColumn"]

        rename_column_node = LogicalPlanNode(node_type=LogicalPlanStepType.RenameColumn)
        rename_column_node.relation_name = relation_name
        rename_column_node.if_exists = if_exists
        rename_column_node.column_name = rename_op["old_column_name"]["value"]
        rename_column_node.new_column_name = rename_op["new_column_name"]["value"]

        plan.add_node(random_string(), rename_column_node)
        return plan

    if "AlterColumn" in operation:
        from opteryx.exceptions import SqlError as _SqlError
        from opteryx.planner.logical_planner.logical_planner_builders import (
            column_type_from_ast,
        )

        alter_col_op = operation["AlterColumn"]
        column_name = alter_col_op["column_name"]["value"]
        column_op = alter_col_op.get("op") or {}
        # These three parse, and each would be silently inert if accepted.
        # Opteryx stores no column default to consult on a later INSERT and has
        # no NULL constraints: a DEFAULT is only ever the value ADD COLUMN
        # writes into the file for the rows that already exist. Refuse rather
        # than record state nothing reads.
        #
        # NOTE the two AST shapes - SetDefault arrives as a dict, DropDefault
        # and SetNotNull as bare strings - so a dict-only membership test would
        # catch the first and silently miss the other two.
        _INERT_COLUMN_OPS = {
            "SetDefault": "SET DEFAULT",
            "DropDefault": "DROP DEFAULT",
            "SetNotNull": "SET NOT NULL",
            "DropNotNull": "DROP NOT NULL",
        }
        op_key = column_op if isinstance(column_op, str) else next(iter(column_op), None)
        if op_key in _INERT_COLUMN_OPS:
            raise UnsupportedSyntaxError(
                f"**ALTER TABLE ... ALTER COLUMN ... {_INERT_COLUMN_OPS[op_key]}** is not "
                "supported. Opteryx honours no column defaults and enforces no NULL "
                "constraints - a DEFAULT is only the value **ADD COLUMN** writes into "
                "existing rows, so there is nothing for this to change."
            )
        if not isinstance(column_op, dict) or "SetDataType" not in column_op:
            raise UnsupportedSyntaxError(
                "Opteryx only supports '**ALTER TABLE** ... ALTER COLUMN ... TYPE ...'."
            )
        set_type_op = column_op["SetDataType"]
        if set_type_op.get("using") is not None:
            raise UnsupportedSyntaxError(
                "**ALTER TABLE ... ALTER COLUMN ... TYPE ... USING** is not supported. "
                "A supported type change is always a lossless widening, which never needs "
                "a transform expression."
            )

        try:
            new_column_type = column_type_from_ast(set_type_op)
        except (_SqlError, ValueError) as err:
            raise UnsupportedSyntaxError(
                f"unsupported column type in **ALTER TABLE ... ALTER COLUMN ... TYPE** for '{column_name}': {err}"
            ) from err

        alter_column_type_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterColumnType)
        alter_column_type_node.relation_name = relation_name
        alter_column_type_node.if_exists = if_exists
        alter_column_type_node.column_name = column_name
        alter_column_type_node.new_column_type = new_column_type

        plan.add_node(random_string(), alter_column_type_node)
        return plan

    if "SetTblProperties" in operation:
        return _plan_tag_ddl(
            operation["SetTblProperties"].get("table_properties") or [],
            relation_name,
            if_exists,
        )

    raise UnsupportedSyntaxError(
        "Opteryx only supports '**ALTER TABLE** ... CLUSTER BY (...)', "
        "'**ALTER TABLE** ... RENAME TO ...', '**ALTER TABLE** ... ADD COLUMN ...', "
        "'**ALTER TABLE** ... DROP COLUMN ...', '**ALTER TABLE** ... RENAME COLUMN ... TO ...', "
        "'**ALTER TABLE** ... ALTER COLUMN ... TYPE ...', "
        "'**ALTER TABLE** ... CREATE TAG ...' and '**ALTER TABLE** ... DROP TAG ...'."
    )


# Reserved property-key prefix. Keys under it are an INTERNAL transport between
# `OpteryxDialect::parse_statement` and this module, never a spelling a reader may
# use - see `_plan_tag_ddl`.
_RESERVED_PROPERTY_PREFIX = "__opteryx."
_TAG_ACTION_KEY = "__opteryx.tag.action"
_TAG_NAME_KEY = "__opteryx.tag.name"
_TAG_VERSION_KEY = "__opteryx.tag.version"


def _plan_tag_ddl(properties, relation_name: str, if_exists: bool):
    """`ALTER TABLE ... CREATE TAG` / `DROP TAG`, arriving as table properties.

    The dialect parses tag DDL itself - sqlparser has no grammar for it - but it
    cannot invent an AST node, so it hands the parsed result over inside
    `SetTblProperties` under reserved keys. This is the only reader of that
    transport.

    A statement the dialect built and one a reader typed by hand are told apart by
    the SHAPE of the key, not by trusting the prefix: the dialect emits an
    unquoted identifier containing dots, which reader text cannot produce (a bare
    key cannot contain a dot, and a quoted key arrives carrying its quote style).
    A reserved key that came from reader text is refused outright, so tag DDL has
    exactly one spelling rather than a documented one and a discoverable one.
    """
    values = {}
    for entry in properties:
        key_value = entry.get("KeyValue") if isinstance(entry, dict) else None
        if key_value is None:
            continue
        key = key_value["key"]
        name = key["value"]
        if not name.startswith(_RESERVED_PROPERTY_PREFIX):
            continue
        if key.get("quote_style") is not None:
            raise UnsupportedSyntaxError(
                f"'{name}' is a reserved internal property name and cannot be set. "
                "To tag a snapshot write '**ALTER TABLE** ... CREATE TAG ...'."
            )
        values[name] = key_value["value"]["Value"]["value"]["SingleQuotedString"]

    if _TAG_ACTION_KEY not in values:
        raise UnsupportedSyntaxError(
            "**ALTER TABLE ... SET TBLPROPERTIES** is not supported."
        )

    plan = LogicalPlan()
    if values[_TAG_ACTION_KEY] == "create":
        node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateTag)
        # Carried as text, not resolved here: CURRENT and PREVIOUS name a snapshot
        # the catalog has to be asked for, and a planner that resolved them would
        # be reading the catalog to build a plan that then reads it again.
        node.version_spec = values[_TAG_VERSION_KEY]
    else:
        node = LogicalPlanNode(node_type=LogicalPlanStepType.DropTag)

    node.relation_name = relation_name
    node.if_exists = if_exists
    node.tag_name = values[_TAG_NAME_KEY]

    plan.add_node(random_string(), node)
    return plan


def _parse_boolean_workspace_property(name: str, value):
    """ON/OFF/TRUE/FALSE -> bool, for a boolean-typed workspace property."""
    if isinstance(value, dict) and "Identifier" in value:
        token = value["Identifier"]["value"].upper()
        if token == "ON":
            return True
        if token == "OFF":
            return False
    if isinstance(value, dict) and "Value" in value:
        literal = value["Value"]["value"]
        if isinstance(literal, dict) and "Boolean" in literal:
            return literal["Boolean"]

    raise UnsupportedSyntaxError(
        f"Workspace property '{name}' is a boolean; use ON, OFF, TRUE or FALSE."
    )


# The workspace properties Opteryx will set, each with the parser that turns its
# AST value into the stored value. A property absent from this map is rejected at
# plan time - an unrecognised name must not be written through to the catalog,
# where a typo would silently become a new, meaningless property.
WORKSPACE_PROPERTIES = {
    "deletion_protection": _parse_boolean_workspace_property,
    # Refuses automated copies of this workspace's data INTO ANOTHER workspace
    # (CTAS, materialized-view refresh). Like deletion_protection it is ON unless
    # explicitly turned off, so this entry is what gives a workspace's owners
    # any way to opt out at all: without it `ALTER WORKSPACE ... SET
    # egress_protection` is rejected here as an unknown property, and the
    # catalog's default stands with no route to clear it.
    "egress_protection": _parse_boolean_workspace_property,
}


def plan_alter_workspace(statement, **kwargs):
    """
    Create a logical plan for ALTER WORKSPACE statement.

    ALTER WORKSPACE workspace SET property TO value

    The parser has no WORKSPACE object type, so the SQL rewriter
    (sql_rewriter.rewrite_alter_workspace) turns this into ALTER FUNCTION,
    which parses to the same `<name> SET <property> TO <value>` shape.
    """
    root_node = "AlterFunction"
    plan = LogicalPlan()

    alter_statement = statement[root_node]

    workspace_name = extract_variable(alter_statement["function"]["name"])
    if isinstance(workspace_name, list):
        workspace_name = ".".join(workspace_name)
    if "." in workspace_name:
        raise UnsupportedSyntaxError(
            f"ALTER WORKSPACE names a workspace, not a relation within one (got '{workspace_name}'). Give the workspace name on its own."
        )

    operation = alter_statement.get("operation") or {}
    actions = (operation.get("Actions") or {}).get("actions") or []
    if len(actions) != 1 or "Set" not in actions[0]:
        raise UnsupportedSyntaxError(
            "Opteryx only supports 'ALTER WORKSPACE <workspace> SET <property> TO <value>'."
        )

    action = actions[0]["Set"]

    property_name = extract_variable(action["name"])
    if isinstance(property_name, list):
        property_name = ".".join(property_name)
    property_name = property_name.lower()

    parser = WORKSPACE_PROPERTIES.get(property_name)
    if parser is None:
        supported = ", ".join(sorted(WORKSPACE_PROPERTIES))
        raise UnsupportedSyntaxError(
            f"'{property_name}' is not a settable workspace property. Supported: {supported}."
        )

    values = action["value"].get("Values") or []
    if len(values) != 1:
        raise UnsupportedSyntaxError(
            f"Workspace property '{property_name}' takes a single value. Give exactly one value."
        )

    alter_workspace_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterWorkspace)
    alter_workspace_node.workspace_name = workspace_name
    alter_workspace_node.property_name = property_name
    alter_workspace_node.property_value = parser(property_name, values[0])

    plan.add_node(random_string(), alter_workspace_node)

    return plan


def plan_create_collection(statement, **kwargs):
    """
    Create a logical plan for CREATE COLLECTION statement.

    CREATE COLLECTION [IF NOT EXISTS] [workspace].[collection]

    Rewritten to CREATE SCHEMA by the SQL rewriter
    (sql_rewriter.rewrite_create_collection) since the parser has no COLLECTION
    object type of its own. `CREATE SCHEMA` spelled directly lands here too -
    the same aliasing DROP SCHEMA already has for DROP COLLECTION.
    """
    root_node = "CreateSchema"
    plan = LogicalPlan()

    create_statement = statement[root_node]

    schema_name = create_statement["schema_name"]
    # The parser wraps the name; `Simple` is the only form a collection name can
    # take (an unqualified or dotted identifier).
    if not isinstance(schema_name, dict) or "Simple" not in schema_name:
        raise UnsupportedSyntaxError("CREATE COLLECTION expects a collection name. Write `CREATE COLLECTION <workspace>.<collection>`.")

    collection_name = extract_variable(schema_name["Simple"])
    if isinstance(collection_name, list):
        collection_name = ".".join(collection_name)

    # A collection lives inside exactly one workspace, so its name is always
    # `workspace.collection`. Rejected here rather than left for a connector to
    # discover, where a bare name would resolve to some default workspace and
    # silently create the collection somewhere the caller did not name.
    if collection_name.count(".") != 1:
        raise UnsupportedSyntaxError(
            f"CREATE COLLECTION names a collection as '<workspace>.<collection>' "
            f"(got '{collection_name}')."
        )

    create_collection_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateCollection)
    create_collection_node.collection_name = collection_name
    create_collection_node.if_not_exists = create_statement.get("if_not_exists", False)

    plan.add_node(random_string(), create_collection_node)

    return plan


def plan_drop(statement, **kwargs):
    """
    Create a logical plan for DROP statement (VIEW or TABLE).

    DROP VIEW [IF EXISTS] view_name
    DROP TABLE [IF EXISTS] table_name
    """
    root_node = "Drop"
    plan = LogicalPlan()

    drop_statement = statement[root_node]
    object_type = drop_statement.get("object_type")

    # The parser accepts these modifiers for every DROP form; Opteryx implements
    # none of them. Accepting them silently is worse than rejecting them: a
    # `DROP COLLECTION ... CASCADE` that quietly did not cascade reads as a
    # successful recursive drop, and `RESTRICT`/`PURGE` promise a guarantee about
    # dependants and storage reclamation that nothing here honours.
    for modifier in ("cascade", "restrict", "purge", "temporary"):
        if drop_statement.get(modifier, False):
            raise UnsupportedSyntaxError(
                f"Opteryx does not support `{modifier.upper()}` on DROP statements. Drop the object without the modifier."
            )

    if object_type == "View":
        # DROP VIEW path (unchanged)
        drop_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropView)

        # Extract view names (can drop multiple views)
        names = drop_statement["names"]
        view_names = []
        for name_parts in names:
            view_name = extract_variable(name_parts)
            if isinstance(view_name, list):
                view_name = ".".join(view_name)
            view_names.append(view_name)

        drop_view_node.view_names = view_names

        # Extract IF EXISTS flag
        drop_view_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_view_node)
        return plan

    elif object_type == "Table":
        # DROP TABLE path (new)
        drop_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropRelation)

        # Extract table names (can drop multiple tables)
        names = drop_statement["names"]
        relation_names = []
        for name_parts in names:
            relation_name = extract_variable(name_parts)
            if isinstance(relation_name, list):
                relation_name = ".".join(relation_name)
            relation_names.append(relation_name)

        drop_relation_node.relation_names = relation_names

        # Extract IF EXISTS flag
        drop_relation_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_relation_node)
        return plan

    elif object_type == "MaterializedView":
        # DROP MATERIALIZED VIEW - same node shape as DROP TABLE (the MV's
        # backing store is a dataset), flagged so execution routes to the
        # connector's MV drop (which also removes the refresh triggers from
        # every source dataset) and so the type guards can point a plain
        # DROP TABLE at the right statement, and vice versa.
        drop_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropRelation)

        names = drop_statement["names"]
        relation_names = []
        for name_parts in names:
            relation_name = extract_variable(name_parts)
            if isinstance(relation_name, list):
                relation_name = ".".join(relation_name)
            relation_names.append(relation_name)

        drop_relation_node.relation_names = relation_names
        drop_relation_node.is_materialized_view = True
        drop_relation_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_relation_node)
        return plan

    elif object_type == "Schema":
        # DROP COLLECTION path — rewritten to DROP SCHEMA by the SQL rewriter
        # (sql_rewriter.rewrite_drop_collection) since the parser has no
        # COLLECTION object type of its own.
        drop_collection_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropCollection)

        names = drop_statement["names"]
        collection_names = []
        for name_parts in names:
            collection_name = extract_variable(name_parts)
            if isinstance(collection_name, list):
                collection_name = ".".join(collection_name)
            collection_names.append(collection_name)

        drop_collection_node.collection_names = collection_names
        drop_collection_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_collection_node)
        return plan

    else:
        raise UnsupportedSyntaxError(f"DROP {object_type} is not supported")


def plan_drop_workspace(statement, **kwargs):
    """
    Create a logical plan for DROP WORKSPACE statement.

    DROP WORKSPACE [IF EXISTS] workspace

    The parser has no WORKSPACE object type, so the SQL rewriter
    (sql_rewriter._rewrite_object_types) turns this into DROP FUNCTION -
    same trick ALTER WORKSPACE uses via ALTER FUNCTION (see
    plan_alter_workspace). Unlike DROP TABLE/VIEW/SCHEMA, DROP FUNCTION is
    its own top-level statement shape in the parser's AST (`DropFunction`,
    not the generic `Drop` object_type dispatch plan_drop handles), keyed by
    `func_desc` (a list of `{name, args}`) rather than `names` - hence a
    dedicated plan function instead of another branch in plan_drop.
    """
    root_node = "DropFunction"
    plan = LogicalPlan()

    drop_statement = statement[root_node]

    func_descs = drop_statement.get("func_desc") or []
    if len(func_descs) != 1:
        raise UnsupportedSyntaxError("DROP WORKSPACE takes exactly one workspace name.")

    workspace_name = extract_variable(func_descs[0]["name"])
    if isinstance(workspace_name, list):
        workspace_name = ".".join(workspace_name)
    if "." in workspace_name:
        raise UnsupportedSyntaxError(
            f"DROP WORKSPACE names a workspace, not a relation within one (got '{workspace_name}'). Give the workspace name on its own."
        )

    drop_workspace_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropWorkspace)
    drop_workspace_node.workspace_name = workspace_name
    drop_workspace_node.if_exists = drop_statement.get("if_exists", False)

    plan.add_node(random_string(), drop_workspace_node)

    return plan


def plan_refresh_materialized_view(statement, **kwargs):
    """Plan REFRESH MATERIALIZED VIEW <name>.

    Desugars to the view's own defining SELECT written back over its backing
    table - which is exactly what a refresh is. Reusing `_plan_ctas` rather than
    building a bespoke operator means a refresh inherits the CoRTAS write path
    unchanged: files written durably first, then a single
    `truncate_and_add_files` snapshot commit, so a refresh is atomic and a
    failed one leaves the previous contents in place.

    What changes versus the old arrangement is who says it. The refresh used to
    BE a `CREATE OR REPLACE TABLE ... AS`, composed by whoever fired it; now
    that statement is an internal detail of this one, and a user-written CTAS
    is refused against a materialized view (see `_reject_materialized_view_target`).
    The statement names the intent, and the intent is the only route in.

    The definition is read here, at plan time, rather than carried on the
    statement: a refresh runs the view's CURRENT definition, so redefining a
    view takes effect on its next refresh rather than at some later moment
    nobody can point to.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.third_party import sqloxide

    relation_name = statement["RefreshMaterializedView"]["name"]

    connector = connector_factory(relation_name, telemetry=None)
    if not isinstance(connector, Writable) or not connector.is_materialized_view(relation_name):
        raise UnsupportedSyntaxError(
            f"{relation_name} is not a materialized view; **REFRESH MATERIALIZED VIEW** "
            "only refreshes materialized views. A table's contents are changed with "
            "**INSERT** or **CREATE OR REPLACE TABLE**."
        )

    try:
        defining_sql = connector.materialized_view_definition(relation_name)
    except ValueError as exc:
        raise UnsupportedSyntaxError(str(exc)) from exc

    parsed = sqloxide.parse_sql(defining_sql, _dialect="opteryx")
    if len(parsed) != 1 or "Query" not in parsed[0]:
        raise UnsupportedSyntaxError(
            f"the recorded definition of materialized view {relation_name} is not a "
            "single **SELECT**; it cannot be refreshed."
        )

    return _plan_ctas(
        relation_name,
        if_not_exists=False,
        query_ast=parsed[0],
        or_replace=True,
        is_refresh=True,
    )


def _plan_ctas(
    relation_name,
    if_not_exists,
    query_ast,
    or_replace=False,
    is_materialized_view=False,
    is_refresh=False,
):
    """Plan CREATE TABLE ... AS SELECT.

    Builds: SELECT subtree (Exit-headed, kept - not stripped) → InsertNode
    (create_target=True). Mirrors plan_insert's SELECT-source branch: keeping
    the Exit node is what makes serial_engine.py route this onto execute_native
    (the native engine) instead of the legacy push-pipeline, which cannot drive
    an InsertNode sink attached anywhere else. Target schema is derived at bind
    time from the SELECT's exit columns.
    """
    plan = LogicalPlan()

    # Snapshot the defining query BEFORE planning it - plan_query annotates
    # the AST dicts in place, and sqloxide.ast_to_sql rejects the mutated
    # shape when the insert operator re-renders it at registration time.
    defining_query = copy.deepcopy(query_ast) if is_materialized_view else None

    source_plan = plan_query(query_ast)
    exit_node_id = source_plan.get_exit_points()[0]
    plan += source_plan
    source_tail_id = exit_node_id

    insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
    insert_step.relation_name = relation_name
    insert_step.values_feeder = None
    insert_step.source_tail_id = source_tail_id
    insert_step.explicit_columns = None
    insert_step.create_target = True
    insert_step.if_not_exists = if_not_exists
    insert_step.or_replace = or_replace
    insert_step.is_materialized_view = is_materialized_view
    # Marks the one write that is ALLOWED to target an existing materialized
    # view. Every other route to writing one is refused at bind time, so this
    # flag is the whole difference between a sanctioned refresh and a CTAS that
    # would quietly turn a view into a table.
    insert_step.is_refresh = is_refresh
    if is_materialized_view:
        # The defining query, kept as AST so the insert operator can re-render
        # it to SQL (sqloxide.ast_to_sql) for the catalog registration - the
        # same trick view_management uses for CREATE VIEW.
        insert_step.defining_query = defining_query

    insert_id = random_string()
    plan.add_node(insert_id, insert_step)
    plan.add_edge(source_tail_id, insert_id)
    return plan


def plan_create_table(statement, **kwargs):
    """
    Create a logical plan for CREATE TABLE statement.

    CREATE TABLE [IF NOT EXISTS] table_name (
        column_name column_type [NOT NULL],
        ...
    )

    Maps sqloxide column types to LogicalCategory and constructs a RelationSchema.
    """
    from opteryx.types.schema import RelationSchema, SchemaColumn

    root_node = "CreateTable"
    plan = LogicalPlan()

    create_table_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateRelation)

    # Extract table name
    table_name_parts = statement[root_node]["name"]
    create_table_node.relation_name = extract_variable(table_name_parts)
    if isinstance(create_table_node.relation_name, list):
        create_table_node.relation_name = ".".join(create_table_node.relation_name)

    # Extract IF NOT EXISTS flag
    create_table_node.if_not_exists = statement[root_node].get("if_not_exists", False)

    # CTAS path
    query_ast = statement[root_node].get("query")
    if query_ast is not None:
        # Check for unsupported options (or_replace is handled below, CTAS-only)
        for option in ["external", "temporary", "transient", "volatile", "iceberg"]:
            if statement[root_node].get(option):
                raise UnsupportedSyntaxError(f"**CREATE TABLE** option not supported: {option}")
        column_defs = statement[root_node].get("columns", [])
        if column_defs:
            raise UnsupportedSyntaxError("**CREATE TABLE** AS **SELECT** cannot specify column definitions. The column names and types come from the **SELECT**.")
        return _plan_ctas(
            relation_name=create_table_node.relation_name,
            if_not_exists=create_table_node.if_not_exists,
            query_ast=query_ast,
            or_replace=statement[root_node].get("or_replace", False),
        )

    # Check for unsupported options (plain CREATE TABLE form — or_replace not supported here)
    for option in ["or_replace", "external", "temporary", "transient", "volatile", "iceberg"]:
        if statement[root_node].get(option):
            raise UnsupportedSyntaxError(f"**CREATE TABLE** option not supported: {option}")

    # Parse columns
    columns = []
    column_defs = statement[root_node].get("columns", [])
    if not column_defs:
        raise UnsupportedSyntaxError("**CREATE TABLE** requires at least one column. List at least one column with its type.")

    for col_def in column_defs:
        col_name = col_def["name"]["value"]

        # ONE vocabulary: a declared column type resolves exactly as a CAST target
        # does (logical_planner_builders.column_type_from_ast). This used to be a
        # hand-written sqlparser-key → name map living only here, which is how DDL
        # came to reject NVARCHAR, VARBINARY, DECIMAL, TIME, INTERVAL, IPV4,
        # TIMESTAMP[unit] and every exact integer width — while silently widening
        # TINYINT/SMALLINT to INTEGER and REAL to DOUBLE (§14).
        from opteryx.planner.logical_planner.logical_planner_builders import (
            column_type_from_ast,
        )

        from opteryx.exceptions import SqlError as _SqlError

        try:
            sql_type_ct = column_type_from_ast(col_def)
        except (_SqlError, ValueError) as err:
            raise UnsupportedSyntaxError(
                f"unsupported column type in **CREATE TABLE** for '{col_name}': {err}"
            ) from err

        # Check for NOT NULL constraint
        col_nullable = True
        col_options = col_def.get("options", [])
        if col_options:
            for opt in col_options:
                if isinstance(opt, dict) and opt.get("option") == "NotNull":
                    col_nullable = False
                    break

        # Create SchemaColumn
        from opteryx.types.schema import mint_column_identity

        flat_col = SchemaColumn(
            name=col_name,
            column_type=sql_type_ct,
            nullable=col_nullable,
            identity=mint_column_identity("$create", col_name),
        )
        columns.append(flat_col)

    create_table_node.columns = columns

    # Build RelationSchema
    schema = RelationSchema(name=create_table_node.relation_name, columns=columns)
    create_table_node.schema = schema

    plan.add_node(random_string(), create_table_node)
    return plan


def plan_truncate(statement, **kwargs):
    """
    Create a logical plan for TRUNCATE TABLE statement.

    TRUNCATE [TABLE] table_name [IF EXISTS]
    """
    root = "Truncate"
    truncate_stmt = statement[root]

    if not truncate_stmt.get("table"):
        raise UnsupportedSyntaxError("**TRUNCATE** without TABLE keyword is not supported. Write `TRUNCATE TABLE <table>`.")

    table_names = truncate_stmt.get("table_names", [])
    if len(table_names) != 1:
        raise UnsupportedSyntaxError("**TRUNCATE** supports a single table name. Truncate one table per statement.")

    # Extract table name
    name_parts = table_names[0].get("name", [])
    relation_name = ".".join(p["Identifier"]["value"] for p in name_parts)

    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.TruncateRelation)
    node.relation_name = relation_name
    node.if_exists = truncate_stmt.get("if_exists", False)

    plan.add_node(random_string(), node)
    return plan


def plan_optimize_table(statement, **kwargs):
    """
    Create a logical plan for OPTIMIZE statement.

    OPTIMIZE TABLE table_name

    Opteryx only supports this exact form: the TABLE keyword is required
    (unlike Databricks, which allows `OPTIMIZE table_name`), and strategy is
    auto-detected from the table's stored CLUSTER BY / sort order, same as
    the scheduled compaction job. ClickHouse's ON CLUSTER/PARTITION/FINAL/
    DEDUPLICATE and Databricks's WHERE/ZORDER BY are all parsed by the
    grammar but have no equivalent here yet, so they are rejected rather
    than silently ignored.
    """
    root = "OptimizeTable"
    optimize_stmt = statement[root]

    if not optimize_stmt.get("has_table_keyword"):
        raise UnsupportedSyntaxError(
            "**OPTIMIZE** without the TABLE keyword is not supported. Write `OPTIMIZE TABLE <table>`."
        )

    for clause, label in (
        ("on_cluster", "ON CLUSTER"),
        ("partition", "PARTITION"),
        ("include_final", "FINAL"),
        ("deduplicate", "DEDUPLICATE"),
        ("predicate", "WHERE"),
        ("zorder", "ZORDER BY"),
    ):
        if optimize_stmt.get(clause):
            raise UnsupportedSyntaxError(
                f"**OPTIMIZE** does not support {label}. Only `OPTIMIZE TABLE <table>` is supported."
            )

    relation_name = extract_variable(optimize_stmt["name"])
    if isinstance(relation_name, list):
        relation_name = ".".join(relation_name)

    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.OptimizeRelation)
    node.relation_name = relation_name

    plan.add_node(random_string(), node)
    return plan


def plan_insert(statement, **kwargs):
    """
    Create a logical plan for INSERT statement.

    Supports:
      INSERT INTO table_name [(c1, c2, ...)] VALUES (v1, v2), ...
      INSERT INTO table_name [(c1, c2, ...)] SELECT ...
    """
    root = "Insert"
    insert_stmt = statement[root]

    if insert_stmt.get("overwrite"):
        raise UnsupportedSyntaxError("**INSERT OVERWRITE** is not supported. **TRUNCATE** the table first, then **INSERT** into it.")

    body = insert_stmt["source"]["body"]

    # Target relation name
    table_name_parts = insert_stmt["table"]["TableName"]
    relation_name = ".".join(logical_planner_builders.build(p).value for p in table_name_parts)

    # Explicit column list (may be empty/None). sqloxide represents each column
    # reference as a compound-identifier part list; a plain (non-dotted) column
    # name is a single-element list wrapping an {"Identifier": {"value": ...}}.
    explicit_columns = []
    for col in insert_stmt.get("columns") or []:
        if (
            isinstance(col, list)
            and len(col) == 1
            and isinstance(col[0], dict)
            and "Identifier" in col[0]
        ):
            explicit_columns.append(col[0]["Identifier"]["value"])
        else:
            raise UnsupportedSyntaxError(
                f"Unsupported column reference in **INSERT** column list: {col}"
            )
    explicit_columns_tuple = tuple(explicit_columns) if explicit_columns else None

    plan = LogicalPlan()

    if "Values" in body:
        # VALUES source — mirror the existing FromClause VALUES path.
        values_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function="VALUES"
        )
        values_step.alias = f"$insert_values-{random_string(6)}"
        values_step.values = [
            tuple(logical_planner_builders.build(value) for value in row["content"])
            for row in body["Values"]["rows"]
        ]
        # Generate placeholder column names. These will be replaced by visit_insert
        # with the actual column names from the target relation's schema.
        if values_step.values:
            num_cols = len(values_step.values[0])
            values_step.columns = tuple(f"$col{i}" for i in range(num_cols))
        else:
            values_step.columns = ()

        values_id = random_string()
        plan.add_node(values_id, values_step)

        insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
        insert_step.relation_name = relation_name
        insert_step.values_feeder = values_step
        insert_step.source_tail_id = None
        insert_step.explicit_columns = explicit_columns_tuple
        insert_id = random_string()
        plan.add_node(insert_id, insert_step)
        plan.add_edge(values_id, insert_id)
    else:
        # SELECT source — plan the sub-query and keep its Exit node (it already
        # carries the correct final_columns/final_names from schema binding),
        # attaching the Insert sink onto it rather than stripping it. This
        # mirrors plan_explain's pattern and keeps the SELECT subplan genuinely
        # Exit-headed, which execute_native requires to run it on the native
        # engine instead of the legacy push-pipeline.
        source_plan = plan_query(insert_stmt["source"])
        exit_node_id = source_plan.get_exit_points()[0]

        plan += source_plan
        source_tail_id = exit_node_id

        insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
        insert_step.relation_name = relation_name
        insert_step.values_feeder = None
        insert_step.source_tail_id = source_tail_id
        insert_step.explicit_columns = explicit_columns_tuple
        insert_id = random_string()
        plan.add_node(insert_id, insert_step)
        plan.add_edge(source_tail_id, insert_id)

    return plan


def plan_analyze_query(statement, **kwargs) -> LogicalPlan:
    root = "Analyze"

    if not statement[root]["has_table_keyword"]:
        raise UnsupportedSyntaxError("**ANALYZE** without TABLE keyword is not supported. Write `ANALYZE TABLE <table>`.")

    plan = LogicalPlan()
    analyze_node = LogicalPlanNode(node_type=LogicalPlanStepType.Analyze)
    analyze_node.action = "analyze_table"
    analyze_node.table_name = ".".join(
        part["Identifier"]["value"] for part in statement[root]["table_name"]
    )
    # FOR COLUMNS <list> scopes the analysis; empty list = whole table.
    # NB: not `.columns` — that attribute is reserved by the binder for
    # projection columns (post_bind expects bound column objects there).
    analyze_node.analyze_columns = [c["value"] for c in statement[root].get("columns") or []]

    analyze_id = random_string()
    plan.add_node(analyze_id, analyze_node)

    return plan


def plan_drop_statistics(statement, **kwargs) -> LogicalPlan:
    """DROP STATISTICS ON t [FOR COLUMNS …] — synthesized by the planner's
    pre-parse interception (no native sqlparser grammar). Reuses the Analyze
    logical node / Table Management physical node, dispatching on `action`."""
    root = "DropStatistics"
    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Analyze)
    node.action = "drop_statistics"
    node.table_name = statement[root]["table_name"]
    node.analyze_columns = list(statement[root].get("columns") or [])

    node_id = random_string()
    plan.add_node(node_id, node)

    return plan


def plan_drop_trigger(statement, **kwargs) -> LogicalPlan:
    """DROP TRIGGER [IF EXISTS] <name> ON <table> — synthesized by the planner's
    pre-parse interception (OpteryxDialect has no native sqlparser grammar for
    trigger statements). The table is required: trigger names are only unique
    per dataset, and it is the permission target (WRITE) the binder checks."""
    root = "DropTrigger"
    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.DropTrigger)
    node.trigger_name = statement[root]["trigger_name"]
    node.table_name = statement[root]["table_name"]
    node.if_exists = statement[root].get("if_exists", False)

    plan.add_node(random_string(), node)

    return plan


def plan_alter_materialized_view_owner(statement, **kwargs) -> LogicalPlan:
    """ALTER MATERIALIZED VIEW <name> OWNER TO <principal> — synthesized by the
    planner's pre-parse interception.

    Unlike REFRESH, this does not desugar into a CTAS: nothing is read and
    nothing is written but one field on the view's record, so it gets its own
    node, its own binder visitor, and its own permission check."""
    root = "AlterMaterializedViewOwner"
    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterMaterializedViewOwner)
    node.relation_name = statement[root]["name"]
    node.new_owner = statement[root]["owner"]
    node.owner_is_current_user = statement[root].get("current_user", False)

    plan.add_node(random_string(), node)

    return plan


def plan_alter_materialized_view_suspended(statement, **kwargs) -> LogicalPlan:
    """ALTER MATERIALIZED VIEW <name> SUSPEND | RESUME — synthesized pre-parse.

    Suspends automatic refresh without removing the machinery that performs it.
    Dropping the view's triggers was previously the only way to stop it
    refreshing, and left no way to tell "deliberately off" from "quietly
    broken"."""
    root = "AlterMaterializedViewSuspended"
    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterMaterializedViewSuspended)
    node.relation_name = statement[root]["name"]
    node.suspended = statement[root]["suspended"]

    plan.add_node(random_string(), node)

    return plan


def build_expression_tree(relation, dnf_list):
    """
    Recursively build an expression tree from a DNF-like list structure.
    The structure can include:
      - Flat clauses (list of tuples) -> AND of predicates
      - OR groups (list of clauses) -> OR of them
      - Mixed (some tuples, some nested lists) -> AND between flat predicates and subgroup
      - Factored form [common, [or_clauses]] -> (common AND (OR ...))
    """

    # Unwrap redundant single nesting
    while isinstance(dnf_list, list) and len(dnf_list) == 1 and isinstance(dnf_list[0], list):
        dnf_list = dnf_list[0]

    # --- Case: Factored form [common_clause, [or_clauses]] ---
    if (
        isinstance(dnf_list, list)
        and len(dnf_list) == 2
        and isinstance(dnf_list[0], list)
        and isinstance(dnf_list[1], list)
        and all(isinstance(c, list) for c in dnf_list[1])
    ):
        common_clause, or_clauses = dnf_list
        left = build_expression_tree(relation, common_clause)
        right = build_expression_tree(relation, or_clauses)
        return Node(node_type=NodeType.AND, left=left, right=right)

    # --- Case: flat clause (AND of tuples) ---
    if all(isinstance(x, tuple) for x in dnf_list):
        and_node = None
        for identifier, operator, value in dnf_list:
            if identifier is True or identifier is False:
                left_node = build_literal_node(identifier)
            else:
                left_node = LogicalColumn(
                    NodeType.IDENTIFIER, source_column=identifier, source=relation
                )
            comparison_node = Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=left_node,
                right=build_literal_node(value),
            )
            if operator.startswith("AnyOp"):
                comparison_node.left, comparison_node.right = (
                    comparison_node.right,
                    comparison_node.left,
                )
            and_node = (
                comparison_node
                if and_node is None
                else Node(node_type=NodeType.AND, left=and_node, right=comparison_node)
            )
        return and_node

    # --- Case: OR group (list of clauses, each clause = list of tuples) ---
    if all(isinstance(x, list) and all(isinstance(p, tuple) for p in x) for x in dnf_list):
        or_node = None
        for clause in dnf_list:
            clause_node = build_expression_tree(relation, clause)
            or_node = (
                clause_node
                if or_node is None
                else Node(node_type=NodeType.OR, left=or_node, right=clause_node)
            )
        return or_node

    # --- Case: Mixed: some flat predicates and some nested groups ---
    if any(isinstance(x, tuple) for x in dnf_list) and any(isinstance(x, list) for x in dnf_list):
        flat_preds = [x for x in dnf_list if isinstance(x, tuple)]
        subgroups = [x for x in dnf_list if isinstance(x, list)]
        left = build_expression_tree(relation, flat_preds)
        right = build_expression_tree(relation, subgroups)
        return Node(node_type=NodeType.AND, left=left, right=right)

    # --- Case: fallback, treat as OR of subgroups ---
    if isinstance(dnf_list, list):
        or_node = None
        for subgroup in dnf_list:
            subgroup_node = build_expression_tree(relation, subgroup)
            or_node = (
                subgroup_node
                if or_node is None
                else Node(node_type=NodeType.OR, left=or_node, right=subgroup_node)
            )
        return or_node

    raise ValueError(f"Unsupported DNF structure: {dnf_list}")


def plan_comment(statement, **kwargs):
    """
    Create a logical plan for a COMMENT ON TABLE/VIEW statement.

    COMMENT [ IF EXISTS ] ON { TABLE | VIEW } object_name IS 'comment_text'
    """
    root_node = "Comment"
    plan = LogicalPlan()

    comment_node = LogicalPlanNode(node_type=LogicalPlanStepType.Comment)

    # Extract object name (e.g., workspace.collection.view)
    object_name_parts = statement[root_node]["object_name"]
    object_name = extract_variable(object_name_parts)
    if isinstance(object_name, list):
        object_name = ".".join(object_name)
    comment_node.object_name = object_name

    # TABLE and VIEW are the two object types Opteryx has anything to comment ON.
    # The rest of sqlparser's CommentObject parses cleanly and has to be turned away
    # by name here - COMMENT ON COLUMN in particular used to reach the operator, where
    # it failed as a missing *dataset* named `ws.collection.table.column`.
    #
    # These arrive as themselves. An earlier sqlparser had no TABLE or VIEW branch in
    # `parse_comment`, so the SQL rewriter rewrote both to EXTENSION on the way past and
    # this checked for that; sqlparser gained CommentObject::Table and ::View, which made
    # the rewrite a downgrade of a correct parse rather than a workaround for a missing
    # one, and it has been deleted.
    object_type = statement[root_node].get("object_type")
    if object_type not in ("Table", "View"):
        raise UnsupportedSyntaxError(
            f"Opteryx does not support '**COMMENT ON** {str(object_type).upper()}'; "
            "comments can be set on a TABLE or a VIEW."
        )
    comment_node.object_type = object_type

    # Extract the comment text
    comment_node.comment = statement[root_node].get("comment", "")

    # Extract IF EXISTS flag
    comment_node.if_exists = statement[root_node].get("if_exists", False)

    # Add the Comment node
    plan.add_node(random_string(), comment_node)

    return plan


from opteryx.planner.logical_planner.merge_desugar import plan_delete  # noqa: E402
from opteryx.planner.logical_planner.merge_desugar import plan_merge  # noqa: E402
from opteryx.planner.logical_planner.merge_desugar import plan_update  # noqa: E402

QUERY_BUILDERS = {
    "Analyze": plan_analyze_query,
    # synthesized pre-parse, like DropTrigger and RefreshMaterializedView
    "AlterMaterializedViewOwner": plan_alter_materialized_view_owner,
    "AlterMaterializedViewSuspended": plan_alter_materialized_view_suspended,
    "DropStatistics": plan_drop_statistics,
    "DropTrigger": plan_drop_trigger,
    "Comment": plan_comment,
    "Explain": plan_explain,
    "Query": plan_query,
    "Set": plan_set_variable,
    "ShowColumns": plan_show_columns,
    "ShowCreate": plan_show_create_query,
    # "ShowFunctions": show_functions_query,
    "ShowVariable": plan_show_variables,  # generic SHOW handler; only SHOW VARIABLES is supported
    # "Use": plan_use
    "CreateSchema": plan_create_collection,  # CREATE COLLECTION, rewritten by the SQL rewriter
    "CreateView": plan_create_view,
    "AlterView": plan_alter_view,
    "AlterTable": plan_alter_table,
    "AlterFunction": plan_alter_workspace,  # ALTER WORKSPACE, rewritten by the SQL rewriter
    "DropFunction": plan_drop_workspace,  # DROP WORKSPACE, rewritten by the SQL rewriter
    "Drop": plan_drop,  # handles DROP VIEW and DROP TABLE
    "CreateTable": plan_create_table,
    "Truncate": plan_truncate,
    "OptimizeTable": plan_optimize_table,
    "Insert": plan_insert,
    "Merge": plan_merge,
    # UPDATE and DELETE are MERGE with a degenerate source - same action
    # codes, same row addresses, same sink (see merge_desugar).
    "Update": plan_update,
    "Delete": plan_delete,
    "RefreshMaterializedView": plan_refresh_materialized_view,  # synthesized pre-parse
}


# The glob metacharacters that make a visibility-filter key a PATTERN rather than a
# relation name. A key holding none of these is only ever matched by exact lookup, so
# a caller passing no patterns pays one dict hit per scan, as before.
VISIBILITY_PATTERN_CHARACTERS = ("*", "?", "[")


def _insert_visibility_filter(logical_plan, nid, node, filter_dnf, telemetry) -> None:
    """Insert the Filter node `filter_dnf` describes directly above the scan at `nid`.

    Called once per matching key, so a scan covered by several keys ends up under
    several stacked Filter nodes - which is the conjunction, without needing to
    combine the DNFs themselves.
    """
    if filter_dnf == []:
        # TODO: This is a hack to make sure that an empty list of filters
        # means that the relation should not be visible
        expression_tree = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Eq",
            left=build_literal_node(True),
            right=build_literal_node(False),
        )

        # If the filter is an empty list, it means that the relation should not be visible
        filter_node = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
            condition=expression_tree,  # Use the built expression tree
            all_relations={node.relation, node.alias},
        )
        logical_plan.insert_node_after(random_string(), filter_node, nid)
        telemetry.visibility_filters_blank_condition_added += 1
    if filter_dnf:
        # Do some basic simplification early, less binding etc to do if we can
        # eliminate some elements from the tree now
        start = time.monotonic_ns()
        filter_dnf = dnf.simplify_dnf(filter_dnf)
        telemetry.time_rewriting_visibility_filters += time.monotonic_ns() - start
        # Apply the transformation from DNF to an expression tree
        expression_tree = build_expression_tree(node.alias, filter_dnf)

        filter_node = LogicalPlanNode(
            node_type=LogicalPlanStepType.Filter,
            condition=expression_tree,  # Use the built expression tree
            all_relations={node.relation, node.alias},
        )

        logical_plan.insert_node_after(random_string(), filter_node, nid)
        telemetry.visibility_filters_condition_added += 1


def apply_visibility_filters(
    logical_plan: LogicalPlan, visibility_filters: dict, telemetry
) -> LogicalPlan:
    """Attach the caller's row-level filters to the scans they cover.

    Keys are matched two ways: as an exact relation name, and - for keys holding a
    glob metacharacter - as an fnmatch pattern over the relation name. A scan gets
    EVERY key that matches it, not the first, and the matches are conjunctive: each
    becomes its own Filter node, so the rows served are the ones passing all of them.
    The most restrictive key therefore always wins, and a pattern can only narrow
    what an exact key already allowed - never widen it.

    Patterns exist so a caller can scope a whole namespace ("platform.*") instead of
    enumerating the relations in it. Enumeration is fail-open - a relation nobody
    remembered to add is served unfiltered, silently - and a namespace that keeps
    growing is precisely where that goes wrong. A pattern makes the default the other
    way round: a new relation under a covered namespace is filtered the moment it
    exists, and one that cannot carry the filter's column fails to bind rather than
    serving rows it should not.

    Matching is case-sensitive (`fnmatchcase`, not `fnmatch`, whose case folding is
    platform-dependent) because relation names are.
    """
    pattern_keys = [
        key
        for key in visibility_filters
        if any(character in key for character in VISIBILITY_PATTERN_CHARACTERS)
    ]

    for nid, node in list(logical_plan.nodes(True)):
        if node.node_type == LogicalPlanStepType.Scan:
            # `None` is "no filter for this relation" and is not a match, which is
            # what keeps an unlisted relation unfiltered. `[]` IS a match - it is
            # the deny-all - so this cannot be a truthiness test.
            filter_dnf = visibility_filters.get(node.relation)
            if filter_dnf is not None:
                _insert_visibility_filter(logical_plan, nid, node, filter_dnf, telemetry)

            # A scan with no relation name (a subquery, a function scan) has nothing
            # for a pattern to match against; the exact lookup above already covers
            # anything keyed on it.
            if node.relation is None:
                continue

            for key in pattern_keys:
                if key != node.relation and fnmatch.fnmatchcase(node.relation, key):
                    pattern_dnf = visibility_filters[key]
                    if pattern_dnf is not None:
                        _insert_visibility_filter(
                            logical_plan, nid, node, pattern_dnf, telemetry
                        )
    return logical_plan


def do_logical_planning_phase(parsed_statement: dict) -> tuple:
    # The sqlparser ast is an array of asts

    statement_type = next(iter(parsed_statement))
    if statement_type not in QUERY_BUILDERS:
        from opteryx.exceptions import UnsupportedSyntaxError
        from opteryx.utils.sql import convert_camel_to_sql_case

        raise UnsupportedSyntaxError(
            f"Opteryx does not support '{convert_camel_to_sql_case(statement_type)}' type queries."
        )
    # CTEs are Common Table Expressions, they're variations of subqueries
    ctes = extract_ctes(parsed_statement)
    return QUERY_BUILDERS[statement_type](parsed_statement), parsed_statement, ctes
