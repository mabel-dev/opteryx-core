# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Constant Folding

Type: Heuristic
Goal: Evaluate Once

We identify branches in expressions where there are no identifiers, these usually
mean we can evaluate them once, in the optimization phase, and replace them with a
constant for handling in the execution phase, reducing the amount of work done by
the execution engine.

We run this strategy twice, once at the beginning, which primarily handles user
entered expressions we can optimize, and again at the end which handles where
we've rewritten expressions at part of other optimizations which can be folded.
"""

from draken.draken_native import LogicalKind
from draken.draken_native import vector_attach_logical_type
from opteryx.compiled.expression.compiled_expression import build_bytecode, lower
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.expression.evaluator import execute_bytecode
from opteryx.managers.virtual_datasets import no_table_data
from opteryx.models import Node, QueryTelemetry
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import BOOLEAN, LogicalCategory
from opteryx.types.logical_type import LogicalCategory as LC

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _build_if_not_null_node(root, value, value_if_not_null) -> Node:
    from opteryx.expression.functions import get_catalog

    node = Node(node_type=NodeType.FUNCTION)
    node.value = "IFNOTNULL"
    node.parameters = [value, value_if_not_null]
    node.schema_column = root.schema_column
    node.query_column = root.query_column

    # The binder runs before the optimizer, so a node minted here is never bound.
    # Resolve the catalog entry directly — the executor requires function_ref.
    resolved = get_catalog().resolve(node.value, list(node.parameters))
    if resolved is None:
        raise ValueError(f"Unable to resolve folded function '{node.value}'")
    node.function_ref = resolved

    # Mirror the binder's literal coercion for IFNOTNULL: both branches feed
    # vector_iif, which rejects mismatched fixed-width types. Coerce literal
    # parameters to the resolved return type so the constant matches the column.
    result_type = resolved.inferred_return_type
    result_lc = result_type.category if result_type is not None else None
    if result_lc not in (None, LogicalCategory.NULL):
        from opteryx.types.scalars.value_parsing import parse_value

        for param in node.parameters:
            if (
                param.node_type == NodeType.LITERAL
                and param.value is not None
                and param.value != set()
            ):
                param.value = parse_value(result_lc, param.value)
                param.type = result_type
                if param.schema_column is not None:
                    param.schema_column.column_type = result_type
    return node


def _build_transparent_node(root, value, telemetry) -> Node:
    # An algebraic reduction (x * 1 -> x, TRUE AND x -> x) must keep the folded
    # expression's output identity — downstream references root's schema_column,
    # not the operand's. NESTED is the planner's transparent wrapper: it lowers
    # to its centre at compile time and every predicate strategy sees through it,
    # so the identity survives with no runtime cost (same mechanism as
    # redundant_cast's identity-context rewrite).
    node = Node(node_type=NodeType.NESTED)
    node.centre = value
    node.schema_column = root.schema_column
    node.query_column = root.query_column
    node.alias = root.alias
    # See if we can fold this further
    return fold_constants(node, telemetry)


# Operators whose operands may be swapped without changing meaning. Ordering
# comparisons are NOT here: `a < b` and `b < a` are different predicates (they are
# convertible, not equal), and treating them as interchangeable would merge two
# distinct conditions and silently drop one.
_COMMUTATIVE_OPERATORS = frozenset({"Eq", "NotEq"})


def _literal_key(value) -> str:
    """Order-insensitive key for a literal value.

    Collection literals are sorted: `col IN [a, b]` and `col IN [b, a]` are the same
    predicate, and the rewrites that build these lists do not all agree on an order.
    Making duplicate detection depend on them agreeing would tie correctness here to
    an invariant maintained somewhere else entirely.
    """
    if isinstance(value, (list, tuple, set, frozenset)):
        return "[" + ",".join(sorted(repr(v) for v in value)) + "]"
    return repr(value)


def _canonical_predicate_key(node):
    """A string equal for two conditions iff they are the same predicate, or None
    when this node cannot be canonicalised.

    None means "never treat as a duplicate". Every unrecognised shape returns None,
    so the failure mode is a missed dedup (harmless) rather than merging two
    predicates that merely look alike (a wrong answer).
    """
    if node is None:
        return "~none"
    node_type = node.node_type

    if node_type == NodeType.NESTED:  # parenthesis wrapper — transparent
        return _canonical_predicate_key(node.centre)

    if node_type == NodeType.LITERAL:
        type_name = getattr(getattr(node.type, "physical", None), "name", "?")
        return f"~lit[{type_name}]{_literal_key(node.value)}"

    if node_type == NodeType.IDENTIFIER:
        schema_column = getattr(node, "schema_column", None)
        identity = getattr(schema_column, "identity", None)
        # Identity, not name: two different columns can share a name across relations.
        return None if identity is None else f"~col[{identity}]"

    if node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        keys = [_canonical_predicate_key(node.left), _canonical_predicate_key(node.right)]
        if any(k is None for k in keys):
            return None
        return f"{node_type.name}({','.join(sorted(keys))})"

    if node_type in (NodeType.DNF, NodeType.CNF):
        keys = [_canonical_predicate_key(p) for p in (node.parameters or [])]
        if any(k is None for k in keys):
            return None
        return f"{node_type.name}({','.join(sorted(keys))})"

    if node_type == NodeType.NOT:
        key = _canonical_predicate_key(node.centre)
        return None if key is None else f"NOT({key})"

    if node_type == NodeType.UNARY_OPERATOR:
        key = _canonical_predicate_key(node.centre)
        return None if key is None else f"{node.value}({key})"

    if node_type == NodeType.CAST:
        key = _canonical_predicate_key(node.left)
        return None if key is None else f"CAST[{node.value}]({key})"

    if node_type in (NodeType.COMPARISON_OPERATOR, NodeType.BINARY_OPERATOR):
        left_key = _canonical_predicate_key(node.left)
        right_key = _canonical_predicate_key(node.right)
        if left_key is None or right_key is None:
            return None
        if node.value in _COMMUTATIVE_OPERATORS:
            left_key, right_key = sorted((left_key, right_key))
        return f"{node.value}({left_key},{right_key})"

    # FUNCTION is deliberately absent: a volatile function (RANDOM(), NOW()) is not
    # idempotent — `RANDOM() > 0.5 AND RANDOM() > 0.5` is not `RANDOM() > 0.5` — and
    # this canonicaliser has no volatility information to tell those from pure ones.
    # Refusing them costs a missed dedup, never a wrong answer.
    return None


def _dedupe_branches(parameters: list, telemetry) -> list:
    """Drop repeated branches from a DNF/CNF node, preserving order.

    Both shapes are flat idempotent lists — DNF is an AND-list (see filter.pyx),
    CNF an OR-list — and `X AND X` is `X`, `X OR X` is `X`.

    This runs AFTER the branches themselves have been folded, which is the whole
    point: the duplicate is not visible before then. DisjunctiveDomainPushdown ANDs
    a weaker predicate derived from an OR onto that OR, then PredicateRewrite
    collapses the OR into exactly the derived predicate — but leaves `X OR False`
    litter behind, so the two only become textually equal once folding has reduced
    it. Neither strategy is wrong alone and neither can see the collision. Left in,
    every row was filtered twice by an identical condition.
    """
    seen: set = set()
    unique = []
    for parameter in parameters:
        key = _canonical_predicate_key(parameter)
        if key is not None:
            if key in seen:
                telemetry.optimization_duplicate_predicate_removed += 1
                continue
            seen.add(key)
        unique.append(parameter)
    return unique


def fold_constants(root: Node, telemetry: QueryTelemetry) -> Node:
    if root.node_type == NodeType.LITERAL:
        # if we're already a literal (constant), we can't fold
        return root

    if root.node_type == NodeType.EXPRESSION_LIST:
        # we currently don't fold CASE expressions
        return root

    if root.node_type == NodeType.SUBQUERY:
        # subqueries are opaque to constant folding; they have no schema_column
        # and any inner identifiers are not visible at this scope.
        return root

    if root.node_type in (NodeType.DNF, NodeType.CNF):
        root.parameters = _dedupe_branches(
            [fold_constants(p, telemetry) for p in root.parameters], telemetry
        )
        if len(root.parameters) == 1:
            # Don't leave a one-branch DNF/CNF behind — a bare condition is the shape
            # every single-predicate Filter already has.
            return root.parameters[0]
        return root

    if root.node_type in {
        NodeType.COMPARISON_OPERATOR,
        NodeType.BINARY_OPERATOR,
        NodeType.EXTRACTION_OPERATOR,
    }:
        # if we have a binary expression, try to fold each side
        root.left = fold_constants(root.left, telemetry)
        root.right = fold_constants(root.right, telemetry)

        # some expressions we can simplify to x or 0.
        if root.node_type == NodeType.BINARY_OPERATOR:
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 * anything = 0 (except NULL)
                node = _build_if_not_null_node(root, root.right, build_literal_node(0))
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything * 0 = 0 (except NULL)
                node = _build_if_not_null_node(root, root.left, build_literal_node(0))
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 1
            ):
                # 1 * anything = anything (except NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything * 1 = anything (except NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value in "Plus"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 + anything = anything (except NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value in ("Plus", "Minus")
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything +/- 0 = anything (except NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Divide"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything / 1 = anything (except NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_reduce += 1
                return node

        if root.node_type == NodeType.COMPARISON_OPERATOR:
            # anything LIKE '%' is true for non null values
            if (
                root.value in ("Like", "ILike")
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.node_type == NodeType.LITERAL
                and root.right.value == "%"
            ):
                # column LIKE '%' is True
                node = Node(node_type=NodeType.UNARY_OPERATOR)
                node.type = BOOLEAN
                node.value = "IsNotNull"
                node.schema_column = root.schema_column
                node.centre = root.left
                node.query_column = root.query_column
                node.alias = root.alias
                telemetry.optimization_constant_fold_reduce += 1
                return node

    if root.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
        # try to fold each side of logical operators
        if root.left is not None:
            root.left = fold_constants(root.left, telemetry)
        if root.right is not None:
            root.right = fold_constants(root.right, telemetry)

        # If we have a logical expression and one side is a constant,
        # we can simplify further
        if root.node_type == NodeType.OR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == BOOLEAN
                and root.left.value
            ):
                # True OR anything is True (including NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == BOOLEAN
                and root.right.value
            ):
                # anything OR True is True (including NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == BOOLEAN
                and not root.left.value
            ):
                # False OR anything is anything (except NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == BOOLEAN
                and not root.right.value
            ):
                # anything OR False is anything (except NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node

        elif root.node_type == NodeType.AND:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == BOOLEAN
                and not root.left.value
            ):
                # False AND anything is False (including NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == BOOLEAN
                and not root.right.value
            ):
                # anything AND False is False (including NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == BOOLEAN
                and root.left.value
            ):
                # True AND anything is anything (except NULL)
                node = _build_transparent_node(root, root.right, telemetry)
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == BOOLEAN
                and root.right.value
            ):
                # anything AND True is anything (except NULL)
                node = _build_transparent_node(root, root.left, telemetry)
                node.type = BOOLEAN
                telemetry.optimization_constant_fold_boolean_reduce += 1
                return node

        return root

    identifiers = get_all_nodes_of_type(root, (NodeType.IDENTIFIER, NodeType.WILDCARD))
    functions = get_all_nodes_of_type(root, (NodeType.FUNCTION,))
    aggregators = get_all_nodes_of_type(root, (NodeType.AGGREGATOR,))

    if any(func.value in ("RANDOM", "RAND", "NORMAL", "RANDOM_STRING") for func in functions):
        # Although they have no params, these are evaluated per row
        return root

    # fold costants in function parameters - this is generally aggregations we're affecting here
    if root.parameters:
        if isinstance(root.parameters, tuple):
            root.parameters = list(root.parameters)
        for i, param in enumerate(root.parameters):
            root.parameters[i] = fold_constants(param, telemetry)

    _root_ct = (
        getattr(root.schema_column, "column_type", None) if root.schema_column is not None else None
    )
    _root_cat = _root_ct.category if _root_ct is not None else None
    if (
        len(identifiers) == 0
        and len(aggregators) == 0
        and _root_cat != LC.INTERVAL
        # NVARCHAR / VARIANT cannot be represented as a folded scalar literal yet
        # (literal materialisation produces a VARCHAR constant), so folding would
        # drop the type. Leave the runtime expression in place to preserve it.
        and _root_cat != LC.NVARCHAR
        and _root_cat != LC.VARIANT
        and _root_cat != LC.ARRAY
        # A VECTOR is a wide fp16 row, not a scalar — literal materialisation has no
        # form for it, so folding EMBED('literal') produced a constant the compiler
        # could not push and the whole expression fell out of the c-native set.
        # (EMBED was shielded by the VARIANT arm until its return type became the
        # real VECTOR(n) it always was.)
        and _root_cat != LC.VECTOR
    ):
        if root.node_type == NodeType.FUNCTION:
            # Some functions (CONCAT, CONCAT_WS, ...) are rewrite-only: they have
            # no kernel/callable_ref of their own and are only ever meant to reach
            # execution after PredicateRewriteStrategy/FunctionRewriteStrategy
            # desugar them (e.g. CONCAT -> StringConcat chains). Those strategies
            # run AFTER ConstantFoldingStrategy, so an all-literal call such as
            # CONCAT('a', 'b') arrived here undesugared and its callable_ref was
            # None -- 'NoneType' object is not callable. Apply the same rewrite
            # here first so folding evaluates the canonical executable form.
            from .predicate_rewriter import _rewrite_function

            rewritten = _rewrite_function(root, telemetry)
            if rewritten is not root or rewritten.node_type != NodeType.FUNCTION:
                return fold_constants(rewritten, telemetry)
            root = rewritten

        table = no_table_data.read()
        bc = build_bytecode(lower(root))
        result_vector = execute_bytecode(bc, table)
        # execute_bytecode returns a Vector for native kernels, but a BARE LIST for
        # a function still on the Python callable_ref path: the VM flags those
        # BC_RESULT_NO_DV and pushes the callable's raw return value (see the
        # BC_FUNCTION arm in evaluation.pyx). Those impls return plain lists (e.g.
        # text.pyx::to_char -> [chr(a) for a in arr]), and constant folding is the
        # ONE place they still run — the native engine refuses an unported function
        # at plan time rather than falling back. Assuming a Vector here crashed
        # `SELECT CHR(200)` with AttributeError: 'list' has no attribute 'to_pylist'.
        # A boolean connective carries NO schema_column — AND/OR are structure, not
        # a projected column — and `_build_transparent_node` copies that None onto
        # the NESTED wrapper it folds through here. Dereferencing it unconditionally
        # crashed any always-false conjunct nested under an OR
        # (`WHERE id < 3 OR (id == -312.458 AND name < 'x')`) with
        # AttributeError: 'NoneType' object has no attribute 'column_type'.
        # No schema_column means no plan-known target descriptor, which is exactly
        # the `target_ct is None` case the re-attach below already handles.
        _target_sc = root.schema_column
        target_ct = _target_sc.column_type if _target_sc is not None else None
        if not isinstance(result_vector, list):
            if target_ct is not None and target_ct.logical is not None:
                # A C-ABI kernel's VecResult carries a descriptor-bearing result
                # (TIMESTAMP64 unit, DECIMAL precision/scale) in the raw domain —
                # the nogil VM's arena adoption has nowhere to hold it (a bare
                # DrakenVector* has no logical_type slot) and re-attaches only at
                # the runtime plan-known boundary, ExprProjectOperator (see
                # _dv_vecresult_adopt_c in evaluation.pyx). Constant folding is the
                # OTHER plan-known boundary: the target descriptor is already known
                # here (root.schema_column.column_type), so re-attach it before
                # to_pylist() reads the value back out, exactly like
                # ExprProjectOperator's `logical` param does at runtime.
                #
                # IPV4 is the one kind that must NOT be attached here. The attach
                # exists so the readback lands in the LITERAL's own domain, and for
                # every other kind it does: a TIMESTAMP64 reads back as a datetime
                # and DECIMAL as a Decimal, both of which build_literal_node stores
                # (converting the datetime back to int64 micros). An IPv4 reads back
                # as dotted-decimal TEXT (ipv4_to_py_str), but a folded IPv4 literal
                # is the raw uint32 — the form _cast_literal_value produces and
                # _materialise_constant_literal consumes. Attaching would hand
                # build_literal_node a str for a UINT32/IPV4 column, and its
                # isinstance(value, (str, bytes)) branch would build a VARCHAR
                # constant: value/type-tag divergence, wrong rows rather than a
                # crash. Left un-attached, the vector is already plain UINT32, the
                # readback is the int, and ExprProjectOperator re-attaches the
                # descriptor downstream from the schema exactly as it does for a
                # CAST-folded IPv4 literal.
                if target_ct.logical.kind != LogicalKind.IPV4:
                    vector_attach_logical_type(result_vector._nb, target_ct.logical)
        result = result_vector[0] if isinstance(result_vector, list) else result_vector.to_pylist()[0]
        telemetry.optimization_constant_fold_expression += 1
        return build_literal_node(result, root, target_ct)

    return root


class ConstantFoldingStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Constant Folding is when we precalculate expressions (or sub expressions)
        which contain only constant or literal values.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # fold constants when referenced in filter clauses (WHERE/HAVING)
        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = fold_constants(node.condition, self.telemetry)
            if node.condition.node_type == NodeType.LITERAL and node.condition.value:
                context.optimized_plan.remove_node(context.node_id, heal=True)
            else:
                context.optimized_plan[context.node_id] = node
        # fold constants when referenced in the SELECT clause
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = [fold_constants(c, self.telemetry) for c in node.columns]
            context.optimized_plan[context.node_id] = node

        # remove nesting in order by and group by clauses
        if node.node_type == LogicalPlanStepType.Order:
            new_order_by = []
            for field, order in node.order_by:
                while field.node_type == NodeType.NESTED:
                    field = field.centre
                new_order_by.append((field, order))
            node.order_by = new_order_by
            context.optimized_plan[context.node_id] = node

        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            node.groups = [g.centre if g.node_type == NodeType.NESTED else g for g in node.groups]
            node.groups = [fold_constants(g, self.telemetry) for g in node.groups]
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
