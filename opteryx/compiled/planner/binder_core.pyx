# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: wraparound=False
# cython: boundscheck=False
# distutils: language = c++

"""The binder's per-node core: name resolution, expression reuse, scope merging
and the per-expression bind every visitor drives (architect plan P2, 2026-10-25).

Moved here from opteryx/planner/binder/binder.py, which re-exports every name.
What runs for every node is here, typed: the already-bound exit, identifier
lookup, the reuse lookup, a literal's column, the `$derived` append and rebind,
and the node's relations. The per-kind arms that mint a computed column (function,
aggregate, CASE, CAST, operators) and the reuse body stay Python in binder.py
(ruling D3) and are called from here.

The binding context stays a Python dataclass (ruling P2.2): its fields are read
once per call into typed locals. Expression fields are read directly
(expressions.pxd, ruling D2); every write goes through the expression's setters.
"""

cimport cython
from libc.stdint cimport uint32_t

from opteryx.compiled.planner.column_table cimport SchemaColumn
from opteryx.compiled.planner.relation_schema cimport RelationSchema
from opteryx.compiled.structures.expressions cimport Expression
from opteryx.compiled.structures.plan_steps cimport PlanStep

from opteryx.compiled.planner.column_table import ConstantColumn
from opteryx.compiled.structures.expressions import Literal
from opteryx.exceptions import AmbiguousDatasetError
from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import UnexpectedDatasetReferenceError
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.types.literal_values import native_literal_value

cdef object _IDENTIFIER = NodeType.IDENTIFIER
cdef object _EVALUATED = NodeType.EVALUATED
cdef object _SUBQUERY = NodeType.SUBQUERY
cdef object _LITERAL = NodeType.LITERAL

# binder.py: the Python arms this core calls. Resolved on first use - binder.py
# imports this module, so it cannot be imported here at load.
cdef object _arms = None


cdef inline object _python_arms():
    global _arms
    if _arms is None:
        from opteryx.planner.binder import binder

        _arms = binder
    return _arms


# The plan step kinds traversal tests for. Resolved on first use, for the same
# reason as the arms above: the logical planner imports the binder.
cdef object _FILTER_STEP = None
cdef object _SUBQUERY_STEP = None
cdef frozenset _RELATION_STEPS = None
cdef frozenset _ALIAS_STEPS = None
cdef frozenset _SCHEMA_STEPS = None


cdef inline void _resolve_step_kinds():
    global _FILTER_STEP, _SUBQUERY_STEP, _RELATION_STEPS, _ALIAS_STEPS, _SCHEMA_STEPS
    if _FILTER_STEP is not None:
        return
    from opteryx.compiled.structures.plan_steps import steps_with
    from opteryx.planner.logical_planner import LogicalPlanStepType

    _SUBQUERY_STEP = LogicalPlanStepType.Subquery
    _RELATION_STEPS = steps_with("relation")
    _ALIAS_STEPS = steps_with("alias")
    _SCHEMA_STEPS = steps_with("schema")
    _FILTER_STEP = LogicalPlanStepType.Filter


cpdef _rebind(context, dict schemas, RelationSchema old, RelationSchema new):
    """Replace schema `old` by `new` in `schemas` (the dict it was read from) and
    wherever this bind shares it (BindingContext.rebind_schema). RelationSchema is
    immutable (architect ruling 2026-09-28): this is how a scope's schema changes."""
    for key, schema in schemas.items():
        if schema is old:
            schemas[key] = new
    context.rebind_schema(old, new)


cpdef _append_derived(context, dict schemas, SchemaColumn column):
    """Add `column` to the scope's `$derived` schema."""
    cdef RelationSchema derived_schema = schemas["$derived"]
    _rebind(context, schemas, derived_schema, derived_schema.with_appended(column))


def merge_schemas(*schemas):
    """
    Handles the merging of relations, requiring a custom merge function.

    Parameters:
        dicts: Tuple[Dict[str, RelationSchema]]
            Dictionaries to be merged.

    Returns:
        A merged dictionary containing RelationSchemas.

    A relation holds each of its columns ONCE: two columns with the same identity are
    the same column, so a repeated identity is added to the merged schema only the first
    time it is seen. Callers merge one context PER BOUND COLUMN — `visit_project` does —
    and appending blindly made every relation's schema grow by a factor of the projection
    width at each Project. That is quadratic on its own (a 90-column SELECT over a
    20-column relation left 1,800 schema columns for everything above it to scan), and it
    was also a wrong ANSWER: a later qualified wildcard expands the schema it is given, so
    `source.*` over a polluted schema emitted every source column many times over and the
    parent Project rejected the query as ambiguous.
    """
    cdef dict merged_dict = {}
    cdef dict identities = {}
    cdef dict dic
    cdef set seen
    cdef list new_columns
    cdef RelationSchema merged
    cdef RelationSchema value
    cdef SchemaColumn column
    for dic in schemas:
        for key, raw in dic.items():
            if key in merged_dict:
                if type(raw) is not RelationSchema:
                    raise InvalidInternalStateError(
                        "Internal Error - merge_schemas expects schemas"
                    )
                value = <RelationSchema>raw
                merged = merged_dict[key]
                # A clone of the schema already held adds nothing - and reading its
                # columns would mint every column of a lazily bound scan.
                if merged.shares_columns(value):
                    continue
                seen = identities[key]
                if seen is None:
                    seen = {column.identity for column in merged.columns}
                    identities[key] = seen
                new_columns = []
                for column in value.columns:
                    if column.identity not in seen:
                        new_columns.append(column)
                for column in new_columns:
                    seen.add(column.identity)
                merged_dict[key] = merged.with_columns(merged.columns + tuple(new_columns))
            else:
                # a distinct schema object of the same value, for branch isolation:
                # re-binding is by object identity (BindingContext.rebind_schema), so a
                # branch must not hold the object it was copied from. The columns are
                # shared - a column is a row of the query's ColumnTable.
                merged = (<RelationSchema?>raw).clone()
                merged_dict[key] = merged
                identities[key] = None  # collected only if `key` comes round again
    return merged_dict


cpdef tuple locate_identifier_in_loaded_schemas(str value, dict schemas):
    """
    Locate a given identifier in a set of loaded schemas.

    Parameters:
        value: str
            The identifier to locate.
        schemas: Dict[str, Schema]
            The loaded schemas to search within.

    Returns:
        A tuple containing the column and its source schema, if found.
    """
    cdef RelationSchema schema
    cdef object found_source_relation = None
    cdef object column = None
    cdef object derived_column = None
    cdef object derived_relation = None
    cdef str lowered = value.lower()

    for name, raw in schemas.items():
        schema = <RelationSchema?>raw
        found = schema.find_lower(lowered)
        if found is None:
            continue
        if name == "$derived" or name == "$project":
            # `$derived` holds what THIS scope is computing — including the aliases the
            # projection being bound has already minted. An alias is NOT a relation, so a
            # name it shares with a real column is not ambiguous: it binds to the input
            # column, which is what PostgreSQL does when an output name and an input name
            # collide. Held aside rather than counted, and used only if nothing real
            # matched (that is how `HAVING <alias>` still resolves).
            #
            # Counting it as a relation made an alias that shadows the column it is
            # computed from self-ambiguous: `SELECT MAX(mass) AS mass, MIN(mass) AS mn`
            # raised AmbiguousIdentifierError on the operand of its own second aggregate.
            #
            # `$project` is `$derived` after a Project's own exit renames it (see
            # project.py) — the same scope-local aliases, just past the point where this
            # level's projection is considered "settled". A set operation's leg-combining
            # visitors merge sibling leg contexts directly, so one leg's finished scratch
            # (e.g. a UNION branch's own `(...) AS ratio`) rides into the set operation's
            # own exit-column resolution alongside the legs' real relation schemas;
            # counting it as a competing relation there raised AmbiguousIdentifierError
            # (TPC-DS Q36's `results_rollup` CTE).
            derived_column = found
            derived_relation = schema
            continue
        if column is not None and found_source_relation is not None:
            # test for duplicates
            raise AmbiguousIdentifierError(identifier=value)
        found_source_relation = schema
        column = found  # don't exit here, so we can test for duplicates

    if column is None:
        return derived_column, derived_relation

    return column, found_source_relation


cdef object _variable_node(node, context):
    """A LITERAL for a variable reference.

    The lookup key is `source_column` — the `@@name` exactly as written. It is NOT
    `value`/`current_name`, which are `alias or source_column`: under
    `SELECT @@version AS v` those read `v`, so the container was asked for a variable
    named `v` and reported it missing.

    The alias rides onto the new node and onto the column's aliases, so it names the
    output column and resolves elsewhere in the query — the same treatment an aliased
    literal gets in `inner_binder`.
    """
    schema_column = context.execution_context.variables.as_column(
        node.source_column,
        plan_context=context.plan_context,
        aliases=[node.alias] if node.alias else None,
    )
    return Literal(
        schema_column=schema_column,
        type=schema_column.column_type,
        value=native_literal_value(schema_column.value, schema_column.column_type),
        alias=node.alias,
        relations=set(),
        arena=context.plan_context.expressions,
    )


cdef bint _shared_holds_it_for_this_qualifier(
    RelationSchema schema, str source_column, str source_lower, str suffix
) except -1:
    # A `$shared-*` schema holds the merged column of a USING / NATURAL JOIN, which
    # belongs to EVERY relation it was merged from (`origin`) and to no other. It is a
    # candidate for `x.col` only when `x` is one of those relations. Admitting every
    # `$shared` schema for every qualifier made `c.id` ambiguous after
    # `a JOIN b USING (id)` - c's own `id` and the merged a/b `id` both matched - so
    # `... JOIN c ON a.id = c.id`, and every USING chain, could not bind.
    cdef str o
    cdef SchemaColumn column = schema.find_lower(source_column.lower())
    if column is None:
        return False
    for o in column.origin or ():
        if o.lower() == source_lower or o.lower().endswith(suffix):
            return True
    return False


cdef dict _candidates(str source, str source_column, dict schemas):
    """Schemas in `schemas` that could provide an identifier qualified by `source`."""
    cdef str name
    cdef str lowered
    cdef dict out
    if not source:
        return schemas
    # A reference like `partsupp.ps_suppkey` from `FROM testdata.tpch.partsupp` carries
    # `node.source = "partsupp"` while the schema is keyed by the full path
    # `testdata.tpch.partsupp`. Match the bare qualifier against both the exact key and
    # the trailing dotted segment.
    #
    # Folded case: a relation ALIAS is an unquoted SQL identifier, and those are
    # case-insensitive by the same rule that already makes column names
    # case-insensitive — `FROM t AS P` addressed as `p.col` is ordinary folding, not a
    # typo. `node.source` itself gets rewritten to the schema's own-cased spelling once
    # a match is found (see locate_identifier), so this is the ONE place case gets
    # folded. Dataset/CTE *names* (as opposed to aliases) stay case-sensitive on
    # purpose — they can resolve to a filesystem path, which is case-sensitive on POSIX.
    cdef str source_lower = source.lower()
    cdef str suffix = f".{source_lower}"
    out = {}
    for name, schema in schemas.items():
        lowered = name.lower()
        if (
            (name.startswith("$shared") and _shared_holds_it_for_this_qualifier(
                <RelationSchema?>schema, source_column, source_lower, suffix
            ))
            or lowered == source_lower
            or lowered.endswith(suffix)
        ):
            out[name] = schema
    return out


cpdef tuple locate_identifier(node, context):
    """
    Locate which schema the identifier is defined in. We return a populated node
    and the context.

    Parameters:
        node: Node
            The node representing the identifier
        context: BindingContext
            The current query context.

    Returns:
        Tuple[Node, Dict]: The updated node and the current context.

    Raises:
        UnexpectedDatasetReferenceError: If the source dataset is not found.
        ColumnNotFoundError: If the column is not found in the schema.
    """
    cdef dict schemas = context.schemas
    cdef dict outer_schemas = context.outer_schemas
    cdef str source = node.source
    cdef str source_column = node.source_column
    cdef str qualifier
    cdef str o
    cdef dict candidate_schemas
    cdef SchemaColumn renamed

    # get the list of candidate schemas
    candidate_schemas = _candidates(source, source_column, schemas)

    # if there are no candidates, we probably don't know the relation — unless an
    # enclosing scope does (a qualified correlated reference, `WHERE l.k = o.k`).
    if not candidate_schemas and not _candidates(source, source_column, outer_schemas):
        if source and source.lower() in {r.lower() for r in context.relations}:
            raise UnexpectedDatasetReferenceError(
                dataset=source,
                message=f"Dataset `{source}` is not available after being used on the right side of a ANTI or SEMI JOIN",
            )
        else:
            raise UnexpectedDatasetReferenceError(dataset=source)

    # look up the column in the candidate schemas
    column, found_source_relation = locate_identifier_in_loaded_schemas(
        source_column, candidate_schemas
    )

    # Not in the local scope — fall outwards to the enclosing query. SQL binds a name to
    # the innermost scope that provides it, so this runs ONLY after the local lookup
    # failed, and can never redirect a name the local scope owns. A hit here is by
    # definition a CORRELATED reference: it is what makes the subquery depend on the
    # outer row, and tagging it is what lets decorrelation orient the predicate without
    # guessing from syntax.
    if column is None and outer_schemas:
        column, found_source_relation = locate_identifier_in_loaded_schemas(
            source_column, _candidates(source, source_column, outer_schemas)
        )
        if column is not None:
            node.is_outer_reference = True
            node.outer_relation = found_source_relation

    # if we didn't find the column, suggest alternatives
    if column is None:
        # Check if the identifier is a variable. Test the name AS WRITTEN
        # (`source_column`), never `current_name` — that is `alias or source_column`, so
        # an aliased variable (`@@version AS v`) tests `v`, fails the `@` check and falls
        # through to ColumnNotFoundError.
        if source_column[0] == "@":
            node = _variable_node(node, context)
            _append_derived(context, schemas, node.schema_column)
            return node, context

        from opteryx.utils import suggest_alternative

        suggestion = suggest_alternative(
            source_column,
            [
                column_name
                for _, schema in candidate_schemas.items()
                for column_name in schema.all_column_names
                if column_name is not None
            ],
        )
        raise ColumnNotFoundError(column=node.value, suggestion=suggestion, span=node.span)
    elif source_column[0] == "@":
        # A second reference to a variable already bound in this query — it was appended
        # to `$derived` above, so the lookup found it. Same rule as the not-found
        # branch: test the name as written, and keep the alias.
        new_node = Literal(
            schema_column=column,
            type=column.column_type,
            value=native_literal_value(column.value, column.column_type),
            alias=node.alias,
            arena=context.plan_context.expressions,
        )
        return new_node, context

    # Update node.source to the found relation name — ALWAYS, not just when empty.
    # `_candidates` matched the qualifier case-insensitively, so a query can type
    # `p.col` against a relation aliased `P`; rewriting `node.source` to the relation's
    # OWN spelling here is what keeps it canonical for every consumer built from
    # `.source` after this point (relation-name lists, `all_relations`, the optimizer) —
    # they compare by exact string and never fold case themselves.
    #
    # One exception: a QUALIFIED reference that resolved to the merged column of a
    # USING / NATURAL JOIN (a `$shared-*` schema, named `^a#^b#`). The canonical
    # relation for `a.id` there is `a` — the origin entry the qualifier matched, in its
    # own spelling — not the internal schema name. `extract_join_fields` sides a join
    # key by `.source`, so leaving `^a#^b#` made `a JOIN b USING (id) JOIN c ON
    # a.id = c.id` (and every USING chain) a join with no key at all.
    shared_hit = False
    if source:
        for key, schema in candidate_schemas.items():
            if key.startswith("$shared") and schema is found_source_relation:
                shared_hit = True
                break
    if shared_hit:
        qualifier = source.lower()
        for o in column.origin or ():
            if o.lower() == qualifier or o.lower().endswith(f".{qualifier}"):
                node.source = o
                break
        else:
            # what next() over the matching origins raised when none matched
            raise StopIteration
    else:
        node.source = found_source_relation.name

    # if we have an alias for a column not known about in the schema, add it. A SELECT
    # alias is another NAME for the column in this scope, and a slot has exactly one name
    # set (architect ruling 2026-09-27): the named column is a new ALIAS slot, swapped
    # into the scope it was found in, rather than a rename of whichever copy of the
    # column this scope happens to hold.
    alias = node.alias
    if alias and alias not in column.all_names:
        renamed = context.plan_context.columns.alias(
            column,
            column.name,
            aliases=[*(column.aliases or []), alias],
            origin=column.origin,
        )
        _rebind(
            context,
            context.schemas,
            found_source_relation,
            found_source_relation.with_columns(
                [renamed if candidate is column else candidate
                 for candidate in found_source_relation.columns]
            ),
        )
        column = renamed

    # Update node.schema_column with the found column
    node.schema_column = column
    # if may need to map source aliases to the columns if they weren't able to be
    # mapped before now
    if column.origin and len(column.origin) == 1:
        node.source = column.origin[0]
    return node, context


cpdef frozenset _identifier_relations(Expression node, dict memo):
    """The relations `node`'s subtree names: every IDENTIFIER under it (itself
    included) contributes its `source` and its bound column's `origin`.

    Memoized per node for one root bind (`memo`, keyed by object id), so an
    expression's relations are built from its children's rather than by re-walking
    the whole subtree at every ancestor - the walk inner_binder did bottom-up was
    quadratic in expression depth. Sound within one root bind: an identifier's
    `source` and column are settled when it binds, which is before any ancestor
    asks."""
    cdef object key = id(node)
    cdef object found = memo.get(key)
    cdef set names
    cdef Expression child
    if found is not None:
        return <frozenset>found
    names = set()
    if node.node_type == _IDENTIFIER:
        if node.source is not None:
            names.add(node.source)
        if node._schema_column is not None:
            names.update(node._schema_column.origin or ())
    for child in node.children():
        names.update(_identifier_relations(child, memo))
    result = frozenset(names)
    memo[key] = result
    return result


def traversive_recursive_bind(node, context, format_cache=None, relations_memo=None):
    # Bind every child first, threading the context through them in their declared
    # order. (List fields were once bound "in parallel" from one context and the
    # results merged — but binding mutates the one shared `schemas` dict in place,
    # so every sibling already saw the others' additions; threading is the same.)
    def _bind_child(child):
        nonlocal context
        bound, context = inner_binder(child, context, format_cache, relations_memo)
        return bound

    node.map_children(_bind_child)
    return node, context


cpdef tuple inner_binder(
    Expression node, context, dict format_cache=None, dict relations_memo=None
):
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).

    ``format_cache`` memoizes format_expression renders for the lifetime of one
    root bind (callers omit it; the root call creates it). Keys are computed
    top-down over pristine subtrees, so within a root bind the cached render of
    every node is exactly what an uncached render would produce — this collapses
    the O(n²) per-node memo-key rendering of deep expressions to O(n).
    ``relations_memo`` does the same for each node's `relations` (see
    _identifier_relations).
    """
    cdef RelationSchema schema
    cdef dict schemas
    cdef str column_name
    cdef str schema_name
    node_type = node.node_type

    # Early exit for columns that are already bound.
    if node._schema_column is not None:
        return node, context

    # An IDENTIFIER is just a lookup to bind the node.
    if node_type == _IDENTIFIER or node_type == _EVALUATED:
        return locate_identifier(node, context)

    # A subquery embedded in an EXPRESSION (`col < (SELECT ...)`, as opposed to one in
    # the FROM clause). Bind its inner plan in a child scope that can see this scope as
    # the enclosing one, so correlated references resolve outwards and get tagged. Its
    # schema stays inside its scope: it must NOT leak into this one.
    if node_type == _SUBQUERY:
        return _python_arms().bind_correlated_subquery(node, context)

    # A calculated column seen again (it appeared earlier in the plan) need not be
    # recalculated: treat the result like an IDENTIFIER. We discard columns not
    # referenced, so this sometimes holds the only reference to child columns, e.g.
    # MAX(id) - we may not have 'id' next time we see it, only MAX(id).
    if format_cache is None:
        format_cache = {}
    if relations_memo is None:
        relations_memo = {}
    column_name = node._query_column or format_expression(node, True, format_cache)
    lowered = column_name.lower()
    for schema_name, raw in context.schemas.items():
        schema = <RelationSchema?>raw
        found_column = schema.find_lower(lowered)
        if found_column is not None and node_type == _LITERAL:
            # A literal's column_name is its own textual form, so the case-insensitive
            # lookup (correct for identifiers) would otherwise collapse two case-distinct
            # string literals ('ss' and 'SS', REPLACE's needle and replacement) onto a
            # single interned constant and hand the executor value == value. Reuse a
            # constant for a literal only when the stored value is byte-identical.
            if not (type(found_column) is ConstantColumn and found_column.value == node.value):
                found_column = None
            # Two literals textually identical but carrying DIFFERENT aliases are two
            # distinct OUTPUT columns; interning them onto one ConstantColumn gives them
            # one identity, so one type and one name (FullOuterToUnionStrategy's
            # synthesized NULLs all collapsed onto the first). Reuse only when the alias
            # agrees too.
            elif node.alias and node.alias not in (found_column.aliases or []):
                found_column = None
            # And on TYPE: two NULL literals are byte-identical and need carry no alias,
            # so an untyped NULL adopted the ConstantColumn of a DIFFERENTLY typed null
            # that merely rendered the same - and that null's type (`COUNT(* WHERE p)`'s
            # IIF ELSE came back BOOL). Both types must be known before this can rule.
            elif (
                node.type is not None
                and found_column.column_type is not None
                and node.type != found_column.column_type
            ):
                found_column = None
        # If the column exists in the schema, bind the node as a repeat of it.
        if found_column is not None:
            return _python_arms()._bind_reused(
                node, context, schema_name, schema, found_column, column_name,
                format_cache, relations_memo,
            )

    schemas = context.schemas

    # do the sub trees off this node
    node, context = traversive_recursive_bind(node, context, format_cache, relations_memo)

    # Now do the node we're at
    if node_type == _LITERAL:
        schema_column = context.plan_context.columns.constant(
            column_name,
            column_type=node.type,
            aliases=[node.alias] if node.alias else [],
            value=node.value,
            nullable=False,
        )
        _append_derived(context, schemas, schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif node_type != _SUBQUERY and not node._do_not_create_column:
        node = _python_arms()._bind_new_column(node, node_type, column_name, context, schemas)

    node.relations = set(_identifier_relations(node, relations_memo))

    context.schemas = schemas
    return node, context


@cython.final
cdef class _ReuseSwap:
    """post_bind's per-expression step: the first expression bound to a column is
    that column's template; each later repeat of a reused expression becomes a copy
    of the template. Passed to map_children as the child callback."""

    cdef dict _reused
    cdef dict _seen

    def __cinit__(self, dict reused):
        self._reused = reused
        self._seen = {}

    def __call__(self, Expression branch):
        return self.swap(branch)

    cdef Expression swap(self, Expression branch):
        cdef object column = branch._schema_column
        if id(branch) in self._reused:
            identity = column.identity
            if identity in self._seen:
                # A copy PER REPEAT, never the stored template itself. Handing every
                # repeat the same object made the bound tree a DAG: IS [NOT]
                # DISTINCT FROM (which repeats its operands) came out with one null
                # test in two places, and a rewrite that inverted it in place for
                # one NOT flipped it for the other too (a WHERE that matched every
                # row instead of none). The recursion below also writes children
                # back into whatever `branch` is, so a shared template was edited
                # once per repeat.
                branch = (<Expression>self._seen[identity]).copy()
        elif column is not None:
            # The template is the expression itself; a repeat takes a copy of it when
            # one turns up. Copying every bound expression up front, in case a repeat
            # appeared, minted ~1,000 throwaway expressions per JOB query.
            self._seen[(<SchemaColumn>column).identity] = branch
        branch.map_children(self)
        return branch


cpdef PlanStep post_bind(PlanStep node, dict reused):
    """The binder skips calculated fields when it performs binding because
    sometimes it doesn't have access to all of the fields used in the calculation -
    so they are bound now. `reused` is the bind's reused_expressions."""
    cdef _ReuseSwap swap = _ReuseSwap(reused)
    cdef Expression column
    _resolve_step_kinds()
    if node.node_type is _FILTER_STEP and node.condition is not None:
        node.condition = swap.swap(node.condition)
    if node._columns:
        # if it doesn't have a schema column here - we can remove it
        node.columns = [swap.swap(column) for column in node._columns if column._schema_column is not None]
    return node


cpdef tuple traverse(visitor, graph, uint32_t node, context):
    """
    Traverses the given graph starting at the given node and calling the
    appropriate visit methods for each node in the graph. This method uses
    a post-order traversal, which visits the children of a node before
    visiting the node itself.

    `visitor` (the BinderVisitor) and `graph` (the LogicalPlan) are Python
    objects, like the context (architect ruling P2.4, 2026-10-25); the steps are
    typed.

    Returns:
        A tuple containing the updated graph and the context.
    """
    cdef list children
    cdef dict enclosing_relations = None
    cdef PlanStep return_node
    cdef PlanStep plan_node
    cdef set all_relations
    cdef list names
    cdef tuple visited
    cdef str alias
    cdef uint32_t child_nid
    cdef uint32_t plan_node_id
    _resolve_step_kinds()

    # Expose the graph on the visitor so handlers (e.g. visit_insert) can
    # reach already-bound upstream nodes by id when needed.
    visitor.graph = graph

    # Recursively visit children
    children = graph.ingoing_edges(node)

    # A Subquery node is a NAMING-SCOPE BOUNDARY: a derived table (and a spliced CTE
    # reference, which is the same node) owns a private relation namespace. Its
    # internal aliases are not addressable from outside, and - the point of clearing
    # them here rather than on the way out - an enclosing alias is not addressable
    # from inside either, so `FROM t p, (SELECT p.id FROM t p) q` shadows rather than
    # collides. This mirrors `BindingContext.open_correlated_scope`, which opens the
    # equivalent scope for a nested subquery; the difference is only that a derived
    # table resolves no names outwards, so there is nothing to carry across.
    #
    # `schemas` needs no equivalent: peers are already isolated from each other (each
    # gets a copy below, and their schemas are accumulated into `exit_context`, not
    # into the shared `context`). `relations` alone is shared across peers, and
    # deliberately - see the merge in the loop.
    if (<PlanStep?>graph[node]).node_type is _SUBQUERY_STEP:
        enclosing_relations = context.relations
        context.relations = {}

    if len(children) == 1:
        # Linear plan (no branching): pass context directly — no copies needed.
        # The child updates context.schemas and context.relations in-place, which
        # is exactly what the parent needs. Avoids expensive deepcopy of schema.
        graph, context = traverse(visitor, graph, children[0][0], context)
    elif children:
        exit_context = context.copy()
        # A branching node is a join or set operation: its legs, left then right,
        # by label (graph.legs refuses unlabelled legs).
        for child_nid in graph.legs(node):
            # Each peer is bound against its own copy, so a peer's SCHEMAS never reach
            # another peer - they are accumulated into `exit_context` and only merged
            # back into `context` once every peer has been bound.
            #
            # `relations` is the exception, and is cumulative ON PURPOSE: the peers of
            # one branching node are the relations of ONE `FROM` scope, and the only
            # thing that makes `FROM t, t` ambiguous is the second scan finding the
            # first scan's name already registered (see visit_scan). Peers are opaque
            # to each other for name RESOLUTION and transparent for name COLLISION.
            _, child_context = traverse(visitor, graph, child_nid, context.copy())
            # merges the schemas from two contexts. The accumulator goes FIRST so the
            # merged dict keeps the legs in order (left leg, then right leg) —
            # `SELECT *` expands schemas in dict order, and SQL requires the left
            # relation's columns before the right's.
            exit_context.schemas = merge_schemas(exit_context.schemas, child_context.schemas)

            context.relations = {
                **context.relations,
                **exit_context.relations,
                **child_context.relations,
            }

        context.schemas = merge_schemas(context.schemas, exit_context.schemas)

    # Visit node and return updated context
    visited = visitor.visit_node(graph[node], context=context)
    context = visited[1]
    if visited[0] is None:
        raise InvalidInternalStateError(f"A binder visitor returned no step for node {node}.")
    return_node = <PlanStep?>visited[0]

    if enclosing_relations is not None:
        # Closing the scope opened above. `visit_subquery` has replaced `relations`
        # with the single name this boundary exports, so the enclosing scope's names
        # come back here and the private ones do not.
        # Case-folded to match visit_scan's collision check (dataset.py) and
        # locate_identifier's resolution (`_candidates` above) - an alias is an
        # unquoted SQL identifier, so `FROM (SELECT ...) Y, (SELECT ...) y` collides
        # exactly as matching-case would.
        alias = return_node.alias
        if alias and alias.lower() in {r.lower() for r in enclosing_relations}:
            # The exported name collides with a relation already in the enclosing
            # scope - `FROM (SELECT ...) y, (SELECT ...) y`, or the two references of
            # `WITH c AS (...) SELECT * FROM c, c`. Genuinely ambiguous, and the same
            # rule visit_scan applies one level down.
            raise AmbiguousDatasetError(dataset=alias)
        context.relations = {**enclosing_relations, **context.relations}

    # We keep track of the relations which are 'visible' along each branch
    all_relations = return_node._all_relations
    if all_relations is None:
        all_relations = set()  # Initialize as an empty set if None
        return_node.all_relations = all_relations

    kind = return_node.node_type
    names = []
    if kind in _RELATION_STEPS:
        names.append(return_node.relation)
    if kind in _ALIAS_STEPS:
        names.append(return_node.alias)
    all_relations.update({value for value in names if value is not None})

    children = graph.ingoing_edges(node)
    for plan_node_id, _, _ in children:
        plan_node = <PlanStep?>graph[plan_node_id]
        if plan_node._all_relations:
            all_relations.update(plan_node._all_relations)

    return_node = post_bind(return_node, context.reused_expressions)
    # A step bound with a schema is re-bound whenever that schema is (RelationSchema
    # is immutable - see BindingContext.rebind_schema).
    if return_node.node_type in _SCHEMA_STEPS:
        context.schema_steps.append(return_node)
    graph[node] = return_node
    return graph, context
