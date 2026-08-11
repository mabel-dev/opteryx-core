# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Edit-time checking: what a statement means, without running it.

This is the planner's front half - rewrite, parse, resolve, bind - stopped the moment
every name and type is settled, and turned into an answer an editor can draw:

  - is it wrong, and WHERE                (a positioned error to underline)
  - what does it return, and of what type (the result shape, before there is a result)
  - what is in scope                       (relation and column names, for completion)

It reads the catalog and it reads nothing else: no data files, no statistics, no
manifests. Nothing it does changes anything - binding checks permissions, it never
exercises them - so it is safe to run on every keystroke of a DROP.

WHY IT SHARES THE PLANNER'S PATH
--------------------------------
`bind_statement` is the same function `query_planner` calls, not a copy of it. A
separate "quick parse" would drift from the real one, and a checker that drifts tells
the reader their statement is fine when it is not - which is worse than no checker,
because it is trusted.

WHAT IT CANNOT DO
-----------------
A statement that does not parse has no plan, so it has no columns and no relations.
Mid-keystroke that is most of the time. The editor keeps the last good check and
redraws completion from it; this returns the error and nothing else rather than
guessing at a half-typed statement from a second, approximate parser.
"""

from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.exceptions import SourcePosition
from opteryx.exceptions import SqlError

__all__ = ["CheckedColumn", "CheckedRelation", "QueryCheck", "check_statement"]


@dataclass(frozen=True)
class CheckedColumn:
    """One column: the name it answers to here, and the type it holds.

    `type` is the canonical SQL spelling (`ColumnType.__str__`) - the same text a
    stored schema persists and `SHOW COLUMNS` prints, so an editor showing it beside a
    completion is showing the reader something they can also type.
    """

    name: str
    type: str
    nullable: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "type": self.type, "nullable": self.nullable}


@dataclass(frozen=True)
class CheckedRelation:
    """A relation in scope, under the name the statement refers to it by.

    `alias` is what qualifies a column here (`p` in `FROM $planets AS p`); `relation`
    is what it actually is. They are the same when nothing was aliased. A CTE or a
    view has been expanded by the time this is built, so what comes back is the real
    relations underneath, not the name written in the FROM clause.
    """

    alias: str
    relation: Optional[str]
    columns: Tuple[CheckedColumn, ...]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "alias": self.alias,
            "relation": self.relation,
            "columns": [column.as_dict() for column in self.columns],
        }


@dataclass(frozen=True)
class CheckedIdentity:
    """One name the statement makes available, and what it stands for.

    This is the completion vocabulary: after `FROM $planets AS t`, `t` is a name the
    reader can type; after `COUNT(*) AS c`, so is `c`. Each says what it resolves to,
    so a suggestor can show `c` and explain it is `COUNT(*)`.

    Attributes:
        identity: the name as it can be referred to - an alias where one was given,
            otherwise the name or rendered expression the engine calls it by.
        type: "relation" or "column".
        definition: what it stands for. A relation's real name for an alias, the
            expression for a computed column, the source column for a reference.
        source: for a column, the relation it is qualified by; None for one computed
            in the statement. Always None for a relation.
        data_type: the column's resolved SQL type; None for a relation.
    """

    identity: str
    type: str
    definition: str
    source: Optional[str] = None
    data_type: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "identity": self.identity,
            "type": self.type,
            "definition": self.definition,
            "source": self.source,
            "data_type": self.data_type,
        }


@dataclass(frozen=True)
class QueryCheck:
    """What a check found. Never raised - an editor wants a diagnostic, not an
    exception, and a statement being wrong is the expected case while it is typed.

    Attributes:
        ok: nothing was found wrong. `error` is None exactly when this is True.
        statement: the statement as submitted, which is what `error.position`
            indexes into.
        error: the error binding would have raised, unraised - so a caller already
            serializing planner errors serializes this the same way. A `SqlError`
            carrying `.position`, a `SourcePosition` range over `statement` for the
            editor to underline; or a `PermissionsError`, which has no position
            because what is refused is the relation, not a place in the text.
        columns: the result shape - one entry per output column, named and typed.
            Empty for a statement that returns no rows (INSERT, CREATE, SET) and for
            one that did not check out.
        relations: every relation the statement reads, with its FULL column list -
            what a suggestor offers after a `t.`. Present even when the statement
            failed to bind, for as far as the binder got: it binds bottom-up, so a
            query broken in its SELECT list has still resolved its FROM.
        identities: every name the statement makes available - relation aliases,
            columns it references, columns it defines - and what each stands for.
            See CheckedIdentity. Also survives a failed bind, partially.

    Everything below is what `opteryx.analyze_query` reports, from the same parse -
    a check is a superset of an analysis, and asking for both should not mean parsing
    the statement twice. These survive a FAILED bind, because they come from the AST:
    a statement that will not bind for want of a parameter still reports which
    parameter, which is the one thing the caller needs to fix it. They are empty only
    when the statement did not parse at all, and so has no AST to read.

        query_type: `Query`, `Insert`, `CreateTable`, `Drop`, ... None if unparsed.
        tables: every relation named, sorted. Not a CTE's own alias, a table-valued
            function, or a `$` system dataset.
        parameters: the `:name` placeholders the statement needs, no leading colon.
            Read from the pre-rewrite AST, so supplying them does not empty this.
        is_read / is_mutation / is_ddl: what kind of statement it is.
        permission_required: "reader", "writer", "owner", or "denied".
    """

    ok: bool
    statement: str
    error: Optional[Exception] = None
    columns: Tuple[CheckedColumn, ...] = ()
    relations: Tuple[CheckedRelation, ...] = ()
    identities: Tuple[CheckedIdentity, ...] = ()
    query_type: Optional[str] = None
    tables: Tuple[str, ...] = ()
    parameters: Tuple[str, ...] = ()
    is_read: bool = False
    is_mutation: bool = False
    is_ddl: bool = False
    permission_required: Optional[str] = None

    @property
    def position(self) -> Optional[SourcePosition]:
        """Where the error is, or None.

        None for a clean statement, for an error nothing recorded a span for, and for
        a `PermissionsError`, which is not a SqlError and so carries no position at
        all - the default keeps those three the same shape rather than three.
        """
        return getattr(self.error, "position", None)

    def as_dict(self) -> Dict[str, Any]:
        """A plain-data form, for a service handing this to a browser."""
        error: Optional[Dict[str, Any]] = None
        if self.error is not None:
            position = self.position
            error = {
                "type": type(self.error).__name__,
                "message": str(self.error),
                "position": None if position is None else position._asdict(),
            }
        return {
            "ok": self.ok,
            "error": error,
            "columns": [column.as_dict() for column in self.columns],
            "relations": [relation.as_dict() for relation in self.relations],
            "identities": [identity.as_dict() for identity in self.identities],
            "query_type": self.query_type,
            "tables": list(self.tables),
            "parameters": list(self.parameters),
            "is_read": self.is_read,
            "is_mutation": self.is_mutation,
            "is_ddl": self.is_ddl,
            "permission_required": self.permission_required,
        }


def _column_name(column) -> Optional[str]:
    """The name a bound column answers to at this point in the plan.

    `current_name` is `alias or source_column`, and an alias may be recorded as a list
    when the same expression was named more than once - the first is the one the
    reader sees.
    """
    name = column.current_name
    if isinstance(name, (list, tuple)):
        name = name[0] if name else None
    if name is None:
        name = column.source_column
    return None if name is None else str(name)


def _checked_column(name: str, column_type, nullable) -> CheckedColumn:
    return CheckedColumn(
        name=name,
        type="UNKNOWN" if column_type is None else str(column_type),
        nullable=True if nullable is None else bool(nullable),
    )


def _output_columns(bound_plan) -> Tuple[CheckedColumn, ...]:
    """The result shape, read off the plan's head.

    The head's `columns` are the bound projection - names as the reader will see them
    (an alias having replaced the expression that produced it) and types the binder
    resolved. A statement with no result set has no such head and reports nothing,
    which is not the same as reporting no columns for one that does.
    """
    heads = bound_plan.get_exit_points()
    if not heads:
        return ()
    head = bound_plan[heads[0]]
    columns = getattr(head, "columns", None)
    if not columns:
        return ()

    checked: List[CheckedColumn] = []
    for column in columns:
        name = _column_name(column)
        if name is None:
            continue
        schema_column = getattr(column, "schema_column", None)
        checked.append(
            _checked_column(
                name,
                None if schema_column is None else schema_column.column_type,
                None if schema_column is None else getattr(schema_column, "nullable", None),
            )
        )
    return tuple(checked)


def _addressable_alias(node) -> Optional[str]:
    """The name the reader can qualify this relation's columns by, or None.

    Usually the alias. But the resolver MINTS an alias when it splices a sub-plan in
    (`$view-a1B2`, `$union-c3D4`) to keep two copies of one relation apart, and that
    name is engine-internal - offering it back would suggest a qualifier that does not
    resolve. Under a spliced alias the relation is still addressable by its own name,
    which is what the reader wrote inside the view or CTE body, so that is used
    instead. The CTE's OWN name survives on the Subquery boundary node above.
    """
    from opteryx.planner.relation_resolver import SYNTHETIC_ALIAS_PREFIXES

    alias = node.alias or node.relation
    if alias is None:
        return None
    alias = str(alias)
    if alias.startswith(SYNTHETIC_ALIAS_PREFIXES):
        return None if node.relation is None else str(node.relation)
    return alias


def _in_scope_relations(bound_plan) -> Tuple[CheckedRelation, ...]:
    """Every relation the bound plan reads, at full width, under the name it is
    addressed by - what a suggestor offers after a `t.`.

    Read off `unpruned_columns` rather than `schema`: the binder narrows a relation's
    schema in place to the columns the statement actually referenced, and completion
    needs the ones it did NOT. visit_scan and visit_subquery record the full set on
    the node for exactly this, on the check path only.

    A CTE or view contributes TWO entries, and both are names the reader can use: the
    boundary, under the name they gave it, exposing what it selects; and the relation
    underneath, under its own name, exposing everything - which is what is in scope
    while they are typing inside the body.
    """
    relations: List[CheckedRelation] = []
    seen: set = set()

    for _, node in bound_plan.nodes(True):
        columns = node.unpruned_columns
        if columns is None:
            continue
        alias = _addressable_alias(node)
        if alias is None or alias in seen:
            continue
        seen.add(alias)
        relations.append(
            CheckedRelation(
                alias=alias,
                relation=None if node.relation is None else str(node.relation),
                columns=tuple(
                    _checked_column(
                        column.name, column.column_type, getattr(column, "nullable", None)
                    )
                    for column in columns
                ),
            )
        )

    relations.sort(key=lambda relation: relation.alias)
    return tuple(relations)


def _expression_roots(node):
    """Every expression hanging off one plan node, however it is stored.

    Node properties hold expressions bare, in lists, and inside `(expr, ascending)`
    tuples for ORDER BY - so this looks for them rather than knowing each property's
    shape, which is how a walk stops finding things the day a property changes.
    """
    from opteryx.models import LogicalColumn
    from opteryx.models import Node

    found = []

    def _walk(value):
        if isinstance(value, (Node, LogicalColumn)):
            found.append(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                _walk(item)

    for prop, value in node.properties.items():
        # `schema` is the relation's own columns, reported as `relations`; the
        # subquery plans hanging off `value` are a different scope with their own
        # names, and reporting them here would offer the reader a name that is not
        # in scope where they are typing.
        if prop in ("schema", "unpruned_columns", "connector", "manifest", "value"):
            continue
        _walk(value)

    return found


def _walk_expression(root, seen):
    """Yield an expression node and everything below it, once each."""
    stack = [root]
    while stack:
        node = stack.pop()
        if node is None or id(node) in seen:
            continue
        seen.add(id(node))
        yield node
        for attr in ("left", "right", "centre"):
            child = getattr(node, attr, None)
            if child is not None:
                stack.append(child)
        parameters = getattr(node, "parameters", None)
        if isinstance(parameters, (list, tuple)):
            stack.extend(p for p in parameters if p is not None)


def _identities(bound_plan) -> Tuple[CheckedIdentity, ...]:
    """The completion vocabulary: every name the statement makes available.

    Three kinds, and they are collected separately rather than by one sweep because
    they mean different things and a sweep cannot tell them apart:

    - a RELATION, under its alias, defined by the relation it actually names;
    - a column the statement DEFINES - what comes out of it - identified by its alias
      or, unaliased, by the rendering the engine names it with (`ROUND(age)`), and
      defined by the expression that computes it;
    - a column the statement REFERENCES anywhere else, defined by its source column.

    A Scan's own `columns` is deliberately skipped: the binder fills it with the
    relation's whole width, so sweeping it would report all twenty columns of a table
    as things the statement references. Those belong in `relations`, which is what a
    suggestor offers after a `t.`.
    """
    from opteryx.expression import NodeType
    from opteryx.expression.formatter import format_expression
    from opteryx.planner.logical_planner import LogicalPlanStepType

    identities: Dict[Tuple[str, str, Optional[str]], CheckedIdentity] = {}

    def _add(identity: CheckedIdentity) -> None:
        # Keyed by SOURCE as well as name, so two relations contributing a column of
        # the same name both survive. Keying on the name alone kept whichever was
        # walked first and gave it that one's source, so `ON p.id = q.id` reported a
        # single `id` belonging to `q` - hiding a referenced column, and asserting a
        # relation for it that the reader could not rely on. Same-named columns from
        # DIFFERENT relations are different things; the same column seen at two levels
        # of the plan is not, and is reconciled after collection.
        identities.setdefault((identity.type, identity.identity, identity.source), identity)

    scan_types = (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset)
    heads = bound_plan.get_exit_points()
    head_id = heads[0] if heads else None

    # Columns spliced in with a sub-plan are qualified by the alias the resolver
    # minted for it, and a reader told their column comes `from $view-bn46` has been
    # told nothing. Inside the body they wrote the relation's own name, so that is
    # what a reference there is reported under.
    minted_aliases: Dict[str, str] = {}
    for _, node in bound_plan.nodes(True):
        if node.node_type in scan_types and node.alias is not None and node.relation is not None:
            addressable = _addressable_alias(node)
            if addressable is not None and addressable != str(node.alias):
                minted_aliases[str(node.alias)] = addressable

    # what each computed column is, keyed by the identity the binder minted for it -
    # this is what lets the head's `c` be reported as `COUNT(*)`. The head itself is
    # excluded: its columns are references TO these, and render as their own names.
    definitions: Dict[Any, str] = {}
    for nid, node in bound_plan.nodes(True):
        if nid == head_id or node.node_type in scan_types:
            continue
        for root in _expression_roots(node):
            schema_column = getattr(root, "schema_column", None)
            if schema_column is None or schema_column.identity in definitions:
                continue
            if root.node_type == NodeType.IDENTIFIER:
                # An aliased identifier RENDERS as its alias - `s.name AS moon`
                # formats to `moon` - so rendering it would define `moon` as `moon`.
                # What it stands for is the column underneath.
                definitions[schema_column.identity] = str(root.source_column or "")
            else:
                definitions[schema_column.identity] = format_expression(root)

    for nid, node in bound_plan.nodes(True):
        if node.node_type in scan_types or node.node_type == LogicalPlanStepType.Subquery:
            # The Subquery boundary is where a CTE or view keeps the name the reader
            # gave it; the Scan under it carries a minted alias - see
            # `_addressable_alias`. Both are relations they can qualify by.
            alias = _addressable_alias(node)
            if alias is not None:
                _add(
                    CheckedIdentity(
                        identity=alias,
                        type="relation",
                        definition=str(node.relation if node.relation is not None else alias),
                    )
                )
            if node.node_type in scan_types:
                continue

        seen: set = set()
        for root in _expression_roots(node):
            for expression in _walk_expression(root, seen):
                if expression.node_type != NodeType.IDENTIFIER:
                    continue
                name = _column_name(expression)
                if name is None:
                    continue
                schema_column = getattr(expression, "schema_column", None)
                if schema_column is None:
                    # Unresolved - the binder never reached it, or it is the very name
                    # that broke the statement. `SELECT nam FROM t` would otherwise
                    # report `nam` as an identity, and a suggestor offering it back
                    # would propose the typo as a column of the table.
                    continue
                identity = schema_column.identity
                # A reference stands for its source column; the head's references
                # stand for whatever expression produced them.
                definition = definitions.get(identity) if nid == head_id else None
                if definition is None:
                    definition = str(expression.source_column or name)
                column_source = (
                    None if expression.source is None else str(expression.source)
                )
                if column_source is not None:
                    column_source = minted_aliases.get(column_source, column_source)
                _add(
                    CheckedIdentity(
                        identity=name,
                        type="column",
                        definition=definition,
                        source=column_source,
                        data_type=(
                            None
                            if schema_column.column_type is None
                            else str(schema_column.column_type)
                        ),
                    )
                )

    # One column reached at two levels of the plan is one identity, not two. The head's
    # output columns carry no relation - by then the reader addresses them by name
    # alone - so `s.name AS moon` was collected once as `moon` from `s` and again as a
    # bare `moon`, and a suggestor would offer the same name twice. Where a name is
    # qualified anywhere, the qualified readings are the answer; a name qualified
    # NOWHERE (a computed column like `c`, or an output the plan does not trace back to
    # one relation) keeps its unqualified reading.
    grouped: Dict[Tuple[str, str], List[CheckedIdentity]] = {}
    for identity in identities.values():
        grouped.setdefault((identity.type, identity.identity), []).append(identity)

    resolved: List[CheckedIdentity] = []
    for group in grouped.values():
        qualified = [identity for identity in group if identity.source is not None]
        resolved.extend(qualified or group[:1])

    return tuple(sorted(resolved, key=lambda i: (i.type, i.identity, i.source or "")))


def check_statement(
    operation: str,
    execution_context,
    query_id: str,
    telemetry,
    parameters=None,
    visibility_filters: Optional[Dict[str, Any]] = None,
    source: Optional[str] = None,
    source_offset: int = 0,
    catalog_cache=None,
) -> QueryCheck:
    """Bind `operation` and report what was found, without running it.

    What becomes a RESULT is what the binder decided about this statement for this
    user: a `SqlError`, a refusal on security grounds, and the read-only-relation
    refusal. Those are all things the reader can fix by typing something else, and are
    exactly what an editor should draw.

    Everything else propagates - a catalog that cannot be reached, an internal state
    error. A checker that swallowed those would report a broken catalog as a clean
    query, which is the one failure mode that makes a checker worse than none.

    The line is drawn by naming the errors rather than by catching a base class,
    because the hierarchy does not draw it: `ReadOnlyConnectorError` is a
    `DatabaseError` and `ParameterError` a bare `ProgrammingError`, neither a
    `SqlError`, though both are purely about what was written. And `DatasetReadError`
    covers both "this table is empty" and "the catalog did not answer" - which is why
    it is deliberately NOT caught, at the price that a check against a table with no
    committed data raises rather than reporting.

    Parsing happens ONCE. What the statement IS - its type, the relations it names,
    the parameters it needs - is read off that same AST and reported whether or not it
    goes on to bind, which is what lets a statement that fails for a missing parameter
    still say which parameter.
    """
    from opteryx.exceptions import ParameterError
    from opteryx.exceptions import PermissionsError
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.exceptions import SecurityError
    from opteryx.planner import bind_logical_plan
    from opteryx.planner import build_logical_plan
    from opteryx.planner import parse_statement
    from opteryx.utils.query_parser import describe_statement

    # The binder's relation gates raise the BUILTIN PermissionError, so it is named
    # alongside the opteryx errors - a caller serializing this should not have to know
    # that one refusal arrives as a Python builtin and the rest do not.
    reportable = (SqlError, SecurityError, ReadOnlyConnectorError, ParameterError, PermissionError)

    def _as_error(error: Exception) -> Exception:
        # Re-typed so every reported refusal is an opteryx error. It carries no
        # position: the gate is on the relation, and no span is recorded where the
        # decision is made.
        if isinstance(error, PermissionError):
            return PermissionsError(str(error))
        return error

    try:
        clean_sql, parsed_statements = parse_statement(
            operation, source=source, source_offset=source_offset, telemetry=telemetry
        )
    except SqlError as error:
        # Nothing parsed, so there is no AST to describe and nothing in scope. The
        # positioned error is the whole answer.
        return QueryCheck(ok=False, statement=operation, error=error)

    described = describe_statement(parsed_statements[0])
    about = {
        "query_type": described["query_type"],
        "tables": tuple(described["tables"]),
        "parameters": tuple(described["parameters"]),
        "is_read": described["is_read"],
        "is_mutation": described["is_mutation"],
        "is_ddl": described["is_ddl"],
        "permission_required": described["permission_required"],
    }

    try:
        logical_plan, _ast = build_logical_plan(
            parsed_statements=parsed_statements,
            clean_sql=clean_sql,
            parameters=parameters,
            telemetry=telemetry,
            catalog_cache=catalog_cache,
        )
    except reportable as error:
        # No plan was built, so there is nothing resolved to offer.
        return QueryCheck(ok=False, statement=operation, error=_as_error(error), **about)

    try:
        bound_plan = bind_logical_plan(
            logical_plan=logical_plan,
            clean_sql=clean_sql,
            visibility_filters=visibility_filters,
            execution_context=execution_context,
            query_id=query_id,
            telemetry=telemetry,
            schema_only=True,
        )
    except reportable as error:
        # Binding is bottom-up and mutates `logical_plan` in place, so a statement
        # that failed above its FROM has still resolved its FROM. Reporting what got
        # bound is what makes completion work on a query that is currently wrong -
        # which, while it is being typed, is every query. Nothing is inferred here:
        # this is the real binder's own output, just less of it.
        return QueryCheck(
            ok=False,
            statement=operation,
            error=_as_error(error),
            relations=_in_scope_relations(logical_plan),
            identities=_identities(logical_plan),
            **about,
        )

    return QueryCheck(
        ok=True,
        statement=operation,
        columns=_output_columns(bound_plan),
        relations=_in_scope_relations(bound_plan),
        identities=_identities(bound_plan),
        **about,
    )
