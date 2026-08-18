# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.managers.virtual_datasets import derived
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import inner_binder, merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # The derived schema is cleared at the END of this visitor, not the start.
    #
    # It used to be popped here, before the columns below were bound. That is exactly the
    # schema an EXIT column may need: an aggregate registers itself in `$derived` (see
    # binder.visit_aggregate), and binding an unbound expression column appends to it (see
    # binder.inner_binder). Clearing it first meant such a column could neither be resolved
    # against it nor added to it -- it died with `KeyError: '$derived'`.
    #
    # SQL never hit this: the SQL planner's EXIT columns are already-bound identifiers, so
    # they short-circuit. A plan built directly against the logical planner -- which is what
    # the OData service does -- can carry a raw aggregate node on the EXIT, and does.

    def _output_name_for_projection(proj_col, schema_col):
        """User-visible name for an explicitly-projected column."""
        if proj_col.alias:
            return proj_col.alias
        if proj_col.query_column:
            return str(proj_col.query_column)
        if proj_col.current_name:
            return proj_col.current_name
        return schema_col.name

    output_columns = []

    # Internal working columns a bare `*` must not expand into — today, the window
    # columns QUALIFY appended so its Filter had something to read (see the QUALIFY
    # hoist in logical_planner). They are minted, random per execution, and no reader
    # named them; a wildcard expands the relations in scope, and the Window node's
    # output relation is one of them, so without this they rode out to the caller.
    hidden_columns = set(node.hidden_columns or ())

    for column in node.columns:
        if column.node_type == NodeType.WILDCARD:
            # Wildcard expansion — schema-driven. Each SCHEMA COLUMN produces exactly
            # one output column, deduped on (identity, name) rather than identity
            # alone: the same schema can be reachable under more than one
            # `context.schemas` key (shared/view schemas) and must expand once, but a
            # derived relation legitimately holds two DISTINCT columns over one
            # underlying identity — `SELECT id AS x, id` names two columns, and
            # visit_subquery emits both (see binder/subquery.py). Deduping on identity
            # alone dropped every copy after the first, so wrapping a query in a
            # derived table silently lost a column.
            if column.value is not None:
                # Qualified wildcard: only columns whose origin matches the qualifier.
                qualifier = column.value[0]
                seen_identities = set()
                for schema in context.schemas.values():
                    for schema_col in schema.columns:
                        if (schema_col.identity, schema_col.name) in seen_identities:
                            continue
                        origin = schema_col.origin
                        if isinstance(origin, str):
                            origin = [origin]
                            schema_col.origin = origin
                        # Case-folded: `origin` holds the relation's own-cased alias
                        # (from dataset.py/subquery.py), `qualifier` is the user's
                        # typed qualifier - same fold as `_candidates` in binder.py.
                        if origin and qualifier.lower() in (o.lower() for o in origin):
                            output_columns.append(
                                LogicalColumn(
                                    node_type=NodeType.IDENTIFIER,
                                    source_column=schema_col.name,
                                    source=None,
                                    alias=schema_col.name,
                                    schema_column=schema_col,
                                )
                            )
                            seen_identities.add((schema_col.identity, schema_col.name))
            else:
                # Bare wildcard: every column from every relation schema. `$derived` is
                # excluded — it's scratch space for computed expressions bound elsewhere
                # (e.g. ORDER BY LENGTH(name) with no explicit Project step), never a
                # real relation `*` should expand into.
                seen_identities = set()
                for name, schema in context.schemas.items():
                    if name == "$derived":
                        continue
                    for schema_col in schema.columns:
                        if schema_col.name in hidden_columns:
                            continue
                        if (schema_col.identity, schema_col.name) in seen_identities:
                            continue
                        output_columns.append(
                            LogicalColumn(
                                node_type=NodeType.IDENTIFIER,
                                source_column=schema_col.name,
                                source=None,
                                alias=schema_col.name,
                                schema_column=schema_col,
                            )
                        )
                        seen_identities.add((schema_col.identity, schema_col.name))
            continue

        # Explicit projection: emit one output per `node.columns` entry, even when
        # multiple entries resolve to the same underlying schema_column (identity).
        # Earlier nodes may have folded same-identity columns into one — EXIT
        # unfolds them back into the user's distinct output names.
        new_col, _ = inner_binder(column, context)
        schema_col = new_col.schema_column
        column_name = _output_name_for_projection(new_col, schema_col)
        output_columns.append(
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=column_name,
                source=None,
                alias=column_name,
                schema_column=schema_col,
            )
        )

    node.columns = output_columns

    context.schemas["$derived"] = derived.schema()

    return node, context


def visit_project(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    columns = []
    projected_column_count = 0

    # Internal working columns a bare `*` must not expand into — see the note in
    # visit_exit. A bare `SELECT *` builds no Project at all (see logical_planner),
    # so this branch is reached only via `SELECT * EXCEPT (...)`, which leaks the
    # same column by the same route.
    hidden_columns = set(node.hidden_columns or ())

    # Handle wildcards, including qualified wildcards.
    for column in list(node.columns):
        if column.node_type != NodeType.WILDCARD:
            columns.append(column)
        elif column.value is None:
            # we're just a wildcard (not qualified), we're probably here because of an EXCEPT modifier
            except_columns = {c.source_column for c in node.except_columns}
            all_columns = []

            for name, schema in list(context.schemas.items()):
                for schema_column in schema.columns:
                    if schema_column.name in hidden_columns:
                        continue
                    if schema_column.name in except_columns:
                        except_columns.remove(schema_column.name)
                        continue

                    all_columns.append(schema_column.name)

                    column_reference = LogicalColumn(
                        node_type=NodeType.IDENTIFIER,  # column type
                        source_column=schema_column.name,  # the source column
                        source=name,  # the source relation
                        schema_column=schema_column,
                    )
                    columns.append(column_reference)
                if name.startswith("$shared") and f"^{name}#" in schema.name:
                    context.schemas.pop(name)

                context.schemas[name] = RelationSchema(
                    name=name, columns=[col.schema_column for col in columns]
                )

            if len(except_columns) > 0:
                from opteryx.exceptions import ColumnNotFoundError

                message = f"EXCEPT references mulitple columns that cannot be found - " + ", ".join(
                    f"'{c}'" for c in except_columns
                )

                if len(except_columns) == 1:
                    from opteryx.utils import suggest_alternative

                    column = except_columns.pop()
                    suggestion = suggest_alternative(column, candidates=all_columns)
                    message = f"EXCEPT references column that cannot be found - '{column}'."
                    if suggestion is not None:
                        message += f" Did you mean '{suggestion}'?."

                raise ColumnNotFoundError(message=message)

        else:
            # Handle qualified wildcards
            # Ensure column.value is a list/tuple for qualified references
            table_name = (
                column.value[0] if isinstance(column.value, (list, tuple)) else column.value
            )

            found_match = False
            shared_schema_names = []
            table_name_lower = table_name.lower()

            # Two passes. `canonical_name` — the re-key target below
            # (`context.schemas[canonical_name]`) and the `source` stamped onto
            # every matched column — must be SETTLED before any column is built:
            # it defaults to the qualifier as typed (the qualified-path/shared
            # cases aren't a relation ALIAS, so there's no other-cased spelling to
            # prefer) but an exact alias match overwrites it with the schema's OWN
            # key, so a case-folded qualifier (`p.*` against `FROM t P`) re-keys
            # onto the EXISTING "P" entry instead of adding a second "p" one - two
            # entries for one relation would then both satisfy `_candidates`'s
            # case-insensitive match and every later reference would misreport as
            # ambiguous. Doing this within a single pass would stamp `source` from
            # whatever `canonical_name` happened to be BEFORE the exact match was
            # found, if a qualified/shared match was iterated first.
            canonical_name = table_name
            for name in context.schemas:
                if name.lower() == table_name_lower:
                    canonical_name = name
                    break

            for name, schema in list(context.schemas.items()):
                # Check if this schema matches the qualified wildcard. Case-folded
                # for the same reason as `_candidates` in binder.py — a relation
                # alias is an unquoted SQL identifier. Match by:
                # 1. Exact key match (e.g., "supplier" == "supplier")
                # 2. Ends with .table_name (e.g., "testdata.tpch_001.supplier" ends with ".supplier")
                # 3. Shared schema pattern (e.g., "$view-ABC" with matching schema.name)
                name_lower = name.lower()
                is_exact_match = name_lower == table_name_lower
                is_qualified_match = name_lower.endswith(f".{table_name_lower}") or (
                    name.startswith("$view") and schema.name.endswith(f"/{table_name}.parquet")
                )
                is_shared_match = (
                    name.startswith("$shared")
                    and f"^{table_name_lower}#" in schema.name.lower()
                )

                if is_exact_match or is_qualified_match or is_shared_match:
                    found_match = True
                    # Expand all columns from this schema
                    for schema_column in schema.columns:
                        column_reference = LogicalColumn(
                            node_type=NodeType.IDENTIFIER,  # column type
                            source_column=schema_column.name,  # the source column
                            source=canonical_name,  # the source relation
                            schema_column=schema_column,
                        )
                        columns.append(column_reference)

                    # Track shared schemas for cleanup after loop
                    if is_shared_match:
                        shared_schema_names.append(name)

            # Clean up shared schemas after processing
            for shared_name in shared_schema_names:
                context.schemas.pop(shared_name)

            # Update the schema mapping if we found a match
            if found_match and columns:
                context.schemas[canonical_name] = RelationSchema(
                    name=canonical_name, columns=[col.schema_column for col in columns]
                )

    projected_column_count = len(columns)

    # Pass-through columns (ORDER BY / HAVING expressions not in the SELECT list) bind
    # in the same scope as the projection so they resolve against the same schemas and
    # survive the schema trim below; they are split back out after binding and emitted
    # by the Project operator, then dropped at the Exit node.
    for column in list(node.passthrough_columns):
        if column.node_type != NodeType.WILDCARD:
            columns.append(column)
            continue
        raise UnsupportedSyntaxError(
            "**ORDER BY** and **HAVING** do not support wildcard projections."
        )

    # Bind the local columns to physical columns
    node.columns, group_contexts = zip(*(inner_binder(col, context) for col in columns))
    bound_columns = list(node.columns)
    node.columns = list(bound_columns[:projected_column_count])
    node.passthrough_columns = list(bound_columns[projected_column_count:])
    context.schemas = merge_schemas(*[ctx.schemas for ctx in group_contexts])

    # Check for duplicates.
    # Two columns sharing the same underlying identity are still distinct
    # outputs when their user-visible names differ — e.g. `SELECT a AS x, a AS y`,
    # or `SELECT supp_nation, cust_nation` over a self-join where both resolve
    # to the same `n_name` identity. We compare on (identity, lower(name)) so
    # case-variant references like `SELECT id, ID` are still flagged.
    def _output_key(c):
        name = c.alias or getattr(c, "value", None)
        if isinstance(name, str):
            name = name.lower()
        elif isinstance(name, (list, dict, set)):
            # An unaliased literal uses its VALUE as its display name, and an ARRAY /
            # VECTOR / STRUCT literal's value is a Python container — unhashable, so the
            # duplicate-name set below raised TypeError instead of doing its job. repr()
            # is stable within a query and keeps distinct literals distinct.
            # Unreachable today: the parenthesised-values guard in logical_planner refuses
            # such a column before binding. Defensive, so that lifting that guard (which
            # needs constant ARRAY/VECTOR materialization) surfaces the real limitation
            # instead of a TypeError from a name-collision check.
            name = repr(name)
        return (c.schema_column.identity, name)

    top_level_columns = list(node.columns) + list(node.passthrough_columns)
    all_top_level_identities = [c.schema_column.identity for c in top_level_columns]
    # O(1) membership + identity→first-node-column map so the schema trimming
    # below is O(n) rather than O(n²) in projection width (wide SELECTs of
    # computed columns — e.g. ClickBench Q30's 90 SUM(col±k) — were dominated by
    # these scans during binding).
    all_top_level_identity_set = set(all_top_level_identities)
    first_node_column_by_identity = {}
    for _n in top_level_columns:
        first_node_column_by_identity.setdefault(_n.schema_column.identity, _n)
    all_top_level_keys = [_output_key(c) for c in top_level_columns]
    if len(set(all_top_level_keys)) != len(all_top_level_keys):
        from collections import Counter

        from opteryx.exceptions import AmbiguousIdentifierError

        duplicates = [
            key for key, count in Counter(all_top_level_keys).items() if count > 1
        ]
        # Report the DISPLAY name, not the raw value: a duplicated literal column
        # (`SELECT 1, 1`) has a non-str value, and joining those raised
        # `TypeError: sequence item 0: expected str instance` — the ambiguity
        # error replaced by a crash that names no column at all.
        matches = sorted(
            {str(c.alias or getattr(c, "value", None)) for c in node.columns
             if _output_key(c) in duplicates}
        )
        raise AmbiguousIdentifierError(
            message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
        )

    # Remove columns not being projected from the schemas, and remove empty schemas.
    #
    # `retained_columns` are read STRUCTURALLY by an operator below rather than named
    # by this projection — today, a CROSS JOIN UNNEST source, whose array lengths ARE
    # the output row count. `SELECT 1 FROM t CROSS JOIN UNNEST(arr) AS v` projects a
    # literal and references no column, so the plain top-level test drops `arr` and
    # leaves the unnest with no source. Keeping them here only widens the schema; the
    # `columns` list below is still built from top-level columns alone, so nothing
    # extra is projected. See BindingContext.retained_columns.
    keep_identities = all_top_level_identity_set | context.retained_columns
    columns = []
    for relation, schema in list(context.schemas.items()):
        schema_columns = [
            column for column in schema.columns if column.identity in keep_identities
        ]
        if len(schema_columns) == 0:
            context.schemas.pop(relation)
        else:
            for column in schema_columns:
                # for each column in the schema, find the (first) node column with
                # the same identity via the prebuilt map (was a linear scan).
                node_column = first_node_column_by_identity.get(column.identity)
                # update the column reference with any AS aliases
                if node_column and node_column.alias:
                    if node_column.schema_column.aliases:
                        node_column.schema_column.aliases.append(node_column.alias)
                    else:
                        node_column.schema_column.aliases = [node_column.alias]
                    if column.aliases:
                        column.aliases.append(node_column.alias)
                    else:
                        column.aliases = [node_column.alias]
            # update the schema with columns we have references to, removing redundant columns
            schema.columns = schema_columns
            schema_column_identities = {i.identity for i in schema_columns}
            for column in top_level_columns:
                if column.schema_column.identity in schema_column_identities:
                    columns.append(column)

    # We always have a $derived schema, even if it's empty
    if "$derived" in context.schemas:
        context.schemas["$project"] = context.schemas.pop("$derived")
        context.schemas["$project"].name = "$project"
    if "$derived" not in context.schemas:
        context.schemas["$derived"] = derived.schema()

    # update the columns attribute, preserving order
    bound_columns = {c.schema_column.identity: c for c in columns}
    # A bare scalar subquery projected directly (`SELECT (SELECT ...) AS x`) binds
    # via bind_correlated_subquery, which — deliberately — leaves its schema_column
    # pointing at the identity the SUBQUERY's OWN inner scope minted, not one
    # registered in any of `context.schemas` here: most subqueries are consumed as
    # an operand inside a larger bound expression (`x < (SELECT ...)`), and that
    # outer expression is what mints the identity that matters, so leaking the
    # subquery's internal identity outward would be wrong for that (the common)
    # case. When the SUBQUERY node IS the top-level column, though, nothing else
    # mints an identity for it, and the lookup below KeyErrors. It never needs
    # resolving against `context.schemas` — decorrelation (`_output_column` in
    # decorrelate_subquery.py) rederives the true value column from the
    # subquery's own plan directly — so it is enough to let it stand in for
    # itself here.
    for column in top_level_columns:
        if column.node_type == NodeType.SUBQUERY:
            bound_columns.setdefault(column.schema_column.identity, column)
    node.columns = [bound_columns[c.schema_column.identity] for c in node.columns]
    node.passthrough_columns = [bound_columns[c.schema_column.identity] for c in node.passthrough_columns]

    return node, context
