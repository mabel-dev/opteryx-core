# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Physical Planner is the final planning step. It converts the optimized and bound
LogicalPlan into a PhysicalPlan whose nodes are concrete execution operators.

Input:  optimized LogicalPlan + QueryProperties
Output: PhysicalPlan — a graph of operator instances ready for the execution engine

For each logical node the planner selects the physical operator from the operator
registry, based on the node type and its properties. An operator that does real work
(a scan reader, the function dataset, a DDL or write sink) is built as its own class;
every other operator is a PhysicalStep — the typed logical step plus the planner's
physical decisions (operator kind, join mode, sizing estimates):

- Scan         → ParquetReadNode (all-parquet manifests), Reader (internal datasets),
                 NullReaderNode (empty-result scans with contradictory predicates)
- Join         → the inner / outer / filter (semi, anti) / existence / cross /
                 nested loop / ASOF / band join kinds
- Aggregate    → the ungrouped or grouped (hashed) aggregate kinds
- Project, Filter, Order, Limit, Union, Window, Exit → their kinds
- DDL          → ViewManagementNode, TableManagementNode

Edge topology from the logical plan is copied directly into the physical plan — no
structural changes occur at this stage.

The Physical Planner does NOT optimize, bind, or rewrite the plan.
"""

from collections import Counter

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.exceptions import compose
from opteryx.exceptions import md_code
from opteryx.exceptions import md_column
from opteryx.expression import NodeType
from opteryx.expression import binary_operands
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import PhysicalPlan
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import SCAN_READERS
from opteryx.models.dataset_format import manifest_format
from opteryx.operators.catalog import get_registry
from opteryx.operators.window.helpers import FRAMED_AGGREGATE_FUNCTIONS
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.physical_planner.execution_estimates import group_count_estimate
from opteryx.planner.physical_planner.execution_estimates import join_output_rows_estimate
from opteryx.planner.plan_context import PlanContext
from opteryx.compiled.structures.plan_steps import steps_with

# Inverse of a comparison op, for normalizing `literal OP column` to
# `column OP literal` (rugo's predicate tuples are always column-relative).
_INVERT_COMPARISON_OP = {
    "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq", "Eq": "Eq", "NotEq": "NotEq",
}


def _translate_jsonl_predicates(predicates, physical_by_identity):
    """Translate pushed-down predicate condition Nodes into rugo's
    ``(physical_column_name, op, value)`` tuple form (see
    opteryx.connectors.jsonl_io.JSONL_OP_XLAT).

    Every entry here was already gated by JsonlPredicatePushable.can_push at
    optimizer time to be exactly a `column OP literal` COMPARISON_OPERATOR with
    op in JSONL_OP_XLAT -- this only re-derives that shape to build the tuple,
    it does not re-validate it. A predicate can_push declined is never in this
    list; it stays behind as an ordinary Filter node above the scan instead.
    """
    from opteryx.connectors.jsonl_io import JSONL_OP_XLAT

    translated = []
    for condition in predicates or []:
        left, right = binary_operands(condition)
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident, literal, op = left, right, condition.value
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident, literal, op = right, left, _INVERT_COMPARISON_OP[condition.value]
        else:  # pragma: no cover -- can_push only admits this shape
            raise InvalidInternalStateError(
                "READ_JSONL received a pushed predicate that is not a plain "
                "column-vs-literal comparison; JsonlPredicatePushable.can_push "
                "should have declined it."
            )
        physical_name = physical_by_identity[ident.schema_column.identity]
        translated.append((physical_name, JSONL_OP_XLAT[op], literal.value))
    return translated


def _translate_csv_predicates(predicates, physical_by_identity):
    """Translate pushed-down predicate condition Nodes into rugo's
    ``(physical_column_name, op, value)`` tuple form (see
    opteryx.connectors.csv_io.CSV_OP_XLAT).

    Identical shape/reasoning to ``_translate_jsonl_predicates`` above -- every
    entry here was already gated by CsvPredicatePushable.can_push at optimizer
    time to be exactly a `column OP literal` COMPARISON_OPERATOR with op in
    CSV_OP_XLAT.
    """
    from opteryx.connectors.csv_io import CSV_OP_XLAT

    translated = []
    for condition in predicates or []:
        left, right = binary_operands(condition)
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident, literal, op = left, right, condition.value
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident, literal, op = right, left, _INVERT_COMPARISON_OP[condition.value]
        else:  # pragma: no cover -- can_push only admits this shape
            raise InvalidInternalStateError(
                "READ_CSV received a pushed predicate that is not a plain "
                "column-vs-literal comparison; CsvPredicatePushable.can_push "
                "should have declined it."
            )
        physical_name = physical_by_identity[ident.schema_column.identity]
        translated.append((physical_name, CSV_OP_XLAT[op], literal.value))
    return translated


def _jsonl_scan_inputs(scan):
    """A manifest-backed JSONL Scan's reader inputs: (files, physical projection,
    predicates as rugo tuples).

    JsonlReadNode serves both READ_JSONL (files resolved by the binder) and
    dataset Scans (files from the manifest) through the same inputs: the file
    list, the physical (in-file) projection names parallel to `columns`, and
    pushed predicates as rugo tuples. Scan schema columns are file-named, so
    the physical name is simply schema_column.name. An empty projection is the
    genuine COUNT(*) shape (zero-column morsels), same as the parquet scan.
    """
    columns = scan.columns or []
    predicates = scan.predicates or []

    physical_by_identity = {c.schema_column.identity: c.schema_column.name for c in columns}
    # A pushed predicate's column is not necessarily projected; its own
    # schema_column carries the same identity→name mapping.
    for condition in predicates:
        for side in binary_operands(condition):
            schema_column = side.schema_column
            if schema_column is not None:
                physical_by_identity.setdefault(schema_column.identity, schema_column.name)

    return (
        [f.file_path for f in scan.manifest.files],
        [c.schema_column.name for c in columns],
        _translate_jsonl_predicates(predicates, physical_by_identity),
    )


def _skene_scan_inputs(scan):
    """A manifest-backed skene Scan's reader inputs: the manifest's file list plus
    the schema columns the reader must decode (scan schema columns are
    file-named, so physical name = schema_column.name). Pushed predicates stay on
    the step and are lowered by the compiler into scan.compiled_predicate; the
    reader applies them exactly and then selects back down to the projection."""
    columns = scan.columns or []

    # The read set is projection ∪ predicate columns: a pushed predicate's
    # column is not necessarily projected (COUNT(*) WHERE x > 5 projects
    # nothing), but the reader must decode it to filter. The reader selects
    # back down to the projection after filtering, so predicate-only columns
    # never leave the scan.
    read_schema_columns = [c.schema_column for c in columns]
    read_identities = {sc.identity for sc in read_schema_columns}
    for condition in scan.predicates or []:
        for referenced in get_all_nodes_of_type(condition, (NodeType.IDENTIFIER,)):
            schema_column = referenced.schema_column
            if schema_column.identity not in read_identities:
                read_identities.add(schema_column.identity)
                read_schema_columns.append(schema_column)

    return [f.file_path for f in scan.manifest.files], read_schema_columns


def _scan_reader_for_manifest(manifest, dataset: str) -> str:
    """Operator-registry name for a manifest-backed Scan, dispatched on the
    dataset's format (FileEntry.file_format — datasets are single-format).

    An empty manifest is an empty relation: any reader yields nothing, so the
    parquet reader serves it. A mixed manifest raises in manifest_format; a
    format with no registered Scan reader raises here, by name.
    """
    file_format = manifest_format(manifest, dataset=dataset)
    if file_format is None:
        return SCAN_READERS[PARQUET]
    reader_name = SCAN_READERS.get(file_format)
    if reader_name is None:
        raise UnsupportedSyntaxError(
            f"Dataset {dataset or '(unnamed)'} is {file_format}, which has no "
            f"scan reader. Supported formats: {', '.join(sorted(SCAN_READERS))}."
        )
    return reader_name


def _create_aggregate_node(logical_node, query_properties, registry):
    return registry.create_step("Aggregate", query_properties, logical_node)


def _create_aggregate_and_group_node(logical_node, query_properties, registry, group_count_estimate):
    # pre_update_columns: a GROUP BY key that nothing above reads still has to be
    # HASHED to separate the groups, but its values never have to be stored — the
    # grouping contract is 64-bit hash identity. Carrying the set here is what lets
    # the sink kill the key once it is hashed.
    return registry.create_step(
        "Aggregate and Group",
        query_properties,
        logical_node,
        pre_update_columns=logical_node.pre_update_columns,
        group_count_estimate=group_count_estimate,
    )


def _create_distinct_node(logical_node, query_properties, registry, group_count_estimate):
    return registry.create_step(
        "Distinct", query_properties, logical_node, group_count_estimate=group_count_estimate
    )


def _create_window_node(logical_node, query_properties, registry):
    functions = logical_node.window_functions or []
    for kind, _output_identity, _arg, _offset in functions:
        if kind not in WINDOW_FUNCTIONS:
            # The supported set is listed FROM the registry, so this message
            # cannot drift out of date the way a hand-written list did.
            supported = ", ".join(f"**{name}**" for name in sorted(WINDOW_FUNCTIONS))
            raise UnsupportedSyntaxError(
                f"Unsupported window function '{kind}'. The supported window functions are {supported}."
            )
    if not logical_node.order_by:
        # The no-ORDER-BY shape is only produced by the INTERSECT/EXCEPT ALL
        # rewrite: a single ROW_NUMBER over a partition.
        if not logical_node.partition_by:
            raise UnsupportedSyntaxError(
                "ROW_NUMBER without **ORDER BY** requires a **PARTITION BY**. Add one, or give the window an **ORDER BY**."
            )
        if len(functions) != 1 or functions[0][0] != "ROW_NUMBER":
            raise UnsupportedSyntaxError(
                "Only ROW_NUMBER() **OVER** (**PARTITION BY** ...) is supported without **ORDER BY**."
            )
    # pre_update_columns: same reason as the sort — the PARTITION BY / ORDER BY
    # keys are not in it, so it is the set the window must still emit.
    return registry.create_step(
        "Window", query_properties, logical_node, pre_update_columns=logical_node.pre_update_columns
    )


def _create_framed_window_node(logical_node, query_properties, registry):
    if not logical_node.order_by:
        raise UnsupportedSyntaxError(
            "A window **FRAME** (ROWS/RANGE BETWEEN ...) requires an **ORDER BY** in its **OVER** (...) clause."
        )
    functions = logical_node.window_functions or []
    for kind, _output_identity, _arg, _frame in functions:
        if kind not in FRAMED_AGGREGATE_FUNCTIONS:
            raise UnsupportedSyntaxError(
                f"Unsupported framed window function '{kind}'. **SUM**, **COUNT**, **AVG**, **MIN** and **MAX** are the supported window aggregate functions."
            )
    if not functions:
        raise UnsupportedSyntaxError("a framed window node with no functions")
    return registry.create_step(
        "Framed Window",
        query_properties,
        logical_node,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _create_exit_node(logical_node, query_properties, registry):
    # A result with two columns sharing an output name is ambiguous — reject it.
    # `SELECT *` over a join does not reach this: the binder's wildcard expansion
    # names each colliding column by its relation (`a.id`, `b.id`), as an explicit
    # qualified reference is named (binder/project.py, visit_exit). What still
    # reaches it is two explicit outputs given the same name (`SELECT a.id AS x,
    # b.id AS x`), which is refused rather than emitted with duplicate names.
    names = Counter(column.alias for column in logical_node.columns or [])
    duplicates = [name for name, count in names.items() if count > 1]
    if duplicates:
        raise AmbiguousIdentifierError(
            message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(duplicates)}`"
        )
    return registry.create_step(
        "Exit",
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _create_explain_node(logical_node, query_properties, registry):
    return registry.create("Explain", query_properties, logical_node)


def _create_filter_node(logical_node, query_properties, registry):
    return registry.create_step(
        "Filter", query_properties, logical_node, pre_update_columns=logical_node.pre_update_columns
    )


def _create_function_dataset_node(logical_node, query_properties, registry):
    if logical_node.function == "READ_JSONL":
        # READ_JSONL streams morsels from a real file via rugo -- it does not fit
        # the generic single-Morsel DATASET_FUNCTIONS path (VALUES/UNNEST/
        # GENERATE_SERIES), so it gets its own scan operator, the same way
        # Parquet scans are routed to "Parquet Reader" in _create_scan_node.
        physical_by_identity = logical_node.jsonl_physical_by_identity or {}
        # `logical_node.columns` reflects whatever projection_pushdown pruned it
        # to; re-derive the matching physical (pre-alias) names by identity --
        # the bind-time `jsonl_physical_columns` list is the FULL, unpruned file
        # column order and would go stale/misaligned once columns are pruned.
        return registry.create(
            "JSONL Reader",
            query_properties,
            logical_node,
            jsonl_files=list(logical_node.jsonl_files or []),
            jsonl_physical_columns=[
                physical_by_identity[column.schema_column.identity]
                for column in (logical_node.columns or [])
            ],
            jsonl_predicates=_translate_jsonl_predicates(
                logical_node.predicates, physical_by_identity
            ),
        )
    if logical_node.function == "READ_PARQUET":
        # Unlike READ_JSONL, READ_PARQUET reuses the existing native ParquetReadNode
        # wholesale (opteryx.planner.binder.dataset's READ_PARQUET branch builds a
        # real FileSystemTable connector + Manifest at bind time, exactly like a
        # catalog-backed/ad-hoc Scan) -- no bespoke operator, no predicate
        # translation; the step's predicates/columns are already real column
        # identifiers (not JSONL's raw-key remap), and the manifest/connector are
        # already fully resolved.
        return registry.create("Parquet Reader", query_properties, logical_node)
    if logical_node.function == "READ_CSV":
        # READ_CSV reads each file whole (no chunking -- see CsvReadNode's module
        # docstring), but the projection/predicate translation shape is otherwise
        # identical to READ_JSONL above.
        physical_by_identity = logical_node.csv_physical_by_identity or {}
        return registry.create(
            "CSV Reader",
            query_properties,
            logical_node,
            csv_physical_columns=[
                physical_by_identity[column.schema_column.identity]
                for column in (logical_node.columns or [])
            ],
            csv_predicates=_translate_csv_predicates(logical_node.predicates, physical_by_identity),
        )
    return registry.create("Function Dataset", query_properties, logical_node)


def _create_heap_sort_node(logical_node, query_properties, registry):
    return registry.create_step(
        "Heap Sort",
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _inner_join_supported(join) -> bool:
    """Whether the native inner join has a plan for this join's shape: no ON at
    all, or an ON made only of `left column = right column` equalities, each
    across the two legs.

    A mixed-numeric key pair (INTEGER vs FLOAT vs DECIMAL) is supported: the
    compiler materializes a CAST column on the narrower side and keys on that, so
    both sides hash the same representation (_join_key_coercions in
    opteryx/managers/execution/compiler.py)."""
    if join.on is None:
        return True
    left_relation_names = set(join.left_relation_names or [])
    right_relation_names = set(join.right_relation_names or [])
    comparisons = get_all_nodes_of_type(join.on, (NodeType.COMPARISON_OPERATOR,))
    if not comparisons:
        return False
    for comparison in comparisons:
        if comparison.value != "Eq":
            return False
        left, right = comparison.left, comparison.right
        if left is None or right is None:
            return False
        if left.node_type != NodeType.IDENTIFIER or right.node_type != NodeType.IDENTIFIER:
            return False
        if not left.schema_column or not right.schema_column:
            return False
        if not (
            (left.source in left_relation_names and right.source in right_relation_names)
            or (left.source in right_relation_names and right.source in left_relation_names)
        ):
            return False
    return True


# Logical join type -> (registered operator, the physical join mode it runs).
# The modes the planner names by the logical type itself are listed with None.
_JOIN_OPERATORS = {
    "inner": ("Inner Join", "inner"),
    "nested loop": ("Nested Loop Join", "nested_loop"),
    "left outer": ("Outer Join", None),
    "full outer": ("Outer Join", None),
    "right outer": ("Outer Join", None),
    "cross join": ("Cross Join", "cross"),
    # LEFT SEMI, LEFT ANTI, LEFT ANTI NULL-AWARE (NOT IN), and the two
    # not-distinct forms (INTERSECT / EXCEPT, where NULL equals NULL)
    "left anti": ("Filter Join", None),
    "left semi": ("Filter Join", None),
    "left anti null-aware": ("Filter Join", None),
    "left semi not-distinct": ("Filter Join", None),
    "left anti not-distinct": ("Filter Join", None),
    # The same existence test as the filter joins above, EMITTED as a BOOL
    # column instead of applied — what a SELECT-list EXISTS / IN reads.
    "left existence": ("Existence Join", None),
    "left existence anti": ("Existence Join", None),
    # ASOF JOIN — nearest-neighbour time-series join
    "asof": ("ASOF Join", "asof"),
    # BAND JOIN — an equi-join whose ON also closes a range on one build-side
    # column, executed as a bisect into sorted per-equi-group runs instead of a
    # full equi fan-out with the range filtered off the top.
    "band": ("Band Join", "band"),
}


def _create_join_node(logical_node, query_properties, registry, output_rows_estimate):
    join_type = logical_node.type
    operator = _JOIN_OPERATORS.get(join_type)
    if operator is None:
        # We don't support other JOIN types, e.g. RIGHT SEMI, RIGHT ANTI
        raise InvalidInternalStateError(f"Unsupported JOIN type '{join_type}'")
    name, mode = operator
    if join_type == "inner" and not _inner_join_supported(logical_node):
        # NotSupportedError, not UnsupportedSyntaxError: the statement parsed and bound
        # fine, so nothing about the SQL is wrong - the engine's inner-join operator
        # simply has no plan for this shape. Naming Draken told the reader about a
        # component they have no way to act on.
        raise NotSupportedError(
            "This JOIN is not supported. The engine's inner join cannot handle the "
            "shape of this query - rewriting the join conditions, or joining the "
            "relations in a different order, may let it run."
        )
    if join_type == "asof" and not (
        logical_node.asof_left_column and logical_node.asof_right_column and logical_node.asof_op
    ):
        raise InvalidInternalStateError(
            "An ASOF join requires asof_left_column, asof_right_column, and asof_op"
        )
    if join_type == "band":
        if (
            logical_node.band_column is None
            or logical_node.band_lower is None
            or logical_node.band_upper is None
        ):
            raise InvalidInternalStateError(
                "A band join requires band_column and both band_lower and band_upper"
            )
        if not logical_node.left_columns or not logical_node.right_columns:
            raise InvalidInternalStateError("A band join requires equi-join keys on both legs")
    # The join's expected output rows sizes the native build sink (see
    # execution_estimates); None for join types with no build payload.
    return registry.create_step(
        name,
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
        join_type=join_type if mode is None else mode,
        join_output_rows_estimate=output_rows_estimate,
    )


def _create_scalar_guard_node(logical_node, query_properties, registry):
    return registry.create_step("Scalar Guard", query_properties, logical_node)


def _create_limit_node(logical_node, query_properties, registry):
    return registry.create_step("Limit", query_properties, logical_node)


def _create_order_node(logical_node, query_properties, registry):
    # pre_update_columns: the sort's ORDER BY keys are not in it (it is
    # snapshotted before the node's own columns are collected), so it is
    # precisely the set the sort must still emit — what lets the sink drop a
    # key column once the sort keys are built instead of gathering it into
    # every output row and having the Exit select throw it away.
    return registry.create_step(
        "Sort", query_properties, logical_node, pre_update_columns=logical_node.pre_update_columns
    )


def _create_project_node(logical_node, query_properties, registry):
    return registry.create_step(
        "Projection", query_properties, logical_node, columns=logical_node.columns
    )


def _validated_scan_overrides(hint_settings, query_properties):
    """Gate and coerce a scan's `WITH(name = value)` settings.

    The logical planner has already checked each NAME against the per-scan
    vocabulary. What is left is the part that needs a session: the SAME
    owner / entitlement / type gate a `SET` of that variable would run, via
    `SystemVariablesContainer.check_settable`. A hint is inline SQL text, so
    without this it would be a way around the variables permission model —
    every per-scan knob is `Visibility.RESTRICTED`.

    Returns a plain ``{name: value}`` mapping, or None when the scan carries no
    settings (the overwhelmingly common case, and the one that must allocate
    nothing).
    """
    if not hint_settings:
        return None
    variables = getattr(query_properties, "variables", None)
    if variables is None:
        # No session container to check against. Fail rather than apply an
        # ungated override — silently dropping it would be worse.
        raise PermissionsError(
            "Per-scan settings cannot be applied without a session: "
            f"{', '.join(md_column(name) for name in sorted(hint_settings))}"
        )
    overrides = {}
    for name, literal in sorted(hint_settings.items()):
        variables.check_settable(name, literal.type)
        overrides[name] = literal.value
    return overrides


def _create_scan_node(logical_node, query_properties, registry):
    """Build the scan node, then refuse any per-scan setting it cannot honour.

    The reader is not known until it has been chosen, so this check runs after
    the build. A `WITH(name = value)` on a relation whose reader never reads
    those settings would otherwise parse, pass the permission gate, and do
    nothing — which is exactly the silent-no-op the hint vocabulary exists to
    prevent. A knob that cannot bind must say so, not measure as "no effect".
    """
    node = _build_scan_node(logical_node, query_properties, registry)
    requested = logical_node.hint_settings
    if requested and not node.honours_scan_overrides:
        names = ", ".join(md_column(name) for name in sorted(requested))
        raise UnsupportedSyntaxError(
            compose(
                f"{names} cannot be set on {md_column(logical_node.relation or 'this relation')}",
                f"it is read by {md_code(node.name)}, which does not take per-scan "
                "IO settings — they are read by the Parquet reader",
            )
        )
    return node


def _build_scan_node(logical_node, query_properties, registry):
    # Gated here, for every reader, even though only the Parquet reader takes
    # them: a setting the session may not make is refused before it is refused
    # for being unreadable.
    scan_overrides = _validated_scan_overrides(logical_node.hint_settings, query_properties)
    connector = logical_node.connector

    if connector == "__null__":
        # Scan marked for empty result (contradictory predicates)
        return registry.create("Null Reader", query_properties, logical_node)
    elif logical_node.for_snapshots_only:
        # SHOW SNAPSHOTS / LINEAGE / SOURCES FOR: this Scan exists so the
        # relation is BOUND — the permission gate, the connector, and the commit
        # history the statement answers from — and is never read. serial_engine
        # answers from the Show node above it and never drives the pipeline.
        #
        # It carries no manifest by design: the history is the result, and
        # building one would pay binding's expensive half to produce a file list
        # nothing looks at. So it cannot take the manifest branch below, and the
        # reader that yields no rows is the honest physical form of a scan whose
        # rows are not part of the answer. SHOW MANIFEST FOR differs here — its
        # Scan does carry a Manifest, because that IS its result.
        return registry.create("Null Reader", query_properties, logical_node)
    elif connector and getattr(connector, "scan_reader", None):
        # A reader that names its own physical scan node decides the reader,
        # BEFORE the manifest branch below and not after it.
        #
        # The ordering is load-bearing. A manifest no longer implies the rows
        # come from files: an external source (a PostgreSQL server) may carry
        # one purely to hold STATISTICS for the planner. Testing the manifest
        # first would route such a scan to a FILE reader, and a manifest with no
        # data files falls through `_scan_reader_for_manifest` to the parquet
        # reader, which reads nothing and yields no rows. Every query against
        # every external table would return empty, with no error anywhere.
        return registry.create(connector.scan_reader, query_properties, logical_node)
    elif connector and logical_node.manifest is not None:
        # Manifest-backed Scan: dispatch on the dataset's single format.
        # For parquet this is the column-chunk range-read path: footer-first
        # planning, per-row-group morsels; works for any connector (local, GCS,
        # S3, Opteryx catalog) — filesystem is resolved from file-path protocol
        # inside the reader if not provided directly by the connector.
        reader_name = _scan_reader_for_manifest(logical_node.manifest, str(logical_node.relation or ""))
        if reader_name == "JSONL Reader":
            files, physical_columns, predicates = _jsonl_scan_inputs(logical_node)
            return registry.create(
                reader_name,
                query_properties,
                logical_node,
                jsonl_files=files,
                jsonl_physical_columns=physical_columns,
                jsonl_predicates=predicates,
            )
        if reader_name == "Skene Reader":
            files, read_schema_columns = _skene_scan_inputs(logical_node)
            return registry.create(
                reader_name,
                query_properties,
                logical_node,
                skene_files=files,
                skene_read_schema_columns=read_schema_columns,
            )
        return registry.create(reader_name, query_properties, logical_node, scan_overrides=scan_overrides)
    elif connector and getattr(connector, "interal_only", False):
        # Internal virtual datasets (for example $one_row) do not use file manifests.
        return registry.create("Reader", query_properties, logical_node)
    else:
        raise UnsupportedSyntaxError(
            "Scans require a file manifest. Non-manifest external scan paths have been removed."
        )


def _create_materialized_cte_ref_node(logical_node, query_properties, registry):
    return registry.create_step(
        "CTE Reference",
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _create_set_node(logical_node, query_properties, registry):
    return registry.create("Set Variable", query_properties, logical_node)


def _create_show_node(logical_node, query_properties, registry):
    object_type = logical_node.object_type

    if object_type in ("TABLE", "VIEW", "MATERIALIZED VIEW", "TASK", "TRIGGER"):
        return registry.create("Show Create", query_properties, logical_node)
    else:
        raise UnsupportedSyntaxError(f"Unsupported SHOW type '{object_type}'")


def _create_create_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, logical_node, action="create_view")


def _create_alter_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, logical_node, action="alter_view")


def _create_drop_view_node(logical_node, query_properties, registry):
    return registry.create("View Management", query_properties, logical_node, action="drop_view")


def _create_show_columns_node(logical_node, query_properties, registry):
    return registry.create("Show Columns", query_properties, logical_node)


def _create_show_manifest_node(logical_node, query_properties, registry):
    return registry.create("Show Manifest", query_properties, logical_node)


def _create_show_snapshots_node(logical_node, query_properties, registry):
    return registry.create("Show Snapshots", query_properties, logical_node)


def _create_show_lineage_node(logical_node, query_properties, registry):
    return registry.create("Show Lineage", query_properties, logical_node)


def _create_show_sources_node(logical_node, query_properties, registry):
    return registry.create("Show Sources", query_properties, logical_node)


def _create_union_node(logical_node, query_properties, registry):
    return registry.create_step(
        "Union",
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _create_unnest_node(logical_node, query_properties, registry):
    return registry.create_step(
        "Unnest Join",
        query_properties,
        logical_node,
        columns=logical_node.columns,
        pre_update_columns=logical_node.pre_update_columns,
    )


def _create_analyze_node(logical_node, query_properties, registry):
    return registry.create("Table Management", query_properties, logical_node)


def _create_comment_node(logical_node, query_properties, registry):
    # COMMENT ON VIEW/TABLE/EXTENSION - use ViewManagementNode with 'comment' action
    return registry.create("View Management", query_properties, logical_node, action="comment")


def _create_create_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="create_relation")


def _create_drop_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_relation")


def _create_create_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="create_collection")


def _create_clone_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="clone_relation")


def _create_clone_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="clone_collection")


def _create_resync_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="resync_relation")


def _create_detach_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="detach_relation")


def _create_drop_collection_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_collection")


def _create_truncate_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="truncate_relation")


def _create_alter_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="cluster_by")


def _create_create_tag_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="create_tag")


def _create_drop_tag_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_tag")


def _create_rollback_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="rollback_relation")


def _create_rename_relation_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="rename_relation")


def _create_add_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="add_column")


def _create_drop_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_column")


def _create_rename_column_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="rename_column")


def _create_alter_column_type_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_column_type")


def _create_add_relationship_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="add_relationship")


def _create_drop_relationship_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_relationship")


def _create_alter_workspace_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_workspace")


def _create_alter_workspace_secure_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_workspace_secure")


def _create_drop_workspace_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_workspace")


def _create_insert_node(logical_node, query_properties, registry):
    return registry.create("Insert", query_properties, logical_node)


def _create_compaction_commit_node(logical_node, query_properties, registry):
    return registry.create("Compaction Commit", query_properties, logical_node)


def _create_merge_node(logical_node, query_properties, registry):
    return registry.create("Merge", query_properties, logical_node)


def _create_drop_trigger_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_trigger")


def _create_create_trigger_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="create_trigger")


def _create_alter_trigger_suspended_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_trigger_suspended")


def _create_alter_trigger_minimum_interval_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_trigger_minimum_interval")


def _create_create_task_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="create_task")


def _create_alter_trigger_owner_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_trigger_owner")


def _create_drop_task_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="drop_task")


def _create_alter_task_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_task")


def _create_listen_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="listen")


def _create_unlisten_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="unlisten")


def _create_alter_materialized_view_owner_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_materialized_view_owner")


def _create_alter_materialized_view_suspended_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="alter_materialized_view_suspended")


def _create_grant_access_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="grant_access")


def _create_revoke_access_node(logical_node, query_properties, registry):
    return registry.create("Relation Management", query_properties, logical_node, action="revoke_access")


def _create_call_procedure_node(logical_node, query_properties, registry):
    # CALL rides the same operator as the DDL statements. Its name reads oddly here -
    # a procedure call manages no relation - but that operator is what actually
    # executes every non-tabular statement (GRANT and REVOKE are already there for the
    # same reason), and it is the path that runs OFF the native per-morsel engine,
    # which is where a Python callable belongs.
    return registry.create("Relation Management", query_properties, logical_node, action="call_procedure")


def _create_show_grants_on_node(logical_node, query_properties, registry):
    # Both listings, attached and effective: one operator, told which question
    # to ask by the `effective` property the logical plan carries.
    return registry.create("Show Grants", query_properties, logical_node)


_DISPATCH = {
    LogicalPlanStepType.Aggregate:        _create_aggregate_node,
    LogicalPlanStepType.AggregateAndGroup: _create_aggregate_and_group_node,
    LogicalPlanStepType.Distinct:         _create_distinct_node,
    LogicalPlanStepType.Exit:             _create_exit_node,
    LogicalPlanStepType.Explain:          _create_explain_node,
    LogicalPlanStepType.Filter:           _create_filter_node,
    LogicalPlanStepType.FunctionDataset:  _create_function_dataset_node,
    LogicalPlanStepType.HeapSort:         _create_heap_sort_node,
    LogicalPlanStepType.Join:             _create_join_node,
    LogicalPlanStepType.Limit:            _create_limit_node,
    LogicalPlanStepType.ScalarSubqueryGuard: _create_scalar_guard_node,
    LogicalPlanStepType.Order:            _create_order_node,
    LogicalPlanStepType.Project:          _create_project_node,
    LogicalPlanStepType.Scan:             _create_scan_node,
    LogicalPlanStepType.MaterializedCteRef: _create_materialized_cte_ref_node,
    LogicalPlanStepType.Set:              _create_set_node,
    LogicalPlanStepType.Show:             _create_show_node,
    LogicalPlanStepType.CreateView:       _create_create_view_node,
    LogicalPlanStepType.AlterView:        _create_alter_view_node,
    LogicalPlanStepType.DropView:         _create_drop_view_node,
    LogicalPlanStepType.ShowColumns:      _create_show_columns_node,
    LogicalPlanStepType.ShowManifest:     _create_show_manifest_node,
    LogicalPlanStepType.ShowSnapshots:    _create_show_snapshots_node,
    LogicalPlanStepType.ShowLineage:      _create_show_lineage_node,
    LogicalPlanStepType.ShowSources:      _create_show_sources_node,
    LogicalPlanStepType.Union:            _create_union_node,
    LogicalPlanStepType.Window:           _create_window_node,
    LogicalPlanStepType.FramedWindow:     _create_framed_window_node,
    LogicalPlanStepType.Unnest:           _create_unnest_node,
    LogicalPlanStepType.Analyze:          _create_analyze_node,
    LogicalPlanStepType.Comment:          _create_comment_node,
    LogicalPlanStepType.CreateRelation:   _create_create_relation_node,
    LogicalPlanStepType.DropRelation:     _create_drop_relation_node,
    LogicalPlanStepType.CreateCollection: _create_create_collection_node,
    LogicalPlanStepType.DropCollection:   _create_drop_collection_node,
    LogicalPlanStepType.CloneRelation:    _create_clone_relation_node,
    LogicalPlanStepType.CloneCollection:  _create_clone_collection_node,
    LogicalPlanStepType.ResyncRelation:   _create_resync_relation_node,
    LogicalPlanStepType.DetachRelation:   _create_detach_relation_node,
    LogicalPlanStepType.TruncateRelation: _create_truncate_relation_node,
    LogicalPlanStepType.AlterRelation:    _create_alter_relation_node,
    LogicalPlanStepType.RenameRelation:   _create_rename_relation_node,
    LogicalPlanStepType.CreateTag:        _create_create_tag_node,
    LogicalPlanStepType.DropTag:          _create_drop_tag_node,
    LogicalPlanStepType.RollbackRelation: _create_rollback_relation_node,
    LogicalPlanStepType.AddColumn:        _create_add_column_node,
    LogicalPlanStepType.DropColumn:       _create_drop_column_node,
    LogicalPlanStepType.RenameColumn:     _create_rename_column_node,
    LogicalPlanStepType.AlterColumnType:  _create_alter_column_type_node,
    LogicalPlanStepType.AddRelationship:  _create_add_relationship_node,
    LogicalPlanStepType.DropRelationship: _create_drop_relationship_node,
    LogicalPlanStepType.AlterWorkspace:   _create_alter_workspace_node,
    LogicalPlanStepType.AlterWorkspaceSecure: _create_alter_workspace_secure_node,
    LogicalPlanStepType.DropWorkspace:    _create_drop_workspace_node,
    LogicalPlanStepType.Insert:           _create_insert_node,
    LogicalPlanStepType.Merge:            _create_merge_node,
    LogicalPlanStepType.CompactionCommit: _create_compaction_commit_node,
    LogicalPlanStepType.DropTrigger:      _create_drop_trigger_node,
    LogicalPlanStepType.CreateTrigger:    _create_create_trigger_node,
    LogicalPlanStepType.AlterTriggerSuspended: _create_alter_trigger_suspended_node,
    LogicalPlanStepType.AlterTriggerMinimumInterval: _create_alter_trigger_minimum_interval_node,
    LogicalPlanStepType.CreateTask:       _create_create_task_node,
    LogicalPlanStepType.DropTask:         _create_drop_task_node,
    LogicalPlanStepType.AlterTask:        _create_alter_task_node,
    LogicalPlanStepType.Listen:           _create_listen_node,
    LogicalPlanStepType.Unlisten:         _create_unlisten_node,
    LogicalPlanStepType.AlterTriggerOwner: _create_alter_trigger_owner_node,
    LogicalPlanStepType.AlterMaterializedViewOwner: _create_alter_materialized_view_owner_node,
    LogicalPlanStepType.AlterMaterializedViewSuspended: _create_alter_materialized_view_suspended_node,
    LogicalPlanStepType.GrantAccess:      _create_grant_access_node,
    LogicalPlanStepType.RevokeAccess:     _create_revoke_access_node,
    LogicalPlanStepType.CallProcedure:    _create_call_procedure_node,
    LogicalPlanStepType.ShowGrantsOn:     _create_show_grants_on_node,
    LogicalPlanStepType.ShowEffectiveGrantsOn: _create_show_grants_on_node,
}


def create_physical_plan(
    logical_plan, query_properties, plan_context: PlanContext, shared_ctes=None
) -> PhysicalPlan:
    plan = PhysicalPlan()
    registry = get_registry()

    for nid, logical_node in logical_plan.nodes(data=True):
        creator = _DISPATCH.get(logical_node.node_type)
        if creator is None:  # pragma: no cover
            raise InvalidInternalStateError(
                f"Unexpected logical node encountered during physical planning: {logical_node.node_type.name}"
            )
        # The operators that size themselves from a planner estimate get it
        # computed here, from the final plan and its PlanContext statistics.
        node_type = logical_node.node_type
        if node_type == LogicalPlanStepType.Join:
            node = creator(
                logical_node,
                query_properties,
                registry,
                join_output_rows_estimate(logical_node, plan_context),
            )
        elif node_type in (LogicalPlanStepType.AggregateAndGroup, LogicalPlanStepType.Distinct):
            node = creator(
                logical_node,
                query_properties,
                registry,
                group_count_estimate(logical_plan, nid, logical_node),
            )
        else:
            node = creator(logical_node, query_properties, registry)

        # Copy optimizer/binder attached metadata from logical node to physical node
        if logical_node.node_type in steps_with("manifest"):
            node.manifest = logical_node.manifest
        node.uuid = logical_node.uuid

        plan.add_node(nid, node)

    for source, destination, relation in logical_plan.edges():
        plan.add_edge(source, destination, relation)

    # Shared CTE bodies become physical plans of their own, carried on the main
    # physical plan (dependencies first — the plan compiler lowers each body into
    # a producer pipeline before any pipeline that reads it). A body has no Exit
    # node: its head feeds a buffer-append sink, not the output queue.
    plan.shared_ctes = {
        cte_key: create_physical_plan(body, query_properties, plan_context)
        for cte_key, body in (shared_ctes or {}).items()
    }

    return plan
