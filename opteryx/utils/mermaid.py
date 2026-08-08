from opteryx.models import PhysicalPlan
from opteryx.operators.catalog import OperatorCategory, get_registry


def _get_logical_node_type(node):
    # Helper function to get logical node type using catalog category or name heuristics
    try:
        # First, try to use catalog metadata for reliable type detection
        registry = get_registry()
        metadata = registry.get(node.__class__)
        if metadata:
            # Some operators share the SET_OP category but represent
            # different logical relations. Distinct is semantically an
            # aggregate-style deduplication, not a union.
            if metadata.name == "Distinct":
                return "AggregateRel"
            if metadata.name == "Union":
                return "UnionRel"
            category = metadata.category
            category_map = {
                OperatorCategory.SCAN: "ReadRel",
                OperatorCategory.JOIN: "JoinRel",
                OperatorCategory.AGGREGATE: "AggregateRel",
                OperatorCategory.PROJECT: "ProjectRel",
                OperatorCategory.FILTER: "FilterRel",
                OperatorCategory.LIMIT: "LimitRel",
                OperatorCategory.SORT: "SortRel",
                OperatorCategory.SET_OP: "SetRel",
                OperatorCategory.DDL: "DDLRel",
                OperatorCategory.IO: "IRel",
            }
            return category_map.get(category)

        # Fall back to name-based heuristics for operators not in catalog
        if getattr(node, "is_scan", False):
            return "ReadRel"
        if getattr(node, "is_join", False):
            return "JoinRel"

        candidate = getattr(node, "name", None) or getattr(node, "node_type", None)
        if candidate is None:
            return None
        s = str(candidate).lower()
        if "aggregate" in s or "group" in s or "distinct" in s:
            return "AggregateRel"
        if "project" in s or "projection" in s:
            return "ProjectRel"
        if "filter" in s or "where" in s:
            return "FilterRel"
        if "limit" in s:
            return "LimitRel"
        if "sort" in s or "order" in s:
            return "SortRel"
        if "union" in s:
            return "UnionRel"
        if "exit" in s:
            return "ExitRel"
        # default: title-case the candidate and append Rel
        token = str(candidate)
        token = token.replace(" ", "_").replace("-", "_")
        token = token[0].upper() + token[1:] if token else token
        return f"{token}Rel"
    except (AttributeError, ValueError, TypeError):
        return None


def _node_predicates(node):
    """The filter terms a node applies, one entry per ANDed term.

    The two node kinds that filter store it differently: a read node carries
    pushed-down predicates in `predicates`, already split into terms by the
    planner, while a FILTER carries a single expression tree in `filter` that
    may itself be a conjunction. Splitting the latter with the planner's own
    splitter — rather than a second implementation here — means both arrive
    downstream in the same shape, and a nested `(a AND b)` is unwrapped the
    way the planner unwraps it.
    """
    predicates = getattr(node, "predicates", None)
    if predicates:
        return list(predicates)

    expression = getattr(node, "filter", None)
    if expression is None:
        return []
    try:
        from opteryx.expression import NodeType
        from opteryx.planner.optimizer.strategies.split_conjunctive_predicates import (
            _inner_split,
        )

        # NodeType.DNF is, despite the name, this engine's n-ary AND — its
        # `parameters` ARE the conjuncts (see expression/__init__.pyx), and
        # it's the shape a multi-term WHERE actually reaches the physical
        # FILTER in. An OR keeps its own node type and is deliberately left
        # whole: it is one term, and splitting it would misrepresent a
        # disjunction as a conjunction.
        if expression.node_type == NodeType.DNF:
            return list(getattr(expression, "parameters", None) or [expression])
        return _inner_split(expression)
    except Exception:  # pragma: no cover - never let telemetry break the query
        # Unsplit is still correct, just one long term instead of several.
        return [expression]


def _format_expressions(expressions):
    """Render a node's expression list, one entry per expression.

    Used for every list a node exposes — a read node's pushed-down predicate
    terms, an aggregate's functions and its grouping keys. Emitted as a list
    rather than a pre-joined string because the entries ARE the structure,
    and a consumer can't recover them from a joined string: " AND " also
    appears inside string literals and BETWEEN, and a comma appears inside
    any two-argument function call. A reader wanting one line can always
    join them; a reader wanting one entry per row can only do that if they
    arrive separately.
    """
    from opteryx.expression import format_expression

    parts = []
    for expression in expressions:
        # As for node.config: one unformattable entry shouldn't cost the
        # whole list, so skip it and keep the rest.
        try:
            rendered = str(format_expression(expression)).strip()
        except Exception:  # pragma: no cover - defensive, as for node.config
            continue
        if rendered:
            parts.append(rendered)
    return parts


def _describe_columns(node):
    """Per-column name/type/pass-through detail for a Project node.

    ``config`` already carries these names, but only as one comma-joined
    string — a consumer can't recover a column's type from it, and a name
    that itself contains a comma (a two-argument function call, say) can't
    even be split back out reliably. This emits the same list structurally,
    with the resolved type the binder attached to each column.

    Two kinds of column are described, because a Project emits both:

    * the projection — the query's output row;
    * ``passthrough_columns`` — computed and emitted for a consumer above
      (ORDER BY, HAVING), then dropped at the Exit node. They are part of
      what this operator produces, so counting only the projection
      understates its work; they are flagged rather than silently merged so
      a reader can tell which columns survive into the result.

    ``hoisted_columns`` are deliberately excluded: those are computed for
    this node's own internal use and never emitted at all.

    Pass-through columns are read from ``node.parameters`` because
    ProjectionNode.columns deliberately holds only the output projection
    (see projection.pyx).
    """
    from opteryx.expression import format_expression

    parameters = getattr(node, "parameters", None) or {}
    projection = list(getattr(node, "columns", None) or [])
    passthrough = list(parameters.get("passthrough_columns") or [])
    if not projection and not passthrough:
        return []

    details = []
    for column, is_passthrough in [(c, False) for c in projection] + [
        (c, True) for c in passthrough
    ]:
        # One unformattable column shouldn't cost the whole list — skip it and
        # keep the rest, the same way `config` is guarded below.
        try:
            name = str(format_expression(column))
        except Exception:  # pragma: no cover - defensive, as for node.config
            continue
        detail = {"name": name, "passthrough": is_passthrough}
        # `column_type` is the attribute on both SchemaColumn (a plain
        # identifier) and ExpressionColumn (a computed column), and its str()
        # is the display form a reader expects — "VARCHAR", or a fully
        # parameterised "DECIMAL(22, 1)". `type` is checked as a fallback for
        # any column class that names it that way instead.
        schema_column = getattr(column, "schema_column", None)
        column_type = getattr(schema_column, "column_type", None)
        if column_type is None:
            column_type = getattr(schema_column, "type", None)
        if column_type is not None:
            detail["type"] = str(column_type)
        details.append(detail)
    return details


# Exact, architect-specified display label per operator CLASS — distinct from
# _get_logical_node_type's coarse category bucket, which can't tell a Parquet
# scan from a function scan, or a hashed group-by from a plain aggregate.
_OPERATOR_LABELS = {
    "ParquetReadNode": "TABLE SCAN",
    "ReaderNode": "SCAN",
    "NullReaderNode": "SCAN",
    "FunctionDatasetNode": "FUNCTION SCAN",
    "FilterNode": "FILTER",
    "ProjectionNode": "PROJECT",
    "WindowNode": "WINDOW",
    "GroupedAggregateHashedNode": "HASHED AGGREGATE",
    "UngroupedAggregateNode": "UNGROUPED AGGREGATE",
    "DistinctNode": "DISTINCT",
    "SortNode": "SORT",
    "HeapSortNode": "HEAP SORT",
    "LimitNode": "LIMIT",
    "DrakenInnerJoinNode": "HASH JOIN",
    "NestedLoopJoinNode": "NESTED JOIN",
    "AsofJoinNode": "ASOF JOIN",
    "CrossJoinNode": "CROSS JOIN",
    "UnnestJoinNode": "UNNEST JOIN",
    "ExitNode": "EXIT",
}

# OuterJoinNode/FilterJoinNode carry their variant in a dynamic self.join_type
# instead of a fixed class-level one — spelled out to match the parenthetical
# convention (e.g. "OUTER JOIN (LEFT OUTER)", "FILTER JOIN (LEFT ANTI NULL-AWARE)").
_OUTER_JOIN_DIRECTIONS = {
    "left outer": "LEFT OUTER",
    "right outer": "RIGHT OUTER",
    "full outer": "FULL OUTER",
}
_FILTER_JOIN_DIRECTIONS = {
    "left semi": "LEFT SEMI",
    "left anti": "LEFT ANTI",
    "left anti null-aware": "LEFT ANTI NULL-AWARE",
}


def _get_operator_label(node):
    class_name = node.__class__.__name__
    if class_name == "OuterJoinNode":
        direction = _OUTER_JOIN_DIRECTIONS.get(node.join_type, node.join_type)
        return f"OUTER JOIN ({direction})"
    if class_name == "FilterJoinNode":
        direction = _FILTER_JOIN_DIRECTIONS.get(node.join_type, node.join_type)
        return f"FILTER JOIN ({direction})"
    return _OPERATOR_LABELS.get(class_name)


def _collect_node_stats(plan: PhysicalPlan, stats: list = None):
    """Build the per-node stats (keyed by node UID) and edge list for a plan.

    This is the definitive per-node/edge record: native op stats overlaid onto
    the plan-node identity, harvested after the run. As a side effect it
    populates ``node.telemetry.operations`` and the shared ``telemetry.edges``
    list — the structured source consumers should read instead of parsing a
    rendered diagram.

    Returns ``(node_stats_by_nid, node_map, excluded_nodes)`` for callers that
    still need to render a diagram from this data (e.g. EXPLAIN).
    """
    # Map node ids to node objects for telemetry fallbacks
    node_map = {nid: node for nid, node in plan.nodes(True)}

    def get_node_stats(plan: PhysicalPlan):
        stats = []
        for nid, node in plan.nodes(True):
            if node.is_not_explained:
                continue
            node_stat = {
                "identity": node.identity,
                "records_in": node.records_in,
                "bytes_in": node.bytes_in,
                "records_out": node.records_out,
                "bytes_out": node.bytes_out,
                "calls": node.calls,
            }
            # Add sensor readings from the node
            sensors = node.sensors()
            node_stat.update(sensors)

            # Native-engine per-operator telemetry, harvested after the run and keyed by
            # plan-node identity (execute_native). The plan-node Python objects never
            # execute on the native path — the C++ Engine does — so their own counters
            # stay zero; overlay the real readings here.
            native_stats = node.telemetry._reading.get("native_op_stats")
            if native_stats:
                native = native_stats.get(node.identity)
                if native:
                    node_stat.update(native)
                    # self_time == execution_time on the native path: the executor times
                    # each operator's own call (the recursive downstream forward is
                    # excluded), so there is no separate downstream component to subtract.
                    # execution_time/self_time are WALL time (may include a blocked wait,
                    # e.g. a scan's get_morsel() pull) summed across every dop worker
                    # thread — not comparable to time_taken_s. cpu_time_ms is the CPU
                    # actually consumed (CLOCK_THREAD_CPUTIME_ID), also thread-summed,
                    # which is near-zero for an operator that's mostly blocked waiting.
                    node_stat["self_time"] = native["execution_time"]
                    node_stat["cpu_time_ms"] = native["cpu_time"] / 1_000_000

            # Add telemetry-specific readings for reader nodes
            if node.is_scan:
                # These may already be present from sensors() (self.readings — the
                # trampoline scan's ScanReadings, flushed by close_source in the
                # driver teardown). Only fall back to the connector-level telemetry
                # object when sensors() gave nothing, so a real flushed reading is
                # never clobbered back to 0. `bytes_processed` is deliberately absent:
                # on the shared telemetry it is a query-wide total across every scan,
                # so it cannot stand in for one node's bytes. Every scan node records
                # its own into readings.
                for _k in ("rows_read", "blobs_read", "columns_read"):
                    if not node_stat.get(_k):
                        node_stat[_k] = getattr(node.telemetry, _k, 0)
                # columns_read has no ScanReadings field — default to the projected
                # column count (native scans override it from scan_facts below).
                if not node_stat.get("columns_read") and getattr(node, "columns", None):
                    node_stat["columns_read"] = len(node.columns)

                # Native scan path: the Cython ScanReadings above are all zero — the
                # C++ engine scanned, not the Cython node. Overlay the real values:
                # plan-time facts (files/row-groups/columns) harvested by identity,
                # and rows/bytes from the native op-stat counters already overlaid
                # onto this row above. native_scan_facts only carries native-path
                # scans, so trampoline scans keep their own telemetry readings.
                native_scan_facts = node.telemetry._reading.get("native_scan_facts")
                if native_scan_facts:
                    facts = native_scan_facts.get(node.identity)
                    if facts:
                        node_stat["files_read"] = facts["files_read"]
                        # A blob == a file for the parquet scan; mirror files_read
                        # (the Cython blobs_read counter is zero on the native path).
                        node_stat["blobs_read"] = facts["files_read"]
                        node_stat["row_groups_read"] = facts["row_groups_read"]
                        node_stat["row_groups_pruned"] = facts["row_groups_pruned"]
                        node_stat["parquet_rows_before_filter"] = facts["parquet_rows_before_filter"]
                        node_stat["columns_read"] = facts["columns_read"]
                        # No pushed predicates on the native path → every column
                        # read is a projection column, none read only for filtering.
                        node_stat["parquet_projection_columns_read"] = facts["columns_read"]
                        node_stat["rows_read"] = node_stat.get("records_out", 0)
                        node_stat["bytes_processed"] = node_stat.get("bytes_out", 0)

            # Operator config — the per-node human-readable summary each operator
            # already exposes for diagnostics (the FILTER's predicate expression,
            # the LIMIT's "N OFFSET M", the PROJECT's column list, the SORT's
            # order-by keys, ...). It's the one field that tells a reader what a
            # node is actually configured to do, so surface it for every operator.
            # The .config properties are cheap string builders but marked no-cover
            # and a few import/format lazily, so guard against a raising one rather
            # than dropping the whole node's telemetry.
            try:
                config = node.config
            except Exception:  # pragma: no cover - never let one node's config break telemetry
                config = None
            if config:
                node_stat["config"] = str(config)

            # Add node-specific attributes
            if getattr(node, "columns", None):
                node_stat["columns"] = len(node.columns)
            # Only Project nodes: every operator has a column list, but this is
            # the one whose whole job is choosing and shaping them, and emitting
            # per-column detail for a wide scan would bloat the telemetry for no
            # reader. `columns` above stays the output-projection count it has
            # always been; the pass-through columns are additional entries here.
            if _get_logical_node_type(node) == "ProjectRel":
                column_details = _describe_columns(node)
                if column_details:
                    node_stat["column_details"] = column_details
            if getattr(node, "limit", None) is not None:
                node_stat["limit"] = node.limit
            if getattr(node, "predicates", None):
                node_stat["has_filters"] = True
            # `has_filters` says only that a predicate exists; this says what
            # it is, one entry per ANDed term. Emitted for every node that
            # filters — a FILTER, and a read node with predicates pushed into
            # it. A FILTER's `config` is its predicate too, but as one joined
            # string that can't be split back apart; a read node's `config` is
            # the dataset, so there the expression has nowhere else to appear.
            predicates = _node_predicates(node)
            if predicates:
                filters = _format_expressions(predicates)
                if filters:
                    node_stat["filters"] = filters
            # A LIMIT's `config` reads "N OFFSET M" — one string a consumer
            # has to parse to get either number back. `limit` is emitted
            # above; this is its other half. Emitted even when zero, so the
            # absence of an offset is stated rather than inferred from a
            # missing key.
            if getattr(node, "offset", None) is not None:
                node_stat["offset"] = node.offset
            # An aggregate's two lists, kept apart: the functions it computes
            # and the keys it groups by are different things, and its
            # `config` ("AGGREGATE (...) GROUP BY (...)") runs them together
            # into one string. Absent on an ungrouped aggregate, which has
            # functions but no keys.
            aggregates = _format_expressions(getattr(node, "aggregates", None) or [])
            if aggregates:
                node_stat["aggregates"] = aggregates
            groups = _format_expressions(getattr(node, "groups", None) or [])
            if groups:
                node_stat["groups"] = groups
            if node.is_scan:
                # The denominator for `columns_read`: how many columns the
                # relation has, so a reader can see the projection pushdown as
                # a ratio ("2 of 13") rather than a bare count that could mean
                # anything without knowing the table's width.
                schema_columns = getattr(getattr(node, "schema", None), "columns", None)
                if schema_columns:
                    node_stat["columns_total"] = len(schema_columns)
            if getattr(node, "at_date", None):
                node_stat["at_date"] = str(node.at_date)
            if getattr(node, "committed_at", None):
                node_stat["committed_at"] = node.committed_at

            # Field dedup (architect decision): the scan-specific readings duplicate
            # the generic operator counters. Collapse each pair onto the generic
            # survivor — backfilling it from the deprecated field when the survivor
            # is unset (a scan is a source: records_in/bytes_in are 0, so they take
            # the scanned rows/bytes) — then drop the deprecated names.
            #   rows_read       → records_in
            #   bytes_processed → bytes_in
            #   files_read      → blobs_read
            if "rows_read" in node_stat:
                if not node_stat.get("records_in"):
                    node_stat["records_in"] = node_stat["rows_read"]
                node_stat.pop("rows_read", None)
            if "bytes_processed" in node_stat:
                if not node_stat.get("bytes_in"):
                    node_stat["bytes_in"] = node_stat["bytes_processed"]
                node_stat.pop("bytes_processed", None)
            if "files_read" in node_stat:
                if not node_stat.get("blobs_read"):
                    node_stat["blobs_read"] = node_stat["files_read"]
                node_stat.pop("files_read", None)
            # downstream_time is dead on the native path (self_time == execution_time)
            # and only ever nonzero under EXPLAIN ANALYZE tracing — drop it here.
            node_stat.pop("downstream_time", None)

            stats.append(node_stat)
        return stats

    node_stats = {x["identity"]: x for x in get_node_stats(plan)}
    if stats:
        for stat in stats:
            node_stats[stat["identity"]] = stat

    # ExitNode is an internal engine relation, not a user-facing operator — it
    # carries no useful telemetry of its own (a pure pass-through) and every
    # consumer (EXPLAIN's own tree builder, Studio's operator-tree renderer)
    # already splices it out and treats its child as the root. Drop it (and
    # the edge into it) from the structured telemetry entirely, rather than
    # emitting it for every consumer to filter back out. This is scoped to
    # telemetry.operations/.edges only — plan_to_mermaid's own diagram string
    # (EXPLAIN) still draws it, unchanged, via node_stats/excluded_nodes below.
    exit_nids = {nid for nid, node in plan.nodes(True) if node.__class__.__name__ == "ExitNode"}

    # Store detailed stats in telemetry operations with node UID as key and type as field
    for nid, node in plan.nodes(True):
        if not node.is_not_explained and nid not in exit_nids:
            stat = node_stats.get(node.identity)
            if stat:
                # Add node type to the stat dictionary
                node_type = _get_logical_node_type(node)
                if node_type:
                    stat["type"] = node_type
                # Exact operator display label (TABLE SCAN, HASH JOIN, etc.) —
                # separate from the coarse "type" category bucket above.
                operator_label = _get_operator_label(node)
                if operator_label:
                    stat["operator"] = operator_label
                # Remove identity field - it's redundant with the key
                stat.pop("identity", None)
                # Use node UID (nid) as the key
                node.telemetry.operations[nid] = stat

    # Build a structured edge list so consumers can reconstruct the plan DAG
    # without parsing the Mermaid string. Direction: from = producer, to = consumer.
    _raw_edges = list(plan.edges())

    excluded_nodes = []
    _any_telemetry = None
    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            excluded_nodes.append(nid)
            continue
        node_stats[nid] = node_stats.pop(node.identity, None)
        if _any_telemetry is None:
            _any_telemetry = node.telemetry

    # Write the edge list to telemetry now that excluded_nodes is finalised
    if _any_telemetry is not None:
        _any_telemetry.edges = [
            {"from": s, "to": t, **(({"leg": r}) if r else {})}
            for s, t, r in _raw_edges
            if s not in excluded_nodes and t not in excluded_nodes and t not in exit_nids
        ]

    return node_stats, node_map, excluded_nodes


def collect_plan_telemetry(plan: PhysicalPlan, stats: list = None) -> None:
    """Populate node.telemetry.operations/.edges from the plan.

    This is the data source for callers that need the plan's structure and
    metrics (e.g. per-query telemetry sent to worker/jobs) but not a rendered
    diagram — no mermaid string is built here.
    """
    _collect_node_stats(plan, stats)


def plan_to_mermaid(plan: PhysicalPlan, stats: list = None) -> str:
    node_stats, node_map, excluded_nodes = _collect_node_stats(plan, stats)
    builder = ""

    for nid, node in plan.nodes(True):
        if node.is_not_explained:
            continue
        builder += f"  {node.to_mermaid(nid)}\n"
    builder += "\n"

    for s, t, r in plan.edges():
        if t in excluded_nodes:
            continue
        stats_ = node_stats.get(s) or {}
        # Prefer node-specific stats (records_out/bytes_out). Only fall back to
        # the node's telemetry for reader/scan nodes or when the stats are
        # missing/zero. This avoids propagating reader telemetry across
        # non-scan nodes which can produce misleading arrow labels.
        source_node = node_map.get(s)
        records = stats_.get("records_out")
        bytes_ = stats_.get("bytes_out")
        if source_node is not None:
            # Use telemetry only for scan nodes or when summary stats are absent/zero.
            # Only rows: `bytes_processed` is a query-wide connector counter (total
            # bytes fetched from storage, summed across every scan), so using it as
            # this edge's payload size would label one edge with the whole query's IO.
            telemetry_rows = getattr(source_node.telemetry, "rows_read", None)
            if (
                (records is None or records == 0)
                and getattr(source_node, "is_scan", False)
                and telemetry_rows not in (None, 0)
            ):
                records = telemetry_rows

        records = 0 if records is None else records
        bytes_ = 0 if bytes_ is None else bytes_
        join_leg = f"**{r.upper()}**<br />" if r else ""
        builder += (
            f'  NODE_{s} -- "{join_leg} {records:,} rows<br />{bytes_:,} bytes" --> NODE_{t}\n'
        )

    # Add termination node
    exit_points = plan.get_exit_points()
    if exit_points:
        exit_node = plan[exit_points[0]]
        total_duration = sum(node.execution_time for nid, node in plan.nodes(True)) / 1e6
        # Prefer telemetry for final counts when present. `bytes_processed` is NOT
        # usable here: it is a query-wide connector counter (the scan's fetched IO
        # volume), not this node's output size, so the terminus edge takes the exit
        # operator's own bytes_out.
        final_rows = getattr(exit_node.telemetry, "rows_read", None) or exit_node.records_out
        final_bytes = exit_node.bytes_out
        final_columns = len(exit_node.columns) if getattr(exit_node, "columns", None) is not None else 0

        builder += f'  NODE_TERMINUS(["{final_rows} rows<br />{final_columns} columns<br />({total_duration:,.2f}ms)"])\n'

        # Find the node feeding into ExitNode
        ingoing = plan.ingoing_edges(exit_points[0])
        if ingoing:
            source_nid = ingoing[0][0]
            builder += f'  NODE_{source_nid} -- "{final_rows:,} rows<br />{final_bytes:,} bytes" --> NODE_TERMINUS\n'

    return "flowchart LR\n\n" + builder
