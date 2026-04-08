# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet Read Node

SQL Query Execution Plan Node that reads Parquet files using the column-chunk
range-read design (docs/parquet-column-reads-design.md).

Instead of downloading whole blobs into a shared-memory ring, this node:

  1. Fetches the Parquet footer for each file (two small range reads each).
  2. Fans out (file × row-group) work units to a thread pool.
  3. For each unit, batches all projected column ranges into one read_ranges()
     call, decodes with rugo, and yields the assembled row group.

The filesystem layer is taken directly from the connector (every catalog-backed
connector already exposes ``self.filesystem``), so this node works identically
for local disk and GCS.

Row groups are yielded in completion order — the thread pool handles overlap
between I/O and decode across all files and row groups simultaneously.
"""

from __future__ import annotations

import time
from concurrent.futures import as_completed
from copy import deepcopy
from typing import Generator

from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io import InMemoryParquetCache
from opteryx.connectors.parquet_io import fetch_columns
from opteryx.connectors.parquet_io import fetch_footer
from opteryx.connectors.parquet_io import iter_row_groups
from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
from opteryx.connectors.parquet_io.thread_pool_manager import LazyPoolProxy
from opteryx.connectors.parquet_io.thread_pool_manager import get_footer_pool
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.models import QueryProperties
from orso.tools import random_string

from opteryx import EOS
from opteryx import config

from .read_node import ReaderNode

_DATA_FORMAT = "arrow,draken"


def _get_footer_pool():
    """Get footer prefetch pool via thread_pool_manager."""
    return get_footer_pool(max_workers=64)


# Module-level pool proxy: lazy wrapper that always defers to thread_pool_manager cache.
# This ensures that even if pools are shut down (e.g., in tests), the proxy will
# get the fresh recreated pool from the cache on next access.
# Footer reads are I/O-bound (two small range reads per file), so threads scale well
# past cpu_count().
_FOOTER_POOL = LazyPoolProxy(_get_footer_pool)


class ParquetReadNode(ReaderNode):
    """Read node backed by column-chunk range reads via ``parquet_io``.

    Activated for filesystem-backed connectors (GCS, S3, local) when the
    manifest contains only ``.parquet`` files.  Non-parquet external scans are
    rejected by the planner.
    """

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.predicates = parameters.get("predicates")
        self._parquet_files_seen: set = set()
        self._predicate_function_nodes_cached = None  # Cache to avoid AST walking per row group
        self._compiled_predicate_dispatcher = None  # Phase 2: Compiled predicate dispatcher

    def _analyze_predicate_type(self, predicate_root):
        """Analyze predicate structure to determine if it can be compiled.

        Returns a descriptor tuple:
        - ("int64_scalar", column_name, operator, scalar_value) for simple int64 comparisons
        - ("float64_scalar", column_name, operator, scalar_value) for float comparisons
        - ("complex_expression", predicate_root) for complex/unsupported predicates
        - None if unable to analyze
        """
        if predicate_root is None:
            return None

        # Check if it's a simple binary comparison
        if (
            not hasattr(predicate_root, "node_type")
            or predicate_root.node_type != NodeType.COMPARISON_OPERATOR
        ):
            return ("complex_expression", predicate_root)

        left = getattr(predicate_root, "left", None)
        right = getattr(predicate_root, "right", None)
        operator = getattr(predicate_root, "operator", None)

        if left is None or right is None or operator is None:
            return ("complex_expression", predicate_root)

        # Pattern: Column <op> Scalar
        if (
            hasattr(left, "__class__")
            and left.__class__.__name__ == "Identifier"
            and hasattr(right, "value")
            and not hasattr(right, "name")
        ):
            right_value = right.value
            if isinstance(right_value, int):
                return ("int64_scalar", left.name, operator, right_value)
            elif isinstance(right_value, float):
                return ("float64_scalar", left.name, operator, right_value)

        # Pattern: Scalar <op> Column (commute operator)
        if (
            hasattr(right, "__class__")
            and right.__class__.__name__ == "Identifier"
            and hasattr(left, "value")
            and not hasattr(left, "name")
        ):
            left_value = left.value
            if isinstance(left_value, int):
                # Commute the operator for reverse comparison
                commuted_op = self._commute_operator(operator)
                return ("int64_scalar", right.name, commuted_op, left_value)
            elif isinstance(left_value, float):
                commuted_op = self._commute_operator(operator)
                return ("float64_scalar", right.name, commuted_op, left_value)

        # Fall back to complex expression
        return ("complex_expression", predicate_root)

    @staticmethod
    def _commute_operator(op: str) -> str:
        """Commute comparison operators (e.g., a < b becomes b > a)."""
        commute_map = {
            "Lt",
            "Gt",
            "LtEq",
            "GtEq",
            "Eq",
            "NotEq",
        }
        return commute_map.get(op, op)

    def _compile_predicate_dispatcher(self, predicate_root):
        """Generate a specialized predicate evaluation function.

        Returns a callable: morsel -> BoolVector mask

        This moves the dispatch decision from per-row-group evaluation time to
        initialization time, eliminating branches from the hot loop.
        """
        if predicate_root is None:
            # No predicate, return identity mask
            return None

        pred_type = self._analyze_predicate_type(predicate_root)

        if pred_type is None:
            # Can't analyze, fall back to generic
            return None

        if pred_type[0] == "int64_scalar":
            # Compile specialized dispatcher for int64 <op> scalar
            column_name, operator, scalar_value = pred_type[1:4]
            return self._compile_int64_scalar_dispatcher(column_name, operator, scalar_value)
        elif pred_type[0] == "float64_scalar":
            # Compile specialized dispatcher for float64 <op> scalar
            column_name, operator, scalar_value = pred_type[1:4]
            return self._compile_float64_scalar_dispatcher(column_name, operator, scalar_value)

        # Fall back to None for generic path
        return None

    @staticmethod
    def _compile_int64_scalar_dispatcher(column_name: str, operator: str, scalar_value: int):
        """Generate specialized function for: int64_column <op> constant

        Example for Q02 (AdvEngineID <> 0):
          Generated function calls vec.not_equals(0) directly, no dispatch.
        """
        # Map SQL operators to Draken vector methods
        op_map = {
            "Eq",
            "NotEq",
            "Lt",
            "Gt",
            "LtEq",
            "GtEq",
        }

        if operator not in op_map:
            # Can't compile this operator, will use generic fallback
            return None

        method_name = op_map[operator]

        # Generate the specialized function
        def specialized_dispatcher(morsel):
            try:
                vec = morsel.column(column_name)
                # Direct call to Draken method - NO dispatch, NO branches!
                return getattr(vec, method_name)(scalar_value)
            except (KeyError, AttributeError, TypeError):
                # Column not available or type mismatch, this shouldn't happen
                # but we handle it gracefully by returning None (no mask)
                return None

        return specialized_dispatcher

    @staticmethod
    def _compile_float64_scalar_dispatcher(column_name: str, operator: str, scalar_value: float):
        """Generate specialized function for: float64_column <op> constant"""
        op_map = {
            "Eq",
            "NotEq",
            "Lt",
            "Gt",
            "LtEq",
            "GtEq",
        }

        if operator not in op_map:
            return None

        method_name = op_map[operator]

        def specialized_dispatcher(morsel):
            try:
                vec = morsel.column(column_name)
                return getattr(vec, method_name)(scalar_value)
            except (KeyError, AttributeError, TypeError):
                return None

        return specialized_dispatcher

    @property
    def name(self) -> str:  # pragma: no cover
        return "Parquet Read"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.connector.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    def sensors(self):
        base = super().sensors()
        base["row_groups_read"] = self.readings.get("row_groups_read", 0)
        base["row_groups_pruned"] = self.readings.get("parquet_row_groups_pruned", 0)
        base["files_read"] = self.readings.get("files_read", 0)
        base["parquet_filter_columns_read"] = self.readings.get("parquet_filter_columns_read", 0)
        base["parquet_projection_columns_read"] = self.readings.get(
            "parquet_projection_columns_read", 0
        )
        base["parquet_rows_before_filter"] = self.readings.get("parquet_rows_before_filter", 0)
        base["parquet_rows_after_filter"] = self.readings.get("parquet_rows_after_filter", 0)
        rows_before_filter = base["parquet_rows_before_filter"]
        if rows_before_filter > 0:
            base["parquet_filter_selectivity"] = (
                base["parquet_rows_after_filter"] / rows_before_filter
            )
        decode_ns = self.readings.get("time_decoding_blobs", 0)
        if decode_ns > 0 and base["row_groups_read"] > 0:
            base["rowgroups_completed_per_s"] = base["row_groups_read"] / (
                decode_ns / 1_000_000_000
            )
        range_requests = self.readings.get("parquet_range_request_count", 0)
        range_bytes = self.readings.get("parquet_range_bytes_requested", 0)
        if range_requests:
            base["parquet_avg_range_bytes"] = int(range_bytes / range_requests)
        cache_hits = self.readings.get("parquet_column_cache_hits", 0)
        cache_misses = self.readings.get("parquet_column_cache_misses", 0)
        cache_lookups = cache_hits + cache_misses
        if cache_lookups:
            base["parquet_column_cache_hit_ratio"] = cache_hits / cache_lookups
        if base["row_groups_read"] > 0:
            base["parquet_avg_emit_wait_ns"] = (
                self.readings.get("time_parquet_emit_wait_ns", 0) / base["row_groups_read"]
            )
        if self.readings.get("parquet_scan_strategy"):
            base["parquet_scan_strategy"] = self.readings["parquet_scan_strategy"]
        lm_pass1 = self.readings.get("parquet_latmat_pass1_row_groups", 0)
        if lm_pass1 > 0:
            base["parquet_latmat_pass1_row_groups"] = lm_pass1
            base["parquet_latmat_pass2_row_groups"] = self.readings.get(
                "parquet_latmat_pass2_row_groups", 0
            )
            base["parquet_latmat_skipped_row_groups"] = self.readings.get(
                "parquet_latmat_skipped_row_groups", 0
            )
            base["parquet_latmat_abandoned_files"] = self.readings.get(
                "parquet_latmat_abandoned_files", 0
            )
            base["parquet_latmat_pass2_bytes"] = self.readings.get("parquet_latmat_pass2_bytes", 0)
            base["parquet_latmat_skipped_pages"] = self.readings.get(
                "parquet_latmat_skipped_pages", 0
            )
            base["parquet_latmat_decoded_pages"] = self.readings.get(
                "parquet_latmat_decoded_pages", 0
            )
            lm_total_pages = (
                base["parquet_latmat_skipped_pages"] + base["parquet_latmat_decoded_pages"]
            )
            if lm_total_pages > 0:
                base["parquet_latmat_page_skip_ratio"] = (
                    base["parquet_latmat_skipped_pages"] / lm_total_pages
                )
            base["parquet_latmat_skip_ratio"] = (
                self.readings.get("parquet_latmat_skipped_row_groups", 0) / lm_pass1
            )
        return base

    @staticmethod
    def _extract_filter_identities(predicates) -> set[str]:
        if not predicates:
            return set()
        identities = set()
        for predicate in predicates:
            identifiers = get_all_nodes_of_type(predicate, select_nodes=(NodeType.IDENTIFIER,))
            for identifier in identifiers:
                schema_column = getattr(identifier, "schema_column", None)
                identity = getattr(schema_column, "identity", None)
                if identity:
                    identities.add(identity)
        return identities

    @staticmethod
    def _compose_predicates(predicates):
        if not predicates:
            return None

        predicate_nodes = [predicate.copy() for predicate in predicates if predicate is not None]
        if not predicate_nodes:
            return None

        root = predicate_nodes.pop()
        while predicate_nodes:
            right = predicate_nodes.pop()
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
        return root

    def _apply_predicates_to_morsel(self, morsel: Morsel, predicate_root):
        """Apply a predicate tree to a Draken Morsel without Arrow round-trip.

        Evaluates the expression tree natively over Draken vectors and applies
        the resulting BoolVector mask via Morsel.filter_mask.  The eval_schema
        pre-cast is no longer needed: the Draken evaluator handles Date32Vector
        integer encoding directly.
        """
        if predicate_root is None:
            return morsel, morsel.num_rows, morsel.num_rows

        from opteryx.expression.evaluator import evaluate_and_append_draken
        from opteryx.expression.evaluator import evaluate_draken

        rows_before_filter = morsel.num_rows

        # >> Use cached function nodes instead of recalculating (AST walk is O(n) in predicate size)
        # Pre-computed in execute() to avoid 281 redundant AST walks for ClickBench queries
        function_nodes = self._predicate_function_nodes_cached or []
        if function_nodes:
            morsel = evaluate_and_append_draken(function_nodes, morsel)

        # >> Phase 2: Use compiled predicate dispatcher if available (no dynamic dispatch!)
        # Compilation happens once at init, then reused for all 281 row groups
        if self._compiled_predicate_dispatcher:
            mask = self._compiled_predicate_dispatcher(morsel)
        else:
            mask = evaluate_draken(predicate_root, morsel)

        filtered = morsel.filter_mask(mask)
        if filtered.num_rows == 0:
            return morsel.slice(0, 0), rows_before_filter, 0
        return filtered, rows_before_filter, filtered.num_rows

    def execute(self, morsel):
        if morsel == EOS:
            yield None
            return

        base_schema = self.parameters["schema"]
        projected_identities = [column.schema_column.identity for column in (self.columns or [])]
        projected_identity_set = set(projected_identities)

        filter_identity_set = self._extract_filter_identities(self.predicates)
        required_identity_set = projected_identity_set.union(filter_identity_set)
        if not required_identity_set and base_schema.columns:
            # Zero-projection/no-filter scans still need one physical column so row
            # counts flow through the pipeline. Keep output projection empty.
            required_identity_set = {base_schema.columns[0].identity}

        read_schema = deepcopy(base_schema)
        read_schema.columns = [
            column for column in base_schema.columns if column.identity in required_identity_set
        ]

        output_schema = deepcopy(base_schema)
        output_schema.columns = [
            column for column in base_schema.columns if column.identity in projected_identity_set
        ]
        output_identity_order = [column.identity for column in output_schema.columns]
        predicate_root = self._compose_predicates(self.predicates or [])

        # >> OPTIMIZATION: Pre-compute function nodes once instead of per row group
        # This avoids AST walking (O(n) in predicate size) for every row group
        # For queries like Q02 with 281 row groups, this saves significant time
        if self._predicate_function_nodes_cached is None and predicate_root:
            self._predicate_function_nodes_cached = get_all_nodes_of_type(
                predicate_root, select_nodes=(NodeType.FUNCTION,)
            )

        # >> Phase 2 OPTIMIZATION: Compile predicate dispatcher once at init time
        # Instead of dynamic dispatch for every row group, compile to specialized function
        # For Q02, reduces from 281 × 30+ branches to 281 × lambda calls
        if self._compiled_predicate_dispatcher is None and predicate_root:
            self._compiled_predicate_dispatcher = self._compile_predicate_dispatcher(predicate_root)

        # ── Two-pass late materialization column split ────────────────────────
        # pass1_column_names: filter columns only — fetched for every row group.
        # pass2_column_names: projection-only columns — fetched only for row groups
        # that have at least one row surviving the Pass 1 predicate evaluation.
        # Two-pass is skipped when predicates are absent, there are no projection-
        # only columns (e.g. SELECT url WHERE url LIKE …), or the feature is off.
        pass2_identity_set = projected_identity_set - filter_identity_set
        two_pass_eligible = (
            config.features.parquet_late_materialization
            and bool(predicate_root)
            and bool(filter_identity_set)
            and bool(pass2_identity_set)
        )
        pass1_column_names: list = []
        pass2_column_names: list = []
        pass1_name_to_identity: dict = {}
        pass2_name_to_identity: dict = {}
        if two_pass_eligible:
            _p1_cols = [c for c in base_schema.columns if c.identity in filter_identity_set]
            _p2_cols = [c for c in base_schema.columns if c.identity in pass2_identity_set]
            pass1_column_names = [c.name for c in _p1_cols]
            pass2_column_names = [c.name for c in _p2_cols]
            pass1_name_to_identity = {c.name: c.identity for c in _p1_cols}
            pass2_name_to_identity = {c.name: c.identity for c in _p2_cols}

        # ── Empty manifest ────────────────────────────────────────────────────
        if not self.manifest or self.manifest.get_file_count() == 0:
            from orso import DataFrame

            as_arrow = DataFrame(rows=[], schema=output_schema).arrow()
            renames = [output_schema.column(col).identity for col in as_arrow.column_names]
            as_arrow = as_arrow.rename_columns(renames)
            yield as_arrow
            return

        self.readings["columns_read"] += len(read_schema.columns)
        self.readings["parquet_filter_columns_read"] += len(filter_identity_set)
        self.readings["parquet_projection_columns_read"] += len(projected_identity_set)
        self.readings["parquet_rows_before_filter"] += 0
        self.readings["parquet_rows_after_filter"] += 0
        self.readings["parquet_filter_selectivity"] += 0
        self.readings["parquet_range_request_count"] += 0
        self.readings["parquet_range_bytes_requested"] += 0
        self.readings["parquet_footer_bytes"] += 0
        self.readings["parquet_column_cache_hits"] += 0
        self.readings["parquet_column_cache_misses"] += 0
        self.readings["time_parquet_read_ranges_ns"] += 0
        self.readings["time_parquet_decode_columns_ns"] += 0
        self.readings["time_parquet_task_queue_wait_ns"] += 0
        self.readings["time_parquet_task_total_ns"] += 0
        self.readings["time_parquet_footer_fetch_ns"] += 0
        self.readings["time_parquet_scheduler_wait_ns"] += 0
        self.readings["time_parquet_rowgroup_completion_ns"] += 0
        self.readings["time_parquet_emit_wait_ns"] += 0
        self.readings["time_parquet_scheduler_empty_wait_ns"] += 0
        self.readings["parquet_scheduler_empty_wait_events"] += 0
        self.readings["parquet_rowgroup_peak_in_flight_max"] += 0
        self.readings["parquet_ranges_in_flight_peak"] += 0
        self.readings["parquet_active_files_peak"] += 0
        self.readings["parquet_active_rowgroups_peak"] += 0
        self.readings["parquet_emit_queue_depth_at_ready_max"] += 0
        self.readings["time_to_first_rowgroup_ns"] += 0
        self.readings["parquet_row_groups_pruned"] += 0
        self.readings["io_ring_slot_bytes"] += 0
        self.readings["io_ring_slot_count"] += 0
        self.readings["io_ring_total_bytes"] += 0
        self.readings["io_ring_producer_full_wait_ns"] += 0
        self.readings["io_ring_producer_full_wait_events"] += 0
        self.readings["io_ring_consumer_empty_wait_ns"] += 0
        self.readings["io_ring_consumer_empty_wait_events"] += 0
        self.readings["io_transfer_ready_backlog_peak"] += 0
        self.readings["io_transfer_emit_wait_ns"] += 0
        self.readings["io_transfer_fragment_count_p50"] += 0
        self.readings["io_transfer_fragment_count_p95"] += 0
        self.readings["io_transfer_fragment_count_max"] += 0
        self.readings["io_transfer_payload_bytes_p50"] += 0
        self.readings["io_transfer_payload_bytes_p95"] += 0
        self.readings["io_transfer_payload_bytes_max"] += 0
        self.readings["io_rowgroup_slice_count"] += 0
        self.readings["io_deserialize_ns"] += 0
        self.readings["io_serialize_ns"] += 0
        self.readings["parquet_latmat_pass1_row_groups"] += 0
        self.readings["parquet_latmat_pass2_row_groups"] += 0
        self.readings["parquet_latmat_skipped_row_groups"] += 0
        self.readings["parquet_latmat_abandoned_files"] += 0
        self.readings["parquet_latmat_pass2_bytes"] += 0
        self.readings["parquet_latmat_skipped_pages"] += 0
        self.readings["parquet_latmat_decoded_pages"] += 0

        # Phase 1 predicate pushdown: extract (col, op, value) triples from pushed-down
        # predicates so the reader can prune row groups using footer min/max stats.
        predicate_stats = extract_predicate_stats(self.predicates or [])

        records_to_read = self.limit if self.limit is not None else float("inf")

        blob_paths = self.manifest.get_file_paths()
        file_sizes = {}
        files = getattr(self.manifest, "files", None)
        if files:
            for file_entry in files:
                size = getattr(file_entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(file_entry.file_path, size)

        # Resolve the filesystem: connectors that own a pre-configured filesystem
        # (FileSystemConnector subclasses) expose it directly.  For connectors that
        # don't (e.g. OpteryxConnector), derive it from the
        # storage protocol embedded in the file paths.
        if hasattr(self.connector, "filesystem"):
            filesystem = self.connector.filesystem
            connector_type = (
                getattr(self.connector, "storage_type", None) or self.connector.__type__
            )
        else:
            from opteryx.connectors.io_systems import create_filesystem

            first_path = blob_paths[0] if blob_paths else ""
            protocol = first_path.split("://")[0] if "://" in first_path else ""
            filesystem = create_filesystem(protocol)
            connector_type = protocol.upper() if protocol else "FILESYSTEM"
        # Column names as they appear in the Parquet file (Parquet uses the
        # original names, not identity aliases).
        column_names = [col.name for col in read_schema.columns]
        # Map data-file column name → query-engine identity for Morsel construction.
        name_to_identity = {col.name: col.identity for col in read_schema.columns}

        # One cache per execute() call: footers shared across all row groups of
        # the same file; column chunks cached for reuse across row groups with
        # identical content (rare but free).
        cache = InMemoryParquetCache()

        prefetched_footers: dict[str, dict] = {}

        unique_blob_paths = list(dict.fromkeys(blob_paths))
        if unique_blob_paths:
            future_to_path = {
                _FOOTER_POOL.submit(
                    fetch_footer,
                    filesystem,
                    blob_name,
                    cache,
                    file_sizes.get(blob_name),
                ): blob_name
                for blob_name in unique_blob_paths
            }
            for future in as_completed(future_to_path):
                blob_name = future_to_path[future]
                prefetched_footers[blob_name] = future.result()

        result_morsel = None
        two_pass_active = two_pass_eligible
        consecutive_full_pass = 0
        scan_column_names = pass1_column_names if two_pass_eligible else column_names

        decode_start = time.monotonic_ns()
        total_rows_before_filter = 0
        total_rows_after_filter = 0
        try:
            for row_group in iter_row_groups(
                filesystem,
                blob_paths,
                scan_column_names,
                cache,
                predicates=predicate_stats,
                file_sizes=file_sizes or None,
                connector=connector_type,
                query_id=getattr(self.properties, "query_id", None),
                prefetched_footers=prefetched_footers,
            ):
                path = row_group.pop("__path__")
                rg_idx = row_group.pop("__row_group__")
                self.readings["parquet_row_groups_pruned"] = row_group.pop(
                    "__row_groups_pruned__", 0
                )
                self.bytes_in += row_group.pop("__bytes_fetched__", 0)
                self.readings["parquet_footer_bytes"] += row_group.pop("__footer_bytes__", 0)
                self.readings["parquet_range_request_count"] += row_group.pop(
                    "__range_request_count__", 0
                )
                self.readings["parquet_range_bytes_requested"] += row_group.pop(
                    "__range_bytes_requested__", 0
                )
                self.readings["time_parquet_read_ranges_ns"] += row_group.pop(
                    "__time_read_ranges_ns__", 0
                )
                self.readings["time_parquet_decode_columns_ns"] += row_group.pop(
                    "__time_decode_columns_ns__", 0
                )
                self.readings["parquet_column_cache_hits"] += row_group.pop(
                    "__cache_column_hits__", 0
                )
                self.readings["parquet_column_cache_misses"] += row_group.pop(
                    "__cache_column_misses__", 0
                )
                self.readings["time_parquet_task_queue_wait_ns"] += row_group.pop(
                    "__task_queue_wait_ns__", 0
                )
                self.readings["time_parquet_task_total_ns"] += row_group.pop("__task_total_ns__", 0)
                self.readings["time_parquet_footer_fetch_ns"] += row_group.pop(
                    "__footer_fetch_ns__", 0
                )
                self.readings["time_parquet_scheduler_wait_ns"] += row_group.pop(
                    "__scheduler_wait_ns__", 0
                )
                self.readings["time_parquet_rowgroup_completion_ns"] += row_group.pop(
                    "__rowgroup_completion_latency_ns__", 0
                )
                self.readings["parquet_rowgroup_peak_in_flight_max"] = max(
                    self.readings.get("parquet_rowgroup_peak_in_flight_max", 0),
                    row_group.pop("__rowgroup_peak_in_flight__", 0),
                )
                self.readings["parquet_ranges_in_flight_peak"] = max(
                    self.readings.get("parquet_ranges_in_flight_peak", 0),
                    row_group.pop("__ranges_in_flight_peak__", 0),
                )
                self.readings["parquet_active_files_peak"] = max(
                    self.readings.get("parquet_active_files_peak", 0),
                    row_group.pop("__active_files_peak__", 0),
                )
                self.readings["parquet_active_rowgroups_peak"] = max(
                    self.readings.get("parquet_active_rowgroups_peak", 0),
                    row_group.pop("__active_rowgroups_peak__", 0),
                )
                self.readings["parquet_rowgroups_in_flight_cap"] = max(
                    self.readings.get("parquet_rowgroups_in_flight_cap", 0),
                    row_group.pop("__rowgroups_in_flight_cap__", 0),
                )
                self.readings["time_parquet_emit_wait_ns"] += row_group.pop("__emit_wait_ns__", 0)
                self.readings["parquet_emit_queue_depth_at_ready_max"] = max(
                    self.readings.get("parquet_emit_queue_depth_at_ready_max", 0),
                    row_group.pop("__emit_queue_depth_at_ready__", 0),
                )
                self.readings["time_parquet_scheduler_empty_wait_ns"] += row_group.pop(
                    "__scheduler_empty_wait_ns__", 0
                )
                self.readings["parquet_scheduler_empty_wait_events"] += row_group.pop(
                    "__scheduler_empty_wait_events__", 0
                )
                self.readings["io_ring_slot_bytes"] = max(
                    self.readings.get("io_ring_slot_bytes", 0),
                    row_group.pop("__io_ring_slot_bytes__", 0),
                )
                self.readings["io_ring_slot_count"] = max(
                    self.readings.get("io_ring_slot_count", 0),
                    row_group.pop("__io_ring_slot_count__", 0),
                )
                self.readings["io_ring_total_bytes"] = max(
                    self.readings.get("io_ring_total_bytes", 0),
                    row_group.pop("__io_ring_total_bytes__", 0),
                )
                self.readings["io_ring_producer_full_wait_ns"] += row_group.pop(
                    "__io_ring_producer_full_wait_ns__", 0
                )
                self.readings["io_ring_producer_full_wait_events"] += row_group.pop(
                    "__io_ring_producer_full_wait_events__", 0
                )
                self.readings["io_ring_consumer_empty_wait_ns"] += row_group.pop(
                    "__io_ring_consumer_empty_wait_ns__", 0
                )
                self.readings["io_ring_consumer_empty_wait_events"] += row_group.pop(
                    "__io_ring_consumer_empty_wait_events__", 0
                )
                self.readings["io_transfer_ready_backlog_peak"] = max(
                    self.readings.get("io_transfer_ready_backlog_peak", 0),
                    row_group.pop("__io_transfer_ready_backlog_peak__", 0),
                )
                self.readings["io_transfer_emit_wait_ns"] += row_group.pop(
                    "__io_transfer_emit_wait_ns__", 0
                )
                self.readings["io_transfer_fragment_count_p50"] = max(
                    self.readings.get("io_transfer_fragment_count_p50", 0),
                    row_group.pop("__io_transfer_fragment_count_p50__", 0),
                )
                self.readings["io_transfer_fragment_count_p95"] = max(
                    self.readings.get("io_transfer_fragment_count_p95", 0),
                    row_group.pop("__io_transfer_fragment_count_p95__", 0),
                )
                self.readings["io_transfer_fragment_count_max"] = max(
                    self.readings.get("io_transfer_fragment_count_max", 0),
                    row_group.pop("__io_transfer_fragment_count_max__", 0),
                )
                self.readings["io_transfer_payload_bytes_p50"] = max(
                    self.readings.get("io_transfer_payload_bytes_p50", 0),
                    row_group.pop("__io_transfer_payload_bytes_p50__", 0),
                )
                self.readings["io_transfer_payload_bytes_p95"] = max(
                    self.readings.get("io_transfer_payload_bytes_p95", 0),
                    row_group.pop("__io_transfer_payload_bytes_p95__", 0),
                )
                self.readings["io_transfer_payload_bytes_max"] = max(
                    self.readings.get("io_transfer_payload_bytes_max", 0),
                    row_group.pop("__io_transfer_payload_bytes_max__", 0),
                )
                self.readings["io_rowgroup_slice_count"] += row_group.pop(
                    "__io_rowgroup_slice_count__", 0
                )
                self.readings["io_deserialize_ns"] += row_group.pop("__io_deserialize_ns__", 0)
                self.readings["io_serialize_ns"] += row_group.pop("__io_serialize_ns__", 0)
                scan_strategy = row_group.pop("__parquet_scan_strategy__", None)
                if scan_strategy:
                    self.readings["parquet_scan_strategy"] = scan_strategy
                time_to_first_rowgroup_ns = row_group.pop("__time_to_first_rowgroup_ns__", 0)
                if time_to_first_rowgroup_ns:
                    existing = self.readings.get("time_to_first_rowgroup_ns", 0)
                    if existing == 0 or time_to_first_rowgroup_ns < existing:
                        self.readings["time_to_first_rowgroup_ns"] = time_to_first_rowgroup_ns

                # Drop any future scheduler metadata keys without breaking
                # the row payload contract expected below.
                for key in [k for k in row_group if k.startswith("__")]:
                    row_group.pop(key, None)

                # ── Morsel assembly ───────────────────────────────────────────
                if two_pass_eligible:
                    from opteryx.expression.evaluator import evaluate_and_append_draken
                    from opteryx.expression.evaluator import evaluate_draken

                    # Build Pass 1 morsel from filter columns only.
                    p1_identity_names = [pass1_name_to_identity[col] for col in row_group]
                    p1_vectors = list(row_group.values())
                    if not p1_identity_names:
                        continue
                    p1_morsel = Morsel.from_vectors(p1_identity_names, p1_vectors)
                    rows_before_filter = p1_morsel.num_rows

                    # Evaluate predicate to get the raw BoolVector mask.
                    # Handle FUNCTION nodes first (mirrors _apply_predicates_to_morsel).
                    function_nodes = get_all_nodes_of_type(
                        predicate_root, select_nodes=(NodeType.FUNCTION,)
                    )
                    if function_nodes:
                        p1_morsel = evaluate_and_append_draken(function_nodes, p1_morsel)
                    mask = evaluate_draken(predicate_root, p1_morsel)

                    self.readings["parquet_latmat_pass1_row_groups"] += 1

                    # Zero-hit fast path: skip Pass 2 entirely for this row group.
                    # Only applies when the abandonment heuristic has not fired.
                    if two_pass_active and not mask.any():
                        total_rows_before_filter += rows_before_filter
                        self.readings["parquet_latmat_skipped_row_groups"] += 1
                        self.readings["row_groups_read"] = (
                            self.readings.get("row_groups_read", 0) + 1
                        )
                        if path not in self._parquet_files_seen:
                            self._parquet_files_seen.add(path)
                            self.readings["files_read"] = len(self._parquet_files_seen)
                            self.readings["blobs_seen"] += 1
                        continue

                    # Pass 2: fetch projection-only columns for this (path, rg_idx).
                    from array import array as _pyarray
                    _mask_arr = _pyarray('B', (1 if v else 0 for v in mask.to_pylist()))
                    pass2_raw = fetch_columns(
                        filesystem,
                        path,
                        rg_idx,
                        pass2_column_names,
                        cache,
                        connector=connector_type,
                        row_mask=_mask_arr,
                    )
                    p2_bytes = pass2_raw.pop("__bytes_fetched__", 0)
                    self.readings["parquet_latmat_pass2_bytes"] += p2_bytes
                    self.bytes_in += p2_bytes
                    self.readings["parquet_latmat_skipped_pages"] += pass2_raw.pop(
                        "__pages_skipped__", 0
                    )
                    self.readings["parquet_latmat_decoded_pages"] += pass2_raw.pop(
                        "__pages_decoded__", 0
                    )
                    for _k in [k for k in list(pass2_raw) if k.startswith("__")]:
                        pass2_raw.pop(_k)

                    # Pass 1 filtered morsel (K rows).
                    p1_filtered = p1_morsel.filter_mask(mask)

                    # Pass 2 vectors are already K rows (decoder applied the mask).
                    p2_identity_names = [pass2_name_to_identity[col] for col in pass2_raw]
                    result_morsel = Morsel.from_vectors(
                        p1_identity_names + p2_identity_names,
                        [
                            p1_filtered.column(n.encode() if isinstance(n, str) else n)
                            for n in p1_identity_names
                        ]
                        + list(pass2_raw.values()),
                    )
                    rows_after_filter = result_morsel.num_rows

                    self.readings["parquet_latmat_pass2_row_groups"] += 1

                    # Abandonment heuristic: when the predicate is consistently
                    # non-selective, stop skipping Pass 2 for zero-survivor row groups.
                    if two_pass_active:
                        if rows_after_filter == rows_before_filter:
                            consecutive_full_pass += 1
                            if (
                                consecutive_full_pass
                                >= config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER
                            ):
                                two_pass_active = False
                                self.readings["parquet_latmat_abandoned_files"] += 1
                        else:
                            consecutive_full_pass = 0

                else:
                    # Single-pass path: existing behaviour, unchanged.
                    identity_names = [name_to_identity[col] for col in row_group]
                    vectors = list(row_group.values())
                    if not identity_names:
                        # Zero-projection query (e.g. COUNT(*) with a pushed-down WHERE
                        # predicate that stripped all columns).  The reader cannot build a
                        # Morsel with no columns; row-level filtering for this case is an
                        # architectural concern for the planner, not the reader.
                        continue
                    result_morsel = Morsel.from_vectors(identity_names, vectors)
                    rows_before_filter = result_morsel.num_rows
                    rows_after_filter = rows_before_filter
                    if predicate_root is not None:
                        result_morsel, rows_before_filter, rows_after_filter = (
                            self._apply_predicates_to_morsel(
                                result_morsel,
                                predicate_root,
                            )
                        )
                total_rows_before_filter += rows_before_filter
                total_rows_after_filter += rows_after_filter
                if output_identity_order:
                    result_morsel = result_morsel.select(output_identity_order)

                num_rows = result_morsel.num_rows
                self.readings["rows_seen"] += num_rows
                self.readings["row_groups_read"] = self.readings.get("row_groups_read", 0) + 1

                # Track distinct files (rg_idx==0 is the first row group of each file)
                if path not in self._parquet_files_seen:
                    self._parquet_files_seen.add(path)
                    self.readings["files_read"] = len(self._parquet_files_seen)
                    self.readings["blobs_seen"] += 1

                # ── LIMIT enforcement ─────────────────────────────────────────
                if records_to_read < num_rows:
                    result_morsel = result_morsel.slice(0, int(records_to_read))
                    records_to_read = 0
                else:
                    records_to_read -= num_rows

                self.readings["blobs_read"] = len(self._parquet_files_seen)
                self.telemetry.blobs_read = len(self._parquet_files_seen)
                self.readings["rows_read"] += result_morsel.num_rows
                self.telemetry.rows_read += result_morsel.num_rows
                self.readings["bytes_processed"] += result_morsel.nbytes
                self.telemetry.bytes_processed += result_morsel.nbytes

                yield result_morsel

                if records_to_read <= 0:
                    break

        finally:
            decode_ns = time.monotonic_ns() - decode_start
            self.readings["time_decoding_blobs"] = (
                self.readings.get("time_decoding_blobs", 0) + decode_ns
            )
            self.telemetry.time_decoding_blobs += decode_ns
            self.readings["parquet_rows_before_filter"] += total_rows_before_filter
            self.readings["parquet_rows_after_filter"] += total_rows_after_filter
            if total_rows_before_filter > 0:
                self.readings["parquet_filter_selectivity"] = (
                    total_rows_after_filter / total_rows_before_filter
                )

        # ── Empty result guard ────────────────────────────────────────────────
        if result_morsel is None:
            self.readings["empty_datasets"] += 1
            from orso import DataFrame
            yield Morsel.from_arrow(DataFrame(rows=[], schema=output_schema).arrow())
