# Licensed under the Apache License, Version 2.0 (the "License");
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Draken-native inner join node.

This node is deliberately narrower than the legacy Arrow-first inner join:
- it keeps both sides in Draken morsels
- it uses the compiled Carchar join state directly
- it aligns output with Draken align_tables

Unsupported shapes fail in the physical planner rather than adding more
Arrow conversions here.
"""
from typing import Generator, Optional

import time
from threading import Lock

from opteryx.compiled.joins import build_side_carchar_morsel_map
from opteryx.compiled.joins import get_last_draken_inner_join_metrics
from opteryx.compiled.joins import inner_join_carchar_morsel_aligned
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import evaluate_and_append_draken
from opteryx.models import QueryProperties

from opteryx import EMPTY
from opteryx import EOS
from opteryx import config

from . import JoinNode


class DrakenInnerJoinNode(JoinNode):
    join_type = "inner"

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.left_columns = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])
        self.on = parameters.get("on")
        self.columns = parameters.get("columns")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

        self.left_morsel = None
        self.left_morsels = []
        self.left_hash = None
        self.left_is_empty = False
        self.lock = Lock()
        self._build_phase = True
        self.carchar_probe_load_factor = float(
            config.get("FEATURE_CARCHAR_PROBE_LOAD_FACTOR", 0.35)
        )

    @staticmethod
    def supports(**parameters) -> bool:
        on = parameters.get("on")
        if on is None:
            return True

        left_relation_names = set(parameters.get("left_relation_names") or [])
        right_relation_names = set(parameters.get("right_relation_names") or [])
        comparisons = get_all_nodes_of_type(on, (NodeType.COMPARISON_OPERATOR,))
        if not comparisons:
            return False

        for comparison in comparisons:
            if comparison.value != "Eq":
                return False
            if comparison.left is None or comparison.right is None:
                return False
            if comparison.left.node_type != NodeType.IDENTIFIER:
                return False
            if comparison.right.node_type != NodeType.IDENTIFIER:
                return False
            if not comparison.left.schema_column or not comparison.right.schema_column:
                return False

            left = comparison.left
            right = comparison.right
            if left.source in left_relation_names and right.source in right_relation_names:
                left_type = left.schema_column.type
                right_type = right.schema_column.type
            elif left.source in right_relation_names and right.source in left_relation_names:
                left_type = right.schema_column.type
                right_type = left.schema_column.type
            else:
                return False

            if (
                left_type != right_type
                and JoinNode._join_numeric_target_arrow_type(left_type, right_type) is not None
            ):
                return False

        return True

    @property
    def name(self):  # pragma: no cover
        return "Inner Join Draken"

    @property
    def config(self):  # pragma: no cover
        return "draken+carchar"

    @staticmethod
    def _iter_morsels(morsel_or_iterable):
        if isinstance(morsel_or_iterable, Morsel):
            yield morsel_or_iterable
            return
        for morsel in morsel_or_iterable:
            if morsel is None or morsel is EMPTY:
                continue
            yield morsel

    @staticmethod
    def _encode_columns(columns):
        encoded = []
        for column in columns:
            if isinstance(column, bytes):
                encoded.append(column)
            else:
                encoded.append(str(column).encode("utf8"))
        return encoded

    def _collect_expression_nodes_for_side(self, relation_names):
        """Collect ON-clause expressions that should be evaluated on one side."""
        if not self.on:
            return []

        exprs = []
        comparisons = get_all_nodes_of_type(self.on, (NodeType.COMPARISON_OPERATOR,))
        side_relations = set(relation_names)

        for comparison in comparisons:
            if comparison.value != "Eq":
                continue
            left = comparison.left
            right = comparison.right

            def _refs_only(node):
                rels = getattr(node, "relations", None)
                if not rels:
                    return False
                return side_relations.issuperset(set(rels))

            if left is not None and left.node_type != NodeType.IDENTIFIER and _refs_only(left):
                exprs.append(left)
            if right is not None and right.node_type != NodeType.IDENTIFIER and _refs_only(right):
                exprs.append(right)

        return exprs

    def _project_morsel(self, morsel: Morsel, keep_names) -> Morsel:
        encoded_keep = [name if isinstance(name, bytes) else name.encode("utf8") for name in keep_names]
        available = set(morsel.column_names)
        selected = [name for name in encoded_keep if name in available]
        if not selected or len(selected) == len(morsel.column_names):
            return morsel
        self.readings["feature_eliminate_join_columns_draken"] += 1
        return morsel.select(selected)

    def _append_left_morsel(self, morsel: Morsel) -> None:
        self.left_morsels.append(morsel)

    def execute(self, Morsel morsel):
        with self.lock:
            if self._build_phase:
                if morsel == EOS:
                    self._build_phase = False
                    if not self.left_morsels:
                        self.left_is_empty = True
                        yield None
                        return

                    start = time.monotonic_ns()
                    self.left_morsel = Morsel.combine(self.left_morsels)
                    self.readings["time_inner_join_left_combine"] += time.monotonic_ns() - start
                    self.left_morsels = []

                    left_exprs = self._collect_expression_nodes_for_side(self.left_relation_names)
                    if left_exprs and self.left_morsel.num_rows > 0:
                        old_cols = set(self.left_morsel.column_names)
                        try:
                            self.left_morsel = evaluate_and_append_draken(
                                left_exprs, self.left_morsel
                            )
                        except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
                            raise UnsupportedSyntaxError(
                                f"Draken inner join expression evaluation does not support this query shape: {err}"
                            ) from err
                        new_cols = set(self.left_morsel.column_names) - old_cols
                        if new_cols:
                            for col in new_cols:
                                if col not in self.left_columns:
                                    self.left_columns.append(col)

                    if self.columns is not None and self.left_morsel.num_rows > 0:
                        candidate_names = [c.schema_column.identity for c in self.columns]
                        available_cols = set(self.left_morsel.column_names)
                        left_keep = [name for name in candidate_names if name in available_cols]
                        for join_col in self.left_columns:
                            join_bytes = join_col if isinstance(join_col, bytes) else str(join_col).encode("utf8")
                            if join_bytes not in left_keep:
                                left_keep.append(join_bytes)
                        if left_keep:
                            self.left_morsel = self._project_morsel(self.left_morsel, left_keep)

                    start = time.monotonic_ns()
                    self.left_hash = build_side_carchar_morsel_map(
                        self.left_morsel,
                        self.left_columns,
                        self.carchar_probe_load_factor,
                    )
                    self.readings["time_inner_join_build_side_hash_map"] += (
                        time.monotonic_ns() - start
                    )
                    self.readings["feature_inner_join_backend_carchar"] += 1
                    self.readings["feature_inner_join_draken"] += 1
                    (
                        _hash_time,
                        _probe_time,
                        _bloom_time,
                        _rows_hashed,
                        _candidate_rows,
                        _matched_rows,
                        _materialize_time,
                        _align_time,
                        _rows_eliminated,
                        bloom_build_time,
                        build_unique_keys,
                        build_total_rows,
                        build_avg_chain_length,
                    ) = get_last_draken_inner_join_metrics()
                    if self.left_hash.has_bloom_filter():
                        self.readings["feature_bloom_filter"] += 1
                        self.readings["time_build_bloom_filter"] += bloom_build_time
                    # Adaptive join statistics (Phase 1, per docs/adaptive_join_statistics.md):
                    # surface chain-length distribution from the build side.
                    self.readings["build_unique_keys"] += build_unique_keys
                    self.readings["build_total_rows"] += build_total_rows
                    # avg chain length is per-build; we report the latest build's value
                    # rather than summing across multiple builds in a join chain.
                    self.readings["build_avg_chain_length"] = build_avg_chain_length
                    yield None
                    return

                for chunk in self._iter_morsels(morsel):
                    if chunk.num_rows == 0:
                        continue
                    start = time.monotonic_ns()
                    self._append_left_morsel(chunk)
                    self.readings["time_inner_join_left_accumulate"] += time.monotonic_ns() - start
                yield None
                return

            else:
                if morsel == EOS:
                    yield EOS
                    return

                if self.left_is_empty:
                    yield EMPTY
                    return

                produced = False
                for chunk in self._iter_morsels(morsel):
                    if chunk.num_rows == 0:
                        continue

                    right_chunk = chunk
                    right_exprs = self._collect_expression_nodes_for_side(self.right_relation_names)
                    if right_exprs and right_chunk.num_rows > 0:
                        old_cols = set(right_chunk.column_names)
                        try:
                            right_chunk = evaluate_and_append_draken(right_exprs, right_chunk)
                        except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
                            raise UnsupportedSyntaxError(
                                f"Draken inner join expression evaluation does not support this query shape: {err}"
                            ) from err
                        new_cols = set(right_chunk.column_names) - old_cols
                        if new_cols:
                            for col in new_cols:
                                if col not in self.right_columns:
                                    self.right_columns.append(col)
                    if self.columns is not None:
                        candidate_names = [c.schema_column.identity for c in self.columns]
                        available_cols = set(right_chunk.column_names)
                        right_keep = [name for name in candidate_names if name in available_cols]
                        for join_col in self.right_columns:
                            join_bytes = join_col if isinstance(join_col, bytes) else str(join_col).encode("utf8")
                            if join_bytes not in right_keep:
                                right_keep.append(join_bytes)
                        if right_keep:
                            right_chunk = self._project_morsel(right_chunk, right_keep)

                    start = time.monotonic_ns()
                    aligned = inner_join_carchar_morsel_aligned(
                        self.left_morsel,
                        right_chunk,
                        self.right_columns,
                        self.left_hash,
                    )
                    total_join_ns = time.monotonic_ns() - start

                    (
                        hash_time,
                        probe_time,
                        bloom_time,
                        rows_hashed,
                        candidate_rows,
                        matched_rows,
                        materialize_time,
                        align_time,
                        rows_eliminated_by_bloom_filter,
                        _bloom_build_time,
                        _build_unique_keys,
                        _build_total_rows,
                        _build_avg_chain_length,
                    ) = get_last_draken_inner_join_metrics()
                    self.readings["time_inner_join_hash"] += hash_time
                    self.readings["time_inner_join_probe"] += probe_time
                    self.readings["time_inner_join_indices"] += materialize_time
                    self.readings["time_bloom_filtering"] += bloom_time
                    self.readings["rows_inner_join_hashed"] += rows_hashed
                    self.readings["rows_inner_join_candidates"] += candidate_rows
                    self.readings["rows_inner_join_matched"] += matched_rows
                    self.readings["rows_eliminated_by_bloom_filter"] += (
                        rows_eliminated_by_bloom_filter
                    )
                    self.readings["time_inner_join_total_kernel"] += total_join_ns
                    self.readings["time_inner_join_align"] += align_time
                    if aligned is not None:
                        produced = True
                        yield aligned

                if not produced:
                    yield EMPTY
