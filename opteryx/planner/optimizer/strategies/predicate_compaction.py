# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Compaction

Type: Heuristic
Goal: Compact multiple predicates on the same column into simplified ranges

This strategy reduces predicate complexity by consolidating multiple conditions
on the same column into a single simplified range or predicate.

Example:
    col > 5 AND col < 10 AND col > 7 AND col < 9
    => col > 7 AND col < 9 (only the most restrictive bounds)

    col > 10 AND col < 5
    => FALSE (contradictory condition)

This enables better predicate pushdown by simplifying the filter expression.
"""

from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


@dataclass
class Limit:
    """Represents a single bound in a value range."""

    value: Optional[int]  # None indicates unbounded
    inclusive: bool  # Whether inclusive (<=, >=, =) or exclusive (<, >)


@dataclass
class ValueRange:
    """Tracks valid range for a column based on multiple predicates."""

    lower: Optional[Limit] = None  # Lower limit of the range
    upper: Optional[Limit] = None  # Upper limit of the range
    untrackable: bool = False  # True if non-numeric predicates mixed in

    def update_with_predicate(self, operator: str, value) -> bool:
        """
        Update range with a new predicate.

        Args:
            operator: One of "=", ">=", "<=", ">", "<"
            value: The literal value to compare against

        Returns:
            True if range is still valid, False if contradiction detected
        """
        # Only handle numeric comparisons. `col = NULL` (and any bound compared
        # against a NULL literal) is not orderable — three-valued SQL semantics
        # mean a NULL bound never participates in range logic — so treat it the
        # same as an unsupported operator: bail range-tracking for this column
        # rather than let a bare Python comparison see a `None`.
        if self.untrackable or value is None or operator not in ("=", ">=", "<=", ">", "<"):
            self.untrackable = True
            return True

        # Create new limit
        new_limit = Limit(value, inclusive=operator in ("=", ">=", "<="))

        # Update lower bound (for >, >=, =)
        if operator in ("=", ">=", ">"):
            if (
                self.lower is None
                or new_limit.value > self.lower.value
                or (
                    new_limit.value == self.lower.value
                    and self.lower.inclusive
                    and not new_limit.inclusive
                )
            ):
                self.lower = new_limit

        # Update upper bound (for <, <=, =)
        if operator in ("=", "<=", "<"):
            if (
                self.upper is None
                or new_limit.value < self.upper.value
                or (
                    new_limit.value == self.upper.value
                    and self.upper.inclusive
                    and not new_limit.inclusive
                )
            ):
                self.upper = new_limit

        # Check for contradictions
        return self._is_valid()

    def _is_valid(self) -> bool:
        """Check if the range is logically valid (no contradictions)."""
        if self.lower is None or self.upper is None:
            return True
        if self.lower.value > self.upper.value:
            return False
        if self.lower.value == self.upper.value:
            # Both bounds at same value - both must be inclusive
            return self.lower.inclusive and self.upper.inclusive
        return True

    def is_equality(self) -> bool:
        """Check if range represents a single value (equality)."""
        if self.lower is None or self.upper is None:
            return False
        return (
            self.lower.value == self.upper.value and self.lower.inclusive and self.upper.inclusive
        )

    def __bool__(self) -> bool:
        """Returns False if range is contradictory."""
        return self._is_valid()

    def __str__(self) -> str:
        """String representation of the range."""
        if self.untrackable:
            return "Unsupported Conditions"
        if not self:
            return "Invalid Range (Contradiction)"

        if self.is_equality():
            return f"= {self.lower.value}"

        _range = ""
        if self.lower is not None:
            _range += f" >{'=' if self.lower.inclusive else ''} {self.lower.value}"
        if self.upper is not None:
            _range += f" <{'=' if self.upper.inclusive else ''} {self.upper.value}"
        return _range.strip()


_TEMPORAL_PHYSICALS = None  # lazily populated from DrakenType on first use


def _literal_domain_key(literal_node) -> Any:
    """Return a key identifying the raw numeric domain of a literal, or None when
    the literal is not temporal.

    Compaction compares literal values against each other (contradiction
    detection, bound selection, equality dedup) using their RAW materialised
    values. Temporal literals materialise into domain-specific integers — a DATE
    literal becomes days-since-epoch, a TIMESTAMP[us] literal becomes
    microseconds-since-epoch. Comparing across domains is meaningless: for
    1975-01-01 vs 1978-01-01 the raw values are 157766400000000 (us) and 2922
    (days), so the *earlier* instant compares as the larger number and the range
    is declared a contradiction — folding the whole filter to FALSE.

    Literals sharing one key are safely order-comparable by their raw values.
    """
    global _TEMPORAL_PHYSICALS
    if _TEMPORAL_PHYSICALS is None:
        from opteryx.types.logical_type import DrakenType

        _TEMPORAL_PHYSICALS = frozenset(
            {
                DrakenType.DATE32,
                DrakenType.TIMESTAMP64,
                DrakenType.TIME32,
                DrakenType.TIME64,
            }
        )

    literal_type = getattr(literal_node, "type", None)
    if literal_type is None:
        return None
    physical = literal_type.physical
    if physical not in _TEMPORAL_PHYSICALS:
        return None
    logical = literal_type.logical
    return (physical, logical.unit if logical is not None else None)


@dataclass
class PredicateOccurrence:
    """Record of a predicate instance within the logical plan."""

    filter_nid: str
    predicate: Node
    operator: str
    value: Any
    domain: Any = None  # `_literal_domain_key` of the literal bound


@dataclass
class BoundCandidate:
    """Potential lower or upper bound for a predicate range."""

    value: Any
    inclusive: bool
    occurrence: PredicateOccurrence


@dataclass
class ColumnAnalysisResult:
    """Outcome of analyzing predicates for a single column."""

    status: str
    required: Optional[List[PredicateOccurrence]] = None
    between_node: Optional[Node] = None  # Set when both bounds compact to a BETWEEN node


class PredicateCompactionStrategy(OptimizationStrategy):  # pragma: no cover
    """
    Compact multiple predicates on the same column into simplified ranges.

    This strategy identifies predicates on the same column and consolidates them
    into a single simplified range by keeping only the most restrictive bounds.

    Example:
        Input:  col > 5 AND col < 10 AND col > 7 AND col < 9
        Output: col > 7 AND col < 9

        Input:  col > 10 AND col < 5
        Output: FALSE (contradiction)
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """Collect filter predicates for later analysis."""
        if node.node_type != LogicalPlanStepType.Filter:
            return context

        state = context.bag.setdefault(
            "predicate_compaction",
            {"filters": {}, "column_occurrences": {}, "filter_chain_roots": {}},
        )

        # Determine which chain this filter belongs to.
        # Traversal is top-down, so the parent node (context.parent_nid) was already
        # visited. If that parent is itself a filter (its node_id appears in
        # filter_chain_roots), we inherit its chain root. Otherwise the parent is the
        # chain boundary and becomes the root.
        #
        # This prevents predicates from different plan branches (e.g. the two sides
        # of an EXCEPT or UNION) being grouped together and incorrectly compacted.
        filter_chain_roots: Dict[str, str] = state["filter_chain_roots"]
        if context.parent_nid in filter_chain_roots:
            chain_root = filter_chain_roots[context.parent_nid]
        else:
            chain_root = context.parent_nid
        filter_chain_roots[context.node_id] = chain_root

        predicates = self._extract_and_predicates(node.condition)

        # Each AND-ed atom may itself be an OR of same-column ranges — try to
        # collapse those before they're fed into the AND-side occurrence tracking
        # below. See `_try_collapse_or_range` for the union-of-ranges algorithm.
        predicates = [self._try_collapse_or_range(predicate) or predicate for predicate in predicates]

        state["filters"][context.node_id] = {"predicates": predicates}

        for predicate in predicates:
            info = self._extract_comparison_info(predicate)
            if not info:
                continue
            column_id, operator, value, domain = info
            # Group by (chain_root, column_id) — predicates on the same column but
            # in different plan branches (different chain_root) stay separate.
            column_key = (chain_root, column_id)
            occurrences: List[PredicateOccurrence] = state["column_occurrences"].setdefault(
                column_key, []
            )
            occurrences.append(
                PredicateOccurrence(
                    filter_nid=context.node_id,
                    predicate=predicate,
                    operator=operator,
                    value=value,
                    domain=domain,
                )
            )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """Analyze collected predicates, removing redundant filters and detecting contradictions."""
        optimized_plan = context.optimized_plan
        if len(optimized_plan) == 0:
            optimized_plan = context.pre_optimized_tree.copy()
            context.optimized_plan = optimized_plan

        state = context.bag.get("predicate_compaction")
        if not state:
            return optimized_plan

        column_occurrences: Dict[str, List[PredicateOccurrence]] = state.get(
            "column_occurrences", {}
        )
        filters_state = state.get("filters", {})

        drop_keys: Set[Tuple[str, int]] = set()
        filters_to_false: Set[str] = set()
        between_replacements: Dict[str, List[Node]] = {}

        for occurrences in column_occurrences.values():
            analysis = self._analyze_column_predicates(occurrences)
            status = analysis.status

            if status == "contradiction":
                filters_to_false.update(occ.filter_nid for occ in occurrences)
                self.telemetry.optimization_predicate_compaction += 1
                self.telemetry.optimization_predicate_compaction_range_simplified += 1
                continue

            if status == "between" and analysis.between_node:
                # Drop every predicate for this column — they're all replaced by BETWEEN.
                filter_nid = occurrences[0].filter_nid
                for occ in occurrences:
                    drop_keys.add((occ.filter_nid, id(occ.predicate)))
                between_replacements.setdefault(filter_nid, []).append(analysis.between_node)
                self.telemetry.optimization_predicate_compaction += 1
                self.telemetry.optimization_predicate_compaction_range_simplified += 1
                continue

            if status != "compacted" or not analysis.required:
                continue

            required_keys = {(occ.filter_nid, id(occ.predicate)) for occ in analysis.required}
            for occ in occurrences:
                key = (occ.filter_nid, id(occ.predicate))
                if key not in required_keys:
                    drop_keys.add(key)

            if len(analysis.required) < len(occurrences):
                removed = len(occurrences) - len(analysis.required)
                self.telemetry.optimization_predicate_compaction += removed
                self.telemetry.optimization_predicate_compaction_range_simplified += 1

        for filter_nid, filter_info in filters_state.items():
            if filter_nid not in optimized_plan:
                continue

            if filter_nid in filters_to_false:
                filter_node = optimized_plan[filter_nid]
                filter_node.condition = Node(NodeType.LITERAL, value=False)
                filter_node.columns = []
                filter_node.relations = set()
                optimized_plan[filter_nid] = filter_node
                continue

            predicates: List[Node] = filter_info.get("predicates", [])
            new_predicates: List[Node] = []
            for predicate in predicates:
                key = (filter_nid, id(predicate))
                if key in drop_keys:
                    continue
                new_predicates.append(predicate.copy())

            # Append BETWEEN nodes that replaced compacted range predicate pairs.
            for between_node in between_replacements.get(filter_nid, []):
                new_predicates.append(between_node)

            if not new_predicates:
                optimized_plan.remove_node(filter_nid, heal=True)
                continue

            new_condition = self._rebuild_filter(new_predicates)
            if new_condition is None:
                optimized_plan.remove_node(filter_nid, heal=True)
                continue

            filter_node = optimized_plan[filter_nid]
            filter_node.condition = new_condition

            identifiers = get_all_nodes_of_type(new_condition, (NodeType.IDENTIFIER,))
            filter_node.columns = identifiers

            relations: Set[str] = set()
            for identifier in identifiers:
                if identifier.source:
                    relations.add(identifier.source)
                schema_column = getattr(identifier, "schema_column", None)
                if schema_column and getattr(schema_column, "origin", None):
                    relations.update(schema_column.origin)
            filter_node.relations = relations

            optimized_plan[filter_nid] = filter_node

        return optimized_plan

    def _analyze_column_predicates(
        self, occurrences: List[PredicateOccurrence]
    ) -> ColumnAnalysisResult:
        """Determine the minimal set of predicates required for a column."""
        if len(occurrences) <= 1:
            return ColumnAnalysisResult(status="unchanged")

        # Every comparison below (contradiction detection, bound selection,
        # equality dedup) orders the literals by their RAW materialised values.
        # That is only meaningful when all of them live in one numeric domain —
        # a days-since-epoch DATE literal and a microseconds-since-epoch
        # TIMESTAMP literal are not comparable as bare integers. Mixed domains:
        # leave every predicate in place. Each compare then evaluates on its own
        # through the unit-aware temporal compare routing.
        if len({occ.domain for occ in occurrences}) > 1:
            return ColumnAnalysisResult(status="unsupported")

        value_range = ValueRange()
        best_lower: Optional[BoundCandidate] = None
        best_upper: Optional[BoundCandidate] = None
        equality_occurrences: List[PredicateOccurrence] = []

        for occurrence in occurrences:
            mapped = self._map_operator(occurrence.operator)
            if mapped is None:
                return ColumnAnalysisResult(status="unsupported")

            # `col = NULL` is not orderable (three-valued semantics) — bail before
            # building a BoundCandidate from it, since `_is_better_lower`/
            # `_is_better_upper` compare `.value` with a bare `>`/`<` and would
            # otherwise see a `None`. ValueRange.update_with_predicate has the same
            # guard for the OR-branch path (`_branch_range`), which never
            # pre-computes bound candidates.
            if occurrence.value is None:
                return ColumnAnalysisResult(status="unsupported")

            if mapped == "=":
                equality_occurrences.append(occurrence)

            if mapped in ("=", ">", ">="):
                candidate = BoundCandidate(
                    value=occurrence.value,
                    inclusive=mapped in ("=", ">="),
                    occurrence=occurrence,
                )
                if self._is_better_lower(candidate, best_lower):
                    best_lower = candidate

            if mapped in ("=", "<", "<="):
                candidate = BoundCandidate(
                    value=occurrence.value,
                    inclusive=mapped in ("=", "<="),
                    occurrence=occurrence,
                )
                if self._is_better_upper(candidate, best_upper):
                    best_upper = candidate

            if not value_range.update_with_predicate(mapped, occurrence.value):
                return ColumnAnalysisResult(status="contradiction")

            if value_range.untrackable:
                return ColumnAnalysisResult(status="unsupported")

        if not value_range:
            return ColumnAnalysisResult(status="contradiction")

        if equality_occurrences:
            equality_value = equality_occurrences[0].value
            matching = [occ for occ in equality_occurrences if occ.value == equality_value]
            if len(matching) == 0:
                return ColumnAnalysisResult(status="contradiction")
            required = [matching[0]]
            status = "compacted" if len(occurrences) > len(required) else "unchanged"
            return ColumnAnalysisResult(status=status, required=required)

        required: List[PredicateOccurrence] = []
        if best_lower:
            required.append(best_lower.occurrence)
        if best_upper and (not required or best_upper.occurrence not in required):
            required.append(best_upper.occurrence)

        # When both a lower and upper bound exist, compact all predicates for this
        # column into a single BETWEEN node regardless of whether individual predicates
        # were already the most restrictive (i.e. even the "unchanged" 2-predicate case).
        if best_lower and best_upper:
            column_node = best_lower.occurrence.predicate.left
            lower_node = best_lower.occurrence.predicate.right
            upper_node = best_upper.occurrence.predicate.right
            between = Node(
                NodeType.BETWEEN,
                left=column_node.copy(),
                right=lower_node.copy(),
                centre=upper_node.copy(),
                # value encodes bound inclusivity: (lower_inclusive, upper_inclusive)
                value=(best_lower.inclusive, best_upper.inclusive),
            )
            return ColumnAnalysisResult(status="between", required=required, between_node=between)

        if not required or len(required) == len(occurrences):
            return ColumnAnalysisResult(status="unchanged", required=required or None)

        return ColumnAnalysisResult(status="compacted", required=required)

    @staticmethod
    def _is_better_lower(candidate: BoundCandidate, current: Optional[BoundCandidate]) -> bool:
        if current is None:
            return True
        if candidate.value > current.value:
            return True
        if candidate.value < current.value:
            return False
        if candidate.inclusive == current.inclusive:
            return False
        return (not candidate.inclusive) and current.inclusive

    @staticmethod
    def _is_better_upper(candidate: BoundCandidate, current: Optional[BoundCandidate]) -> bool:
        if current is None:
            return True
        if candidate.value < current.value:
            return True
        if candidate.value > current.value:
            return False
        if candidate.inclusive == current.inclusive:
            return False
        return (not candidate.inclusive) and current.inclusive

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Only run if there are FILTER clauses in the plan."""
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0

    def _extract_and_predicates(self, node: LogicalPlanNode) -> list:
        """
        Extract all AND-ed predicates from an expression.

        e.g., (A AND B AND C) => [A, B, C]
        """
        if node is None:
            return []

        if node.node_type != NodeType.AND:
            return [node]

        left = self._extract_and_predicates(node.left)
        right = self._extract_and_predicates(node.right)
        return left + right

    def _extract_comparison_info(self, node: LogicalPlanNode) -> Optional[Tuple[str, str, any]]:
        """
        Extract column ID, operator, and value from a comparison node.

        Returns:
            (column_id, operator, value) tuple or None if not a simple comparison
        """
        if node.node_type != NodeType.COMPARISON_OPERATOR:
            return None

        # Must be simple: column OP literal
        if node.left.node_type != NodeType.IDENTIFIER:
            return None

        if node.right.node_type != NodeType.LITERAL:
            return None

        col_id = node.left.schema_column.identity
        operator = node.value
        value = node.right.value
        domain = _literal_domain_key(node.right)

        return (col_id, operator, value, domain)

    def _map_operator(self, sql_operator: str) -> Optional[str]:
        """Map SQL operator to range operator."""
        mapping = {
            "Eq": "=",
            "NotEq": None,  # Can't compact inequality
            "Gt": ">",
            "GtEq": ">=",
            "Lt": "<",
            "LtEq": "<=",
        }
        return mapping.get(sql_operator)

    # -- OR-range union -----------------------------------------------------
    #
    # `col < 4 OR (col >= 4 AND col < 7) OR (col >= 7 AND col < 9)` is a union
    # of contiguous ranges on one column and is equivalent to `col < 9`. The
    # AND-side of this strategy already tracks the tightest lower/upper bound
    # across predicates ANDed together (`ValueRange`); this reuses the same
    # machinery per OR-branch, then merges the resulting ranges.

    def _split_or_unwrap(self, node: Optional[Node]) -> list:
        """Split an OR (and NESTED-OR) node into its flat list of branches."""
        if node is None:
            return []
        if node.node_type == NodeType.NESTED:
            return self._split_or_unwrap(node.centre)
        if node.node_type != NodeType.OR:
            return [node]
        return self._split_or_unwrap(node.left) + self._split_or_unwrap(node.right)

    def _split_and_unwrap(self, node: Optional[Node]) -> list:
        """Split an AND (and NESTED-AND) node into its flat list of conjuncts.

        Distinct from `_extract_and_predicates`: that one is only ever handed the
        top-level filter condition (never NESTED at the root in practice here),
        this one is handed individual OR-branches, which commonly arrive
        parenthesised — e.g. the `(col >= 4 AND col < 7)` branch above — so it
        must see through NESTED wrappers to find the AND underneath.
        """
        if node is None:
            return []
        if node.node_type == NodeType.NESTED:
            return self._split_and_unwrap(node.centre)
        if node.node_type != NodeType.AND:
            return [node]
        return self._split_and_unwrap(node.left) + self._split_and_unwrap(node.right)

    def _branch_range(self, branch: Node) -> Dict[str, Any]:
        """Reduce one OR-branch to a range on a single column.

        Returns a dict with "status" of:
          - "ok": branch is a clean same-column range; lower/upper/col_* set.
          - "contradiction": branch's own bounds can never be satisfied (e.g.
            `col > 10 AND col < 5`) — col_id/domain/col_node are still set so
            the caller can confirm this branch belongs to the same column
            before dropping it.
          - "unsupported": branch isn't a clean single-column range predicate
            (different columns, non-numeric domain, NotEq/LIKE/etc).
        """
        predicates = self._split_and_unwrap(branch)
        value_range = ValueRange()
        col_id = None
        domain = None
        col_node = None
        lower_node = None
        upper_node = None

        for predicate in predicates:
            info = self._extract_comparison_info(predicate)
            if info is None:
                return {"status": "unsupported"}
            p_col_id, operator, value, p_domain = info
            if col_id is None:
                col_id, domain, col_node = p_col_id, p_domain, predicate.left
            elif p_col_id != col_id or p_domain != domain:
                return {"status": "unsupported"}

            mapped = self._map_operator(operator)
            if mapped is None:
                return {"status": "unsupported"}

            prev_lower, prev_upper = value_range.lower, value_range.upper
            if not value_range.update_with_predicate(mapped, value):
                return {
                    "status": "contradiction",
                    "col_id": col_id,
                    "domain": domain,
                    "col_node": col_node,
                }
            if value_range.untrackable:
                return {"status": "unsupported"}
            if value_range.lower is not prev_lower:
                lower_node = predicate.right
            if value_range.upper is not prev_upper:
                upper_node = predicate.right

        if col_id is None:
            return {"status": "unsupported"}

        return {
            "status": "ok",
            "col_id": col_id,
            "domain": domain,
            "col_node": col_node,
            "lower": value_range.lower,
            "lower_node": lower_node,
            "upper": value_range.upper,
            "upper_node": upper_node,
        }

    @staticmethod
    def _ranges_touch(last_upper: Optional[Limit], next_lower: Optional[Limit]) -> bool:
        """Whether two sorted ranges overlap or are adjacent with no gap between them."""
        if last_upper is None or next_lower is None:
            return True
        if next_lower.value < last_upper.value:
            return True
        if next_lower.value == last_upper.value:
            return last_upper.inclusive or next_lower.inclusive
        return False

    @staticmethod
    def _is_bigger_upper(candidate: Optional[Limit], current: Optional[Limit]) -> bool:
        if current is None:
            return False
        if candidate is None:
            return True
        if candidate.value != current.value:
            return candidate.value > current.value
        return candidate.inclusive and not current.inclusive

    def _merge_branch_ranges(self, ranges: list) -> list:
        """Merge overlapping/adjacent same-column ranges into the minimal covering set."""

        def sort_key(r):
            lower = r["lower"]
            if lower is None:
                return (0, 0, 0)
            return (1, lower.value, 0 if lower.inclusive else 1)

        ordered = sorted(ranges, key=sort_key)
        merged = [dict(ordered[0])]
        for current in ordered[1:]:
            last = merged[-1]
            if self._ranges_touch(last["upper"], current["lower"]):
                if self._is_bigger_upper(current["upper"], last["upper"]):
                    last["upper"] = current["upper"]
                    last["upper_node"] = current["upper_node"]
            else:
                merged.append(dict(current))
        return merged

    def _build_range_node(
        self,
        col_node: Node,
        lower: Optional[Limit],
        lower_node: Optional[Node],
        upper: Optional[Limit],
        upper_node: Optional[Node],
    ) -> Node:
        """Build a comparison or BETWEEN node from a merged (lower, upper) range.

        Caller guarantees not both bounds are None (see `_try_collapse_or_range`).
        """
        col = col_node.copy()
        if lower is not None and upper is not None and lower.value == upper.value:
            return Node(NodeType.COMPARISON_OPERATOR, value="Eq", left=col, right=lower_node.copy())
        if lower is None:
            op = "LtEq" if upper.inclusive else "Lt"
            return Node(NodeType.COMPARISON_OPERATOR, value=op, left=col, right=upper_node.copy())
        if upper is None:
            op = "GtEq" if lower.inclusive else "Gt"
            return Node(NodeType.COMPARISON_OPERATOR, value=op, left=col, right=lower_node.copy())
        return Node(
            NodeType.BETWEEN,
            left=col,
            right=lower_node.copy(),
            centre=upper_node.copy(),
            value=(lower.inclusive, upper.inclusive),
        )

    def _try_collapse_or_range(self, node: Node) -> Optional[Node]:
        """Collapse an OR of same-column ranges into a single range predicate (or
        a smaller OR of disjoint ranges), when every branch is a clean range on
        the same column in the same numeric domain.

        e.g. col<4 OR (col>=4 AND col<7) OR (col>=7 AND col<9)  =>  col<9

        Bails (returns None, leaving the OR node untouched) rather than guessing
        when:
          - any branch touches a different column, mixes numeric domains, or
            contains a non-range predicate (NotEq, LIKE, ...) — including a
            "contradiction" branch (self-impossible bounds) that turns out to be
            on a different column: dropping it is only NULL-safe when every
            branch shares one column (see below), so a mismatch bails entirely
            rather than dropping it unchecked.
          - the merged ranges would cover the entire numeric domain. That case
            is semantically `col IS NOT NULL`, not TRUE — collapsing to TRUE
            would wrongly match rows where col IS NULL (the original OR
            evaluates NULL there, because every branch does), so it is left
            alone rather than risk it.

        Dropping a self-contradictory branch (e.g. `col > 10 AND col < 5`) is
        NULL-safe specifically because every surviving branch is on the same
        column: when col IS NULL, the dropped branch would have evaluated to
        NULL (not FALSE), but every remaining branch also evaluates to NULL for
        the same NULL column — so the OR's result (NULL) is unchanged either way.
        """
        unwrapped = node
        while unwrapped is not None and unwrapped.node_type == NodeType.NESTED:
            unwrapped = unwrapped.centre
        if unwrapped is None:
            return None

        # DisjunctionSimplificationStrategy (runs earlier in the pipeline) flattens
        # 3+-branch ORs into a NodeType.CNF node (`.parameters` = flat branch list)
        # for efficient n-ary evaluation; 2-branch ORs are left as plain OR nodes.
        if unwrapped.node_type == NodeType.CNF:
            branches = list(unwrapped.parameters)
        elif unwrapped.node_type == NodeType.OR:
            branches = self._split_or_unwrap(unwrapped)
        else:
            return None

        if len(branches) < 2:
            return None

        ranges = []
        col_id = None
        domain = None
        col_node = None
        for branch in branches:
            result = self._branch_range(branch)
            if result["status"] == "unsupported":
                return None
            if col_id is None:
                col_id, domain, col_node = result["col_id"], result["domain"], result["col_node"]
            elif result["col_id"] != col_id or result["domain"] != domain:
                return None
            if result["status"] == "contradiction":
                continue
            ranges.append(result)

        if not ranges:
            return None

        merged = self._merge_branch_ranges(ranges)

        if not (len(ranges) < len(branches) or len(merged) < len(ranges)):
            return None  # nothing dropped, nothing merged -- no improvement

        new_nodes = []
        for merged_range in merged:
            if merged_range["lower"] is None and merged_range["upper"] is None:
                return None
            new_nodes.append(
                self._build_range_node(
                    col_node,
                    merged_range["lower"],
                    merged_range["lower_node"],
                    merged_range["upper"],
                    merged_range["upper_node"],
                )
            )

        removed = len(branches) - len(new_nodes)
        self.telemetry.optimization_predicate_compaction += removed
        self.telemetry.optimization_predicate_compaction_range_simplified += 1

        if len(new_nodes) == 1:
            return new_nodes[0]
        if len(new_nodes) == 2:
            return Node(NodeType.OR, left=new_nodes[0], right=new_nodes[1])

        # 3+ surviving branches: match DisjunctionSimplificationStrategy's
        # flattened n-ary representation rather than a nested binary OR tree.
        cnf = Node(node_type=NodeType.CNF)
        cnf.parameters = new_nodes
        return cnf

    def _rebuild_filter(self, predicates: list) -> LogicalPlanNode:
        """
        Rebuild filter expression from a list of predicates.

        Args:
            predicates: List of predicate nodes

        Returns:
            AND chain of predicates
        """
        if not predicates:
            return None
        if len(predicates) == 1:
            return predicates[0]

        result = predicates[0]
        for pred in predicates[1:]:
            result = Node(NodeType.AND, left=result, right=pred)
        return result
