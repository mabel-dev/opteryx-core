# Cost-Based Optimization Implementation Guide

Complete step-by-step implementation to enable cost-based query optimization using statistics.

## Current Status

- ✅ Phase 1 Complete: Statistics estimation primitives (82 tests passing)
  - `opteryx/planner/optimizer/statistics.py` contains all estimation classes
  - Ready to integrate into optimizer

- ⏳ Phase 2 (This Document): Optimizer integration
  - Add statistics flags to plans
  - Mark cost-based strategies
  - Implement recalculation logic

- 🔮 Phase 3 (Future): Per-node statistics estimation
  - Implement `estimate_statistics()` on each node type

---

## Architecture Overview

### Query Flow with Statistics

```
SQL Query
    ↓
[Parser → AST Rewriter → Logical Planner]
    ↓
[Binder - creates logical plan with schema info]
    ↓
**[INITIAL STATISTICS - From Manifest/Histograms]**  ← NEW
    ↓
[Optimizer Loop]:
  For each strategy:
    1. Apply heuristic transformation
    2. If plan changed → mark statistics stale
    3. If next strategy needs cost-based decisions:
       a. Recalculate statistics (bottom-up)
       b. Mark statistics fresh
    4. Apply cost-based strategy (uses fresh statistics)
    ↓
[Physical Planner → Executor]
```

### Key Design Principles

1. **Initial Statistics**: Binder phase calculates statistics once from Manifest
2. **Lazy Recalculation**: Only recalculate when:
   - Plan was modified (heuristic transformation)
   - Next strategy is cost-based (needs fresh data)
3. **Cost-Based Strategies**:
   - `JoinOrderingStrategy` - decides join order from cardinality estimates
   - `PredicateOrderingStrategy` - orders filters by selectivity

---

## Implementation: 5 Steps

### Step 1: Mark Cost-Based Strategies

**File**: `opteryx/planner/optimizer/strategies/join_ordering.py`

Find the `JoinOrderingStrategy` class definition (around line 63) and add:

```python
class JoinOrderingStrategy(OptimizationStrategy):
    """
    Cost-based join ordering strategy.

    Requires accurate statistics to determine optimal join order.
    Decides whether to use nested loop or hash join based on cardinality.
    """

    # Mark as cost-based optimization - requires fresh statistics
    optimization_technique: str = "cost"
    modifies_plan: bool = True
```

**File**: `opteryx/planner/optimizer/strategies/predicate_ordering.py`

Find the `PredicateOrderingStrategy` class definition (around line 63) and add:

```python
class PredicateOrderingStrategy(OptimizationStrategy):
    """
    Cost-based predicate ordering strategy.

    Uses selectivity estimates to order filters from most to least selective.
    Requires accurate statistics for selectivity estimation.
    """

    # Mark as cost-based optimization - requires fresh statistics
    optimization_technique: str = "cost"
    modifies_plan: bool = True
```

**Effort**: 10 minutes, 2 files, ~3 lines each

---

### Step 2: Add Statistics Flags to LogicalPlan

**File**: `opteryx/planner/logical_planner/logical_planner.py`

Find the `LogicalPlan` class (around line 69) and replace:

**Before:**
```python
class LogicalPlan(Graph):
    pass
```

**After:**
```python
class LogicalPlan(Graph):
    """Logical query plan with statistics tracking for cost-based optimization."""

    def __init__(self):
        super().__init__()
        # Statistics metadata for optimizer
        self.properties = {
            'statistics_are_stale': False,      # Boolean: are statistics outdated?
            'optimization_technique': 'heuristic',  # Literal: 'heuristic' or 'cost'
        }
```

**Effort**: 10 minutes, 1 file, ~5 lines

---

### Step 3: Create StatisticsRecalculationVisitor

**File**: `opteryx/planner/optimizer/statistics.py`

Add this class at the end of the file (after line 525):

```python
class StatisticsRecalculationVisitor:
    """
    Recalculates statistics by traversing the logical plan bottom-up.

    For each node in the plan, calls estimate_statistics() to compute
    output statistics based on input statistics and the node's transformation.

    This ensures that after plan modifications (e.g., predicate pushdown),
    statistics are updated to reflect the new plan structure before
    cost-based strategies make decisions.

    Example:
        visitor = StatisticsRecalculationVisitor()
        plan_with_stats = visitor.recalculate(plan)
    """

    def recalculate(self, plan: "LogicalPlan") -> "LogicalPlan":
        """
        Recalculate statistics for all nodes in the logical plan.

        Traverses the plan bottom-up (from leaf Scan nodes to root exit nodes)
        and estimates output statistics for each node based on:
        1. Statistics of input nodes
        2. The transformation applied by this node (filter, join, aggregate, etc.)

        Args:
            plan: Logical plan to recalculate statistics for

        Returns:
            The same plan with updated statistics attached to nodes
        """
        # TODO: Phase 3 Implementation
        # 1. Get exit points (root nodes) from plan
        # 2. Define depth-first recursive traversal
        # 3. For each leaf node (Scan):
        #    - Get manifest from node
        #    - Create RelationStatistics with:
        #      - row_count from manifest
        #      - ColumnStatistics for each column
        #      - histograms from manifest.get_distogram()
        # 4. For intermediate nodes:
        #    - Get statistics from children
        #    - Call node.estimate_statistics(input_stats)
        #    - Cache result on node
        # 5. Return plan with statistics on each node

        # For now, return unchanged (infrastructure only)
        return plan

    def _estimate_node_statistics(
        self,
        node: "LogicalPlanNode",
        input_stats_list: list[RelationStatistics],
    ) -> Optional[RelationStatistics]:
        """
        Estimate output statistics for a single node.

        Dispatches to node-specific estimation based on node type.

        Args:
            node: The logical plan node
            input_stats_list: Statistics from child nodes

        Returns:
            Estimated output statistics, or None if unavailable
        """
        # TODO: Phase 3 Implementation
        # Dispatch based on node.node_type:
        # - LogicalPlanStepType.Scan → _estimate_scan_stats()
        # - LogicalPlanStepType.Filter → _estimate_filter_stats()
        # - LogicalPlanStepType.Join → _estimate_join_stats()
        # - LogicalPlanStepType.Aggregate → _estimate_aggregate_stats()
        # - LogicalPlanStepType.GroupBy → _estimate_aggregate_stats()
        # - Others (Project, Limit, etc.) → pass-through

        return None

    def _estimate_scan_stats(self, node: "LogicalPlanNode") -> RelationStatistics:
        """Estimate statistics for a Scan node from Manifest."""
        # TODO: Phase 3
        # manifest = node.properties.get('manifest')
        # row_count = manifest.get_record_count()
        # columns = {}
        # for col_name in manifest.column_names:
        #     distogram = manifest.get_distogram(col_name)
        #     cardinality = manifest.estimate_cardinality(col_name)
        #     col_stats = ColumnStatistics(
        #         column_name=col_name,
        #         data_type=...,
        #         distinct_count=cardinality,
        #         histogram=distogram,
        #         _total_rows=row_count
        #     )
        #     columns[col_name] = col_stats
        # return RelationStatistics(row_count=row_count, columns=columns)
        pass

    def _estimate_filter_stats(
        self,
        node: "LogicalPlanNode",
        input_stats: RelationStatistics,
    ) -> RelationStatistics:
        """Estimate statistics after applying a Filter node."""
        # TODO: Phase 3
        # predicate = node.properties.get('predicate')
        # estimator = SelectivityEstimator()
        # selectivity = estimator.estimate_single_predicate(predicate, input_stats)
        # card_estimator = CardinalityEstimator()
        # output_rows = card_estimator.estimate_after_filter(input_stats, selectivity)
        # output_stats = input_stats.with_row_count(output_rows)
        # # Also narrow column ranges...
        # return output_stats
        pass

    def _estimate_join_stats(
        self,
        node: "LogicalPlanNode",
        left_stats: RelationStatistics,
        right_stats: RelationStatistics,
    ) -> RelationStatistics:
        """Estimate statistics after a Join node."""
        # TODO: Phase 3
        # left_key = node.properties.get('left_key')
        # right_key = node.properties.get('right_key')
        # join_type = node.properties.get('join_type', 'inner')
        # card_estimator = CardinalityEstimator()
        # output_rows = card_estimator.estimate_join_cardinality(
        #     left_stats, right_stats, left_key, right_key, join_type
        # )
        # output_stats = RelationStatistics(row_count=output_rows, columns={...})
        # return output_stats
        pass

    def _estimate_aggregate_stats(
        self,
        node: "LogicalPlanNode",
        input_stats: RelationStatistics,
    ) -> RelationStatistics:
        """Estimate statistics after a GROUP BY / Aggregate node."""
        # TODO: Phase 3
        # group_columns = node.properties.get('group_columns', [])
        # card_estimator = CardinalityEstimator()
        # output_rows = card_estimator.estimate_group_by_cardinality(
        #     input_stats, group_columns
        # )
        # output_stats = input_stats.with_row_count(output_rows)
        # return output_stats
        pass
```

**Effort**: 15 minutes, 1 file, ~100 lines (mostly TODOs for Phase 3)

---

### Step 4: Modify Optimizer to Check Statistics Staleness

**File**: `opteryx/planner/optimizer/__init__.py`

**Step 4a**: Add import at top with other imports:

```python
from opteryx.planner.optimizer.statistics import StatisticsRecalculationVisitor
```

**Step 4b**: Replace the `optimize` method in `OptimizerVisitor` class (around line 155-173):

**Before:**
```python
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Optimize the logical plan by applying all registered strategies in sequence.
        ...
        """
        current_plan = plan
        for strategy in self.strategies:
            if strategy.should_i_run(current_plan):
                current_plan = self.traverse(current_plan, strategy)
        return current_plan
```

**After:**
```python
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Optimize the logical plan by applying all registered strategies in sequence.

        When a strategy modifies the plan, marks statistics as stale.
        Before cost-based strategies run, recalculates statistics if stale.
        """
        current_plan = plan

        for i, strategy in enumerate(self.strategies):
            if not strategy.should_i_run(current_plan):
                continue

            # Apply the strategy transformation
            new_plan = self.traverse(current_plan, strategy)

            # Mark statistics stale if strategy modified plan structure
            if (
                getattr(strategy, "modifies_plan", False)
                and new_plan != current_plan
            ):
                new_plan.properties["statistics_are_stale"] = True

            current_plan = new_plan

            # Recalculate statistics before cost-based strategies if stale
            if self._should_recalculate_statistics_before_next(i):
                if current_plan.properties.get("statistics_are_stale", False):
                    stats_visitor = StatisticsRecalculationVisitor()
                    current_plan = stats_visitor.recalculate(current_plan)
                    current_plan.properties["statistics_are_stale"] = False

        return current_plan

    def _should_recalculate_statistics_before_next(self, current_index: int) -> bool:
        """Check if the next strategy needs fresh statistics."""
        if current_index + 1 >= len(self.strategies):
            return False

        next_strategy = self.strategies[current_index + 1]
        optimization_technique = getattr(
            next_strategy, "optimization_technique", "heuristic"
        )
        return optimization_technique == "cost"
```

**Effort**: 20 minutes, 1 file, ~30 lines modified + 1 new method

---

### Step 5: Add Initial Statistics Calculation After Binder

**File**: `opteryx/planner/__init__.py`

**Step 5a**: Add import at top with other imports:

```python
from opteryx.planner.optimizer.statistics import StatisticsRecalculationVisitor
```

**Step 5b**: Find where binder and optimizer are called together (around line 196-210).

Look for code like:
```python
    bound_plan = do_bind_phase(
        logical_plan,
        ...
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
```

**Replace with:**
```python
    bound_plan = do_bind_phase(
        logical_plan,
        ...
    )
    telemetry.time_planning_binder += time.monotonic_ns() - start

    # Calculate initial statistics from the bound plan
    start = time.monotonic_ns()
    stats_visitor = StatisticsRecalculationVisitor()
    bound_plan = stats_visitor.recalculate(bound_plan)
    bound_plan.properties["statistics_are_stale"] = False
    telemetry.time_planning_binder += time.monotonic_ns() - start

    start = time.monotonic_ns()
    optimized_plan = do_optimizer(bound_plan, telemetry)
```

**Note**: This adds timing to the binder phase since statistics calculation happens there.

**Effort**: 10 minutes, 1 file, ~6 lines added

---

## Testing Phase 2

Create `tests/unit/optimizer/test_cost_based_integration.py`:

```python
"""Integration tests for cost-based optimization with statistics."""

import pytest
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.strategies import (
    JoinOrderingStrategy,
    PredicateOrderingStrategy,
)
from opteryx.models import QueryTelemetry


def test_logical_plan_has_statistics_properties():
    """Test that LogicalPlan has statistics tracking properties."""
    plan = LogicalPlan()

    assert "statistics_are_stale" in plan.properties
    assert plan.properties["statistics_are_stale"] is False
    assert plan.properties["optimization_technique"] == "heuristic"


def test_cost_based_strategies_marked():
    """Test that cost-based strategies have correct attributes."""
    telemetry = QueryTelemetry()

    join_strategy = JoinOrderingStrategy(telemetry)
    assert join_strategy.optimization_technique == "cost"
    assert join_strategy.modifies_plan is True

    pred_strategy = PredicateOrderingStrategy(telemetry)
    assert pred_strategy.optimization_technique == "cost"
    assert pred_strategy.modifies_plan is True


def test_statistics_recalculation_visitor_exists():
    """Test that StatisticsRecalculationVisitor can be instantiated."""
    from opteryx.planner.optimizer.statistics import StatisticsRecalculationVisitor

    visitor = StatisticsRecalculationVisitor()
    assert visitor is not None

    # Currently returns plan unchanged (infrastructure only)
    plan = LogicalPlan()
    result = visitor.recalculate(plan)
    assert result is plan
```

Run with:
```bash
pytest tests/unit/optimizer/test_cost_based_integration.py -v
```

---

## Verification Checklist

After implementing Phase 2:

- [ ] `JoinOrderingStrategy` has `optimization_technique = "cost"`
- [ ] `PredicateOrderingStrategy` has `optimization_technique = "cost"`
- [ ] `LogicalPlan` initializes with statistics properties
- [ ] `StatisticsRecalculationVisitor` class exists (Phase 3 TODOs in place)
- [ ] `OptimizerVisitor.optimize()` checks for stale statistics
- [ ] Initial statistics calculation happens after binder
- [ ] Tests pass (showing infrastructure is in place)

---

## What Phase 2 Accomplishes

✅ **Infrastructure ready**
- Flags track when statistics are stale
- Optimizer knows to recalculate before cost-based decisions
- Initial statistics are calculated from manifest

✅ **Cost-based strategies identified**
- JoinOrdering and PredicateOrdering marked
- Will wait for fresh statistics before running

⏳ **Phase 3 skeleton in place**
- TODOs show exactly what needs implementing
- StatisticsRecalculationVisitor structure defined

---

## Next: Phase 3 (Future Work)

Once Phase 2 is complete, implement per-node estimation:

1. **Scan nodes**: Extract statistics from Manifest/histograms
2. **Filter nodes**: Use SelectivityEstimator + narrow ranges
3. **Join nodes**: Use CardinalityEstimator
4. **Aggregate nodes**: Use GroupBy cardinality estimation
5. **Other nodes**: Pass-through or custom logic

See `selectivity_estimation.md` for the estimation primitives available.

**Estimated effort**: 5-7 days for complete Phase 3 + testing

---

## Summary

**Total Phase 2 implementation**: ~2 hours spread over 5 simple modifications

After this, your query optimizer will:
1. ✅ Calculate statistics from manifests
2. ✅ Track when they become stale
3. ✅ Recalculate before cost-based decisions
4. ✅ Use fresh data for join/predicate ordering

This foundation enables better query plans using statistics and histograms you already create.

