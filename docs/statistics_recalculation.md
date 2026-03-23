# Statistics Recalculation Design

## Overview

As the query optimizer applies transformations to the logical plan, the statistics attached to each node can become stale. For example, a predicate pushdown strategy that moves a filter closer to the source narrows the cardinality estimates for downstream operators, but those operators' statistics are no longer valid.

Cost-based optimization strategies depend on accurate cardinality and row count estimates to make sound decisions. Without fresh statistics after plan rewrites, these strategies operate with outdated information, leading to suboptimal or incorrect cost comparisons.

This document describes a mechanism to selectively recalculate statistics to keep them fresh before cost-based optimizations, while avoiding the overhead of recalculation after every plan transformation.

## Design Principles

1. **Lazy recalculation**: Only recalculate when necessary (before cost-based decisions)
2. **Explicit tracking**: Use flags to signal when statistics are invalid
3. **Pluggable estimation**: Each operator type can implement custom cardinality logic
4. **Iterative optimization**: Support multiple optimization passes with intermediate recalculation

## Flags

### `statistics_are_stale: bool`

Indicates that the logical plan structure has been modified and node-level statistics are no longer valid.

**Set to `True` when:**
- A node's rewrite method modifies the plan tree (e.g., predicate pushdown, join reordering)
- A transformation changes the cardinality, column bounds, or data flow through a node
- A new node is inserted into the plan

**Set to `False` when:**
- Statistics have just been recalculated
- A node is first created (stats are assumed valid until proven otherwise)

**Semantics**: This flag belongs to the plan, not individual nodes. It indicates whether any node in the tree has rewritten the plan since the last recalculation pass.

### `optimization_technique: Literal["heuristic", "cost"]`

Indicates what type of optimization strategy is about to run.

**`"heuristic"`**: Rule-based strategies that apply transformations regardless of cost
- Examples: Predicate pushdown, column pruning, limit propagation
- Do not require accurate statistics (though they may use them)
- Can be applied freely even if statistics are stale

**`"cost"`**: Cost-based optimization strategies that compare alternatives and choose the lowest-cost option
- Examples: Join ordering, index selection, aggregation placement
- **Require** accurate statistics to make valid comparisons
- Should not run if statistics are stale

## Optimizer Flow

```python
class Optimizer:
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        current_plan = plan
        current_plan.statistics_are_stale = False  # Initial stats assumed valid

        for strategy in self.strategies:
            if not strategy.should_i_run(current_plan):
                continue

            # Recalculate statistics if needed
            if (current_plan.statistics_are_stale and
                strategy.optimization_technique == "cost"):
                current_plan = self.recalculate_statistics(current_plan)
                current_plan.statistics_are_stale = False

            # Apply the strategy
            current_plan = strategy.run(current_plan)

        return current_plan
```

## Strategy Definition

Strategies declare their optimization technique:

```python
class OptimizationStrategy(ABC):
    """Base class for all optimization strategies"""

    # Declare optimization type
    optimization_technique: Literal["heuristic", "cost"] = "heuristic"

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Return True if this strategy should be applied to this plan"""
        pass

    def run(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Apply the strategy and return the modified plan.

        If the method modifies the plan structure, it should set:
            plan.statistics_are_stale = True

        Returns the same plan object (mutated) or a new plan.
        """
        pass
```

## Examples

### Heuristic Strategy (Predicate Pushdown)

```python
class PredicatePushdownStrategy(OptimizationStrategy):
    optimization_technique = "heuristic"  # Doesn't depend on costs

    def run(self, plan: LogicalPlan) -> LogicalPlan:
        # Move filters closer to sources
        for node_id in plan.topological_order(reverse=True):
            node = plan[node_id]
            if isinstance(node, FilterNode):
                # Check if we can push this filter to a child
                pushed = self._try_push_filter(plan, node_id)
                if pushed:
                    plan.statistics_are_stale = True  # Plan structure changed

        return plan
```

### Cost-Based Strategy (Join Ordering)

```python
class JoinOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"  # Depends on accurate statistics

    def run(self, plan: LogicalPlan) -> LogicalPlan:
        # Find join sequences and reorder based on cost
        join_chains = self._find_join_chains(plan)

        for chain in join_chains:
            # Use statistics to estimate costs of different orderings
            best_order = self._find_cheapest_order(chain, plan)

            if best_order != chain.current_order:
                self._reorder_joins(plan, chain, best_order)
                plan.statistics_are_stale = True  # Plan structure changed

        return plan
```

## Statistics Recalculation

The recalculation pass traverses the plan bottom-up (from sources toward the exit), computing output statistics for each node based on input statistics:

```python
class StatisticsRecalculationPass:
    def recalculate(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Traverse the plan bottom-up and recalculate output statistics for each node.
        Returns the plan with fresh statistics.
        """

        # Process nodes in topological order (sources first)
        for node_id in plan.topological_order():
            node = plan[node_id]

            # Gather input statistics from child nodes
            input_stats = []
            for child_id in plan.children(node_id):
                child = plan[child_id]
                input_stats.append(child.output_statistics)

            # Estimate output statistics for this node
            if len(input_stats) == 0:
                # Source node (Scan, etc.)
                node.output_statistics = node.estimate_statistics(None)
            elif len(input_stats) == 1:
                # Single-input node (Filter, Project, etc.)
                node.output_statistics = node.estimate_statistics(input_stats[0])
            else:
                # Multi-input node (Join, Union, etc.)
                node.output_statistics = node.estimate_statistics(*input_stats)

        return plan
```

## Operator Estimation Methods

Each logical plan node type implements `estimate_statistics()`:

```python
class LogicalPlanNode(ABC):
    def estimate_statistics(self, input_stats: RelationStatistics |
                           tuple[RelationStatistics, ...] | None) -> RelationStatistics:
        """
        Given input statistics, estimate output statistics for this node.

        Args:
            input_stats: Statistics from input node(s), or None for source nodes

        Returns:
            RelationStatistics object with estimated row count, column bounds, cardinalities
        """
        pass
```

### Example Implementations

**Scan Node**:
```python
class ScanNode(LogicalPlanNode):
    def estimate_statistics(self, input_stats: None) -> RelationStatistics:
        # Statistics come from the data source (manifest, file metadata, etc.)
        if self.manifest:
            return self.manifest.get_statistics()
        return RelationStatistics()  # Unknown stats
```

**Filter Node**:
```python
class FilterNode(LogicalPlanNode):
    def estimate_statistics(self, input_stats: RelationStatistics) -> RelationStatistics:
        # Estimate selectivity of the predicate
        selectivity = self._estimate_selectivity(self.predicate, input_stats)

        output = input_stats.copy()
        output.row_count = int(input_stats.row_count * selectivity)

        # Narrow column bounds based on filter conditions
        for col, bounds in self._extract_bounds(self.predicate):
            output.update_bounds(col, bounds)

        return output
```

**Join Node**:
```python
class JoinNode(LogicalPlanNode):
    def estimate_statistics(self, left_stats: RelationStatistics,
                           right_stats: RelationStatistics) -> RelationStatistics:
        # Estimate output cardinality using join selectivity
        left_card = left_stats.cardinality(self.left_key)
        right_card = right_stats.cardinality(self.right_key)

        join_selectivity = 1.0 / max(left_card, right_card, 1)

        output = RelationStatistics()
        output.row_count = int(left_stats.row_count * right_stats.row_count * join_selectivity)

        # Bounds are union of left and right columns
        output.bounds = {**left_stats.bounds, **right_stats.bounds}

        return output
```

**Aggregate Node**:
```python
class AggregateNode(LogicalPlanNode):
    def estimate_statistics(self, input_stats: RelationStatistics) -> RelationStatistics:
        output = RelationStatistics()

        if self.group_by_columns:
            # Output row count ≈ product of group column cardinalities
            group_cardinality = 1
            for col in self.group_by_columns:
                col_card = input_stats.cardinality(col)
                group_cardinality *= col_card

            output.row_count = min(group_cardinality, input_stats.row_count)
        else:
            # GROUP BY with no columns returns 1 row
            output.row_count = 1

        return output
```

## Optimization Pipeline Example

Consider a query with filters and joins:

```
SELECT * FROM a
JOIN b ON a.id = b.a_id
WHERE a.age > 30 AND b.status = 'active'
```

Initial logical plan:
```
Filter(b.status = 'active')
  └─ Filter(a.age > 30)
      └─ Join(a.id = b.a_id)
          ├─ Scan(a)
          └─ Scan(b)
```

**Pass 1: Predicate Pushdown** (heuristic)
```
step 1. should_i_run(plan) → True
step 2. optimization_technique = "heuristic" → No need to recalculate
step 3. run(plan):
   - Move filters down the tree
   - plan.statistics_are_stale = True
   - Return modified plan
```

After Pass 1:
```
Join(a.id = b.a_id)
├─ Filter(a.age > 30)
│  └─ Scan(a)
└─ Filter(b.status = 'active')
   └─ Scan(b)
```

**Pass 2: Join Ordering** (cost-based)
```
step 1. should_i_run(plan) → True
step 2. optimization_technique = "cost"
step 3. plan.statistics_are_stale == True → Recalculate!
   - Traverse bottom-up: Scan(a) → Filter → Scan(b) → Filter → Join
   - Each node estimates output based on input
   - Filter(a.age > 30) narrows row count for table a
   - Filter(b.status = 'active') narrows row count for table b
   - Join sees accurate cardinality estimates
step 4. Now run(plan):
   - Compare costs of different join orders with accurate stats
   - Choose optimal order
   - plan.statistics_are_stale = True (plan changed)
```

**Pass 3: Other Heuristics** (no recalculation needed if no cost passes follow)
```
step 1. should_i_run(plan) → True (e.g., column pruning)
step 2. optimization_technique = "heuristic" → No need to recalculate
step 3. run(plan) → Modify plan
```

## When to Set `statistics_are_stale = True`

Set this flag whenever a node's rewrite method modifies the plan tree:

- **Predicate Pushdown**: When filters are moved to different positions
- **Join Reordering**: When join order is changed
- **Limit Propagation**: When limit constraints are pushed down
- **Column Pruning**: When columns are removed (may affect intermediate cardinalities)
- **Subquery Unnesting**: When subquery structure is flattened
- **Aggregate Elimination**: When aggregations are removed or combined

Do **not** set this flag for:
- Statistics-only updates (new statistics, same plan structure)
- Cosmetic changes that don't affect cardinality or data flow

## Cost Model Integration

Once function costs are available in the catalog, operators can incorporate actual CPU/memory costs:

```python
class FilterNode(LogicalPlanNode):
    def estimate_cost(self, input_stats: RelationStatistics) -> OperatorCost:
        rows_in = input_stats.row_count
        rows_out = self.estimate_statistics(input_stats).row_count

        # Cost = cost to evaluate predicate on all input rows
        predicate_cost = self.predicate.get_evaluation_cost(rows_in)

        return OperatorCost(
            cpu_cost=predicate_cost,
            output_rows=rows_out
        )
```

Cost-based strategies can then compare alternatives:

```python
class JoinOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"

    def _find_cheapest_order(self, join_chain, plan):
        # Try different join orders
        costs = {}
        for permutation in join_chain.permutations():
            estimated_cost = self._estimate_join_chain_cost(permutation, plan)
            costs[permutation] = estimated_cost

        return min(costs, key=costs.get)
```

## Future Work

1. **Adaptive statistics**: Update cardinality estimates as data flows through execution
2. **Statistics caching**: Cache expensive estimations across optimization passes
3. **Predicate analysis**: Improve selectivity estimation using histogram data
4. **Cost calibration**: Collect actual execution costs and tune estimation models
5. **Multi-pass optimization**: Iterate: transform → recalculate → transform → ... until converged

## References

- Postgres planner: Uses `set_planning_option()` to recalculate plans between passes
- Volcano optimizer: Cascades optimization with multiple derivation steps and memoization
- Calcite optimizer: Explicit cost model and statistics propagation in Volcano framework
