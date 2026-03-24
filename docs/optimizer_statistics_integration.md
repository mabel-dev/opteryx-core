# Optimizer Statistics Integration Design

## Overview

This document describes how statistics estimation is integrated into the query optimization pipeline to enable cost-based optimization decisions.

## Architecture

### Query Pipeline

```
SQL → Parser → AST Rewriter → Logical Planner → Binder → Optimizer → Physical Planner → Executor
                                                                ↓
                                                    [Statistics Recalculation]
```

### Statistics Flags on Logical Plan

The `LogicalPlan` carries two metadata flags:

```python
plan.properties['statistics_are_stale'] = False  # Initially fresh after binder
plan.properties['optimization_technique'] = 'heuristic'  # or 'cost'
```

- **`statistics_are_stale`**: Boolean flag indicating if statistics are outdated
  - `False` = statistics are current and valid
  - `True` = plan was modified by a transformation; statistics need recalculation

- **`optimization_technique`**: Literal["heuristic", "cost"]
  - `"heuristic"` = rules-based optimization (predicate pushdown, projection pushdown, etc.)
  - `"cost"` = cost-based optimization (join ordering, predicate ordering)

## Workflow

### 1. Binder Phase (Initial Statistics Calculation)

After binding, compute initial statistics for the entire plan:

```
Binder Output
    ↓
Calculate Statistics (bottom-up from Scans to Exit)
    ↓
Set statistics_are_stale = False
    ↓
Pass to Optimizer
```

**Implementation**:
```python
def do_binder(ast, catalog) -> LogicalPlan:
    plan = bind_plan(ast, catalog)

    # Calculate initial statistics
    stats_calculator = StatisticsRecalculationVisitor()
    plan = stats_calculator.recalculate(plan)

    # Mark statistics as fresh
    plan.properties['statistics_are_stale'] = False

    return plan
```

### 2. Optimizer Phase

The optimizer applies strategies sequentially:

```
for strategy in strategies:
    if strategy.should_i_run(plan):
        # Apply heuristic transformation
        new_plan = apply_strategy(plan)

        # Mark statistics stale if plan changed
        if new_plan != plan:
            new_plan.properties['statistics_are_stale'] = True

        # Recalculate if next strategy needs fresh stats
        if has_cost_based_strategy_next(strategy):
            new_plan = recalculate_statistics(new_plan)
            new_plan.properties['statistics_are_stale'] = False

        plan = new_plan
```

**Cost-Based Strategies** (require fresh statistics):
- `JoinOrderingStrategy` - decides join order based on cardinality and sizes
- `PredicateOrderingStrategy` - orders predicates by estimated selectivity

**Heuristic Strategies** (don't need statistics):
- `PredicatePushdownStrategy` - moves filters down (always safe)
- `ProjectionPushdownStrategy` - moves projections down (always safe)
- `DistinctPushdownStrategy` - moves distinct down (always safe)
- etc.

### 3. Statistics Recalculation

When `statistics_are_stale = True` and next strategy is cost-based:

```
StatisticsRecalculationVisitor.recalculate(plan):
    1. Traverse plan bottom-up (from Scans to Exit)
    2. For each node:
       a. Get statistics from child nodes
       b. Call node.estimate_statistics(child_stats)
       c. Update node's output statistics
    3. Return plan with updated statistics
```

**Time Complexity**: O(n) where n is number of nodes in plan

**Key Nodes That Transform Statistics**:
- `Scan`: Initial statistics from Manifest
- `Filter`: Reduce row count and narrow ranges
- `Join`: Estimate output cardinality
- `Aggregate`: Reduce rows to group cardinality
- `Project`: Keep statistics same (same rows, subset of columns)

## Implementation Details

### Plan Properties

```python
# In LogicalPlan (or added to each plan)
plan.properties = {
    'statistics_are_stale': False,      # Boolean
    'optimization_technique': 'cost',    # Literal["heuristic", "cost"]
    'node_statistics': {...}             # Optional: cached statistics
}
```

### Strategy Declarations

```python
class OptimizationStrategy:
    # Base class attributes
    optimization_technique: Literal["heuristic", "cost"] = "heuristic"
    modifies_plan: bool = False  # Does this strategy change the plan structure?

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Check if strategy should run"""
        pass

    def visit(self, node: LogicalPlanNode, context) -> OptimizerContext:
        """Apply strategy to single node"""
        pass

    def complete(self, plan: LogicalPlan, context) -> LogicalPlan:
        """Finalize strategy on entire plan"""
        pass
```

### Cost-Based Strategies

```python
class JoinOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"  # Requires fresh statistics
    modifies_plan = True            # Changes plan structure

    def should_i_run(self, plan: LogicalPlan) -> bool:
        # Only run if statistics are fresh OR were just recalculated
        return not plan.properties.get('statistics_are_stale', False)

class PredicateOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"  # Requires fresh statistics
    modifies_plan = True

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return not plan.properties.get('statistics_are_stale', False)
```

### Heuristic Strategies

```python
class PredicatePushdownStrategy(OptimizationStrategy):
    optimization_technique = "heuristic"  # Doesn't need statistics
    modifies_plan = True

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return True  # Always runs, doesn't care about statistics
```

## Decision Logic in Optimizer

```python
def optimize(self, plan: LogicalPlan) -> LogicalPlan:
    current_plan = plan

    for i, strategy in enumerate(self.strategies):
        if not strategy.should_i_run(current_plan):
            continue

        # Apply strategy
        new_plan = self.traverse(current_plan, strategy)

        # Did plan change?
        if strategy.modifies_plan and new_plan != current_plan:
            new_plan.properties['statistics_are_stale'] = True

        current_plan = new_plan

        # Check if next strategy needs fresh statistics
        next_strategy = self.strategies[i + 1] if i + 1 < len(self.strategies) else None
        if next_strategy and next_strategy.optimization_technique == "cost":
            if current_plan.properties.get('statistics_are_stale', False):
                # Recalculate statistics before cost-based strategy
                stats_recalc = StatisticsRecalculationVisitor()
                current_plan = stats_recalc.recalculate(current_plan)
                current_plan.properties['statistics_are_stale'] = False

    return current_plan
```

## Benefits

1. **Accuracy**: Statistics are recalculated exactly when needed
2. **Performance**: Avoid recalculating unnecessarily
3. **Correctness**: Cost-based strategies always see fresh statistics
4. **Flexibility**: Easy to identify which strategies are cost-based vs heuristic
5. **Debuggability**: Can track when statistics become stale

## Example Scenario

```
Query: SELECT * FROM users JOIN orders USING (user_id)
       WHERE users.country = 'US'

1. Binder
   - Creates logical plan
   - Calculates initial statistics
   - statistics_are_stale = False

2. Optimizer Strategies:
   a. PredicatePushdown (heuristic)
      - Moves "users.country = 'US'" below join
      - Modifies plan → statistics_are_stale = True

   b. ManifestPruning (heuristic)
      - Prunes files for country column
      - Modifies plan → statistics_are_stale = True

   c. JoinOrdering (cost-based, next)
      - Checks: statistics_are_stale = True
      - Recalculates statistics BEFORE running
      - Now has fresh selectivity estimates for 'country = US'
      - Uses fresh cardinality to decide join order
      - SelectivityEstimator uses histogram if available
      - statistics_are_stale = False

3. Result:
   - Join order based on accurate row counts
   - Small filtered users table becomes left input
   - Efficient join execution
```

## Testing Strategy

1. **Unit Tests**: Test each estimation primitive
2. **Integration Tests**: Test full statistics flow through optimizer
3. **Cost Validation**: Compare estimated vs actual cardinality after execution
4. **Regression Tests**: Ensure no plan corruption during recalculation

## Future Enhancements

1. **Selective Recalculation**: Only recalculate affected subtrees
2. **Caching**: Cache statistics for unchanged plan subtrees
3. **Execution Feedback**: Update histograms based on actual cardinality
4. **Cardinality Anomalies**: Detect and flag estimates that differ from actuals
5. **Per-Strategy Thresholds**: Different dampening factors for different strategies
