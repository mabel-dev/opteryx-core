# Selectivity and Cardinality Estimation

## Overview

Cost-based optimization requires accurate estimates of how many rows will flow through each operator. This document describes the estimation primitives for:

1. **Single predicate selectivity**: Given a filter, estimate what fraction of rows will pass through
2. **Multiple predicate selectivity**: Combine multiple filters, accounting for correlation
3. **Cardinality estimation**: Predict output row counts for joins, aggregates, etc.
4. **Range narrowing**: Predict new column bounds after filtering

## Selectivity with Exponential Dampening

The core insight is that **predicates on different columns are not independent**. After filtering on column A, the remaining rows have a different distribution, and filters on column B won't be as selective as the original histogram suggests.

### The Dampening Factor

When applying multiple predicates, each successive predicate is dampened:

```python
estimator = SelectivityEstimator(dampening_factor=0.75)

# First predicate: use histogram-based selectivity
# s1 = histogram.selectivity(col_a > 50) = 0.20 (20%)

# Second predicate: dampened
# s2_raw = histogram.selectivity(col_b < 100) = 0.30
# s2_damped = s2_raw * 0.75 = 0.225 (22.5% instead of 30%)

# Third predicate: also dampened
# s3_raw = histogram.selectivity(col_c = 'active') = 0.15
# s3_damped = s3_raw * 0.75 = 0.1125 (11.25% instead of 15%)

# Combined selectivity
combined = 0.20 * 0.225 * 0.1125 ≈ 0.00506 (0.5%)
```

### Why Dampening Works

- **First predicate**: Uses actual distribution from histogram → accurate estimate
- **Second predicate**: After first filter, rows remaining may have non-uniform distribution across column B
  - Correlation assumption: subsequent filters are less selective than they appear
  - Dampening factor (0.75) = we assume 75% of the predicted selectivity

- **Each successive predicate**: Further dampening because we have less certainty about distributions after multiple filters

### Choosing the Dampening Factor

- **0.5**: Very pessimistic; assumes heavy correlation between filters. Use when you have multiple correlated columns.
- **0.75**: Conservative default; good general-purpose choice (SQL Server default region).
- **0.9**: Optimistic; assumes filters are nearly independent. Use when columns are known to be uncorrelated.
- **1.0**: No dampening; assumes perfect independence (often too optimistic).

## Estimation Primitives

### 1. Single Predicate Estimation

```python
estimator = SelectivityEstimator()

predicate = Predicate(
    column_name="age",
    predicate_type=PredicateType.RANGE,
    lower_bound=30.0,
    upper_bound=None  # Open-ended: age >= 30
)

selectivity = estimator.estimate_single_predicate(predicate, input_stats)
# Returns: 0.25 (25% of rows have age >= 30)

output_rows = cardinality_estimator.estimate_after_filter(
    input_stats,
    selectivity=selectivity
)
# Returns: 2500 (if input_stats.row_count = 10000)
```

**Supported predicate types:**
- `RANGE`: `col >= lower AND col <= upper` (use histogram)
- `EQUALITY`: `col = value` (selectivity = 1 / distinct_count)
- `IN_LIST`: `col IN (v1, v2, v3)` (selectivity = list_size / distinct_count)
- `LIKE`: `col LIKE pattern` (conservative estimate = 0.1)

### 2. Multiple Predicate Estimation

```python
predicates = [
    Predicate("age", PredicateType.RANGE, lower_bound=30.0),
    Predicate("status", PredicateType.EQUALITY, lower_bound="active"),
    Predicate("region", PredicateType.IN_LIST, values=["US", "CA", "MX"]),
]

combined_selectivity = estimator.estimate_multiple_predicates(
    predicates,
    input_stats
)
# Returns: 0.005 (0.5% of rows pass all three filters)
# With dampening: first filter at full selectivity,
#                 subsequent filters dampened by 0.75 each
```

**Important**: Predicates should be on **different columns**. If you have multiple predicates on the same column (e.g., `age > 30 AND age < 50`), merge them into a single range predicate first:

```python
# ❌ Don't do this:
predicates = [
    Predicate("age", PredicateType.RANGE, lower_bound=30.0, upper_bound=None),
    Predicate("age", PredicateType.RANGE, lower_bound=None, upper_bound=50.0),
]

# ✅ Do this:
predicate = Predicate(
    "age",
    PredicateType.RANGE,
    lower_bound=30.0,
    upper_bound=50.0
)
```

### 3. Cardinality Estimation

#### After GROUP BY
```python
card_estimator = CardinalityEstimator()

output_rows = card_estimator.estimate_group_by_cardinality(
    input_stats=filtered_stats,
    group_columns=["customer_id", "product_category"]
)
# Returns: min(distinct(customer_id) * distinct(product_category), input_rows)
# Example: min(5000 * 25, 100000) = 100000 (bounded by input)
```

#### After JOIN
```python
output_rows = card_estimator.estimate_join_cardinality(
    left_stats=left_table_stats,
    right_stats=right_table_stats,
    left_key="customer_id",
    right_key="c_id",
    join_type="inner"
)
# Returns: (left_rows * right_rows) / max(left_distinct, right_distinct)
# Example: (1M * 100K) / max(50K, 100K) = 1M * 100K / 100K = 1M rows
```

### 4. Range Narrowing

```python
range_estimator = RangeEstimator()

original_range = ColumnRange(lower_bound=0, upper_bound=120)  # Age 0-120

predicate = Predicate(
    "age",
    PredicateType.RANGE,
    lower_bound=30.0,
    upper_bound=65.0
)

new_range = range_estimator.narrow_range_for_predicate(
    original_range,
    predicate
)
# Returns: ColumnRange(30.0, 65.0)
```

## Integration with Statistics Recalculation

These primitives are used during the bottom-up statistics recalculation pass:

```python
class FilterNode(LogicalPlanNode):
    def estimate_statistics(self, input_stats: RelationStatistics) -> RelationStatistics:
        # Step 1: Estimate selectivity
        selectivity = estimator.estimate_single_predicate(self.predicate, input_stats)

        # Step 2: Estimate output row count
        output_stats = input_stats.copy()
        output_stats.row_count = cardinality_estimator.estimate_after_filter(
            input_stats,
            selectivity=selectivity
        )

        # Step 3: Narrow affected column ranges
        col_stats = output_stats.get_column(self.predicate.column_name)
        if col_stats:
            new_range = range_estimator.narrow_range_for_predicate(
                col_stats.value_range,
                self.predicate
            )
            output_stats = output_stats.update_column_range(
                self.predicate.column_name,
                new_range
            )

        return output_stats
```

## Column Statistics and Histograms

Column statistics include:

```python
@dataclass
class ColumnStatistics:
    column_name: str
    data_type: str
    distinct_count: Optional[int]      # Number of distinct values
    value_range: ColumnRange           # Min/max values
    histogram: Optional[object]        # Future: histogram/sketch backing
```

**Without histogram** (current state):
- Use uniform distribution assumption
- Selectivity = intersection_width / total_width

**With histogram** (future):
- Query histogram for exact selectivity
- Fall back to uniform distribution for values outside histogram range

## Accuracy Considerations

### When Estimates Will Be Good

- ✓ Predicates on uncorrelated columns (dampening factor close to 1.0)
- ✓ Large datasets (law of large numbers helps)
- ✓ Histograms available for first predicate
- ✓ Predicates that are simple ranges/equality

### When Estimates Will Be Poor

- ✗ Correlated predicates (e.g., `state = 'CA' AND zip_code IN (9xxxx)`)
  - Dampening helps but isn't perfect
- ✗ String patterns with LIKE (very conservative estimate)
- ✗ Skewed distributions (uniform assumption breaks down)
- ✗ Small datasets (variance is high)

### Tuning Dampening Factor

If you have execution statistics, compare actual to estimated cardinality:

```python
actual_rows = 500
estimated_rows = 2500
actual_selectivity = actual_rows / input_rows

# Try adjusting dampening_factor
# If estimated > actual: increase dampening_factor (more pessimistic)
# If estimated < actual: decrease dampening_factor (more optimistic)
```

## Future Enhancements

1. **Histogram integration**: Replace uniform distribution with actual distribution shapes
2. **Correlation tracking**: Learn which column pairs are correlated; adjust dampening dynamically
3. **Execution feedback**: Update estimates based on actual execution cardinality
4. **Skew detection**: Identify and handle non-uniform distributions
5. **Join statistics**: Store join column correlation information (e.g., from foreign keys)

## References

- **SQL Server Cardinality Estimation**: Uses exponential selectivity dampening in CE version 120+
- **Postgres ANALYZE**: Accumulates histograms and stores them in `pg_stats`
- **Calcite optimizer**: Implements uniform distribution assumption with pluggable histograms
