# Comprehensive GROUP BY Test Suite

## Overview

This document describes the extensive GROUP BY test suite created to flush out issues in the GROUP BY capability. The suite consists of **three main test files** with **500+ test cases** covering unit tests, integration tests, stress tests, regression tests, and advanced scenarios.

## Test Files

### 1. Integration Tests: `tests/integration/test_groupby_comprehensive.py`
**Size:** 24KB | **Test Classes:** 20+ | **Test Cases:** 150+

Core integration tests covering fundamental GROUP BY functionality with real data from `testdata.satellites` and `testdata.missions`.

**Coverage:**
- **Basic GROUP BY**: Single/multi-column grouping, COUNT, SUM, AVG, MIN, MAX
- **Aggregation Functions**: All aggregates, COUNT(DISTINCT), multiple aggregates together
- **WHERE + GROUP BY**: Filtering before aggregation
- **HAVING**: Post-aggregation filtering
- **ORDER BY**: Sorting by group column, aggregate, multiple columns, DESC/ASC
- **NULL Semantics**: NULL in keys, NULL in aggregates, COUNT(*) vs COUNT(column)
- **Data Types**: Integer, string, year/date columns
- **Cardinality**: Low cardinality (few groups), high cardinality (many groups)
- **LIMIT/OFFSET**: Pagination with GROUP BY
- **Expressions**: Case expressions, computed columns
- **Edge Cases**: Empty results, single row, all same key, global aggregates

**Test Classes:**
- `TestBasicGroupBy` - Foundational single and multi-column tests
- `TestGroupByWithWhereClause` - Filtering combinations
- `TestGroupByWithHavingClause` - Post-aggregation filtering
- `TestAggregationFunctions` - Each aggregation function individually
- `TestNullHandling` - NULL semantics
- `TestGroupByCardinality` - Low/high cardinality scenarios
- `TestGroupByOrdering` - Complex ordering
- `TestGlobalAggregation` - Aggregates without GROUP BY
- `TestExpressionGroupBy` - Complex expressions
- `TestComplexGroupBy` - Multiple features combined
- `TestGroupByEdgeCases` - Boundary conditions
- `TestGroupByDataTypes` - Various data types

### 2. Unit Tests: `tests/unit/operators/test_groupby_comprehensive_unit.py`
**Size:** 28KB | **Test Classes:** 20+ | **Test Cases:** 200+

Low-level unit tests using `ShuffleGroupByOperation` directly with `Morsel` objects. These tests validate the core aggregation engine.

**Coverage:**
- **Morsel Ingestion**: Single/multiple morsels, overlapping groups
- **Multi-Column Grouping**: 2-3 column GROUP BY with various combinations
- **Aggregation Variations**: All functions, multiple aggregates on different columns
- **NULL Handling**: NULL in keys, NULL in aggregates, all NULL values
- **Global Aggregates**: GROUP BY with no columns
- **Large Datasets**: Stress testing with 10k+ rows
- **High/Low Cardinality**: Stress testing both extremes
- **String Grouping**: String keys, unicode, case sensitivity
- **Mixed Types**: Integer + string columns
- **Edge Cases**: Empty input, single row, cardinality extremes
- **Aggregate Invariants**: AVG = SUM/COUNT, MIN ≤ MAX, COUNT(DISTINCT) ≤ COUNT
- **Consistency**: Order independence, merging consistency

**Test Classes:**
- `TestShuffleGroupByBasics` - Basic morsel ingestion
- `TestShuffleGroupByAggregations` - All aggregation functions
- `TestMultiColumnGroupBy` - Multi-column tests
- `TestNullHandling` - NULL semantics
- `TestGlobalAggregation` - Global aggregates
- `TestStressTesting` - Large datasets, many morsels
- `TestStringGroupBy` - String handling
- `TestMixedTypeGroupBy` - Mixed data types
- `TestEdgeCases` - Single row, empty, extremes
- `TestAggregateInvariants` - Mathematical properties
- `TestConsistency` - Result determinism

### 3. Advanced Integration Tests: `tests/integration/test_groupby_advanced.py`
**Size:** 23KB | **Test Classes:** 20+ | **Test Cases:** 150+

Advanced scenarios including CTEs, subqueries, complex expressions, performance characteristics, and regression tests.

**Coverage:**
- **CTEs**: GROUP BY in CTEs, nested CTEs with GROUP BY
- **Subqueries**: GROUP BY in derived tables, subqueries with GROUP BY
- **Complex Expressions**: Arithmetic, CASE, string functions, COALESCE, CAST
- **Complex Aggregates**: Aggregates on expressions, multiple COUNT(DISTINCT)
- **Complex Ordering**: Multiple aggregate ordering, expressions in ORDER BY
- **LIMIT/OFFSET**: Large result sets, pagination
- **DISTINCT**: Interaction with GROUP BY
- **Performance Characteristics**: Large result sets, many aggregates, many group columns
- **Regressions**: Case sensitivity, aliases, mixed case keywords, position numbers
- **Filtering**: Complex WHERE + HAVING combinations
- **Consistency**: Deterministic results, row accounting
- **NULL Handling**: NULL grouping, COUNT variants
- **Real Data**: Missions table tests

**Test Classes:**
- `TestGroupByWithCTE` - Common Table Expressions
- `TestGroupByWithSubqueries` - Subquery patterns
- `TestComplexGroupByExpressions` - Complex expressions
- `TestGroupByAggregateEdgeCases` - Aggregate edge cases
- `TestGroupByWithOrderByComplexity` - Complex ordering
- `TestGroupByWithLimit` - Pagination
- `TestGroupByDistinctInteraction` - DISTINCT interactions
- `TestGroupByPerformanceCharacteristics` - Performance tests
- `TestGroupByRegressions` - Regression and edge cases
- `TestGroupByWithFiltering` - Filter combinations
- `TestGroupByConsistency` - Result consistency
- `TestGroupByNullHandling` - Advanced NULL handling
- `TestGroupByWithMissions` - Real missions data

## Test Organization

### By Scope

**Unit Tests (Direct Morsel Operations)**
- `tests/unit/operators/test_groupby_comprehensive_unit.py`
- Tests the `ShuffleGroupByOperation` directly
- No SQL parsing, pure aggregation logic
- 200+ test cases

**Integration Tests (SQL Execution)**
- `tests/integration/test_groupby_comprehensive.py` - Basic integration
- `tests/integration/test_groupby_advanced.py` - Advanced integration
- Full SQL execution with planner and optimizer
- 300+ test cases

### By Feature Area

| Feature | Unit | Basic | Advanced | Notes |
|---------|------|-------|----------|-------|
| Single Column GROUP BY | ✓ | ✓ | ✓ | All aggregates covered |
| Multi-Column GROUP BY | ✓ | ✓ | ✓ | Up to 3 columns |
| COUNT(*) | ✓ | ✓ | ✓ | Various scenarios |
| COUNT(column) | ✓ | ✓ | ✓ | NULL handling |
| COUNT(DISTINCT) | ✓ | ✓ | ✓ | Multiple columns |
| SUM | ✓ | ✓ | ✓ | Precision tested |
| AVG | ✓ | ✓ | ✓ | Mathematical verification |
| MIN/MAX | ✓ | ✓ | ✓ | Ordering verified |
| WHERE + GROUP BY | ✓ | ✓ | ✓ | Filter before agg |
| GROUP BY + HAVING | ✓ | ✓ | ✓ | Filter after agg |
| ORDER BY | ✓ | ✓ | ✓ | All variants |
| LIMIT/OFFSET | ✗ | ✓ | ✓ | Pagination |
| NULL Handling | ✓ | ✓ | ✓ | Comprehensive |
| CTEs | ✗ | ✗ | ✓ | Complex queries |
| Subqueries | ✗ | ✗ | ✓ | Derived tables |
| Expressions | ✗ | ✓ | ✓ | CASE, arithmetic |
| Global Aggregates | ✓ | ✓ | ✗ | No GROUP BY |
| Performance | ✗ | ✗ | ✓ | Stress scenarios |
| Regression | ✗ | ✓ | ✓ | Specific bugs |

### By Issue Category

**Correctness**
- Aggregate accuracy (SUM, AVG, MIN, MAX)
- COUNT(*) vs COUNT(column) semantics
- NULL value handling
- COUNT(DISTINCT) correctness
- GROUP key uniqueness (no duplicates)
- All rows accounted for

**Edge Cases**
- Empty input
- Single row
- All same group (cardinality 1)
- Each row unique (max cardinality)
- High cardinality (100k+ groups)
- NULL in keys, NULL in values
- Zero and negative values
- Float precision

**Combinations**
- WHERE + GROUP BY + HAVING
- Multiple GROUP BY columns
- Multiple aggregates
- Complex expressions
- LIMIT, OFFSET, ORDER BY
- CTEs, subqueries

**Regressions**
- Case sensitivity
- Keyword casing
- Column aliases
- Position references
- Mixed operators

## Running the Tests

### Run All GROUP BY Tests

```bash
# Run all GROUP BY tests
pytest tests/integration/test_groupby_comprehensive.py -v
pytest tests/integration/test_groupby_advanced.py -v
pytest tests/unit/operators/test_groupby_comprehensive_unit.py -v

# Or use make shortcuts
make test  # Full regression suite
make t     # Quick regression suite
```

### Run Specific Test Classes

```bash
# Basic aggregation functions
pytest tests/integration/test_groupby_comprehensive.py::TestAggregationFunctions -v

# Unit tests for aggregation invariants
pytest tests/unit/operators/test_groupby_comprehensive_unit.py::TestAggregateInvariants -v

# Advanced CTEs
pytest tests/integration/test_groupby_advanced.py::TestGroupByWithCTE -v

# Stress tests
pytest tests/unit/operators/test_groupby_comprehensive_unit.py::TestStressTesting -v
```

### Run Specific Tests

```bash
# Single test
pytest tests/integration/test_groupby_comprehensive.py::TestAggregationFunctions::test_count_star -v

# Tests matching pattern
pytest tests/integration/test_groupby_comprehensive.py -k "null" -v
pytest tests/unit/operators/test_groupby_comprehensive_unit.py -k "stress" -v
```

### Performance Tests

```bash
# Stress tests with timing
pytest tests/unit/operators/test_groupby_comprehensive_unit.py::TestStressTesting -v --durations=0

# Large result sets
pytest tests/integration/test_groupby_advanced.py::TestGroupByPerformanceCharacteristics -v
```

## Test Data

Tests use the following datasets:

- **testdata.satellites** - ~200+ satellite records with:
  - planetId (1-8, low cardinality)
  - name (string, high cardinality)
  - radius (numeric, varied)
  - yearDiscovered (year, varied)

- **testdata.missions** - Mission records with:
  - Company (string, variable cardinality)
  - Status (categorical)
  - Price (numeric, nullable)

## Key Test Scenarios

### 1. Aggregation Correctness
```sql
SELECT planetId, COUNT(*), SUM(radius), AVG(radius), MIN(radius), MAX(radius)
FROM testdata.satellites
GROUP BY planetId
HAVING COUNT(*) > 1
```

### 2. Multi-Column GROUP BY
```sql
SELECT planetId, name, COUNT(*) as cnt
FROM testdata.satellites
GROUP BY planetId, name
HAVING COUNT(*) > 0
ORDER BY cnt DESC
```

### 3. NULL Handling
```sql
SELECT Company, COUNT(*) as cnt_all, COUNT(Price) as cnt_price
FROM testdata.missions
GROUP BY Company
```

### 4. Complex Expressions
```sql
SELECT
    CASE WHEN radius > 2000 THEN 'large' ELSE 'small' END,
    COUNT(*) as cnt
FROM testdata.satellites
GROUP BY CASE WHEN radius > 2000 THEN 'large' ELSE 'small' END
```

### 5. Stress Test (Unit Level)
```python
# 10,000 rows, 100 groups
morsel = _morsel_from_dict({
    "k": [(i % 100) for i in range(10000)],
    "v": list(range(10000))
})
```

## Expected Coverage

The test suite provides coverage of:

- ✓ **Unit Tests**: Direct aggregation engine logic
- ✓ **Integration Tests**: Full SQL pipeline
- ✓ **Correctness**: Mathematical properties and invariants
- ✓ **Edge Cases**: Boundary conditions and extremes
- ✓ **Regression**: Specific known issues
- ✓ **Performance**: Stress scenarios and large datasets
- ✓ **Consistency**: Deterministic and reproducible results

## Adding New Tests

When adding new tests, follow this structure:

```python
class TestGroupByNewFeature:
    """Test description."""
    
    def test_specific_scenario(self, session):
        """Specific scenario description."""
        result = session.execute_to_arrow(
            "SELECT ... GROUP BY ..."
        ).to_pylist()
        
        assert len(result) > 0
        # Add specific assertions
```

For unit tests:

```python
def test_specific_unit_scenario(self):
    """Specific scenario description."""
    morsel = _morsel_from_dict({"k": [1, 1, 2], "v": [10, 20, 30]})
    op = ShuffleGroupByOperation(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )
    op.ingest(morsel)
    result = _result_to_dict(op.finalize(), ["k"])
    
    assert result[1]["sum_v"] == 30
```

## Known Test Characteristics

### Unit Tests
- **Fast**: ~1-10ms per test
- **Deterministic**: Always produce same results
- **Isolated**: No external dependencies
- **Comprehensive**: All code paths tested

### Integration Tests
- **Medium Speed**: ~10-100ms per test
- **Data-Dependent**: Results depend on testdata
- **End-to-End**: Test full pipeline
- **Real-World**: Use realistic queries

### Advanced Tests
- **Varied Speed**: 10-500ms depending on complexity
- **Complex Scenarios**: CTEs, subqueries, multiple stages
- **Regression-Focused**: Specific edge cases

## Success Criteria

Tests should:
1. ✓ Execute without errors
2. ✓ Produce deterministic results
3. ✓ Verify mathematical invariants
4. ✓ Check for NULL handling correctness
5. ✓ Ensure no data loss (all rows accounted for)
6. ✓ Validate aggregate accuracy
7. ✓ Confirm group key uniqueness
8. ✓ Cover edge cases and stress scenarios

## References

- `opteryx/operators/shuffle.py` - ShuffleGroupByOperation implementation
- `opteryx/operators/shuffle_node.py` - ShuffleNode for distributed GROUP BY
- `tests/integration/test_shuffle_groupby_golden.py` - Existing golden tests
- `tests/unit/aggregations/` - Bloom filter GROUP BY tests
