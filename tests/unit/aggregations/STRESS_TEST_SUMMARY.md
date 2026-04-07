# Group Key Codec Stress Tests - Test Coverage Summary

## Overview

`test_group_key_codec_stress.py` provides comprehensive stress testing and performance validation for the Opteryx group key codec. This test suite contains **64 test functions** across **10 test classes**, organized by stress test category.

The tests are designed to be thorough but may take significant time to run (many marked as `@pytest.mark.slow`). Tests can be filtered to run only quick tests or specific categories.

## File Statistics

- **Total Test Functions**: 64
- **Test Classes**: 10
- **Quick Tests (not marked slow)**: 10
- **Slow/Stress Tests**: 54
- **Lines of Code**: ~1,372
- **Approximate Total Execution Time (all tests)**: ~30-45 minutes
- **Quick Test Suite Execution Time**: ~2 seconds

## Test Categories

### 1. Massive Datasets (6 tests)
**Location**: `TestMassiveDatasets`

Tests codec performance with very large volumes of data:
- **1M uniform distribution** int keys
- **1M Zipfian distribution** (power-law) keys  
- **1M clustered distribution** keys
- **10M distinct keys** (full cardinality)
- **1M string keys** with uniform distribution
- **1M mixed int/string keys**

**Purpose**: Verify codec handles production-scale data volumes without degradation.

### 2. String Pathological Cases (8 tests)
**Location**: `TestStringPathologicalCases`

Tests extreme string scenarios:
- Single key with **1M different string values**
- **10K keys repeated 100 times** each
- Strings from **1 byte to 1MB** (parametrized across 6 sizes)
- **Unicode BMP plane** characters (Latin, Japanese, Arabic, Cyrillic)
- **Unicode SMP plane** including emoji and mathematical symbols
- **Combining characters** and Unicode normalization edge cases
- **Right-to-left (RTL) and bidirectional text**
- **Zero-width and invisible characters** (format marks, control chars)

**Purpose**: Ensure string encoding handles all Unicode planes and pathological cases correctly.

### 3. Numeric Pathological Cases (6 tests)
**Location**: `TestNumericPathologicalCases`

Tests extreme numeric scenarios:
- **All powers of 2** from 2^0 to 2^62
- **Sequential numbers** 1 to 1M
- **Clustered numbers** around int64 boundaries (MIN/MAX)
- **Sparse and gapped distributions** with 1 trillion step size
- **Negative numbers** distributed uniformly
- **Float-like behavior** with various distributions (parametrized: uniform, clustered)

**Purpose**: Validate numeric encoding across full range and special distributions.

### 4. Type Cardinality Mixing (4 tests)
**Location**: `TestTypeCardinalityMixing`

Tests mixed-cardinality key combinations:
- **High cardinality strings + low cardinality ints** (10K × 2)
- **Low cardinality strings + high cardinality ints** (2 × 10K)
- **Three key columns** with different cardinalities (100 × 50 × 10)
- **Date32 + int64 + string** mix

**Purpose**: Verify codec handles unbalanced cardinality combinations efficiently.

### 5. Null Distribution Patterns (5 tests)
**Location**: `TestNullDistributionPatterns`

Tests various null value patterns:
- **Different null rates** (1%, 5%, 10%, 50%, 95%) in single key
- **All nulls clustered** together
- **Nulls scattered vs clustered** comparison
- **Different null rates per column** in multi-key scenarios
- **Nulls with high cardinality string keys**

**Purpose**: Ensure null handling is robust across different null density patterns.

### 6. Memory Stress and Offsets (4 tests)
**Location**: `TestMemoryStressAndOffsets`

Tests memory allocation and offset encoding:
- **100 encode/decode cycles** with 10K keys
- **1K encode/decode cycles** with 1K keys
- **Offset overflow boundaries** with large payloads
- **Cumulative vs fresh encoding** stability comparison

**Purpose**: Validate memory management and offset encoding correctness.

### 7. Aggregation Stress (7 tests)
**Location**: `TestAggregationStress`

Tests aggregation functions at scale:
- **COUNT aggregation** with 1M keys
- **SUM aggregation** with 1M numeric values
- **AVG aggregation** with 1M values
- **MIN aggregation** with 1M values
- **MAX aggregation** with 1M values
- **Mixed aggregations** (COUNT + SUM + AVG + MIN + MAX)
- **Aggregations with various distributions** (parametrized: uniform, exponential, clustered)

**Purpose**: Verify all aggregation functions work correctly at scale with various data distributions.

### 8. Round-Trip Stability (3 tests)
**Location**: `TestRoundTripStability`

Tests encode/decode cycle stability:
- **Bit-for-bit stability** verification
- **String keys stability** across 10 iterations
- **Multi-key stability** across 10 iterations

**Purpose**: Ensure encode/decode cycles produce identical results (deterministic).

### 9. Randomized Property Tests (4 tests)
**Location**: `TestRandomizedPropertyTests`

Fuzz and property-based tests:
- **Fuzz random int keys** (100 trials with random parameters)
- **Fuzz random string keys** (100 trials)
- **Property: all values accounted for** (50 trials)
- **Property: no data corruption** (50 trials with count verification)

**Purpose**: Detect edge cases through randomized testing with seed-based reproducibility.

### 10. Advanced Edge Combinations (5 tests)
**Location**: `TestAdvancedEdgeCombinations`

Tests complex combinations of edge cases:
- **Mega strings + nulls + high cardinality ints** (50K rows, 1KB strings, 10% nulls)
- **All Unicode planes with different frequencies**
- **Powers of 2 with all aggregations and nulls**
- **Extreme cardinality combinations** (1 value, all unique, 10 values)
- **Sequential then reverse sequential keys**

**Purpose**: Test realistic worst-case scenarios combining multiple stress factors.

## Running the Tests

### Run All Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v
```

### Run Only Quick Tests (excludes slow tests)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "not slow" -v
```

### Run Only Slow/Stress Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "slow" -v
```

### Run Specific Test Class
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestMassiveDatasets -v
```

### Run Specific Test Function
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestStringPathologicalCases::test_unicode_bmp_plane -v
```

### Run with Timing Information
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v --durations=10
```

### Run in Parallel (using pytest-xdist)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -n auto
```

## Test Design Principles

### 1. Parametrization
Multiple tests use `@pytest.mark.parametrize` to test variations:
- String sizes: `[1, 100, 1_000, 10_000, 100_000, 1_000_000]`
- Distribution types: `["uniform", "exponential", "clustered"]`
- Null rates: `[0.01, 0.05, 0.1, 0.5, 0.95]`

### 2. Deterministic Randomization
Random tests use `random.seed()` for reproducible results:
```python
def test_fuzz_random_int_keys(self):
    random.seed(42)  # Same seed = same test data
    for trial in range(100):
        # ... test with random data
```

### 3. Data Distribution Generators
Helper functions create realistic distributions:
- `_generate_zipfian_distribution()`: Power-law/Zipfian distribution
- `_generate_clustered_distribution()`: Clustered data around centers

### 4. Proper Validation
Every test verifies:
- **Correctness**: Aggregation results match expectations
- **Completeness**: All input rows accounted for (`sum(counts) == input_size`)
- **Consistency**: Multiple executions produce identical results

## Performance Expectations

Based on test execution times:

| Test Category | Count | Time per Test | Total |
|---------------|-------|---------------|-------|
| Quick Tests | 10 | ~0.1-1s | ~2s |
| Unicode Tests | 5 | ~0.1-0.5s | ~2s |
| Numeric Tests | 6 | ~0.1-2s | ~5s |
| Type Mixing | 4 | ~1-5s | ~12s |
| Null Patterns | 5 | ~2-10s | ~25s |
| Memory Stress | 4 | ~5-60s | ~80s |
| Aggregations | 7 | ~10-120s | ~400s |
| Round-trip | 3 | ~20-100s | ~200s |
| Randomized | 4 | ~30-150s | ~300s |
| Edge Combos | 5 | ~20-120s | ~300s |

**Total Time**: Approximately 30-45 minutes for full suite.

## CI/CD Integration

### Quick Smoke Test
Run only non-slow tests in CI:
```yaml
pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "not slow"
```

### Full Regression Suite
Run all tests periodically (nightly, before releases):
```yaml
pytest tests/unit/aggregations/test_group_key_codec_stress.py
```

### Performance Benchmarking
Track timing changes with:
```yaml
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v --durations=20
```

## Common Test Failures and Debugging

### ValueError: Duplicate values in key columns
**Cause**: Test data had unexpected duplicates  
**Fix**: Verify `_rows_by_key()` grouping logic

### AssertionError: count mismatch
**Cause**: Some rows lost during aggregation  
**Fix**: Check null handling and encoding logic

### OverflowError: Python int too large
**Cause**: Integer values exceed int64 bounds  
**Fix**: Ensure all values are within `[-2^63, 2^63-1]`

### Timeout or excessive memory
**Cause**: Slow codec or memory leak  
**Fix**: Profile with smaller datasets first

## Extending the Test Suite

To add new stress tests:

1. **Choose appropriate test class** or create new one for distinct category
2. **Mark as `@pytest.mark.slow`** if execution time > 5 seconds
3. **Use helper functions**: `_finalize_rows()`, `_rows_by_key()`, `_normalize_value()`
4. **Include docstring** explaining the scenario
5. **Verify assertions** check both correctness and completeness
6. **Add parametrization** for test variations
7. **Use deterministic seeds** for random tests

### Example Test Template
```python
@pytest.mark.slow
def test_my_stress_scenario(self):
    """Test description of what is being stressed."""
    size = 100_000
    cardinality = 1_000
    
    # Generate test data
    keys = [i % cardinality for i in range(size)]
    values = [float(random.random()) for _ in range(size)]
    
    # Execute
    table = pa.table({"k": pa.array(keys, type=pa.int64()), 
                      "v": pa.array(values, type=pa.float64())})
    rows = _finalize_rows(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    
    # Validate
    assert len(rows) <= cardinality
    assert sum(row["cnt"] for row in rows) == size
```

## Related Test Files

- `test_group_key_codec_extensive.py`: Comprehensive functional tests and edge cases
- `test_group_key_codec_rewrite.py`: Integration tests with rewrite optimization
- `test_bloom_groupby_correctness.py`: Bloom filter group-by correctness
- `test_bloom_groupby_telemetry.py`: Bloom filter performance telemetry

## Pytest Configuration

The `@pytest.mark.slow` marker is configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
]
```

This allows:
- `pytest -m "not slow"` to skip slow tests
- `pytest -m "slow"` to run only slow tests
- `pytest` to run all tests