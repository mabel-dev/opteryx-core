# Group Key Codec Stress Tests - Quick Reference

## File Location
`tests/unit/aggregations/test_group_key_codec_stress.py`

## Quick Stats
- **64 total tests** (52 base + parametrized variants)
- **10 test classes** organized by stress category
- **10 quick tests** (~2 sec)
- **54 slow tests** (~30-45 min total)

## Most Common Commands

### Run Quick Tests Only (Recommended for CI)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "not slow"
```

### Run All Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v
```

### Run Specific Test Class
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestMassiveDatasets -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestStringPathologicalCases -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestNumericPathologicalCases -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestTypeCardinalityMixing -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestNullDistributionPatterns -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestMemoryStressAndOffsets -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestAggregationStress -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestRoundTripStability -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestRandomizedPropertyTests -v
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestAdvancedEdgeCombinations -v
```

### Run Specific Test
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestMassiveDatasets::test_1m_uniform_distribution_int_keys -v
```

### Show Test Timings
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v --durations=10
```

### Run in Parallel (faster)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -n auto -v
```

## Test Categories Overview

| Class | Tests | Time | Focus |
|-------|-------|------|-------|
| TestMassiveDatasets | 6 | ~30s | 1M-10M keys, various distributions |
| TestStringPathologicalCases | 8 | ~2s | Unicode, extreme sizes, special chars |
| TestNumericPathologicalCases | 6 | ~5s | Powers of 2, boundaries, sequences |
| TestTypeCardinalityMixing | 4 | ~12s | Mixed cardinality combinations |
| TestNullDistributionPatterns | 5 | ~25s | Null rates, clustering patterns |
| TestMemoryStressAndOffsets | 4 | ~80s | Encode/decode cycles, offsets |
| TestAggregationStress | 7 | ~400s | COUNT/SUM/AVG/MIN/MAX at scale |
| TestRoundTripStability | 3 | ~200s | Determinism and consistency |
| TestRandomizedPropertyTests | 4 | ~300s | Fuzz testing, property validation |
| TestAdvancedEdgeCombinations | 5 | ~300s | Complex edge case combinations |

## What Each Test Validates

### Massive Datasets
✓ 1M uniform keys  
✓ 1M Zipfian (power-law) keys  
✓ 1M clustered keys  
✓ 10M unique keys  
✓ 1M string keys  
✓ 1M mixed key types  

### String Pathological Cases
✓ 1M different string values  
✓ 10K × 100 repetitions  
✓ Strings: 1 byte to 1MB  
✓ Unicode BMP, SMP, emoji  
✓ RTL/bidirectional text  
✓ Zero-width characters  
✓ Combining characters  

### Numeric Pathological Cases
✓ Powers of 2 (2^0 to 2^62)  
✓ Sequential 1..1M  
✓ Boundary clustering  
✓ Sparse distributions  
✓ Negative numbers  

### Type Cardinality Mixing
✓ High card string + low card int  
✓ Low card string + high card int  
✓ Three columns mixed cards  
✓ Date32 + int64 + string  

### Null Distribution Patterns
✓ Null rates: 1%, 5%, 10%, 50%, 95%  
✓ All nulls clustered  
✓ Nulls scattered  
✓ Per-column null rates  
✓ Nulls with high cardinality  

### Memory Stress & Offsets
✓ 100 encode/decode cycles  
✓ 1K encode/decode cycles  
✓ Large payload offsets  
✓ Fresh vs cumulative encoding  

### Aggregation Stress
✓ COUNT at 1M scale  
✓ SUM at 1M scale  
✓ AVG at 1M scale  
✓ MIN at 1M scale  
✓ MAX at 1M scale  
✓ Mixed aggregations  
✓ Various distributions (uniform, exponential, clustered)  

### Round-Trip Stability
✓ Bit-for-bit determinism  
✓ String key stability  
✓ Multi-key consistency  

### Randomized Property Tests
✓ Fuzz int keys (100 trials)  
✓ Fuzz string keys (100 trials)  
✓ All values accounted for (50 trials)  
✓ No data corruption (50 trials)  

### Advanced Edge Combinations
✓ Mega strings + nulls + high card  
✓ All Unicode planes mixed frequencies  
✓ Powers of 2 + all aggregations + nulls  
✓ Extreme cardinality ranges  
✓ Sequential + reverse sequential  

## Filtering Patterns

### Only String Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestStringPathologicalCases -m "not slow"
```

### Only Numeric Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestNumericPathologicalCases -v
```

### Only Aggregation Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestAggregationStress -v
```

### Only Unicode Tests (Quick)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -k "unicode" -v
```

### Only Null Distribution Tests
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TestNullDistributionPatterns -v
```

## Performance Tips

- Use `-m "not slow"` for local development (2 sec)
- Use parallel execution with `-n auto` for speed
- Run full suite only before commits/releases
- Check `--durations=10` to identify bottlenecks
- Set timeout for CI: `--timeout=300` (5 min per test)

## Debugging Failed Tests

```bash
# See detailed failure info
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TESTNAME -vv --tb=long

# Drop into debugger
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TESTNAME --pdb

# Show local variables on failure
pytest tests/unit/aggregations/test_group_key_codec_stress.py::TESTNAME -vv --showlocals
```

## Expected Results

✓ All quick tests should pass in ~2 seconds  
✓ All tests should pass when given sufficient time  
✓ No memory leaks or crashes  
✓ Consistent results across runs (deterministic)  

## Related Files

- `test_group_key_codec_extensive.py` - Functional tests
- `test_group_key_codec_rewrite.py` - Integration tests
- `STRESS_TEST_SUMMARY.md` - Detailed documentation