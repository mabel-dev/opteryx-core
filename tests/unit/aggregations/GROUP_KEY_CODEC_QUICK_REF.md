# Group Key Codec Tests - Quick Reference

## 📋 Test Files at a Glance

| File | Tests | Time | Purpose |
|------|-------|------|---------|
| `test_group_key_codec_rewrite.py` | 16 | <0.1s | Baseline validation |
| `test_group_key_codec_extensive.py` | 88 | 0.5s | Edge cases, boundaries |
| `test_group_key_codec_stress.py` | 64 | 1.7s quick / 45min full | Stress, scale, fuzz |
| **Total** | **168** | **2s quick / 46s min / 46min full** | Complete coverage |

## 🚀 Common Commands

### Quick Sanity Check (2 seconds)
```bash
pytest tests/unit/aggregations/test_group_key_codec*.py -m "not slow" -v
```

### Run Extensive Tests Only (0.5 seconds)
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v
```

### Run Original Tests Only (<0.1 seconds)
```bash
pytest tests/unit/aggregations/test_group_key_codec_rewrite.py -v
```

### Run Full Stress Suite (30-45 minutes)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -v
```

### Run Quick Stress Tests Only (1.7 seconds)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "not slow" -v
```

### With Timing Breakdown (shows slowest tests)
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v --durations=10
```

### Run Specific Test Class
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py::TestStringEncoding -v
```

### Run Specific Test Function
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py::TestInt64BoundaryValues::test_int64_max_value -v
```

### Run with Detailed Output (show print statements, etc.)
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v -s
```

### Run in Parallel (faster execution)
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v -n auto
```

### Show All Markers
```bash
pytest --markers | grep slow
```

## 🎯 Test Categories Quick Reference

### Extensive Tests (88 total)
- **Int64 Boundaries** (8) - min/max/zero/powers
- **String Encoding** (16) - unicode, emoji, RTL, sizes
- **Null Handling** (9) - various null patterns
- **Date/Time Types** (11) - date32, time32, time64, timestamp
- **Type Combinations** (6) - multi-key type mixes
- **Large Datasets** (5) - 10K, 100K scaling
- **Duplicate Patterns** (4) - all same, alternating, clustered
- **Aggregation Correctness** (6) - COUNT, SUM, AVG, MIN, MAX
- **Round-Trip Stability** (3) - encode/decode/encode cycles
- **Offset Stability** (3) - monotonic, no overlaps
- **Payload Integrity** (3) - corruption detection
- **Null Propagation** (5) - nulls in aggregations
- **Stress Combinations** (2) - complex edge cases
- **Edge Interactions** (3) - empty string, unicode, nulls
- **Stability** (2) - consistency, order independence
- **Corners** (6) - minimal cases

### Stress Tests (64 total) - Marked with @slow
- **Massive Datasets** (6) - 1M-10M keys
- **String Pathology** (8) - BMP, SMP, combining chars, RTL
- **Numeric Pathology** (6) - powers of 2, sequential, sparse
- **Type Cardinality** (4) - mixed high/low card columns
- **Null Patterns** (5) - 1%-95% null rates
- **Memory Stress** (4) - encode/decode cycles
- **Aggregation Stress** (7) - 1M keys at scale
- **Round-Trip Stability** (3) - determinism verification
- **Randomized Property** (4) - fuzz testing
- **Advanced Combinations** (5) - extreme scenarios

## 🔍 Debugging Tips

### Find which test is failing
```bash
# Run with traceback
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v --tb=short

# Run with full traceback
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v --tb=long
```

### Run single failing test with debug output
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py::TestInt64BoundaryValues::test_int64_max_value -vv -s
```

### Show test parameters (for parametrized tests)
```bash
pytest tests/unit/aggregations/test_group_key_codec_stress.py --collect-only -q
```

### Debug with pdb
```bash
pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v --pdb
```

## 📊 Performance Expectations

| Test Type | Count | Typical Time | Notes |
|-----------|-------|--------------|-------|
| Quick smoke | ~10 | <2s | Good for CI/CD |
| Extensive suite | 88 | ~0.5s | Full edge cases |
| Stress quick | 10 | ~1.7s | No @slow tests |
| All stress | 64 | 30-45min | For thorough validation |
| **Typical CI run** | ~100 | ~2-3s | -m "not slow" |
| **Nightly run** | 168 | ~46min | Full regression |

## 🎨 Using Make Commands

```bash
make test   # Full regression including group key codec tests
make t      # Quick regression (includes smoke tests)
make b      # Run current test query (brace.py)
```

## 💡 Best Practices

1. **Pre-commit**: Run quick smoke tests
   ```bash
   pytest tests/unit/aggregations/test_group_key_codec*.py -m "not slow" -v
   ```

2. **Before push**: Run extensive tests
   ```bash
   pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v
   ```

3. **Before release**: Run full suite
   ```bash
   pytest tests/unit/aggregations/test_group_key_codec_stress.py -v
   ```

4. **Debugging a failure**: Run with verbose + short traceback
   ```bash
   pytest <test_path> -v --tb=short -s
   ```

## 🏷️ Pytest Markers

- `@pytest.mark.slow` - Long-running stress tests (30-45+ seconds each)
- Used by: Skip with `-m "not slow"`, Run only with `-m "slow"`

## 📝 Test Helper Functions

Located in each test file:
- `_finalize_rows(group_by_columns, aggregations, table)` - Execute groupby
- `_rows_by_key(rows, key_columns)` - Group results for assertions
- `_normalize_value(value)` - Normalize bytes/strings

## 🚨 Common Issues & Fixes

| Issue | Command |
|-------|---------|
| Import errors | `cd opteryx-core && pytest ...` |
| Slow on first run | Subsequent runs cached, normal |
| Memory usage high | Run quick tests only, use `-m "not slow"` |
| Want to see timing | Add `--durations=N` flag |
| Tests won't collect | Check pyproject.toml pytest config |

## 📌 File Organization

```
tests/unit/aggregations/
├── test_group_key_codec_rewrite.py          (16 original tests)
├── test_group_key_codec_extensive.py        (88 extensive tests)
├── test_group_key_codec_stress.py           (64 stress tests)
├── GROUP_KEY_CODEC_TEST_SUITE.md            (comprehensive docs)
├── GROUP_KEY_CODEC_QUICK_REF.md             (this file)
└── STRESS_TEST_SUMMARY.md                   (stress test details)
```

## ✅ Verification Checklist

After making changes to group key codec:

- [ ] `pytest tests/unit/aggregations/test_group_key_codec_rewrite.py -v` passes
- [ ] `pytest tests/unit/aggregations/test_group_key_codec_extensive.py -v` passes
- [ ] `pytest tests/unit/aggregations/test_group_key_codec_stress.py -m "not slow" -v` passes
- [ ] `make test` completes without errors
- [ ] No memory leaks observed in stress tests
- [ ] All aggregation functions produce correct results

---

**Last Updated**: 2024
**Test Total**: 168 tests across 3 files
**Coverage**: 15+ categories, all edge cases