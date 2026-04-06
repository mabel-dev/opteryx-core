# GROUP BY Column Combination Test Generator

## Overview

This automated test generator creates and executes **GROUP BY queries** for **all combinations of columns** in your database tables. It systematically tests 4 different query patterns to flush out bugs that only occur with specific column combinations.

## Purpose

GROUP BY bugs are often triggered by specific combinations of:
- Column data types (integer, string, numeric)
- Column properties (aggregatable vs. groupable)
- Query patterns (single vs. multi-aggregate, single vs. multi-column GROUP BY)

This generator tests ALL valid combinations to identify exactly which ones fail, making debugging much easier.

## The 4 Test Patterns

The generator tests these 4 fundamental GROUP BY patterns:

### Pattern P1: Single Column with COUNT
```sql
SELECT COUNT(*), {column_a} FROM {table} GROUP BY {column_a}
```
**Tests**: Basic single-column GROUP BY with COUNT(*)

### Pattern P2: Single Column with Multiple Aggregates
```sql
SELECT MAX({column_b}), COUNT(*), {column_a} FROM {table} GROUP BY {column_a}
```
**Tests**: Multiple aggregates on a single GROUP BY column (this is where the segfault bug was found!)

### Pattern P3: Two Columns with COUNT
```sql
SELECT COUNT(*), {column_a}, {column_b} FROM {table} GROUP BY {column_a}, {column_b}
```
**Tests**: Multi-column GROUP BY with COUNT(*)

### Pattern P4: Two Columns with Multiple Aggregates
```sql
SELECT MAX({column_c}), COUNT(*), {column_a}, {column_b} FROM {table} GROUP BY {column_a}, {column_b}
```
**Tests**: Multi-column GROUP BY with multiple aggregates

## Quick Start

### Minimal Usage
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json
```

### With Verbose Output
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --verbose
```

### Save Results
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --output results.json
```

### Test Specific Suite
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --suite satellites_comprehensive
```

### Limit Tests
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --limit 10
```

## Configuration File

The `groupby_combo_tests_config.json` file defines:
1. **Test suites**: Which tables to test
2. **Columns**: Available columns and their properties
3. **Test patterns**: The 4 query patterns (or custom patterns)

### Column Properties

Each column must be marked as:
- `aggregatable: true/false` - Can be used in aggregates (MAX, SUM, AVG, COUNT, etc.)
- `groupable: true/false` - Can be used in GROUP BY clause

Example configuration:
```json
{
  "test_suites": [
    {
      "name": "satellites_comprehensive",
      "table": "testdata.satellites",
      "columns": [
        {
          "name": "planetId",
          "type": "integer",
          "aggregatable": true,
          "groupable": true
        },
        {
          "name": "name",
          "type": "string",
          "aggregatable": false,
          "groupable": true
        },
        {
          "name": "radius",
          "type": "numeric",
          "aggregatable": true,
          "groupable": true
        },
        {
          "name": "yearDiscovered",
          "type": "integer",
          "aggregatable": true,
          "groupable": true
        }
      ],
      "test_patterns": [...]
    }
  ]
}
```

## Output Interpretation

### Execution Display

**Verbose mode** shows each test:
```
[1/24] Pattern P1: SELECT COUNT(*), planetId FROM testdata.satellites GROUP BY planetId...
  ✓ Passed (8 rows, 125.3ms)

[2/24] Pattern P1: SELECT COUNT(*), name FROM testdata.satellites GROUP BY name...
  ✓ Passed (168 rows, 87.2ms)

[3/24] Pattern P2: SELECT MAX(radius), COUNT(*), planetId FROM testdata.satellites GROUP BY planetId...
  ✗ CRASHED: Process crash (likely segfault)
```

**Compact mode** shows progress:
```
Generated 24 test combinations
================================================================================
..F.C.....F.......C....
```

Symbols:
- `.` = Test passed ✓
- `F` = Test failed ✗
- `C` = Process crashed 🔴

### Summary Report

```
================================================================================
TEST EXECUTION SUMMARY
================================================================================

Total Tests:      24
Passed:           20 ✓
Failed:           2 ✗
Crashed:          2 🔴
Pass Rate:        83.3%

================================================================================
FAILED/CRASHED TESTS (4)
================================================================================

Pattern: P1
Status: FAILED
Table: testdata.missions
Columns: Company, Price
SQL: SELECT COUNT(*), Company, Price FROM testdata.missions GROUP BY Company, Price
Error Type: ValueError
Error: Unsupported column type

...

================================================================================
PROBLEMATIC COLUMN COMBINATIONS (4)
================================================================================

Pattern P2 | Table: testdata.satellites | Columns: planetId, radius | Status: crashed
Pattern P4 | Table: testdata.satellites | Columns: planetId, name, radius | Status: crashed
Pattern P2 | Table: testdata.missions | Columns: Company, Price | Status: failed
Pattern P3 | Table: testdata.missions | Columns: Company, Status | Status: failed
```

## Analyzing Results with JSON Export

Export results for detailed analysis:
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --output results.json
```

### Find All Crashes
```bash
jq '.results[] | select(.status == "crashed")' results.json
```

### Find All Failures
```bash
jq '.results[] | select(.status == "failed")' results.json
```

### Get Problematic Columns
```bash
jq '.results[] | select(.status != "passed") | {pattern: .pattern_id, table: .table, columns: .columns_used}' results.json
```

### Find Failures by Pattern
```bash
jq '.results[] | select(.status != "passed") | .pattern_id' results.json | sort | uniq -c
```

### Find Slow Queries (>100ms)
```bash
jq '.results[] | select(.execution_time_ms > 100)' results.json
```

### Get Summary Statistics
```bash
jq '.summary' results.json
```

## Understanding Findings

When the generator finds issues, you'll see specific patterns:

### Example 1: Crash on Multiple Aggregates
```
Pattern P2 crashes: planetId + radius
Pattern P4 crashes: planetId + name + radius
```
**Interpretation**: The bug occurs when combining COUNT(*) with other aggregates on numeric columns.

### Example 2: Failures on String Columns
```
Pattern P1 fails: Company (string)
Pattern P3 fails: Company + Status (both string)
```
**Interpretation**: GROUP BY on string columns fails.

### Example 3: No Issues
```
All 24 tests passed ✓
Pass Rate: 100%
```
**Interpretation**: This table works correctly with all query patterns.

## Use Cases

### 1. Systematic Bug Finding
Run the full suite to find ALL problematic column combinations:
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --verbose --output results.json
```

### 2. Regression Testing
After fixing a bug, run the suite to ensure it's fixed:
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json
```

### 3. Quick Smoke Test
Test just a few combinations to verify basics work:
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --limit 10
```

### 4. Focused Testing
Test a specific table thoroughly:
```bash
python tests/groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --suite satellites_comprehensive --verbose
```

### 5. CI/CD Integration
Add to your test pipeline:
```bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --output /tmp/combo_results.json && \
jq '.summary' /tmp/combo_results.json
```

## Customization

### Adding a New Table

1. Edit `groupby_combo_tests_config.json`
2. Add a new test suite:

```json
{
  "test_suites": [
    {
      "name": "your_table_name",
      "table": "database.your_table",
      "columns": [
        {
          "name": "column1",
          "type": "integer",
          "aggregatable": true,
          "groupable": true
        },
        {
          "name": "column2",
          "type": "string",
          "aggregatable": false,
          "groupable": true
        },
        {
          "name": "column3",
          "type": "numeric",
          "aggregatable": true,
          "groupable": false
        }
      ],
      "test_patterns": [...]  // Uses existing patterns from config
    }
  ]
}
```

### Custom Patterns

To add custom patterns, modify the `test_patterns` array:

```json
{
  "pattern_id": "P5",
  "description": "Three aggregates with GROUP BY",
  "template": "SELECT COUNT(*), SUM({column_b}), AVG({column_c}), {column_a} FROM {table} GROUP BY {column_a}",
  "required_columns": ["column_a", "column_b", "column_c"],
  "column_a_must_be": "groupable",
  "column_b_must_be": "aggregatable",
  "column_c_must_be": "aggregatable"
}
```

## Command-Line Options

```
--config CONFIG           Path to configuration JSON file (required)
--suite SUITE             Run specific test suite (default: all suites)
--verbose                 Show detailed output for each test
--limit N                 Run first N tests only
--output FILE             Export results to JSON file
```

## Examples

### Run all tests with details
```bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --verbose
```

### Run first 20 tests and save results
```bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --limit 20 \
    --output results.json
```

### Test only missions table
```bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --suite missions_comprehensive \
    --verbose
```

### Combine options
```bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --suite satellites_comprehensive \
    --verbose \
    --limit 50 \
    --output /tmp/sat_results.json
```

## Troubleshooting

### No tests are generated
**Problem**: The script runs but shows "0 test combinations"
**Solution**: 
1. Check table name is correct in config
2. Verify column names match actual schema
3. Ensure at least one column is marked `groupable: true`
4. Ensure at least one pattern has required columns available

### Tests hang or timeout
**Problem**: Script appears to freeze on a particular test
**Solution**:
1. Press Ctrl+C to interrupt
2. Use `--verbose` to see which query is hanging
3. Run with `--limit` to test fewer combinations
4. Test that specific query manually to debug

### JSON export fails
**Problem**: "Cannot serialize TestStatus"
**Solution**: This is a known issue. The script handles it automatically now.

### Column combinations not exhaustive
**Problem**: Not all combinations are being tested
**Solution**: Check if:
1. `columns_must_be_different` is set (excludes same column twice)
2. Column constraints (`column_a_must_be`, etc.) are filtering out combinations
3. Pattern requirements are too restrictive

### Results show mostly failures
**Problem**: Many tests are failing
**Solution**:
1. Run with `--verbose` to see specific error messages
2. Check if table/column names are correct
3. Verify column type flags are accurate
4. Test that basic queries work manually first

## Performance

### Expected Performance

| Scenario | Tests | Time | Notes |
|----------|-------|------|-------|
| 2 columns, 4 patterns | 8 | <1s | Very fast |
| 4 columns, 4 patterns | 24 | 2-5s | Quick |
| 8 columns, 4 patterns | 96 | 10-30s | Medium |
| --limit 100 | 100 | 30-60s | Substantial |
| Full table (many columns) | 200+ | 1-5min | Comprehensive |

### Optimization Tips

1. **Start small**: Use `--limit 10` to find bugs quickly
2. **Run focused**: Use `--suite` to test one table at a time
3. **Skip verbose**: Remove `--verbose` for faster execution (shows progress dots instead)
4. **Parallel runs**: Run different suites in parallel (different terminals)
5. **Subset config**: Create a minimal config with 2-3 test tables for quick testing

## Integration with CI/CD

### GitHub Actions Example
```yaml
- name: Run GROUP BY combo tests
  run: |
    python tests/groupby_combo_generator.py \
      --config tests/groupby_combo_tests_config.json \
      --output combo_results.json
    
- name: Check combo test results
  run: |
    jq '.summary' combo_results.json
    test $(jq '.summary.crashed' combo_results.json) -eq 0
```

### Local Pre-commit Hook
```bash
#!/bin/bash
python tests/groupby_combo_generator.py \
    --config tests/groupby_combo_tests_config.json \
    --limit 20 || exit 1
```

## Files

- `groupby_combo_generator.py` - Main test generator script
- `groupby_combo_tests_config.json` - Configuration file
- `COMBO_TESTS_QUICK_START.md` - Quick reference guide
- `README_COMBO_TESTS.md` - This file

## Related Tests

See also:
- `tests/integration/test_groupby_comprehensive.py` - Full integration tests
- `tests/unit/operators/test_groupby_comprehensive_unit.py` - Unit tests
- `tests/integration/test_groupby_advanced.py` - Advanced scenarios

## Contributing

To add tests or patterns:
1. Edit `groupby_combo_tests_config.json` to add tables/patterns
2. Run generator to test your config
3. Submit results and findings

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Run with `--verbose` to see detailed output
3. Export results with `--output results.json` for analysis
4. Report findings with pattern ID, columns, SQL, and error type