# Function Cost Estimation Tools - Summary

## What's Been Created

A complete suite of tools for estimating, analyzing, comparing, and importing function execution costs into the opteryx function catalog.

## Files Created

```
tools/
├── estimate_function_costs.py          # Main benchmarking script
├── import_function_costs.py            # Import costs into catalog
├── analyze_function_costs.py           # Analyze cost data and generate insights
├── compare_function_costs.py           # Compare measurements or against catalog
├── COST_ESTIMATION_QUICKSTART.md       # Quick start guide (5-minute intro)
├── COST_ESTIMATION_README.md           # Comprehensive documentation
└── COST_ESTIMATION_SUMMARY.md          # This file
```

## Tools Overview

### 1. estimate_function_costs.py
**Purpose**: Benchmark function kernels and generate cost estimates

**Key Features**:
- Synthetic data generation for all data types
- Configurable sample sizes and iteration counts
- Benchmarks with high-precision timing
- Exports results as JSON
- Progress reporting

**Usage**:
```bash
python estimate_function_costs.py --functions UPPER,LOWER --output costs.json
python estimate_function_costs.py --output all_costs.json  # benchmark all
```

**Output**: JSON file with cost measurements for each kernel

---

### 2. import_function_costs.py
**Purpose**: Update function definitions with cost estimates

**Key Features**:
- Dry-run preview (no file modifications)
- Automated source code updates
- Patch file generation
- Safety-first approach

**Usage**:
```bash
python import_function_costs.py costs.json              # Preview
python import_function_costs.py costs.json --apply      # Update files
python import_function_costs.py costs.json --patch      # Generate patch
```

**Workflow**:
1. Generate costs with `estimate_function_costs.py`
2. Preview changes with `import_function_costs.py`
3. Review proposed updates
4. Apply with `--apply` flag
5. Commit to git

---

### 3. analyze_function_costs.py
**Purpose**: Analyze cost data and generate insights

**Key Features**:
- Statistical distribution analysis
- Per-engine cost breakdowns
- Fastest/slowest function rankings
- Outlier detection for optimization candidates
- Formatted text or JSON output

**Usage**:
```bash
python analyze_function_costs.py costs.json                    # Print report
python analyze_function_costs.py costs.json --output report.txt  # Save to file
python analyze_function_costs.py costs.json --json              # JSON output
```

**Reports Include**:
- Cost distribution (min, max, mean, median, percentiles)
- Per-engine analysis
- Top 10 fastest/slowest functions
- Statistical outliers (optimization candidates)

---

### 4. compare_function_costs.py
**Purpose**: Compare cost measurements to detect regressions/improvements

**Key Features**:
- Compare two measurements
- Compare against current catalog
- Detailed change reporting
- Configurable threshold for highlighting changes

**Usage**:
```bash
# Compare against previous measurement
python compare_function_costs.py --baseline old.json --current new.json

# Compare against catalog
python compare_function_costs.py --catalog --current new.json

# Custom threshold
python compare_function_costs.py --baseline old.json --current new.json --threshold 20
```

**Reports Show**:
- Improvements (lower cost)
- Regressions (higher cost)
- New kernels
- Removed kernels
- Summary statistics

---

## Typical Workflow

### Initial Setup
```bash
# 1. Benchmark all functions
python estimate_function_costs.py --output initial_costs.json

# 2. Review results
python analyze_function_costs.py initial_costs.json

# 3. Import into catalog
python import_function_costs.py initial_costs.json --apply

# 4. Commit
git add -A && git commit -m "Add function cost estimates"
```

### After Making Changes
```bash
# 1. Benchmark changed functions
python estimate_function_costs.py --functions FUNC1,FUNC2 --output updated.json

# 2. Check for improvements/regressions
python compare_function_costs.py --catalog --current updated.json

# 3. Update if happy
python import_function_costs.py updated.json --apply
```

### Tracking Performance Over Time
```bash
# Save baseline
cp function_costs.json baseline_costs.json

# Later, benchmark again
python estimate_function_costs.py --output current_costs.json

# Compare against baseline
python compare_function_costs.py --baseline baseline_costs.json --current current_costs.json
```

## Data Format

### Input/Output Format
JSON structure for cost data:

```json
{
  "timestamp": 1234567890.123,
  "total_functions": 42,
  "total_kernels": 87,
  "successful": 82,
  "functions": {
    "UPPER": [
      {
        "kernel_id": "varchar",
        "engine": "arrow",
        "cost_us_per_million": 125.43,
        "sample_size": 1000000,
        "runs": 5,
        "min_us": 120.1,
        "max_us": 130.2,
        "avg_us": 125.43,
        "success": true
      }
    ]
  }
}
```

### Cost Units
**cost_us_per_million**: Microseconds required to process one million rows

| Cost | Time per Row | Time per 1M Rows |
|------|--------------|-----------------|
| 10 | 0.01 μs | 10 ms |
| 100 | 0.1 μs | 100 ms |
| 1000 | 1 μs | 1 second |

## Key Features

### ✅ What These Tools Do
- **Automated benchmarking** of all function kernels
- **Accurate cost measurement** using high-precision timing
- **Flexible configuration** for sample sizes and iterations
- **Safe updates** with preview-before-apply workflow
- **Comprehensive analysis** with statistics and outlier detection
- **Change tracking** to detect regressions and improvements
- **Multiple output formats** (text, JSON, patches)

### ✅ What They Support
- All OrsoTypes: INTEGER, DOUBLE, BOOLEAN, VARCHAR, DATE, TIMESTAMP, etc.
- All execution engines: Arrow, Draken, NumPy, Python
- Batch and streaming benchmarks
- Custom data generation
- Statistical analysis and reporting

### ✅ Safety Features
- Dry-run mode (preview changes before applying)
- No automatic modifications to source files
- Detailed change reports before committing
- Error handling and fallback mechanisms
- Version control integration-friendly

## Technical Details

### Benchmarking Methodology

1. **Data Generation**: Create synthetic test data for each type
2. **Warm-up**: Optional runs to stabilize performance
3. **Measurement**: High-precision timing with `perf_counter_ns()`
4. **Extrapolation**: Calculate cost per million rows
5. **Aggregation**: Average across multiple runs

### Supported Data Types
- Numeric: INTEGER, DOUBLE, DECIMAL
- String: VARCHAR, BLOB
- Temporal: DATE, TIMESTAMP, TIME
- Complex: ARRAY, STRUCT, VECTOR
- Special: NULL

### Execution Engines
- **Arrow**: Fast vectorized operations
- **Draken**: Cython-compiled optimized code
- **NumPy**: Efficient numeric operations
- **Python**: General-purpose fallback

## Usage Examples

### Quick Benchmark
```bash
# 30 seconds - benchmark 2 functions
python estimate_function_costs.py --functions UPPER,LOWER --output quick.json
```

### Detailed Benchmark
```bash
# 30 minutes - benchmark all functions with high accuracy
python estimate_function_costs.py --runs 10 --output detailed.json
```

### Custom Configuration
```bash
# Benchmark with specific parameters
python estimate_function_costs.py \
  --functions SUBSTRING,CONCAT,REPLACE \
  --sample-sizes 10000,100000,1000000 \
  --runs 20 \
  --output custom.json
```

### Full Pipeline
```bash
# Complete workflow
python estimate_function_costs.py --output costs.json
python analyze_function_costs.py costs.json
python compare_function_costs.py --catalog --current costs.json
python import_function_costs.py costs.json --apply
git add -A && git commit -m "Update function costs"
```

## Integration Points

### Catalog Integration
Costs are stored in `KernelSpec.cost_us_per_million`:

```python
from opteryx.expression.functions.catalog import get_catalog

catalog = get_catalog()
cost = catalog.get_cost("UPPER")  # Get cost in μs/million
```

### Optimizer Integration
The cost-based optimizer uses these values:

```python
# In cost model calculations
function_cost = catalog.get_cost(func_name)
rows_processed = batch_size
total_cost = (function_cost / 1_000_000) * rows_processed
```

## Troubleshooting

### Issue: Benchmarking is too slow
**Solution**: Use `--sample-sizes` to skip large batches or `--functions` to benchmark only specific functions

### Issue: Some functions fail to benchmark
**Solution**: This is expected; functions with specific type requirements can't be auto-benchmarked. The script skips them gracefully.

### Issue: Costs seem inaccurate
**Solution**: Run with `--runs 10` for better averaging. Close other applications and re-run for more stable results.

### Issue: Import failed or made unwanted changes
**Solution**: Undo with `git checkout -- opteryx/expression/functions/` or `git reset --hard`

## Documentation

- **COST_ESTIMATION_QUICKSTART.md** - 5-minute introduction
- **COST_ESTIMATION_README.md** - Comprehensive documentation
- **COST_ESTIMATION_SUMMARY.md** - This file (tool overview)

## Next Steps

1. **Start small**: Run `estimate_function_costs.py --functions UPPER,LOWER`
2. **Review results**: Run `analyze_function_costs.py` on the output
3. **Preview changes**: Run `import_function_costs.py` without `--apply`
4. **Read the docs**: Check COST_ESTIMATION_QUICKSTART.md for more examples

## Command Reference

```bash
# Benchmarking
python estimate_function_costs.py --functions NAME1,NAME2 --output costs.json
python estimate_function_costs.py --sample-sizes 1000,100000 --output costs.json
python estimate_function_costs.py --runs 10 --output costs.json

# Importing
python import_function_costs.py costs.json              # Preview
python import_function_costs.py costs.json --apply      # Update files
python import_function_costs.py costs.json --patch      # Generate patch

# Analysis
python analyze_function_costs.py costs.json             # Print report
python analyze_function_costs.py costs.json --json      # JSON output
python analyze_function_costs.py costs.json --output report.txt

# Comparison
python compare_function_costs.py --baseline old.json --current new.json
python compare_function_costs.py --catalog --current new.json
python compare_function_costs.py --baseline old.json --current new.json --threshold 20
```

---

**Created**: 2026-03-23
**Version**: 1.0
**Status**: Ready for use

For detailed usage instructions, see COST_ESTIMATION_QUICKSTART.md or COST_ESTIMATION_README.md.
