# Function Cost Estimation Tools

This directory contains a suite of tools for estimating, analyzing, and managing function execution costs in the opteryx function catalog.

## Overview

The cost estimation system measures how long function kernels take to execute and stores these measurements as **cost_us_per_million**: microseconds required to process one million rows.

This cost data drives the cost-based optimizer, enabling intelligent query plan selection based on the actual performance characteristics of functions.

## Tools

### 1. `estimate_function_costs.py`

**Purpose**: Benchmark function kernels and generate cost estimates.

**What it does**:
- Generates synthetic test data for various data types
- Benchmarks each function kernel with multiple input sizes (1K, 10K, 100K, 1M rows)
- Measures execution time and extrapolates to cost per million rows
- Exports results as JSON

**Usage**:

```bash
# Benchmark all functions (takes a while)
python estimate_function_costs.py --output costs.json

# Benchmark specific functions
python estimate_function_costs.py --functions UPPER,LOWER,SUBSTRING --output costs.json

# Use different sample sizes
python estimate_function_costs.py --sample-sizes 1000,50000,500000 --output costs.json

# Run more iterations for higher accuracy
python estimate_function_costs.py --runs 10 --output costs.json
```

**Options**:
- `--functions`: Comma-separated list of function names to benchmark
- `--output`: Path to output JSON file (default: `function_costs.json`)
- `--runs`: Number of runs per sample size for averaging (default: 5)
- `--sample-sizes`: Comma-separated list of sample sizes to test

**Output Format**:
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

### 2. `import_function_costs.py`

**Purpose**: Import cost estimates into the function catalog.

**What it does**:
- Reads cost estimates from a JSON file
- Locates function definitions in source code
- Updates `cost_us_per_million` values in KernelSpec definitions
- Optionally applies changes to source files

**Usage**:

```bash
# Preview changes without applying
python import_function_costs.py costs.json

# Apply changes to source files
python import_function_costs.py costs.json --apply

# Generate a patch file instead
python import_function_costs.py costs.json --patch --output costs.patch
```

**Options**:
- `costs_file`: JSON file from `estimate_function_costs.py` (required)
- `--apply`: Write changes to source files (default: dry-run preview)
- `--patch`: Generate a patch file instead of modifying files
- `--output`: Output path for patch file

**Workflow**:
1. Run `estimate_function_costs.py` to generate `costs.json`
2. Run `import_function_costs.py costs.json` to preview changes
3. Review the proposed updates
4. Run with `--apply` to update the source code
5. Commit the changes to version control

### 3. `analyze_function_costs.py`

**Purpose**: Analyze cost data and generate insights.

**What it does**:
- Computes statistical distribution of costs
- Identifies fastest and slowest functions
- Groups costs by execution engine
- Detects outliers (candidates for optimization)
- Generates comprehensive analysis reports

**Usage**:

```bash
# Generate formatted analysis report
python analyze_function_costs.py costs.json

# Save report to file
python analyze_function_costs.py costs.json --output analysis_report.txt

# Export as JSON for programmatic use
python analyze_function_costs.py costs.json --json --output analysis.json
```

**Options**:
- `costs_file`: JSON file from `estimate_function_costs.py` (required)
- `--output`: Save report to file (default: print to stdout)
- `--json`: Output as JSON instead of formatted text

**Report Sections**:
- **Cost Distribution**: Min, max, mean, median, percentiles
- **Costs by Engine**: Breakdown by execution engine (arrow, numpy, python, draken)
- **Top 10 Fastest**: Functions with lowest execution costs
- **Top 10 Slowest**: Functions with highest execution costs
- **Optimization Candidates**: Statistical outliers (unusual costs, likely high complexity)

## Complete Workflow

### Initial Setup

1. **Benchmark all functions**:
   ```bash
   python estimate_function_costs.py --output initial_costs.json
   ```

2. **Review the results**:
   ```bash
   python analyze_function_costs.py initial_costs.json
   ```

3. **Import costs into catalog**:
   ```bash
   python import_function_costs.py initial_costs.json --apply
   ```

4. **Commit the changes**:
   ```bash
   git add opteryx/expression/functions/implementations/
   git commit -m "Add cost estimates for function kernels"
   ```

### Regular Updates

After making changes to function implementations:

1. **Re-benchmark affected functions**:
   ```bash
   python estimate_function_costs.py --functions FUNC1,FUNC2 --output updated_costs.json
   ```

2. **Review changes**:
   ```bash
   python import_function_costs.py updated_costs.json
   ```

3. **Apply if satisfied**:
   ```bash
   python import_function_costs.py updated_costs.json --apply
   ```

## Cost Estimation Details

### Methodology

1. **Test Data Generation**: For each type (INTEGER, VARCHAR, DATE, etc.), we generate random synthetic data
2. **Benchmarking**: Run each kernel with increasing batch sizes (1K → 10M rows)
3. **Time Measurement**: Use `time.perf_counter_ns()` for high-precision timing
4. **Extrapolation**: Calculate cost per million rows from the largest sample size
5. **Averaging**: Report mean, min, max across multiple runs

### Accuracy Considerations

- **Cold Start**: First run may be slower (JIT compilation, caching). We run multiple iterations and average.
- **System Load**: Run when system is relatively idle for more consistent results
- **Array vs Vector**: Cython kernels may behave differently than pure Python
- **Null Handling**: Some functions have different code paths for null values

### Engine-Specific Notes

- **Arrow**: Generally fastest for vectorized operations on PyArrow arrays
- **Draken** (Cython): Optimized compiled code, good for complex operations
- **NumPy**: Efficient for numeric operations
- **Python**: Fallback for general-purpose operations, usually slowest

## Interpreting Results

### Cost Per Million Rows

Cost is expressed in **microseconds per million rows**. For example:
- `cost_us_per_million = 100.0` means the function takes ~100 microseconds to process 1 million rows
- This translates to ~0.0001 milliseconds per row

### Total Query Cost

For an operation processing N rows through a function:
```
total_time_us = (cost_us_per_million / 1_000_000) * N
```

Example: If UPPER() costs 125 μs/M and processes 10 million rows:
```
total_time_us = (125 / 1_000_000) * 10_000_000 = 1,250 μs = 1.25 ms
```

## Common Issues

### Benchmarking Takes Too Long

- Reduce `--sample-sizes` to skip large batches: `--sample-sizes 1000,100000`
- Reduce `--runs`: `--runs 3` (less averaging but faster)
- Benchmark subset of functions: `--functions UPPER,LOWER`

### Some Functions Fail to Benchmark

- Not all functions can be benchmarked automatically (some need specific input types or context)
- This is normal; check the error messages in the output
- You can manually set costs for these in the source code

### Costs Seem Unrealistic

- Cold start effects: Run `--runs 10` for better averaging
- System load: Close other applications and re-run
- Check if the kernel actually executed (some fail silently)

## Integration with Query Planner

The cost estimates feed into the cost-based optimizer:

1. **Query Parsing**: Query is parsed into an AST
2. **Plan Generation**: Multiple plan candidates are generated
3. **Cost Calculation**: Each plan's cost is calculated using function costs
4. **Plan Selection**: The lowest-cost plan is chosen
5. **Execution**: The plan is executed

See `opteryx/planner/optimizer/bench/cost_model.py` for the cost calculation logic.

## Future Improvements

Potential enhancements to the cost estimation system:

- [ ] Parameterized costs (vary by input types)
- [ ] Data distribution effects (cache hits, branch prediction)
- [ ] Per-architecture measurements (different CPUs)
- [ ] Automatic regression detection
- [ ] Integration with CI/CD for cost tracking
- [ ] Cost tracking across versions

## References

- Function catalog: `opteryx/expression/functions/catalog.py`
- Benchmark model: `opteryx/planner/optimizer/bench/cost_model.py`
- Function implementations: `opteryx/expression/functions/implementations/`
- Statistics system: See memory files for statistics_capabilities.md
