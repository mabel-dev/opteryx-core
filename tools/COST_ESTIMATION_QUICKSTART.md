# Cost Estimation Quick Start Guide

## TL;DR - Get Running in 5 Minutes

```bash
# 1. Run benchmarks
python estimate_function_costs.py --functions UPPER,LOWER --output costs.json

# 2. Preview changes
python import_function_costs.py costs.json

# 3. Apply to catalog (if happy with preview)
python import_function_costs.py costs.json --apply

# 4. Analyze results
python analyze_function_costs.py costs.json
```

## The Three Essential Scripts

### 1. **estimate_function_costs.py** - Measure Function Performance

Benchmarks functions and generates cost estimates.

```bash
# Benchmark just a couple functions (fast)
python estimate_function_costs.py --functions UPPER,LOWER --output my_costs.json

# Benchmark all functions (slow, ~30 min+)
python estimate_function_costs.py --output all_costs.json

# Custom configuration
python estimate_function_costs.py \
  --functions UPPER,CONCAT,SUBSTRING \
  --sample-sizes 10000,100000,1000000 \
  --runs 10 \
  --output detailed_costs.json
```

**Output**: JSON file with cost estimates for each function kernel

### 2. **import_function_costs.py** - Update the Catalog

Updates the function definitions with the cost estimates.

```bash
# Preview changes (no modifications)
python import_function_costs.py my_costs.json

# Apply changes (modifies source files!)
python import_function_costs.py my_costs.json --apply

# Generate a patch file instead
python import_function_costs.py my_costs.json --patch --output changes.patch
```

**Key Steps**:
1. Always preview first: `python import_function_costs.py costs.json`
2. Review the proposed changes
3. If satisfied: `python import_function_costs.py costs.json --apply`
4. Commit to git: `git add -A && git commit -m "Update function costs"`

### 3. **analyze_function_costs.py** - Analyze the Results

Generates reports and identifies optimization opportunities.

```bash
# Print detailed analysis report
python analyze_function_costs.py my_costs.json

# Save to file
python analyze_function_costs.py my_costs.json --output report.txt

# Export as JSON for processing
python analyze_function_costs.py my_costs.json --json --output analysis.json
```

**Reports Show**:
- Fastest/slowest functions
- Cost distribution statistics
- Per-engine analysis
- Optimization candidates (statistical outliers)

## Bonus Tools

### compare_function_costs.py - Track Changes Over Time

Compare two cost measurements to detect regressions or improvements.

```bash
# Compare against previous measurement
python compare_function_costs.py \
  --baseline baseline_costs.json \
  --current new_costs.json

# Compare against current catalog
python compare_function_costs.py \
  --catalog \
  --current new_costs.json

# Show only significant changes (>20%)
python compare_function_costs.py \
  --baseline old.json \
  --current new.json \
  --threshold 20
```

## Typical Workflows

### Scenario 1: Initial Cost Catalog Setup

```bash
# 1. Benchmark all functions
python estimate_function_costs.py --output initial_costs.json
# ☕ Go get coffee, this takes a while...

# 2. Review the results
python analyze_function_costs.py initial_costs.json

# 3. Import into catalog
python import_function_costs.py initial_costs.json

# 4. Preview the changes
# (already shown by step 3)

# 5. Apply to source code
python import_function_costs.py initial_costs.json --apply

# 6. Verify and commit
git diff opteryx/expression/functions/
git add -A
git commit -m "Add initial function cost estimates"
```

### Scenario 2: Update After Function Changes

```bash
# 1. Benchmark the changed functions
python estimate_function_costs.py \
  --functions SUBSTRING,CONCAT,REPLACE \
  --output updated_costs.json

# 2. Compare with current catalog
python compare_function_costs.py \
  --catalog \
  --current updated_costs.json

# 3. Check for improvements/regressions
# (review the output)

# 4. Update catalog if happy
python import_function_costs.py updated_costs.json --apply
git commit -m "Update costs for string functions"
```

### Scenario 3: Investigating Performance Issues

```bash
# 1. Benchmark suspicious functions
python estimate_function_costs.py \
  --functions COSINE_SIMILARITY,EMBED \
  --runs 20 \
  --output investigation.json

# 2. Analyze the detailed costs
python analyze_function_costs.py investigation.json

# 3. Look for outliers
# (identify unusually slow functions)

# 4. Review source code for optimization opportunities
# (look at the slowest functions)
```

## Understanding the Output

### Cost Values Explained

**cost_us_per_million**: Microseconds to process 1 million rows

| Cost | Time/Row | Per 1M Rows | Per 1B Rows |
|------|----------|-------------|------------|
| 10.0 | 0.01 μs | 10 ms | 10 s |
| 100.0 | 0.1 μs | 100 ms | 100 s |
| 1000.0 | 1 μs | 1 s | 1000 s |

### Cost Estimates for Different Function Types

- **Simple arithmetic** (ABS, SIGN): 20-100 μs/M
- **String operations** (UPPER, CONCAT): 100-500 μs/M
- **Temporal functions** (YEAR, MONTH): 50-300 μs/M
- **Hash/encoding** (SHA256, MD5): 1000-5000 μs/M
- **Vector operations** (COSINE_SIMILARITY): 5000-50000 μs/M

## Common Issues & Solutions

### "Benchmarking takes forever"

**Problem**: Benchmarking all functions takes too long

**Solutions**:
```bash
# Benchmark a subset
python estimate_function_costs.py --functions UPPER,LOWER --output quick.json

# Use smaller sample sizes
python estimate_function_costs.py --sample-sizes 1000,100000 --output quick.json

# Fewer runs (less accurate but faster)
python estimate_function_costs.py --runs 2 --output quick.json
```

### "Some functions failed to benchmark"

**Problem**: Error messages about functions not benchmarking

**Expected**: Not all functions can be auto-benchmarked (some need specific types or context)
- This is normal
- Failed functions just keep their existing costs (usually 0.0)
- You can manually set costs in the source code if needed

### "My costs look too high/low"

**Possible causes**:
- System under load (close other applications)
- Cold start effects (JIT compilation, caching)
- Function needs specific input types

**Solution**: Re-run with more iterations:
```bash
python estimate_function_costs.py --functions SUSPICIOUS_FUNC --runs 20
```

### "I accidentally applied changes I didn't want"

**Recovery**:
```bash
# Undo the last commit
git reset --soft HEAD~1

# Or restore from backup
git checkout HEAD -- opteryx/expression/functions/
```

## File Reference

| File | Purpose |
|------|---------|
| `estimate_function_costs.py` | Benchmark functions |
| `import_function_costs.py` | Update catalog with costs |
| `analyze_function_costs.py` | Analyze cost data |
| `compare_function_costs.py` | Compare measurements |
| `COST_ESTIMATION_README.md` | Detailed documentation |
| `COST_ESTIMATION_QUICKSTART.md` | This file |

## Next Steps

1. **Run your first benchmark**: Start with `estimate_function_costs.py --functions UPPER,LOWER`
2. **Review the results**: Use `analyze_function_costs.py` to understand the data
3. **Read the full docs**: See `COST_ESTIMATION_README.md` for details
4. **Set up regular updates**: Add cost benchmarking to your CI/CD pipeline

## Questions?

Refer to:
- **How do costs work?** → See "Cost Per Million Rows" in README
- **How accurate are estimates?** → See "Accuracy Considerations" in README
- **Which functions are slow?** → Use `analyze_function_costs.py --slowest`
- **How to integrate with optimizer?** → See "Integration with Query Planner" in README

---

**Happy benchmarking!** 🚀
