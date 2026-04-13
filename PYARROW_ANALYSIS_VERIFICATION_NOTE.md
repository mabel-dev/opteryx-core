# PyArrow Analysis Verification Note

**Status**: Analysis requires corrections  
**Date**: 2024  
**Scope**: Verification of PYARROW_ELIMINATION_ANALYSIS.md findings

---

## Corrections & Clarifications

### Files Misclassified as "DEAD IMPORTS"

**Finding**: During spot-check verification, several files marked as "DEAD" actually have active PyArrow usage:

#### 1. `compiled/table_ops/null_avoidant_ops.pyx`

**Initial Classification**: DEAD (0 usage)  
**Actual Status**: ACTIVE

**Corrected Usage**: 1 (not 0)

```python
# Line 42-52
for chunk in column.chunks if isinstance(column, pyarrow.ChunkedArray) else [column]:
    bitmap_buffer = chunk.buffers()[0]  # validity buffer
```

**Reason**: Uses `pyarrow.ChunkedArray` type check. This is legitimate Arrow array handling.

**Revised Action**: Move to TIER 1 (LOW PRIORITY) - requires type check replacement or Arrow interface preservation.

---

#### 2. `connectors/catalogs/local_catalog.py`

**Initial Classification**: DEAD (0 usage)  
**Actual Status**: ACTIVE

**Corrected Usage**: 4 (not 0)

```python
# Line 26-34
def __init__(self, pa_schema: pa.Schema):
    self._pa = pa_schema
    self.column_names = list(pa_schema.names)
    self.columns = [
        {"name": n, "arrow_type": t} for n, t in zip(pa_schema.names, pa_schema.types)
    ]

# Line 145-149
arrow_schema = pq.read_schema(parquet_file)
return MinimalSchema(arrow_schema)
```

**Reason**: Uses PyArrow schema objects for parquet file inspection. This is fundamental to the catalog connector.

**Revised Action**: Move to TIER 4 (OUT OF SCOPE) - Keep for connector/parquet phase. Strategic dependency.

---

#### 3. `expression/functions/implementations/arithmetic.py`

**Initial Classification**: DEAD (0 usage)  
**Actual Status**: ACTIVE

**Corrected Usage**: 2+ (not 0)

```python
# Line 93-106
def safe_power(base_array, exponent_array):
    """Wrapper around pyarrow's compute.power function."""
    if base_array.dtype.kind == "i" and exponent_array.dtype.kind == "i" and single_exponent >= 0:
        result = compute.power(base_array, exponent_array)
    else:
        result = compute.power(base_array.astype(numpy.float64), exponent_array)
```

**Reason**: Direct usage of `compute.power()` kernel in arithmetic operations.

**Revised Action**: Move to TIER 2 (MEDIUM PRIORITY) - Replace compute.power with Draken equivalent.

---

## Analysis Methodology Issues

The initial analysis used a simple heuristic:
```python
usage_count = len(re.findall(r'(pyarrow\.|pa\.|from pyarrow)', content)) - import_lines
```

**Limitations identified**:
1. **Regex doesn't distinguish usage vs. comments** - Docstrings count as matches
2. **Negative usage counts** - Indicates more matches in comments than actual code
3. **Type annotation imports** - `pa.Schema` in type hints counted as usage, but may be necessary for type safety
4. **Method names matching pattern** - False positives from variable names like `path` or `span`

**Recommended verification approach**:
1. Manual inspection of files marked "DEAD" (negative or zero usage)
2. Check type annotations separately from runtime usage
3. Distinguish between type checks and actual operations
4. Review docstrings for intentionality

---

## Revised Classification Summary

### Actual DEAD IMPORTS (High Confidence)

These files import PyArrow but have NO runtime references to it:

- `expression/functions/registrar/arithmetic.py` - imports `compute` but doesn't call it
- `expression/functions/registrar/arithmetic_extended.py` - same
- `operators/distinct_node.pyx` - import at module level, no usage
- `operators/non_equi_join_node.pyx` - import at module level, no usage
- `models/execution_context.py` - import at module level, no usage
- `planner/optimizer/strategies/statistics_only_response.py` - import at module level, no usage

**Verified**: These can be safely removed.

---

### Files Requiring Re-Analysis

- `null_avoidant_ops.pyx` - ACTIVE (type checks + array interface)
- `local_catalog.py` - ACTIVE (connector/IO layer, keep)
- `arithmetic.py` - ACTIVE (compute kernels)
- `expression/operations/comparisons.py` - Recount: has negative usage, needs manual check
- `expression/operations/string_matching.py` - Recount: has negative usage, needs manual check
- `expression/evaluator/temporal_ops.py` - Recount: has negative usage, needs manual check
- `expression/evaluator/evaluation.py` - Recount: has negative usage, needs manual check
- `operators/filter_join_node.pyx` - Recount: has negative usage, needs manual check

---

## Corrected Priority Levels

### 🟢 TIER 0: CONFIRMED DEAD (Safe to Remove)

| File | Action |
|------|--------|
| `expression/functions/registrar/arithmetic.py` | Remove import |
| `expression/functions/registrar/arithmetic_extended.py` | Remove import |
| `operators/distinct_node.pyx` | Remove import |
| `operators/non_equi_join_node.pyx` | Remove imports |
| `models/execution_context.py` | Remove import |
| `planner/optimizer/strategies/statistics_only_response.py` | Remove import |

**Effort**: 30 minutes  
**Confidence**: HIGH ✅

---

### 🟡 TIER 1: REQUIRES MANUAL VERIFICATION

These need code inspection to confirm actual vs. false-positive usage:

| File | Initial Call | Actual Status | Priority |
|------|--------------|---------------|----------|
| `compiled/table_ops/null_avoidant_ops.pyx` | DEAD → | ACTIVE | Recategorize |
| `connectors/catalogs/local_catalog.py` | DEAD → | ACTIVE (IO) | Move to OUT-OF-SCOPE |
| `expression/functions/implementations/arithmetic.py` | DEAD → | ACTIVE | Move to MEDIUM-TERM |
| `expression/operations/comparisons.py` | -1 usage | ??? | VERIFY |
| `expression/operations/string_matching.py` | -1 usage | ??? | VERIFY |
| `expression/evaluator/temporal_ops.py` | -2 usage | ??? | VERIFY |
| `expression/evaluator/evaluation.py` | -1 usage | ??? | VERIFY |
| `operators/filter_join_node.pyx` | -1 usage | ??? | VERIFY |

---

## Recommended Next Steps

1. **Run corrected analysis** with improved regex patterns:
   - Exclude docstrings/comments
   - Separate type annotations from runtime usage
   - Manual spot-check all "negative usage" files

2. **Update PYARROW_ELIMINATION_ANALYSIS.md** with:
   - Correct tier assignments
   - Verified quick-wins list
   - Identified blocker files

3. **Execute immediate removals** (TIER 0):
   - Safe to do today without regression risk

4. **Audit anti-patterns** in `types/_null_handling.py`:
   - Verify actual impact before refactoring

---

## Key Learnings

- **Simple heuristics break down** with edge cases (comments, type hints, false regex matches)
- **Manual verification is essential** for accurate prioritization
- **Type annotations complicate analysis** (need to distinguish from runtime usage)
- **Negative usage counts are a red flag** indicating regex overmatch

---

## Next Verification Pass

Suggest running a manual audit with:

```python
# Improved heuristic:
# 1. Strip comments and docstrings
# 2. Check for actual function calls vs. type checks
# 3. Verify isinstance() operations have Draken equivalents
# 4. Identify compute.* kernel usage patterns
```

**Estimated time**: 2-3 hours for manual review  
**Expected accuracy**: 95%+ after verification pass