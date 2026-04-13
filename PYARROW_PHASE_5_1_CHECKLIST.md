# Phase 5.1 PyArrow Elimination Checklist

**Phase**: 5.1 (Dead Code & Anti-Patterns)  
**Timeline**: 1 week  
**Effort**: 10-12 hours  
**Risk**: Minimal ✅  
**Owner**: [Assign to engineer]

---

## Pre-Execution (30 minutes)

- [ ] **Read** all three analysis documents:
  - [ ] `PYARROW_ELIMINATION_ANALYSIS.md`
  - [ ] `PYARROW_ANALYSIS_VERIFICATION_NOTE.md`
  - [ ] `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md`

- [ ] **Create feature branch**:
  ```bash
  git checkout -b feature/pyarrow-phase-5-1
  ```

- [ ] **Baseline test run**:
  ```bash
  make q  # Quick regression (required before changes)
  ```
  ✓ If passes, proceed. If fails, investigate before starting.

- [ ] **Create tracking issue**:
  - [ ] Title: "Phase 5.1: PyArrow Dead Imports & Anti-Patterns"
  - [ ] Link to this checklist
  - [ ] Update status as you progress

---

## Task 1: Remove Dead Import #1

**File**: `expression/functions/registrar/arithmetic.py`  
**Lines**: 1-11

### Step 1a: Identify the import
```bash
grep -n "from pyarrow import compute" opteryx-core/opteryx/expression/functions/registrar/arithmetic.py
```

Expected output: Line 1 or nearby

### Step 1b: Verify it's unused
```bash
grep -n "compute\." opteryx-core/opteryx/expression/functions/registrar/arithmetic.py
```

Expected output: NO matches (confirming it's dead)

### Step 1c: Remove the import
- [ ] Open file in editor
- [ ] Locate line: `from pyarrow import compute`
- [ ] Delete the entire line
- [ ] Save file

### Step 1d: Verify syntax
```bash
python -m py_compile opteryx-core/opteryx/expression/functions/registrar/arithmetic.py
```

Expected: No output (success)

### Step 1e: Mark complete
- [ ] Commit: `git add opteryx-core/opteryx/expression/functions/registrar/arithmetic.py`
- [ ] Commit message: `refactor: remove dead pyarrow import from arithmetic registrar`

---

## Task 2: Remove Dead Import #2

**File**: `expression/functions/registrar/arithmetic_extended.py`  
**Lines**: 1-11

### Step 2a: Verify it's unused
```bash
grep -n "compute\." opteryx-core/opteryx/expression/functions/registrar/arithmetic_extended.py
```

Expected: NO matches

### Step 2b: Remove the import
- [ ] Delete line: `from pyarrow import compute`
- [ ] Save file

### Step 2c: Verify syntax
```bash
python -m py_compile opteryx-core/opteryx/expression/functions/registrar/arithmetic_extended.py
```

### Step 2d: Mark complete
- [ ] Commit with message: `refactor: remove dead pyarrow import from arithmetic_extended registrar`

---

## Task 3: Remove Dead Import #3

**File**: `operators/distinct_node.pyx`  
**Lines**: ~17

### Step 3a: Locate the import
```bash
grep -n "import pyarrow" opteryx-core/opteryx/operators/distinct_node.pyx
```

### Step 3b: Verify it's unused in code
```bash
grep -n "pyarrow\." opteryx-core/opteryx/operators/distinct_node.pyx | grep -v "import"
```

Expected: NO matches

### Step 3c: Remove the import
- [ ] Delete line: `import pyarrow`
- [ ] Save file

### Step 3d: Mark complete
- [ ] Commit: `refactor: remove dead pyarrow import from distinct_node`

---

## Task 4: Remove Dead Imports #4

**File**: `operators/non_equi_join_node.pyx`  
**Lines**: ~22-26

### Step 4a: Locate imports
```bash
grep -n "import pyarrow\|from pyarrow" opteryx-core/opteryx/operators/non_equi_join_node.pyx
```

Expected: 2 import lines

### Step 4b: Verify they're unused
```bash
grep "pyarrow\." opteryx-core/opteryx/operators/non_equi_join_node.pyx | grep -v import
```

Expected: NO matches

### Step 4c: Remove both imports
- [ ] Delete: `import pyarrow`
- [ ] Delete: `from pyarrow import Table`
- [ ] Save file

### Step 4d: Mark complete
- [ ] Commit: `refactor: remove dead pyarrow imports from non_equi_join_node`

---

## Task 5: Remove Dead Import #5

**File**: `models/execution_context.py`  
**Lines**: ~10

### Step 5a: Locate the import
```bash
grep -n "import pyarrow" opteryx-core/opteryx/models/execution_context.py
```

### Step 5b: Verify it's unused
```bash
grep "pyarrow\." opteryx-core/opteryx/models/execution_context.py | grep -v import
```

Expected: NO matches

### Step 5c: Remove the import
- [ ] Delete line: `import pyarrow`
- [ ] Save file

### Step 5d: Mark complete
- [ ] Commit: `refactor: remove dead pyarrow import from execution_context`

---

## Task 6: Remove Dead Import #6

**File**: `planner/optimizer/strategies/statistics_only_response.py`  
**Lines**: ~24

### Step 6a: Locate the import
```bash
grep -n "import pyarrow" opteryx-core/opteryx/planner/optimizer/strategies/statistics_only_response.py
```

### Step 6b: Verify it's unused
```bash
grep "pyarrow\." opteryx-core/opteryx/planner/optimizer/strategies/statistics_only_response.py | grep -v import
```

Expected: NO matches

### Step 6c: Remove the import
- [ ] Delete line: `import pyarrow`
- [ ] Save file

### Step 6d: Mark complete
- [ ] Commit: `refactor: remove dead pyarrow import from statistics_only_response`

---

## Checkpoint 1: Test Dead Import Removals

After completing Tasks 1-6:

```bash
make q
```

**Success criteria**:
- [ ] All tests pass
- [ ] No new errors
- [ ] No import errors for removed statements

**If tests fail**:
- [ ] Review error message
- [ ] Check if any file actually uses the removed import (false positive)
- [ ] Restore import if necessary
- [ ] Re-run tests

---

## Task 7: Audit Anti-Pattern File

**File**: `types/_null_handling.py`  
**Issue**: 5 imports gated behind try/except (violates Rule 9)

### Step 7a: Review the file
```bash
cat opteryx-core/opteryx/types/_null_handling.py | head -100
```

### Step 7b: Identify all try/except blocks
```bash
grep -n "try:" opteryx-core/opteryx/types/_null_handling.py
```

### Step 7c: For each try/except block, identify:
- [ ] Line number of try block
- [ ] What PyArrow code it contains
- [ ] What the fallback behavior is

**Create audit table** (fill in manually):

| Line | PyArrow Code | Fallback | Function |
|------|--------------|----------|----------|
| 95   | `isinstance(value, pa.Scalar)` | return False | `is_null()` |
| 151  | `isinstance(value, pa.Scalar)` | return False | `is_nan()` |
| 208  | `isinstance(value, pa.Scalar)` | return False | `is_inf()` |
| 281  | `isinstance(vector, pa.Array)` | return False | `is_null_vector()` |
| 320  | `isinstance(vector, pa.Array)` | return 0 | `null_count_vector()` |

### Step 7d: Search for call sites
```bash
grep -r "is_null\|is_nan\|is_inf\|is_null_vector\|null_count_vector" opteryx-core/opteryx \
  --include="*.py" --include="*.pyx" | grep -v "test" | grep -v ".pyc" | wc -l
```

Expected: 50+ call sites

### Step 7e: Sample 5-10 call sites
```bash
grep -r "is_null(" opteryx-core/opteryx --include="*.py" | head -5
```

For each, verify:
- [ ] Does it pass Draken vectors? (preferred)
- [ ] Does it pass NumPy arrays? (supported)
- [ ] Does it pass PyArrow scalars? (would break with removal)

### Step 7f: Decision point

**Question**: Do any call sites pass PyArrow scalars at runtime?

- [ ] **If NO** → Safe to remove PyArrow branches (Task 8)
- [ ] **If MAYBE** → Need to trace callers further (escalate to architect)
- [ ] **If YES** → Cannot remove; requires refactoring (escalate)

### Step 7g: Document finding
Create a comment in the issue:

```markdown
## Anti-Pattern Audit Results

### is_null()
- Callers found: [list files]
- Pass PyArrow scalars? [YES/NO/UNKNOWN]

### is_nan()
- Callers found: [list files]
- Pass PyArrow scalars? [YES/NO/UNKNOWN]

### is_inf()
- Callers found: [list files]
- Pass PyArrow scalars? [YES/NO/UNKNOWN]

### is_null_vector()
- Callers found: [list files]
- Pass PyArrow Arrays? [YES/NO/UNKNOWN]

### null_count_vector()
- Callers found: [list files]
- Pass PyArrow Arrays? [YES/NO/UNKNOWN]

### Recommendation
[Safe to remove / Requires more investigation / Escalate to architect]
```

---

## Task 8: Fix Anti-Pattern (Conditional on Audit Results)

**Only proceed if audit in Task 7 shows NO PyArrow scalars/arrays are passed**

### Step 8a: For each try/except block, remove PyArrow branch

**Example**: `is_null()` function around line 95

**Before**:
```python
try:
    import pyarrow as pa
    if isinstance(value, pa.Scalar):
        return not value.is_valid
except ImportError:
    pass
```

**After**: Delete entire try/except block (or keep only fallback if needed)

### Step 8b: After removing each block, verify function still works
```bash
python -c "from opteryx.types._null_handling import is_null; print(is_null(None))"
```

### Step 8c: Repeat for all 5 blocks

### Step 8d: Final verification
```bash
python -m py_compile opteryx-core/opteryx/types/_null_handling.py
```

### Step 8e: Commit
```bash
git add opteryx-core/opteryx/types/_null_handling.py
git commit -m "refactor: remove anti-pattern try/except imports from _null_handling

Per architectural rule 9 (fail-fast), removed gated PyArrow imports.
Verified all callers use Draken vectors or numpy arrays.

- Removed try/except from is_null()
- Removed try/except from is_nan()
- Removed try/except from is_inf()
- Removed try/except from is_null_vector()
- Removed try/except from null_count_vector()

Audit completed in [issue #XYZ]"
```

---

## Checkpoint 2: Test Anti-Pattern Removal

If Task 8 was completed:

```bash
make q
```

**Success criteria**:
- [ ] All tests pass
- [ ] `types/_null_handling.py` works correctly
- [ ] No import errors

**If tests fail**:
- [ ] Audit may have missed a PyArrow scalar/array caller
- [ ] Restore try/except blocks
- [ ] Escalate to architect

---

## Task 9: Verify All Changes (2 hours)

### Step 9a: Run full regression suite
```bash
make test
```

**Expected**: 100% pass rate

**If failures occur**:
- [ ] Review error message
- [ ] Identify which file caused failure
- [ ] Check if removed import is actually needed (false positive in analysis)
- [ ] Restore import if necessary
- [ ] Update analysis documents with correction
- [ ] Re-run tests

### Step 9b: Check for import errors specifically
```bash
python -c "import opteryx; print('✓ Import successful')"
```

### Step 9c: Quick smoke test
```bash
python << 'EOF'
import opteryx

# Quick sanity check
session = opteryx.session()
print("✓ Session creation successful")

# Try a simple query (if possible)
try:
    results = session.sql("SELECT 1 as test")
    print("✓ Query execution successful")
except Exception as e:
    print(f"⚠ Query test: {e}")

EOF
```

### Step 9d: Check for dead imports in modified files
```bash
python -m vulture opteryx-core/opteryx/expression/functions/registrar/ \
  opteryx-core/opteryx/operators/ \
  opteryx-core/opteryx/models/ \
  opteryx-core/opteryx/planner/ \
  opteryx-core/opteryx/types/ \
  --min-confidence 90 | grep "unused import"
```

Expected: No pyarrow imports should appear

---

## Task 10: Review & Clean Up

### Step 10a: Review all commits
```bash
git log --oneline feature/pyarrow-phase-5-1 -n 7
```

### Step 10b: Check total imports removed
```bash
git diff main feature/pyarrow-phase-5-1 -- '*.py' '*.pyx' | grep -c "^-.*import pyarrow"
```

Expected: At least 8-10 lines removed

### Step 10c: Verify no new PyArrow imports added
```bash
git diff main feature/pyarrow-phase-5-1 -- '*.py' '*.pyx' | grep "^+.*import pyarrow"
```

Expected: No output

### Step 10d: Update tracking
- [ ] Document import count reduction in issue
- [ ] Note any audit findings about `_null_handling.py`
- [ ] Note any false positives in analysis (for correction)

---

## Final Checkpoint: Merge Readiness

Before creating PR, confirm:

- [ ] `make q` passes (quick regression)
- [ ] `make test` passes (full regression)
- [ ] All commits have clear, descriptive messages
- [ ] No new PyArrow imports added
- [ ] Anti-pattern audit completed and documented
- [ ] False positives in analysis noted for correction
- [ ] Code review checklist:
  - [ ] Dead imports actually unused
  - [ ] No syntax errors
  - [ ] Commit messages clear
  - [ ] Related tests still pass

---

## Create Pull Request

### PR Template:

```markdown
## Phase 5.1: PyArrow Dead Imports & Anti-Pattern Fixes

**Related**: [Link to issue]
**Status**: Ready for review

### Summary
- Removed 6 confirmed dead PyArrow imports
- Audited anti-pattern violations in `types/_null_handling.py`
- [If applicable: Removed try/except gating per Rule 9]

### Changes
- `expression/functions/registrar/arithmetic.py` - removed dead import
- `expression/functions/registrar/arithmetic_extended.py` - removed dead import
- `operators/distinct_node.pyx` - removed dead import
- `operators/non_equi_join_node.pyx` - removed dead imports (2)
- `models/execution_context.py` - removed dead import
- `planner/optimizer/strategies/statistics_only_response.py` - removed dead import
- [If applicable: `types/_null_handling.py` - removed anti-pattern try/except]

### Testing
- [x] `make q` passes
- [x] `make test` passes
- [x] Import verification successful
- [x] Smoke test successful

### Audit Results
[Paste anti-pattern audit results if applicable]

### False Positives Found
[List any files that were misclassified in analysis]

### Post-Merge
Next: Phase 5.2 (Type System & Utilities)
- [ ] Implement Draken null vector factory
- [ ] Begin type coercion consolidation
```

---

## Post-Merge Tasks

After PR is merged:

- [ ] Delete feature branch: `git branch -d feature/pyarrow-phase-5-1`
- [ ] Update `PYARROW_ELIMINATION_ANALYSIS.md` with:
  - [ ] Completed items marked ✓
  - [ ] Any false positives corrected
  - [ ] Anti-pattern audit results documented
- [ ] Create ticket for Phase 5.2
- [ ] Review blockers for Phase 5.2:
  - [ ] Draken null vector factory implemented?
  - [ ] Type marker system designed?

---

## Troubleshooting

### Error: "ImportError: cannot import name 'compute' from pyarrow"

**Cause**: Likely a false positive; the file actually uses compute.

**Fix**:
1. Restore the import
2. Search for actual usage: `grep -n "compute\." [file]`
3. Note discrepancy in analysis
4. Update analysis document

---

### Error: Tests fail after removal

**Cause**: An import was actually needed (false positive in analysis).

**Fix**:
1. Restore the import
2. Verify tests pass
3. Note which file was a false positive
4. Update `PYARROW_ANALYSIS_VERIFICATION_NOTE.md`

---

### Error: Module import fails

**Cause**: Cascading dependency - another file imports from a modified file.

**Fix**:
1. Check error traceback for calling file
2. Ensure that calling file still has its imports
3. If calling file removed import, restore it

---

## Success Metrics

**After Phase 5.1 completion**:

- [ ] 6+ dead imports removed
- [ ] 5 anti-pattern violations fixed (if applicable)
- [ ] 100% test pass rate maintained
- [ ] No new PyArrow imports introduced
- [ ] All commits documented
- [ ] Analysis documents updated

**Expected time**: 10-12 hours  
**Expected impact**: -10-15 imports, improved code health, Rule 9 compliance

---

## Sign-Off

- [ ] Code review: [Name]
- [ ] Architecture review: [Name] (if anti-patterns modified)
- [ ] QA sign-off: [Name]

**Phase 5.1 Complete**: ___________

---

## Next Phase

Phase 5.2 begins when:
- [ ] Phase 5.1 merged to main
- [ ] Draken null vector factory available
- [ ] Type marker system designed
- [ ] Phase 5.2 blockers cleared