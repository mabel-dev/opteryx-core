# PyArrow Elimination Initiative - Executive Summary

**Initiative**: Phase 5 PyArrow Removal  
**Scope**: opteryx-core/opteryx package (363 files total)  
**Status**: ANALYSIS COMPLETE - Ready for execution  
**Architect Approval Required**: Yes (strategic decisions needed)

---

## Current State

**PyArrow Dependency Footprint**:
- **Files affected**: 64 out of 363 (17.6%)
- **Import statements**: 105+ across codebase
- **Primary clusters**: Expression evaluator, operators, type system, utilities

**Usage Breakdown**:
- Type checking (pyarrow.types.*): 20+ files
- Array construction (pa.array, pa.nulls): 25+ files  
- Compute kernels (compute.*): 35+ files
- Schema/interop: 8+ files

**Architectural Violations Found**: 
- 5 imports gated behind try/except (violates Rule 9: fail-fast principle)
- Strategic misalignment with "eradicate PyArrow" goal

---

## Elimination Roadmap

### Phase 5.1: Dead Code & Anti-Patterns (1 week)
**Goal**: Quick wins and code health improvements

**Actions**:
- ✅ Remove 6 confirmed dead imports (~30 min)
- ✅ Fix 5 try/except anti-patterns in `types/_null_handling.py` (~2-3 hours)
- ✅ Clean 8-10 low-impact utility files (~8-10 hours)

**Impact**: -11 dead imports, -5 architectural violations, improved clarity  
**Risk**: Minimal ✅  
**Blockers**: None

---

### Phase 5.2: Type System & Utilities (2-3 weeks)
**Goal**: Decouple core infrastructure from PyArrow

**Actions**:
- Consolidate type coercion (`types/_scalar_to_vector.py`, `expression/evaluator/type_coercion.py`)
- Replace Arrow nulls with Draken vector factory
- Remove Arrow schema conversions (move to IO layer)
- Eliminate Arrow type checks (replace with internal type marker)

**Files Affected**: ~20 files, 40-50 import removals  
**Impact**: Medium - cleaner architecture, Draken-first design  
**Risk**: Low-Medium (low direct usage, but foundational)  
**Blockers**: Draken null vector factory (must exist first)

---

### Phase 5.3: Function Kernels & Operations (3-4 weeks)
**Goal**: Replace PyArrow compute kernels with Draken equivalents

**Actions**:
- String functions: Replace `compute.ascii_upper`, `compute.length`, etc.
- Temporal functions: Replace `compute.year`, `compute.cast`, etc.
- Filter operations: Replace `compute.filter`, `compute.is_null`, `compute.cast`

**Files Affected**: ~25 files (text.py, temporal.py, ops.py sub-paths)  
**Impact**: High - improves performance by removing intermediates  
**Risk**: Medium (hot path, requires validation)  
**Blockers**: Draken string/temporal kernel expansion

---

### Phase 5.4: Operators & Advanced Features (4-6 weeks)
**Goal**: Complete operators and complex operations

**Actions**:
- Join operators: Standardize on Draken vector format
- Read operators: Ensure connector compatibility
- Advanced features: Nested types, special operations

**Files Affected**: ~12 files (join_node.pyx, read_node.pyx, etc.)  
**Impact**: High - affects query execution core  
**Risk**: High (critical path, join correctness)  
**Blockers**: All prior phases must be complete

---

### Phase 6: Connector/IO Layer (Future Sprint)
**OUT OF SCOPE** for Phase 5  
**Rationale**: Parquet reading through `pyarrow.parquet` is intentional and foundational to data ingestion. Handle in dedicated connector refactor.

---

## Quick Wins (Start Here)

### Immediate Actions (Today - 1-2 hours)

✅ **Remove dead imports** (6 files):
```
- expression/functions/registrar/arithmetic.py
- expression/functions/registrar/arithmetic_extended.py
- operators/distinct_node.pyx
- operators/non_equi_join_node.pyx
- models/execution_context.py
- planner/optimizer/strategies/statistics_only_response.py
```

This is SAFE and yields immediate improvement.

### This Week (4-6 hours)

✅ **Audit & remove anti-patterns** (types/_null_handling.py):
- 5 imports with try/except gating (violates fail-fast principle)
- Audit all 50+ call sites
- Remove PyArrow branches entirely (require Draken)

---

## Risk Assessment & Mitigation

### High-Risk Areas

| Risk | Impact | Mitigation |
|------|--------|-----------|
| **Hot path degradation** (`expression/ops.py` - 84 usage) | Performance regression | Use clickbench baseline, measure early, don't rush Phase 5.3 |
| **Join operator correctness** (unnest, outer, non-equi) | Query results wrong | Full regression suite + correctness tests before commit |
| **Type coercion bugs** (scattered across 5+ files) | Silent data corruption | Consolidated type system first, comprehensive auditing |

### Blockers That Must Be Resolved First

1. **Draken null vector factory** - needed for `pa.nulls()` replacement
2. **Internal type marker system** - needed to replace `isinstance(x, pa.Table)` checks
3. **Draken compute kernel expansion** - string, temporal, comparison operations
4. **Verified call sites** - `types/_null_handling.py` (50+ locations)

**Recommendation**: Secure these blockers before Phase 5.2 starts.

---

## Success Criteria

- [ ] **All 64 files** either PyArrow-free OR strategic (connectors only)
- [ ] **No dead imports** - clean codebase (use `vulture` to audit)
- [ ] **No anti-patterns** - no try/except import gating (enforce in CI/pre-commit)
- [ ] **Performance held** - `make clickbench` >= baseline
- [ ] **Regression suite passes** - `make test` passes on all phases
- [ ] **Documented decision** - strategic PyArrow use in IO layer justified and tracked

---

## Resource Estimate

### Effort by Phase

| Phase | Effort | Risk | Confidence |
|-------|--------|------|-----------|
| 5.1 (Dead code) | 1 week | LOW | HIGH ✅ |
| 5.2 (Type system) | 2-3 weeks | LOW-MED | MEDIUM 🟡 |
| 5.3 (Compute kernels) | 3-4 weeks | MEDIUM | MEDIUM 🟡 |
| 5.4 (Operators) | 4-6 weeks | HIGH | LOW 🔴 |
| **Total** | **10-14 weeks** | - | - |

**Recommended**: 1-2 developer, focus on Phase 5.1 & 5.2 immediately. Phase 5.3 & 5.4 in subsequent sprints.

---

## Comparison: NumPy Elimination (Lessons Applied)

We successfully eliminated NumPy from most of the codebase. PyArrow elimination follows similar patterns:

✅ **Lessons Learned**:
1. Start with dead code (easy wins, boost morale)
2. Fix anti-patterns first (clarifies intent)
3. Build replacement infrastructure incrementally (don't rush)
4. Measure performance aggressively (perf regressions are hidden killers)
5. Batch small file removals together (reduces CI churn)

✅ **Apply to PyArrow**:
- Use same approach: dead code → anti-patterns → replacement → migration
- Build Draken kernel suite first (prerequisite)
- Use same testing strategy (regression + clickbench)

---

## Key Decisions Required from Architecture

### Decision 1: Parquet I/O Strategy
**Question**: Keep `pyarrow.parquet` in connector layer, or eliminate entirely?  
**Options**:
- **Option A** (Recommended): Keep parquet via PyArrow, defer to Phase 6
- **Option B**: Migrate to Rugo parquet decoder (requires evaluation)

**Recommendation**: Option A. Parquet is foundational; handle in connector refactor.

---

### Decision 2: Dictionary Encoding Fastpath
**Question**: The dictionary fastpath in `expression/ops.py` uses PyArrow types. Should we keep it for performance or refactor to Draken?

**Options**:
- **Option A**: Keep dictionary fastpath (strategic performance)
- **Option B**: Refactor to Draken dictionary vectors (cleaner architecture)

**Recommendation**: Option B (refactor). Aligns with "Draken-first" architecture. Requires validation that performance is maintained.

---

### Decision 3: Try/Except Anti-Pattern
**Question**: The `types/_null_handling.py` module violates Rule 9 by gating imports behind try/except. How should we handle this?

**Options**:
- **Option A**: Remove PyArrow branches; require Draken/NumPy paths
- **Option B**: Refactor to support multi-backend gracefully (requires design)

**Recommendation**: Option A. Violates fail-fast principle. Clean removal.

---

## Timeline Proposal

**Milestone 1 (Week 1)**: Phase 5.1 - Dead code + anti-patterns  
→ Checkpoint: `make q` passes, no regressions

**Milestone 2 (Weeks 2-3)**: Phase 5.2 - Type system consolidation  
→ Checkpoint: `make test` passes, Draken kernels validated

**Milestone 3 (Weeks 4-6)**: Phase 5.3 - Compute kernel replacement  
→ Checkpoint: `make clickbench` validates performance

**Milestone 4 (Weeks 7-10)**: Phase 5.4 - Operators (if time allows)  
→ Checkpoint: `make test` + execution tests pass

---

## Related Work Items

1. **Draken Kernel Expansion** (blocking Phase 5.3)
   - Temporal functions (year, month, cast, etc.)
   - String functions (upper, lower, length, etc.)
   - Null handling (is_null, coalesce, etc.)

2. **Type System Consolidation** (needed for Phase 5.2)
   - Unified type marker for Arrow/Draken distinction
   - Type predicate library (replaces `pyarrow.types.*`)

3. **Connector Layer Design** (Phase 6)
   - Strategic use of PyArrow for data ingestion
   - Clear contracts for external data sources

---

## Recommended First Actions

### This Week:
1. Review PYARROW_ELIMINATION_ANALYSIS.md in detail
2. Resolve architectural decisions (see above)
3. Remove 6 confirmed dead imports
4. Audit `types/_null_handling.py` call sites

### Next Sprint:
1. Fix anti-patterns in `types/_null_handling.py`
2. Implement Draken null vector factory
3. Begin Phase 5.2 (type system consolidation)
4. Start Draken kernel expansion planning

---

## Contacts & Escalation

- **Architecture**: Requires review of Phase 5.3+ risk profile
- **Performance**: clickbench must be baselined before Phase 5.3
- **Test Lead**: Regression suite + execution tests for Phase 5.4

---

## Success Story Impact

**Upon Completion**:
- ✅ PyArrow fully eradicated from core opteryx code (except strategic IO layer)
- ✅ Codebase 100% aligned with "fail-fast" architecture
- ✅ Improved performance (fewer intermediates)
- ✅ Cleaner Draken integration (single execution path)
- ✅ Reduced dependency footprint

**Message**: "Opteryx now owns its entire execution stack—from SQL to Draken to SIMD."

---

## Document Index

- `PYARROW_ELIMINATION_ANALYSIS.md` — Detailed file-by-file analysis with priorities
- `PYARROW_ANALYSIS_VERIFICATION_NOTE.md` — Corrections and verification findings
- This document — Executive summary and decision points