# NumPy/PyArrow Eradication: Master Index

**Status:** 86/88 tests passing | Analysis Complete | Ready for Implementation  
**Last Updated:** Current  
**Objective:** Eliminate NumPy and PyArrow from query execution hot paths

---

## 📚 Documentation Map

This directory contains the complete analysis and implementation guide for removing NumPy and PyArrow dependencies from Opteryx's performance-critical code paths.

### For Quick Start 🚀
1. **[ERADICATION-SUMMARY.md](./ERADICATION-SUMMARY.md)** — Read this first
   - Executive summary of the challenge and opportunity
   - What we found and why it matters
   - Implementation strategy overview
   - Success criteria and timeline

### For Detailed Analysis 📊
2. **[numpy-pyarrow-eradication-analysis.md](./numpy-pyarrow-eradication-analysis.md)** — Comprehensive breakdown
   - All 56 files categorized by severity (HOT/WARM/COLD)
   - Specific usage patterns in each file
   - Detailed eradication roadmap by phase
   - Risk mitigation strategies

### For Project Management 📋
3. **[eradication-tracking-matrix.md](./eradication-tracking-matrix.md)** — Progress tracking
   - Priority matrix of all files
   - Status tracking template
   - Phase descriptions
   - Dependency graph for sequencing

### For Implementation 🔧
4. **[eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md)** — Code patterns
   - 9 concrete implementation patterns with before/after code
   - Type coercion Cython template
   - Testing and validation examples
   - Checklist for each pattern

### For File Reference 📑
5. **[ERADICATION-FILES.md](./ERADICATION-FILES.md)** — File-by-file guide
   - Quick lookup for all 56 files
   - Imports, key functions, and line numbers
   - Replacement strategy for each file
   - Dependency graph visualization

---

## 🎯 Quick Navigation by Role

### If you're the architect/reviewer:
1. Read [ERADICATION-SUMMARY.md](./ERADICATION-SUMMARY.md) (5 min)
2. Review [numpy-pyarrow-eradication-analysis.md](./numpy-pyarrow-eradication-analysis.md) sections on risk/timeline (10 min)
3. Check [eradication-tracking-matrix.md](./eradication-tracking-matrix.md) for phasing (5 min)

### If you're implementing Phase 1:
1. Read [ERADICATION-SUMMARY.md](./ERADICATION-SUMMARY.md) - Executive Summary (5 min)
2. Find your file in [ERADICATION-FILES.md](./ERADICATION-FILES.md) - PHASE 1 section (2 min)
3. Read relevant pattern in [eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md) (10 min)
4. Start coding, reference examples in pattern doc (ongoing)

### If you're implementing Phase 2-3:
1. Reference [eradication-tracking-matrix.md](./eradication-tracking-matrix.md) for dependencies (2 min)
2. Check file status in [eradication-tracking-matrix.md](./eradication-tracking-matrix.md) matrix (1 min)
3. Find file details in [ERADICATION-FILES.md](./ERADICATION-FILES.md) (2 min)
4. Apply patterns from [eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md) (ongoing)

### If you're coordinating the effort:
1. Use [eradication-tracking-matrix.md](./eradication-tracking-matrix.md) for status updates
2. Update progress bar in tracking matrix
3. Cross-reference [ERADICATION-FILES.md](./ERADICATION-FILES.md) for dependencies when scheduling PRs
4. Ensure Phase 1 → Phase 2 → Phase 3 sequencing

---

## 📈 At a Glance

### The Numbers
- **56 files** import numpy or pyarrow
- **16 files** in HOT paths (🔴 CRITICAL - must eradicate)
- **21 files** in WARM paths (🟡 MEDIUM - should review)
- **19 files** in COLD paths (🟢 ACCEPTABLE - no action needed)

### The Phases
| Phase | Files | Timeline | Impact | Status |
|-------|-------|----------|--------|--------|
| **1: CRITICAL** | 3 | 2-3 weeks | 15-30% faster | ⏳ TODO |
| **2: HIGH** | 8 | 2-3 weeks | 30-50% faster | ⏳ TODO |
| **3: MEDIUM** | 9 | 1-2 weeks | 50%+ faster | ⏳ TODO |
| **4: WARM** | 9 | 1 week | Cleanup | ⏳ TODO |
| **5: COLD** | 27 | — | No action | ✅ SKIP |

### Start Here (Priority Order)
1. `opteryx/expression/operations/__init__.py` — Filter operations (HIGHEST IMPACT)
2. `opteryx/expression/operations/comparisons.py` — Comparison operators
3. `opteryx/expression/__init__.py` — Logical operations

---

## 🚀 Implementation Path

### Week 1: Foundation (Phase 1)
```
Mon-Wed: operations/__init__.py
         - Replace numpy.logical_or/place with Draken
         - Update null handling
         - Tests passing: make q
         
Thu-Fri: operations/comparisons.py  
         - Migrate to Draken comparison kernels
         - Tests passing: make q
```

### Week 2-3: Hot Paths (Phase 2)
```
Week 2: expression/__init__.py (LOGICAL_OPERATIONS)
        operations/string_matching.py
        operations/list_ops.py
        
Week 3: binary_operators.py
        unary_operations.py
        evaluator/type_coercion.py
```

### Week 4-5: Secondary (Phase 3)
```
Week 4: evaluator/arithmetic.py
        operations/fastpath_*.py
        operations/type_coercion.py
        
Week 5: evaluator/arithmetic_dispatch.py
        evaluator/function_execution.py
        evaluator/comparisons.py
        evaluator/temporal_ops.py
        operations/array_ops.py
```

### Week 6: Polish (Phase 4)
```
Function implementations
Function registrars
Consolidation and cleanup
Final benchmarking
```

---

## 📋 Key Files to Focus On

### Must Change (Phase 1 - CRITICAL)
- ✏️ `opteryx/expression/operations/__init__.py` — Filter dispatch
- ✏️ `opteryx/expression/operations/comparisons.py` — Comparisons
- ✏️ `opteryx/expression/__init__.py` — Logical ops

### Should Change (Phase 2 - HIGH)
- ✏️ `opteryx/expression/operations/string_matching.py`
- ✏️ `opteryx/expression/operations/list_ops.py`
- ✏️ `opteryx/expression/binary_operators.py`
- ✏️ `opteryx/expression/unary_operations.py`
- ✏️ `opteryx/expression/evaluator/type_coercion.py`
- ✏️ `opteryx/expression/evaluator/arithmetic.py`
- ✏️ `opteryx/expression/operations/fastpath_constant.py`
- ✏️ `opteryx/expression/operations/fastpath_dictionary.py`

### Nice to Change (Phase 3 & 4)
- ⏸️ 17 additional files (see tracking matrix)

### Don't Touch (Phase 5 - ACCEPTABLE)
- ✅ 19 files in COLD paths (planning, schema, init)
- ✅ PyArrow at query boundaries (serial_engine.py)

---

## 🔧 Tools & Commands

### Testing
```bash
# Minimum regression suite (required for every PR)
make q

# Full test suite
make test

# Performance benchmark
make clickbench

# Quick compile + test
make c && make q
```

### Verification
```bash
# Check remaining imports
grep -r "import numpy\|from numpy\|import pyarrow\|from pyarrow" \
  opteryx/expression/operations/__init__.py

# Run specific test
python -m pytest tests/test_operators/test_filter.py -v
```

---

## 📌 Success Criteria

- ✅ All 88 tests passing
- ✅ No numpy/pyarrow in core hot-path files
- ✅ 30-50% query performance improvement
- ✅ All expression evaluation on Draken vectors
- ✅ PyArrow only at boundaries (serial_engine.py, connectors)

---

## 🤝 Collaboration

### Documenting Progress
Update [eradication-tracking-matrix.md](./eradication-tracking-matrix.md):
1. Change status from ⏳ TODO → 🔧 IN PROGRESS → ✓ DONE
2. Update progress bar at top of matrix
3. Note any issues discovered

### Creating a PR
Follow this template:
```
Title: Eradicate NumPy/PyArrow from [filename]

Phase: 1/2/3/4
Files: [list changed files]
Imports Removed: numpy, pyarrow
Draken APIs Used: [list specific vector_ops or BoolVector methods]

Changes:
- Replaced numpy.logical_or() with BoolVector.or_()
- Replaced pyarrow.compute.equal() with vector_ops.vector_equal_*()
- [other changes]

Performance: [before/after times from make clickbench]
Tests: make q [result]
```

### Code Review Checklist
- [ ] No numpy/pyarrow imports at top of file
- [ ] All tests passing (make q)
- [ ] Draken vectors used consistently
- [ ] No unnecessary allocations
- [ ] Performance not regressed (or improved)
- [ ] Follows patterns in eradication-patterns-and-examples.md

---

## 🎓 Learning Resources

### Understanding Draken
- Draken vectors: `opteryx/compiled/draken/vectors/`
- Vector kernels: `opteryx/compiled/vector_ops/`
- Interop: `opteryx/compiled/draken/interop/arrow.py`

### Understanding the Expression System
- Expression evaluator: `opteryx/expression/__init__.py`
- Binary operators: `opteryx/expression/binary_operators.py`
- Operations dispatch: `opteryx/expression/operations/`

### Understanding the Patterns
- Read [eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md)
- Each pattern has BEFORE/AFTER code
- Test templates included

---

## ❓ FAQ

**Q: What if a Draken kernel doesn't exist yet?**  
A: Check in `opteryx/compiled/vector_ops/`. If not there, it may need to be added. Flag in PR.

**Q: Can I skip COLD path files?**  
A: Yes! They're acceptable. Only focus on HOT and WARM paths.

**Q: How do I know if my implementation is fast enough?**  
A: Run `make clickbench` before and after. Should see improvement.

**Q: What if tests fail?**  
A: Check the pattern in eradication-patterns-and-examples.md. Reference existing working code in Phase 1 PRs.

**Q: Can I work on multiple phases in parallel?**  
A: Phase 1 must complete first. Phase 2 can start once Phase 1 is merged. Phase 3-4 can be in parallel.

---

## 📞 Support

If you have questions:
1. Check [eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md) for your specific pattern
2. Look at Phase 1 PRs for working examples
3. Reference [ERADICATION-FILES.md](./ERADICATION-FILES.md) for file specifics
4. Review [numpy-pyarrow-eradication-analysis.md](./numpy-pyarrow-eradication-analysis.md) for context

---

## 📖 Document Versions

| Document | Purpose | Audience | Read Time |
|----------|---------|----------|-----------|
| ERADICATION-SUMMARY.md | Overview & strategy | Architects, reviewers | 15 min |
| numpy-pyarrow-eradication-analysis.md | Detailed breakdown | Engineers, architects | 30 min |
| eradication-tracking-matrix.md | Progress & sequencing | Project managers | 10 min |
| eradication-patterns-and-examples.md | Implementation guide | Implementers | 45 min |
| ERADICATION-FILES.md | File reference | All | 20 min |
| **ERADICATION-INDEX.md** | **This document** | **Entry point** | **5 min** |

---

## 🎯 Next Action

**Ready to start?**

1. Read [ERADICATION-SUMMARY.md](./ERADICATION-SUMMARY.md) (5 min)
2. Find your file in [ERADICATION-FILES.md](./ERADICATION-FILES.md) - PHASE 1 (2 min)
3. Open [eradication-patterns-and-examples.md](./eradication-patterns-and-examples.md) for pattern reference
4. Start with Pattern 1 or Pattern 2 (depending on your file)
5. Create PR with title: "Eradicate NumPy/PyArrow from [filename]"

---

**Good luck! The fairies are counting on you.** 🧚✨