# PyArrow Elimination Initiative - Documentation Index

**Phase**: 5 (PyArrow Removal)  
**Status**: Analysis Complete - Ready for Execution  
**Last Updated**: 2024

---

## 📚 Documentation Overview

This directory contains comprehensive analysis and actionable guidance for eliminating PyArrow from the opteryx codebase. Start here to understand the initiative.

### For Different Audiences

**👨‍💼 Decision Makers / Architects**:
→ Start with: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md`
- High-level overview of effort, risk, timeline
- Strategic decisions required
- Resource estimates
- Phase breakdown

**👨‍💻 Engineers Executing Phase 5.1**:
→ Start with: `PYARROW_PHASE_5_1_CHECKLIST.md`
- Step-by-step task list
- Verification commands
- Success criteria
- Troubleshooting guide

**🔬 Technical Deep Dive**:
→ Start with: `PYARROW_ELIMINATION_ANALYSIS.md`
- All 64 files categorized by priority
- Usage patterns explained
- Replacement strategies outlined
- Risk assessment per tier

**⚠️ Accuracy & Limitations**:
→ Read: `PYARROW_ANALYSIS_VERIFICATION_NOTE.md`
- Analysis corrections and caveats
- False positives identified
- Methodology limitations
- Verification recommendations

---

## 📋 Document Directory

| Document | Purpose | Audience | Length |
|----------|---------|----------|--------|
| **PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md** | Strategic overview, decisions, timeline | Architects, PMs, leads | 10 min read |
| **PYARROW_ELIMINATION_ANALYSIS.md** | Detailed file-by-file analysis with priorities | Engineers, architects | 30 min read |
| **PYARROW_ANALYSIS_VERIFICATION_NOTE.md** | Corrections, caveats, methodology notes | QA, auditors, reviewers | 15 min read |
| **PYARROW_PHASE_5_1_CHECKLIST.md** | Executable task list for Phase 5.1 | Engineers (hands-on) | Reference |
| **PYARROW_ELIMINATION_README.md** | This file - navigation guide | Everyone | 5 min read |

---

## 🎯 Quick Navigation

### "I need to understand the scope"
→ See: **PYARROW_ELIMINATION_ANALYSIS.md** → Section "Summary Table: All 64 Files"

### "I need to decide if we should do this"
→ See: **PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md** → Section "Risk Assessment & Mitigation"

### "I need to start removing dead imports today"
→ See: **PYARROW_PHASE_5_1_CHECKLIST.md** → Section "Task 1-6"

### "I need to understand the anti-pattern violations"
→ See: **PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md** → Section "Key Decisions Required"

### "The analysis says this file is dead, but I think it's not"
→ See: **PYARROW_ANALYSIS_VERIFICATION_NOTE.md** → Section "Actual DEAD IMPORTS"

### "What's the timeline for complete removal?"
→ See: **PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md** → Section "Elimination Roadmap"

### "What files should I focus on?"
→ See: **PYARROW_ELIMINATION_ANALYSIS.md** → Section "Elimination Priority Map"

---

## 🚀 Quick Start

### For Execution Teams (Start Here)

1. **Read**: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` (10 min)
   - Understand scope and risk profile
   - Verify team is ready to proceed

2. **Read**: `PYARROW_PHASE_5_1_CHECKLIST.md` (overview section)
   - Understand Phase 5.1 timeline and scope
   - Plan task assignment

3. **Execute**: Follow `PYARROW_PHASE_5_1_CHECKLIST.md` step-by-step
   - Each task has detailed sub-steps
   - Includes validation commands
   - Includes troubleshooting

4. **Validate**: Run checkpoints
   ```bash
   make q  # Quick regression (after each task group)
   make test  # Full regression (after Phase 5.1 complete)
   ```

5. **Review**: Check `PYARROW_ANALYSIS_VERIFICATION_NOTE.md`
   - Understand analysis limitations
   - Note false positives for correction
   - Plan Phase 5.2

---

## 📊 Key Findings at a Glance

### By the Numbers
- **64 files** import PyArrow
- **105+ import statements** across codebase
- **6 confirmed dead imports** (safe to remove today)
- **5 anti-pattern violations** (try/except gating)
- **~8-15 weeks** for complete removal (phased)

### Quick Wins (Phase 5.1)
- Remove 6 dead imports: **30 minutes**
- Fix anti-patterns: **2-4 hours**
- Verify suspicious files: **2-3 hours**
- **Total**: 1 week effort, minimal risk

### Major Blockers for Later Phases
- Draken null vector factory
- Internal type marker system
- Draken compute kernel expansion
- Unified type system consolidation

---

## 🎓 Understanding the Analysis

### File Categorization Tiers

**TIER 0**: Confirmed dead (safe to remove immediately)
- Files with 0 usage in analysis
- Manually verified no hidden dependencies
- Examples: `arithmetic.py` registrar files

**TIER 1**: Anti-patterns (violate architectural rules)
- Try/except gating imports (Rule 9 violation)
- Requires audit but clear fix path
- Examples: `types/_null_handling.py`

**TIER 2**: Quick wins (low complexity, low risk)
- Utilities with isolated usage
- Replace with Draken equivalents
- Timeline: 2-3 weeks

**TIER 3**: Medium complexity (type system)
- Type coercion, schema conversion
- Requires consolidation
- Timeline: 3-4 weeks

**TIER 4**: High complexity (expression evaluator)
- Hot paths with compute kernels
- Requires Draken kernel expansion
- Timeline: 4-6 weeks

**TIER 5**: Critical (operators, I/O)
- Join nodes, read operators
- Architectural impact
- Timeline: 2-3 weeks

**TIER 6**: Out of scope (connectors)
- Parquet I/O, external data
- Handle in Phase 6
- Keep intentionally

---

## ⚠️ Important Notes

### Analysis Limitations

The analysis uses regex heuristics which have edge cases:
- **False negatives**: Some usage patterns not caught (commented code, type hints)
- **False positives**: Some imports appear unused but aren't
- **Recommendation**: Manually verify any file before removal

See: `PYARROW_ANALYSIS_VERIFICATION_NOTE.md` for detailed limitations.

### Architectural Rules Being Enforced

This initiative enforces the architectural rules from `.github/copilot-instructions.md`:

**Rule 9 (Critical)**: "Do not gate imports behind try/except, failing at import time if a required dependency is missing rather than silently degrading functionality"

**Rule 3**: "Never duplicate logic in Python and Cython unless explicitly requested"

**Rule 2**: "The execution engine is Cython/C++ glued together with Python"

---

## 📈 Progress Tracking

### Metrics to Track

After each phase, measure:
- **Import count**: Target 0 (or <10 if IO layer kept)
- **Dead imports**: Target 0
- **Anti-pattern violations**: Target 0
- **Test pass rate**: Target 100%
- **Performance**: Target no regression

### Checkpoints

| Checkpoint | Command | Expected |
|-----------|---------|----------|
| **Post Phase 5.1** | `make q` | 100% pass |
| **Post Phase 5.2** | `make test` | 100% pass |
| **Post Phase 5.3** | `make clickbench` | No regression |
| **Pre Phase 5.4** | Manual audit | All operators verified |

---

## 🤝 Decision Points Requiring Approval

**Before Phase 5.2**:
1. **Parquet I/O strategy**: Keep PyArrow for parquet reading?
2. **Dictionary encoding**: Refactor fastpath to Draken or keep for perf?
3. **Anti-pattern handling**: Remove try/except or refactor gracefully?

See: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` → "Key Decisions Required"

---

## 🔄 Phase Flow

```
Phase 5.1 (1 week)
├─ Remove dead imports (6 files)
├─ Fix anti-patterns (5 violations)
└─ Verify suspicious files (3 files)
    ↓
    Checkpoint: make q passes

Phase 5.2 (2-3 weeks)
├─ Consolidate type system
├─ Replace Arrow nulls
├─ Build Draken type marker
└─ Remove schema conversions
    ↓
    Checkpoint: make test passes

Phase 5.3 (3-4 weeks)
├─ Replace compute kernels
├─ String functions
├─ Temporal functions
└─ Filter operations
    ↓
    Checkpoint: make clickbench passes

Phase 5.4 (4-6 weeks)
├─ Operator refactoring
├─ Join nodes
├─ Read/write operators
└─ Advanced features
    ↓
    Checkpoint: All tests + execution

Phase 6 (Future)
└─ Connector/IO layer strategy
    (Intentional PyArrow use)
```

---

## 📞 Contacts & Escalation

### For Questions About...

**Scope/Analysis**:
- Refer to: `PYARROW_ELIMINATION_ANALYSIS.md`
- Escalate to: Architecture lead

**Execution (Phase 5.1)**:
- Refer to: `PYARROW_PHASE_5_1_CHECKLIST.md`
- Escalate to: Engineering lead

**Strategic Decisions**:
- Refer to: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md`
- Escalate to: Architect

**Analysis Accuracy**:
- Refer to: `PYARROW_ANALYSIS_VERIFICATION_NOTE.md`
- Escalate to: QA/auditor

---

## ✅ Success Definition

Phase 5 is complete when:

- [ ] All dead imports removed (6 files)
- [ ] All anti-patterns fixed (5 violations)
- [ ] Type system consolidated
- [ ] Compute kernels replaced with Draken equivalents
- [ ] Operators refactored (joins, read, write)
- [ ] 100% test pass rate maintained
- [ ] Performance at baseline (clickbench)
- [ ] Remaining PyArrow use is intentional and documented (IO layer only)

---

## 📝 Related Documents

**Architecture**:
- `.github/copilot-instructions.md` - Core rules and principles
- `.github/CONTRIBUTING.md` - Development workflow

**Previous Initiatives**:
- NumPy elimination (lessons learned)
- Orso removal (similar complexity)

**Ongoing**:
- Draken kernel expansion (blocking Phase 5.3)
- Type system design (needed for Phase 5.2)
- Connector layer strategy (Phase 6)

---

## 🎓 How to Use These Documents Together

### Scenario 1: "I'm new to this project and need to understand what we're doing"

1. Read: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` (10 min)
2. Skim: `PYARROW_ELIMINATION_ANALYSIS.md` → "Summary Table" section (5 min)
3. Ask questions to your lead

### Scenario 2: "I'm assigned Phase 5.1 starting tomorrow"

1. Read: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` (10 min) - understand context
2. Read: `PYARROW_PHASE_5_1_CHECKLIST.md` - entire document (30 min prep)
3. Create feature branch and start Task 1
4. Keep checklist open as reference during execution

### Scenario 3: "A file is marked 'dead' but I think it has usage"

1. Check: `PYARROW_ANALYSIS_VERIFICATION_NOTE.md` → "Actual DEAD IMPORTS" section
2. If not listed there, manually verify using grep
3. Report finding to update analysis

### Scenario 4: "We need to decide if Phase 5.3 is worth the effort"

1. Review: `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` → "Risk Assessment" section
2. Review: `PYARROW_ELIMINATION_ANALYSIS.md` → "Tier 3 & Tier 4" sections
3. Schedule sync with architecture lead

---

## 🚨 Critical Reminders

**DO NOT**:
- [ ] Remove imports without running `make q` first
- [ ] Proceed past Phase 5.2 without performance baseline
- [ ] Merge anti-pattern fixes without audit of call sites
- [ ] Skip verification steps (listed in checklist)

**DO**:
- [ ] Run full test suite after each phase
- [ ] Document any false positives found
- [ ] Get approval before architectural changes
- [ ] Measure performance impact before/after critical changes

---

## 📖 Version History

| Date | Status | Changes |
|------|--------|---------|
| 2024 | Initial | Complete analysis + Phase 5.1 planning |

---

## 📌 TL;DR

**What**: Remove PyArrow from ~64 files across opteryx codebase

**Why**: Phase 5 of dependency elimination initiative; aligns with "Draken-first" architecture

**When**: 1-2 developers, 10-14 weeks total (phased), starting with Phase 5.1 this sprint

**How**: 
1. Remove dead imports (Phase 5.1) - 1 week, safe
2. Fix type system (Phase 5.2) - 2-3 weeks, low risk
3. Replace compute kernels (Phase 5.3) - 3-4 weeks, medium risk
4. Refactor operators (Phase 5.4) - 4-6 weeks, high risk
5. Keep IO layer intentional (Phase 6) - future sprint

**Where to start**: `PYARROW_PHASE_5_1_CHECKLIST.md` if executing, or `PYARROW_ELIMINATION_EXECUTIVE_SUMMARY.md` if planning

---

**Questions?** Check the relevant document above. Can't find answer? Escalate to architecture lead.