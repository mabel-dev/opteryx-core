#!/usr/bin/env python3
"""
NumPy Usage Inventory and Diagnostics
======================================

This file contains structured data from the NumPy diagnostic audit for opteryx-core.
Use this for quick reference on NumPy usage patterns, priorities, and replacement strategy.

Generated: 2024
Status: Phase 6 Planning
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Tuple


class HotPathLevel(Enum):
    """Priority level for NumPy usage removal"""

    CRITICAL = "CRITICAL (query execution hot path)"
    HIGH = "HIGH (linear scan or frequent setup)"
    MEDIUM = "MEDIUM (occasional operations)"
    LOW = "LOW (utilities, setup)"
    TEST_ONLY = "TEST_ONLY (validation/mocks)"


class ReplacementEffort(Enum):
    """Estimated effort for replacement"""

    TRIVIAL = "Trivial (< 1 hour)"
    LOW = "Low (1-5 hours)"
    MEDIUM = "Medium (5-20 hours)"
    HIGH = "High (20-40 hours)"
    CRITICAL = "Critical (40+ hours, architectural)"


@dataclass
class NumpyUsageFile:
    """Single file with NumPy usage"""

    path: str
    file_type: str  # "pyx", "py", "test"
    line_count: int
    numpy_refs: int
    hot_path_level: HotPathLevel
    effort: ReplacementEffort
    usage_types: List[str] = field(default_factory=list)
    blocking_issues: List[str] = field(default_factory=list)
    notes: str = ""

    def __str__(self):
        return f"{self.path} ({self.numpy_refs} refs, {self.effort.value})"


# ============================================================================
# CRITICAL HOT-PATH FILES - Phase 6a/6b Priority
# ============================================================================

CRITICAL_HOTPATH = [
    NumpyUsageFile(
        path="opteryx/expression/__init__.py",
        file_type="py",
        line_count=850,
        numpy_refs=51,
        hot_path_level=HotPathLevel.CRITICAL,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["array creation", "boolean indexing", "mask operations", "dtype", "arange"],
        blocking_issues=["boolean array indexing pattern", "numpy.place() for null handling"],
        notes="Most critical: every query touches evaluate_dnf/short_cut_and/or. Requires careful refactoring.",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/joins/cross_join.pyx",
        file_type="pyx",
        line_count=180,
        numpy_refs=36,
        hot_path_level=HotPathLevel.CRITICAL,
        effort=ReplacementEffort.HIGH,
        usage_types=["ndarray", "dtype inspection", "empty allocation", "resize"],
        blocking_issues=[
            "Cython type declarations",
            "memoryview syntax",
            "numpy.ndarray type checking",
        ],
        notes="Array-based join output. Core to join operator architecture.",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/joins/inner_join.pyx",
        file_type="pyx",
        line_count=200,
        numpy_refs=19,
        hot_path_level=HotPathLevel.CRITICAL,
        effort=ReplacementEffort.HIGH,
        usage_types=["asarray", "empty", "ndarray type", "dtype"],
        blocking_issues=["Index buffer conversion", "Type casting in hot loop"],
        notes="Probe-side join filtering. Must maintain performance.",
    ),
    NumpyUsageFile(
        path="opteryx/expression/ops.py",
        file_type="py",
        line_count=120,
        numpy_refs=32,
        hot_path_level=HotPathLevel.CRITICAL,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["dtype checking", "asarray coercion", "type validation"],
        blocking_issues=["Type system integration"],
        notes="Binary and unary operator evaluation. Used in every expression.",
    ),
    NumpyUsageFile(
        path="opteryx/expression/binary_operators.py",
        file_type="py",
        line_count=150,
        numpy_refs=23,
        hot_path_level=HotPathLevel.CRITICAL,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["type checking", "integer check", "generic check"],
        blocking_issues=["Type system"],
        notes="Arithmetic and comparison operators. High frequency execution.",
    ),
]

# ============================================================================
# HIGH PRIORITY - Phase 6b/6c
# ============================================================================

HIGH_PRIORITY = [
    NumpyUsageFile(
        path="opteryx/expression/casts.py",
        file_type="py",
        line_count=180,
        numpy_refs=23,
        hot_path_level=HotPathLevel.HIGH,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["datetime64", "dtype conversion", "type coercion"],
        blocking_issues=["datetime64 replacement needed"],
        notes="Type casting engine. Handles temporal types (datetime64).",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/vector_ops/vector_match_against.pyx",
        file_type="pyx",
        line_count=200,
        numpy_refs=15,
        hot_path_level=HotPathLevel.HIGH,
        effort=ReplacementEffort.HIGH,
        usage_types=["linalg.norm", "dot product", "asarray", "array operations"],
        blocking_issues=["Linear algebra replacement"],
        notes="Vector similarity computation. Needs C++ norm/dot implementations.",
    ),
    NumpyUsageFile(
        path="opteryx/operators/heap_sort_node.pyx",
        file_type="pyx",
        line_count=150,
        numpy_refs=15,
        hot_path_level=HotPathLevel.HIGH,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["array operations", "index management"],
        blocking_issues=["Sort index buffer handling"],
        notes="Sorting operator. Critical for ORDER BY execution.",
    ),
    NumpyUsageFile(
        path="opteryx/expression/unary_operations.py",
        file_type="py",
        line_count=120,
        numpy_refs=15,
        hot_path_level=HotPathLevel.HIGH,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["type checking", "conversion"],
        blocking_issues=["Type system"],
        notes="Unary operators (NOT, negation, etc.)",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/table_ops/null_avoidant_ops.pyx",
        file_type="pyx",
        line_count=100,
        numpy_refs=8,
        hot_path_level=HotPathLevel.HIGH,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["array allocation", "masking", "index handling"],
        blocking_issues=["Null mask representation"],
        notes="Null handling in projections. Used in every null-aware filter.",
    ),
]

# ============================================================================
# MEDIUM PRIORITY - Phase 6c/6d
# ============================================================================

MEDIUM_PRIORITY = [
    NumpyUsageFile(
        path="opteryx/vectors/embeddings.py",
        file_type="py",
        line_count=350,
        numpy_refs=64,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["linalg.norm", "dot product", "vstack", "asarray", "float32"],
        blocking_issues=["Vector math implementation"],
        notes="Text embeddings (BM25, static hash provider). NOT in query hot path (setup only).",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/structures/buffers.pyx",
        file_type="pyx",
        line_count=200,
        numpy_refs=13,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.HIGH,
        usage_types=["ndarray I/O", "empty", "ascontiguousarray"],
        blocking_issues=["Buffer interface redesign"],
        notes="Index buffer management. Used across join operators.",
    ),
    NumpyUsageFile(
        path="opteryx/expression/evaluator/type_coercion.py",
        file_type="py",
        line_count=100,
        numpy_refs=11,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.LOW,
        usage_types=["type checking"],
        blocking_issues=["Type system"],
        notes="Type coercion logic. Utility functions.",
    ),
    NumpyUsageFile(
        path="opteryx/compiled/vector_ops/vector_levenshtein.pyx",
        file_type="pyx",
        line_count=100,
        numpy_refs=6,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.LOW,
        usage_types=["zeros array allocation"],
        blocking_issues=["None"],
        notes="Levenshtein distance computation. DP table allocation.",
    ),
    NumpyUsageFile(
        path="opteryx/types/_orso_types.py",
        file_type="py",
        line_count=80,
        numpy_refs=20,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.LOW,
        usage_types=["dtype mapping", "type system"],
        blocking_issues=["Type system redesign"],
        notes="Type registry. Should be first Phase 6 task (unblocks others).",
    ),
]

# ============================================================================
# LOW PRIORITY - Phase 6d/6e or Test Cleanup
# ============================================================================

LOW_PRIORITY = [
    NumpyUsageFile(
        path="opteryx/planner/logical_planner/logical_planner_builders.py",
        file_type="py",
        line_count=300,
        numpy_refs=17,
        hot_path_level=HotPathLevel.LOW,
        effort=ReplacementEffort.LOW,
        usage_types=["type checking"],
        blocking_issues=["Type system"],
        notes="Query planning. Not in execution hot path.",
    ),
    NumpyUsageFile(
        path="opteryx/expression/functions/implementations/temporal.py",
        file_type="py",
        line_count=200,
        numpy_refs=16,
        hot_path_level=HotPathLevel.MEDIUM,
        effort=ReplacementEffort.MEDIUM,
        usage_types=["datetime64 conversion"],
        blocking_issues=["Temporal type replacement"],
        notes="Temporal function implementations (DATE_TRUNC, etc).",
    ),
    NumpyUsageFile(
        path="opteryx/utils/series.py",
        file_type="py",
        line_count=80,
        numpy_refs=9,
        hot_path_level=HotPathLevel.LOW,
        effort=ReplacementEffort.LOW,
        usage_types=["series manipulation"],
        blocking_issues=["None"],
        notes="Utility functions. Not critical.",
    ),
]

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

SUMMARY_STATS = {
    "total_files": 129,
    "total_numpy_refs": 968,
    "cython_files": 26,
    "python_files": 73,
    "test_files": 50,
    "critical_hotpath_files": len(CRITICAL_HOTPATH),
    "high_priority_files": len(HIGH_PRIORITY),
    "medium_priority_files": len(MEDIUM_PRIORITY),
    "low_priority_files": len(LOW_PRIORITY),
}

# ============================================================================
# NUMPY OPERATIONS FREQUENCY
# ============================================================================

OPERATION_FREQUENCY = {
    "numpy.ndarray": (170, "Type declarations and checks"),
    "numpy.array": (168, "Array construction"),
    "numpy.int*": (126, "Integer dtype specification"),
    "numpy.float*": (108, "Float dtype specification"),
    "numpy.empty": (69, "Buffer allocation (deferred fill)"),
    "numpy.datetime64": (52, "Temporal value representation"),
    "numpy.asarray": (50, "Array type coercion"),
    "numpy.bool_": (38, "Boolean dtype"),
    "numpy.full": (20, "Array fill with value"),
    "numpy.zeros": (19, "Zero-initialized array"),
    "numpy.linalg.norm": (16, "Vector normalization"),
    "numpy.import_array": (16, "Cython NumPy initialization"),
    "numpy.uint*": (15, "Unsigned integer dtype"),
    "numpy.integer": (15, "Integer type checking"),
    "numpy.object_": (13, "Object dtype"),
    "numpy.array_equal": (13, "Array comparison"),
    "numpy.arange": (13, "Index sequence generation"),
    "numpy.issubdtype": (12, "Dtype inheritance checking"),
    "numpy.generic": (11, "Base NumPy scalar type"),
    "numpy.nan": (9, "NaN constant"),
}

# ============================================================================
# REPLACEMENT STRATEGIES BY OPERATION
# ============================================================================

REPLACEMENT_STRATEGIES = {
    "numpy.ndarray": {
        "replacement": "Cython custom buffer type or Python ABC",
        "effort": "HIGH",
        "risk": "HIGH (widespread type change)",
        "notes": "Appears 170 times. Core to Cython interface. Requires architecture change.",
    },
    "numpy.array": {
        "replacement": "Python list or custom buffer factory",
        "effort": "MEDIUM",
        "risk": "MEDIUM (behavior change for construction)",
        "notes": "Constructor. Can often be replaced with list + conversion.",
    },
    "numpy.int64/float32": {
        "replacement": "Python int/float or C type constants",
        "effort": "LOW",
        "risk": "LOW (dtype specification)",
        "notes": "Create mapping: int_dtype = 'int64', float_dtype = 'float32'",
    },
    "numpy.empty": {
        "replacement": "Direct malloc in Cython or custom allocator",
        "effort": "MEDIUM",
        "risk": "MEDIUM (memory management)",
        "notes": "69 uses. Need careful memory lifecycle management.",
    },
    "numpy.datetime64": {
        "replacement": "int64 microseconds + wrapper class",
        "effort": "MEDIUM",
        "risk": "MEDIUM (temporal semantics)",
        "notes": "52 uses. Must verify datetime arithmetic still works.",
    },
    "numpy.asarray": {
        "replacement": "Custom type guard or buffer protocol",
        "effort": "MEDIUM",
        "risk": "HIGH (type coercion semantics)",
        "notes": "50 uses. Core to type system. Need careful testing.",
    },
    "numpy.linalg.norm": {
        "replacement": "Custom C++ implementation",
        "effort": "MEDIUM",
        "risk": "MEDIUM (numerical correctness)",
        "notes": "16 uses. Vector similarity calculations. Needs validation.",
    },
    "numpy.zeros/full": {
        "replacement": "Direct allocation + initialization loop",
        "effort": "LOW",
        "risk": "LOW (simple pattern)",
        "notes": "39 combined uses. Straightforward replacement.",
    },
    "numpy.arange": {
        "replacement": "Python range() or custom index generator",
        "effort": "LOW",
        "risk": "LOW (index generation)",
        "notes": "13 uses. Simple sequence generation.",
    },
}

# ============================================================================
# PHASE DECOMPOSITION
# ============================================================================

PHASE_ROADMAP = {
    "Phase_6a_Prerequisite": {
        "name": "Type System Foundation",
        "effort_hours": 3,
        "files": ["opteryx/types/_orso_types.py"],
        "goal": "Remove dtype mappings, unblock other phases",
        "risk": "LOW",
        "test_impact": "86/88 -> 86/88",
    },
    "Phase_6b_ExpressionEval": {
        "name": "Expression Evaluation Refactor",
        "effort_hours": 45,
        "files": [
            "opteryx/expression/__init__.py",
            "opteryx/expression/ops.py",
            "opteryx/expression/binary_operators.py",
            "opteryx/expression/unary_operations.py",
            "opteryx/expression/casts.py",
            "opteryx/expression/operations/*.py",
        ],
        "goal": "Replace mask operations, datetime64, asarray patterns",
        "risk": "MEDIUM (algorithm changes need validation)",
        "test_impact": "86/88 -> 86/88 (with careful testing)",
        "critical_path": True,
    },
    "Phase_6c_CompiledJoins": {
        "name": "Compiled Join Operators Refactor",
        "effort_hours": 40,
        "files": [
            "opteryx/compiled/joins/cross_join.pyx",
            "opteryx/compiled/joins/inner_join.pyx",
            "opteryx/compiled/joins/filter_join.pyx",
            "opteryx/compiled/structures/buffers.pyx",
        ],
        "goal": "Replace ndarray with custom buffers, remove dtype checking",
        "risk": "HIGH (interface changes)",
        "test_impact": "86/88 -> 84/88 initially, then 88/88",
        "critical_path": True,
    },
    "Phase_6d_VectorOps": {
        "name": "Vector Operations Refactor",
        "effort_hours": 20,
        "files": [
            "opteryx/compiled/vector_ops/vector_match_against.pyx",
            "opteryx/compiled/vector_ops/vector_*.pyx",
        ],
        "goal": "Replace linalg.norm, zeros allocation",
        "risk": "MEDIUM (math correctness)",
        "test_impact": "No change (separate code path)",
    },
    "Phase_6e_EmbeddingsUtility": {
        "name": "Embeddings & Utility Layer",
        "effort_hours": 25,
        "files": [
            "opteryx/vectors/embeddings.py",
            "opteryx/expression/functions/implementations/*.py",
        ],
        "goal": "Replace vector math, utility functions",
        "risk": "LOW (not in query hot path)",
        "test_impact": "No change",
    },
}

# ============================================================================
# KEY FINDINGS & RECOMMENDATIONS
# ============================================================================

KEY_FINDINGS = """
FINDINGS:
---------

1. EXPRESSION EVALUATION IS THE KEYSTONE
   - 51 refs in __init__.py, heavily used in every query
   - Boolean indexing pattern: true_indices = true_indices[result_bool]
   - This is NOT trivial to replace without algorithm redesign
   - Must be Phase 6b (high impact)

2. CYTHON INTERFACE IS DEEPLY EMBEDDED
   - 26 Cython files use numpy.ndarray as type annotation
   - Cython's `cdef numpy.ndarray[dtype_t, ndim=1]` is core to type safety
   - Replacement requires buffer protocol or custom ABC
   - This is Phase 6c (architectural)

3. VECTOR OPERATIONS HAVE EXTERNAL DEPENDENCY
   - numpy.linalg.norm() used 16 times
   - No internal BLAS library yet
   - Could use custom C++ or external BLAS
   - This is Phase 6d (medium effort)

4. TYPE SYSTEM CAN BE QUICK WIN
   - _orso_types.py has simple dtype mapping
   - Remove and replace with native types
   - This is Phase 6a prerequisite (unblocks others)

5. TESTS CAN CONTINUE USING NUMPY
   - 50+ test files use numpy for validation
   - Low risk to keep numpy in tests
   - Add adapters (convert_to_numpy) as needed
   - Can be handled separately

RECOMMENDATIONS:
----------------

PRIORITY ORDER:
1. Phase 6a: Type system (3 hours, LOW RISK)
2. Phase 6b: Expression eval (45 hours, MEDIUM RISK, HIGH IMPACT)
3. Phase 6c: Join operators (40 hours, HIGH RISK, CRITICAL)
4. Phase 6d: Vector ops (20 hours, MEDIUM RISK)
5. Phase 6e: Embeddings (25 hours, LOW RISK)

TOTAL EFFORT: ~130-140 hours (3-4 weeks for experienced engineer)

VALIDATION STRATEGY:
- Run `make q` after each phase
- Run `make test` at phase boundaries
- Use ClickBench for performance validation
- Create unit tests for each replaced operation

RISK MITIGATION:
- Create proof-of-concept for buffer replacement
- Pre-implement C++ norm() function
- Extensive masking operation testing
- Keep old code for comparison testing

SUCCESS CRITERIA (Phase 6 Complete):
- All 88 tests passing
- Zero numpy imports in opteryx/compiled/
- Zero numpy imports in opteryx/expression/ (except test utilities)
- Performance maintained or improved
- ClickBench runs successfully
"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def get_all_files() -> List[NumpyUsageFile]:
    """Get all files in priority order"""
    return CRITICAL_HOTPATH + HIGH_PRIORITY + MEDIUM_PRIORITY + LOW_PRIORITY


def get_files_by_priority(level: str) -> List[NumpyUsageFile]:
    """Get files by priority level"""
    mapping = {
        "CRITICAL": CRITICAL_HOTPATH,
        "HIGH": HIGH_PRIORITY,
        "MEDIUM": MEDIUM_PRIORITY,
        "LOW": LOW_PRIORITY,
    }
    return mapping.get(level, [])


def total_refs_by_priority() -> Dict[str, int]:
    """Calculate total refs by priority"""
    result = {}
    for level, files in [
        ("CRITICAL", CRITICAL_HOTPATH),
        ("HIGH", HIGH_PRIORITY),
        ("MEDIUM", MEDIUM_PRIORITY),
        ("LOW", LOW_PRIORITY),
    ]:
        result[level] = sum(f.numpy_refs for f in files)
    return result


def print_summary():
    """Print diagnostic summary"""
    print("\n" + "=" * 80)
    print("NUMPY DIAGNOSTIC AUDIT SUMMARY")
    print("=" * 80)
    print(f"\nTotal Files: {SUMMARY_STATS['total_files']}")
    print(f"Total NumPy References: {SUMMARY_STATS['total_numpy_refs']}")
    print(f"Cython Files: {SUMMARY_STATS['cython_files']}")
    print(f"Python Files: {SUMMARY_STATS['python_files']}")
    print(f"Test Files: {SUMMARY_STATS['test_files']}")

    print("\n" + "-" * 80)
    print("Files by Priority:")
    print("-" * 80)
    for level, count in [
        ("CRITICAL", SUMMARY_STATS["critical_hotpath_files"]),
        ("HIGH", SUMMARY_STATS["high_priority_files"]),
        ("MEDIUM", SUMMARY_STATS["medium_priority_files"]),
        ("LOW", SUMMARY_STATS["low_priority_files"]),
    ]:
        refs = total_refs_by_priority().get(level, 0)
        print(f"  {level:10s}: {count:2d} files, {refs:3d} refs")

    print("\n" + "-" * 80)
    print("Top 10 NumPy Operations:")
    print("-" * 80)
    for i, (op, (count, desc)) in enumerate(
        sorted(OPERATION_FREQUENCY.items(), key=lambda x: x[1][0], reverse=True)[:10], 1
    ):
        print(f"  {i:2d}. {op:20s} ({count:3d} refs) - {desc}")

    print("\n" + "-" * 80)
    print("Phase Effort Estimate:")
    print("-" * 80)
    total_effort = 0
    for phase, info in PHASE_ROADMAP.items():
        hours = info["effort_hours"]
        total_effort += hours
        print(f"  {info['name']:35s} {hours:3d} hours")
    print(f"  {'TOTAL':35s} {total_effort:3d} hours")

    print("\n" + KEY_FINDINGS)
    print("=" * 80 + "\n")


if __name__ == "__main__":
    print_summary()
