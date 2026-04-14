# NumPy/PyArrow Eradication Tracking Matrix

## Quick Reference - Sort by Priority

| File | Imports | Path | Severity | Status | Next Steps | Owner |
|------|---------|------|----------|--------|-----------|-------|
| **PHASE 1: CRITICAL** | | | | | | |
| opteryx/expression/operations/__init__.py | numpy, pyarrow | HOT | 🔴 CRITICAL | ⏳ TODO | Replace numpy.logical_or/place with Draken | - |
| opteryx/expression/operations/comparisons.py | pyarrow | HOT | 🔴 CRITICAL | ⏳ TODO | Migrate to Draken comparison kernels | - |
| opteryx/expression/__init__.py | numpy, pyarrow, compute | HOT | 🔴 CRITICAL | ⏳ TODO | Replace LOGICAL_OPERATIONS dispatch, remove .to_numpy() | - |
| opteryx/expression/unary_operations.py | numpy, pyarrow | HOT | 🟠 HIGH | ⏳ TODO | IS NULL to Draken, NOT operations | - |
| **PHASE 2: HIGH** | | | | | | |
| opteryx/expression/operations/string_matching.py | pyarrow | HOT | 🟠 HIGH | ⏳ TODO | LIKE/RLIKE to Draken regex kernels | - |
| opteryx/expression/operations/list_ops.py | pyarrow | HOT | 🟠 HIGH | ⏳ TODO | IN/NOT IN to vector_in_list | - |
| opteryx/expression/binary_operators.py | numpy, pyarrow | HOT | 🟠 HIGH | ⏳ TODO | JSON path consolidation, binary ops to Draken | - |
| opteryx/expression/evaluator/type_coercion.py | numpy, pyarrow | HOT | 🟠 HIGH | ⏳ TODO | Type coercion to Cython, remove PyArrow casts | - |
| opteryx/expression/evaluator/arithmetic.py | pyarrow | HOT | 🟠 HIGH | ⏳ TODO | Arithmetic dispatch to Draken kernels | - |
| opteryx/expression/operations/fastpath_constant.py | pyarrow | HOT | 🟠 HIGH | ⏳ TODO | Wrap PyArrow interactions, move to Cython | - |
| opteryx/expression/operations/fastpath_dictionary.py | pyarrow | HOT | 🟠 HIGH | ⏳ TODO | Wrap PyArrow interactions, move to Cython | - |
| **PHASE 3: MEDIUM** | | | | | | |
| opteryx/expression/evaluator/arithmetic_dispatch.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Arithmetic dispatch consolidation | - |
| opteryx/expression/evaluator/function_execution.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Function parameter coercion to Draken | - |
| opteryx/expression/evaluator/comparisons.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Temporal comparison coercion | - |
| opteryx/expression/evaluator/temporal_ops.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Temporal type casting | - |
| opteryx/expression/operations/type_coercion.py | numpy, pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Temporal array coercion | - |
| opteryx/expression/operations/array_ops.py | numpy | WARM | 🟡 MEDIUM | ⏳ TODO | Array operation wrappers | - |
| opteryx/expression/operations/special_ops.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | JSON path operations | - |
| opteryx/expression/intervals.py | pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Interval type operations | - |
| **PHASE 4: WARM PATHS (Lower priority)** | | | | | | |
| opteryx/expression/functions/implementations/arithmetic.py | numpy, pyarrow, compute | WARM | 🟡 MEDIUM | ⏳ TODO | Function kernel implementations | - |
| opteryx/expression/functions/implementations/logical.py | numpy, pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Logical function kernels | - |
| opteryx/expression/functions/implementations/temporal.py | numpy, pyarrow, compute | WARM | 🟡 MEDIUM | ⏳ TODO | Temporal function kernels | - |
| opteryx/expression/functions/implementations/text.py | numpy, pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Text function kernels | - |
| opteryx/expression/functions/implementations/utility.py | numpy, pyarrow | WARM | 🟡 MEDIUM | ⏳ TODO | Utility function kernels | - |
| opteryx/expression/functions/registrar/arithmetic.py | pyarrow, compute | WARM | 🟡 MEDIUM | ⏳ TODO | Function registration metadata | - |
| opteryx/expression/functions/registrar/constant.py | numpy | WARM | 🟡 MEDIUM | ⏳ TODO | Constant function registration | - |
| opteryx/expression/functions/registrar/logical.py | numpy | WARM | 🟡 MEDIUM | ⏳ TODO | Logical function registration | - |
| opteryx/expression/functions/registrar/utility.py | numpy | WARM | 🟡 MEDIUM | ⏳ TODO | Utility function registration | - |
| **PHASE 5: COLD PATHS (Acceptable)** | | | | | | |
| opteryx/models/dataframe.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Minimal impact - keep as needed | - |
| opteryx/models/execution_context.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Schema layer only | - |
| opteryx/types/schema.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Schema conversion utility | - |
| opteryx/operators/base_plan_node.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Telemetry and type checking | - |
| opteryx/managers/execution/serial_engine.py | pyarrow | WARM | 🟡 MEDIUM | ✅ ACCEPTABLE | Query result boundary - keep PyArrow | - |
| opteryx/types/_null_handling.py | numpy, pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Defensive imports for external types | - |
| opteryx/types/_scalar_to_vector.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Conversion boundary utility | - |
| opteryx/connectors/catalogs/local_catalog.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Parquet metadata reading | - |
| opteryx/utils/arrow.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Arrow helper functions | - |
| opteryx/utils/arrow_interop.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Conversion utilities | - |
| opteryx/utils/dates.py | numpy, pyarrow, compute | COLD | 🟢 LOW | ✅ ACCEPTABLE | Date utility functions | - |
| opteryx/utils/parquet_decoder.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Parquet metadata | - |
| opteryx/utils/sql.py | numpy, pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | SQL parsing utilities | - |
| opteryx/utils/firestore_utils.py | numpy | COLD | 🟢 LOW | ✅ ACCEPTABLE | Firestore serialization | - |
| opteryx/vectors/embeddings.py | numpy | COLD | 🟢 LOW | ✅ ACCEPTABLE | ML feature embeddings | - |
| opteryx/vectors/vector_types.py | numpy | COLD | 🟢 LOW | ✅ ACCEPTABLE | Vector type utilities | - |
| opteryx/planner/__init__.py | numpy, pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Logical planning phase | - |
| opteryx/planner/ast_rewriter.py | numpy | COLD | 🟢 LOW | ✅ ACCEPTABLE | AST rewriting | - |
| opteryx/planner/logical_planner/logical_planner_builders.py | numpy | COLD | 🟢 LOW | ✅ ACCEPTABLE | Logical plan building | - |
| opteryx/planner/optimizer/strategies/statistics_only_response.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Query optimization | - |
| opteryx/query_session.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Query session initialization | - |
| opteryx/compiled/draken/vectors/arithmetic_kernels.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | Kernel initialization | - |
| opteryx/__main__.py | pyarrow | COLD | 🟢 LOW | ✅ ACCEPTABLE | CLI output formatting | - |

---

## Progress Tracking

```
COMPLETED (0%):
████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0/56

PHASE 1 (0%):  ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0/3
PHASE 2 (0%):  ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0/8
PHASE 3 (0%):  ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0/9
PHASE 4 (0%):  ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0/9
PHASE 5 (100%):████████████████████████████████████████████████████ 27/27
```

---

## Legend

| Symbol | Meaning |
|--------|---------|
| 🔴 CRITICAL | Must eliminate - in query execution hot path |
| 🟠 HIGH | Should eliminate - in hot path operations |
| 🟡 MEDIUM | Should review - in warm path or frequent calls |
| 🟢 LOW | Acceptable - cold path (planning, initialization) |
| ⏳ TODO | Not started |
| ✅ ACCEPTABLE | No action needed |
| 🔧 IN PROGRESS | Work in progress |
| ✓ DONE | Completed |

---

## Phase Descriptions

- **PHASE 1 (CRITICAL):** Core filter operations and logical operations - highest impact, must be done first
- **PHASE 2 (HIGH):** String matching, list ops, binary operators, type coercion - major hot paths
- **PHASE 3 (MEDIUM):** Function execution, arithmetic dispatch, temporal ops - secondary hot paths
- **PHASE 4 (WARM):** Function implementations and registrars - boundary code, moderate priority
- **PHASE 5 (COLD):** Planning, schema, connectors - initialization and utilities, no action needed

---

## Dependency Graph (for sequencing)

```
PHASE 1: Core
├─ operations/__init__.py (filter_operations)
├─ operations/comparisons.py (equal, lt, gt, etc.)
└─ __init__.py (LOGICAL_OPERATIONS)
    │
    ├─> PHASE 2: String/List/Binary
    │   ├─ operations/string_matching.py (like, rlike)
    │   ├─ operations/list_ops.py (in_list)
    │   ├─ binary_operators.py (arrow_op, etc.)
    │   └─ unary_operations.py (is_null, not)
    │       │
    │       ├─> PHASE 3: Type Coercion & Arithmetic
    │       │   ├─ evaluator/type_coercion.py
    │       │   ├─ evaluator/arithmetic.py
    │       │   ├─ operations/type_coercion.py
    │       │   └─ evaluator/arithmetic_dispatch.py
    │       │
    │       └─> PHASE 4: Function Implementations
    │           └─ functions/implementations/*
    │
    └─> PHASE 5: Cold Paths (no dependencies)
        └─ (planners, connectors, schema, utils)
```
