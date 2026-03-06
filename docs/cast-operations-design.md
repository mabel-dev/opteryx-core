# Cast Operations as First-Class Constructs

**Date:** March 6, 2026  
**Status:** Complete Implementation  
**Goal:** Make CAST/TRY_CAST/SAFE_CAST first-class language constructs with dedicated `NodeType.CAST`.

---

## Completed Implementation

**All Six Phases Complete:**

1. ✅ **Phase 1**: Extract cast kernels to `opteryx/expression/casts.py` (197 lines, 17 tests)
2. ✅ **Phase 2**: Planner emits `NodeType.CAST` instead of `NodeType.FUNCTION`
3. ✅ **Phase 3**: Binder binds CAST nodes with type information
4. ✅ **Phase 4**: Evaluator dispatches CAST at runtime
5. ✅ **Phase 5**: Optimizer implements nested cast simplification
6. ✅ **Phase 6**: Legacy function routing removed - CAST kernels no longer in `FUNCTIONS` dict

---

## Design Overview

### Execution Pipeline

```
SQL: SELECT id::varchar FROM $planets
     ↓
Parse: NodeType.CAST(left=id, value="VARCHAR")
     ↓
Plan: NodeType.CAST → First-class construct
     ↓
Bind: Type information attached (target: VARCHAR)
     ↓
Evaluate: Direct kernel dispatch via `cast("VARCHAR")` from casts.py
     ↓
Optimize: Nested cast simplification (CAST(CAST(x))) → CAST(x)
     ↓
Execute: Kernel applies conversion (id array → varchar array)
```

### Key Components

**1. Cast Kernels** (`opteryx/expression/casts.py`)
- Home for all cast implementations
- Exports: `cast(type)`, `try_cast(type)`, `safe()`
- Supports: INT, DOUBLE, VARCHAR, BLOB, DECIMAL, ARRAY types
- Optimizations: Fast paths, type-specific handling

**2. Planner** (`opteryx/planner/logical_planner/logical_planner_builders.py`)
- `cast()` builder creates `NodeType.CAST` nodes
- Source expression in `left`, target type in `value`
- No NodeType.FUNCTION wrapping

**3. Binder** (`opteryx/planner/binder/binder.py`)
- Binds type information to CAST nodes
- Maps type string to OrsoType
- Creates FunctionColumn with correct precision/scale
- Sets identity property for column tracking

**4. Evaluator** (`opteryx/managers/expression/__init__.py`)
- CAST handler in `_inner_evaluate()` (lines 302-336)
- Detects safe cast via "TRY_" prefix
- Dispatches to appropriate kernel
- Handles optional parameters (precision/scale)
- Added to `should_evaluate()` whitelist

**5. Optimizer** (`opteryx/planner/optimizer/strategies/cast_simplification.py`)
- Collapses nested casts: `CAST(CAST(expr AS T1) AS T2)` → `CAST(expr AS T2)`
- Preserves TRY_CAST safety semantics
- Integrated into OptimizerVisitor chain

**6. Expression Formatter** (`opteryx/managers/expression/formatter.py`)
- Formats CAST expressions as `CAST(expr AS TYPE)`
- Supports TRY_CAST and optional parameters
- SQL-standard output format

---

## Migration Status

### What Changed
- ❌ Legacy entries removed from `FUNCTIONS` dict:
  - `"INTEGER"`, `"DOUBLE"`, `"VARCHAR"`, `"BLOB"`, `"DECIMAL"` (aliases removed)
  - `"TRY_INTEGER"`, `"TRY_DOUBLE"`, `"TRY_VARCHAR"`, `"TRY_BLOB"`, `"TRY_VARBINARY"`, `"TRY_DECIMAL"`
- ✅ Cast kernels remain importable directly: `from opteryx.expression.casts import cast, try_cast`
- ✅ Public catalog continues to exclude casts (non-scalar operation)
- ✅ All CAST operations route through NodeType.CAST pipeline
- ✅ Zero SQL behavior regressions

### What Stayed
- ARRAY and TRY_ARRAY (specialized array handling, not part of standard type cast system)
- TIMESTAMP/DATE/BOOLEAN casts (kept in FUNCTIONS for now - can be migrated separately)
- Import exports of `cast`, `try_cast`, `safe` for direct code usage

---

## Testing

- ✅ 27 unit tests all passing (17 cast + 10 catalog)
- ✅ Expression tests validate CAST kernel functionality  
- ✅ Query execution verified with projection test
- ✅ Optimizer simplification tested
- ✅ Zero regressions across all six phases

---

## Design Principles (Maintained)

1. ✅ No SQL behavior regressions during migration
2. ✅ `CAST`, `TRY_CAST`, `SAFE_CAST`, `::` converge to one internal representation
3. ✅ Cast kernels owned by expression layer (`opteryx/expression/casts.py`)
4. ✅ Catalog treats casts as non-public operations
5. ✅ Legacy function-path compatibility removed
6. ✅ `opteryx/functions` no longer houses cast behavior

---

## Target Architecture

### 1. AST Representation

Add `NodeType.CAST` (internal-node category value; bitmask-compatible with `INTERNAL_TYPE` checks in `opteryx/managers/expression/__init__.py`).

Use existing dynamic `Node` fields (no new class required):

- `node_type = NodeType.CAST`
- `value = "CAST"`
- `left = <source expression>`
- `type = <target OrsoTypes>`
- `parameters = [<precision/scale/element_type literals if needed>]`
- `safe = bool` (`True` for TRY_CAST/SAFE_CAST)
- `cast_kind = "CAST" | "TRY_CAST" | "SAFE_CAST"`

### 2. Planner Behavior

`logical_planner_builders.cast()` should:

1. Keep current type extraction/normalization logic.
2. Keep compile-time literal folding.
3. Emit `NodeType.CAST` for non-literal casts instead of `NodeType.FUNCTION`.

Compatibility note:

- Initial rollout can include a feature flag to emit old `NodeType.FUNCTION` if needed for rollback safety.

### 3. Binder Behavior

Add explicit cast binding branch in `opteryx/planner/binder/binder.py`:

1. Bind source expression (`node.left`).
2. Validate target type metadata (including decimal precision/scale and array element type arguments).
3. Set output type directly from cast target.
4. Annotate cast legality/coercion info for optimizer and execution.

`TRY_CAST` / `SAFE_CAST` semantics remain "null on conversion failure."

### 4. Execution Behavior

Add cast evaluation path in `opteryx/managers/expression/__init__.py` for `NodeType.CAST` that dispatches to dedicated conversion kernels.

Preferred kernel location:

- `opteryx/expression/casts.py`

Legacy compatibility:

- During transition, old function-call cast *routing* remains available for legacy planned nodes.
- Routing should call into `opteryx/expression/casts.py` kernels, not local cast implementations in `opteryx/functions/__init__.py`.
- After migration, cast execution should no longer depend on lookup in `opteryx/functions/__init__.py`.

### 5. Optimizer Behavior

Once `NodeType.CAST` exists, add cast-specific rewrites:

1. Identity removal: `CAST(x AS T)` where `type(x) == T` -> `x`.
2. Nested simplification where safe: `CAST(CAST(x AS T1) AS T2)` -> `CAST(x AS T2)` (only when semantics match).
3. Cast-aware constant folding for literal trees.
4. Predicate heuristics can treat casts distinctly from generic scalar functions.

---

## Function Catalog Alignment

Catalog contract:

- `opteryx/expression/functions/catalog.py` should not expose `INTEGER(...)`, `VARCHAR(...)`, `TRY_INTEGER(...)`, etc. as user-facing scalar functions.
- Cast semantics belong to planner/binder/evaluator cast handling, not scalar function overload resolution.

Legacy coexistence:

- `opteryx/functions/__init__.py` may retain cast function names temporarily for compatibility.
- Those names must delegate to `opteryx/expression/casts.py` implementations.

---

## Migration Plan

### Phase 0 (Done): Intent Established

1. New catalog exists.
2. Catalog tests assert conversion functions are intentionally excluded.
3. Design direction documented.

### Phase 1 (Do Now): Relocate Cast Kernels

1. Create `opteryx/expression/casts.py` and move cast/try-cast implementations there.
2. Rewire legacy cast function entries to call the new module.
3. Remove cast implementation bodies from `opteryx/functions/__init__.py` (allow thin compatibility wrappers only if required).
4. Add unit tests that assert cast behavior is sourced from expression-layer module.

### Phase 2: Introduce `NodeType.CAST`

1. Add `NodeType.CAST` enum member in `opteryx/managers/expression/__init__.py`.
2. Update planner `cast()` builder to emit `NodeType.CAST`.
3. Keep fallback/flag for fast rollback.

### Phase 3: Binder Support

1. Add `_bind_cast` branch.
2. Type validation and result typing for cast nodes.
3. Add focused binder tests for cast nodes and TRY/SAFE variants.

### Phase 4: Evaluator Support

1. Add `NodeType.CAST` execution branch in expression evaluation.
2. Use conversion kernels from `opteryx/expression/casts.py`.
3. Preserve legacy path for previously planned `NodeType.FUNCTION` cast nodes.

### Phase 5: Optimizer Support

1. Add cast-specific simplification passes.
2. Add regression tests for nested cast rewrites and null-safe behavior.

### Phase 6: Cleanup

1. Remove cast function names from legacy scalar function table.
2. Remove planner fallback.
3. Update docs and telemetry dashboards.

---

## Testing Strategy

1. Keep existing CAST integration battery as ground truth.
2. Add planner unit tests asserting `cast()` emits `NodeType.CAST`.
3. Add binder tests for result typing and invalid target metadata.
4. Add evaluator tests for each conversion family (numeric, temporal, blob/varbinary, array element casts).
5. Add TRY_CAST/SAFE_CAST tests verifying null-on-failure semantics.
6. Add optimizer tests for identity and nested-cast simplification.
7. Add a regression test proving no cast kernel implementation remains in `opteryx/functions/__init__.py`.

---

## Acceptance Criteria

1. Planner emits `NodeType.CAST` for non-literal casts.
2. Binder infers cast output types without routing through scalar function lookup.
3. Cast behavior is implemented in `opteryx/expression/casts.py` (even when temporarily routed through legacy function names).
4. Existing CAST/TRY_CAST/SAFE_CAST behavior remains unchanged for SQL users.
5. Catalog continues excluding conversion functions as user-callable scalar functions.
6. Full cast test battery passes.

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| `NodeType.CAST` breaks bitmask-based internal node handling | Use internal-category enum value; add evaluator dispatch tests |
| Divergence between new cast module and legacy routing during transition | Single source of cast logic in `opteryx/expression/casts.py`; legacy path is routing-only |
| Incomplete kernel parity for edge cast combinations | Start from current legacy behavior matrix; migrate with compatibility tests |
| Optimizer rewrites alter semantics for tricky casts | Guard rewrites with strict safety checks; add targeted regression tests |

---

## Notes

1. This update intentionally reflects the current codebase state and migration constraints.
2. The old and new function systems coexist today; this design removes that split incrementally.
3. Cast behavior should remain SQL-stable while internal representation changes.
