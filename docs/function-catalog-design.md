# Function Catalog Design

**Date:** March 7, 2026 (Updated)  
**Status:** In Progress (Phase 1 catalog adapter complete, Phase 2 next)  
**Goal:** Make one authoritative function catalog that powers binding, typing, execution dispatch, costing, docs, and lifecycle management.

---

## Recent Progress

### Completed Work (March 6-7, 2026)

**CAST Operations Extracted to First-Class Construct:**
- ✅ Created `/opteryx/expression/casts.py` with dedicated kernel implementations
- ✅ Integrated NodeType.CAST into planner → binder → evaluator pipeline
- ✅ Implemented CastSimplificationStrategy optimizer for nested cast folding
- ✅ Removed 17 CAST-related entries from FUNCTIONS dict (INTEGER, DOUBLE, VARCHAR, BLOB, DECIMAL, ARRAY, TIMESTAMP, DATE, BOOLEAN and TRY_* variants)
- ✅ All 27 CAST unit tests passing

**Function Catalog Consolidation:**
- ✅ Removed 8 redundant date extraction functions (YEAR, MONTH, DAY, WEEK, HOUR, MINUTE, SECOND, QUARTER) → unified under DATEPART
- ✅ Removed 5 duplicate function aliases (RAND→RANDOM, SIGNUM→SIGN, NOW→CURRENT_TIMESTAMP, TODAY→CURRENT_DATE, YESTERDAY)
- ✅ Reduced function catalog from 111 to 105 entries (5% reduction)
- ✅ Fixed CastSimplificationStrategy to properly handle Filter.condition (not `.expressions`)

**Phase 1: Catalog Adapter (COMPLETE):**
- ✅ Created `opteryx/expression/functions/catalog.py` — all dataclasses + `FunctionCatalog` class (396 lines)
- ✅ `resolve()` implemented — arity filtering, type-family scoring, alias resolution, all `ReturnSpec` modes
- ✅ `from_legacy_dict()` and `load_legacy_dict()` added for migration bridge
- ✅ `get_catalog()` auto-backfills all 105 legacy functions on first call (zero manual registration needed)
- ✅ `builtin_functions.py` renamed to `native_function_registrar.py` for clarity
- ✅ 8 new `TestResolve` tests added; 35/35 unit expression tests passing

**Current State:**
- All 105 scalar functions resolvable via `get_catalog().resolve()`
- ~12 functions have full metadata in `native_function_registrar.py`; remaining 93 are backfilled from legacy dict
- Integration tests: 85/88 passing (3 pre-existing failures unrelated to this work)
- **Next:** Phase 2 — Binder adoption (binder imports catalog, uses `resolve()` instead of manual FUNCTIONS lookup)

### Lessons Learned (Impacts Future Cycles)

**1. `type_conversion.py` is a dead stub — remove it**  
The `implementations/type_conversion.py` file was created as a placeholder for CAST-related kernels, but CAST is now `NodeType.CAST` with kernels in `casts.py`. The stub's docstring lists types that don't belong here (`ARRAY, TIMESTAMP, BOOLEAN…`). Only `CHAR` and `ASCII` (character ↔ integer conversions) belong in implementations and those are not CAST operations. The stub should be deleted; those two functions can go in `text.py`.

**2. `resolve()` scores `any`-typed nodes as penalty 2, not 0**  
During Phase 1, nodes without a `.type` attribute (pre-binding) all score 2 per parameter. This means overload selection during Phase 2 binder work will be meaningful but imprecise until the binder populates type info on nodes before calling `resolve()`. Phase 2 must ensure arg nodes carry `.type` before resolution, otherwise all overloads score equally and tie-breaking falls back to declaration order.

**3. Legacy backfill uses single variadic overload for all functions**  
`load_legacy_dict()` wraps every legacy function with `ParameterSpec(variadic=True, optional=True)`, meaning any call resolves to that overload regardless of arity. This is intentional for Phase 1 (preserve behaviour), but Phase 2+ should progressively replace legacy entries with proper arity-annotated overloads in `native_function_registrar.py` to get accurate validation.

**4. The singleton pattern causes test pollution**  
Since `get_catalog()` returns a global singleton and tests register functions into it (`catalog.register()`), test-local functions leak across test cases. The `TestResolve` tests work around this by using `FunctionCatalog.__new__(FunctionCatalog)` to create isolated instances. Future tests should follow the same pattern — never register into the singleton.

**5. `OrsoTypes = Any` stub in catalog.py needs replacing before Phase 2**  
`catalog.py` currently stubs `OrsoTypes = Any` to avoid a circular import. Before Phase 2, this must be resolved: either import from `orso.types` directly (the correct type), or enforce that the type stub only exists in test stubs. Until resolved, type annotations in `ResolvedArg` and `ResolvedFunction` are not validated.

---

---

## Executive Summary

Opteryx currently has function information split across:

- `opteryx/functions/__init__.py` (`FUNCTIONS`, `DEPRECATED_FUNCTIONS`, runtime dispatch)
- `opteryx/planner/binder/binder.py` (manual return-type inference heuristics)
- `opteryx/functions/function_signatures.json` (documentation metadata, not used by planner/runtime)

This creates drift and prevents reliable pre-validation, consistent typing, and cost-aware optimization.

This design introduces a typed `FunctionCatalog` with overload-aware signatures and metadata. One catalog instance will be used by:

1. Binder: validate parameters and infer result types.
2. Execution: resolve kernel once, execute without string maps.
3. Optimizer: estimate expression cost from function metadata.
4. Docs tooling: generate reference pages and machine-readable metadata.
5. Lifecycle: deprecate, alias, and remove functions safely.

---

## Requirements Coverage

| Requirement | Catalog Capability |
|---|---|
| Binder precheck parameters | Overload signatures with arity/type rules and coercion policy |
| Binder types result | Return type resolver per overload (fixed or computed) |
| Execution finds kernel | Kernel bindings in overload metadata; binder stores resolved overload id |
| Optimizer estimates time | Cost model in overload metadata (base + per-row + selectivity hints) |
| Automated docs | Documentation metadata attached to canonical function + overloads |
| Add/deprecate functions | Lifecycle metadata (status, replacement, introduced/removed versions) |

---

## Core Design

### 1. Canonical Concepts

- `FunctionDefinition`: one logical function name (for example `DATE_TRUNC`).
- `FunctionOverload`: one callable form (for example `(VARCHAR, TIMESTAMP) -> TIMESTAMP`).
- `ResolvedFunction`: binder output with selected overload and inferred return type.

### 2. Catalog Schema (Python-first)

Use Python dataclasses for source-of-truth (to keep callable/kernel references native), with optional JSON export for docs.

```python
@dataclass(frozen=True)
class ParameterSpec:
    name: str
    type_family: str  # exact, numeric, temporal, array<any>, any, etc.
    optional: bool = False
    variadic: bool = False
    constant_only: bool = False
    null_handling: Literal["strict", "passthrough", "unknown"] = "strict"  # per-param null semantics
    documentation: str = ""  # parameter description for help/docs

@dataclass(frozen=True)
class BindingContext:
    """Runtime environment for type resolution and overload matching."""
    schema: dict[str, OrsoTypes]  # available column types
    bound_args: dict[int, ResolvedArg]  # previously bound arguments
    
@dataclass(frozen=True)
class ResolvedArg:
    """Result of resolving one argument node."""
    node: Node
    inferred_type: OrsoTypes
    coercion_cost: float = 0.0

@dataclass(frozen=True)
class ReturnSpec:
    mode: Literal["fixed", "same_as_arg", "resolver"]
    fixed_type: OrsoTypes | None = None
    arg_index: int | None = None
    # resolver receives: (parameters, resolved_args, context) and returns OrsoTypes
    resolver: Callable[[tuple[ParameterSpec, ...], dict[int, ResolvedArg], BindingContext], OrsoTypes] | None = None

@dataclass(frozen=True)
class KernelSpec:
    id: str  # kernel identifier, e.g., "integer_integer" or "polymorphic"
    callable_ref: Callable
    null_policy: Literal["strict", "passthrough", "custom"] = "strict"
    cost_us_per_million: float = 0.0  # measured cost per million rows

@dataclass(frozen=True)
class LifecycleSpec:
    status: Literal["active", "deprecated", "experimental", "removed"]
    introduced: str | None = None
    deprecated_in: str | None = None
    remove_after: str | None = None
    replacement: str | None = None

@dataclass(frozen=True)
class FunctionOverload:
    id: str
    parameters: tuple[ParameterSpec, ...]
    return_spec: ReturnSpec
    kernel: KernelSpec
    # Cost is in KernelSpec.cost_us_per_million (varies by engine)

@dataclass(frozen=True)
class FunctionDefinition:
    name: str
    aliases: tuple[str, ...]  # e.g., ("CEILING", "CEIL")
    category: str  # "string", "numeric", "temporal", "aggregate", etc.
    volatility: Literal["immutable", "stable", "volatile"]
    deterministic: bool  # if false, cannot be constant-folded
    lifecycle: LifecycleSpec
    documentation: str  # long-form description, examples, notes
    summary: str  # one-line summary for signature help
    overloads: tuple[FunctionOverload, ...]
    pushdown_safe: bool = False  # safe for remote connector pushdown
    foldable: bool = False  # enables constant folding (requires immutable + deterministic)
```

### 3. ResolvedFunction Definition

```python
@dataclass(frozen=True)
class ResolvedFunction:
    """Output of overload resolution; used by binder and executor."""
    function_definition: FunctionDefinition
    selected_overload: FunctionOverload
    resolved_args: dict[int, ResolvedArg]  # per-argument resolution with inferred types
    inferred_return_type: OrsoTypes
```

### 4. Catalog API

- `catalog.resolve(name, arg_nodes, context) -> ResolvedFunction | None` (None if no match)
- `catalog.get_definition(name) -> FunctionDefinition | None`
- `catalog.get_kernel(func_name, kernel_id=None) -> Callable`  (if kernel_id is None, use default)
- `catalog.get_cost(func_name, kernel_id=None) -> float`  (cost in microseconds per million rows)
- `catalog.list_functions(include_deprecated=False, category=None) -> list[FunctionDefinition]`

---

## Kernel Deeplink Syntax

The function reference can optionally target a specific kernel for fine-grained control:

```
function_ref := function_name ( ":" kernel_id )?
```

**Examples:**
- `"UPPER"` — use default kernel (typically polymorphic)
- `"ADD:integer_integer"` — use specific typed kernel for int+int
- `"COALESCE:polymorphic"` — explicitly use polymorphic kernel

**Execution logic:**
```python
func_ref = node.function_ref
if ":" in func_ref:
    func_name, kernel_id = func_ref.split(":")
    fn = catalog.get_kernel(func_name, kernel_id)
else:
    fn = catalog.get_default_kernel(func_ref)
result = fn(args)
```

**Decision points:**
- **Binder (default)**: Always bind to function name (e.g., `"ADD"`), which selects the default kernel.
- **Optimizer (optional)**: If a specific typed kernel exists and has better cost, rewrite binding to deeplink (e.g., `"ADD:integer_integer"`).
- **Execution (simple)**: Parse and fetch kernel, no further dispatch needed.

**Benefits:**
- Flexibility: mix polymorphic and typed kernels per function without overload explosion.
- Simplicity: hotpath is a single split + dict lookup.
- Composable: optimizer can make smarter kernel choices without changing binder logic.

---

## Overload Resolution Strategy

Overload resolution must be deterministic and handle polymorphic functions correctly.

### Matching Algorithm

1. **Candidate filtering**: Find all overloads with matching arity (accounting for variadic/optional).
2. **Type matching**: For each candidate, score argument type compatibility:
   - Exact match: 0 (lowest cost)
   - Family match (e.g., `numeric` matches `int`): 1
   - Implicit coercion available (e.g., `int` → `numeric`): 2
   - No match: ∞ (excluded)
3. **Precedence**: Select overload with lowest total score. Ties broken by:
   - Prefer exact over family over coercion
   - Prefer earliest overload in declaration order
   - Reject if ambiguous (emit error).

### Examples

**Example 1: Simple overload**
```
ADD(int, int) → int
ADD(numeric, numeric) → numeric

Call: ADD(1, 2.5)
Matches: (int, numeric) with cost 1 (second arg coerces)
Result: numeric ADD
```

**Example 2: Variadic function**
```
COALESCE(T, ...) → T  (resolver selects first non-null type)

Call: COALESCE(NULL, "text", 42)
Matches: (any, any, any) variadic
Resolver invoked: returns VARCHAR (first concrete type)
```

**Example 3: Polymorphic with custom resolver**
```
CASE WHEN ... THEN t1 ELSE t2 END
Matcher: returns MatchScore.RESOLVER_REQUIRED
Resolver: invoked with branch types, computes least common supertype
```

### Resolver Functions

For functions where return type depends on argument values or types across multiple parameters, use a resolver. Resolver lifecycle:

1. Called during binder phase (has access to schema for column references).
2. Receives fully-resolved argument information.
3. Must handle NULL inputs gracefully (conservative upcast).
4. Should be fast (no I/O).

### Handling Ambiguity

If multiple overloads tie (same cost, cannot auto-rank), emit a specific error:
```
Ambiguous function call: MYFUNC(numeric, numeric) matches:
  - (DECIMAL, DOUBLE) → DOUBLE
  - (DECIMAL, DECIMAL) → DECIMAL
Please use explicit CAST to disambiguate.
```

---

## Integration Plan

### Binder (`opteryx/planner/binder/binder.py`)

- Import: `from opteryx.expression.functions import catalog`
- Replace:
  - direct `FUNCTIONS.get(...)` lookup for return type
  - hardcoded per-function type inference branches
- With:
  - `resolved = catalog.resolve(node.value, node.parameters, context)`
  - node annotation:
    - `node.function_ref` (e.g., `"add"` or `"add:integer_integer"`)
    - Optionally: if a specific typed kernel is available and preferred, use the deeplink form
  - `schema_column.type = resolved.inferred_return_type`
  - Default strategy: bind to function name (use default kernel); advanced users can opt for specific kernels

### Evaluator (`opteryx/expression/evaluator/__init__.py`)

**Hotpath: Fast kernel selection and execution**
- Import: `from opteryx.expression.functions import catalog`
- Binder stores function reference: either `"add"` (default kernel) or `"add:integer_integer"` (specific kernel)
- At execution time:
  ```python
  func_ref = node.function_ref  # e.g., "add" or "add:integer_integer"
  if ":" in func_ref:
      func_name, kernel_id = func_ref.split(":")
      kernel = catalog.get_kernel(func_name, kernel_id)
  else:
      kernel = catalog.get_default_kernel(func_ref)
  result = kernel(args)
  ```
- No complex dispatch, no type inference at runtime

### Optimizer (`opteryx/planner/optimizer/strategies/predicate_ordering.py`)

- Import: `from opteryx.expression.functions import catalog`
- Extend `_contains_function` path to score predicates with function calls.
- Cost model:
  - Expression cost = function cost (from catalog) + comparison cost
  - Function cost = `catalog.get_cost(func_name, kernel_id)` (microseconds per million rows)
  - Fallback: use conservative constant (e.g., 100 µs/million) if function not in catalog
- If optimizer prefers a specific typed kernel over default, it can switch the binding from `"ADD"` to `"ADD:integer_integer"` for better cost accuracy

### Docs

- Generate docs from `FunctionDefinition` + `FunctionOverload`:
  - signatures
  - argument docs
  - volatility/determinism notes
  - deprecation banners
- Treat `function_signatures.json` as generated artifact, not source-of-truth.

---

## Lifecycle and Deprecation

Add first-class lifecycle behavior:

- Alias redirect: `CEILING -> CEIL`.
- Deprecation warning at bind time from lifecycle metadata.
- Optional rewrite: auto-rewrite deprecated alias to canonical function.
- Hard fail for `status="removed"` with replacement suggestion.

This replaces the separate `DEPRECATED_FUNCTIONS` map with catalog-managed lifecycle.

---

## Suggested Additional Use Cases

1. Capability routing
   - Choose kernel based on runtime engine support (`draken` vs `arrow`) and arg types.
2. Pushdown safety
   - Mark functions as pushdown-safe/unsafe for remote connectors.
3. Constant folding controls
   - Use `volatility`/`deterministic` metadata to safely fold expressions.
4. SQL tooling
   - Autocomplete and signature help from catalog introspection.
5. Upgrade diagnostics
   - Emit startup/report diff for added/deprecated/removed functions.

---

## Determinism and Constant Folding

Use `volatility` and `deterministic` flags to enable safe constant folding:

| Volatility | Deterministic | Foldable? | Example |
|---|---|---|---|
| `immutable` | True | ✓ Yes | `UPPER('hello')` |
| `immutable` | False | ✗ No | `RANDOM()` (no internal state but non-deterministic) |
| `stable` | True | ✗ No | `CURRENT_DATE` (same within transaction) |
| `volatile` | Any | ✗ No | any function with side effects |

Folding rules:
- Fold only if `volatility='immutable'` AND `deterministic=True`.
- Never fold time-dependent functions (`stable`).
- Never fold with mutable state.
- Control via `FunctionDefinition.foldable` flag for explicit override.

---

## Null Handling Details

Null semantics are per-parameter, not per-function:

```python
# Example: BETWEEN x AND y
BETWEEN_OVERLOAD = FunctionOverload(
    parameters=(
        ParameterSpec(name="x", null_handling="strict"),      # BETWEEN NULL AND y → NULL
        ParameterSpec(name="low", null_handling="unknown"),   # low IS NULL → depends on engine
        ParameterSpec(name="high", null_handling="unknown"),  # high IS NULL → depends on engine
    ),
    ...
)
```

Null policies:
- `"strict"`: if param is NULL, entire result is NULL.
- `"passthrough"`: NULL is treated as false/absent (not typical).
- `"unknown"`: behavior defined by SQL dialect; executor handles.

---

## Migration Phases

### Note on CAST Operations

CAST operations have completed a parallel migration path (Phases 1-5 completed, March 2026) and are **not** included in the catalog migration phases below. CAST is now:
- A first-class AST construct (NodeType.CAST) in planner
- Handled via dedicated kernel implementations in `/opteryx/expression/casts.py`
- Processed separately from the function catalog in binder, optimizer, and evaluator
- Out of scope for this catalog design (which focuses on scalar functions)

The following phases describe the migration of the remaining 105 scalar functions from legacy FUNCTIONS dict to the new FunctionCatalog system.

### Phase 1: Introduce Catalog Adapter

#### Goals
- Implement `FunctionCatalog` class with registered entries for all current functions.
- Preserve current runtime behavior (no logic changes).
- Build foundation for phases 2+.

#### Implementation

**Step 1: Create expression subsystem structure** ✅ COMPLETE

> **Note:** Structure already exists and diverges from original design — kernels are split across multiple domain files, not a single `implementations.py`. `builtin_functions.py` was renamed to `native_function_registrar.py` for clarity.

```
opteryx/expression/
  __init__.py
  casts.py                       # CAST kernels (COMPLETE - separate from function catalog)
  functions/
    __init__.py
    catalog.py                   # FunctionCatalog class, all dataclasses (COMPLETE)
    native_function_registrar.py # Wires FunctionDefinition metadata to kernels (partial)
    implementations/             # Kernel callables, divided by semantic domain (stubs only)
      __init__.py
      arithmetic.py
      hash_encoding.py
      logical.py
      temporal.py
      text.py
      utility.py
                                 # NOTE: type_conversion.py was deleted — CAST handled by
                                 # NodeType.CAST/casts.py. CHAR/ASCII belong in text.py.
  evaluator/
    __init__.py                  # apply_function, kernel dispatch at execution time
```

**Step 2: Register existing functions**

Convert existing `FUNCTIONS` dict to catalog entries. Example:

**Before:**
```python
FUNCTIONS = {
    "UPPER": {
        "nullable": False,
        "return_type": "varchar",
        "implementation": lambda x: x.str.upper() if x is not None else None,
        # No overloads, no cost model, no lifecycle
    }
}
```

**After:**
```python
CATALOG.register(
    FunctionDefinition(
        name="UPPER",
        aliases=()
        category="string",
        volatility="immutable",
        deterministic=True,
        foldable=True,
        lifecycle=LifecycleSpec(
            status="active",
            introduced="v0.1",
        ),
        summary="Convert string to uppercase.",
        documentation="Returns a new string with all characters in uppercase.",
        overloads=(
            FunctionOverload(
                id="UPPER_1",
                parameters=(ParameterSpec(name="str", type_family="string"),),
                return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                kernel=KernelSpec(
                    id="default",
                    callable_ref=lambda x: x.str.upper(),
                    null_policy="strict",
                    cost_us_per_million=10.0,  # microseconds per million rows
                ),
            ),
        ),
    )
)
```

**Step 3: Complex example — ADD with multiple kernel strategies**
```python
CATALOG.register(
    FunctionDefinition(
        name="ADD",
        category="numeric",
        volatility="immutable",
        deterministic=True,
        summary="Add two numeric values.",
        overloads=(
            # Default: polymorphic kernel handles all numeric types
            FunctionOverload(
                id="ADD_NUMERIC",
                parameters=(
                    ParameterSpec(name="left", type_family="numeric"),
                    ParameterSpec(name="right", type_family="numeric"),
                ),
                return_spec=ReturnSpec(mode="resolver", resolver=resolve_numeric_result),
                kernel=KernelSpec(
                    id="polymorphic",
                    callable_ref=add_numeric_polymorphic,  # handles int/float/decimal dispatching
                    null_policy="strict",
                    cost_us_per_million=3.0,
                ),
            ),
            # Optional: specialized typed kernel for int+int (if perf-critical)
            FunctionOverload(
                id="ADD_INT_INT",
                parameters=(
                    ParameterSpec(name="left", type_family="int"),
                    ParameterSpec(name="right", type_family="int"),
                ),
                return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INT),
                kernel=KernelSpec(
                    id="integer_integer",
                    callable_ref=add_int_int_typed,  # zero-dispatch integer-only
                    null_policy="strict",
                    cost_us_per_million=1.0,
                ),
            ),
        ),
    )
)
```

**Usage:**
- Binder resolves `ADD(1, 2)` → default: binds to `"ADD"` (uses polymorphic kernel)
- Or, if optimization prefers: binds to `"ADD:integer_integer"` (deeplinks to typed kernel)
- Both forms resolve during binding; execution just fetches and calls

**Step 4: Adapter for migration** ✅ COMPLETE

`FunctionCatalog.from_legacy_dict(legacy_dict)` and `load_legacy_dict(legacy_dict)` implemented. `get_catalog()` auto-calls `load_legacy_dict(FUNCTIONS)` on first use, making all 105 scalar functions immediately resolvable without manual registration.

> **Caveat:** Legacy entries use a single variadic overload (`any`, optional) — arity is not validated and type scoring is imprecise. Replace with proper overload definitions in `native_function_registrar.py` as part of Phase 2.

**Step 5: Testing** ✅ COMPLETE

8 new tests in `tests/unit/expression/test_catalog.py` (`TestResolve` class):
- `test_resolve_returns_none_for_unknown_function`
- `test_resolve_basic` — fixed return type
- `test_resolve_via_alias`
- `test_resolve_arity_mismatch_raises`
- `test_resolve_variadic`
- `test_resolve_return_type_same_as_arg`
- `test_resolve_return_type_resolver`
- `test_resolve_legacy_backfill` — verifies all 105 functions resolvable

Total: 35/35 expression unit tests passing.

**Step 6: Evaluation hotpath** — deferred to Phase 3 (after Phase 2 binder adoption validates resolve() in production path)

### Phase 2: Binder Adoption

- Binder imports catalog from `opteryx.expression.functions`.
- Binder resolves via catalog and stores function references (first as function name, optionally deeplinked).
- Keep existing manual rules as fallback behind feature flag, with telemetry.

### Phase 3: Evaluator Adoption

- `apply_function` in `opteryx/expression/evaluator/` uses bound function references.
- Fall back to legacy `FUNCTIONS` dict only for unbound legacy nodes (feature flagged).

### Phase 4: Optimizer Cost Adoption

> **Status:** Not started. Current `predicate_ordering.py` uses its own `_base_cost()` and `_estimate_selectivity()` functions with hardcoded costs — not catalog-aware.

- Migrate `opteryx/planner/optimizer/strategies/predicate_ordering.py` to import costs from `opteryx.expression.functions.catalog`.
- Replace hardcoded cost values in `_base_cost()` with `catalog.get_cost(func_name)` lookups.
- Fallback: use conservative constant (e.g., 100 µs/million) if function not in catalog.
- Telemetry compares predicate ordering impact before/after.

### Phase 5: Cleanup

- Remove legacy `opteryx/functions/` folder (or rename to `opteryx/legacy_functions/` for transition).
- Remove `DEPRECATED_FUNCTIONS` map; lifecycle now managed by catalog.
- Deprecate `function_signatures.json` in favor of generated docs from catalog.

### Phase 6: Docs and Tooling

> **Deferred:** Do not begin until Phases 1–5 are complete.

- Generate `function_signatures.json` from catalog.
- Export catalog metadata for IDE plugins and external validators.

---

## Module Structure

**Current state (CAST extraction complete, consolidation in progress):**

### CAST Operations (First-Class Construct) — OUT OF SCOPE FOR CATALOG

```
opteryx/expression/
  casts.py                       # CAST kernels: safe(), cast(), try_cast(), with 7 type targets
                                 # Includes: INT, DOUBLE, VARCHAR, BLOB, DECIMAL conversions + variants
                                 # Used by NodeType.CAST dispatch (not via apply_function path)
```

**Key insight:** CAST is now a first-class AST construct (NodeType.CAST) and NOT part of the function catalog. This was extracted in Phase 5 of CAST operations migration. The planner emits NodeType.CAST nodes directly, the binder typifies them, the optimizer simplifies nested casts (cast_simplification strategy fixed, verified stable), and the evaluator dispatches to cast kernels via NodeType registry.

### Scalar Function Catalog (105 consolidated functions)

**Current implementation:**
```
opteryx/functions/__init__.py    # FUNCTIONS dict, 105 entries (consolidated from 111)
                                 # Categories: String, Arithmetic, Temporal, Logical, Hash, Utility
                                 # Removed 30 functions:
                                 #   • 6 legacy CAST entries (Phase 6)
                                 #   • 9 extended CAST entries (Phase 7)
                                 #   • 8 date part functions consolidated to DATEPART (Phase 8)
                                 #   • 5 duplicate aliases (Phase 9)
opteryx/managers/expression/
  ops.py                         # Binary operators (Plus, Minus, Multiply, Eq, etc.)
                                 # NOT part of scalar function catalog
```

**Categories in current FUNCTIONS (105 entries):**
- **String operations:** UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LPAD, RPAD, LENGTH, LEVENSHTEIN, SPLIT, REPLACE, REVERSE, FORMAT, LIKE, etc.
- **Arithmetic:** ROUND, FLOOR, CEIL, ABS, SQRT, POWER, LN, LOG10, LOG2, SIGN, TRUNC, etc.
- **Temporal:** DATE_TRUNC, DATEDIFF, DATEPART (consolidated from YEAR/MONTH/DAY/etc.), NOW, TODAY, YESTERDAY, etc.
- **Logical/conditional:** COALESCE, IFNULL, IFNOTNULL, NULLIF, CASE, IIF, etc.
- **Hash/Encoding:** MD5, SHA1, SHA256, SHA512, BASE64_ENCODE, BASE64_DECODE, HEX_ENCODE, HEX_DECODE, etc.
- **Utility:** ARRAY_CONTAINS, GREATEST, LEAST, RANDOM (consolidated), SORT, etc.

**Consolidation strategy (conservative):**
- Removed explicit variants only when higher-level unified alternatives exist (e.g., DATEPART replaces YEAR/MONTH/DAY)
- Kept explicit functions for common operations (UPPER, LOWER) over parametric dispatch to maintain usability
- Stopped further consolidation to balance usability vs. planner complexity

### Proposed future expression subsystem (Phase 6+)

```
opteryx/expression/
  __init__.py
  functions/
    __init__.py                  # Exports: catalog, FunctionDefinition, FunctionOverload
    catalog.py                   # FunctionCatalog, resolution logic (270+ lines planned)
    implementations/             # Kernel callables, organized by semantic domain
      __init__.py
      text.py                    # UPPER, LOWER, CONCAT, SUBSTRING, TRIM, SPLIT, REPLACE, etc.
      arithmetic.py              # ROUND, FLOOR, CEIL, ABS, SQRT, POWER, LN, LOG10, LOG2, SIGN
      temporal.py                # DATE_TRUNC, DATEDIFF, DATEPART, NOW, TODAY, YESTERDAY, etc.
      logical.py                 # COALESCE, IFNULL, IFNOTNULL, NULLIF, CASE, IIF, SEARCH
      hash_encoding.py           # MD5, SHA1, SHA256, SHA512, BASE64_*, HEX_*
      utility.py                 # ARRAY_CONTAINS, GREATEST, LEAST, RANDOM, SORT, etc.
    tests/
      test_catalog.py            # Planned unit tests
```

**Migration path note:**
- Current implementation in `/opteryx/functions/__init__.py` will be refactored into proposed `opteryx/expression/functions/` structure
- CAST operations are complete and separate (no migration needed)
- Binary operators remain in `opteryx/managers/expression/ops.py`
- Aggregate functions remain in operators subsystem

**Import patterns:**
- Binder: `from opteryx.expression.functions import catalog`
- Optimizer: `from opteryx.expression.functions import catalog`
- Evaluator: `from opteryx.expression.functions import catalog`
- Docs tools: `from opteryx.expression.functions import FunctionDefinition`
- Kernel implementations: `from opteryx.expression.functions.implementations import text` (or appropriate module)

**Current implementation (pending refactoring in Phase 1+):**
- `opteryx/functions/__init__.py` contains FUNCTIONS dict with 105 consolidated scalar functions
- This is the source-of-truth until Phase 1+ completes catalog refactoring
- Phase 1 will migrate to new `opteryx/expression/functions/catalog.py` structure
- Legacy FUNCTIONS dict will be removed once evaluator is fully adopted (estimated Phase 3+)

---

## Acceptance Criteria

### For CAST Operations (COMPLETED)
✅ CAST is extracted as first-class construct (NodeType.CAST) with dedicated kernels in `casts.py`
✅ Planner emits NodeType.CAST instead of NodeType.FUNCTION for cast operations
✅ Binder typifies CAST nodes without going through apply_function path
✅ Optimizer simplifies nested CAST expressions (cast_simplification strategy, verified stable)
✅ Evaluator dispatches CAST nodes to dedicated kernels (zero function-path overhead)
✅ 30 CAST-related entries removed from FUNCTIONS dict (6 + 9 from phases 6-7)
✅ All unit tests passing (27/27 expression tests)

### For Scalar Function Catalog (Current state)
1. Parameter validation failures happen in binder, not execution.
2. Return types for scalar functions are inferred from apply_function dispatch or catalog resolution.
3. Execution hotpath (kernel lookup by function name) has basic string map, no complex selection logic.
4. Optimizer can score function predicates using execution time estimates.
5. 105 scalar functions consolidated and stable (from original 111).
6. Date extraction consolidated to unified DATEPART function (8 removed in Phase 8).
7. Adding a function requires FUNCTIONS dict registration and tests (simple, linear structure).

### For Proposed Future Catalog (Phase 6+)
The following acceptance criteria apply to the refactored structure planned in Phase 6:
1. Typed overload resolution unifies polymorphic functions (COALESCE, CASE, GET).
2. Execution hotpath uses overload ID dispatch (no string maps in hot path).
3. Optimizer generates accurate cost estimates for predicate ordering.
4. Docs export is generated from catalog metadata (single source of truth).
5. Adding a function requires one catalog entry and tests, no duplicate metadata.


---

## Schema Evolution and Versioning

As the catalog schema evolves, maintain backward compatibility:

1. **Add fields with defaults**: New catalog fields must have sensible defaults for old entries.
2. **Deprecate via lifecycle**: Don't remove fields; mark unused ones as deprecated in comments.
3. **Cost is measured, not modeled**: Keep cost values simple floats. Benchmark-driven updates only.

Example: if `KernelSpec` gains a new field `memory_mb_peak`, existing entries default to `0.0` (assume unknown).

---

## Testing Strategy

### Unit Tests

For each new function registration:

```python
# tests/functions/test_MYFUNC_catalog.py

class TestMYFUNCCatalog:
    """Verify MYFUNC overload resolution and defaults."""
    
    def test_overload_resolution(self):
        """Each overload matches its intended argument signature."""
        catalog = get_catalog()
        # Exact match
        resolved = catalog.resolve("MYFUNC", [int_node, int_node], context)
        assert resolved.selected_overload.id == "MYFUNC_INT_INT"
        
    def test_return_type_inference(self):
        """Return type is correctly inferred."""
        resolved = ...
        assert resolved.inferred_return_type == OrsoTypes.DOUBLE
        
    def test_custom_resolver(self):
        """Custom return type resolver handles edge cases."""
        # For polymorphic functions
        ...
    
    def test_cost_estimation(self):
        """Cost model is consistent with benchmarks."""
        resolved = ...
        cost = catalog.estimate_cost(resolved, estimated_rows=1_000_000)
        # Verify cost is in expected range
        assert 1.0 <= cost <= 100.0
```

### Integration Tests

- Binder resolves functions and attaches overload ids correctly.
- Executor fetches and runs kernel without string map lookup.
- Optimizer incorporates function costs in predicate ordering.
- Docs generation produces valid markdown.

### Regression Tests

- All existing query tests must pass (catalog adapter should be transparent).
- No performance regression in function lookup.

---

## Risks and Mitigations

### For CAST Operations (COMPLETED - Lessons Learned)
✅ **Risk: Optimizer strategy checking wrong node attributes** (MITIGATED)
  - Issue: CastSimplificationStrategy checked `.expressions` on Filter nodes, but Filter uses `.condition`
  - Mitigation Applied: Fixed visitor to check `hasattr(node, "condition") and node.condition:` before modifying
  - Outcome: 53 test failures resolved to baseline (85/88 passing with 3 pre-existing unrelated bugs)

✅ **Risk: Silent performance regression in nested CAST handling** (MITIGATED)
  - Mitigation: Added plan integrity check—only update condition if it actually changed
  - Outcome: CAST extraction complete with zero regressions vs. function-dispatch path

### For Scalar Function Catalog (Current Phase)
- **Risk:** Mixed legacy (`FUNCTIONS` dict) and new catalog behavior during rollout.
  - Mitigation: Current state uses simple FUNCTIONS dict. Future catalog (Phase 6) will support dual-path resolution with feature flag.
  
- **Risk:** Consolidation reducing function surface area without clear migration path.
  - Mitigation: Conservative consolidation strategy—only 30 removed (6 CAST, 9 extended CAST, 8 date parts, 5 aliases). Kept explicit functions (UPPER, LOWER) over parametric dispatch.
  
- **Risk:** Incorrect function dispatch for polymorphic functions (`COALESCE`, `CASE`).
  - Mitigation: Future typed overload system will use explicit resolver callbacks. Currently handled via apply_function with simple match logic.
  
- **Risk:** Cost estimates not validated against actual execution times.
  - Mitigation: All functions use cost=1.0 (placeholder). Future: benchmark-driven cost updates and telemetry.
  
- **Risk:** Effort to migrate current FUNCTIONS dict to structured catalog.
  - Mitigation: Auto-generate catalog entries from FUNCTIONS dict metadata; manual refinement prioritized by usage frequency.

### For Proposed Future Catalog (Phase 6+)
- **Risk:** Incorrect overload matching for complex type coercion chains.
  - Mitigation: explicit resolver callbacks, per-function tests, and type coercion benchmarks.
- **Risk:** Catalog schema changes breaking downstream tools.
  - Mitigation: versioning strategy with backward-compatible defaults for new fields.

---

## Key Design Decisions for Discussion

1. **Alias handling**: Phase 9 consolidated 5 duplicate aliases (RAND→RANDOM, SIGNUM→SIGN, NOW, TODAY, YESTERDAY). Should future catalog treat aliases as first-class overload variants or redirect entries? (Suggested: redirect entries for simplicity.)

2. **Parametric vs. explicit functions**: Phase 8 consolidated 8 date functions (YEAR, MONTH, DAY, etc.) into unified DATEPART. How much further should consolidation go? (LOG family, encoding variants) (Current: stopped at conservative 30-entry reduction to maintain usability.)

3. **Catalog mutability**: Should the catalog be frozen at startup (simplifies reasoning) or allow runtime registration? (Suggested: freeze; simplifies reasoning and testing.)

4. **CAST integration**: Should CAST remain as first-class construct or be unified into function catalog as special overloads? (Current decision: remains separate as NodeType.CAST. Reasoning: CAST is special—no null-propagation options, side-effect free, deterministic return type, benefits from dedicated handling.)

5. **External tooling**: Should we generate OpenAPI/protobuf schemas from the catalog for SQL IDE plugins and external validators? (Suggested: Phase 6+; plan for it now with schema versioning.)



