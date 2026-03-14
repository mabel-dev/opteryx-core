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
- ✅ Reduced function catalog from 111 to 105 entries (5% reduction), then to 92 (further cleanup)
- ✅ Fixed CastSimplificationStrategy to properly handle Filter.condition (not `.expressions`)

**Phase 1: Catalog Adapter (COMPLETE):**
- ✅ Created `opteryx/expression/functions/catalog.py` — all dataclasses + `FunctionCatalog` class (396 lines)
- ✅ `resolve()` implemented — arity filtering, type-family scoring, alias resolution, all `ReturnSpec` modes
- ✅ `from_legacy_dict()` and `load_legacy_dict()` added for migration bridge
- ✅ `get_catalog()` auto-backfills all legacy functions on first call (zero manual registration needed)
- ✅ `builtin_functions.py` renamed to `native_function_registrar.py` for clarity
- ✅ 8 new `TestResolve` tests added; 35/35 unit expression tests passing

**Current State:**
- All 92 scalar functions resolvable via `get_catalog().resolve()`
- `GET` function removed from `FUNCTIONS` dict; subscript access (`arr[0]`, `struct->'key'`) is now handled as `NodeType.BINARY_OPERATOR` (`MapAccess`/`Arrow`) in the planner — not a catalog function
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

**6. `GET` is not a catalog function — subscript access is a binary operator**  
`GET(arr, 0)` and `arr[0]` / `struct->'key'` are handled by the planner as `NodeType.BINARY_OPERATOR` (`MapAccess` and `Arrow` operators), not as function catalog entries. `GET` is in `DEPRECATED_FUNCTIONS` with `None` as replacement (removed in 0.28.0). Do not add a GET entry to `native_function_registrar.py`; the subscript path bypasses the function catalog entirely.

**7. `null_policy` belongs on `KernelSpec`, not on `FunctionDefinition`**  
Null handling is a property of the kernel implementation, not of the logical function. A future overload of CONCAT might handle nulls differently than the current one. Annotate `null_policy` on each `KernelSpec` individually. The evaluator reads `node.function_ref.selected_overload.kernel.null_policy` — this is the correct access path.

**8. The evaluator fallback path must stay until `managers/expression` is removed**  
`apply_bounded_function` falls back to legacy `apply_function` when `node.function_ref` is `None`. This covers any FUNCTION node that was not bound (e.g., nodes produced by legacy code paths or tests that skip the binder). The fallback can only be removed once `managers/expression/__init__.py` is deleted and the new evaluator is the sole execution path.

**9. Phase 4 (optimizer cost) can use `catalog.get_cost(func_name)` directly**  
`FunctionCatalog.get_cost()` already exists and returns `cost_us_per_million` from the selected kernel. `predicate_ordering.py` currently has hardcoded costs in `_base_cost()` — replace with `get_cost()` lookups, falling back to a conservative default (e.g. `100.0`) for functions not yet in the catalog or legacy backfill entries (which have cost `0.0` by default, so treat `0.0` as "unknown" not "free").

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

The following phases describe the migration of the remaining 92 scalar functions from legacy FUNCTIONS dict to the new FunctionCatalog system.

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

`FunctionCatalog.from_legacy_dict(legacy_dict)` and `load_legacy_dict(legacy_dict)` implemented. `get_catalog()` auto-calls `load_legacy_dict(FUNCTIONS)` on first use, making all 92 scalar functions immediately resolvable without manual registration.

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
- `test_resolve_legacy_backfill` — verifies all 92 functions resolvable

Total: 44/44 expression unit tests passing.

**Step 6: Evaluation hotpath** — completed in Phase 3.

### Phase 2: Binder Adoption ✅ COMPLETE

- Binder imports catalog via `_get_function_catalog()` singleton.
- 80-line per-function `if/elif` type-inference chain replaced with 3-line `catalog.resolve()` delegation.
- `node.function_ref` (a `ResolvedFunction`) stored on every bound FUNCTION node.
- Literal coercion for CASE/COALESCE/IFNULL preserved in binder (AST mutation, not type inference).
- Integration test baseline: 71 failing (pre-existing, identical before and after).

### Phase 3: Evaluator Adoption ✅ COMPLETE

- `apply_bounded_function(node, *parameters)` implemented in `opteryx/expression/evaluator/__init__.py`.
- Dispatches via `node.function_ref.selected_overload.kernel.callable_ref` — zero name lookup.
- Null policy driven by `kernel.null_policy` (declared in `KernelSpec`):
  - `"strict"` (default): strip nulls before kernel call, backfill after. Fast path for pure functions.
  - `"passthrough"`: all rows including nulls forwarded to kernel. Required for COALESCE, CASE, IIF, IFNULL, IFNOTNULL, CONCAT, SUBSTRING.
- `null_policy="passthrough"` annotated on 7 kernel specs in `native_function_registrar.py`.
- Falls back to legacy `apply_function` for nodes where `function_ref` is not set (pre-bound or legacy paths).

### Phase 4: Optimizer Cost Adoption ✅ COMPLETE

- `_catalog_function_cost(node)` added to `predicate_ordering.py` — walks the expression subtree, sums `catalog.get_cost()` for every FUNCTION node, falls back to 100.0 µs/million for cost=0.0 entries (legacy backfill, treated as unknown/expensive per Lesson 9).
- `_order_complex_predicates(predicates, telemetry)` added — orders function-containing predicates by ascending catalog cost using the same greedy approach as simple predicates.
- `order_predicates()` now returns `ordered_simple + ordered_complex` instead of `ordered_simple + complex_preds` (unordered). Both buckets are now cost-ordered.
- `_base_cost()` (type-based comparison costs) left unchanged — it answers a different question (comparison operator cost by data type, not function execution cost).
- Telemetry counter `optimization_cost_based_predicate_ordering` incremented when complex predicates are reordered.

### Phase 5a: Kernel Migration (prerequisite for expression engine rewrite)

> **Status:** Next up. Prerequisite for the expression engine rewrite — the new evaluator must be able to import kernels from `opteryx/expression/functions/implementations/` directly.

The 4 kernel files in `opteryx/functions/` are pure computation with no dispatch logic. Moving them is mechanical:

- `opteryx/functions/string_functions.py` → `opteryx/expression/functions/implementations/string.py`
- `opteryx/functions/number_functions.py` → `opteryx/expression/functions/implementations/numeric.py`  
- `opteryx/functions/date_functions.py` → `opteryx/expression/functions/implementations/temporal.py`
- `opteryx/functions/other_functions.py` → `opteryx/expression/functions/implementations/other.py`

After moving:
- Update import paths in `native_function_registrar.py` to use the new locations.
- `opteryx/functions/__init__.py` becomes a thin shim re-importing from the new locations, so `managers/expression` (which calls `apply_function` via the `FUNCTIONS` dict) continues working unchanged.
- `function_signatures.json` stays in place (Phase 6 concern).

### Expression Engine Rewrite (sequenced after Phase 5a)

With kernels in `opteryx/expression/functions/implementations/`, the new evaluator can import directly from there. `managers/expression/__init__.py` and `apply_function` remain alive but are no longer called by the new engine. This is the correct point to do the expression engine rewrite — clean import targets, no circular deps, no need to touch `opteryx/functions/` during the rewrite.

### Phase 5b: Dispatch Machinery Cleanup (after expression engine rewrite is live)

Once the expression engine is the live execution path and `managers/expression/__init__.py` has no callers:

- Delete `managers/expression/__init__.py` — `apply_function` has no live callers.
- Remove `FUNCTIONS` dict from `opteryx/functions/__init__.py` — backfill in `catalog.py` no longer needed.
- Remove `DEPRECATED_FUNCTIONS` — lifecycle is now catalog-managed (`LifecycleSpec`).
- Remove `fixed_value_function` — move zero-arg function handling into binder internals or catalog.
- Delete the now-empty `opteryx/functions/__init__.py` shim.
- Deprecate `function_signatures.json` in favour of generated docs from catalog (Phase 6).

### Phase 6: Docs and Tooling

> **Deferred:** Do not begin until Phases 1–5b are complete.

- Generate `function_signatures.json` from catalog metadata.
- Export catalog metadata for IDE plugins and external validators.

#### Phase 6 Backlog

Immediate follow-on work once Phase 6 starts:

- Re-add `SHOW FUNCTIONS`, backed by the catalog rather than a hand-maintained list.
- Add `DESCRIBE FUNCTION <name>` / equivalent introspection surface using the same export path.
- Extend the exported JSON beyond today's signature-help fields to include lifecycle, volatility,
  determinism, null policy, foldability, pushdown safety, kernel ids, and cost estimates.
- Use catalog metadata to scaffold tests:
  - alias resolution tests
  - arity validation tests
  - return-type inference tests
  - export consistency tests
- Add a benchmark-driven script to populate and refresh `cost_us_per_million` values rather than
  editing them by hand.

#### Broader Documentation Artifact Opportunities

The same "generate reference artifacts from code" approach should not stop at scalar functions.
Useful next targets:

- **Data types**
  - Generate a machine-readable type catalog from `OrsoTypes` plus local normalization/conversion
    rules.
  - Export canonical names, aliases, families, temporal/numeric flags, array element support,
    literal syntax notes, and connector-facing type mappings.
- **Aggregates**
  - Generate an aggregate catalog from aggregate registrations and planner/runtime support tables.
  - Export aggregate names, aliases, DISTINCT support, wildcard support, null-handling semantics,
    group-by-only restrictions, partial/final aggregation support, and fallback/Draken support.
- **Operators**
  - Generate an operator catalog from operator maps and expression kernels.
  - Export operator symbol/name, operand type matrix, result types, precedence/associativity,
    pushdown safety, and special syntax notes (`->`, `->>`, `ANY`, `IN`, subscript access).
- **Type casting**
  - Generate cast reference data from `NodeType.CAST`, `casts.py`, and target type definitions.
  - Export source→target compatibility, `CAST` vs `TRY_CAST` behavior, optional precision/scale/length
    arguments, null/error semantics, and examples of lossy vs lossless conversions.

These do not all need one shared schema, but they should follow the same design principle:
runtime metadata is the source of truth; exported documentation/reference artifacts are generated.

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

### Scalar Function Catalog (92 consolidated functions)

**Current implementation (Phases 1–4 complete):**
```
opteryx/expression/
  evaluator/__init__.py           # apply_bounded_function — dispatches via node.function_ref.kernel
                                  # Falls back to legacy apply_function if function_ref not set
  functions/
    __init__.py                   # Exports: get_catalog()
    catalog.py                    # FunctionCatalog, FunctionDefinition, FunctionOverload,
                                  # KernelSpec, ReturnSpec, ResolvedFunction, LifecycleSpec
    native_function_registrar.py  # All managed FunctionDefinitions with resolver callbacks
    implementations/              # STUBS — kernel callables not yet moved here (Phase 5a)
      __init__.py
      string.py                   # stub
      numeric.py                  # stub
      temporal.py                 # stub
      other.py                    # stub
      utility.py                  # stub

opteryx/functions/               # LEGACY — to be migrated in Phase 5a/5b
  __init__.py                    # FUNCTIONS dict (92 entries), apply_function, DEPRECATED_FUNCTIONS
                                 # Will become a shim after Phase 5a, deleted after Phase 5b
  string_functions.py            # → implementations/string.py (Phase 5a)
  number_functions.py            # → implementations/numeric.py (Phase 5a)
  date_functions.py              # → implementations/temporal.py (Phase 5a)
  other_functions.py             # → implementations/other.py (Phase 5a)
  function_signatures.json       # Legacy docs — replaced by catalog export in Phase 6

opteryx/managers/expression/
  __init__.py                    # Current live evaluator — calls apply_function
                                 # TO BE DELETED after expression engine rewrite
  ops.py                        # Binary operators — NOT part of scalar function catalog
```

**Categories in current FUNCTIONS (92 entries):**
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

**Target structure after Phase 5a (kernel migration):**
```
opteryx/expression/
  evaluator/__init__.py           # apply_bounded_function (live), apply_function fallback
  functions/
    __init__.py
    catalog.py
    native_function_registrar.py
    implementations/
      __init__.py
      string.py                   # kernels from string_functions.py
      numeric.py                  # kernels from number_functions.py
      temporal.py                 # kernels from date_functions.py
      other.py                    # kernels from other_functions.py
      utility.py

opteryx/functions/               # shim — re-imports from opteryx/expression/functions/implementations/
  __init__.py                    # FUNCTIONS dict still present for managers/expression compat
  function_signatures.json       # until Phase 6
```

**Import patterns (current and target):**
- Binder: `from opteryx.expression.functions import get_catalog as _get_function_catalog` ✅
- Optimizer: `from opteryx.expression.functions import get_catalog` ✅
- Evaluator: `from opteryx.expression.evaluator import apply_bounded_function` ✅ (new engine)
- Kernel implementations: `from opteryx.expression.functions.implementations import string` (after Phase 5a)
- Docs tools: `from opteryx.expression.functions.catalog import FunctionDefinition`

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

### For Scalar Function Catalog (Phases 1–4 — COMPLETE)
✅ Parameter validation failures happen in binder (catalog.resolve() raises TypeError on arity mismatch).
✅ Return types for scalar functions inferred via catalog.resolve() — per-function resolver callbacks.
✅ Execution hotpath (apply_bounded_function) dispatches via node.function_ref.kernel.callable_ref — no string map.
✅ Optimizer scores function predicates using catalog.get_cost() — complex predicates now cost-ordered.
✅ 92 scalar functions consolidated and stable (from original 111 → 105 → 92).
✅ Date extraction consolidated to unified DATEPART function (8 removed in Phase 8).
✅ Adding a function requires one FunctionDefinition entry in native_function_registrar.py.

### For Phase 5a: Kernel Migration
1. All 4 kernel files moved to `opteryx/expression/functions/implementations/`.
2. `native_function_registrar.py` imports from new locations only.
3. `opteryx/functions/__init__.py` is a pure shim — no kernel logic, only re-imports.
4. All 44 expression unit tests still passing.
5. Integration test baseline unchanged.

### For Phase 5b: Dispatch Machinery Cleanup (post expression engine rewrite)
1. `managers/expression/__init__.py` deleted.
2. `apply_function` has no callers — removed from `opteryx/functions/__init__.py`.
3. `FUNCTIONS` dict removed — catalog backfill no longer needed.
4. `DEPRECATED_FUNCTIONS` removed — lifecycle managed by `LifecycleSpec` in catalog.
5. `opteryx/functions/__init__.py` shim deleted entirely.


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


