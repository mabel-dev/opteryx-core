# Function Catalog Design

**Date:** March 6, 2026  
**Status:** Proposed  
**Goal:** Make one authoritative function catalog that powers binding, typing, execution dispatch, costing, docs, and lifecycle management.

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

### Phase 1: Introduce Catalog Adapter

#### Goals
- Implement `FunctionCatalog` class with registered entries for all current functions.
- Preserve current runtime behavior (no logic changes).
- Build foundation for phases 2+.

#### Implementation

**Step 1: Create expression subsystem structure**
```
opteryx/expression/
  __init__.py
  functions/
    __init__.py
    catalog.py         # FunctionCatalog class, dataclasses (ParameterSpec, KernelSpec, etc.)
    implementations.py # Actual kernel callables (add_numeric_polymorphic, add_int_int_typed, etc.)
  evaluator/
    __init__.py        # apply_function, kernel dispatch at execution time
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

**Step 4: Adapter for migration**

Provide `FunctionCatalog.from_legacy_dict(legacy_dict)` to auto-generate basic catalog entries from existing `FUNCTIONS` dict. This minimizes manual work but may produce conservative defaults. Manual review and refinement required for complex functions.

**Step 5: Testing**

Add catalog tests:
```
opteryx/expression/functions/tests/test_catalog.py:
- test_resolve_exact_match
- test_resolve_family_match
- test_resolve_ambiguous_error
- test_custom_resolver (for CASE, COALESCE)
- test_variadic_matching
- test_cost_estimation
```

**Step 6: Evaluation hotpath**

Update `opteryx/expression/evaluator/__init__.py` to use bound function references (instead of legacy FUNCTIONS dict):
```python
from opteryx.expression.functions import catalog

def apply_function(node, args):
    func_ref = node.function_ref
    if ":" in func_ref:
        func_name, kernel_id = func_ref.split(":")
        kernel = catalog.get_kernel(func_name, kernel_id)
    else:
        kernel = catalog.get_default_kernel(func_ref)
    return kernel(args)
```

### Phase 2: Binder Adoption

- Binder imports catalog from `opteryx.expression.functions`.
- Binder resolves via catalog and stores function references (first as function name, optionally deeplinked).
- Keep existing manual rules as fallback behind feature flag, with telemetry.

### Phase 3: Evaluator Adoption

- `apply_function` in `opteryx/expression/evaluator/` uses bound function references.
- Fall back to legacy `FUNCTIONS` dict only for unbound legacy nodes (feature flagged).

### Phase 4: Optimizer Cost Adoption

- Optimizer imports from `opteryx.expression.functions.catalog`.
- Function-aware predicate cost estimation.
- Telemetry compares old and new ordering impact.

### Phase 5: Cleanup

- Remove legacy `opteryx/functions/` folder (or rename to `opteryx/legacy_functions/` for transition).
- Remove `DEPRECATED_FUNCTIONS` map; lifecycle now managed by catalog.
- Deprecate `function_signatures.json` in favor of generated docs from catalog.

### Phase 6: Docs and Tooling

- Generate `function_signatures.json` from catalog.
- Export catalog metadata for IDE plugins and external validators.

---

## Module Structure

**New expression subsystem:**
```
opteryx/expression/
  __init__.py
  functions/
    __init__.py                  # Exports: catalog, FunctionDefinition, FunctionOverload, etc.
    catalog.py                   # FunctionCatalog, resolution logic (270+ lines)
    implementations/             # Kernel callables, organized by semantic domain
      __init__.py
      type_conversion.py         # CAST variants, BOOLEAN, INTEGER, DOUBLE, DECIMAL, VARCHAR, DATE, BLOB, TRY_*
      text.py                    # UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LPAD, RPAD, LEVENSHTEIN, SPLIT, REPLACE, etc.
      arithmetic.py              # ROUND, FLOOR, CEIL, ABS, SQRT, POWER, LN, LOG10, LOG2, LOG, SIGN, TRUNC
      temporal.py                # DATE_TRUNC, DATEDIFF, DATEPART, YEAR, MONTH, DAY, WEEK, HOUR, MINUTE, SECOND, etc.
      logical.py                 # COALESCE, IFNULL, IFNOTNULL, NULLIF, CASE, IIF, SEARCH
      hash_encoding.py           # MD5, SHA1, SHA224, SHA256, SHA384, SHA512, BASE64_*, BASE85_*, HEX_*
      utility.py                 # ARRAY_CONTAINS, GREATEST, LEAST, RANDOM, SORT, JSONB_OBJECT_KEYS, etc.
    tests/
      test_catalog.py            # 7 passing unit tests
  evaluator/
    __init__.py                  # apply_function, hotpath kernel dispatch
    tests/
      test_evaluator.py
```

**Semantic organization notes:**
- Kernels grouped by *function domain* (not by implementation mechanism).
- Each module contains related functions and their typed/polymorphic kernel variants.
- Binary operators (Plus, Minus, Multiply, Eq, etc.) handled separately in `opteryx/managers/expression/binary_operators.py`.
- Aggregate functions handled via operators subsystem (not in evaluator).

**Import patterns:**
- Binder: `from opteryx.expression.functions import catalog`
- Optimizer: `from opteryx.expression.functions import catalog`
- Evaluator: `from opteryx.expression.functions import catalog`
- Docs tools: `from opteryx.expression.functions import FunctionDefinition`
- Kernel implementations: `from opteryx.expression.functions.implementations import text` (or appropriate module)

**Legacy path (migration only):**
- `opteryx/functions/__init__.py` remains during phases 1–3 for backward compatibility
- Removed in phase 5 once evaluator is fully adopted

---

## Acceptance Criteria

1. Parameter validation failures happen in binder, not execution.
2. Return types for scalar functions are inferred only via catalog resolution.
3. Execution hotpath (kernel lookup by overload id) has no string maps or selection logic.
4. Optimizer can score function predicates using catalog costs.
5. Docs export is generated from the same metadata used by binder/runtime.
6. Adding a function requires one catalog registration and tests, with no duplicate metadata files.

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

- Risk: Mixed legacy and catalog behavior during rollout.
  - Mitigation: dual-path resolution with telemetry and feature flag.
- Risk: Incorrect overload matching for polymorphic functions (`COALESCE`, `GET`, `CASE`).
  - Mitigation: explicit resolver callbacks and dedicated tests.
- Risk: Cost values measured one way but used differently during optimization.
  - Mitigation: benchmark-driven costs, telemetry on cost accuracy, periodic re-validation.
- Risk: Effort to migrate old functions to structured overloads.
  - Mitigation: auto-generate basic catalog entries from `FUNCTIONS` dict; manual refinement prioritized by usage frequency.

---

## Key Design Decisions for Discussion

1. **Alias handling**: Should aliases be first-class in the overload table, or handled as separate entries that redirect to the canonical function at resolution time? (Current: separate redirect.)

2. **Kernel deeplinks**: By default, binder binds to function name (e.g., `"ADD"`) using default/polymorphic kernel. Should optimizer/planner have authority to switch to specific typed kernels (e.g., `"ADD:integer_integer"`) for perf-critical paths? (Suggested: yes, via binder-time flag or post-binding rewrite.)

3. **Catalog mutability**: Should the catalog be frozen at startup or allow runtime registration of new functions? (Suggested: freeze; simplifies reasoning and testing.)

4. **External tooling**: Should we generate OpenAPI/protobuf schemas from the catalog for SQL IDE plugins and external validators? (Suggested: deferred to Phase 6, but plan for it now.)

