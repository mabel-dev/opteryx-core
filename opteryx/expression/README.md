# Expression Module

The `expression` module handles SQL expression evaluation after parsing and binding. It contains the dispatch layer for literals, column references, functions, operators, casts, predicates, and Draken-vector execution.

## Overview

Sources are Cython (`.pyx`); a few function-definition leaves remain plain
Python (`.py`). Generated `.c`/`.cpp`/`.so` artefacts are not listed.

```
expression/
├── Core Evaluation
│   ├── __init__.pyx                # Main evaluate() dispatcher, NodeType enum
│   ├── binary_operators.pyx        # Binary operator implementations
│   ├── unary_operations.pyx        # Unary operator implementations
│   ├── casts.pyx                   # Type casting utilities
│   └── formatter.pyx               # Expression formatting/display
│
├── Functions (Database Functions)
│   ├── functions/
│   │   ├── catalog.pyx             # FunctionCatalog: registry, resolution, metadata
│   │   ├── implementations/        # Kernel implementations
│   │   │   ├── arithmetic.py
│   │   │   ├── temporal.py
│   │   │   ├── text.pyx
│   │   │   ├── logical.pyx
│   │   │   └── utility.pyx
│   │   └── registrar/              # Function definitions by category
│   │       ├── arithmetic.pyx
│   │       ├── text.pyx
│   │       ├── temporal.pyx / temporal_extra.pyx
│   │       ├── aggregate.pyx
│   │       ├── logical.pyx
│   │       ├── hash_encoding.pyx
│   │       ├── constant.pyx
│   │       └── utility.pyx
│
├── Filter Operations (Vector Operations)
│   └── operations/                 # Modular package for filter operations
│       ├── __init__.pyx            # Main filter_operations() dispatcher
│       ├── comparisons.pyx         # Eq, NotEq, Lt, Gt, etc.
│       ├── list_ops.pyx            # InList, NotInList
│       ├── string_matching.pyx     # Like, RLike, InStr patterns
│       ├── array_ops.pyx           # AnyOp*, AllOp*, array contains
│       └── special_ops.pyx         # JSON path queries
│
├── Evaluation Engine
│   └── evaluator/
│       ├── __init__.py             # Package entry / legacy submodule aliases
│       ├── _impl.pyx               # Compiled evaluator entry points
│       ├── evaluation.pyx          # Bytecode/evaluation orchestration
│       ├── arithmetic.pyx          # Arithmetic kernels
│       ├── comparisons.pyx         # Comparison kernels
│       ├── string_ops.pyx          # String expression kernels
│       ├── temporal_ops.pyx        # Temporal expression kernels
│       ├── json_ops.pyx            # JSON expression kernels
│       ├── case_eval.pyx           # CASE expression handling
│       ├── type_coercion.pyx       # Coercion helpers
│       └── function_execution.pyx  # Function kernel dispatch
│
├── Metadata & Catalogs
│   ├── operator_catalog.pyx        # Operator definitions and metadata
│   └── intervals.pyx               # INTERVAL type operations
```

## Key Components

### Core Evaluation (`__init__.pyx`)

- **`evaluate()`** - Main entry point for evaluating expressions against data morsels
- **`NodeType`** - Enum of all AST node types (FUNCTION, BINARY_OPERATOR, LITERAL, etc.)
- **`get_all_nodes_of_type()`** - AST traversal utility
- Type coercion and constant-folding optimization

### Functions Subsystem (`functions/`)

Handles all built-in SQL functions (UPPER, LOWER, SUM, etc.).

**Architecture:**
- **`FunctionCatalog`** - Central registry mapping function names to definitions
- **`FunctionDefinition`** - Metadata for a function (name, parameters, overloads, lifecycle)
- **`FunctionOverload`** - A specific callable form (e.g., `SUM(numeric)` vs `SUM(temporal)`)
- **`registrar/`** - Domain-specific modules defining functions by category
  - Each module exports `get_builtin_*_functions()` returning a list of `FunctionDefinition`
  - Categories: arithmetic, text, temporal, aggregate, utility, etc.

**Type Resolution:**
- Overload matching by arity (argument count)
- Type family scoring (numeric, string, temporal, array, etc.)
- Automatic coercion cost calculation
- Fallback to "any" type for polymorphic functions

### Filter Operations (`operations/`)

High-performance vector operations for WHERE clauses and predicates.

**Modular Design:**
Each operation category is a separate module with clear responsibility:

| Module | Operators |
|--------|-----------|
| `comparisons.pyx` | `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq` |
| `list_ops.pyx` | `InList`, `NotInList` |
| `string_matching.pyx` | `Like`, `RLike`, `InStr`, `ILike`, etc. |
| `array_ops.pyx` | `AnyOp*`, `AllOp*`, `@>`, `@>>` |
| `special_ops.pyx` | `@?` (JSON path) |

**Encoding shapes:** All vectors are `DrakenVector` in the unified format
(dense / constant / dict), accessed uniformly as `data[selection[i]]`. The
uniform path is the correctness contract; shape-specialized fast paths are the
exception, permitted only with architect sign-off (see Draken Vector Model in
the root `CLAUDE.md`).

**Key Function:**
```python
filter_operations(left_arr, left_type, operator, right_arr, right_type)
```
Returns a boolean array for filtering rows, with proper null semantics (tri-state).

### Evaluation Engines

**Compiled Evaluator** (`evaluator/`)
- Evaluates expressions using compiled Cython/C++ paths and Draken vectors
- Splits arithmetic, comparison, string, temporal, JSON, CASE, coercion, and function execution into separate modules
- Uses bytecode/evaluation helpers for the hot path rather than a Python tree walk

**Function Execution** (`evaluator/function_execution.pyx`)
- Kernel dispatch: maps FunctionDefinition to callable
- Type coercion for kernel parameters (Draken vectors only in the hot path)
- Null handling policies (compress, passthrough, bypass, etc.)

### Supporting Modules

**`binary_operators.pyx`** - Binary operator kernels (arithmetic, comparison, logical)
**`unary_operations.pyx`** - Unary operator kernels (NOT, negation)
**`casts.pyx`** - Type casting functions (CAST, implicit coercions)
**`formatter.pyx`** - Pretty-print expressions for error messages
**`operator_catalog.pyx`** - Operator metadata (precedence, associativity)
**`intervals.pyx`** - INTERVAL arithmetic and type operations

## Data Flow

### Expression Evaluation Flow

```
Planner creates LogicalPlan
    ↓
Binder binds column references and types
    ↓
Evaluator.evaluate(expression, morsel)
    ├─ For each node type:
    │  ├─ FUNCTION → FunctionCatalog.resolve() → kernel dispatch
    │  ├─ BINARY_OPERATOR → filter_operations() or arithmetic
    │  ├─ UNARY_OPERATOR → unary_operations
    │  ├─ CAST → casts module
    │  └─ LITERAL → constant value
    ↓
Result DrakenVector
```

### Function Resolution Flow

```
FunctionCall node (name, args)
    ↓
FunctionCatalog.resolve(name, arg_nodes, context)
    ├─ Resolve alias → canonical name
    ├─ Filter overloads by arity
    ├─ Score remaining overloads by type family
    ├─ Select best match (lowest score)
    └─ Return ResolvedFunction
    ↓
Evaluator.apply_bounded_function()
    ├─ Get kernel from KernelSpec
    ├─ Coerce parameters for the selected kernel engine
    ├─ Call kernel
    └─ Apply null policy
    ↓
Result vector
```

## Architecture Decisions

### 1. Modular Operations Package
**Why:** Separated concerns for different operation types
- Easier to locate and modify specific operations
- Better testability (test comparisons independently from string matching)
- Reduces merge conflicts (each operation type in separate file)

**Trade-off:** Slight import overhead, but negligible

### 2. Function Catalog + Registrar Pattern
**Why:** Centralizes function metadata and resolution
- Single source of truth for function signatures
- Enables static analysis and introspection
- Registrar package allows adding new functions without touching core code

**Trade-off:** More indirection than direct function references

### 3. Tri-State Boolean for Filtering
**Why:** SQL null semantics (NULL AND TRUE = NULL, not FALSE)
- Preserves information for downstream operators
- Compressed during evaluation for performance
- Expanded back during result assembly

## Performance Considerations

1. **Uniform vector access** - The `data[selection[i]]` pattern covers dense,
   constant, and dict-encoded shapes through one path; constant/dict layouts
   give compression for free without shape-specific branches.
2. **Null Compression** - Remove nulls before operation, restore after (for
   non-null-sensitive ops).
3. **Native kernels** - Compiled Cython/C kernels keep hot paths out of Python
   object loops; numpy/pyarrow are banned from execution paths.

## Adding New Operations

To add a new filter operation (e.g., `NewOp`):

1. **Choose category** - Create in appropriate module or new one
2. **Implement handler** - Add function in that module
3. **Add dispatcher** - Add the case in the operations dispatcher
4. **Update metadata** - Add to `_SKIP_COMPRESSION_OPS` if needed
5. **Test** - Add test in appropriate test file

Example:
```python
# In operations/comparisons.pyx (or new module)
def my_new_operation(arr, value, dict_candidate=False):
    # Implementation
    pass

# In operations/__init__.pyx dispatcher
if operator == "MyNewOp":
    return comparisons.my_new_operation(arr, value, dict_candidate)
```

## Current Maintenance Notes

1. **Null handling** - Keep tri-state SQL semantics (`NULL AND TRUE = NULL`)
   consistent across comparison, list, and string-matching operations.
2. **Generated C/C++ files** - Many `.c`/`.cpp` files are Cython outputs. Edit the `.pyx` sources unless you are intentionally inspecting generated code.

## Related Modules

- **`planner/`** - Creates LogicalPlan with expression nodes
- **`planner/binder/`** - Binds columns to schema types
- **`operators/`** - Physical operators that use evaluated expressions
- **`models/`** - LogicalColumn, Node, and schema types
- **`draken/`** - Native vector substrate used by the evaluator
