# Expression Module

The `expression` module handles all aspects of SQL expression evaluation in Opteryx, from parsing and binding to execution and optimization.

## Overview

```
expression/
├── Core Evaluation
│   ├── __init__.py                 # Main evaluate() dispatcher, NodeType enum
│   ├── binary_operators.py         # Binary operator implementations
│   ├── unary_operations.py         # Unary operator implementations
│   ├── casts.py                    # Type casting utilities
│   └── formatter.py                # Expression formatting/display
│
├── Functions (Database Functions)
│   ├── functions/
│   │   ├── catalog.py              # FunctionCatalog: registry, resolution, metadata
│   │   ├── compat.py               # Backward-compat layer for legacy imports
│   │   ├── implementations/        # Kernel implementations (text, arithmetic, etc.)
│   │   └── registrar/              # Function definitions by category
│   │       ├── arithmetic.py
│   │       ├── text.py
│   │       ├── temporal.py
│   │       └── ...
│
├── Filter Operations (Vector Operations)
│   ├── operations/                 # Modular package for filter operations
│   │   ├── __init__.py             # Main filter_operations() dispatcher
│   │   ├── comparisons.py          # Eq, NotEq, Lt, Gt, etc.
│   │   ├── list_ops.py             # InList, NotInList
│   │   ├── string_matching.py      # Like, RLike, InStr patterns
│   │   ├── array_ops.py            # AnyOp*, AllOp*, array contains
│   │   ├── special_ops.py          # JSON path queries
│   │   ├── fastpath_constant.py    # Constant-encoded vector optimization
│   │   ├── fastpath_dictionary.py  # Dictionary-encoded vector optimization
│   │   ├── fastpath_telemetry.py   # Performance metrics
│   │   └── type_coercion.py        # Type conversion utilities
│   └── ops.py                      # (LEGACY - to be deprecated)
│
├── Evaluation Engines
│   └── evaluator/
│       ├── draken.py               # Draken vector evaluation engine
│       └── function_execution.py   # Function kernel dispatch
│
├── Metadata & Catalogs
│   ├── operator_catalog.py         # Operator definitions and metadata
│   └── intervals.py                # INTERVAL type operations
```

## Key Components

### Core Evaluation (`__init__.py`)

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
| `comparisons.py` | `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq` |
| `list_ops.py` | `InList`, `NotInList` |
| `string_matching.py` | `Like`, `RLike`, `InStr`, `ILike`, etc. |
| `array_ops.py` | `AnyOp*`, `AllOp*`, `@>`, `@>>` |
| `special_ops.py` | `@?` (JSON path) |

**Fastpath Optimization:**
- **Constant encoding** (`fastpath_constant.py`) - Vectors where all values are identical
- **Dictionary encoding** (`fastpath_dictionary.py`) - Dictionary-compressed string vectors
- **Telemetry** (`fastpath_telemetry.py`) - Performance metrics for fastpath usage

**Key Function:**
```python
filter_operations(left_arr, left_type, operator, right_arr, right_type)
```
Returns a boolean array for filtering rows, with proper null semantics (tri-state).

### Evaluation Engines

**Draken Engine** (`evaluator/draken.py`)
- Evaluates expressions using Draken vectors (compiled vector types)
- ~1415 lines; candidates for further modularization by operation type
- 39 functions handling all operation types

**Function Execution** (`evaluator/function_execution.py`)
- Kernel dispatch: maps FunctionDefinition to callable
- Type coercion for kernel parameters (Arrow ↔ Draken ↔ NumPy)
- Null handling policies (compress, passthrough, bypass, etc.)

### Supporting Modules

**`binary_operators.py`** - Binary operator kernels (arithmetic, comparison, logical)
**`unary_operations.py`** - Unary operator kernels (NOT, negation)
**`casts.py`** - Type casting functions (CAST, implicit coercions)
**`formatter.py`** - Pretty-print expressions for error messages
**`operator_catalog.py`** - Operator metadata (precedence, associativity)
**`intervals.py`** - INTERVAL arithmetic and type operations

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
Result vector (PyArrow or Draken)
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
    ├─ Coerce parameters for kernel engine (arrow/draken/numpy)
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

### Fastpath Optimizations

1. **Constant Encoding** - All-same-value vectors skip comparisons
2. **Dictionary Encoding** - String vectors use dictionary indices instead of full strings
3. **Null Compression** - Remove nulls before operation, restore after (for non-null-sensitive ops)
4. **Draken Vectors** - Native vector types avoid PyArrow overhead

### Telemetry

The `fastpath_telemetry` module tracks:
- Dictionary fastpath hits/fallbacks
- Constant fastpath hits/fallbacks

Useful for profiling and identifying optimization opportunities.

## Adding New Operations

To add a new filter operation (e.g., `NewOp`):

1. **Choose category** - Create in appropriate module or new one
2. **Implement handler** - Add function in that module
3. **Add dispatcher** - Add case in `_inner_filter_operations()`
4. **Update metadata** - Add to `_SKIP_COMPRESSION_OPS` if needed
5. **Test** - Add test in appropriate test file

Example:
```python
# In operations/comparisons.py (or new module)
def my_new_operation(arr, value, dict_candidate=False):
    # Implementation
    pass

# In operations/__init__.py dispatcher
if operator == "MyNewOp":
    return comparisons.my_new_operation(arr, value, dict_candidate)
```

## Known Issues & TODOs

### Debt to Address

1. **`ops.py`** - Legacy file, superseded by `operations/` package. Can be deprecated once all imports updated.

2. **`evaluator/draken.py`** (1415 lines) - Can be split by operation category similar to operations package:
   - `arithmetic.py` - +, -, *, /, %, etc.
   - `comparisons.py` - ==, !=, <, >, etc.
   - `string_ops.py` - concat, substring, regex
   - `temporal_ops.py` - date_add, extract
   - `array_ops.py` - array element access, aggregates

3. **`__init__.py`** (845 lines) - Extract:
   - `node_types.py` - NodeType enum
   - `evaluator.py` - Main evaluate() dispatch
   - `evaluation_strategies.py` - Constant vector, type coercion logic

4. **Null Handling** - Duplicated logic across fastpath modules. Consider unified `null_semantics.py` module.

## Related Modules

- **`planner/`** - Creates LogicalPlan with expression nodes
- **`planner/binder/`** - Binds columns to schema types
- **`operators/`** - Physical operators that use evaluated expressions
- **`models/`** - LogicalColumn, Node, and schema types
- **`compiled/draken/`** - Draken vector engine (used by evaluator)
