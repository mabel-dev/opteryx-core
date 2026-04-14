# Draken Value Model Design

## Context

Draken is the native execution value model for the query engine. It is designed to carry typed semantic values through execution without relying on PyArrow or NumPy as the internal representation.

Python remains important, but only at planning and binding edges where compatibility, orchestration, and user-facing integration are required. Once execution begins, values should stay in Draken-native structures.

## Core Model

Draken represents data as typed vectors. This is the unit of execution, and expression outputs are always vectors, not scalars. Even when an expression is logically constant, the runtime representation should be a typed Draken constant vector so downstream operators continue to operate on vector data.

This model keeps the execution path explicit:

1. planner and binder establish expression semantics
2. execution evaluates to vectors
3. operators consume vectors directly
4. boundary layers convert only when needed for external compatibility

## Typed Semantic Values

Draken is not just storing raw bytes; it carries semantic types that preserve meaning through execution. Some types require special handling because their meaning is richer than a simple primitive payload:

- `DATE`
- `TIMESTAMP`
- `TIME`
- `INTERVAL`
- `DECIMAL`

These types must be treated as typed semantic values, not generic numeric or string payloads. Their behavior affects comparison, casting, arithmetic, grouping, and output formatting.

### Temporal types

`DATE`, `TIMESTAMP`, and `TIME` are semantic time values, but they are not all interchangeable.

Civil time is not handled yet. Where civil-time semantics are needed, interpretation must be offset-aware rather than assumed to be naive local time.

### Interval type

`INTERVAL` is especially complex. It is not a plain scalar quantity and should be treated as a first-class semantic type with dedicated handling in arithmetic and casting paths.

### Decimal type

`DECIMAL` requires exactness and metadata-aware behavior. It should not be reduced to an approximate floating-point representation in execution.

## Constant Vectors

Literal values should prefer typed Draken constant vectors. This keeps literals aligned with the vector execution model and avoids unnecessary expansion into materialized arrays or external container types.

Constant vectors are the natural representation for:

- literal expressions
- folded constants
- repeated values that remain semantically constant through a plan

## Boundary Rules

PyArrow and NumPy are not the internal execution representation.

They may appear at compatibility boundaries, but they are not the substrate of expression evaluation, filtering, aggregation, or other hot execution paths. The engine should prefer native Draken vectors and kernels for internal processing.

The rule of thumb is simple:

- planning / binding edges: Python is acceptable
- execution core: Draken vectors only
- external conversion boundaries: Arrow or other compatibility formats may be used intentionally

## Implications for the Engine

This value model shapes the eradication effort as a consequence of architecture, not as the headline. If execution is Draken-native, then internal dependencies on PyArrow and NumPy become boundary concerns rather than core concerns.

That gives the engine a clear direction:

1. keep expression outputs vectorized
2. preserve semantic types in native form
3. use constant vectors for literals
4. handle temporal, interval, and decimal types explicitly
5. reserve Python and Arrow for compatibility edges only