# From Tree-Walking to Bytecode: How Opteryx Replaced Its Expression Evaluator

Every SQL engine needs an expression evaluator. For most analytical queries, the WHERE clause is the critical path — it runs against every morsel of data the engine reads, on every column in the predicate. What the evaluator costs per-batch is what the query costs at scale.

Opteryx's original evaluator was a recursive Python tree-walker: `_eval_value(node, morsel)` called itself depth-first through a Node tree, dispatching each node type to a Draken vector kernel. It worked, and it was correct. But it had an inherent ceiling: every expression evaluation touched the Python runtime on every node, every morsel, every query. No amount of Cython in the kernels underneath could change that.

We replaced it.

---

## The Problem with a Tree-Walker

The original `_eval_value` was a Cython `.pyx` function, but its dispatch structure was Python-shaped: a sequence of `if node_type == NT_AND` branches, recursive calls, and attribute reads through Python objects. Each morsel (a batch of rows) meant:

- A full tree traversal to resolve column identities — even though those identities don't change between morsels.
- Operator strings looked up at evaluation time — `"Eq"`, `"Gt"`, etc. — even though the operator is fixed at bind time.
- Function callables resolved by walking `node.function_ref.selected_overload.kernel.callable_ref` — per morsel.
- The GIL held throughout, because the tree nodes are Python objects.

For a predicate like `event_time > '2024-01-01'::TIMESTAMP AND region = 'us-east-1'`, the tree-walker did the same work on batch 1 as on batch 10,000. Every resolution was repeated from scratch.

---

## The New Architecture: Three Layers

The replacement is a three-layer pipeline that separates *binding* from *execution*. Work that can be done once at bind time is removed from the hot path entirely.

```
Python Node tree (planner/binder output)
         │
         ▼
  [Layer 1] C++ CompiledExpressionArena
         │  lower(node) → CompiledExpression struct tree
         │
         ▼
  [Layer 2] Cython linearizer
         │  build_bytecode(handle) → CompiledBytecode
         │  (postfix walk, resolves metadata once)
         │
         ▼
  [Layer 3] Cython stack machine
            execute_bytecode(bc, morsel) → BoolVector
            (pure-bitmap path: nogil)
```

Each layer has a clear job. The C++ arena owns memory and lifetimes. The Cython linearizer resolves everything it can at bind time. The stack machine executes with as little overhead as possible.

---

## Layer 1: The C++ Arena

The arena lowers a Python Node tree into a mirrored C++ struct tree:

```cpp
struct CompiledExpression {
    int node_type;
    PyObject* value;
    PyObject* schema_column;
    PyObject* source_node;
    CompiledExpression* left;
    CompiledExpression* right;
    CompiledExpression* centre;
    std::vector<CompiledExpression*> parameters;
};
```

`CompiledExpressionArena` owns a `std::deque<CompiledExpression>` — deque gives stable pointers even as nodes are added, so child pointers never dangle. Python object lifetimes are managed by a separate `held_refs_` vector that holds strong references for the arena's lifetime.

This is the only step that crosses the Python/C++ boundary. After `lower()` returns, the evaluator never touches Python Node objects in the hot path.

---

## Layer 2: The Linearizer

`build_bytecode()` walks the lowered tree in postfix order, emitting `BytecodeInstr` C structs into a flat array. Each instruction encodes everything the executor will need, resolved once:

```cython
ctypedef struct BytecodeInstr:
    int opcode          # BCOpcode enum
    int arity           # variadic ops
    int op_code         # OP_EQ/OP_GT/... integer for BC_COMPARE
    int flags           # temporal type flags for BC_COMPARE
    int bool_value      # 0/1 for BC_LOAD_LIT_BOOL
    PyObject* literal_obj    # pre-resolved literal scalar/set
    PyObject* compare_op_str # operator string for BC_BINARY_OP/BC_UNARY_OP
    PyObject* left_orso_type # schema type for coercion
    PyObject* right_orso_type
    PyObject* column_identity # bytes — morsel lookup key
    PyObject* column_name
    PyObject* source_node    # BC_LEGACY only
    PyObject* callable_ref   # kernel callable for BC_FUNCTION/BC_CAST
```

The linearizer's job is to front-load as much work as possible. For a comparison operator, `_linearize` does this once at bind time:

```cython
if nt == _NT_COMPARISON_OPERATOR:
    # Read schema types from children BEFORE linearising them.
    left_type = getattr(left_sc, "type", None) if left_sc is not None else None
    right_type = getattr(right_sc, "type", None) if right_sc is not None else None

    # Run temporal validation once — raises at bind time, not execution.
    _validate_temporal_at_bind(node.left.node_type, left_type, ...)

    # Resolve op string to integer code.
    op_code_val = <int>op_codes.get(op_str, 0)

    # Pre-set temporal flags.
    flags = 0
    if left_type is _OrsoTypes_DATE or left_type is _OrsoTypes_TIMESTAMP:
        flags |= BC_CMP_LEFT_TEMPORAL
    if right_type is _OrsoTypes_DATE or right_type is _OrsoTypes_TIMESTAMP:
        flags |= BC_CMP_RIGHT_TEMPORAL
```

The same pre-resolution applies to:
- **Functions**: `callable_obj = func_ref_meta.selected_overload.kernel.callable_ref` resolved once; stored as `callable_ref` in the instruction.
- **Casts**: The cast closure factory is called at bind time; the returned callable is stored.
- **Column loads**: `schema_column.identity` and `schema_column.name` are encoded to bytes once and stored in the instruction.
- **BETWEEN**: Bounds and inclusivity flags extracted from the node tree; stored as literal scalars.

The linearizer also computes `max_stack_depth` during the walk by simulating the stack pointer. The executor pre-allocates a Python list to exactly that depth — no dynamic growth.

---

## The Opcode Set

The bytecode has 17 opcodes plus one fallback:

| Opcode | Stack | Operation |
|--------|-------|-----------|
| `BC_LOAD_COL` | push 1 | Load column from morsel by pre-resolved identity |
| `BC_LOAD_LIT_BOOL` | push 1 | Load boolean constant |
| `BC_LOAD_LIT_SCALAR` | push 1 | Load scalar literal |
| `BC_LOAD_LIT_SET` | push 1 | Load pre-built CarcharSet/PerfectHashSet |
| `BC_AND` | pop 2, push 1 | BoolVector AND |
| `BC_OR` | pop 2, push 1 | BoolVector OR |
| `BC_XOR` | pop 2, push 1 | BoolVector XOR |
| `BC_NOT` | pop 1, push 1 | BoolVector NOT |
| `BC_DNF` | pop N, push 1 | Variadic AND fold |
| `BC_CNF` | pop N, push 1 | Variadic OR fold |
| `BC_COMPARE` | pop 2, push 1 | Typed comparison via draken_compare |
| `BC_BETWEEN` | pop 1, push 1 | Range check with pre-extracted bounds |
| `BC_BINARY_OP` | pop 2, push 1 | Arithmetic/string vector ops |
| `BC_UNARY_OP` | pop 1, push 1 | IS NULL, IsEmpty, BitwiseNot, etc. |
| `BC_FUNCTION` | pop N, push 1 | Pre-resolved kernel callable |
| `BC_EXTRACTION` | pop 1, push 1 | Arrow / LongArrow / MapAccess |
| `BC_CAST` | pop 1, push 1 | Pre-resolved cast closure |
| `BC_LEGACY` | push 1 | Fall back to `_eval_value()` tree-walker |

`BC_LEGACY` is explicit. When the linearizer encounters a node type it doesn't handle natively yet (CASE expressions, primarily), it emits a single `BC_LEGACY` instruction carrying `source_node`. The executor calls `_eval_value()` on that node. Nothing is silently falling back; the fallback is an opcode.

---

## Layer 3: The Stack Machine

The executor reads the flat instruction array in order, maintaining a pre-allocated Python list as an operand stack:

```python
def execute_bytecode(CompiledBytecode bc, Morsel morsel):
    if bc.is_pure_bitmap:
        return evaluate_bitmap(bc, morsel)  # fast path

    stack = [None] * bc.max_stack_depth
    sp = 0

    for slot in bc.instrs:
        opcode = slot.opcode

        if opcode == BC_LOAD_COL:
            stack[sp] = morsel.column(
                <bytes>slot.column_identity,
                <bytes>slot.column_name
            )
            sp += 1

        elif opcode == BC_AND:
            stack[sp - 2] = stack[sp - 2].and_vector(stack[sp - 1])
            sp -= 1

        elif opcode == BC_COMPARE:
            v_left = stack[sp - 2]
            v_right = stack[sp - 1]
            if slot.op_code != OP_UNKNOWN:
                stack[sp - 2] = draken_compare_int(slot.op_code, v_left, v_right)
            else:
                stack[sp - 2] = draken_compare(<str>slot.compare_op_str, v_left, v_right)
            sp -= 1

        elif opcode == BC_FUNCTION:
            arity = slot.arity
            func = <object>slot.callable_ref
            args = stack[sp - arity : sp]
            stack[sp - arity] = func(*args)
            sp = sp - arity + 1

        elif opcode == BC_LEGACY:
            stack[sp] = _eval_value(<object>slot.source_node, morsel)
            sp += 1
        # ... etc
```

No dict lookups. No string comparisons in the hot loop. No attribute resolution. The opcode is an integer; the operator code is an integer; the callable is a direct reference.

---

## The Pure-Bitmap Fast Path

When a bytecode contains only boolean column loads and boolean algebra — `BC_LOAD_COL`, `BC_LOAD_LIT_BOOL`, `BC_AND`, `BC_OR`, `BC_XOR`, `BC_NOT`, `BC_DNF`, `BC_CNF` — the linearizer sets `is_pure_bitmap = True`.

For these bytecodes, the executor takes a different path entirely:

1. **Pre-pass (GIL held)**: Extract the raw bitmap pointer and null mask from each `BC_LOAD_COL` column. Allocate scratch `uint8_t*` buffers — one per stack slot, plus two scratch.
2. **Inner loop (nogil)**: Enter `c_execute_bytecode_inner()` with the GIL released. The loop operates on `uint8_t*` scratch buffers. No Python objects. No refcounting. Binary ops use pointer-swapping to avoid aliasing. DNF/CNF use SIMD popcount for short-circuit evaluation.
3. **Post-pass (GIL held)**: Wrap the result buffer back into a `BoolVector`.

This covers a large fraction of real OLAP WHERE clauses: conjunctions and disjunctions of boolean columns, materialized aggregation masks, pre-filtered flag columns. For those, the evaluation is 2–3x faster because zero GIL work happens during the inner loop.

---

## What Didn't Change

The planner and binder still produce Python Node trees. The operator API is unchanged — `filter.pyx` calls `lower()` and `build_bytecode()` once at bind time and `execute_bytecode()` per morsel. Code outside the evaluator boundary doesn't see the bytecode.

CASE expressions still route through `BC_LEGACY` to `_eval_value()`. This is not a limitation we're hiding — `BC_LEGACY` is an explicit opcode in the bytecode listing. As native opcodes are added for each remaining node type, the fallback shrinks.

---

## The Design Constraint That Shaped Everything

The `BytecodeInstr` struct stores `PyObject*` borrowed pointers. The executor casts them to typed Cython classes — `BoolVector`, `Morsel`, `bytes` — and dispatches into typed `cpdef` methods directly. It never touches refcounts.

This works because `CompiledBytecode._held_refs` holds a Python list of strong references to every object any instruction points at, for the bytecode's lifetime. The executor never INCREFs or DECREFs; the list is the owner. This keeps the hot path free of CPython bookkeeping while remaining memory-safe.

The node type constants are compile-time `DEF` values in both the linearizer and the executor — they fold to C-level integer comparisons and, with `optimize.use_switch=True`, a jump table. A runtime check at import time asserts they match the `NodeType` IntEnum values. If they diverge, the module fails to import rather than silently misevaluating expressions.

---

The bytecode system is not the end state. It's the foundation. Now that expressions compile to a flat typed array, the next steps — JIT dispatch, SIMD specialization for specific opcode sequences, bytecode-level predicate reordering — become tractable. The tree-walker didn't give us that surface. The bytecode does.
