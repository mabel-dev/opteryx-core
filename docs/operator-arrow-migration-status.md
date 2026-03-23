# Operator Arrow Migration Status

Companion to `draken-arrow-eradication-plan.md`. That document covers Arrow
usage inside the draken engine internals; this document tracks Arrow usage at
the **operator layer** — the nodes in `opteryx/operators/`.

The `_DATA_FORMAT` sentinel on each node controls what format the pipeline
dispatcher delivers to `execute()`:

- `"draken"` — node receives and returns `Morsel` objects only. No Arrow on the hot path.
- `"arrow"` — node receives and returns `pyarrow.Table` objects. Fully on Arrow.
- `"arrow,draken"` — node accepts both. Internally the node may still call `ensure_arrow_table()` or use `pyarrow` directly.

---

## Fully Ported — `_DATA_FORMAT = "draken"`

These nodes have no direct `pyarrow` dependency on the execution path.

| Node | File |
|---|---|
| Filter | `filter_node.py` |
| Projection | `projection_node.py` |
| Sort | `sort_node.py` |
| Limit | `limit_node.py` |
| Shuffle | `shuffle_node.py` |
| Draken Aggregate | `draken_aggregate_node.py` |
| Draken Aggregate + Group By | `draken_aggregate_and_group_node.py` |
| Draken Inner Join | `draken_inner_join_node.py` |
| Function Dataset | `function_dataset_node.py` |
| Exit | `exit_node.py` |

---

## Partially Ported — `_DATA_FORMAT = "arrow,draken"`

These nodes declare dual-format support but still contain direct `pyarrow`
calls in their bodies. They are not fully migrated.

| Node | File | Nature of Remaining Arrow Usage |
|---|---|---|
| Read Node | `read_node.py` | `struct_to_jsonb`, `normalize_morsel`, `merge_schemas` — all use `pyarrow` directly for schema manipulation and struct→JSONB conversion |
| Parquet Read | `parquet_read_node.py` | Schema casting (`_cast_table_to_schema`, `_cast_morsel_to_schema`), empty-table construction on the no-rows fast path |
| Heap Sort | `heap_sort_node.py` | `pyarrow.concat_tables` when assembling the chunk buffer at EOS; `pyarrow.Array`/`ChunkedArray` type checks in the compression probe |
| Non-Equi Join | `non_equi_join_node.py` | `pyarrow.concat_tables` to assemble the left relation at EOS before converting to `Morsel` |
| Distinct | `distinct_node.py` | Dynamic `import pyarrow` **inside** `execute()` plus an `isinstance(converted, pyarrow.Table)` guard — violates the no-gated-import rule |
| Union | `union_node.py` | Calls `ensure_arrow_table()` unconditionally on every morsel; effectively Arrow-only at runtime despite the dual-format declaration |
| Set Variable | `set_variable_node.py` | Inherits dual-format declaration; passes through without conversion |
| Table Management | `table_management_node.py` | Inherits dual-format declaration; passes through without conversion |
| View Management | `view_management_node.py` | Inherits dual-format declaration; passes through without conversion |
| Explain | `explain_node.py` | Inherits dual-format declaration; passes through without conversion |

---

## Arrow-Only — `_DATA_FORMAT = "arrow"`

These nodes are completely unported. They require Arrow tables on input and
produce Arrow tables on output. All core join operators fall into this bucket.

| Node | File | Notes |
|---|---|---|
| Cross Join | `cross_join_node.py` | `pyarrow.concat_tables`, `Table.from_batches`, `Table.from_arrays`, `Table.from_pydict`, `pyarrow.schema`, `pyarrow.field`, `pyarrow.array` throughout |
| Filter Join (semi/anti) | `filter_join_node.py` | Calls `ensure_arrow_table` on every morsel; delegates to Arrow-native bloom-filter probe |
| Nested Loop Join | `nested_loop_join_node.py` | `pyarrow.concat_tables`, `pyarrow.compute`, `Array.from_buffers`, `pyarrow.py_buffer` for bloom-filter result materialisation |
| Outer Join (LEFT/RIGHT/FULL) | `outer_join_node.py` | `pyarrow.concat_tables`, `Array.from_buffers`, `pyarrow.nulls`, `pyarrow.table` — including null-row construction for unmatched left rows |
| Unnest Join | `unnest_join_node.py` | `pyarrow.schema`, `pyarrow.field`, `pyarrow.array`, `Table.from_arrays`, `Table.from_batches` throughout `_cross_join_unnest_column` and `_cross_join_unnest_literal` |
| Null Reader | `null_reader_node.py` | Produces empty Arrow tables; uses `pyarrow.table`, `pyarrow.array`, `pyarrow.null()`, catches `pyarrow.lib.ArrowInvalid` |
| Show Columns | `show_columns_node.py` | `pyarrow.Table.from_pylist` for result construction |
| Show Create | `show_create_node.py` | `pyarrow.Table.from_pylist` for result construction |
| Show Value | `show_value_node.py` | `pyarrow.Table.from_pylist` for result construction |

---

## Known Rule Violations

### `distinct_node.py` — gated import inside `execute()`

```opteryx-core/opteryx/operators/distinct_node.py#L61-63
import pyarrow

if isinstance(converted, pyarrow.Table):
```

Importing inside a function body to gate a dependency is explicitly prohibited
by the architectural rules. The correct fix is either to complete the migration
(remove the Arrow branch) or move the import to module level and fail at import
time if pyarrow is absent.

### `union_node.py` — misleading dual-format declaration

`_DATA_FORMAT = "arrow,draken"` implies the node can consume `Morsel` objects
natively. In practice `execute()` immediately calls `ensure_arrow_table(morsel)`
on every morsel it receives, converting any `Morsel` back to Arrow before doing
any work. The declaration should be `"arrow"` until the node is genuinely ported.

---

## Migration Priority

The join cluster is the highest-value target. All five Arrow-only join nodes
(`cross_join`, `nested_loop_join`, `outer_join`, `filter_join`, `unnest_join`)
are on the hot path for multi-table queries and represent the largest single
block of unported execution logic. Porting any one of them also removes the
need for the `ensure_arrow_table` conversion step in `base_plan_node.py` for
that code path.

Suggested order:

1. **`union_node.py`** — fix the misleading format declaration first; straightforward port since it only concatenates morsels.
2. **`null_reader_node.py`** — produces empty tables; can be rewritten to emit empty `Morsel` objects trivially.
3. **`non_equi_join_node.py`** — already converts to `Morsel` after assembly; remove the Arrow accumulation step.
4. **`nested_loop_join_node.py`** — mid-complexity; bloom-filter probe result materialisation is the main Arrow dependency.
5. **`outer_join_node.py`** — high complexity; null-row construction for unmatched rows needs a native Draken equivalent.
6. **`cross_join_node.py`** — high complexity; Cartesian product expansion is currently entirely Arrow-based.
7. **`filter_join_node.py`** — depends on outer/nested-loop being ported first (shared bloom-filter infrastructure).
8. **`unnest_join_node.py`** — highest complexity; both literal and column unnest paths are deeply Arrow-coupled.