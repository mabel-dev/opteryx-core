# Operators

The `operators` package contains all physical execution plan nodes for the Opteryx query engine.
Each operator corresponds to a SQL clause or computation and is implemented as a Cython extension
compiled into a single `_operators.so` module via `_operators.pyx`.

## Architecture

Operators are **push-based**: upstream operators push `Morsel` (column batches) downstream through
edges. The scheduler (see `opteryx/execution/`) orchestrates parallel execution across a DAG.

```
Reader operators  →  Filter  →  Project  →  Aggregate  →  Sort  →  Limit  →  Result
       ↑                   ↑           ↑              ↑          ↑
  Null, Parquet     WHERE      SELECT        GROUP BY    ORDER BY  LIMIT
```

## Operator Categories

### Readers (Source)

Source operators that read data from external storage into Draken vectors.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `parquet_read` | — | Read Parquet files via Rugo |
| `null_reader` | — | Return empty result set |
| `read` | — | Generic file reader |

### Filters

Predicate operators that filter rows based on expressions.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `filter` | `WHERE` | Filter rows by predicate |
| `filter_join` | `JOIN ... ON` | Semi-join filter |

### Projections

Transform and select columns from input rows.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `projection` | `SELECT` | Select columns, apply expressions |

### Aggregates

Compute aggregate functions over groups or the entire input.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `aggregate` | `AGGREGATE` | General-purpose aggregation |
| `grouped_aggregate_hashed` | `GROUP BY` | Grouped aggregation via hash table |

### Joins

Combine rows from multiple inputs.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `cross_join` | `CROSS JOIN` | Cartesian product |
| `hashed_inner_join` | `INNER JOIN` | Hash-based inner join |
| `outer_join` | `OUTER JOIN` | Outer join with null-fill |
| `asof_join` | `ASOF JOIN` | As-of join (temporal) |
| `nested_loop_join` | `JOIN` | Nested loop join (fallback) |
| `non_equi_join` | `JOIN` | Non-equi join |
| `unnest_join` | `CROSS JOIN UNNEST` | Flatten nested arrays |

### Sort & Limit

Order and constrain result sets.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `sort` | `ORDER BY` | Sort input by key columns |
| `heap_sort` | — | Top-k partial sort |
| `limit` | `LIMIT` | Restrict row count |

### Distinct & Union

Result-set operations.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `distinct` | `SELECT DISTINCT` | Deduplicate rows |
| `union` | `UNION` | Combine result sets |

### DDL / Metadata

Table and catalog management.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `insert` | `INSERT` | Write rows |
| `view_management` | `CREATE VIEW` | Create/drop views |
| `table_management` | `CREATE TABLE` | Create/drop tables |
| `show_columns` | `SHOW COLUMNS` | List columns |
| `show_create` | `SHOW CREATE` | Show DDL |
| `show_value` | `SHOW VARIABLE` | Read system variable |
| `set_variable` | `SET VARIABLE` | Modify system variable |
| `relation_management` | — | Internal relation ops |

### Function Datasets

Source operators for built-in functions that return tables.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `function_dataset` | `SELECT * FROM TABLE(...)` | Built-in table functions |

### Utility

Terminal and diagnostic operators.

| Operator | SQL Clause | Purpose |
|----------|-----------|---------|
| `exit` | — | End query execution |
| `explain` | `EXPLAIN` | Show execution plan |

## Data Flow

All operators work with **Draken Vectors** in the unified format
(`data[selection[i]]`). Morsels are groupings of related vectors (one per
column) with a shared logical row count.

```
Morsel = {
    columns: [DrakenVector, DrakenVector, ...],
    column_names: [bytes, bytes, ...]
}
```

Operators are push-based: they receive `Morsel` objects from upstream and yield
`Morsel` objects to downstream. The EOS sentinel (`_EOS_SENTINEL`) signals
exhaustion.

## Add a New Operator

1. Create a subdirectory `opteryx/operators/<name>/` with `__init__.py` and the implementation `.pyx` file.
2. Register the operator class in `_operators.pyx` via the operator catalog.
3. Add tests in `tests/`.

## Notes

- `docs/design/` contains operator design documents.
- The `catalog.py` module maps SQL operator names to Python classes.
- All operators in subdirectories are compiled via textual include in
  `_operators.pyx`; no separate build step is needed for individual operators.
