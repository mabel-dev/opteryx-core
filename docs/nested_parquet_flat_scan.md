# Nested Parquet: Flat Scan with On-the-Fly Key Generation

**Status:** Proposal  
**Author:** Design review prompted by Rey, Rieger, Neumann (2025)  
**Reference:** "Nested Parquet Is Flat, Why Not Use It? How To Scan Nested Data With On-the-Fly Key Generation and Joins" — Alice Rey, Maximilian Rieger, Thomas Neumann.  
Proceedings of the 2025 ACM International Conference on Management of Data (SIGMOD'25), Article 192, 24 pages.  
DOI: https://doi.org/10.1145/3725329

---

## 1. Problem Statement

Opteryx currently has a significant gap in nested Parquet support. Rugo reads LIST columns correctly — it decodes repetition and definition levels and reconstructs an `ArrayVector` via a finite-state-machine walk through the levels. But STRUCT columns are silently skipped: metadata parsing recognises them (`parquet_reader.pyx` type dispatch at line 1734), sets `logical_type = "struct"`, and then produces no vector. Arrays of structs are similarly unreadable. The binder treats dotted identifiers as literal column names (e.g. `posts.text` is looked up verbatim rather than resolved as field `text` within column `posts`). The `FlatColumn.fields` member in `schema.py` was reserved for struct field descriptions but is never populated.

The consequence is that data stored in common real-world schemas — log events with nested payloads, TPC-H lineitem-within-orders nesting, JSON-derived Parquet — cannot be queried at all, and this failure is silent rather than explicit.

The Rey et al. paper provides a concrete, implementable solution that is well-aligned with Opteryx's design constraints.

---

## 2. The Paper's Core Contribution

### 2.1 What existing systems do wrong

Every major system (DuckDB, Trino, AsterixDB) converts the Dremel encoding into an internal nested type representation. This requires:

- Adding list/struct/map types to every operator in the execution engine
- Running the full Dremel finite-state machine (FSM) during the scan, which serialises access to inner levels — parallelisation granularity is dictated by the outermost required level
- Preventing the query optimiser from seeing across nesting boundaries, so predicate pushdown stops at the top level

The paper reports that DuckDB suffers a 50× performance drop going from flat to one nesting level, then declines further linearly. Trino shows a similar trend. Against their native nested implementations, the paper's approach achieves an average 20× speedup over Trino and 60× over DuckDB across realistic benchmarks (DBLP, TPC-H, Twitter, XMark). [§6.4]

### 2.2 The proposed approach

Rather than reconstructing nesting, treat each repeated group in the Parquet schema as a separate flat relation. Connect the relations with on-the-fly generated integer keys derived from the repetition levels already present in the file. Express the query as a join tree over these flat relations and feed that tree to the existing query optimiser.

**Surrogate key (SK):** the row number of each entry in a relation. For the root level, SK = row index. For a nested level, SK = the count of distinct parent-initiating repetition-level transitions encountered so far.

**Ancestor key (AK):** a foreign key pointing from a child relation back to the row number of the corresponding ancestor. A node nested inside multiple repeated groups carries one AK per ancestor level.

The key generation algorithm (paper Listing 1) is:

```
ancestorKey = -1
for i in range(rowCount):
    ancestorKey += repetitionLevel[i] <= ancestorLevel
    ancestorKeyBuffer[i] = ancestorKey
```

This is a single vectorisable pass over the repetition level array, which Rugo already reads. The paper shows the key computation overhead is 1.5–2.8× of the raw scan cost on TPC-H lineitem, and does not grow with scale factor — acceptable given the orders-of-magnitude query speedup. [§6.6, Table 2]

The join tree construction algorithm (paper Listing 2) sorts required nodes in preorder traversal order and builds the tree in a single linear pass, selecting between surrogate-to-ancestor key joins (parent–child) and ancestor-to-ancestor key joins (sibling). The result is a standard relational join plan that the existing query optimiser can reorder and into which it can push predicates. [§4]

Because surrogate keys are row numbers and ancestor keys are sorted by parent order, both sides of every join are already sorted. This allows merge joins to replace hash joins, eliminating the sort phase. The paper shows merge joins outperform hash joins once the hash table exceeds CPU cache capacity, which is the common case for large nested datasets. [§5, Figure 8]

---

## 3. Current State in Opteryx

| Layer | LIST | STRUCT | MAP | Dotted path |
|---|---|---|---|---|
| **Rugo C++ reader** | R/D levels decoded, `_make_array_vector()` FSM (`parquet_reader.pyx:1448–1590`) | Detected, silently dropped (`parquet_reader.pyx:1734`) | Detected, silently dropped | N/A |
| **Draken vectors** | `ArrayVector` (full) | No `StructVector` | No `MapVector` | N/A |
| **SQL parser** | Parsed | Parsed | Parsed | Parsed as identifiers |
| **Binder** | Flat `ArrayVector` column only | Not populated | Not populated | Literal name match only (`binder.py:57–84`) |
| **Operators** | `CROSS JOIN UNNEST` (`unnest_join.pyx`) | None | None | N/A |
| **Schema** | `element_type` field exists | `FlatColumn.fields` exists but never populated (`schema.py:72,89`) | Not modelled | Not resolved |

**What works today:** querying flat LIST columns (string arrays), unnesting them via explicit `CROSS JOIN UNNEST`, with null-list and null-element preservation. Struct and map columns, arrays of structs, and dotted field access all silently produce empty or wrong results.

---

## 4. Proposed Architecture

The paper's approach maps cleanly onto Opteryx's layers. Crucially, the paper's "no nested types in operators" property aligns with Opteryx's own design constraint: Draken does not need a StructVector, and the execution engine does not need per-operator struct/map handling.

### 4.1 Phase 1 — STRUCT column support without joins

STRUCT fields have `max_repetition_level = 0` — they are not repeated, so every struct row maps 1:1 to its parent row. No join keys are needed. The struct is simply a container for co-located flat columns that happen to share a path prefix.

**Rugo change (`decode_column.cpp`, `parquet_reader.pyx`):**  
When the schema walker encounters a STRUCT node (no physical type, has children), recurse into its children and emit each leaf as an independently named column. The emitted column name is the dotted path from the Parquet schema root (e.g. `address.city`). No new vector type is required; every emitted column is a scalar vector of the appropriate Draken type.

**Binder change (`binder.py:57–84`):**  
Before the verbatim column lookup, split the identifier on `.`. If the full dotted name matches a column emitted by Rugo (e.g. `address.city`), resolve it directly. This is a one-line change to the search predicate and requires no schema model changes.

**Benefit:** Every STRUCT column in every Parquet file in a data lake becomes queryable. No operator changes. No new Draken types. STRUCT data projected out of logs and event streams is immediately accessible.

### 4.2 Phase 2 — Replace the LIST FSM with key generation

The current `_make_array_vector()` function (parquet_reader.pyx:1448–1590) implements the Dremel FSM: it walks repetition and definition levels to reconstruct nested offsets and builds an `ArrayVector`. This is the approach the paper identifies as the performance bottleneck in DuckDB and Trino.

**Rugo change:**  
For LIST columns (max_repetition_level > 0), instead of constructing an `ArrayVector` in the scanner, emit two outputs:

1. The leaf values as a flat scalar vector (all list elements across all rows, in order).
2. An `Int64Vector` containing the ancestor key for each element — computed from the repetition levels using the paper's Listing 1 algorithm.

The ancestor key is the row number of the parent list entry. With this, the list elements are a flat relation joinable to the parent relation on the ancestor key equalling the parent's surrogate key (its row index).

This eliminates `_make_array_vector()` entirely. The `ArrayVector` type may still be kept for results that need to materialise a list column, but it is no longer constructed during the scan hot path.

**Planner change:**  
When a query references a LIST column's elements (directly or via `UNNEST`), the planner emits a join node rather than passing an `ArrayVector` to the unnest operator. The join is: `parent.rowid = child.ancestor_key`. Since both sides are sorted, a merge join is selected.

**Benefit:** Selective predicates on list elements can now be pushed below the join by the existing predicate pushdown strategy. This matches the paper's Figure 7 — a filter on `comments.text` currently has to wait until after the FSM scan of the full list; with the key-based approach, the binder sees it as a filter on a flat column and pushes it to the scan. The paper reports that for realistic workloads with selective predicates, the join-based approach matches or beats the FSM even when the FSM can theoretically skip inner-level values. [§6.5, Figure 15]

The existing `CROSS JOIN UNNEST` operator (`unnest_join.pyx`) becomes a thin wrapper: it now joins on the generated ancestor key column rather than iterating an `ArrayVector`. The user-facing SQL is unchanged.

### 4.3 Phase 3 — Arrays of structs and deep nesting

With Phases 1 and 2 in place, arrays of structs follow without new mechanisms:

- The struct fields are emitted as flat scalar columns (Phase 1).
- The surrounding list emits ancestor keys for its elements (Phase 2).
- The planner constructs the join tree from paper Listing 2, walking the schema's nesting tree in preorder and selecting `joinSkAk` or `joinAkAk` depending on the relationship between predecessor and parent in the traversal.

For deeper nesting (arrays within arrays), each level's scan emits its own ancestor key columns referencing each ancestor level independently. A query accessing level 3 columns without requiring level 2 can skip level 2 entirely — the ancestor key at level 3 that references level 0 is computed directly from the repetition level without materialising any intermediate. This is the "level skipping" optimisation the paper identifies as significant for sparse access patterns. [§6.3, Figure 10]

**Dotted path syntax in the binder:**  
For a reference like `posts.comments.text`, the binder:

1. Identifies the schema node path `[posts, comments, text]`.
2. Determines the nesting levels and node groupings (paper §3.1 normalisation).
3. Constructs the initial join tree via paper Listing 2.
4. Injects the join tree into the logical plan before optimisation.

The query optimiser then treats the joins as standard joins, applying predicate pushdown, cardinality estimation, and join reordering without any awareness of their nested-data origin.

### 4.4 Merge join for sorted key columns

Surrogate keys are row numbers — strictly ascending. Ancestor keys are sorted within each parent entry's rows — ascending within each group, with group boundaries marked by rep_level transitions. Both join sides are therefore already sorted on the join key.

The join executor should detect key-generated join columns (flagged by a column attribute set during scan) and select merge join rather than hash join. The merge join implementation needs no separate sort phase, only the scan-merge step. The paper's microbenchmark (Figure 8) shows merge join is comparable to hash join for small datasets and outperforms for datasets exceeding CPU cache, which is the typical case for list-heavy analytical queries.

Opteryx's existing join infrastructure (`opteryx/operators/join/`) already has a merge join path. The planner change is to route key-generated joins to it.

---

## 5. Benefits to Opteryx

### 5.1 Closing a silent correctness gap

STRUCT columns are currently dropped without error. A user querying a Parquet file with a struct-typed column receives no indication that fields are missing. Phase 1 turns silently wrong results into correct results.

### 5.2 Performance on LIST queries

The current FSM in `_make_array_vector()` constructs an `ArrayVector` that wraps all list elements and their offsets. When the query only needs to filter on one element field, the entire ArrayVector is still materialised. With the key-based approach, selective predicates are pushed to the flat element scan before the join, and only matching element rows enter the join. The paper's results suggest this is the dominant performance factor for selective list queries. [§6.5]

For non-selective full-scan queries, the key generation adds measurable overhead (1.5–2.8× of raw scan, paper Table 2), but the unlock of join-level parallelism and query-optimiser visibility offsets this. The paper demonstrates that even for the worst-case full-scan workload (paper §6.1 Q4), the approach remains 10–45× faster than DuckDB and Trino.

### 5.3 No new operator surface area

The paper's central claim — and the reason it fits Opteryx's architecture — is that the approach introduces no new data types and no new operator logic. Every nested data access becomes a join over flat columns. Draken does not need StructVector. No operator needs `instanceof ArrayType` branches. The execution engine's hot paths are unchanged.

### 5.4 Query optimiser participation

With the FSM approach, the query optimiser cannot see inside a list scan. With the key-based approach, nested column filters are predicates on flat scalar columns. The existing predicate pushdown strategy (`predicate_pushdown.py`) will push them to the scan automatically. The existing join reordering strategy will choose build and probe sides based on cardinality estimates. These are zero-change benefits.

### 5.5 Trivial parallelisation over row groups

The paper's Listing 3 shows that ancestor key computation parallelises over row groups: the starting ancestor key for row group `r` is the sum of parent-column row counts across all preceding row groups, which is available from the Parquet footer metadata without reading any data. Rugo's existing row-group-parallel scan architecture (`parquet_io_parallelization_progress.md`) can adopt this directly.

---

## 6. What This Does Not Cover

**ORC files:** Rugo does not currently read ORC. The paper notes ORC encodes nesting as repetition counts rather than Dremel levels, requiring reading all ancestor levels to access any inner level. The paper's approach does not directly apply to ORC. [§7]

**MAP columns:** Maps require a key lookup step on top of the join-based framework and are not addressed in the paper. This design does not propose MAP support.

**Schema evolution:** If a Parquet file's nested schema changes between row groups, the column path → dotted-name mapping must be re-derived per row group. The paper does not address this; it is a separate concern.

**Sideways information passing:** The paper notes (§6.5) that for extremely selective predicates on outer levels, the FSM approach can skip inner-level row groups entirely, which the join-based approach cannot without additional "sideways information passing" — propagating outer-level filter hits to the inner-level scan. This is identified as a known gap and is not addressed in this design.

---

## 7. Implementation Sequence

1. **Phase 1 — STRUCT field emission from Rugo** (`decode_column.cpp`, `parquet_reader.pyx`): Recurse into struct schema nodes, emit leaf columns as dotted-path-named flat columns. Update binder to try dotted-path match before literal match. Populate `FlatColumn.fields` in the schema for IDE-style column completion.

2. **Phase 2 — LIST key generation** (`decode_column.cpp`, `parquet_reader.pyx`): Replace `_make_array_vector()` with a key-emission path. Update `CROSS JOIN UNNEST` to join on ancestor key columns. Verify `make q` passes and existing unnest tests are unchanged.

3. **Phase 3 — Join tree construction** (planner/binder): Implement paper Listing 2 for multi-level dotted path references. Inject synthetic join nodes into the logical plan. Route key-generated joins to merge join in the executor.

4. **Phase 4 — Merge join routing**: Flag key-generated join columns at scan time. Detect the flag in the join planner and select merge join.

Each phase is independently releasable and does not regress earlier phases.

---

## 8. Open Questions for the Architect

1. **LIST column backward compatibility:** The existing `ArrayVector` return type for LIST columns is used by application code that calls `to_pylist()` on morsel columns. Phase 2 changes the physical representation. Should the `ArrayVector` be reconstructed at the morsel boundary for external callers, or should this be a breaking change to the column API?

2. **Dotted-name collision:** If a Parquet file has a top-level column named `address_city` and also a struct column `address` with field `city`, both would emit as `address.city` and `address_city` respectively (or depending on naming convention). Is the dotted-path naming convention an acceptable canonical form, or should struct field columns carry a different disambiguation marker?

3. **UNNEST syntax vs implicit join:** With Phase 3, a user can write `SELECT posts.comments.text FROM events` without an explicit `CROSS JOIN UNNEST`. This is more ergonomic but changes the cardinality semantics silently (the result has one row per comment, not per event). Is implicit unnest via dotted path acceptable, or should dotted access on array-typed paths remain an error requiring explicit `UNNEST`?

4. **Scope:** The paper's primary target is production analytical workloads on data lakes. Opteryx runs against Mabel (GCS/S3 object stores). Row-group parallelism is the primary I/O unit. Does the estimated key computation overhead (1.5–2.8× of scan) remain acceptable at object store latencies, where scan cost is IO-dominated rather than CPU-dominated?
