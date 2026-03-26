# Transient Vector Index Design

**Date:** March 11, 2026  
**Status:** In Progress  
**Goal:** Replace the current hashed-token pseudo-vector path with a real native vector retrieval path built for query-lifetime use, not long-term storage.

---

## Executive Summary

Opteryx currently has a text-token normalization and hashed-signature path in
`opteryx/compiled/functions/vectors.pyx`, but it does not have a real vector
index.

This design introduces a native vector retrieval subsystem with these
properties:

1. embedding generation is native C/C++, not Python
2. nearest-neighbor indexing is native C/C++, not Python
3. indexes are transient and are expected to live only for the duration of a
   query or operator phase
4. the first target is fast local semantic retrieval, not long-term index
   persistence

Recommended initial stack:

1. embedding model: `sentence-transformers/all-MiniLM-L6-v2`
2. tokenizer + inference runtime: native ONNX Runtime C++
3. vector index: `USearch` C++ API
4. metric: cosine
5. vector dtype in the index: `float16`
6. vector dimension: 384

The key design choice is that this is an execution primitive, not a storage
format. The index should be cheap to build, cheap to search, and cheap to
discard.

Current implementation status:

1. SQL-visible `COSINE_SIMILARITY` and `COSINE_DISTANCE` exist.
2. `ORDER BY` can evaluate functional sort expressions, not only identifiers.
3. `ORDER BY COSINE_DISTANCE(...) LIMIT k` and `ORDER BY COSINE_SIMILARITY(...) LIMIT k`
   have a native vector top-k execution path.
4. Exact vector top-k is the default execution path for transient queries.
5. Transient `USearch` exists as an experimental opt-in path for planner-recognized
   nearest-neighbor top-k query shapes, but it is not the default.
6. `EMBED(...)` now defaults to a native MiniLM provider when the vendored
   ONNX Runtime SDK and model assets are present.
7. Custom providers can still override the default provider registration.
8. `MATCH() AGAINST()` now executes as a semantic embedding predicate using
   `EMBED(...)` + cosine thresholding.

---

## Problem

The current `vectors.pyx` code provides:

1. tokenization
2. lowercasing
3. punctuation stripping
4. stop-word removal
5. lightweight lemma normalization
6. a 1024-d hashed token signature

This is useful as a lexical heuristic, but it is not a real vector retrieval
system.

Current limitations:

1. no learned embeddings
2. no exact or ANN vector index
3. no native top-k nearest-neighbor search over dense vectors
4. no explicit filtered vector search primitive
5. cosine similarity is computed over a collision-prone hashed token signature,
   not over semantic embeddings

That means the current path is better understood as token normalization plus a
cheap text-similarity heuristic. It is not a credible long-term design for
semantic retrieval inside a database engine.

---

## Core Constraint

The index is transient.

That materially changes the design.

This is not a document about:

1. long-term on-disk ANN indexes
2. background index maintenance
3. durable vector storage formats
4. offline compaction
5. cross-session persistence

This is a document about a disposable execution structure:

1. build within a query
2. search within a query
3. destroy when the query finishes

That makes this design much closer to hash-join build-side state or group-by
state than to a durable search service index.

---

## Design Principles

1. Native first. The engine implementation must be C/C++ and callable from
   Python, not implemented in Python.
2. Build-use-burn. The index is an execution primitive.
3. Small hot working set. Favor compact vectors and compact index state.
4. Fast startup over maximal recall. Because the index is transient, build cost
   matters more than it would for a persistent service.
5. Clear layer boundaries. Embedding generation is not indexing. Indexing is
   not SQL planning.
6. Preserve optional lexical behavior separately. Existing token-based matching
   should not be silently redefined as semantic search.

---

## Goals

1. Replace the current pseudo-vector cosine path with a real dense-vector path.
2. Keep embedding generation fully local.
3. Keep the hot path fully native.
4. Integrate with the existing extension/build system cleanly.
5. Support top-k nearest-neighbor search over query-lifetime indexes.
6. Support constrained search over a subset of row ids when a relational
   predicate has already narrowed the candidate set.
7. Keep build latency low enough that transient use is defensible.

---

## Non-Goals

1. Persisting indexes across sessions by default
2. Building a full text-search engine
3. Replacing all lexical search behavior with semantic search
4. Implementing transformer inference in Cython itself
5. Supporting arbitrary user-supplied Python embedding functions in the engine
6. Solving multimodal retrieval in the first version

---

## Recommended Native Stack

### Embedding Model

Use `sentence-transformers/all-MiniLM-L6-v2` as the initial embedding model.

Current implementation note:

1. the engine now ships a native MiniLM embedding path built on ONNX Runtime C++
2. the default model assets are vendored under `third_party/models/all-MiniLM-L6-v2`
3. `EMBED(...)` no longer falls back to the legacy hashed-token path from
   `vectors.pyx`

Why:

1. small and fast
2. 384 embedding dimensions
3. good enough retrieval quality for an engine-first baseline
4. easy to pair with transient indexing because vector size and inference cost
   are modest

Here, "dimension" means the width of the output embedding vector.

That is not the same thing as model size.

Examples:

1. `all-MiniLM-L6-v2` produces one 384-wide embedding per text input
2. a "3 billion parameter" model refers to the number of learned weights in the
   model, not the width of its output embedding vector

Those are different measurements and the term is overloaded in casual
discussion. In this document, "dimension" always means embedding-vector width.

This is not the highest-quality local model available. It is the best fit for
the current priorities:

1. fully local
2. speed over quality
3. easy integration with a native vector index

### Tokenization and Inference

Use ONNX Runtime C++ for model inference.

Tokenizer handling should also remain native. The practical options are:

1. ONNX Runtime Extensions with a Hugging Face-compatible tokenizer path
2. a vendored native tokenizer compatible with the selected model assets

The engine should not depend on the Python `sentence-transformers` stack.

### Index

Use the `USearch` C++ API directly.

Why:

1. native C++ integration
2. cosine distance support
3. compact scalar storage support
4. filtered search support at the native layer
5. very low integration overhead compared with building ANN in-house

For transient use, `USearch` is attractive because it gives immediate access to
fast exact/ANN search without forcing the project to invent index mechanics
before validating product value.

---

## Why `vectors.pyx` Still Has Value

`vectors.pyx` is not useless. It still contains logic that may remain relevant
even after semantic retrieval is added:

1. lexical tokenization
2. stop-word removal
3. basic normalization
4. tokenization and normalization logic that may still support explicit lexical features

What should change is the role of that file.

It should no longer be treated as the engine's vector retrieval strategy.

Instead:

1. token-based lexical search remains a lexical path
2. embedding-based search becomes a semantic path
3. the two should be exposed and optimized separately

---

## High-Level Architecture

```text
SQL query
  -> planner identifies vector-search operator
  -> upstream relational filters narrow candidate rows
  -> embedding generator converts query text to dense vector
  -> transient vector index builds over candidate row embeddings
  -> top-k search executes, optionally with native candidate filtering
  -> results flow back into relational plan
  -> index is destroyed
```

There are two distinct build/search modes.

### Mode A: Query-Time Build Over Existing In-Memory Embeddings

This is the preferred first mode.

Flow:

1. source rows already have dense embeddings available in the current execution
   state
2. operator reads candidate vectors
3. transient `USearch` index is built for those candidates
4. query vector is embedded natively
5. nearest-neighbor search runs over the transient index

This avoids query-time document embedding generation for the candidate rows.

### Mode B: Fully Dynamic Build From Raw Text

Flow:

1. source rows contain raw text
2. query operator embeds all candidate text rows at runtime
3. transient `USearch` index is built
4. query vector is embedded
5. nearest-neighbor search runs

This is much more expensive and should not be the default design center.

If Mode B is required, it should be treated as a fallback or explicitly slower
path.

---

## Initial Operator Shape

The engine needs a native operator primitive with a shape roughly like:

```text
build_transient_vector_index(
    row_ids,
    vectors,
    metric=cosine,
    dtype=float16,
)

search_transient_vector_index(
    query_vector,
    k,
    candidate_filter=None,
)
```

Recommended native responsibilities:

1. validate vector dimension and dtype
2. optionally normalize input vectors for cosine search
3. add rows into `USearch`
4. execute top-k search
5. support an optional candidate row-id filter
6. return row ids and distances

The SQL planner and binder should remain outside this native module.

---

## Build Strategy For Transient Use

Transient lifetime changes the tradeoffs significantly.

For a durable search service, it is often acceptable to spend substantial time
building an index if it will be reused many times.

For query-lifetime use, that is usually wrong.

Initial strategy:

1. prefer small candidate sets
2. build after relational pruning, not before
3. avoid indexing rows that have already been excluded by non-vector predicates
4. favor low-overhead insertion paths over sophisticated long-lived tuning

Implication:

The main question is not "what is the best ANN configuration in isolation?".

The main question is "what build + search strategy wins when amortized over one
query execution?".

---

## Candidate Filtering

This is likely critical.

Many database queries will already have:

1. partition pruning
2. predicate pushdown
3. workspace/table restrictions
4. time/window filters
5. join-derived candidate restrictions

The vector operator should exploit those reductions.

The preferred execution order is:

1. relational pruning first
2. transient vector index build second
3. vector search third

If a smaller candidate set is already known, the native vector module should
accept it directly rather than build an index over the full relation.

This is one of the strongest reasons to integrate `USearch` natively rather than
through a thin Python wrapper.

---

## Data Representation

### Embedding Vector Type

Recommended first-pass representation:

1. inference output: `float32`
2. search input to `USearch`: `float16`

Recommended engine type model:

1. semantic embeddings should become a first-class vector type in the engine,
   not remain "generic arrays with conventions"
2. the native execution representation should be a dedicated `VectorVector`
3. Arrow interchange should use fixed-size lists of floats, not variable-length
   generic arrays

Recommended Arrow physical representation:

```text
FixedSizeList<float32, N>
```

For example, a 384-d embedding column should be represented in Arrow as:

```text
FixedSizeList<float32, 384>
```

Rationale:

1. keep inference numerics straightforward
2. reduce memory footprint in the transient index
3. improve cache behavior
4. reduce build/search bandwidth cost
5. preserve a fixed dimension contract for native kernels
6. avoid treating semantic vectors as arbitrary nested arrays

If recall degradation is unacceptable, fall back to `float32`.

Rationale for a dedicated `VectorVector`:

1. embeddings have stronger invariants than generic arrays
2. vector distance kernels want contiguous `rows x dims` numeric storage
3. fixed dimension should be validated at construction time
4. downstream execution should not need to rediscover that an `ArrayVector`
   "happens to be an embedding"

This does not require an Arrow extension type immediately. The practical first
step is:

1. Arrow/parquet boundary uses `FixedSizeList<float32, N>`
2. Draken/native execution decodes that into `VectorVector`
3. vector kernels consume `VectorVector`

### Row Identity

The index payload should be a `uint64_t` row identifier or execution-local row
reference.

This should not be a Python object handle.

### Memory Ownership

The native module should own:

1. the index instance
2. any compact vector buffers used to feed the index
3. any optional filter bitmaps or row-id lists

The Python boundary should receive only:

1. arrays/memoryviews for input
2. row-id and distance outputs

---

## Native Module Boundary

Two viable integration patterns exist in this repo:

1. Cython wrapping a C API
2. nanobind wrapping a C++ API

Recommendation:

1. use ONNX Runtime C++ directly for embedding generation
2. use `USearch` C++ directly for indexing
3. expose a small nanobind module for the transient vector operator

Why nanobind is the better fit here:

1. the repo already uses it
2. both ONNX Runtime and `USearch` are more naturally consumed from C++
3. the boundary should stay thin
4. this avoids writing a custom C shim only to re-wrap it in Cython

Cython is still reasonable for leaf kernels. It is a less natural fit for a
composed subsystem built around external C++ libraries.

---

## Execution Modes

### 1. Brute-Force Baseline

Before adding ANN tuning, implement an exact baseline.

This should:

1. embed query text
2. scan candidate embeddings
3. compute cosine similarity
4. keep top-k

Why do this first:

1. correctness baseline
2. performance baseline
3. helps prove that transient ANN build cost is justified

It is entirely possible that for small candidate sets, exact search will beat
transient ANN.

### 2. Transient `USearch` Mode

Then add ANN/exact search via transient `USearch`.

This should be preferred only when:

1. candidate count is large enough
2. build cost is amortized by search cost reduction

The planner or operator may eventually need a threshold such as:

1. exact path below `N`
2. transient `USearch` at or above `N`

The threshold must be measured, not guessed.

---

## Planner and SQL Surface

This document does not fully define the SQL syntax.

However, the architecture assumes some way to express:

1. query text or query embedding
2. target embedding column or vector-producing expression
3. optional top-k
4. optional similarity threshold

Current product direction:

`MATCH() AGAINST()` has been deliberately repurposed as a semantic predicate
implemented with embeddings and cosine thresholding. Explicit lexical behavior,
if preserved, should live behind separate lexical function names.

---

## Expected Benefits

1. real semantic retrieval instead of hashed-token approximation
2. native execution throughout the hot path
3. lower memory and latency than heavier local embedding models
4. direct compatibility with transient execution-state design
5. room to evolve later toward persistent vector storage if the product ever
   needs it

---

## Risks

### 1. Transient Build Cost May Dominate

This is the primary risk.

If the candidate set is too small, building a transient index may be slower than
exact search.

Mitigation:

1. build exact baseline first
2. benchmark threshold crossover
3. choose execution strategy by candidate size

### 2. Tokenizer Complexity

Native tokenizer integration may be more awkward than index integration.

Mitigation:

1. keep tokenizer/model assets version-locked
2. build a narrow embedding API
3. avoid making tokenizer internals part of general engine APIs

### 3. Semantic/Lexical Confusion

Users may expect current token matching semantics to stay unchanged.

Mitigation:

1. keep lexical and semantic paths distinct
2. expose clear SQL/operator names
3. document behavior differences explicitly

### 4. Model Upgrade Churn

Changing embedding models later changes retrieval behavior and possibly vector
shape.

Mitigation:

1. define the vector operator around abstract dimension/metric constraints
2. isolate model-specific code behind a native embedding interface

---

## Implementation Plan

The implementation should proceed in stages with measurable exit criteria. The
goal is to avoid overcommitting to ANN machinery before proving that it beats a
simple exact path for transient workloads.

### Stage 0: Measurement Harness

Build a benchmark harness before changing execution semantics.

Deliverables:

1. a native benchmark for exact cosine top-k over dense vectors
2. a native benchmark for transient `USearch` build + search
3. a benchmark dataset generator for candidate sets in the thousands,
   hundreds of thousands, and millions
4. benchmark outputs for:
   - total operator latency
   - rows per second
   - peak memory
   - index build time
   - query embedding time
   - top-k search time

Validation gates:

1. benchmark can run outside SQL/planner integration
2. exact baseline numbers are stable across repeated runs
3. transient `USearch` numbers are captured separately for build and search

### Stage 1: Native Embedding Subsystem

Implement the embedding generator as a standalone native component.

Scope:

1. vendor or pin model assets for `all-MiniLM-L6-v2`
2. add native tokenizer support
3. add ONNX Runtime C++ inference
4. implement mean pooling and L2 normalization natively
5. expose a narrow callable interface

Recommended interface:

```text
embed_text(text) -> float32[384]
embed_text_batch(texts) -> float32[n,384]
```

Deliverables:

1. one native module for embedding generation
2. deterministic asset loading rules
3. tests with fixed inputs and tolerances against known embeddings

Validation gates:

1. single-text inference works
2. batch inference works
3. output shape is always 384
4. normalization is correct within tolerance
5. startup and per-text latency are measured

### Stage 2: Exact Vector Search Baseline

Implement the simplest correct dense-vector search path first.

Scope:

1. accept precomputed query embeddings
2. accept candidate embeddings as contiguous buffers
3. compute cosine similarity exactly
4. maintain top-k without a full sort if possible
5. return row ids plus scores or distances

Recommended interface:

```text
exact_search(
    query_vector,
    candidate_row_ids,
    candidate_vectors,
    k,
)
```

Deliverables:

1. native exact-search kernel
2. correctness tests against a simple reference implementation
3. benchmark numbers across candidate sizes

Current implementation status:

1. native nanobind module added as `opteryx.compiled.nanobind.vector_search`
2. exact cosine top-k entry point added as `exact_search_cosine`
3. focused correctness tests added for ordering, zero-vector handling, and
   dimension mismatch
4. initial micro-benchmark harness added for candidate-size sweeps

Validation gates:

1. ranking correctness is verified
2. throughput is measured at operator granularity
3. exact search establishes the baseline crossover target for ANN

### Stage 3: Native Transient `USearch` Wrapper

Only after Stage 2 should the ANN wrapper be added.

Scope:

1. vendor `USearch`
2. create a transient index wrapper around the C++ API
3. support build, search, and destroy within one operator lifetime
4. support candidate filtering where the candidate set is already known
5. support `float16` and `float32` index storage

Recommended interface:

```text
create_index(dim, metric, dtype)
add_batch(index, row_ids, vectors)
search_index(index, query_vector, k)
destroy_index(index)
```

Optional v1 extension:

```text
search_index(index, query_vector, k, candidate_filter)
```

Deliverables:

1. one native transient-index module
2. lifecycle tests for create/build/search/destroy
3. benchmark comparisons versus Stage 2 exact search

Current implementation status:

1. vendored `USearch` added under `third_party/usearch`
2. native nanobind wrapper added as `opteryx.compiled.nanobind.usearch_native`
3. initial API supports constructor, reserve, add, add_batch, search, size,
   capacity, dimensions, and memory_usage
4. focused unit tests compare `USearch` exact search to the Stage 2 exact
   baseline on small datasets
5. initial benchmark harness compares exact search versus transient `USearch`
   build plus search

Initial measurement:

1. at 1,000 candidate rows and 384 dimensions, transient `USearch` build plus
   search is materially slower than the exact baseline
2. this is expected and reinforces the requirement that ANN selection be driven
   by measured crossover points, not by assumption
3. lowering `USearch` expansion settings reduces build cost materially, but
   build time still dominates total latency at small candidate sizes
4. for example, with `expansion_add=16` and `expansion_search=16`:
   1. at 1,000 rows, exact search is about 0.57 ms while transient `USearch`
      build plus search is about 182 ms
   2. at 5,000 rows, exact search is about 2.93 ms while transient `USearch`
      build plus search is about 1,629 ms

Validation gates:

1. no persistent storage assumptions leak into the design
2. build/search/destroy work safely across repeated invocations
3. transient `USearch` beats exact search at some measured candidate size
4. memory growth is bounded and released on teardown

### Stage 4: Execution-Strategy Selector

Add a strategy layer that chooses exact scan or transient ANN based on measured
workload shape.

Scope:

1. choose exact search below the crossover point
2. choose transient `USearch` above the crossover point
3. make the threshold configurable for testing
4. collect execution counters for later tuning

Deliverables:

1. strategy-selection logic in the operator path
2. telemetry for:
   - candidate count
   - exact path chosen
   - transient ANN path chosen
   - build time
   - search time

Validation gates:

1. strategy choice is deterministic for a given threshold
2. both paths return the same ordering within acceptable tolerance
3. the threshold is based on measurements, not a fixed guess

Current implementation status:

1. exact-versus-ANN selection now exists in the vector top-k operator path
2. `USearch` is currently gated by:
   - planner-recognized nearest-neighbor top-k query shape
   - a candidate-row threshold
3. telemetry now records:
   - exact vector top-k usage
   - `USearch` vector top-k usage
   - rows indexed into transient `USearch`
   - `USearch` fallback count
4. candidate-count crossover is still heuristic and needs refinement

### Stage 5: Operator Integration

Integrate the native pieces into the execution engine.

Scope:

1. add a vector-search execution primitive or physical operator
2. ensure relational pruning happens before vector-index build
3. wire text query embedding into the operator
4. pass candidate row ids and candidate embeddings through a native boundary
5. return row ids and scores back into the relational plan

Deliverables:

1. one operator-level integration path
2. integration tests using real candidate data
3. explain-plan visibility if appropriate for the engine

Validation gates:

1. operator works on realistic query shapes
2. candidate pruning occurs before index build
3. operator-local memory is released at completion

Current implementation status:

1. `HeapSortNode` contains the current vector top-k execution path
2. `SortNode` can evaluate functional sort expressions before ordering
3. planner/operator fusion now explicitly marks nearest-neighbor top-k shapes
4. end-to-end SQL tests cover the vector top-k route
5. filtered vector top-k over relationally pruned candidates is not yet
   implemented as a distinct execution step

### Stage 6: SQL Surface

Once the operator is stable, define the SQL shape explicitly.

Scope:

1. add explicit semantic-search syntax or functions
2. keep lexical search separate
3. define top-k semantics
4. optionally define threshold semantics if needed

Deliverables:

1. binder/planner support for semantic vector search
2. documentation for semantic predicate behavior versus explicit lexical functions
3. conformance tests for syntax and semantics

Validation gates:

1. SQL semantics are explicit
2. `MATCH() AGAINST()` semantic threshold behavior is explicit and test-covered
3. planner/operator integration is test-covered

### Stage 7: Hardening

After functional integration, harden the path for production use.

Scope:

1. failure handling for missing model assets
2. OOM and allocation-failure handling during transient builds
3. repeated-query stability
4. concurrency behavior under multiple operator instances
5. telemetry and profiling hooks

Deliverables:

1. stress tests
2. leak and lifetime checks
3. operator-level performance report

Validation gates:

1. no obvious memory leaks across repeated runs
2. concurrent use is correct for the intended ownership model
3. end-to-end throughput meets or exceeds the target defined in this document

### Suggested Build Order

The implementation order should be:

1. Stage 0
2. Stage 1
3. Stage 2
4. benchmark and establish crossover
5. Stage 3
6. Stage 4
7. Stage 5
8. Stage 6
9. Stage 7

This ordering is important. If Stage 2 exact search already meets the workload
target for the dominant candidate-size range, Stage 3 may be deferred or scoped
down.

---

## Current Decisions

The following points are treated as working decisions for v1.

1. Embeddings are not expected to be stored durably.
2. The design target is candidate sets in the millions, not just thousands.
3. The exact-vs-ANN choice is a performance decision, not a doctrinal one.
4. The initial interface should accept text queries only.
5. The first version can be English-only.
6. Speed and native integration are more important than absolute embedding
   quality in the initial model choice.
7. `MATCH() AGAINST()` is part of the semantic vector path for v1.
8. The transient index is initially owned by one operator instance and discarded
   when that operator completes.
9. `float16` is the preferred initial index storage type for a 384-d embedding,
   with `float32` kept as the fallback validation mode.
10. Semantic embeddings should move toward a dedicated engine-native
    `VectorVector` type instead of continuing as generic `ArrayVector`.
11. The Arrow interchange/storage representation for embeddings should be
    `FixedSizeList<float32, N>`.

---

## Remaining Open Questions

1. What performance target should govern the execution-strategy switch between
   exact scan and transient ANN?

Recommended initial target:

1. sustain at least 1 million candidate rows per second end-to-end on the
   vector-search path, measured at the operator boundary
2. use exact scan below the crossover point
3. use transient `USearch` only once it beats exact scan for the measured
   workload shape

2. Do we need vector-distance threshold filtering in addition to top-k in v1?

Recommendation:

1. support top-k first because it defines stable operator output cardinality
2. add threshold filtering only if a real query shape needs it
3. if both are supported, define the order explicitly as "apply threshold, then
   return top-k of survivors" or "take top-k, then apply threshold"

3. When should the engine construct `VectorVector` natively instead of passing
   through generic array representations?

Recommendation:

1. introduce `VectorVector` when table-backed embedding columns are added as a
   real engine feature
2. keep Arrow physical storage as `FixedSizeList<float32, N>`
3. avoid expanding vector semantics across the generic `ArrayVector` path any
   further than necessary

---

## Recommended Next Work

The next useful engine work is not more ANN scaffolding. It is feature
completion around the existing top-k path.

### 1. Filtered Vector Top-K

Implement the production query shape:

```text
WHERE relational_filters ...
ORDER BY COSINE_DISTANCE(vector_col, query_vector)
LIMIT k
```

Execution order should be:

1. relational pruning
2. candidate embedding extraction
3. exact or transient `USearch` over only the surviving candidates

This is the highest-leverage next step because ranking the entire relation is
not the dominant database workload shape.

### 2. Table-Backed Vector Columns

Add explicit support for real embedding columns stored as:

```text
FixedSizeList<float32, N>
```

This work should include:

1. Arrow/parquet boundary validation
2. native Draken decoding into `VectorVector`
3. SQL tests over real table-backed vector columns, not only literals and
   inline `VALUES`

### 3. Planner Costing Refinement

Refine the exact-vs-`USearch` decision using:

1. candidate count
2. vector dimension
3. measured operator timings

The planner/operator should eventually choose the ANN path based on measured
workload shape rather than a fixed threshold alone.
