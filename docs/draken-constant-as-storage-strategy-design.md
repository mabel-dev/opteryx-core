# Draken Vector Type System: Constant as a Storage Strategy

## Status
Draft. Iterations 1-21 are complete.

## Current Progress Notes

What is complete so far:

1. the encoding enum and base vector contract now support constant encoding
2. `Int64Vector` supports native constant encoding
3. `Float64Vector` supports native constant encoding
4. `StringVector` supports native constant encoding
5. `StringVector` now preserves dictionary metadata and reports dictionary encoding for dictionary-backed construction paths
6. the remaining native scalar vectors now support typed constant encoding:
   - `BoolVector`
   - `Date32Vector`
   - `TimeVector`
   - `TimestampVector`
   - `IntegerVector`
7. literal projection/evaluation now emits typed constant-encoded vectors when the output type is known
   - the first producer path is `evaluate_and_append(...)` on morsels
   - legacy `ConstantVector` remains available as a fallback for unsupported or truly untyped cases
8. expression coercion helpers now recognize typed constant encoding as a constant shape
   - they no longer rely only on the legacy `ConstantVector` class name
   - predicate fastpaths and evaluator coercion can unwrap typed constant vectors during migration
9. constant predicate fastpaths now accept typed constant encoding in `opteryx/expression/ops.py`
   - legacy `ConstantVector` is still supported
   - typed fixed-width and string constant vectors now share the same fastpath entry point
10. simple aggregate helpers now accept typed constant encoding in `opteryx/operators/draken_aggregate_node.py`
   - `sum`, `min`, and `max` no longer depend on the legacy `ConstantVector` class
   - global aggregate collection can consume typed constant vectors without materializing dense values first
11. `GroupStateStore` Python routing/telemetry checks now recognize constant encoding directly
   - wrapper-level output checks no longer rely on the legacy `ConstantVector` class name
   - this keeps Python-side routing/telemetry aligned with the encoding abstraction during migration
12. `GroupStateStore` compiled constant-key handling now supports both typed constant encoding and legacy `ConstantVector`
   - the compiled path uses `const_accessor()` for typed vectors and preserves the legacy constant path
   - wrapper routing now sends constant-key group-bys through the compiled constant path consistently
   - `hash_one` and the phase-1 constant-engine telemetry now match the constant fastpath contract
13. the direct Carchar constant mode now accepts typed constant encoding for single-aggregate workloads
   - typed constant keys can enter `MODE_CONSTANT` through `const_accessor()`
   - typed constant values are consumed correctly for `count`, `sum`, `min`, `max`, `avg`, and `hash_one`
   - typed all-null constant values now preserve null semantics in constant mode
14. CSV and JSON writer source paths now recognize typed constant encoding directly
   - the serializers no longer need a legacy `ConstantVector` instance for supported constant scalar exports
   - supported typed constant writer shapes now include fixed-width scalars and strings, including typed all-null constants
   - focused CSV/JSON writer validation is now passing for typed constant and typed all-null exports
15. DRKM spill/restore now preserves typed constant encoding for supported native scalar vectors
   - typed constant columns serialize through `const_accessor()` instead of the legacy `ConstantVector` pointer shape
   - DRKM restore now reconstructs typed constant vectors for supported physical dtypes while keeping legacy `ConstantVector` compatibility for untyped payloads
16. ingest-time normalization now converts some single-value encoded inputs directly into typed constant vectors
   - Arrow single-entry dictionaries now become typed constants when the column is uniformly valid or uniformly null
   - Arrow single-run REE / run-end encoded arrays now become typed constants when the run spans the full logical length
   - Parquet-native single-entry dictionary decode now becomes typed constant when nullness is uniform
17. shared scalar constant construction now prefers typed vectors over `ConstantVector`
   - `constant_vector.from_scalar(...)` now emits typed constant vectors for supported typed cases and only falls back to `ConstantVector` for untyped or mixed-null compatibility shapes
   - `vector_iif(...)` now normalizes its public inputs through that shared helper and accepts typed constant vectors in its constant branches
18. Python-side consumer dispatch now avoids most direct `ConstantVector` storage checks where they were no longer needed
   - predicate fastpaths, rounding dispatch, projection telemetry, aggregate helpers, and constant-wrapper helpers now prefer encoding- or behavior-based checks
   - the remaining explicit `ConstantVector` references are now concentrated in low-level compatibility/storage paths rather than routine consumer dispatch
19. `ConstantVector` has now been removed entirely
   - shared scalar construction now lives in `vectors/scalar_constructors.pyx`
   - writers, DRKM, group-by engines, and expression helpers now consume typed constant encoding only
   - repeated Python sequences only become typed constant when all rows are the same non-null value; mixed valid/null repeated inputs fall back instead of recreating row-level nullable constants

What we learned from Iterations 4-6:

1. the fixed-width model works well with:
   - one typed scalar sidecar
   - a vector-level `is_null` flag
   - `.encoding == DRAKEN_ENCODING_CONSTANT`
2. the initially scoped methods were not enough on their own
   - in practice `null_count`, `is_null`, `to_pylist`, `hash_into`, `compress_into`, and `__str__` also needed constant-aware behavior to keep vectors generally usable
3. `null_bitmap_ptr()` returning `NULL` for constant vectors remains a good v1 choice
   - it keeps row-level null semantics out of the encoding
   - all-null columns are still representable through vector-level `is_null`
4. Arrow expansion is a fine boundary behavior for v1
   - native constant storage stays inside Draken
   - Arrow export can materialize repeated values without undermining the design
5. constructor validation matters
   - negative lengths should be rejected
   - `value=None` should only be allowed with `is_null=True`
6. variable-width types need a typed payload, not just a scalar pointer
   - for `StringVector`, `ConstAccessor.value_ptr` needs to point to a `DrakenConstantStringPayload*`
   - that payload must carry both data and length
7. some string zero-copy APIs do not have a natural constant-backed shape
   - `buffers()`, `lengths()`, `view()`, and `c_iter()` are currently better treated as unsupported than as fake dense buffers
   - future iterations should decide deliberately whether these stay unsupported for constant encoding or gain a separate constant-aware contract
8. the current dictionary pattern is "dense materialization plus dictionary metadata", not dictionary-only physical storage
   - that is how the numeric vectors behave today
   - making `StringVector` match that pattern is the pragmatic fix before any future deeper storage rewrite
9. `IntervalVector` should not be forced into the same iteration as the simple scalars
   - it is a two-component typed value, not a single fixed-width scalar
   - it deserves its own design/implementation pass if constant encoding is needed there
10. after adding new Cython fields to vector classes, focused tests may pass before the full tree is rebuilt
   - that is enough to validate the vector behavior locally
   - but a full rebuild is still needed to eliminate extension layout warnings across the wider tree
11. producer-side migration works cleanly when it is anchored on schema type, not literal Python value
   - the output schema already tells us whether `1` means integer, double, date-part result, or some other typed literal
   - that keeps the typed constant constructor choice deterministic
12. temporal literal producers need an explicit scalar-to-physical coercion step
   - `datetime.date`, `datetime.datetime`, and `datetime.time` values should be converted through the target Arrow type first
   - the typed constant vectors still want their physical day / tick / microsecond representation internally
13. consumer migration should use encoding- and behavior-based checks rather than storage class names
   - the legacy `ConstantVector` class can coexist during migration
   - but new helpers should ask whether a vector is constant-encoded, then unwrap the scalar through a common path
14. predicate fastpaths should detect constant encoding before eager Arrow materialization
   - otherwise the constant fastpath disappears before it can run
   - the wrapper needs to branch on storage shape first, then materialize only for fallback paths
15. typed constant predicate support needs a generic compatibility layer during migration
   - not every typed vector exposes the exact legacy `ConstantVector` convenience methods
   - a small scalar-based constant fastpath is enough to bridge that gap for `Eq`/`NotEq`/`InList`/range comparisons
16. aggregate helpers need the same “constant-like” bridge as predicate/coercion code
   - global aggregation mostly needs scalar extraction plus valid-row count
   - once that helper exists, `SUM`/`AVG`/`MIN`/`MAX` become straightforward for typed constants
17. the Python group-by wrapper should use encoding-level checks even before the compiled backend is fully migrated
   - that lets telemetry and wrapper decisions converge early
   - the compiled constant-key consumption work can then land as a separate iteration without reworking the Python surface again
18. compiled Cython iterations can require a forced rebuild of the full extension tree
   - incremental `build_ext --inplace` is not always enough after class layout changes
   - validation should wait until stale binary-layout mismatches are cleared
19. `setup.py` does not currently track Draken vector `.pxd` files as dependencies for re-Cythonization
   - changing vector layout in a `.pxd` can leave checked-in generated `.cpp` files stale
   - for now, vector layout iterations should force re-Cythonization of the affected `.pyx` files before trusting rebuild/test results
20. constant-engine telemetry needs to remain distinct from the generic `GroupStateStore` fallback signal
   - the compiled constant fastpath is implemented inside `GroupStateStore`
   - but the user-facing phase telemetry expects `feature_groupby_engine_constant` to be exclusive when that path is used
21. Carchar constant support should be validated directly at the engine level
   - the Python wrapper now intentionally reroutes normal constant-key workloads to `GroupStateStore`
   - so Carchar constant-mode tests should instantiate the compiled engine directly
22. the existing `StringVector` layout warnings are still separate build debt
   - they do not block fixed-width constant validation in Carchar
   - but direct Carchar constant tests are currently more reliable on fixed-width keys until the string layout warnings are cleared
23. consumer dispatch must check `.encoding` before dense typed-vector `isinstance(...)` branches
   - constant-encoded `Int64Vector`, `Float64Vector`, `BoolVector`, and `IntegerVector` are still instances of their dense classes
   - if a consumer branches on class first, it can dereference `ptr.data` on a constant vector and crash instead of using `const_accessor()`
24. DRKM has the same dispatch rule as writers and evaluators
   - typed constant `StringVector` still has `dtype == DRAKEN_STRING`, and typed constant numerics still report their physical fixed-width dtype
   - storage paths must branch on `.encoding` before they branch on physical dtype, or typed constants get serialized as dense storage by mistake
25. ingest-time constant normalization must respect vector-level nullability
   - single-value dictionary or REE inputs can only become typed constant when the whole column is valid or the whole column is null
   - mixed row-level null patterns must stay on the existing dictionary / dense paths until there is a row-nullable constant design
26. ingest normalization should happen before dictionary materialization or generic fallback conversion
   - Arrow dictionary arrays should be checked before `dictionary_decode()`
   - Arrow REE arrays should be checked before `to_pylist()` fallback
   - Parquet decoded dictionaries should be checked before the dictionary-vs-dense storage decision
27. the generic scalar helper is the right migration seam for narrowing `ConstantVector`
   - changing `constant_vector.from_scalar(...)` lets projection, aggregate helper setup, and other scalar-producing paths benefit together
   - it also makes the remaining `ConstantVector` uses much easier to interpret as intentional compatibility cases
28. public compiled kernels should normalize their own scalar-like inputs at the boundary
   - `vector_iif(...)` had internal normalization logic already, but its public signature was still stricter than the behavior it wanted to support
   - moving normalization to the entrypoint keeps typed constant production and legacy scalar compatibility aligned
29. behavior-based detection is a good intermediate step before full legacy removal
   - for Python call sites, `encoding == constant` plus a small legacy-capability check (for example `scalar_value`) is enough to avoid spreading storage-class checks further
   - that lets the codebase converge on one constant-like surface even while low-level compatibility paths still exist
30. rounding was a useful migration canary because it crosses constant folding and runtime evaluation
   - the constant-aware branch belongs at the function boundary, not inside the dense rounding kernel
   - keeping the dense kernel dense avoided pushing a constant return shape through a path that did not need to own it
31. removing the class entirely became straightforward once scalar creation had its own home
   - `from_scalar(...)` / `from_sequence(...)` were the reusable part of the old module
   - moving them to a neutral helper module made the final removal mostly a mechanical import cleanup
32. mixed valid/null repeated Python sequences are worth keeping out of typed constant v1
   - preserving those shapes would just recreate row-level nullable constant semantics under a different name
   - falling back for those inputs keeps the typed constant contract simple and honest

What this changes for future iterations:

1. future vector ports should assume they need more than `from_constant`, `const_accessor()`, `__getitem__`, `take()`, and `to_arrow()`
2. each port should at minimum check:
   - null reporting
   - Python list conversion
   - hashing/compression helpers
   - debug/string representation
3. `StringVector` needs one extra convention not present in fixed-width vectors:
   - `ConstAccessor.value_ptr` should point to a `DrakenConstantStringPayload*`
   - consumers must interpret string constant payloads through that struct, because raw bytes alone are insufficient without a length
4. future consumer-facing iterations should audit whether they rely on dense-only helpers
   - if they do, they should switch to `const_accessor()`/`dict_accessor()`/`encoding`
   - they should not force typed constant vectors to materialize fake dense storage just to satisfy legacy helper APIs
5. string dictionary work deserves its own explicit roadmap item
   - it is adjacent to constant encoding because it uses the same accessor/discriminant seams
   - it should be completed before we assume string storage behavior matches the fixed-width vectors
6. producer iterations should prefer schema-driven typed construction over value-driven inference
   - that avoids accidentally creating the wrong typed constant vector for ambiguous Python literals
7. consumer iterations should centralize constant unwrapping
   - repeated `__class__.__name__ == "ConstantVector"` checks are brittle
   - a shared “constant-like” helper keeps legacy and typed constant storage compatible during rollout
8. when migrating fastpaths, parity tests should cover:
   - legacy constant vectors
   - typed fixed-width constants
   - typed all-null constants
   - typed string constants
9. aggregate helper tests should cover both direct helper behavior and collector-level behavior
   - helper-only tests catch scalar/count math quickly
   - collector tests confirm the real aggregate flow is using those helpers correctly
10. wrapper-level group-by tests should separate:
   - Python detection/telemetry behavior
   - compiled backend constant-key execution behavior
   so Iteration 13 can land cleanly before Iteration 14 updates the compiled fast path

## Why This Exists

The recent dictionary-encoding work moved Draken in the right direction:

1. vectors are becoming data-typed again (`Int64Vector`, `Float64Vector`, `StringVector`)
2. encoding is becoming a storage detail exposed through accessors
3. hot paths can branch once per morsel on encoding instead of repeatedly checking storage classes

That work makes constant encoding the natural next step.

Today constants still live mostly behind `ConstantVector`, which has the same core problem `DictionaryVector` had:

1. it is storage-typed, not data-typed
2. callers must special-case it everywhere
3. every consumer has to rediscover the real value type before it can do useful work

The goal of this design is to let typed vectors carry constant storage directly, so callers can keep dispatching on data type and only opt into constant-aware fast paths when they care.

## Review of the Dictionary-Encoding Work

The dictionary track got the important architectural pieces right.

### What Landed Well

1. Typed vectors gained `from_dict(...)` constructors instead of overloading `from_arrow(...)`.
2. Consumers can ask vectors for storage-specific access through `dict_accessor()`.
3. Dense access is explicit through `dense_ptr()` and `null_bitmap_ptr()`.
4. `.encoding` introduced the right top-level idea: branch on storage once, not by concrete class.
5. Group-by, writers, and function paths have already started moving away from `isinstance(..., DictionaryVector)`.

Those are exactly the seams constant encoding should use as well.

### What The Dictionary Work Exposed

The remaining rough edges are useful lessons for constant encoding.

1. `DictionaryVector` still exists, so the old storage-typed model has not been fully retired.
2. `.encoding` is currently effectively "dictionary or dense"; it is not yet a general storage discriminant.
3. Some paths still dispatch on concrete storage classes rather than accessors.
4. String dictionary support took longer because one storage model was still trying to fit multiple physical shapes awkwardly.

The takeaway is simple: constant encoding should not become "another special vector". It should become another encoding exposed by typed vectors.

## Problem

`ConstantVector` is currently a separate type whose real meaning is:

"this column has one repeated value for `length` rows"

That is storage information, not logical type information.

This causes repeated branching like:

```python
if value.__class__.__name__ == "ConstantVector":
    ...
elif value.__class__.__name__ == "Int64Vector":
    ...
```

and:

```cython
if isinstance(value_vector, ConstantVector):
    ...
elif (<Vector> value_vector).dict_accessor() != NULL:
    ...
else:
    ...
```

This is now spread across:

1. expression coercion and predicate fast paths
2. aggregate collectors
3. Carchar and GroupStateStore group-by paths
4. CSV / JSON writers
5. projection and evaluation plumbing

That is exactly the failure mode the dictionary redesign was trying to eliminate.

## Core Principle

Vectors remain data-typed.

Encoding is a storage detail.

Constant storage should therefore be represented as:

1. `Int64Vector` with constant encoding
2. `Float64Vector` with constant encoding
3. `StringVector` with constant encoding
4. `BoolVector`, `Date32Vector`, `TimestampVector`, and other typed vectors with constant encoding

not as a separate untyped `ConstantVector` that callers have to peel apart.

## Proposed Encoding Model

Extend the storage discriminant:

```cython
ctypedef enum DrakenEncoding:
    DRAKEN_ENCODING_DENSE      = 0
    DRAKEN_ENCODING_DICTIONARY = 1
    DRAKEN_ENCODING_RLE        = 2
    DRAKEN_ENCODING_CONSTANT   = 3
```

The important semantic split is:

1. dense: one physical value per row
2. dictionary: one code per row, values in a side dictionary
3. constant: one physical value for the whole vector
4. scalar decode fallback: `vec[i]`, always correct, never the hot path

`ConstantVector` should stop being the public execution abstraction. Constant becomes just another encoding that typed vectors may expose.

## Accessor Pattern

Mirror dictionary access with a constant accessor.

```cython
cdef struct ConstAccessor:
    size_t      length
    DrakenType  value_type
    void*       value_ptr
    uint8_t     is_null
```

Properties:

1. `length` is the logical row count.
2. `value_type` is the typed vector's logical type.
3. `value_ptr` points to the single stored value payload.
4. `is_null` means the constant represents SQL NULL for every row.
5. when `is_null != 0`, `value_ptr` is ignored and may be `NULL` or a type-valid placeholder

Base vector API becomes:

```cython
cdef class Vector:
    cdef DictAccessor* dict_accessor(self) noexcept
    cdef ConstAccessor* const_accessor(self) noexcept
    cdef void* dense_ptr(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept
```

and `.encoding` should become a real discriminant, not an inferred dictionary-vs-dense helper.

## Typed Storage Layout

### Fixed-Width Vectors

For fixed-width typed vectors, add optional constant sidecar state directly to the vector class, alongside the existing optional dictionary sidecar.

Conceptually:

```cython
cdef class Int64Vector(Vector):
    cdef DrakenFixedBuffer* ptr          # existing dense storage
    cdef int64_t _const_value
    cdef bint _has_const
    cdef bint _const_is_null
    cdef ConstAccessor _const_accessor
```

Equivalent fields would exist for other fixed-width vectors with their native scalar type.

Behavior:

1. dense vectors keep using `ptr.data`
2. constant vectors leave `ptr.data == NULL` or unused
3. `ptr.length` remains authoritative for logical row count
4. `null_bitmap_ptr()` returns `NULL` for constant encoding in v1
5. `_const_is_null` means the whole typed vector is SQL NULL without any row bitmap

This keeps the vector data-typed while making constant storage explicit and cheap.

### String Vectors

Strings need one owned payload rather than a row buffer. Reuse the existing constant string payload pattern, but hang it off `StringVector` constant storage instead of a separate `ConstantVector`.

Conceptually:

```cython
cdef class StringVector(Vector):
    cdef DrakenVarBuffer* ptr
    cdef DrakenConstantStringPayload* _const_value
    cdef bint _has_const
    cdef bint _const_is_null
    cdef ConstAccessor _const_accessor
```

This lets string kernels stay type-directed while still getting O(1) constant storage.

## Constructors

Each typed vector should gain an explicit constant constructor:

```cython
Int64Vector.from_constant(value, length, is_null=False)
Float64Vector.from_constant(value, length, is_null=False)
StringVector.from_constant(value, length, is_null=False)
Date32Vector.from_constant(value, length, is_null=False)
...
```

Rules:

1. constructor validates and coerces exactly once
2. constructor stores one value payload plus length
3. constructor sets `.encoding == DRAKEN_ENCODING_CONSTANT`
4. constructor never materializes a dense row buffer
5. constructor accepts an `is_null` flag for typed all-null columns

This parallels the new `from_dict(...)` model and keeps Arrow import separate from storage constructors.

## `__getitem__`, `take`, and Arrow Export Semantics

### `__getitem__`

Typed vectors decode by encoding:

1. dense: return row `i`
2. dictionary: decode row code then return dictionary entry
3. constant: return `None` when `is_null` is set, otherwise return the single stored value

This preserves the existing "scalar decode always works" contract.

### `take`

For constant-encoded typed vectors:

1. any non-empty take remains constant-encoded
2. result length becomes `len(indices)`
3. no dense materialization is required
4. negative-index null padding should fall back to existing generic materialization rules if the caller relies on it

### `to_arrow()`

`to_arrow()` may expand to a regular Arrow array in v1.

That is acceptable because Arrow is an interop boundary, not the motor representation.

The design goal is native constant storage inside Draken, not constant-native Arrow export.

## Null Support Recommendation

Your inclination to avoid nullable constant encoding is a good default for v1.

I recommend:

### v1

Support only:

1. non-null typed constants
2. typed all-null constants via `is_null=1` and no row bitmap

Do not support:

1. row-level null bitmaps on constant-encoded typed vectors
2. "constant value plus sparse null rows" in the first pass

Why:

1. it keeps the encoding definition crisp: one value, one length
2. it avoids turning constant into a disguised dense/null-bitmap shape
3. it keeps kernels simple: either read one value or know the whole vector is null
4. it avoids repeating the `ConstantVector` complexity inside every typed vector

### Storage Rule

Typed constant encoding has vector-level nullability only:

1. `is_null = 0`: every row has the same non-null typed value
2. `is_null = 1`: every row is SQL NULL for that typed column

There is no row-level null bitmap for constant encoding.

That means typed all-null constants are valid native shapes, while mixed valid/null row patterns are not part of this encoding.

### Bare `NULL`

Untyped literal `NULL` is the awkward case.

Recommended handling:

1. if the expression is typed by context (`CAST(NULL AS BIGINT)`, typed branch output, known schema), use typed constant-null encoding
2. if the expression remains genuinely untyped, keep the existing `ConstantVector` only as a narrow compatibility carrier for untyped null/scalar fallback until binder typing is tightened

That lets us avoid blocking the whole redesign on bare-NULL typing.

## Why Not Reuse `DrakenConstantBuffer` As-Is

`DrakenConstantBuffer` proved the storage idea works, but it keeps the old problem alive:

1. the buffer carries its own `value_type`
2. the vector wrapping it is still storage-typed
3. callers still end up branching on `ConstantVector`

For the new model, `DrakenConstantBuffer` is better treated as:

1. an implementation reference for payload ownership
2. maybe a temporary internal helper for spill/restore
3. not the long-term public execution abstraction

## Interaction With Dictionary Encoding

A vector should expose one active storage encoding at a time.

For v1:

1. dense and constant are mutually exclusive
2. dictionary and constant are mutually exclusive
3. RLE remains separate future work

No "dictionary of one entry" shortcut should be used to represent constants. That would blur semantics and would be slower than a dedicated constant accessor.

## Migration Plan

### Phase 1: Add Constant Encoding Primitives

1. add `DRAKEN_ENCODING_CONSTANT`
2. add `ConstAccessor`
3. add `const_accessor()` to `Vector`
4. make `.encoding` return a true stored discriminant
5. add `from_constant(...)` to core typed vectors

### Phase 2: Teach Callers To Branch On Encoding

Update hot paths to use:

1. data type first
2. then encoding-specific accessor if needed

Priority sites:

1. expression evaluator literal coercion
2. expression predicate fast paths
3. aggregate helpers
4. Carchar group-by
5. GroupStateStore
6. CSV / JSON writers

Target pattern:

```cython
if vec.encoding == DRAKEN_ENCODING_CONSTANT:
    ca = vec.const_accessor()
elif vec.encoding == DRAKEN_ENCODING_DICTIONARY:
    da = vec.dict_accessor()
else:
    ptr = vec.dense_ptr()
```

one branch per morsel, never "is this a ConstantVector?" per callsite.

### Phase 3: Narrow `ConstantVector`

Once typed constant encoding is in place:

1. stop producing `ConstantVector` for typed scalar outputs
2. keep it only for temporary compatibility if needed
3. restrict it to truly untyped / unsupported fallback cases

### Phase 4: Remove `ConstantVector`

Remove it once:

1. typed constants cover all native scalar types we care about
2. spill/restore supports typed constant encoding
3. expression / grouping / writers no longer branch on `ConstantVector`

## Iteration-Sized Implementation Plan

The goal here is to keep each step small enough to complete, review, and test in a single iteration.

### Iteration 1: Add Encoding Enum Value

Status: Complete

Scope:

1. add `DRAKEN_ENCODING_CONSTANT` to the shared encoding enum
2. update any Python mirrors of the enum values
3. add or update a tiny unit test that asserts the new enum is exposed consistently

Why this is a good single iteration:

1. no behavior change yet
2. very small blast radius
3. prepares the codebase for explicit constant dispatch

### Iteration 2: Add Constant Accessor API To The Base Vector Contract

Status: Complete

Scope:

1. add `ConstAccessor` to the Draken buffer declarations
2. add `const_accessor()` to `Vector` and `.pxd` declarations
3. make the base implementation return `NULL`
4. add a focused test that dense and dictionary vectors return `NULL` for `const_accessor()`

Why this is a good single iteration:

1. introduces the abstraction seam before any storage migration
2. should not change existing execution behavior

### Iteration 3: Make `.encoding` A Real Discriminant

Status: Complete

Scope:

1. stop inferring `.encoding` only from `dict_accessor()`
2. give vectors an explicit encoding state, or equivalent typed override path
3. keep existing dense and dictionary results unchanged
4. add tests for dense and dictionary vectors so this refactor is behavior-preserving

Why this is a good single iteration:

1. it is foundational for constant support
2. it can be validated without migrating any constant producers yet

### Iteration 4: Add Constant Storage To One Fixed-Width Typed Vector

Recommended target:

1. `Int64Vector`

Status:

1. complete

Scope:

1. add constant sidecar fields to `Int64Vector`
2. implement `Int64Vector.from_constant(value, length, is_null=False)`
3. implement `const_accessor()` for `Int64Vector`
4. update `__getitem__`, `__len__`, and `take()` to honor constant encoding
5. keep dense and dictionary behavior unchanged
6. add unit tests for:
   - constant non-null
   - constant all-null
   - `take()`
   - `to_arrow()`
   - `encoding`

Why this is a good single iteration:

1. fixed-width numeric is the simplest proving ground
2. it validates the model before repeating it elsewhere

### Iteration 5: Port One More Fixed-Width Typed Vector

Recommended target:

1. `Float64Vector`

Status:

1. complete

Scope:

1. mirror the `Int64Vector` constant-encoding support
2. add tests matching the `Int64Vector` cases
3. confirm existing dictionary behavior still passes

Why this is a good single iteration:

1. proves the design is repeatable across fixed-width numerics
2. exposes any float-specific issues early

### Iteration 6: Port `StringVector`

Status:

1. complete

Scope:

1. add constant string payload storage to `StringVector`
2. implement `StringVector.from_constant(value, length, is_null=False)`
3. implement `const_accessor()` for string constants
4. update `__getitem__`, `take()`, and `to_arrow()`
5. add tests for:
   - bytes/string coercion
   - constant non-null
   - constant all-null
   - `take()`
   - empty string vs null

Why this is a good single iteration:

1. string is the main non-fixed-width case
2. it prevents the design from becoming numeric-only

### Iteration 7: Make `StringVector` Preserve Dictionary Encoding

Status:

1. complete

Scope:

1. add real `dict_accessor()` plumbing to `StringVector`
2. preserve dictionary metadata in `from_dict(...)` and `from_dict_buffers(...)`
3. preserve dictionary metadata through `take()`
4. make `.encoding` report `DRAKEN_ENCODING_DICTIONARY` for dictionary-backed string vectors
5. add focused tests for string dictionary encoding and gather preservation

Why this is a good single iteration:

1. it closes the biggest mismatch between the fixed-width vectors and `StringVector`
2. it keeps later consumer work honest by making string dictionary encoding observable through the same API shape as other vectors

### Iteration 8: Add Typed Constant Constructors To The Remaining Native Scalar Vectors

Status:

1. complete

Recommended targets:

1. `BoolVector`
2. `Date32Vector`
3. `TimestampVector`
4. `TimeVector`
5. `IntegerVector`
6. exclude `IntervalVector` from this iteration

Scope:

1. add `from_constant(...)` and `const_accessor()` to each intended scalar type
2. add minimal per-type correctness tests
3. keep `IntervalVector` deferred for a separate iteration

Why this is a good single iteration:

1. mostly repetitive once the first three vectors are done
2. can be kept bounded by excluding any type that needs extra design work

### Iteration 9: Switch Literal Producers For Typed Outputs

Status: complete

Scope:

1. update projection / evaluator paths that currently emit `ConstantVector` for typed literals
2. emit typed constant-encoded vectors instead when the output type is known
3. leave truly untyped fallback cases on `ConstantVector`
4. add integration tests for literal projection and mixed projection output shapes

Why this is a good single iteration:

1. this is the first point where the new storage starts flowing through execution
2. it is still limited to production, not broad consumption changes

### Iteration 10: Update Expression Coercion Helpers To Understand Typed Constant Encoding

Status: complete

Scope:

1. replace `ConstantVector` class-name checks in expression coercion helpers
2. use `.encoding` plus `const_accessor()` or typed APIs instead
3. keep compatibility with legacy `ConstantVector` during migration
4. add focused tests for typed constant literal coercion in predicate evaluation

Why this is a good single iteration:

1. expression coercion is a contained consumer surface
2. it reduces one of the most visible storage-type leaks

### Iteration 11: Update Constant Predicate Fast Paths

Status: complete

Scope:

1. replace `ConstantVector` checks in [opteryx/expression/ops.py](/Users/justin/Nextcloud/opteryx-core/opteryx/expression/ops.py)
2. make the fast path operate on typed constant encoding
3. keep fallback support for legacy `ConstantVector`
4. add predicate parity tests for:
   - constant non-null
   - constant all-null
   - fixed-width and string examples

Why this is a good single iteration:

1. it is a focused consumer
2. it has clear correctness tests

### Iteration 12: Update Aggregate Helper Logic

Status: complete

Scope:

1. replace `ConstantVector` handling in [opteryx/operators/draken_aggregate_node.py](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_node.py)
2. make `sum/min/max` and related helper paths use typed constant encoding
3. preserve legacy fallback temporarily
4. add tests for aggregate parity on typed constant columns

Why this is a good single iteration:

1. Python aggregate helpers are simpler than compiled group-by engines
2. it prepares for the lower-level engine changes

### Iteration 13: Update `GroupStateStore` Python Routing

Status: complete

Scope:

1. replace `ConstantVector` output/routing checks in [opteryx/operators/group_state_store.py](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/group_state_store.py)
2. use `.encoding` constants instead
3. update telemetry expectations if needed
4. add or adjust routing tests

Why this is a good single iteration:

1. small surface area
2. keeps planner/runtime routing aligned with the new abstraction

### Iteration 14: Update `GroupStateStore` Compiled Fast Paths

Status: complete

Scope:

1. replace `ConstantVector`-specific logic in [group_state_store.pyx](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/group_state_store.pyx)
2. use typed constant encoding through `const_accessor()`
3. keep behavior parity for constant-key/grouped aggregations
4. add targeted tests for constant-key fast paths

Why this is a good single iteration:

1. one compiled consumer at a time
2. narrower and easier than starting with Carchar

### Iteration 15: Update Carchar Group-By Engine

Status: complete

Scope:

1. replace `ConstantVector` branches in [carchar_group_state_engine.pyx](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/carchar_group_state_engine.pyx)
2. use typed constant encoding through `const_accessor()`
3. preserve current routing/fallback behavior
4. add targeted tests for:
   - constant keys
   - constant values
   - all-null constant values where relevant

Why this is a good single iteration:

1. this is the biggest constant consumer
2. it should be done only after the simpler consumers are stable

### Iteration 16: Update CSV / JSON Writers

Status: complete

Scope:

1. replace `ConstantVector`-specific encoding branches in:
   - [csv_rows.pyx](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/io/csv_rows.pyx)
   - [json_rows.pyx](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/io/json_rows.pyx)
2. use typed constant encoding through `const_accessor()`
3. keep output byte-for-byte compatible where practical
4. add export tests for typed constant and typed all-null columns

Why this is a good single iteration:

1. contained consumer surface
2. easy to validate with golden outputs

What landed:

1. both writers now recognize typed constant encoding directly through `const_accessor()`
2. focused CSV/JSON tests found and resolved two real dispatch bugs:
   - typed constant `StringVector` must be dispatched before the dense string path, otherwise the writers call `view()` on a constant-encoded string
   - typed constant fixed-width vectors must also be dispatched before dense typed-vector branches, otherwise the writers dereference `ptr.data` on constant vectors
3. focused validation is green:
   - `pytest tests/compiled/io/test_csv_rows.py -q`
   - `pytest tests/compiled/io/test_json_rows.py -q`

### Iteration 17: Add DRKM Native Constant-Encoding Support For Typed Vectors

Status: complete

Scope:

1. add storage-format support for typed constant encoding
2. preserve `is_null` and logical type
3. add spill/restore parity tests
4. keep temporary compatibility for legacy `ConstantVector` payloads if still needed

Why this is a good single iteration:

1. storage round-trip is self-contained
2. avoids materialization churn once the execution path starts producing typed constants

What landed:

1. DRKM write now recognizes typed constant encoding before dense physical dtype branches
2. typed constant columns serialize via `const_accessor()` and preserve their physical dtype in the DRKM column metadata
3. DRKM read now restores supported typed constants into:
   - `Int64Vector`
   - `Float64Vector`
   - `BoolVector`
   - `StringVector`
   - `Date32Vector`
   - `TimeVector`
   - `TimestampVector`
   - `IntegerVector`
4. legacy `ConstantVector` DRKM compatibility is still preserved for untyped constant payloads
5. focused validation is green:
   - `pytest tests/draken/morsels/test_morsel_io.py -q -k 'typed_constant_columns or constant_columns'`
   - `pytest tests/unit/core/test_constant_motor_path_guards.py -q`

Current branch note:

1. the full `tests/draken/morsels/test_morsel_io.py` file still contains unrelated dictionary-path failures on this branch
2. those failures are outside Iteration 17:
   - one string dictionary round-trip still restores dense
   - one numeric dictionary write path is still unsupported in DRKM for that test shape

### Iteration 18: Normalize Single-Value Encodings To Typed Constant On Ingest

Status: complete

Scope:

1. Arrow import:
   - detect single-entry dictionary arrays and emit typed constant vectors instead of materializing dense
   - detect run-end / REE arrays with a single logical value and emit typed constant vectors
2. Parquet-native decode:
   - detect decoded dictionary columns with cardinality `1` and emit typed constant vectors instead of typed dictionary vectors
3. keep mixed-cardinality dictionary and multi-run REE inputs on their existing dictionary / dense paths
4. add focused tests for:
   - Arrow single-entry dictionary to typed constant
   - Arrow single-run REE to typed constant
   - Parquet single-entry dictionary to typed constant
   - all-null single-value cases where representable

Why this is a good single iteration:

1. it captures a real compression shape we now know how to represent natively
2. it keeps import-time normalization separate from later legacy `ConstantVector` cleanup

What landed:

1. Arrow import now normalizes single-entry dictionary arrays to typed constant vectors when nullness is uniform
2. Arrow import now normalizes single-run REE / run-end encoded arrays to typed constant vectors when the single run spans the full logical column
3. Parquet-native decode now normalizes single-entry dictionary columns to typed constant vectors when nullness is uniform
4. mixed-null single-value inputs intentionally remain on the existing non-constant paths
   - the current constant encoding is vector-nullable, not row-nullable
5. focused validation is green:
   - `pytest tests/draken/interop/test_arrow_constant_ingest.py -q`
   - `pytest tests/rugo/test_dictionary_vector_decode.py -q`

### Iteration 19: Stop Producing `ConstantVector` For Typed Cases

Status: complete

Scope:

1. audit constant-producing sites
2. ensure typed outputs now produce typed constant encoding
3. narrow `ConstantVector` to genuinely untyped fallback cases only
4. add regression tests asserting typed literal outputs no longer use `ConstantVector`

Why this is a good single iteration:

1. this is the migration checkpoint
2. by now consumers should already understand the new encoding

What landed:

1. `constant_vector.from_scalar(...)` now prefers typed constant vectors for supported scalar types and typed dtypes
2. repeated-value sequence detection now routes all-valid typed cases through the same typed constant helper instead of constructing `ConstantVector` directly
3. `vector_iif(...)` now normalizes its public inputs through the shared scalar helper and accepts typed constant vectors in its constant branches
4. `ConstantVector` remains in place for genuinely untyped and mixed-null compatibility cases
5. focused validation is green:
   - `pytest tests/draken/vectors/test_vector_encoding.py -q`
   - `pytest tests/unit/functions/test_iif.py -q`
   - `pytest tests/unit/operators/test_projection_constant_morsel.py -q`

### Iteration 20: Remove Legacy `ConstantVector` Call-Site Dependencies

Status: complete

Scope:

1. search for remaining `ConstantVector` checks across Python and Cython
2. remove compatibility branches where no longer needed
3. add regression guards preventing new storage-type dispatch from returning

Why this is a good single iteration:

1. clear cleanup task
2. easy to review mechanically

What landed:

1. Python-side constant detection now prefers encoding- or behavior-based checks instead of direct `ConstantVector` naming in:
   - predicate fastpaths
   - aggregate helpers
   - projection telemetry
   - group-by wrapper routing helpers
   - evaluator constant wrappers
2. `ROUND` dispatch now accepts typed constant-like inputs without depending on `ConstantVector`
   - the constant-like branch stays at the arithmetic/function boundary
   - the dense rounding kernel remains a dense-kernel implementation
3. an unused `ConstantVector` import was removed from `QuerySession.execute_to_morsels(...)`
4. the remaining explicit `ConstantVector` references are now mostly low-level compatibility/storage paths
   - DRKM spill/restore compatibility
   - alignment/storage kernels
   - a few compiled engine branches that still need legacy support until the final removal decision
5. focused validation is green:
   - `pytest tests/unit/functions/test_round_function.py -q`
   - `pytest tests/unit/core/test_expression_constant_fastpath.py -q`
   - `pytest tests/unit/operators/test_draken_aggregate_node_constants.py -q`
   - `pytest tests/unit/operators/test_projection_constant_morsel.py -q`

### Iteration 21: Remove `ConstantVector` Entirely

Status: complete

Scope:

1. move shared scalar helpers out of the legacy module
2. delete the class, source files, generated source, and stale extension artifact
3. switch remaining imports to the shared scalar helper module
4. remove the remaining legacy runtime branches in group-by / writers / DRKM
5. adjust tests and docs accordingly

Why this is a good single iteration:

1. it closes the loop on the migration
2. it makes the final architecture obvious to future contributors

What landed:

1. shared scalar creation now lives in:
   - `opteryx/draken/vectors/scalar_constructors.pxd`
   - `third_party/mabel/draken/vectors/scalar_constructors.pyx`
2. the legacy `ConstantVector` sources and artifact were removed:
   - `opteryx/draken/vectors/constant_vector.pxd`
   - `third_party/mabel/draken/vectors/constant_vector.pyx`
   - `third_party/mabel/draken/vectors/constant_vector.cpp`
   - stale in-place extension artifact
3. remaining runtime consumers now use typed constant encoding directly:
   - `vector_iif`
   - CSV / JSON writers
   - compiled `GroupStateStore`
   - Carchar group-state engine
   - DRKM read/write
   - morsel alignment
4. tests that directly instantiated `ConstantVector` were removed or rewritten around typed constant vectors
5. focused validation is green:
   - `pytest tests/draken/vectors/test_vector_encoding.py -q`
   - `pytest tests/unit/core/test_expression_constant_fastpath.py -q`
   - `pytest tests/draken/morsels/test_morsel_io.py -q -k 'constant_columns or typed_constant_columns'`
   - `pytest tests/unit/operators/test_group_state_store_constant_fastpath.py -q`
   - `pytest tests/unit/functions/test_iif.py -q`
   - `pytest tests/draken/vectors/test_vector_from_sequence.py -q -k 'constant or fallback'`
   - `pytest tests/compiled/io/test_csv_rows.py -q`
   - `pytest tests/compiled/io/test_json_rows.py -q`

## Spill / DRKM

The dictionary project already showed the value of preserving encodings through DRKM instead of materializing them away.

Constant encoding should do the same.

DRKM representation only needs:

1. logical type
2. encoding = constant
3. length
4. `is_null` flag
5. single stored payload

When `is_null` is set, the payload is ignored.

That is simpler than the existing `ConstantVector` story and should be encoded natively from the start.

## Testing Plan

### Unit

1. `from_constant(...)` for each supported typed vector
2. `__getitem__` parity with dense baseline
3. `take()` preserves constant encoding
4. `to_arrow()` correctness
5. `const_accessor()` payload correctness
6. typed all-null coverage with `is_null=1`

### Integration

1. projection literals emit typed constant-encoded vectors
2. aggregate and group-by paths consume constant encoding without `ConstantVector`
3. JSON / CSV export parity
4. DRKM spill/restore parity

### Regression Guards

1. no new `__class__.__name__ == "ConstantVector"` checks
2. no new `isinstance(..., ConstantVector)` checks in hot paths
3. encoding dispatch uses `.encoding` plus accessors

## Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| `.encoding` remains inferred instead of explicit | constant cannot be represented cleanly | make encoding a true stored discriminant |
| null handling expands scope too early | constant path becomes bitmap-heavy and stops being simple | keep v1 to non-null plus optional typed all-null only |
| `ConstantVector` remains widely produced | callers never converge on typed encoding dispatch | switch producers first, then consumers |
| string constants become a second-class path | we repeat the dictionary string lag | design string constant payload up front, not as a fallback |

## Success Criteria

1. Typed vectors can represent constant storage natively.
2. New constant-producing paths emit typed vectors, not `ConstantVector`.
3. Hot paths dispatch on `.encoding` and accessors rather than storage classes.
4. Constant columns remain O(1) in storage with respect to row count.
5. `ConstantVector` can be narrowed to compatibility-only and then removed.

## Recommendation

Proceed with constant as a first-class typed encoding, not as an extension of `ConstantVector`.

Specifically:

1. copy the good parts of the dictionary redesign: explicit constructors, accessor-based storage, encoding discriminant
2. avoid repeating the bad parts: separate storage-typed vector, inferred type at every callsite, broad special casing
3. keep null support intentionally small in v1

If we do that, constant becomes the first proof that "encoding is storage, type is semantics" is the actual Draken model rather than just a dictionary-specific exception.
