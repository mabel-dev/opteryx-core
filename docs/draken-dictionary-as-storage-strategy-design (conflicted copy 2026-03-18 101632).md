# Draken Vector Type System: Encoding as a Storage Strategy

## Status
Active — Phase 1 complete; Phase 2 transition and stabilization are next.

Current implementation snapshot:
- `DictAccessor` has been added to the Draken buffer declarations.
- `DictionaryVector.dict_accessor()` is implemented and returns a view over the existing dictionary buffer.
- Dense vectors now expose `dense_ptr()` and `null_bitmap_ptr()`.
- The evaluator package has been split into named modules, while preserving the existing public import surface.
- Carchar key detection, key ingestion, and dictionary-backed value-column routing now use accessor-based paths.
- `carchar_group_state_engine.pyx` no longer contains direct `DictionaryVector` checks or the `_dictionary_key_kind` wrapper.
- Python-layer expression/function/operator call sites that previously special-cased dictionary encoding have been moved to accessor- or Arrow-shape-based logic.
- The grouped-aggregation planner no longer forces `COUNT(*)` to the older `GroupStateStore` backend solely because the aggregation column is `None`.
- Group-by telemetry now distinguishes `CarcharGroupStateEngine` from `GroupStateStore` explicitly; the old `legacy` label has been removed.
- Typed vector classes now expose explicit `from_dict(...)` constructors rather than overloading `from_arrow(...)` with dictionary semantics.
- Typed `from_arrow(...)` paths have been narrowed back to dense Arrow interop; dictionary Arrow arrays are rejected and must not be treated as the storage constructor shape.
- The fixed-width typed `from_dict(...)` constructors now use typed Cython memoryviews internally for codes, dictionary payloads, and row validity instead of generic Python-object indexing in the hot construction path.
- `StringVector.from_dict(...)` now treats codes and row validity as typed inputs, but its dictionary payload is still Python-level because it has not yet been moved to raw arena-plus-offsets inputs.

Immediate next work:
- Finish moving the parquet decoder callsites off `_make_dictionary_vector(...)` and onto typed `from_dict(...)` constructors. Fixed-width numeric dictionary columns now use typed constructors; string dictionary columns still emit `DictionaryVector` until the raw string constructor exists.
- Replace the remaining Python-level string dictionary constructor shape with a raw string storage constructor using arena bytes plus offsets and validity.
- Decide and implement the DRKM transition path so dictionary encoding can be preserved without requiring public `DictionaryVector` round-tripping at the storage boundary.
- Stabilize planner and telemetry behavior for dictionary-backed group-by paths so readings match actual engine selection and fastpath use.
- Make unsupported dictionary float shapes plan explicitly to `GroupStateStore` instead of selecting Carchar and then erroring at runtime.
- Replace the remaining Abseil-backed distinct sets inside Carchar so dictionary grouping and distinct aggregation are fully on the Carchar path.

Interpretation note:
- The status and phase sections below describe the current project state.
- The struct, type-model, and accessor sections that follow remain the target architecture, not a claim that every step has already landed.

## Problem

`DictionaryVector` is compression-typed, not data-typed. Every other vector in Draken names what the data *is*: `Float64Vector`, `Int64Vector`, `StringVector`. `DictionaryVector` names how the data is *stored*.

This forces every consumer to peel back the encoding before it can do anything useful:

```cython
# carchar_group_state_engine.pyx — _maybe_init_carchar_mode
elif fn == "sum":
    value_vector = morsel.column(column)
    if isinstance(value_vector, Float64Vector):
        self._agg_mode = AGG_SUM
        self._value_kind = VALUE_FLOAT64
    elif isinstance(value_vector, (Int64Vector, IntegerVector)):
        self._agg_mode = AGG_SUM
        self._value_kind = VALUE_INT64
    else:
        self._init_legacy_backend()   # ← DictionaryVector[int] falls here, wrong
```

The current affected sites:
- `carchar_group_state_engine.pyx` — `_maybe_init_carchar_mode` (single and multi-agg), all 14 ingest method value-column branches
- `expression/evaluator/` — `draken.py` and public re-exports in `__init__.py`
- `opteryx/operators/group_state_store.py` — routing decisions

Any gap produces silently wrong results. For example `MIN`/`MAX` on a `DictionaryVector[DRAKEN_FLOAT64]` column currently routes to `VALUE_OBJECT` and the `_ingest_object_minmax_for_states` string-bytes path, returning `b''` instead of the actual float minimum.

## Current Struct Layout (Before This Change)

From `opteryx/draken/core/buffers.pxd`:

```c
struct DrakenDictionaryBuffer {
    uint8_t*         codes;               // code array, code_width bytes each
    uint8_t          code_width;          // 1, 2, or 4
    uint8_t*         null_bitmap;         // row-level nulls (bit per row)
    size_t           length;              // number of rows
    uint8_t          ordered;
    DrakenVarBuffer* dictionary_values;   // PROBLEM: reused for both string and numeric
    DrakenType       type;                // always DRAKEN_DICTIONARY
};

struct DrakenVarBuffer {
    uint8_t*   data;        // for string: UTF-8 bytes; for numeric: raw typed bytes
    int32_t*   offsets;     // for string: [N+1] byte offsets; for numeric: NULL
    uint8_t*   null_bitmap; // entry-level nulls in the dictionary itself
    size_t     length;      // number of distinct values in the dictionary
    DrakenType type;        // actual element type
};
```

`DrakenVarBuffer` is reused for both string and numeric dictionary value stores. For numeric types `offsets` is NULL and `data` is a flat array of `itemsize`-byte values. This is wrong: `uint8_t*` as a generic byte pointer is standard C but `DrakenVarBuffer` is semantically and structurally a string buffer — reusing it for numerics means every numeric consumer must check `offsets == NULL` to know they're in numeric mode, and `_dict_itemsize_for_type` fills a gap that should not exist.

## Revised Struct Layout (After This Change)

Split `DrakenDictionaryBuffer.dictionary_values` into two typed fields with a discriminating `value_category`. Only one is non-NULL:

```c
typedef enum DrakenDictValueCategory {
    DRAKEN_DICT_VARIABLE = 0,  // dict_string_values is live (variable-width: strings, binary)
    DRAKEN_DICT_FIXED    = 1,  // dict_numeric_values is live (fixed-width: int8..int64, float32/64)
} DrakenDictValueCategory;

struct DrakenDictionaryBuffer {
    uint8_t*                  codes;               // code array
    uint8_t                   code_width;           // 1, 2, or 4
    uint8_t*                  null_bitmap;          // row-level nulls
    size_t                    length;               // number of rows
    uint8_t                   ordered;
    DrakenDictValueCategory   value_category;       // which union field is live
    DrakenVarBuffer*          dict_string_values;   // non-NULL for string dict
    DrakenFixedBuffer*        dict_numeric_values;  // non-NULL for numeric dict
    DrakenType                type;                 // always DRAKEN_DICTIONARY
};
```

`DrakenVarBuffer` is now string-only. `DrakenFixedBuffer` is already the correct type for fixed-width numeric data — `itemsize` is set by the element type, `data` is a flat typed array, `null_bitmap` is per-entry and optional:

```c
struct DrakenFixedBuffer {
    void*      data;        // flat array: int8_t*, int64_t*, double*, etc.
    uint8_t*   null_bitmap; // optional, 1 bit per entry
    size_t     length;      // number of distinct entries
    size_t     itemsize;    // bytes per entry: 1, 2, 4, or 8
    DrakenType type;        // DRAKEN_INT8 / DRAKEN_INT64 / DRAKEN_FLOAT64 etc.
};
```

Reading a numeric dictionary value for row `i` becomes unambiguous:

```cython
# Once per morsel/vector — detect encoding and get typed pointer:
da = vec.dict_accessor()
if da != NULL:
    # da.value_category == DRAKEN_DICT_NUMERIC confirmed
    value_ptr = <int64_t*> da.dict_numeric_values.data   # cast once
    # then in tight loop — no branches per row:
    value = value_ptr[_read_code(da, i)]
```

The type-dispatch happens once when entering the loop, not once per row.

## Encoding Property

Each vector exposes a read-only `.encoding` property returning a `DrakenEncoding` enum:

```cython
ctypedef enum DrakenEncoding:
    DRAKEN_ENCODING_DENSE      = 0
    DRAKEN_ENCODING_DICTIONARY = 1
    DRAKEN_ENCODING_RLE        = 2   # future
```

This lets consumers make a single readable dispatch without calling accessors just to check for NULL:

```cython
if vec.encoding == DRAKEN_ENCODING_DICTIONARY:
    da = vec.dict_accessor()
    # use da
elif vec.encoding == DRAKEN_ENCODING_DENSE:
    ptr = vec.dense_ptr()
    # use ptr
else:
    # fallback path for any other encoding: vec[i] scalar decode
```

The `.encoding` check is one branch per morsel. Consumers that only handle dense and dictionary remain correct and readable as new encodings are added — they fall through to the scalar loop.

**Dense vs scalar:** Dense means a flat typed C array, one element per row, nothing unpacked at access time. Scalar means a single constant value repeated across all rows (a `ConstantVector`). These are orthogonal concepts. A `ConstantVector` has its own encoding; it does not go through the dense array path.

## Proposed Design

The remainder of this document describes the intended end state after the accessor migration. Parts of it are implemented today, but large sections below are still design targets rather than current code.

### Core Principle

Vectors are data-typed. Encoding is a storage detail that typed vectors expose through *opt-in accessor interfaces*, not through a separate type. Consumers dispatch on data type (`Float64Vector`, `Int64Vector`, `StringVector`) and optionally request encoding-specific bulk accessors when they can exploit them.

`vec[i]` always works and always returns a decoded Python value. It exists for correctness-only fallback code. It should not appear in hot loops.

### Encoding Accessor Pattern

Each typed vector exposes zero or more encoding accessors. All return either a live C-level handle or `NULL` if that encoding is not active. The `.encoding` property tells you upfront which accessor will be non-NULL:

```cython
# Encoding discriminant — one branch per morsel, never per row
cdef DrakenEncoding encoding         # DRAKEN_ENCODING_DENSE / DICTIONARY / RLE

# Dictionary accessor — non-NULL when encoding == DRAKEN_ENCODING_DICTIONARY
cdef DictAccessor* dict_accessor(self)

# RLE accessor — non-NULL when encoding == DRAKEN_ENCODING_RLE (future)
cdef RLEAccessor* rle_accessor(self)

# Dense accessor — always available (encoding == DRAKEN_ENCODING_DENSE, or as fallback)
cdef void* dense_ptr(self)
cdef uint8_t* null_bitmap_ptr(self)
```

`DictAccessor` exposes the split struct directly:

```cython
ctypedef struct DictAccessor:
    uint8_t*           codes              # raw code array
    uint8_t            code_width         # 1, 2, or 4
    uint8_t*           row_nulls          # row-level null bitmap (may be NULL)
    size_t             length             # number of rows
    DrakenEncoding     value_category     # DRAKEN_DICT_VARIABLE or DRAKEN_DICT_FIXED
    DrakenVarBuffer*   string_values      # non-NULL for string dict
    DrakenFixedBuffer* numeric_values     # non-NULL for numeric dict
```

A consumer doing `SUM` on a value column — one branch per morsel, zero branches per row:

```cython
if value_vec.encoding == DRAKEN_ENCODING_DICTIONARY:
    da = value_vec.dict_accessor()
    if da.value_category == DRAKEN_DICT_FIXED:
        if da.numeric_values.type == DRAKEN_FLOAT64:
            f64_ptr = <double*> da.numeric_values.data   # cast once
            for row_idx in range(row_count):
                state_index = state_indices[row_idx]
                if _bitmap_is_valid(da.row_nulls, row_idx):
                    self._f64_state[state_index] += f64_ptr[_read_code(da, row_idx)]
        elif da.numeric_values.type in (DRAKEN_INT64, ...):
            i64_ptr = <int64_t*> da.numeric_values.data  # cast once
            for row_idx in range(row_count):
                ...
elif value_vec.encoding == DRAKEN_ENCODING_DENSE:
    f64_ptr = <double*> value_vec.dense_ptr()            # cast once
    for row_idx in range(row_count):
        ...
```

The type-branch on `da.numeric_values.type` or `value_vec` type happens once per morsel. The inner loop is a tight pointer-dereference loop with no branches.

### Extensibility to Future Encodings

Adding RLE requires:
1. Defining `RLEAccessor` struct (values array + run-lengths array)
2. Adding `rle_accessor()` to typed vectors with an RLE backend
3. Adding RLE-aware logic to consumers that benefit (columnar scanners, simple filters)

Consumers that don't handle RLE call `rle_accessor()`, get NULL, fall to dense or dict, and remain correct. No consumer is broken by adding a new encoding. No flags accumulate.

The three encodings in scope:
- **Dense** — flat typed array. `dense_ptr()` + `null_bitmap_ptr()`. Always available as final fallback.
- **Dictionary** — `dict_accessor()`. Useful for GROUP BY key comparison (avoid decode) and for value columns in aggregation when the dictionary is small (cache-friendly decode).
- **RLE** — `rle_accessor()`. Useful for scanners/filters, not generally useful for GROUP BY value accumulation (every run still contributes individually to aggregates). Future work.

### New Type Model

The typed vectors (`Float64Vector`, `Int64Vector`, `StringVector`) gain optional dictionary or RLE backends. The external class name stays the same:

```
Float64Vector   — dense backend (current) OR dictionary backend (new)
Int64Vector     — dense backend (current) OR dictionary backend (new)
StringVector    — dense backend (current) OR dictionary backend (new)
```

`DictionaryVector` as a public type is removed. `DrakenDictionaryBuffer` as an internal struct is retained as the dictionary backend.

Named constructors distinguish backends:
```cython
Float64Vector(length)                               # dense
Float64Vector.from_dict(codes, dict_data, ...)      # dictionary-backed
```

Current transition note:
- `from_arrow(...)` remains the Arrow interop API and still assumes Arrow runtime objects.
- `from_dict(...)` is now the intended storage/backend constructor seam.
- For fixed-width vectors the internal `cdef from_dict(...)` implementations now operate on typed memoryviews, with only a thin Python wrapper at the classmethod boundary.
- `StringVector.from_dict(...)` has not reached the same endpoint yet because its dictionary payload still arrives as decoded Python values rather than raw arena bytes and offsets.
- `DictionaryVector` therefore still exists today as a compatibility/storage type for the parts of the system that have not yet moved to typed dictionary backends, notably string parquet decode and DRKM dictionary serde.

### What Is Removed

- `DictionaryVector` public class
- `_dictionary_key_kind` / `_dictionary_type_to_key_kind` in `carchar_group_state_engine.pyx` — replaced by `da.value_type` from the accessor
- `isinstance(vec, DictionaryVector)` at all call sites — replaced by `vec.dict_accessor() != NULL`
- `_dict_compare` in the evaluator — folds into typed compare functions via `dict_accessor()`
- `_ingest_object_minmax_for_states` numeric branch workaround (see below)

Landing note:
- This section is still target-state, not current-state.
- `DictionaryVector` cannot actually be removed until both remaining dependencies are migrated: string parquet decode and DRKM dictionary serialization/deserialization.

### What Is Preserved

- `DrakenDictionaryBuffer` struct — unchanged, becomes private backend
- `DrakenVarBuffer` reuse for numeric dict values — unchanged internally
- `_read_code` inline helper — retained, moved to shared header or inline in accessor impl
- All Parquet reader dictionary page handling — adapts callsite only

Additional implementation constraint discovered during Phase 2:
- DRKM morsel storage currently has an explicit dictionary encoding format and round-trips `DictionaryVector` directly. Removing the public `DictionaryVector` type therefore requires a typed dictionary-aware DRKM path or an equivalent private wrapper at the storage boundary.

## Migration Path

### Phase 1 — Add accessors alongside existing types (non-breaking)
- Add `dict_accessor()` as a method on `DictionaryVector` returning a `DictAccessor*` view of its own buffer. No new types yet.
- Add `dense_ptr()` / `null_bitmap_ptr()` to all existing dense vectors.
- Update Carchar and evaluator to use `dict_accessor()` instead of `isinstance(..., DictionaryVector)` and `_dictionary_key_kind`.
- All existing code keeps working; `DictionaryVector` still exists.

Phase 1 progress update:
- Accessor APIs have landed on the existing vector types without changing the public type model.
- Carchar now uses accessor-based logic for dictionary-backed key classification and key ingestion.
- Carchar dictionary-backed value-column routing and decode paths have also been moved to accessor-based handling, and the remaining direct `DictionaryVector` references were removed from `carchar_group_state_engine.pyx`.
- Evaluator code has been moved out of a monolithic package initializer into named modules, preserving the package-level API while making the remaining accessor migration more localised.
- Python-layer expression/function/operator call sites that were still reasoning about dictionary encoding explicitly have been cleaned up.
- A rebuild after the cleanup succeeded, and targeted smoke checks plus focused dictionary regression runs exercised compare/filter/text/temporal and grouped-aggregation behavior.
- Phase 1 exit criteria are considered met: accessor-based dictionary handling is in place across the main runtime and grouped-aggregation engine.
- What remains is not Phase 1 cleanup; it is Phase 2 stabilization and architectural follow-through.

### Phase 2 — Typed-vector backend transition and engine hardening
- Add `Float64Vector.from_dict(...)`, `Int64Vector.from_dict(...)`, `StringVector.from_dict(...)`.
- Parquet reader produces these instead of `DictionaryVector`.
- `DictionaryVector` becomes a deprecated alias.
- Move DRKM dictionary serialization/deserialization off direct `DictionaryVector` round-tripping.
- Finish planner hardening for grouped aggregation so unsupported shapes route directly to `GroupStateStore` instead of selecting Carchar and failing later.
- Bring Carchar telemetry/readings into alignment with actual engine choice and dictionary fastpath use.
- Remove the remaining Abseil-backed distinct-set usage inside Carchar so the Carchar group-state path is internally self-consistent.

Phase 2 progress update:
- Explicit typed `from_dict(...)` entrypoints now exist on `Float64Vector`, `Int64Vector`, `IntegerVector`, `Date32Vector`, `TimeVector`, `TimestampVector`, and `StringVector`.
- Typed `from_arrow(...)` constructors have been corrected back to dense Arrow interop only; dictionary Arrow arrays are no longer accepted there.
- The fixed-width typed `from_dict(...)` implementations now use typed codes, typed dictionary value buffers, and typed row-validity buffers internally rather than generic `object` arguments in the `cdef` construction path.
- This means the constructor split is now real at the API and implementation level for fixed-width vectors: `from_arrow(...)` is interop, `from_dict(...)` is backend construction.
- The parquet reader now routes fixed-width numeric dictionary columns through typed `from_dict(...)` constructors instead of `_make_dictionary_vector(...)`.
- The parquet reader still emits `DictionaryVector` for string dictionary columns because the raw string constructor still needs arena-bytes-plus-offsets inputs rather than Python values.
- DRKM morsel serialization and deserialization still preserve dictionary encoding by writing and reading explicit dictionary segments and reconstructing `DictionaryVector` directly.
- The string constructor is only partially across the boundary: codes and row validity are typed, but dictionary payload is still supplied as Python values. A raw arena-plus-offsets string constructor is still required before the string path is fully storage-native.
- Because of that, Phase 2 is no longer blocked on API shape; it is now blocked on the remaining producer and persistence migrations: raw string constructors, string parquet decode, and DRKM dictionary serde.

### Phase 3 — Remove DictionaryVector
- Delete public class after no producer or persistence path requires it.
- Fix any remaining isinstance checks in non-Carchar code.
- Remove or privatise the remaining dictionary-specific morsel-storage glue once typed vectors carry dictionary encoding through DRKM directly.

Phase 1 is done. Phase 2 is the active transition track. Phase 3 remains cleanup after the typed-vector backend is stable and the remaining fallback/planner issues are closed.

## Related Work

- [draken-native-engine-design.md](draken-native-engine-design.md) — overall Draken engine architecture
- [draken-migration-phases-design.md](draken-migration-phases-design.md) — current rewrite phase tracking
- [draken-filter-operators-design.md](draken-filter-operators-design.md) — filter dispatch context
