# List Ops Draken-Native Conversion Plan

**Date:** March 6, 2026  
**Status:** Planning  
**Goal:** Convert all compiled list_ops from Arrow/numpy to Draken-native implementations

---

## Executive Summary

The `list_ops` module (compiled Cython code) currently works exclusively with Arrow arrays and returns numpy arrays. This plan converts all operations to be Draken-native, accepting Draken vectors and returning Draken vectors.

**Impact:** This will cause the expression engine to fail temporarily (addressed in Phase 2).

---

## Current Architecture

```
Expression Engine
    ↓
_inner_filter_operations() [Arrow/numpy gateway]
    ↓
list_ops.* functions (Arrow arrays → numpy arrays)
    - list_in_string(arrow_array, str) → uint8_t[::1]
    - list_in_list(arrow_array, list) → uint8_t[::1]
    - etc.
    ↓
numpy.frombuffer() → Expression result
```

**Problem:** Everything goes through Arrow/numpy marshalling. No direct Draken execution.

---

## Desired Architecture

```
Expression Engine
    ↓
ensure_draken_vector() [Draken gateway - TBD]
    ↓
list_ops.* functions (Draken vectors → Draken vectors)
    - list_in_string(draken_vector, str) → BoolVector
    - list_in_list(draken_vector, list) → BoolVector
    - etc.
    ↓
Draken-native result (no numpy or Arrow involved)
```

**Benefit:** Native Draken execution, no format conversions, performance.

---

## Scope: List Ops Functions to Convert

### String Matching Operations

| Operation | File | Current Pattern | New Pattern | Complexity |
|-----------|------|-----------------|-------------|-----------|
| `list_in_string()` | list_in_string.pyx | Arrow → numpy | StringVector → BoolVector | Medium |
| `list_in_string_case_insensitive()` | list_in_string.pyx | Arrow → numpy | StringVector → BoolVector | Medium |
| `match_like_bmh()` | match_like.pyx | Arrow → numpy | StringVector → BoolVector | Medium |
| `match_like_regex()` | match_like.pyx | Arrow → numpy | StringVector → BoolVector | High |

### List/Set Operations

| Operation | File | Current Pattern | New Pattern | Complexity |
|-----------|------|-----------------|-------------|-----------|
| `list_in_list()` | list_in_list.pyx | Arrow → numpy | Vector → BoolVector | Medium |

### Numeric Operations

| Operation | File | Current Pattern | New Pattern | Complexity |
|-----------|------|-----------------|-------------|-----------|
| Future: numeric comparisons | TBD | Arrow → numpy | Int64Vector/Float64Vector → BoolVector | Low-Medium |

---

## Conversion Strategy

### Phase 1: String Operations (list_in_string.pyx)

**Current Signature:**
```cython
cpdef uint8_t[::1] list_in_string(object column, str needle):
    # column: pyarrow.Array or pyarrow.ChunkedArray
    # returns: uint8_t memoryview (binary result array)
```

**New Signature:**
```cython
cpdef BoolVector list_in_string(StringVector vec, str needle):
    # vec: Draken StringVector
    # returns: Draken BoolVector (matching indices)
```

**Conversion Steps:**
1. Replace `pyarrow.Array`/`ChunkedArray` input with `StringVector` (from `cimport`)
2. Replace output `uint8_t[::1]` with `BoolVector` return
3. Update internal buffering logic to work directly with Vector data
4. Remove `chunk.py` and `chunk_view` handling (Draken vectors handle chunking differently)
5. Keep the core substring algorithm (BMH, skip tables) - just adapt to vector layout

**Key Changes:**
- ✅ Core algorithm: Boyer-Moore-Horspool stays the same
- ✅ Encoding: Still UTF-8 (strings in Draken are UTF-8)
- ✅ Pattern building: Reuse skip table logic
- ❌ Chunking: Vectors may have different layout; use Draken iteration
- ❌ Output: Return BoolVector instead of numpy array

### Phase 2: List Operations (list_in_list.pyx)

**Current Signature:**
```cython
cpdef uint8_t[::1] list_in_list(object column, object values):
    # column: pyarrow.Array
    # values: list or numpy array
    # returns: uint8_t memoryview
```

**New Signature:**
```cython
cpdef BoolVector list_in_list(Vector vec, list values):
    # vec: Any Draken Vector
    # values: list of values to check
    # returns: BoolVector (1 if in list, 0 otherwise)
```

**Conversion Steps:**
1. Accept generic `Vector` input (works with Int64Vector, StringVector, etc.)
2. Build a hash set from `values` (same as before)
3. Replace Arrow-specific null handling with Draken null bitmap logic
4. Return BoolVector instead of numpy array
5. Use Draken's vector iteration pattern

### Phase 3: Like Operations (match_like.pyx)

**Current Signature:**
```cython
cpdef uint8_t[::1] match_like_bmh(object column, str pattern):
    # column: pyarrow.Array
    # pattern: SQL LIKE pattern
    # returns: uint8_t memoryview
```

**New Signature:**
```cython
cpdef BoolVector match_like_bmh(StringVector vec, str pattern):
    # vec: Draken StringVector
    # pattern: SQL LIKE pattern
    # returns: BoolVector
```

**Conversion Steps:**
1. Extract string data directly from StringVector
2. Keep LIKE pattern compilation (KMP/regex-based)
3. Iterate Draken vector and build BoolVector result
4. Handle null positions via Draken null bitmap

---

## BoolVector Creation Pattern

All conversions must create a BoolVector result. Reference pattern:

```cython
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.interop.arrow cimport vector_from_arrow

# Option 1: Build from numpy array (intermediate step)
import pyarrow as pa
result_array = pa.array(result_list, type=pa.bool_())
return vector_from_arrow(result_array)

# Option 2: Build BoolVector directly from Draken (preferred, TBD)
# - Need to understand BoolVector construction from raw buffer
# - Likely: DrakenFixedBuffer with bool data + null bitmap
```

---

## Integration Points That Will Break (Phase 2)

1. **_inner_filter_operations()** (ops.py)
   - Currently: calls list_ops and does `numpy.frombuffer(result, dtype=bool)`
   - Will need: BoolVector result handling
   - **Action:** Modify to accept BoolVector from list_ops

2. **_constant_fastpath()** (ops.py)
   - Currently: returns numpy arrays
   - May need: BoolVector wrapper or conversion back to numpy (temporary)
   - **Action:** TBD based on constant path design

3. **Dictionary fastpath** (_dictionary_fastpath())
   - Currently: list_ops used for dictionary string matching
   - Will need: BoolVector handling
   - **Action:** Update dictionary fastpath to work with BoolVector

4. **Expression manager interface**
   - Currently: expects numpy/Arrow from operations
   - Will need: BoolVector support
   - **Action:** Design cursor/expression layer integration (Phase 2)

---

## Implementation Order

1. **list_in_string.pyx** (StringVector → BoolVector)
   - Core algorithm well-understood
   - Highest ROI (heavily used in queries)
   - Simpler than regex

2. **list_in_list.pyx** (Vector → BoolVector)
   - Generic (works with any vector type)
   - Hash-based, straightforward conversion
   - Critical for IN-list operations

3. **match_like.pyx** (StringVector → BoolVector)
   - Depends on LIKE regex parsing (complex)
   - Medium usage
   - Can defer to later phase if needed

---

## Technical Considerations

### Null Handling

**Current:** Arrow null bitmaps (handled by pyarrow)  
**New:** Draken null bitmaps (in vector)
- Access: `vec.ptr.null_bitmap` (from Draken C struct)
- Pattern: Check bit position for each row
- Output: Propagate nulls to BoolVector result

### Performance

**Current:** Arrow/numpy marshalling overhead  
**New:** Direct Draken execution
- ✅ Zero-copy when possible
- ✅ Avoids serialization/deserialization
- ⚠️ Must maintain bit-level performance (e.g., SIMD in BMH)

### Vector Types

Need to handle:
- **StringVector** - for string pattern matching
- **Int64Vector** - for numeric IN-list
- **Float64Vector** - for numeric IN-list
- **Other vectors** - may not need operations (constants suffice)

---

## Key Questions Needing Answers

1. **BoolVector Construction**: What's the optimal way to create a BoolVector from scratch in Cython?
   - Via Arrow → vector_from_arrow() (simple, but temporary conversion)
   - Direct DrakenFixedBuffer construction (optimal but more complex)

2. **Chunking**: Do Draken vectors have a concept of chunks? How does iteration work?
   - StringVector internal layout?
   - Multi-segment storage?

3. **Regex in Cython**: match_like.pyx uses regex - can this stay as-is with StringVector input?
   - Likely yes, just feed vector data to regex engine

4. **Error Handling**: What exceptions do we throw for invalid patterns/values?
   - Keep same semantics as Arrow version?

---

## Files to Modify

```
opteryx/compiled/list_ops/
├── list_in_string.pyx          (STRING operations)
├── list_in_list.pyx            (LIST/SET operations)
├── match_like.pyx              (LIKE operations)
└── list_ops.pyx                (aggregator - no changes needed)

opteryx/managers/expression/
├── ops.py                       (Phase 2: BoolVector handling)
└── __init__.py                  (Phase 2: integration)
```

---

## Success Criteria

- ✅ All list_ops accept Draken vectors and return BoolVector
- ✅ No Arrow or numpy inside list_ops code
- ✅ Null handling preserved (same semantics)
- ✅ Performance maintained or improved
- ✅ Tests pass (once expression engine updated)

---

## Timeline Estimate

| Phase | Task | Complexity | Estimate |
|-------|------|-----------|----------|
| 1a | list_in_string conversion | Medium | 2-3 days |
| 1b | list_in_list conversion | Medium | 1-2 days |
| 1c | match_like conversion | High | 2-3 days |
| 2 | Expression engine integration | Medium | 2-3 days |
| 3 | Testing & debugging | Medium | 2-3 days |
| **Total** | | | **9-14 days** |

---

## Next Steps

1. ✅ **This phase**: Agree on architecture (this document)
2. **Phase 1a**: Convert list_in_string.pyx
   - Decide on BoolVector construction method
   - Test with simple string matching
3. **Phase 1b**: Convert list_in_list.pyx
4. **Phase 1c**: Convert match_like.pyx
5. **Phase 2**: Update expression engine to accept BoolVector
   - Modify _inner_filter_operations()
   - Handle dictionary fastpath
6. **Phase 3**: Integration testing & performance validation
