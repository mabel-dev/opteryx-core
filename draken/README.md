# Draken

Draken is the native columnar vector substrate for Opteryx Core. Columnar data in the query engine is represented as `DrakenVector` across Python, Cython, and C++ boundaries.

The native implementation lives in `draken_native.cpp` (nanobind) and the core C++ structs in
`core/buffers.h`. This document covers the Python-facing interface. If there is any conflict between
this file and `core/buffers.h`, the header is authoritative.

## Build

Draken is built as part of the top-level extension build:

```bash
make compile
```

To build only Draken extensions while isolating from broader Opteryx Cython breakage:

```bash
make draken
```

Run Draken tests with:

```bash
make dt
```

---

## The Vector Model

Every column is a `DrakenVector`:

```c
struct DrakenVector {
    void*             data;        // typed payload
    const uint32_t*   selection;   // indices into data — never NULL
    uint32_t          data_length; // physical value count in data array
    uint32_t          length;      // logical row count
    uint8_t*          validity;    // 1-bit null mask; NULL means all-valid
    DrakenType        type;        // element type (dispatch key)
    uint8_t           flags;       // layout hints (DRAKEN_SEL_IDENTITY, DRAKEN_SEL_PERMUTATION); 0 = unknown
}
```

**The single access pattern is `data[selection[i]]` for `i in [0, length)`.**

The three physical shapes differ only in what `selection` points at:

| Shape    | `selection` points at           | `data_length`  |
|----------|---------------------------------|----------------|
| Dense    | identity permutation            | `== length`    |
| Constant | global zero vector              | `== 1`         |
| Dict     | owned per-vector codes          | `< length`     |

`data_length` is the physical size of the `data` array. **Uniqueness of values is guaranteed only by the compress-builders** (e.g. `vector_from_string_dict_sequence`). Other dict constructors may admit duplicate values in `data` — do not assume they are distinct. Kernels always read via `data[selection[i]]` regardless.

Specialization is not required for correctness — the access pattern above works for all three shapes. Shape-specialized fast paths require explicit architect approval (see §11 of the engineering contract).

**`flags` layout hints** (set by constructors, advisory only — never use in hot loops):

| Bit constant             | Meaning |
|--------------------------|---------|
| `DRAKEN_SEL_IDENTITY`    | `selection[i] == i` (true dense; implies permutation) |
| `DRAKEN_SEL_PERMUTATION` | bijection over `data`; `data_length == length` |

---

## Importing

```python
import draken.draken_native as dn
```

`draken/__init__.py` loads the native extension with `RTLD_GLOBAL` so bridge symbols are visible to
consumer extensions compiled against `core/draken_bridge.h`. Always import through the package, not
the `.so` directly.

---

## Types

`dn.DrakenType` is an enum. Values relevant to Python consumers:

| Member         | Numeric | Python equivalent                     | Notes |
|----------------|---------|---------------------------------------|-------|
| `INT8`         | 1       | `int`                                 | |
| `INT16`        | 2       | `int`                                 | |
| `INT32`        | 3       | `int`                                 | |
| `INT64`        | 4       | `int`                                 | |
| `DECIMAL`      | 5       | `decimal.Decimal`                     | int64 unscaled value; precision ≤ 18 |
| `FLOAT32`      | 20      | `float`                               | |
| `FLOAT64`      | 21      | `float`                               | |
| `DATE32`       | 30      | `datetime.date`                       | |
| `TIMESTAMP64`  | 40      | `datetime.datetime`                   | |
| `TIME32`       | 41      | `datetime.time`                       | |
| `TIME64`       | 42      | `datetime.time`                       | |
| `INTERVAL`     | 43      | `(months: int, ms: int)`              | |
| `BOOL`         | 50      | `bool`                                | |
| `VARCHAR`      | 60      | `str` (ASCII)                         | |
| `NVARCHAR`     | 63      | `str` (UTF-8 codepoint-aware)         | |
| `VARBINARY`    | 64      | `bytes`                               | |
| `VARIANT`      | 65      | `str` (JSON text)                     | Polymorphic JSON value |
| `ARRAY`        | 80      | `list`                                | |
| `NULL`         | 101     | `None`                                | Every row null; no data, no validity |
| `VECTOR_FP16`  | 102     | `list[float]` (embeddings)            | |
| `DECIMAL128`   | 103     | `decimal.Decimal`                     | int128 unscaled value; precision ≤ 38 |

Parameterized types carry out-of-band metadata (not stored in the struct):

- **TIMESTAMP64 / TIME32 / TIME64** — `unit` (`"s"`, `"ms"`, `"us"`, `"ns"`) and `offset_minutes`
- **DECIMAL** — `precision` (1–18) and `scale` (0–precision)
- **VECTOR_FP16** — `dimension` (number of fp16 elements per row)

Constructors attach this metadata automatically.

---

## Vector

`dn.Vector` is the Python wrapper around an owned `DrakenVector`.

### Properties

```python
v.type    # DrakenType
v.length  # int — logical row count
```

### Indexing and conversion

```python
len(v)          # logical row count
v[i]            # Python value at row i (None for null)
v.to_pylist()   # list[value | None] — full column as Python objects
```

### Reductions

```python
v.sum()   # scalar | None
v.min()   # scalar | None
v.max()   # scalar | None
v.hash()  # int64 — hash of entire column
```

### Boolean operations (BOOL vectors only)

```python
a.bool_and(b)  # element-wise AND (Kleene 3VL) → Vector[BOOL]
a.bool_or(b)   # element-wise OR  (Kleene 3VL) → Vector[BOOL]
a.bool_not()   # logical NOT → Vector[BOOL]
a.bool_any()   # bool | None  (OR reduction)
a.bool_all()   # bool | None  (AND reduction)
```

### Comparisons

```python
# op codes: 0=eq  1=ne  2=lt  3=le  4=gt  5=ge  6=in_list
v.compare_scalar(scalar, op)     # → Vector[BOOL]
v.compare_vector(other, op)      # → Vector[BOOL]
v.between(low, high, inclusive)  # → Vector[BOOL]
v.in_list(values: list)          # → Vector[BOOL]
```

### Arithmetic

```python
v.neg()   # negation → same type
```

### Row selection

```python
v.take(indices: list[int])  # gather by row indices → Vector (same type)
v.drop_nulls()              # keep only valid rows, shrinking length (NULL/ARRAY/FP16 only)
v.dictionary_encode()       # dedupe into a Dict shape; length and null rows unchanged
v.materialize()             # expand dict shape to dense
```

### Array element access (ARRAY vectors only)

```python
v.array_length(i)     # int — element count at row i
v.array_get(i, j)     # value at element j of row i
```

---

## Constructors

All constructors return a `Vector`. `None` in the input list produces a null row.

### Dense (identity selection)

```python
dn.vector_from_sequence(values: list[int | None])            # → INT64
dn.vector_int8_from_sequence(values)                         # → INT8
dn.vector_int16_from_sequence(values)                        # → INT16
dn.vector_int32_from_sequence(values)                        # → INT32
dn.vector_float32_from_sequence(values)                      # → FLOAT32
dn.vector_float64_from_sequence(values)                      # → FLOAT64
dn.vector_from_bool_sequence(values: list[bool | None])      # → BOOL
dn.vector_from_string_sequence(values: list[str | None])     # → VARCHAR
dn.vector_from_nvarchar_sequence(values: list[str | None])   # → NVARCHAR
dn.vector_from_bytes_sequence(values: list[bytes | None])    # → VARBINARY
dn.vector_date32_from_sequence(values: list[date | None])    # → DATE32
dn.vector_timestamp_from_sequence(
    values: list[datetime | None],
    unit: str = "us",
    offset_minutes: int = 0,
)                                                            # → TIMESTAMP64
dn.vector_time32_from_sequence(values, unit="s"|"ms")        # → TIME32
dn.vector_time64_from_sequence(values, unit="us"|"ns")       # → TIME64
dn.vector_decimal_from_sequence(
    values: list[Decimal | None],
    precision: int,
    scale: int,
)                                                            # → DECIMAL
dn.vector_interval_from_sequence(
    values: list[tuple[int, int] | None],  # (months, ms)
)                                                            # → INTERVAL
dn.vector_fp16_from_sequence(
    values: list[list[float] | None],
    dimension: int,
)                                                            # → VECTOR_FP16
dn.vector_array_from_sequence(values: list[list | None])     # → ARRAY
```

### Constant (single value broadcast)

```python
dn.vector_from_constant(value, length: int)          # → INT64
dn.vector_from_bool_constant(value, length)          # → BOOL
dn.vector_varchar_from_constant(value, length)       # → VARCHAR
# Also: vector_int8/16/32_from_constant, vector_float32/64_from_constant,
#       vector_date32/time32/time64/timestamp/decimal/interval_from_constant
```

### Dict (deduplicated)

```python
dn.vector_from_dict(
    values: list[int | None],
    codes: list[int],
    nullable: list[bool] | None,
)                                                           # → INT64
dn.vector_from_string_dict_sequence(values: list[str | None])  # auto-dedup → VARCHAR
# Also: vector_from_bool_dict, vector_int8/16/32_from_dict,
#       vector_float32/64_from_dict, vector_date32/time32/time64/timestamp/decimal/interval_from_dict
```

---

## Morsel

A `Morsel` groups related vectors (a column batch). It holds Python object references (refcount-based keep-alive) but owns no C++ resources directly. A vector is freed only when all holders — including the `Morsel` — have been released.

```python
m = dn.Morsel()
m.append(col1)   # add a Vector
m.append(col2)
m[0]             # Vector at index 0
len(m)           # number of columns
```

---

## C Extension Interop

Extensions compiled in Cython or C++ that need to pass vectors across the boundary without Python
overhead use the bridge API in `core/draken_bridge.h`. `draken_bridge.h` is the authoritative source;
the headline functions are:

```c
// Unwrap — returns a BORROWED pointer, valid only while `obj` is kept alive.
// Raises TypeError (never segfaults) if obj is not a Vector.
const DrakenVector* draken_vector_unwrap(PyObject* obj);
const DrakenVector* draken_array_child_unwrap(PyObject* obj);  // child of a DRAKEN_ARRAY

// Own — wrap hand-allocated (draken_malloc) buffers in a NEW Python Vector handle.
// Ownership of data/validity transfers to the Vector (freed on GC).
PyObject* draken_vector_own_raw(void* data, uint8_t* validity,
                                uint32_t length, DrakenType type);
PyObject* draken_vector_own_dict_i64(...);   // dict-encoded int64
PyObject* draken_vector_own_string(...);     // string-family
PyObject* draken_vector_own_array(...);      // DRAKEN_ARRAY[string]
PyObject* draken_vector_own_timestamp(...);  // DRAKEN_TIMESTAMP64

// Own a VecResult op result. draken_vector_own is C++-only; from C, use the
// C-linkage trampoline draken_vecresult_own_c.
PyObject* draken_vector_own(VecResult res);        // C++ only
PyObject* draken_vecresult_own_c(VecResult res);   // C-linkage
```

Buffers handed to any `draken_vector_own_*` function must be allocated with the Draken allocator
(`draken_malloc`). Because `draken_native` is loaded with `RTLD_GLOBAL` by `draken/__init__.py`, these
symbols are resolved at consumer extension load time with no additional linkage step.

---

## Quickstart

```python
from datetime import date
import draken.draken_native as dn

# Build vectors
ids    = dn.vector_from_sequence([1, 2, 3, None, 5])
names  = dn.vector_from_string_sequence(["alice", "bob", "carol", None, "eve"])
active = dn.vector_from_bool_sequence([True, False, True, None, True])

# Gather rows by explicit index
result = names.take([0, 2, 4])

print(result.to_pylist())   # ['alice', 'carol', 'eve']
print(ids.sum())            # 11
print(ids.min(), ids.max()) # 1 5

# Dict-encoded column (common from Parquet)
codes_vec = dn.vector_from_string_dict_sequence(
    ["cat", "dog", "cat", "bird", "dog", "cat"]
)
print(codes_vec.type)       # DrakenType.VARCHAR
print(codes_vec.to_pylist())

# Range predicate
ages   = dn.vector_from_sequence([10, 25, 42, 17, 33])
adults = ages.between(18, 99, inclusive=True)
print(adults.to_pylist())   # [False, True, True, False, True]
```

---

## Key Rules

- **`selection` is never NULL.** Do not NULL-check it.
- **`validity == NULL` means all-valid.** Do not allocate a validity bitmap for all-non-null columns.
- **No fallbacks.** Fail fast and explicitly on type mismatches. Do not silently coerce.
