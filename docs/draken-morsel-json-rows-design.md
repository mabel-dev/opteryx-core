# Draken Morsel JSON Row Builder Design

## Scope

Design a fast, Draken-native path to convert a `Morsel` into per-row JSON objects, emitted as a Draken `StringVector` of UTF-8 JSON bytes.

Primary use case:

- `Morsel -> ["{\"col\":1}", "{\"col\":2}", ...]`

This document defines the serialization design only. It does not define cursor API changes, HTTP streaming, or generalized JSON document construction beyond flat row objects.

---

## Goals

1. Serialize Draken morsels to JSON rows without Arrow conversion.
2. Keep Python object creation out of the hot row loop.
3. Return a Draken-native string result first, with Python `list[str]` only as an outer wrapper when needed.
4. Support the common scalar Draken vector types with direct Cython/C-level encoders.
5. Preserve null semantics correctly.
6. Never fall back to Arrow, Python row materialization, or Python JSON serialization.

## Non-Goals (v1)

1. Pretty-printed JSON.
2. Nested column expansion into nested JSON objects.
3. Streaming directly to sockets or files.
4. A public stable wire-format contract beyond UTF-8 JSON text.

---

## Output Contract

Primary API:

```cython
cpdef StringVector morsel_to_json_rows(
    Morsel morsel,
    object columns=None,
    object raw_json_columns=None,
    bint omit_null_fields=False,
)
```

Behavior:

- Each output entry is one JSON object for one row.
- Output values are UTF-8 JSON bytes in a `StringVector`.
- A Python-facing helper may decode to `list[str]` if required by the caller.

Why `StringVector` first:

- It matches the existing Draken-native execution model.
- It avoids allocating one Python `str` per row in the hot path.
- It composes naturally with downstream export code.

---

## Placement

Preferred module:

- `opteryx/compiled/io/json_rows.pyx`
- `opteryx/compiled/io/__init__.py`

Rationale:

- This is output serialization, not aggregation logic.
- It should sit near other compiled IO/export boundaries rather than in query operators.

---

## Existing Interfaces We Should Reuse

The design depends on interfaces already present in the codebase:

- `Morsel.column(name)` and `Morsel.column_names`
- typed Draken vectors exposed from `morsel.column(...)`
- `StringVectorBuilder.with_counts(...)`
- `StringVectorBuilder.with_estimate(...)`
- `StringVectorBuilder.append_bytes(...)`
- `StringVector.c_iter()`

These are already the right primitives for a no-Arrow, low-allocation serializer.

---

## High-Level Design

Use a compiled two-pass serializer:

1. Build a per-column encoding plan once for the morsel schema.
2. Precompute serialized key fragments for every selected column.
3. First pass computes exact output byte lengths per row.
4. Allocate the output `StringVector` with exact total capacity.
5. Second pass writes each JSON row directly into the builder.

This mirrors the successful pattern already used elsewhere in Draken code:

- inspect schema once
- preallocate exactly or near-exactly
- write bytes directly

---

## Serialization Plan

For each selected column, create a small plan entry:

- column name bytes
- pre-escaped key prefix
  - first field form: `"col":`
  - subsequent field form: `,"col":`
- encoder kind enum
- vector reference
- null handling strategy

Suggested encoder enum:

- `ENC_INT`
- `ENC_FLOAT`
- `ENC_BOOL`
- `ENC_STRING`
- `ENC_DICT`
- `ENC_CONST`
- `ENC_RAW_JSON`
- `ENC_UNSUPPORTED`

`raw_json_columns` marks columns whose byte payload should be inserted as JSON values without quoting. This is intended for existing JSONB-style binary payloads that already contain valid JSON text.

---

## Fast Paths

## Int64 / Integer

Use stack-buffer decimal formatting and append ASCII bytes directly.

Properties:

- no Python conversion
- exact control over emitted bytes
- same style as existing Draken numeric-to-string kernels

## Float64

Use a compact round-trippable formatter.

Rules:

- finite values serialize as JSON numbers
- `NaN`, `+Inf`, `-Inf` serialize as `null`

This avoids emitting invalid JSON.

## Bool

Emit literal bytes:

- `true`
- `false`

## String

Read directly from Draken string buffers using the C iterator or direct view.

Escape only when necessary:

- `"`
- `\`
- control characters `< 0x20`

Everything else can be copied through unchanged.

## ConstantVector

Serialize the scalar once, then reuse the same encoded fragment for every row.

## DictionaryVector

Serialize each dictionary value once, cache encoded fragments by code, then copy by code in the row loop.

This is important for low-cardinality string/object columns.

---

## Unsupported Types

There is no fallback path.

If a column type does not have a native encoder, serialization fails fast with a clear exception.

v1 unsupported candidates unless native encoders are implemented:

- arrays
- interval values
- timestamps
- date/time
- arbitrary non-native vectors
- arbitrary binary values without an explicit JSON encoding policy

Failure is preferable to silently dropping onto a slower or semantically different path.

This keeps the implementation honest:

- Cython/C++ encoder exists, so the type is supported
- Cython/C++ encoder does not exist, so the feature is unavailable for that schema

---

## Null Handling

Default:

- JSON nulls are emitted as `"col":null`

Optional mode:

- if `omit_null_fields=True`, null-valued fields are skipped entirely for that row

Example:

```json
{"id":1,"name":null}
```

or

```json
{"id":1}
```

The default should be explicit null emission because it is simpler, more predictable, and cheaper.

---

## Row Builder Strategy

Recommended row construction logic:

1. Start with `{`.
2. For each selected column:
   - skip null field if `omit_null_fields=True`
   - append precomputed key prefix
   - append encoded value bytes
3. End with `}`.

Avoid per-row Python `dict` creation entirely.

Implementation note:

- use exact-length precomputation where practical
- otherwise use a reusable `std::string` scratch buffer with reserved capacity
- append final row bytes into `StringVectorBuilder`

---

## Two-Pass Allocation

### Pass 1: Measure

For each row, compute:

- braces
- commas
- key prefix lengths
- encoded value lengths

Accumulate:

- `row_lengths[i]`
- `total_bytes`

### Pass 2: Emit

Allocate:

```cython
builder = StringVectorBuilder.with_counts(num_rows, total_bytes)
```

Then write each row exactly once.

Why this matters:

- avoids repeated builder growth
- avoids repeated string reallocations
- keeps output generation cache-friendly

---

## Type-Specific Notes

## Strings Are Text

For JSON-object serialization, `StringVector` should be treated as UTF-8 text content, not arbitrary binary blobs.

If arbitrary binary data needs to be supported later, define a separate policy:

- reject
- base64 encode
- hex encode

That policy should not be implicit in v1. Until one is implemented natively, reject the column.

## Existing JSON / JSONB Columns

For existing JSON payload columns already stored as bytes containing JSON text:

- use `raw_json_columns`
- validate policy at the call site, not in the hot loop

This avoids double-encoding:

- correct: `"payload":{"a":1}`
- incorrect: `"payload":"{\"a\":1}"`

## Temporal Types

If implemented natively, emit ISO-8601-compatible text and quote it as JSON strings.

If not implemented natively, reject the schema.

---

## API Layering

Recommended layering:

1. Compiled core:
   - `morsel_to_json_rows(...) -> StringVector`
2. Thin Python helper:
   - `morsel_to_json_strings(...) -> list[str]`
3. Export/cursor integration:
   - use `StringVector` directly where possible

This preserves performance while keeping the caller surface simple.

---

## Integration Points

Natural consumers:

- final result export
- JSONL writing
- HTTP/REST row streaming
- trace/debug output where row JSON is needed

This should sit after projection/rename work is complete, so it sees the final visible schema rather than internal column identities.

---

## Testing Plan

Unit coverage should include:

1. empty morsel
2. single-column numeric rows
3. mixed scalar types
4. strings requiring escaping
5. null emission
6. omit-null-fields mode
7. dictionary-backed columns
8. constant columns
9. raw JSON insertion
10. unsupported-type rejection correctness

Performance tests should include:

1. narrow numeric schema
2. wide mixed schema
3. long-string heavy schema
4. low-cardinality dictionary schema
5. comparison against Python `json.dumps(row)` over `to_pylist()`

The benchmark to beat is not generic Python JSON alone; it is the full cost of:

- materializing Python rows
- then serializing them

That entire path should be avoided.

---

## Rollout Plan

## Phase 1

- flat object rows only
- direct encoders for int, float, bool, string
- null emission
- `StringVector` output

## Phase 2

- constant and dictionary fast paths
- `raw_json_columns`
- optional Python `list[str]` convenience wrapper

## Phase 3

- temporal native formatters
- array/object native encoders
- integration into result export paths

---

## Summary

The correct design is not:

- `morsel.to_arrow()`
- `table.to_pylist()`
- `json.dumps(row)` in Python
- any hidden fallback path for unsupported types

The correct design is:

- stay in Draken
- compile a per-schema serializer plan
- use two-pass sizing
- write JSON row bytes directly into a `StringVectorBuilder`
- support only types with native Cython/C++ encoders
- fail fast for everything else

That gives the right performance shape for JSON row export and keeps the implementation aligned with the rest of the Draken-native engine work.
