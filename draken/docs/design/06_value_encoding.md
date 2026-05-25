# Draken Value Encoding (DRAFT)

> Status: DRAFT. `00_data_model.md` describes the *access* model (`data[selection[i]]`)
> but quietly assumes **fixed-width** elements. This doc says what `data` physically
> *is* for each `DrakenType`, where **parameterized** type metadata lives, and how
> variable-length / nested / bit-packed types map onto the unified view.

## The split: fixed-width is the easy case

For fixed-width scalars, `data` is a plain `T[]` and `data[selection[i]]` is element
`i`. Covers: `int8/16/32/64`, `float32/64`, `date32` (int32 days), `time`/`timestamp`
(int64), `decimal` (see below), `interval`, scalar `fp16`. Width is a property of the
type. Nothing more to say — these are why the unified model is clean.

The rest of this doc is the types where "what is `data`" needs care.

## Parameterized types — where do the parameters live? (the conversation)

`DrakenType` is a bare tag and is the *dispatch* key — but several types need extra
metadata that is **not** a dispatch key yet **is** required for correctness:

| Type            | Parameter(s)            | `draken_old` storage today |
|-----------------|-------------------------|----------------------------|
| `decimal`       | precision, scale        | `int8 _precision`, `int8 _scale`; values = int64 unscaled |
| `timestamp`/`time` | unit (s/ms/us/ns), tz? | `_unit_code` (0..3); no tz field (UTC-normalized) |
| `vector` (fp16) | dimension               | `Py_ssize_t _dimensions` |

These live as ad-hoc fields on per-type Cython classes today — which the C++-first
model removes. They need a home. Options:

- **(a) A `type_param` field on `DrakenVector`** — one word, interpreted per type
  (decimal: `precision<<8 | scale`; timestamp: unit code; vector: dimension). Simple,
  travels with the vector. Cost: struct grows (a `uint32` pushes it 40 → 48 bytes on
  LP64; still ≤ one cache line). Vector dimension wants ~`uint32`, so this is the
  field width that matters.
- **(b) A separate parameterized-type descriptor** (pointer) — keeps the hot struct
  lean; an indirection for the rare types that need it.
- **(c) Schema/column-level** — params carried out-of-band, vector stays bare.

Recommendation to react to: **(a) a single `uint32 type_param`**, type-interpreted,
defaulting to 0/unused. It keeps params with the vector (no side lifetime), and
`uint32` covers vector dimension. Timezones: **UTC-normalize timestamps** (store unit
only) and handle tz at bind/display — otherwise tz needs more than a small code and
forces option (b). Decide tz explicitly.

/JJ/ we need a logical type concept and I'd have thought things like scale/precision would live at the same level as the statistics (min/max/null_count)

### Resolved: physical type vs logical type (supersedes the inline `type_param` idea above)

Adopt the **physical / logical type split** — exactly DuckDB's `PhysicalType` vs
`LogicalType`:

- **Physical type** = `DrakenType` on the hot struct: the *storage layout* and the
  *dispatch key*. One physical type backs many logical types (int64 storage backs
  BIGINT, DECIMAL(≤18), TIMESTAMP, DATE-as-int, …).
- **Logical type** = the SQL type **with its parameters** (precision/scale, unit, tz,
  dimension), in a **descriptor carried at the vector level** — the same *home* as the
  statistics side-channel, per your note. Crucial difference from stats: the logical
  type is **mandatory** for parameterized types (you can't do decimal arithmetic
  without the scale), whereas stats are optional / "don't know." Same home, opposite
  optionality.

> **RESOLVED — logical-type descriptor: out-of-band, interned, immutable, mandatory.**
> - **Home:** out-of-band, keyed by column (the `00` canonical-struct decision) — NOT
>   a field on the 40-byte hot struct. Shares the *location* with the stats
>   side-channel but **not its nullability**.
> - **Optionality (opposite of stats):** stats absent = "don't know" (fail-safe).
>   Logical type absent for a *parameterized* physical type = **hard error** (you
>   cannot interpret decimal/timestamp/vector data without it). It is **mandatory and
>   non-nullable** for those types. Do not model logical-type and stats as one optional
>   blob — that would silently lose scale.
> - **Lifetime:** **interned, immutable, shared** via a small global registry. A
>   vector holds a **borrowed pointer** to its `LogicalType` — no per-vector
>   allocation, no per-vector free, stays entirely out of the RAII churn discussion in
>   `01`. All DECIMAL(10,2) vectors share one descriptor instance.


The hot inner loop dispatches on the **physical** type and never reads the logical
type; coercion/semantics/display read the logical type **at op entry**, not per row.

Consequences (your answers):
- **Decimal — do both.** Logical `DECIMAL(p,s)` → physical **int64** for p ≤ 18
  (fast, common) or **int128** for p ≤ 38. Physical type = storage width; logical type
  = (p, s).
  <!-- /opus/ "Do both" means int64-decimal and int128-decimal are two PHYSICAL types,
  so they're two more cells in the promote-at-dispatch matrix (02): int64-dec × float,
  int128-dec × int64-dec, etc. Two practical notes for the kernels:
   - int128 has NO native SIMD on either target (NEON and AVX2 are ≤64-bit integer
     lanes), so int128-decimal kernels are scalar — fine, but don't expect the int64
     compare/reduction SIMD wins to carry over. Keep int64-decimal as the fast common
     path (it already is) and treat int128 as the correct-but-scalar tier.
   - DECIMAL arithmetic that overflows int64 must PROMOTE int64→int128 mid-op
     (multiply of two DECIMAL(18,…) needs ~36 digits). Decide where that overflow
     promotion lives — almost certainly at the same dispatch/promotion layer as
     cross-type, not inside the kernel. Worth one line so it isn't discovered later. -->

- **Timestamp — carry tz.** Logical `TIMESTAMP` carries unit + timezone (assume UTC
  when unspecified); physical is int64. tz living in the logical descriptor is exactly
  why a tiny inline `type_param` was the wrong home. **Library (resolved): Google
  CCTZ** (`google/cctz`) — **but prefer C++20 `<chrono>` zoned_time if/when we're on
  C++20** (stdlib, no vendor; the standardised form of Howard Hinnant's `date`). We're
  not on C++20 today, so CCTZ is the vendor for now. Either way it needs the **IANA tz
  database** at runtime — the one §4 dependency cost (vs fixed-offset, which loses
  DST/named zones).
  <!-- /opus/ RESOLVED (architect, /JJ/ "store as offsets"): **fixed-offset only.** The
  logical descriptor carries a numeric UTC offset (minutes, ±HH:MM); physical stays
  int64 UTC instant. NO tzdata/CCTZ dependency — stays inside §4's zero-dep goal, and
  drops the CCTZ line from the 08 sign-offs. Scope consequence to state plainly: named
  zones ("America/New_York") and DST-aware arithmetic are NOT supported in v1; we store
  and round-trip the offset the data arrived with. If a workload later needs named-zone
  semantics, that is a separate, explicit vendoring decision (tzdata) — not a silent
  upgrade of this field. CLOSED. -->

- **Vector(fp16)** — dimension is a logical-type parameter too.

## Strings — German strings + arena (carry forward from `draken_old`)

Variable-length, but made fixed-width-per-row so it fits `data[selection[i]]`:

- `data` is an array of **16-byte slots** (`DrakenStringSlot`,
  `draken/core/string_slot.h`):
  - short (≤ 12 bytes): `[uint32 length][12 inline bytes]`
  - long  (> 12 bytes): `[uint32 length][uint32 prefix][uint32 hash32][uint32 arena_offset]`
    - `prefix` is the first four payload bytes encoded big-endian for unsigned lexicographic prefix comparison.
    - `hash32` is the lower 32 bits of `XXH3_64bits(payload, length)`.
    - `arena_offset` is a byte offset into the vector's arena; offsets are capped at 4 GB per vector.
- An owned **byte arena** (`DrakenStringArena`) holds long-form payloads; short
  strings touch no arena.
- A single 8-byte `length || prefix` load short-circuits many comparisons without
  touching the arena. Equality and hashing for long strings may also use `hash32`
  on the no-arena path; ordering still compares payload bytes when prefixes tie.

So for strings: `data` = slots, `selection` = identity/codes (dict strings = unique
slots + codes), arena = a side buffer **owned by the vector** (per `01_ownership.md`).
The unified access pattern is preserved; only the per-element *interpretation* differs.
Port `string_slot.h`, the arena, and `DrakenVarBuffer` from `draken_old`.

## Nested: arrays (offsets + child) and structs (JSON)

### Arrays / list — resolved: offsets + child, the Arrow/DuckDB way

A composite, not a single `data` array: an `int32 offsets[length+1]` plus a **child
`DrakenVector`**. Logical row `i` is `child[offsets[sel[i]] : offsets[sel[i]+1]]`.
Recursion allowed (array-of-array → child is itself an array vector). This is the one
shape that does **not** reduce to one flat `data` buffer. Port the `draken_old`
`DrakenArrayBuffer` (offsets + child + element type).

/JJ/ how do duckdb, clickhouse, etc handle nested types?

**How others do it (answer) — the consensus is *offsets + a child column*:**
- **Arrow**: `List<T>` = int32 offsets buffer + child array; `Struct` = parallel
  child arrays.
- **DuckDB**: `LIST` = a child Vector + per-row `{offset, length}`; `STRUCT` =
  parallel child Vectors; `MAP` = `LIST<STRUCT<key,value>>`. Recursive Vectors.
- **ClickHouse**: `Array(T)` = `ColumnArray` = a flattened child column + a cumulative
  offsets column; `Tuple`/`Nested` = parallel columns.

So **array = int32 offsets + a child `DrakenVector`** (recursive) is the standard, and
a future `STRUCT` is **parallel child vectors**. Importantly, none of them give nested
types a *new* struct family — they reuse the one column/vector type with a child
pointer + offsets. That also answers the "separate buffer structs vs unify" question
below: **unify** — one tagged vector whose *auxiliary* (string arena, or array
offsets + child) is interpreted by the physical type, rather than keeping
`DrakenVarBuffer` / `DrakenArrayBuffer` / embedding-buffer as distinct C structs.
(DuckDB = one `Vector` + variant auxiliary; Arrow = one `ArrayData` +
`buffers[]`/`children[]`. ClickHouse's class-per-type is the model we *don't* want —
we already rejected a per-type class hierarchy in `02`.)

### Structs (and maps) — resolved: JSON documents (dirty but cheap)

A `STRUCT` value is stored as a **JSON document in a `string` value** — the *logical*
type is STRUCT, the *physical* type is `string` (German-string slots + arena). Field
access (`s.field`) is JSON-path extraction at op time, using the vendored **`yyjson`**.
Maps go the same way (JSON object).

- **Cheap:** zero new physical machinery — reuses string storage, dict-encoding,
  hashing, and the arena wholesale; heterogeneous/sparse structs work for free.
- **Dirty (accepted):** no columnar per-field access (parse the doc to read one
  field), no per-field stats/pushdown, larger than columnar (JSON text + repeated
  keys), parse cost per access. Fine for a not-hot path; revisit only if struct field
  access becomes a measured bottleneck — then promote to parallel child vectors (the
  DuckDB way).

**Scope (resolved): extract-only.** Structs are accessed via field extraction
(`->`, `->>`, path ops) only — **whole-struct equality / GROUP BY / JOIN on a struct
value are out of scope for v1.** That deliberately sidesteps the canonical-serialization
problem (no whole-value comparison ⇒ no canonical-form requirement). Numeric fidelity
on extraction is fine: `yyjson` preserves integer precision (parses to `sint64`/`uint64`
when in range) and exposes raw number text, so extracting a bigint/decimal field does
not silently round through a double. (Lossy round-trip would only bite whole-struct
comparison, which we don't support.)

> **RESOLVED — STRUCT/MAP are EXTRACT-ONLY in v1 (architect, /JJ/).** Supported:
> field extraction (`s.field`, JSON-path) via vendored `yyjson`. **Unsupported in v1
> (must raise a clear "unsupported", never silently mis-answer):** equality (`=`),
> `GROUP BY`, `DISTINCT`, `JOIN`, and `ORDER BY` on a **whole** struct/map value. This
> is what makes the JSON-in-string representation *correct* rather than merely cheap:
> by not offering whole-value comparison we never expose the JSON-text-equality hole
> (key order / whitespace / lossy numerics). Extracted scalar fields are ordinary
> typed values and behave normally. If whole-struct comparison is ever required, the
> trigger is to promote to **parallel child vectors** (the DuckDB way) — NOT to start
> canonicalizing JSON.

<!--
/opus/ CLOSED. The extract-only scope removes the correctness hole at the source — no
canonical-JSON machinery needed, no numeric-fidelity foot-gun, because whole-value
equality/grouping/join simply isn't offered. The one implementation requirement: those
ops must FAIL LOUDLY on a struct operand (typed error at bind time ideally), not fall
through to hashing the JSON bytes — otherwise the hole reopens silently. Make that a
bind-layer check (03_binding / opteryx binder), not just a runtime guard. -->


Consequence for the physical/logical split: `STRUCT`/`MAP` are **logical** types with
**physical type = `string`** — no struct-specific buffer. So the only genuinely
composite *physical* shape is **array** (offsets + child); everything else is
fixed-width or string-backed.

## bool — bit-packed

`data` is a bit array (1 bit/value, LSB-first). `data[selection[i]]` reads bit
`sel[i]`. Dense reads bits sequentially; dict/permutation gather bits by code.
Validity is a separate bitmap (don't conflate the value bits with the null bits).

/JJ/ is this breaking an invariant?

**No — it's the same bit-addressing the validity mask already uses.** `data[selection[i]]`
is shorthand for "logical value = `element_at(data, selection[i])`," and `element_at`
is type-specific. For sub-byte types the element is a bit, so `element_at` is a
bit-extract — exactly what we already do for the validity bitmap. Bool value access is
`bit(data, selection[i])` (bit-extract + the usual selection indirection); validity is
`bit(validity, i)` (bit-extract, logical-indexed). The bit-addressing is not new, so no
invariant is broken — only the literal pointer-index form doesn't apply to sub-byte
elements.

Decision to make explicit: **bit-packed (1 bit/value)** — compact, consistent with
validity, what `draken_old` does — **vs 1 byte/value** (DuckDB unpacks bool to int8 for
execution: uniform pointer-indexing, ~8× memory). Recommendation: keep **bit-packed**
(memory-bound engine; predicate *results* — `BoolVector` masks — are inherently
bit-packed anyway) and treat bool as the documented sub-byte element with a bit accessor.

## null type

No `data`; `length` rows, all null. Trivial.

/JJ/ full validity array?

**No full validity array.** The NULL type is self-describing: `type == NULL` ⇒ every
row is null, so the vector carries **no `data` and no validity buffer** — readers
short-circuit on the type tag. (Contrast: a *typed* column that happens to be entirely
null still carries an all-zero validity bitmap, because its type says values exist —
the type tag alone doesn't imply nullness there.)

## Encoding ↔ ops

Each type's value handling lives in its typed kernel (`02_dispatch_and_ops.md`):
string compare = `len||prefix` then arena tail; decimal compare/arith = scale-aware
(fast path when scales match); array ops recurse into the child; bool ops are
bitwise. The encoding here defines what those kernels read.

## Open questions

- [ ] Parameterized-type metadata home: `type_param` field (a) vs descriptor (b) vs
      schema-level (c)? Accept the 40→48B struct growth for (a)? /JJ/ see comment above
- [ ] Decimal storage: int64 unscaled (`draken_old`, ~18 digits) vs `decimal128`
      (38 digits) vs both? Width drives the value layout. /JJ/ I guess now it the time to do both
- [ ] Timestamp timezone: UTC-normalize (recommended, unit-only) or carry tz? /JJ/ carry tz, assume UTC if not specified
- [ ] Arrays: reuse `DrakenVector` (`data = offsets` + child ptr) or keep a distinct
      `DrakenArrayBuffer` that the unified view adapts? /JJ/ see comment above
- [ ] Do strings/arrays/vectors keep separate buffer structs (`DrakenVarBuffer`,
      `DrakenArrayBuffer`, embedding buffer) as today, or unify under one tagged buffer? /JJ/ how do other DBs do it?

## Source to port from
`draken_old/src/core/string_slot.h`, `draken_old/src/core/string_arena.*`,
`draken_old/src/core/buffers.h` (`DrakenVarBuffer`, `DrakenArrayBuffer`),
`draken_old/vectors/{string,decimal,timestamp,vector_vector,array}_vector.*`.
