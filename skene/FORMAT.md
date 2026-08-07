# The `.skene` File Format

**Version 1 — DRAFT. Not frozen.** Byte layouts below are implemented and
tested, but v1 is not released: fields may still change without a version bump
until it is. Once frozen, every change follows §12.

This is the **normative specification**: it defines the bytes, and is complete
enough to write an independent reader from. The *rationale* — why Parquet and the
existing IPC format were rejected, what was traded away, which decisions are
still open — lives in `opteryx-core/docs/SKENE_FILE_FORMAT_DESIGN.md` and is not
repeated here.

The reference implementation is this repository. Where this document and
`include/skene/format.h` disagree, **format.h is authoritative and this document
is stale** — fix the document.

---

## 1. What it is

`.skene` stores one **row group** of [draken](https://github.com/mabel-dev)
columnar vectors, losslessly.

It exists because Parquet cannot express draken's logical types. An IPv4 column
is a `UINT32` refined by an `IPV4` descriptor; Parquet stores the 32 bits and
loses the refinement on every round trip, so consumers must recover it from a
sidecar. `.skene` carries the descriptor natively. It likewise **restores** a
column's dictionary encoding and layout hints rather than re-deriving them.

It is **not** portable and no foreign reader is promised. It is not a Parquet
replacement for interchange — Parquet remains the default for stored datasets;
`.skene` is for the cases where the draken-native round trip matters.

---

## 2. Conventions

| | |
|---|---|
| Byte order | **Little-endian**, for every multi-byte field and every stored buffer. Declared in the head; a reader on a big-endian machine MUST reject the file rather than byte-swap. |
| Integers | `u8`/`u16`/`u32`/`u64` unsigned, `i16`/`i32`/`i64` signed, two's complement. |
| Offsets | **Absolute from byte 0 of the file**, unless stated otherwise. |
| Structs | Packed — no implicit padding. Explicit `reserved`/`pad` fields MUST be written as zero. A reader MUST **reject** a non-zero `reserved` field in the head or tail (§4.3); `pad` inside footer records is covered by the footer checksum and MAY be ignored. |
| Checksums | XXH3-64 over the bytes named, algorithm identified in the head. |
| Alignment | None is guaranteed. A reader MUST NOT cast a pointer into the buffer to a multi-byte type; copy the bytes out. |
| Keywords | MUST / MUST NOT / SHOULD / MAY as in RFC 2119. |

`draken` type tags (`DrakenType`), vector layout flags (`DrakenVector.flags`),
string slot layout (`DrakenStringSlot`) and logical-type enumerations
(`LogicalKind`, `TimestampUnit`) are **draken's**, referenced by value and never
redefined here. Their authority is `draken/core/buffers.h`,
`draken/core/string_slot.h` and `draken/logical_type.h`.

---

## 3. File structure

```
 byte 0
 ┌────────────────────────────────────────────┐
 │ HEAD                             16 bytes  │  magic FIRST
 ├────────────────────────────────────────────┤
 │ DATA region                                │
 │   column 0 sections (then its children's)  │  a column subtree is contiguous
 │   column 1 sections …                      │
 ├────────────────────────────────────────────┤
 │ INDEX region                               │  optional sections
 ├────────────────────────────────────────────┤
 │ FOOTER                     footer_bytes    │
 ├────────────────────────────────────────────┤
 │ TAIL                             24 bytes  │  magic LAST
 └────────────────────────────────────────────┘
 EOF
```

Magic appears at **both** ends. The head magic rejects an unrelated or
front-truncated object on the first four bytes read; the tail magic plus
`footer_bytes` locates the footer in one range request with no linear parse.

**All sections of one column, and of its descendants, are contiguous**, so
reading a column is a single range request.

The INDEX region is adjacent to the footer so that one range request can fetch
the footer together with every filter and index.

### 3.1 Read procedure

1. Read the last 24 bytes. Validate the tail (§4.2).
2. Read `[filesize - 24 - footer_bytes, filesize - 24)`. Verify
   `footer_checksum`. A reader wanting indexes SHOULD extend this request
   backwards over the INDEX region — it is contiguous with the footer.
3. Parse the footer (§5). Validate (§11).
4. Prune using statistics (§8) and optional filters (§9).
5. For each surviving column, read its section extents and reconstruct (§7).

The head MAY be validated at any point; a reader that never reads byte 0 (a
range-GET reader) MUST still validate the tail's `version`, `endianness` and
`checksum_algorithm`, which duplicate the head's for exactly this reason.

---

## 4. Head and tail

### 4.1 Head — 16 bytes at offset 0

| offset | size | field | value |
|---|---|---|---|
| 0 | 4 | `magic` | `0x4E454B53` — ASCII `SKEN` |
| 4 | 2 | `version` | format version, `1` |
| 6 | 1 | `endianness` | `0` little, `1` big |
| 7 | 1 | `checksum_algorithm` | `0` XXH3-64 |
| 8 | 8 | `reserved` | `0` |

> **Bytes 0–5 are frozen for all time.** `magic` at offset 0 and `version` at
> offset 4 MUST NOT move or change width in any future version. A build reads at
> most two versions (§12), so stepping an old file forward requires identifying
> it with a build that cannot read it. Everything from offset 6 onward is free to
> change with a version bump.

### 4.2 Tail — 24 bytes, ending at EOF

| offset from tail start | size | field |
|---|---|---|
| 0 | 4 | `footer_bytes` — length of the FOOTER region |
| 4 | 8 | `footer_checksum` — over exactly `footer_bytes` of footer |
| 12 | 2 | `version` — MUST equal the head's |
| 14 | 1 | `endianness` — MUST equal the head's |
| 15 | 1 | `checksum_algorithm` — MUST equal the head's |
| 16 | 4 | `reserved` — `0` |
| 20 | 4 | `magic` — `0x4E454B53`; the final four bytes of the file |

### 4.3 Every byte is verified

The footer has its own checksum and each section has its own, but **nothing
checksums the head or the tail** — they are where a checksum would have to be
recorded, so they cannot cover themselves.

Every other field in them is therefore constrained to an exact value or a small
set: `magic`, `version` (within the read window), `endianness`,
`checksum_algorithm`, and `footer_bytes` (against the object size), plus the
requirement that the head and tail **agree** on the three duplicated fields. That
leaves only `reserved`, which is why a reader MUST reject a non-zero `reserved`
rather than ignore it: an ignored field is an unverified field, and these 12
bytes are the only ones in the file no checksum protects.

Checking costs nothing in forward compatibility. Any future version that gives
those bytes meaning bumps `version`, and a reader outside its window rejects the
file on the version alone.

> A conforming implementation SHOULD test this by sweeping a corrupted bit
> through every byte position of a known-good file and asserting that **every**
> one is rejected. Any accepted position is a region of the file nothing
> verifies.

---

## 5. Footer

The footer is a flat byte stream, parsed sequentially. It contains no internal
offsets and requires no allocation to walk. Order:

1. **File header** (§5.1) — 48 bytes
2. **Writer tag** — `writer_tag_bytes` bytes, provenance only
3. **Column directory** — `column_count` entries (§5.2), each of which nests its
   own children
4. **Section directory** — `section_count` entries (§5.3), 36 bytes each
5. **Statistics blobs** (§8), in the same order columns appear in the directory

The footer MUST end exactly at the start of the tail. A reader MUST treat a
trailing or short remainder as `kMalformed`.

### 5.1 File header — 48 bytes

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `row_count` | logical rows in the row group |
| 8 | 4 | `column_count` | **top-level** columns; children are nested, not counted here |
| 12 | 4 | `section_count` | total entries in the section directory, children included |
| 16 | 16 | `file_uuid` | all-zero means unset |
| 32 | 8 | `created_at_unix_us` | provenance only, **never load-bearing** |
| 40 | 4 | `writer_tag_bytes` | length of the tag that follows |
| 44 | 4 | `file_flags` | `0`; reserved |

### 5.2 Column directory entry

A fixed 72-byte head, then variable-length parts, then children — depth first.

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `field_id` | stable identity across schema evolution; `0` means unassigned. Assigned by the catalog, not by this format. Readers matching columns by **name** break on rename. |
| 4 | 4 | `name_bytes` | length of the name that follows the head |
| 8 | 4 | `type` | `DrakenType`, verbatim |
| 12 | 1 | `vector_flags` | `DrakenVector.flags`, **verbatim** (§7.5) |
| 13 | 1 | `logical_present` | `0`/`1`; a `LogicalTypeDescriptor` follows the name |
| 14 | 1 | `selection_kind` | §7.2 |
| 15 | 1 | `value_order` | §7.6 |
| 16 | 4 | `length` | logical row count |
| 20 | 4 | `data_length` | physical value count |
| 24 | 4 | `child_count` | `0` except `DRAKEN_ARRAY`, which has `1` |
| 28 | 4 | `section_index` | index of this column's first **required**-section directory entry |
| 32 | 4 | `section_count` | how many required entries belong to **this** column (children have their own) |
| 36 | 4 | `stats_bytes` | `0` == no statistics tracked (§8) |
| 40 | 8 | `string_slot_count` | string family only; `0` otherwise |
| 48 | 8 | `string_arena_used` | string family only |
| 56 | 8 | `string_arena_cap` | string family only |
| 64 | 1 | `string_payloads_elided` | string family only (§7.4) |
| 65 | 3 | `pad` | `0` |
| 68 | 4 | `index_section_index` | first **optional**-section directory entry (§5.4) |
| 72 | 4 | `index_section_count` | how many optional entries belong to this column |
| 76 | 4 | `reserved` | `0` |

### 5.4 Two slices, because there are two regions

A column's required sections live in the DATA region and its optional ones in
the INDEX region next to the footer, so each column carries **two** contiguous
directory slices rather than one.

This is what makes a pruning read one request: fetch the footer and the index
region together, decide from the statistics and filters which columns are worth
reading, and only then issue range requests for their data. A single slice would
force the two regions to interleave, and an index scattered through the data
region is an index you must read the data to reach.

A reader MUST reject a **required** section kind appearing in the index slice.
Unknown kinds are skipped there, so a required section in that slice would be
silently ignored — the exact failure the required/optional split exists to
prevent.

Immediately following the head, in order:

1. `name_bytes` bytes of column identity (not NUL-terminated)
2. a `LogicalTypeDescriptor` (§6), only if `logical_present == 1`
3. `child_count` complete child entries, recursively

### 5.3 Section directory entry — 36 bytes

| offset | size | field |
|---|---|---|
| 0 | 2 | `kind` (§7.1) |
| 2 | 2 | `encoding` (§7.7) |
| 4 | 8 | `offset` — absolute from file start |
| 12 | 8 | `stored_bytes` — length on disk, post-encoding |
| 20 | 8 | `plain_bytes` — length after decoding; equals `stored_bytes` when `PLAIN` |
| 28 | 8 | `checksum` — over the **stored** bytes, not the decoded ones |

---

## 6. Logical type descriptor — 12 bytes

Present only when `logical_present == 1`. This is the POD projection of draken's
`LogicalType`, which in memory is a borrowed pointer into a process-global
interned registry and MUST NOT be written as one. A reader re-interns it.

| offset | size | field | notes |
|---|---|---|---|
| 0 | 1 | `kind` | draken `LogicalKind` |
| 1 | 1 | `unit` | draken `TimestampUnit` |
| 2 | 2 | `offset_minutes` | signed; fixed UTC offset, not a named zone |
| 4 | 1 | `precision` | DECIMAL |
| 5 | 1 | `scale` | DECIMAL |
| 6 | 2 | `reserved` | `0` |
| 8 | 4 | `dimension` | VECTOR_FP16 embedding width |

**A descriptor is MANDATORY** — a writer MUST fail, and a reader MUST reject a
file lacking one — for `DRAKEN_TIMESTAMP64`, `DRAKEN_TIME32`, `DRAKEN_TIME64`,
`DRAKEN_DECIMAL`, `DRAKEN_DECIMAL128` and `DRAKEN_VECTOR_FP16`. Those physical
tags are uninterpretable alone.

`IPV4` is deliberately **not** in that list. It *refines* an already complete
`UINT32`, so its absence degrades an IPv4 column to a well-formed unsigned
integer column — a display and cast regression, never a wrong answer. Carrying it
anyway is the reason this format exists.

---

## 7. Columns

Every column is stored in draken's **general form**: a `data` array of
`data_length` values plus a `selection` of `length` codes, read uniformly as
`data[selection[i]]`. Dense and constant columns are degenerate dictionaries
(identity codes, all-zero codes), so there is one storage shape, not three.

### 7.1 Section kinds

**Required** — the column cannot be reconstructed without them:

| kind | name | payload |
|---|---|---|
| 1 | `DATA` | §7.3 |
| 2 | `SELECTION` | `length × u32` codes; present **iff** `selection_kind == STORED` |
| 3 | `VALIDITY` | `ceil(length / 8)` bytes, 1 bit per logical row, LSB-first, **set == valid**. Absent means every row is valid; a writer MUST NOT emit an all-valid bitmap (§7.8). Bits at or above `length` are padding and carry no meaning. |
| 4 | `STRING_SLOTS` | `string_slot_count × 16` bytes of `DrakenStringSlot`, verbatim |
| 5 | `STRING_ARENA` | `string_arena_used` bytes, verbatim. Absent when `string_arena_used == 0`. |

**Optional** — accelerators only:

| kind | name |
|---|---|
| 256 | `BLOOM` (§9.1) |
| 257 | `PERMUTATION` (§9.2) |
| 258 | `ZONE_MAP` (§9.3) |

> **The extensibility rule.** Kinds below **256** are required; kinds **256 and
> above** are optional. A reader MUST reject an unrecognised **required** kind
> and MUST silently skip an unrecognised **optional** kind.
>
> What makes skipping safe, and the constraint every future section MUST satisfy:
> **an optional section MUST be reconstructible from the required sections**, so
> ignoring it can only cost speed, never correctness. Anything carrying
> information not otherwise present is a required section and introducing one is
> a version bump. There is no third category, and "optional" is never a route for
> passing data to a reader that will ignore it.

### 7.2 `selection_kind`

| value | name | `SELECTION` section | reader constructs |
|---|---|---|---|
| 0 | `CONSTANT` | absent | the shared global zero selection |
| 1 | `IDENTITY` | absent | the shared global identity permutation |
| 2 | `STORED` | present | owned codes decoded from the section |

This is a **written fact**, not derived from `data_length` versus `length`.

> A writer MUST classify by **scanning the selection array's contents**, not from
> the counts. An all-distinct value-ordered column has `data_length == length`
> *and a genuine permutation selection*; a writer inferring `IDENTITY` from
> `data_length == length` would store no selection and silently reorder every
> row on read.

Consistency requirements, which a reader MUST enforce (§11):
`CONSTANT` implies `data_length == 1`; `IDENTITY` implies
`data_length == length`; every stored code MUST be `< data_length`.

### 7.3 `DATA` payload, by family

| family | payload |
|---|---|
| Fixed-width | `data_length × draken_type_itemsize(type, logical)` bytes, verbatim. `VECTOR_FP16`'s width comes from the descriptor's `dimension`. |
| `DRAKEN_BOOL` | `ceil(data_length / 8)` bytes, bit-packed, LSB-first |
| `DRAKEN_ARRAY` | `(length + 1) × i32` offsets — sized by the **logical** row count, since arrays are stored dense |
| `DRAKEN_NULL` | **no `DATA` section**, and no `VALIDITY`: the type alone states every row is null |
| String family | **no `DATA` section** — see §7.4 |

### 7.4 The string family

`DRAKEN_VARCHAR`, `DRAKEN_NVARCHAR`, `DRAKEN_VARBINARY`, `DRAKEN_VARIANT`.

In memory, `data` points at a `DrakenStringArena` whose `slots` and `arena`
members are **absolute pointers**. Those pointers are never written. Instead:

- the scalar fields live in the column directory (`string_slot_count`,
  `string_arena_used`, `string_arena_cap`, `string_payloads_elided`),
- the slot block is the `STRING_SLOTS` section,
- the payload bytes are the `STRING_ARENA` section.

A reader allocates a fresh block, copies both sections into it, and rebuilds the
two pointers. `owns_buffers` is **not** carried: it is `0` by construction,
because the reader's own ownership record governs the block.

Slots are position-independent — a long slot stores a `u32` arena **offset**, not
a pointer — so slots and arena are byte-for-byte relocatable.

#### `string_payloads_elided`

A **length-only** column records each value's length but deliberately never
materializes its bytes. It has no arena, and every long slot is stamped with the
trap offset `0xFFFFFFFF` (`STR_ELIDED_PAYLOAD_OFFSET`) so that any accidental
dereference faults immediately instead of returning adjacent memory.

Losing this flag turns that trap into a ~4 GB out-of-bounds read. Writing it
correctly is therefore not sufficient — **a reader MUST verify it**:

- `string_payloads_elided == 1` ⟹ `string_arena_used == 0`, **no** `STRING_ARENA`
  section, and **every** long slot's `arena_offset == 0xFFFFFFFF`.
- `string_payloads_elided == 0` ⟹ **every** long slot satisfies
  `arena_offset + length <= string_arena_used`.

Either violation MUST be rejected. Both checks are one linear pass over the
slots.

### 7.5 `vector_flags`

`DrakenVector.flags` is stored and restored **verbatim**. These are layout hints
(`SEL_IDENTITY`, `SEL_PERMUTATION`, `DICT_KEYS_SORTED`, `DICT_CODES_DENSE`,
`ROW_SORTED`, `ROW_SORTED_DESC`). Re-deriving them rather than restoring them is
precisely what disqualified Parquet.

A hint is never a correctness guarantee: a consumer that ignores every flag MUST
get the same answer via the uniform `data[selection[i]]` path.

### 7.6 `value_order`

| value | meaning |
|---|---|
| 0 | `AS_WRITTEN` — no ordering claim |
| 1 | `ASCENDING` — `data[0 .. data_length)` is sorted ascending and deduplicated |

When `ASCENDING`:

- `data[0]` and `data[data_length - 1]` **are** the minimum and maximum,
- `data_length` **is the exact distinct count**, not an estimate,
- a predicate resolves to a contiguous code interval by binary search.

A writer MUST NOT set this unless both properties hold. Deduplication MUST key on
the **bit pattern**, never on engine equality: under draken's float order
`-0.0 == 0.0`, so an equality-based dedup would collapse them and a column
containing `-0.0` would read back as `0.0`.

Types with no defined order MUST always be `AS_WRITTEN`: `DRAKEN_VARIANT` (no
collation), `DRAKEN_ARRAY` (no whole-array comparison), `DRAKEN_VECTOR_FP16`.

Null rows' selection codes MUST still be valid in-range indices; they are masked
by `VALIDITY` and MUST NOT introduce a value into `data` that no non-null row
references, or `data_length` ceases to be the exact distinct count.

### 7.7 Encodings

| value | name | applies to |
|---|---|---|
| 0 | `PLAIN` | anything; `stored_bytes == plain_bytes` |
| 1 | `BITPACK` | `u32` arrays (selection codes) at a fixed bit width |
| 2 | `DELTA_BITPACK` | ascending 4- or 8-byte integer arrays |
| 3 | `ZSTD` | any body; `plain_bytes` is the decoded size |

There is deliberately no bare `DELTA`: differences stored at the source width are
never smaller than the values, so nothing would produce one. Delta only pays
combined with bit packing.

`BITPACK` width comes from `data_length`, not from scanning for a maximum: every
code is already `< data_length`, so the bound is known before the array is read.

`DELTA_BITPACK` computes differences in **unsigned** arithmetic and wraps
deliberately. For an ascending signed array the wrapping unsigned difference is
the true step magnitude regardless of sign (`-5` → `3` gives `8`), and it cannot
overflow the way signed subtraction does when the array spans more than half the
type's range. It applies ONLY where ascending order is established by
construction — a value-ordered column — never inferred from data that happens to
look sorted.

`ZSTD` is applied **per section**, never to the whole file. Whole-file
compression is 0.7–5.7% smaller (measured on TPC-H) but a reader cannot
decompress a slice, so reading one column would mean fetching and decompressing
every column — destroying the property §3 exists for. Per-section keeps each
extent independently fetchable and independently decodable, and that is worth
the few percent.

A writer attempts `ZSTD` only where it measurably pays. Three gates, each set
from measurement (BENCHMARKS.md) rather than intuition:

**Encoding.** Only `PLAIN` bodies are candidates. A bit-packed or delta body has
already had its redundancy removed, so layering a general compressor over it
costs CPU for nothing.

**Section kind.** Only `DATA`, `VALIDITY`, `STRING_SLOTS` and `STRING_ARENA`.
The string family is where the bytes are and where they compress — 0.25x for
arenas, 0.43x for slots. The rest are excluded for cause: `BLOOM` is hash bits
and measures 1.00x, a `PLAIN` `SELECTION` is one bit packing declined and so is
high-entropy by definition, and `ZONE_MAP` is kilobytes per file.

**Size.** Only bodies of at least **10240 bytes**. Sections below that are 87%
of all sections but hold ~1.2% of the recoverable bytes.

These are writer-side policy, not reader obligations: a reader MUST decode any
`ZSTD` section it is given, on any kind, at any size.

Every encoder DECLINES when the result would not be smaller than plain, and the
writer then emits `PLAIN`. "Not worth it" is a normal outcome measured on actual
size, never a guess — so a compressed file is never larger than an uncompressed
one, section by section.

> Compression is **not** cosmetic for stored data. Measured on TPC-H, skene
> without it is 1.9–3.8x larger than the equivalent ZSTD Parquet; with it, 0.92–
> 1.09x. The cause is the string family: 16-byte slots plus a verbatim arena keep
> almost all their redundancy after the other encodings have run, and text
> columns dominate real tables. Spill is the exception and stays uncompressed —
> written once, read once, wall-clock bound.

### 7.8 All-valid bitmaps are not written

An absent `VALIDITY` section already means every row is valid, so a bitmap whose
every in-range bit is set states nothing. Writers MUST drop it rather than store
or compress it.

This is not a micro-optimisation. Producers supply redundant all-ones bitmaps as
a matter of course — every column of every TPC-H table arrives with one — and
they were ~400 KB per file of pure restatement. Dropping beats compressing on
every axis: the bytes leave the file entirely, there is no compress or decompress
cost, and the saving does not depend on the section clearing the size floor
above (an all-ones bitmap for a typical row group falls *below* it).

The check masks off the padding bits above `length`, which are meaningless and
must not decide the outcome. A column that comes back with no bitmap where it
went in with a redundant one is CORRECT: nullness is the contract, not the
presence of a buffer.

An unrecognised encoding on a **required** section MUST be rejected — the column
cannot be decoded. Adding an encoding for a required section is therefore a
version bump, unlike adding an optional section, which is free.

There is deliberately no affine/run encoding for selections: `selection_kind`
expresses identity and all-zero selections by storing **no section at all**.

`BITPACK` bodies begin with an 8-byte header: `u32 count`, `u8 bit_width`
(`0..32`, where `0` means every value is zero), `u8 pad[3]`; then
`ceil(count * bit_width / 8)` bytes, LSB-first, no padding between values.

`DELTA_BITPACK` bodies begin with `u32 count`, `u8 item_bytes` (4 or 8),
`u8 bit_width` (`0..64`), `u16 pad`; then `item_bytes` holding the first value
verbatim; then `ceil((count-1) * bit_width / 8)` packed differences.

---

## 8. Statistics

Per column, all optional. **Absent means "not tracked", never "zero".**

A blob is `stats_bytes` long and appears in the statistics region in the same
depth-first order as the column directory, skipping columns whose `stats_bytes`
is `0`. A reader encountering a blob **longer** than it understands MUST read the
prefix it knows and skip the remainder — this is what lets statistics be added
without a version bump.

Version 1 blob — 48 bytes:

| offset | size | field |
|---|---|---|
| 0 | 4 | `flags` — bitmask below |
| 4 | 4 | `reserved` |
| 8 | 8 | `min_ordinal` (`i64`) |
| 16 | 8 | `max_ordinal` (`i64`) |
| 24 | 8 | `null_count` |
| 32 | 8 | `sum_low` — low half of an `i128` |
| 40 | 8 | `sum_high` — high half of an `i128` |

| bit | flag | present when |
|---|---|---|
| 0 | `MIN` | `min_ordinal` is meaningful |
| 1 | `MAX` | `max_ordinal` is meaningful |
| 2 | `NULL_COUNT` | |
| 3 | `SUM` | |
| 4 | `ROW_SORTED` | mirrors `DRAKEN_ROW_SORTED` |
| 5 | `ROW_SORTED_DESC` | direction; meaningful only with bit 4 |

A field whose flag is clear MUST be zero and MUST NOT be read.

**`min`/`max` are `ordinalize()` ordinals**, the same dialect the catalog
manifest speaks, so a predicate literal's ordinal compares directly against them
at plan time. Two consequences a reader MUST respect:

- Ordinals are **monotonic but not injective** — string ordinals pack the first 8
  content bytes and collide on a shared prefix. Pruning is therefore
  **conservative**: a file may be read unnecessarily, never skipped wrongly. They
  MUST NOT be used as an equality proxy or a sort key.
- `ORDINAL_NULL` is `INT64_MIN`. Min/max are over **non-null** values only.

No `MIN`/`MAX` is written for `DRAKEN_DECIMAL128` (no ordinalize kernel exists —
returning a lossy `i64` proxy for a 128-bit type would be worse than absence),
nor for `VARIANT` / `ARRAY` / `VECTOR_FP16`, which have no order.

**`SUM` is a signed 128-bit accumulator**, for integer and DECIMAL columns only.
For DECIMAL it is the unscaled total; the reader applies the descriptor's scale.
It cannot overflow at any row count this format addresses
(`|2^63 × 2^32| = 2^95 ≪ 2^127`), so there is no overflow flag.

> **`SUM` MUST NOT be written for `FLOAT32`/`FLOAT64`.** Floating-point addition
> is not associative, so a stored sum and a recomputed one disagree in the low
> bits, and a query would return different answers depending on whether the
> optimizer used the footer.

---

## 9. Optional sections

### 9.1 `BLOOM` (256)

A Split-Block Bloom Filter, byte-compatible with the Parquet SBBF: a sequence of
32-byte blocks, each 8 little-endian `u32` words; XXH64 (seed 0) over the value's
plain bytes; block selection `((hash >> 32) * num_blocks) >> 32`; block count
always a power of two.

Built over the **`data` array** (`data_length` values), not the logical rows — on
a value-ordered column that is the deduplicated dictionary, so the filter costs
NDV insertions rather than row-count insertions and is exactly as accurate.

### 9.2 `PERMUTATION` (257)

A whole-file row order under a multi-column sort specification.

```
u16 key_count │ u16 reserved │ u32 length          (8 bytes)
key_count × { u32 column_ordinal │ u8 descending │ u8 nulls_first │ u16 reserved }
length   × u32 row ordinals
```

`length` MUST equal the file's `row_count`. `nulls_first` MUST follow draken's
single sort null-ordering rule — NULLS FIRST ascending, NULLS LAST descending — a
permutation written under a different rule is a different order, silently.

An identity permutation MUST NOT be stored; the correct encoding is
`ROW_SORTED` in `vector_flags`.

### 9.3 `ZONE_MAP` (258)

Per-row-chunk code bounds, for skipping byte ranges *within* a column.

```
u32 chunk_rows │ u32 chunk_count                    (8 bytes)
chunk_count × { u32 min_code │ u32 max_code }
```

`chunk_count` MUST be `ceil(length / chunk_rows)`, and every `max_code` MUST be
`< data_length` — a reader rejects a zone map that disagrees rather than pruning
on bounds that cannot address the dictionary.

Written ONLY for a value-ordered column with a stored selection. With
`value_order == ASCENDING` a predicate resolves to a code interval, so a chunk
whose `[min_code, max_code]` misses that interval provably contains no matching
row and its slice of the `SELECTION` section need not be read. On an unordered
column the codes carry no order and the bounds would be noise, so none is
written. None is written below one chunk either: the footer's own min/max
already covers the column.

A negative answer from a zone map is PROOF that a chunk holds no match; a
positive answer is only "cannot rule it out", so a reader must still evaluate the
rows it fetches.

---

## 10. Spill profile

The same format with everything optional switched off: `value_order == 0`,
`stats_bytes == 0` on every column, no optional sections. Spill data is written
once, read once, in-process, and wall-clock bound, so no read acceleration is
worth paying for. This is a **profile**, not a variant — a spill file is an
ordinary `.skene` file and any reader reads it.

---

## 11. Reader conformance

A conforming reader MUST validate in this order and MUST NOT interpret any
content before all of it passes:

1. Tail `magic`, then head `magic` if the head was read.
2. `version` within the supported window (§12) — otherwise fail naming **both**
   the file's version and the reader's.
3. `endianness` matches the host; `checksum_algorithm` is one this build implements.
4. `footer_bytes` is consistent with the object size; every section's
   `offset + stored_bytes` lies within the data region.
5. `footer_checksum`, then each section's `checksum` before that section is used.
6. Structural consistency: `selection_kind` against `data_length`/`length`
   (§7.2); every selection code `< data_length`; `data_length <=
   string_slot_count` for string columns; `string_payloads_elided` against the
   slots and arena (§7.4); `child_count == 1` iff the type is `DRAKEN_ARRAY`.
7. Unrecognised **required** section kind or encoding → reject. Unrecognised
   **optional** section kind → skip.
8. `reserved` in the head and tail is zero (§4.3) — the only bytes in the file
   no checksum covers.

**There is no partial or best-effort read.** The format copies buffers verbatim
and rebuilds absolute pointers from stored offsets; continuing past a detected
inconsistency is memory corruption, not a wrong answer. Every failure MUST name
what was inconsistent.

`keyhash_buf` (draken's carried key-hash) is deliberately **not** stored. It is a
derived cache whose absence is correct by construction: a consumer recomputes it.

---

## 12. Versions and migration

**A `skene` build reads at most two versions — the one it writes and its
immediate predecessor — and writes exactly one.** There is no mode that writes an
older version.

A file more than one version behind is **migrated, not read**: `binary vX`
migrates `(X-1) → X` and nothing else, so a file at version `F` reaches `N` by
running binaries `v(F+1), v(F+2), … v(N)` in order. Binaries are retained as
releases for this purpose.

```
 v1 file ──[binary v2]──> v2 ──[binary v3]──> v3 ──[binary v4]──> v4
```

Migration MUST be verified, not assumed: required-section content MUST round-trip
exactly, so a migrator re-reads its own output and compares decoded buffers
before anything replaces the original. Optional sections MAY be dropped or
rebuilt — they are reconstructible by definition (§7.1).

Because a build cannot read a file more than one version old, **any build MUST be
able to identify any file**: reading `magic` and `version` MUST succeed for every
version, including versions that build cannot read and versions that do not exist
yet. That is what freezes bytes 0–5 (§4.1).

### What does and does not bump the version

| change | bump? |
|---|---|
| New optional section kind (≥ 256) | **No** |
| New statistic appended to the stats blob | **No** |
| New encoding used only on optional sections | **No** |
| New required section kind, or a layout change to one | **Yes** |
| New encoding on a required section | **Yes** |
| Any change to the column or section directory layout | **Yes** |
| Any change to bytes 0–5 of the head | **Never permitted** |

---

## 13. Implementation status

Implemented and tested, writer **and** reader: §4, §5, §6, §7 in full (including
§7.6 value ordering and all three §7.7 encodings), §8, §9.1 bloom filters, §9.3
zone maps, §10, §11 in full, §12 version window and identification.

Not implemented: §9.2 permutations. Deliberately deferred rather than
outstanding — nothing in the engine produces a stored sort order yet, so the
section would have no writer and no consumer. It is an optional section and adds
with no version bump when something needs it.

Also not implemented: the migration entry point of §12, which cannot exist until
there is a v2 to migrate from.

Value ordering is not applied to ARRAY children. Ordering one is correct under
the uniform access contract, but it produces a dict-shaped array child, and
draken builds array children dense everywhere else — a storage layer is the wrong
place to hand the engine a shape it has never executed.

A writer MUST fail loud rather than emit a file whose `value_order` or statistics
claim more than it computed. The reference implementation rejects those options
rather than silently downgrading them.
