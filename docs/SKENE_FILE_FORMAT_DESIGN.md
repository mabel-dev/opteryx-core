# `.skene` — Draken-native columnar file format

**Status: DESIGN v2 — NOT APPROVED, NOT IMPLEMENTED.** Sections marked **[S-n]**
are decisions the architect owns; §14 collects the ones still open.

Replaces [`SORT_SPILL_DESIGN.md`](SORT_SPILL_DESIGN.md) §4 as the serialization
design. §4.1/§4.2 of that document (why Parquet and the rugo IPC format were both
rejected) still stand and are not re-argued.

---

## 1. Rulings carried into this version

| | Ruling |
|---|---|
| Home | Separate repo `../skene`. **C++, no Python.** Folds into opteryx-core later. |
| Dependencies | **skene imports draken.** skene and rugo are **parallel and disjoint** — neither imports the other. |
| Row groups | **SUPERSEDED 2026-08-08: one file holds MANY row groups** — 16 per file at 262144 rows each. The original ruling (one file = one row group) is kept below where it is argued, with the measurement that overturned it. See §11.5.4. |
| Versioning | Magic + mandatory version; reader handles **current and current-1**; anything else fails loud. |
| Statistics | **No KMV, no histograms, no char-class counts.** Those are the catalog's, and dataset-level. The file carries the stats the catalog does *not* have — MIN/MAX. |
| DECIMAL128 | **No min/max stats.** |
| ARRAY | Nested column entry — option (A). |
| Compression | Open to any algorithm, including ones not currently vendored. |
| Spill | Spill uses `.skene`. **No statistics computed for spill**, and no value ordering — spill needs no read acceleration. |
| B-tree | Dropped — value-ordered storage (§7) covers it. |
| Value ordering | **Optional and flagged**, per column-directory `value_order`. On for result files, off for spill. |
| Ordered flags | If the data is ordered, the flag is **not** dropped — `DRAKEN_DICT_KEYS_SORTED` is widened past dict shape (§7.3). |
| Constant columns | **No selection/permutation array.** `selection_kind` says so explicitly (§6.1). |
| Footer min/max | `ordinalize()` ordinals — one dialect with the catalog (§10.1). |

---

## 2. What already exists (survey)

| Capability | Where | Usable by skene? |
|---|---|---|
| Split-Block Bloom Filter writer + reader (Parquet-compatible SBBF, XXH64) | `rugo/src/parquet/_bloom_writer.hpp`, `bloom_filter.cpp` | **No** — rugo is disjoint. See §9.1. |
| Join-side bloom (different shape entirely) | `src/cpp/bloom_filter_ops.hpp` | No — opteryx, and a different structure |
| KMV sketch, HLL++, histograms, char-class counts | `opteryx/utils/kmv.py`, `vector_sketch_reduce.cpp`, `src/cpp/hllpp.h`, `_analyze.py` | **Not needed** — catalog's job, per ruling |
| Draken statistics design (min/max as *location*, all optional, absent = "don't know") | `draken/docs/design/05_statistics.md` | Yes as doctrine; the location-vs-value split is resolved in §10 |
| Sorted-dictionary property end to end (sort dict → `DRAKEN_DICT_KEYS_SORTED` → binary-search consumers) | `SORTED_DICTIONARY_DESIGN.md` | **Yes — this is the precedent §7 generalises** |
| `DRAKEN_ROW_SORTED` / `_DESC`, incl. NULLS FIRST-asc / LAST-desc | `draken/core/buffers.h` | Yes |
| Single-block string layout `[DrakenStringArena \| slots \| arena \| validity]` | `draken/ops/kernels/result_helpers.cpp` | Yes — the read-side reconstruction primitive |
| `xxhash.h`, vendored and header-inlined | via `string_slot.h` | Yes — checksums need no new dependency |
| Draken's total order for every sortable type, incl. float NaN-highest / −0.0 == 0.0 | `draken/morsels/sort.hpp` | Yes — and §7.4 is where it bites |

**Does not exist anywhere:** permutation/sort-order storage, any ordered index,
any format beyond Parquet/CSV/JSONL.

---

## 3. Goals / non-goals

**Goals**

1. Round-trip a `CxxMorsel` losslessly — including everything Parquet drops:
   `LogicalType` (so an IPv4 column comes back **typed IPV4**, not bare UINT32),
   `DrakenVector.flags`, and the dict `selection` **restored, never re-derived**.
2. Footer index: read one column with one range request after one footer read.
3. Per-column MIN/MAX sufficient to skip a file, and room for filters/indexes.
4. Explicit magic + version; an unsupported version fails loud **before anything
   is interpreted**.
5. Native end to end. No Python on the read path — the specific failure of the
   rugo IPC format, whose deserializer falls back to `column_deserializer.pyx`
   for every string/dict/array tag.

**Non-goals**

- Portability. Nothing outside this tree reads `.skene`, and no foreign reader is
  promised.
- ~~Multiple row groups. Scale is by file count — which is what the job-results
  manifest already does with `part_NNNN`.~~ **Reversed 2026-08-08.** Scaling by
  file count alone made a ClickBench mirror 396 objects against Parquet's 99 for
  the same data: ~0.1ms of fixed per-file cost locally is ~40ms of a full scan
  before a byte is read, and remotely each of those is a GET rather than a
  syscall. A file now holds 16 row groups. What made this safe rather than a
  parallelism regression is that the SCAN's unit of work became
  `(file, row group)` at the same time — see §11.5.4.
- Random *row* access. Column-granular, not row-granular.

---

## 4. File anatomy

```
 ┌──────────────────────────────────────────────┐
 │ HEAD   magic "SKEN" │ version u16 │ …         │  16 bytes
 ├══════════════════════════════════════════════┤
 │ ROW GROUP 0                                  │
 │   DATA region                                │
 │     column 0 : all its sections, contiguous  │  ← one range GET per column
 │     column 1 : all its sections, contiguous  │
 │   INDEX region (blooms, zone maps)           │  ← adjacent to the RG footer,
 │   ROW GROUP FOOTER                           │     so one GET takes both
 │     column directory + section directory     │
 ├──────────────────────────────────────────────┤
 │ ROW GROUP 1 …                                │  16 per file
 ├══════════════════════════════════════════════┤
 │ FILE FOOTER  magic "SKNI" │ schema           │  ← the whole pruning surface,
 │   row group directory                        │     and the ONLY thing a
 │   per-row-group statistics                   │     pruning reader fetches
 ├──────────────────────────────────────────────┤
 │ TAIL   footer_len u32 │ footer_xxh3 u64      │
 │        version u16 │ rsv u16 │ magic "SKEN"  │  24 bytes, fixed
 └──────────────────────────────────────────────┘
```

The FILE footer deliberately holds **no section directory**. That is what keeps
it small enough to always fetch: the expensive metadata — a column directory is
28KB on a 105-column schema — stays behind the row group footers and is paid for
only by the row groups that survive pruning.

Magic at both ends, as Parquet does: the head magic rejects an unrelated or
front-truncated object immediately; the tail magic plus `footer_len` finds the
footer in one range request with no linear parse.

Read protocol: GET the tail → validate → GET the FILE footer → prune ROW GROUPS
on its per-row-group statistics (no row group footer has been read yet, and the
ones ruled out never are) → per surviving row group, GET its footer at the offset
the row group directory gave, verifying it against the checksum recorded there
(extend that request backwards over its INDEX region when filters are wanted) →
prune columns → one range GET per surviving column.

**Validation order is fixed and total:** magic → version → declared lengths
against the real object size → checksum → *only then* interpret one byte of
content. Nothing is memcpy'd and no pointer is rebuilt before the checksum
passes. This is not fussiness: the format memcpys buffers and rebuilds absolute
pointers, so a wrong layout interpreted as a right one is memory corruption, not
a wrong answer.

---

## 5. The footer

Flat, offset-addressed binary. No thrift, no JSON, no new dependency, no
allocation to parse. Every variable-length field carries an explicit length and
is bounds-checked against the footer extent before it is read.

```
FILE HEADER
  u64  row_count
  u32  column_count            // top-level columns
  u32  section_count
  u64  created_at_unix_us      // provenance only, never load-bearing
  u32  writer_tag_len ; bytes  // provenance only
  u8   file_flags

COLUMN DIRECTORY ×column_count            (recursive for ARRAY — §11)
  u32  name_len ; bytes                   // column identity
  u32  type                               // DrakenType, verbatim
  u8   vector_flags                       // DrakenVector.flags, VERBATIM
  u8   lt_present
  u8   value_order                        // §7: 0 = as-written, 1 = ascending
  u8   child_count                        // 0 except ARRAY
  u32  length                             // logical rows
  u32  data_length                        // physical values
  if lt_present:
      u8 kind; u8 unit; i16 offset_minutes; u8 precision; u8 scale;
      u16 pad; u32 dimension              // LogicalType POD, re-interned on read
  u32  section_index ; u32 section_n      // slice of the section directory
  u32  stats_offset  ; u32 stats_len      // 0,0 == no statistics (§10)
  … child column directory entries ×child_count …

SECTION DIRECTORY ×section_count
  u16  kind                    // §8
  u16  encoding                // §9
  u64  offset                  // absolute from file start
  u64  stored_bytes            // on disk, post-encoding
  u64  plain_bytes             // after decoding; == stored_bytes when PLAIN
  u64  xxh3                    // of the stored bytes

STATISTICS BLOBS (§10)
```

`stats_len == 0` is first-class: **absent means "not tracked", never "zero"** —
§05's cardinal rule, and the same rule `record_count=None` already follows in
the manifest path. Spill files carry `stats_len == 0` everywhere by construction.

---

## 6. Column encoding — the §11 question, resolved without a shape branch

You refused [D-3] (approving an encoding-shape inspection). The format does not
need one.

`00_data_model.md` settles it in its own words:

> Dict is the general form; dense and constant are degenerate dictionaries
> (identity codes / all-zero codes). This is why one access pattern covers all
> three.

So `.skene` stores exactly that general form for every column:

```
DATA      section : data_length values
SELECTION section : length uint32 codes   — see selection_kind below
VALIDITY  section : (length+7)/8 bytes, or absent
```

`data_length` and `length` round-trip verbatim, so `draken_is_dense` /
`is_constant` / `is_dict` classify identically on read-back.

### 6.1 `selection_kind` — written, not derived

**Ruled: constant columns get no selection array.** The column directory carries
an explicit `selection_kind`:

| `selection_kind` | SELECTION section | reader builds |
|---|---|---|
| `CONSTANT` | **absent** | `draken_zero_sel(length)` |
| `IDENTITY` | **absent** | `draken_identity_sel(length)` |
| `STORED` | present, with its own encoding (§9) | owned codes, decoded from the section |

This is a **written fact, not a derived one**. The reader does not classify the
vector to decide what to build — it reads one byte. That is the difference that
matters against the refused [D-3]: there is no inspection whose two arms could
disagree, and a `selection_kind` that contradicts `data_length` (e.g. `CONSTANT`
with `data_length != 1`) is a **detectable corruption**, checked and rejected,
rather than a silent reshape.

This ruling also settles the old **[S-1]**: `CONSTANT` and `IDENTITY` store zero
bytes, so the reader has nothing to materialize *from* and attaching the shared
global is the only sensible construction. `codes_buf` is null for both — which
is exactly what `VectorOwner` documents for identity/zero selections.

Under value ordering (§7) most columns are `STORED` anyway; `IDENTITY` is the
spill/`value_order = 0` case, and `CONSTANT` is free everywhere.

---

## 7. Value-ordered storage — your "always sort the values" idea

> *"I do wonder if we always sort the values and provide the permutation to order
> by the dataset rows — this means we could binary search / range filter pretty
> quickly — may mean we don't need a b-tree."*

It works, it composes exactly with §6, and it does replace the b-tree. It is not
free, and the costs are not uniform across column shapes. Here is the analysis.

### 7.1 What it is

Store each column's `data` array **sorted ascending and deduplicated**, with
`selection` mapping row → value index. This is not a new mechanism — it is
`SORTED_DICTIONARY_DESIGN.md` generalised from "dictionary-encoded Parquet
columns" to "every column", and it lands in the same place: `data` ascending,
codes remapped, row order preserved by `selection`.

Consequences, all good:

- **Range and equality predicates collapse to a code interval.** Two binary
  searches over `data`, then an integer range compare on the code stream — no
  per-row value materialization. This is precisely the existing sorted-dict win.
- **MIN/MAX are `data[0]` and `data[data_length-1]`** — exact, free, no extra pass.
- **Dedup is a size win** wherever there are repeats: `data_length` shrinks, and
  the codes narrow to `ceil(log2(data_length))` bits under bitpacking (§9).
- **No b-tree.** Sorted values + codes give ordered lookup on an immutable file
  with less machinery and better locality. Agreed and dropped.

### 7.2 What it costs

- **Write:** a sort + dedup **per column**. This is the dominant new write cost
  and it is O(n log n) per column, not per file.
- **Size, all-distinct columns:** dedup saves nothing, and the selection can no
  longer be `AFFINE` — it becomes a real permutation. The information-theoretic
  floor for a permutation of n items is `log2(n!) ≈ n·(log2 n − 1.44)` bits, so
  ~18.6 bits/row at n = 1M versus 4 bytes plain. Against that, sorted data
  delta-encodes far better than unsorted data. **Which side wins depends on the
  data and must be measured, not assumed.**
- **Size, low-cardinality columns:** unambiguous win — dedup shrinks `data`, and
  codes bitpack to a few bits.

**Therefore: value ordering is a per-file write option, not a law.** The column
directory carries `value_order`, so the file is self-describing and the reader
never guesses. Recommended default: **on for result files, off for spill** —
spill is write-once-read-once and wall-clock-bound, where an extra per-column
sort is pure cost. This also matches your "no statistics for spill" ruling: spill
files become the minimal shape of the same format.

### 7.3 The flag gap — RULED: widen the flag (option ii)

> *"all columns are 'dictionaries', that's the unified format"* — and *"if we
> have ordered flags, we shouldn't drop them if the data is ordered."*

`DRAKEN_DICT_KEYS_SORTED` is what carries the win into execution, and today it is
gated on dict *shape*. `draken_vector_mark_dict_sorted` is a **silent no-op**
otherwise:

```c
// draken_native.cpp:2329
if (draken_is_dict(&v))          // data_length > 1 && data_length < length
    v.flags |= DRAKEN_DICT_KEYS_SORTED;
```

So an all-distinct value-ordered column is sorted on disk, comes back sorted in
memory, and the hint is dropped. Under the unified model that gate is simply
wrong: every vector *is* a dictionary, so "the dictionary's keys are ascending"
is meaningful for every one of them.

**This is an opteryx-core change, small and separable from skene.** The blast
radius is three call sites — verified, not assumed:

| site | change |
|---|---|
| `draken_native.cpp:2329` `draken_vector_mark_dict_sorted` | drop the `draken_is_dict` gate — set the flag whenever the data is ascending |
| `buffers.h` `draken_dict_is_sorted()` | drop the `draken_is_dict(v) &&` conjunct |
| `buffers.h` `draken_dict_sorted_dense()` | drop the `draken_is_compressed(v) &&` conjunct |
| `int64_reductions.h:100,153` | consumers of `draken_dict_sorted_dense` — no code change, they simply start firing on sorted dense columns |
| `draken_native.cpp:8757` | Python-visible property — starts reporting true for sorted dense vectors |

Soundness of dropping the gates: `KEYS_SORTED` means `data[0..data_length)` is
ascending and `CODES_DENSE` means every code in `[0, data_length)` is referenced
by a valid row. Together those imply `data[0]` is the column min and
`data[data_length-1]` the max — and that implication does not depend on
`data_length < length`. It holds identically for a dense sorted column. The
comment in `buffers.h` saying the flag is "scoped to dict shape / meaningless on
dense/constant" is stale under the unified model and gets fixed with the code.

The flag stays a **pure hint** in both directions: set only when certain, and a
consumer that ignores it must get the identical answer via the uniform path.

No new flag bit is needed, so bits 6–7 stay reserved.

### 7.4 Two correctness traps in "sort and dedup"

**Floats.** `SORTED_DICTIONARY_DESIGN.md` skips floats entirely for Parquet
because "NaN and −0.0 break monotonic code ranges". Draken *does* define a total
order (NaN highest, −0.0 == 0.0), so sorting is well-defined here — but
**dedup must be bitwise, not by engine equality.** Under engine order −0.0 and
0.0 compare equal, so an equality-based dedup would collapse them and a column
containing −0.0 would read back as 0.0. That is silent data corruption on a
round trip. Dedup keys on the bit pattern; the two values sort adjacently and
both survive. Same rule for any type where engine equality is coarser than byte
identity.

**Types with no order.** `VARIANT` has no defined collation (buffers.h says so
explicitly), `ARRAY` has no whole-array comparison, and `VECTOR_FP16` has no
ordering. These are stored `value_order = 0`, always. Not an error — just not
ordered.

**Nulls.** Nulls live in the validity bitmap, not in `data`. A null row's
selection entry must still be a **valid in-range index** (the reader bounds-checks
every code), so null rows point at index 0 and are masked by validity. Sort
order follows draken's one rule — NULLS FIRST ascending — but since nulls are not
values, that rule constrains the *permutation section* (§9.2), not `data`.

---

## 8. Sections, and the extensibility rule

A section is `(kind, encoding, offset, stored_bytes, plain_bytes, xxh3)`.

**Required** — the column cannot be reconstructed without them:

| kind | payload |
|---|---|
| `DATA` | fixed-width: `data_length × draken_type_itemsize(type, lt)`. BOOL: `(data_length+7)/8` bit-packed. ARRAY: `(length+1) × int32` offsets. NULL: empty. |
| `SELECTION` | `length` uint32 codes (§6) |
| `VALIDITY` | `(length+7)/8` bytes; absent ⟹ all valid |
| `STRING_SLOTS` | `slot_count × 16` bytes, verbatim |
| `STRING_ARENA` | `arena_used` bytes, verbatim |

**Optional** — accelerators only: `PERMUTATION` (§9.2), and future filters/indexes.

> **The extensibility rule.** A reader **skips** an optional section whose `kind`
> it does not recognise, and **fails loud** on an unrecognised required section.
> This is what lets us add a filter or an index with no version bump.
>
> What makes skipping safe: **an optional section must be provably
> reconstructible from the required sections, so ignoring it can only cost speed,
> never correctness.** Anything carrying information not otherwise present is a
> required section, and introducing one bumps the version. There is no third
> category, and "optional" is never a route for sneaking data past an old reader.

### 8.1 String family — the two traps, explicitly

`DrakenStringArena.slots` and `.arena` are **absolute pointers** and are never
written. The scalar fields (`length`, `arena_used`, `arena_cap`,
`payloads_elided`, `type`) go in the column directory's string sub-header; slots
and arena bytes go in their own sections; on read the block is allocated through
the existing `[DrakenStringArena | slots | arena | validity]` layout
(`result_helpers.cpp`) and the two pointers are rebuilt into it. `owns_buffers`
is **not** carried — it is 0 by construction, because `VectorOwner`'s
`unique_ptr` *is* the ownership record.

`payloads_elided` round-trips, and is **verified on read, not merely written**:

- `payloads_elided == 1` ⟹ `arena_used == 0`, `STRING_ARENA` absent, and **every**
  long slot's `arena_offset == STR_ELIDED_PAYLOAD_OFFSET`.
- `payloads_elided == 0` ⟹ every long slot satisfies
  `arena_offset + length <= arena_used`.

Either violation is a hard error. §4.3 is right that losing the flag "turns a
trap value into a 4 GB out-of-bounds read" — across a process boundary and a week
of object storage, writing it correctly is not enough. One linear pass over the
slots, cheap against the arena memcpy it guards.

Selection codes get the same treatment: every code `< data_length`, and
`data_length <= slot_count`. A corrupt `data_length` is then a contradiction the
reader catches, never a reshape it silently performs.

### 8.2 Explicitly dropped

`VectorOwner::keyhash_buf` is not carried. It is a derived E37 cache whose own
header states presence == validity, and that any op not explicitly propagating it
yields `nullptr` with the consumer recomputing. `nullptr` on read-back is correct
by construction.

---

## 9. Encodings, indexes, compression

### 9.1 Encoding pipeline

Per section, a **transform** then optionally a **compressor**. Both recorded in
the section directory, so every combination is self-describing:

| transform | applies to | why |
|---|---|---|
| `NONE` | anything | |
| `AFFINE(base, stride)` | selection | 8 bytes for identity/constant (§6) |
| `BITPACK(width)` | selection | `ceil(log2(data_length))` bits/row — the big win on dict columns |
| `DELTA` | ascending fixed-width `data` | value-ordered storage (§7) makes this apply to *every* ordered column, not just naturally-sorted ones |

You are open to compressors we do not vendor. My recommendation is to **build the
transform layer first and measure before adding any compressor at all**:
`DELTA` on sorted data and `BITPACK` on codes are dependency-free and target
exactly the two shapes this format produces. A general-purpose compressor on top
of well-transformed columnar data typically buys much less than it does on raw
bytes, and we have a standing rule to benchmark before optimizing. Once measured,
the candidates worth evaluating are zstd (already vendored and linked, no new
dependency), and LZ4 for decode-bound remote reads. **[S-3]** stays open pending
those numbers.

> **Encodings are not free to add later.** §8's rule lets us add an optional
> *section* with no version bump, because ignoring it only costs speed. A new
> **encoding** on a *required* section is the opposite: an old reader hitting an
> unknown `encoding` on `DATA` or `SELECTION` cannot decode the column and must
> fail loud. **Adding an encoding to a required section is a version bump.** N-1
> absorbs that fine, but it means the encoding set wants settling before v1
> freezes rather than accreting afterwards.

### 9.1.1 streamvbyte — assessed, recommend not yet

[fast-pack/streamvbyte](https://github.com/fast-pack/streamvbyte): Apache-2.0
(compatible), SSE4.1 + AArch64 NEON with a portable scalar fallback and runtime
dispatch (so the RISC-V aspiration degrades to scalar rather than failing),
~33–41 GB/s decode, `streamvbyte_delta_encode` for ascending sequences. It is a
credible vendor candidate. Vendoring still needs your agreement under §4.

Assessed against the four uint32-shaped arrays this format actually produces:

| array | shape | streamvbyte | better option |
|---|---|---|---|
| **ARRAY offsets** `int32[length+1]` | monotonic, small deltas | **strong fit** — delta + svb, deltas are sublist lengths, ~1.25 B/value vs 4 | — |
| **SELECTION codes** | row-order, bounded by `data_length` | 1.25 B/value at best | **`BITPACK`** — `ceil(log2(data_length))` bits: 1 B at ≤256 distinct, **0.5 B at ≤16** |
| **PERMUTATION** | a permutation, bounded by `length` | ~3.25 B/value at 1M rows | **`BITPACK`** — 20 bits = 2.5 B at 1M rows |
| **sorted `data`, 32-bit types** | ascending | good fit with delta | — |
| **sorted `data`, 64-bit types** (INT64, TIMESTAMP64, DECIMAL, UINT64) | ascending | **does not apply — streamvbyte is 32-bit only** | delta + bitpack |

The pattern: streamvbyte's per-value adaptive width is exactly what we *don't*
need on codes and permutations, because we already know the bound a priori
(`data_length`, `length`) and can bitpack to it — which beats streamvbyte across
the whole low-cardinality range where codes dominate file size. And it cannot
touch our most common wide types at all, since INT64/TIMESTAMP64/DECIMAL are
64-bit.

That leaves it genuinely good at one thing — the offsets array you named — plus
32-bit sorted data. **Recommend: build `BITPACK` + `DELTA` first (no dependency,
covers codes and permutations, which are the bulk of the bytes value ordering
adds), measure, and vendor streamvbyte only if ARRAY offsets or 32-bit sorted
columns turn out to be a material share of real result-file size.** Deferring
costs one version bump later, which N-1 is designed to absorb.

Every section carries an `xxh3` of its stored bytes; the footer carries its own.
A corrupt column is caught at that column, not at an arbitrary later pointer
rebuild.

### 9.2 Permutations (sort orders)

Value ordering (§7) sorts each column *independently*, which is not a row order.
A `PERMUTATION` section stores a whole-file row order under a multi-column sort
spec:

```
u16  key_count
  ×key_count:  u32 column_ordinal │ u8 descending │ u8 nulls_first │ u16 pad
u32  length                       // == file row_count
u32[length] row ordinals
```

Multiple permutations per file. An `ORDER BY` matching a stored spec reads the
permutation instead of sorting; a `LIMIT n` over it touches only the first `n`
ordinals' rows.

Two things to keep honest:

- The nulls-first/last fields must match draken's single sort null-ordering rule
  (NULLS FIRST ascending, LAST descending — `draken/morsels/sort.hpp`). A
  permutation written under a different rule is a different order, silently.
- When a permutation would be the identity, the correct encoding is **not** a
  permutation section — it is `DRAKEN_ROW_SORTED` (+ `_DESC`) in the column's
  `vector_flags`, which already exists and already carries into execution. Never
  store an identity permutation.

### 9.3 Bloom filters — in scope; put the SBBF in draken, not a third copy

In v1, per your ruling. Long-term storage strengthens the case: point lookups on
a stored dataset are exactly the gap value ordering does not close (it answers
"the value is in range", a bloom answers "the value is absent").

**Use the SBBF, not the join bloom.** They are not interchangeable:
`src/cpp/bloom_filter_ops.hpp` is a probe-side structure — a flat 64-bit-chunk
bit array over hashes the join has already computed, with a SIMD probe. It has no
serialized form, no block structure, and no false-positive-rate sizing contract.
The SBBF in `rugo/src/parquet/_bloom_writer.hpp` is built to be *written down*:
32-byte blocks for cache locality, `bloom_num_blocks(ndv, fpp)` sizing, and a
matching reader. For a file format we want the one designed to be a file.

**Where it goes.** skene cannot import rugo, so porting means a *third* bloom in
this tree. The clean answer is to move the SBBF pair down into **draken**, which
both rugo and skene import — one implementation, two consumers, no drift, and
rugo's Parquet path keeps working through the same code. That is a relocation of
live, tested rugo code, so it wants your nod rather than my assumption; the
fallback is a copy in skene, which I would rather not do.

Built over the **`data` array** (`data_length` values), not the logical rows: on
a value-ordered column that is the deduplicated dictionary, so the filter costs
NDV insertions instead of row-count insertions and is exactly as accurate.
`data_length` is the exact NDV (§10.2a), so `bloom_num_blocks` gets a true count
rather than an estimate — the sizing is right by construction.

### 9.4 B-tree — dropped

Agreed: value-ordered storage gives ordered lookup on an immutable single-row-group
file with less machinery and better locality. Nothing further proposed.

---

## 10. Statistics

Per your ruling the file carries only what the catalog does not. No KMV, no
histograms, no char-class counts. All optional; absent means "not tracked", never
"zero". **Spill files carry none at all.**

### 10.1 Scalar stats

| stat | representation |
|---|---|
| `min` / `max` | **`ordinalize()` ordinals, int64** — ruled [S-5b] |
| `null_count` | u64 |
| `is_sorted` / `descending` | u8, mirroring `DRAKEN_ROW_SORTED` |

Ordinals are the right call and the machinery is **already built**:
`ops/ordinalize.h` exists natively, and `DrakenType.ordinalize(value)` is
documented as being for exactly this — *"used at plan time to compare a predicate
literal's ordinal key against a file's precomputed min/max ordinal bounds"*. One
dialect with the catalog, 8 fixed bytes per bound, and no truncation flags to
design: for strings the ordinal is already a monotonic truncation (first 8
content bytes, big-endian, `>> 1`).

Two properties of ordinals that must be stated, because they are load-bearing:

- **Coarse, never wrong.** `ordinalize` is monotonic but not injective — string
  ordinals collide on a shared prefix, and its own header says it is fine for
  "coarse range pruning" but "NOT safe as a sort key or an equality proxy". So
  pruning is **conservative**: a file may be read unnecessarily, never skipped
  wrongly. That is the fail-safe direction.
- **`ORDINAL_NULL == INT64_MIN`** sorts nulls first. Min/max are over non-null
  values, so the writer must exclude nulls rather than let `INT64_MIN` become
  every nullable column's min.

**Excluded from min/max:** `DECIMAL128` (ruled; and `ordinalize.h` deliberately
has no DECIMAL128 entry — it throws rather than return a lossy int64 proxy),
plus `VARIANT` / `ARRAY` / `VECTOR_FP16` for the same no-order reason §7.4
excludes them from value ordering.

Under value ordering, min/max are `data[0]` and `data[data_length-1]` — exact and
free.

### 10.2 What else avoids reads

You asked. Three things, in descending order of value.

**(a) Exact NDV — already there, costs nothing.** With value ordering the data
array is sorted *and deduplicated*, so `data_length` **is the exact distinct
count**, not an estimate — and it is already a field in the column directory.
Combined with `null_count` and `row_count`, the footer answers `COUNT(*)`,
`COUNT(col)`, `COUNT(DISTINCT col)`, and `IS NULL` / `IS NOT NULL` selectivity
**with zero column reads**. And `data_length == 1 && null_count == 0` means the
column is constant, so *any* predicate on it is answerable from the footer alone.

The one invariant this depends on: null rows' codes point at a real value index
(§7.4), so they never inflate `data_length` with a phantom entry. Worth an
assertion, not just a comment.

**(b) Intra-column zone maps — the biggest remaining win.** Within one row group
a predicate either reads a whole column or none of it. Split the row space
into fixed chunks (8192 rows) and store, per chunk, the **min and max code**:

```
u32 chunk_rows
u32 chunk_count
×chunk_count: u32 min_code │ u32 max_code
```

Then a predicate resolves to a code interval by binary search on the sorted
`data` (§7.1), and every chunk whose `[min_code, max_code]` misses that interval
is skipped — so the reader fetches only the surviving byte ranges of the
SELECTION section. This is Parquet's page index without pages, and it composes
exactly with value ordering because a code interval *is* a value interval once
the data is sorted.

Cost is trivial: 8 bytes per 8192 rows — ~1 KB for a million rows. It is an
**optional section**, so it adds with no version bump, and it is the one addition
I would actually push for after v1.

**(c) Precomputed SUM — in, for exact types only.** `SUM(col)` and `AVG(col)`
answered from the footer with no read.

Stored as a **signed 128-bit** accumulator, not int64: a column of INT64 values
overflows int64 long before it overflows int128 (worst case
`2^63 × 2^32 = 2^95 ≪ 2^127`), so int128 cannot overflow at any row count this
format can address, and no overflow flag is needed. Covers INT8…INT64,
UINT8…UINT64, and DECIMAL (int64-backed unscaled — the scale is in the
`LogicalType`, so the stored sum is the unscaled total and the reader applies
scale, exactly as the column does).

**Not stored for FLOAT32/FLOAT64.** Floating-point addition is not associative,
so a write-time sum differs in the low bits from a read-time one, and the query
would return different answers depending on whether the optimizer used the
footer. That is answer-instability dressed as an optimization. Absent means "not
tracked" and the engine sums the column, which is the only stable answer.

**Not proposed:** anything cross-column (correlations, joint selectivity) — that
is the catalog's job and dataset-level, per your ruling on where stats live.

---

## 11. ARRAY — option (A), as ruled

The column directory entry carries `child_count` and its children's entries
recursively. Each child keeps its own sections, its own extent and its own stats,
so nesting the *directory* costs nothing in addressability. `ARRAY<ARRAY<T>>`
falls out of the same recursion, and `VectorOwner`'s destructor already frees the
whole subtree.

Parent `data` is `int32 offsets[length+1]`, sized by the **logical** row count
(arrays are stored dense — `draken_native.cpp` D.13). Round-trip invariants,
unchanged from the in-memory model: an empty sublist is
`offsets[i] == offsets[i+1]` with the validity bit **set**; a null row is the
same offsets with the bit **clear**.

Arrays are never value-ordered (§7.4).

---

## 11.5 Long-term storage — what it changes

> *"we may use this format for long term storage for some datasets — parquet
> still being the default, but some we may want an optimized format"*

That retires the premise the whole design started from ("ephemeral, read back
exactly once"). Four consequences, one of which contradicts an earlier ruling.

### 11.5.1 N-1 plus migration — RULED

**A `skene` binary reads at most two versions and writes exactly one.** There is
no mode that writes an older version — that would put two writers in one binary
and make it ambiguous what a file at a given version actually contains.

So `binary vX` migrates `(X-1) → X`, and nothing else. Moving a file forward
several versions is several runs of several binaries, one hop each:

| to move N-3 → N | binary | reads | writes |
|---|---|---|---|
| step 1: N-3 → N-2 | `skene` v(N-2) | N-3, N-2 | N-2 |
| step 2: N-2 → N-1 | `skene` v(N-1) | N-2, N-1 | N-1 |
| step 3: N-1 → N | `skene` v(N) | N-1, N | N |

Binaries are **retained as GitHub releases** and installed as needed. No single
binary can shortcut the chain, and none is expected to.

Three obligations follow, and they are not optional extras — the chain does not
work without them:

1. **The N-1 reader is retained in the SOURCE, not just in a released binary.**
   Migration needs to read its input. So reader code is versioned from v1 onward
   (`reader_v1.cpp`, `reader_v2.cpp`, …) and dispatched on the file's version.
   Deleting an old reader without first removing its version from the chain
   breaks migration silently — so retirement is a deliberate, ordered step.

2. **Any build must be able to identify any file.** An operator holding an N-3
   file has to know which retained binary to reach for. `probe_version()`
   therefore parses **magic and version only**, succeeds for versions this build
   cannot read — including versions that do not exist yet — and never touches a
   footer, follows an offset, or allocates. **This freezes the first 8 bytes of
   the head forever.** That is the price of a working migration chain and it is
   worth paying; everything past those 8 bytes stays free to change.

3. **Binaries must be retained and self-describing.** `supported_versions_string()`
   prints `writes vN, reads vN-1..vN`, so routing a file to a binary is reading
   two strings rather than guessing from a release date.

Migration must also be **verified, not assumed**: required-section content has to
round-trip exactly, so migrate re-reads its own output and compares decoded
buffers before replacing anything. Optional sections may legitimately be dropped
or rebuilt — they are reconstructible by definition (§8) — but a required byte
lost in migration is the data loss the chain exists to prevent.

### 11.5.2 Field IDs — cheap now, painful later

Long-term storage implies schema evolution: columns get added, dropped and
renamed over a dataset's life, and files written years apart must line up.
Matching on **name** breaks on rename; this is the lesson Parquet and Iceberg
both learned retrospectively.

So the column directory carries a **`u32 field_id`** alongside the name, for
top-level columns and ARRAY children alike. Assignment is the catalog's business,
not the format's — the format only guarantees the slot exists and round-trips.
Adding it now costs 4 bytes per column; adding it later costs a version bump and
a migration. **Recommend: in v1.**

### 11.5.3 Three more bytes of cheap insurance

All in the file header, all one byte, all "impossible to retrofit safely":

- **`endianness`** — the format memcpys native little-endian buffers. Both current
  targets are LE, but a stored file outliving the fleet should make a big-endian
  reader **fail loud**, not read garbage.
- **`checksum_algorithm`** — so xxh3 can be replaced without a required-section
  bump.
- **`file_uuid` (16 bytes)** — lineage and manifest dedup; free to write, awkward
  to add once files exist.

### 11.5.4 Row groups per file — REVERSED 2026-08-08

The original ruling was **one file = one row group**: scale by file count,
because the manifest is already the multi-file layer with per-file stats. That
held until the file count itself became the cost.

**What overturned it.** A ClickBench mirror was 396 objects against Parquet's 99
for the same data. Per-file fixed cost measures ~0.1ms locally (open+mmap and the
footer fetch both land there), so ~40ms of a full scan is spent before any data
is read, plus a 4x larger manifest (5.0ms against 2.3ms to plan a stats-only
query). On object storage the same count is 396 GETs at tens of milliseconds
each, not 396 syscalls.

**The ruling now: 16 row groups per file at 262144 rows per row group** (~4.2M
rows/file). 262144 is where Parquet landed too — the balance between amortising
remote IO and the cost of processing one unit.

**Three things had to be true for this to pay rather than cost**, and each is a
constraint on everything downstream:

1. **The scan's claim unit is `(file, row group)`, never `file`.** Measured on
   16M rows with the claim unit tied to row group size: flat from 64k to 256k
   rows per claim (340/311/326ms), then 750ms at 1M and 1809ms at 4M. Packing
   row groups without making them independently claimable reproduces the bottom
   of that table.
2. **Every row group is independently addressable** — its own footer at its own
   offset with its own checksum, so a reader fetches the small file-level index,
   prunes, and reads only the survivors' directories. Measured on the 105-column
   schema: 89.4KB of file index against 451.8KB for all 16 row group
   directories.
3. **Per-row-group statistics live in the FILE footer.** Manifest bounds
   necessarily coarsen to the union across a file's row groups — that is
   expected and correct, not a regression to fight. What recovers the lost
   pruning is the file footer's per-row-group per-column min/max. Measured
   across real ClickBench predicates, the number of row groups actually READ is
   identical at every packing level; only the fetch count changes, by 12-16x.

Delivered 2026-08-08. Bracketed against Parquet (the box drifts ~9% within a
bracket, so the ratio is the number): 0.781 before, 0.798 after — flat, with the
file count down 16.5x.

**What still does not change.** Row-level deletes, snapshots and schema history
remain table-format concerns that live in the manifest, not here. Nothing
proposed.

---

## 12. Versioning and compatibility

Reader handles **current and current-1**. Consequences, stated so they are not
discovered later:

- Two readers live at any time. Every layout change ships an N-1 reader **and**
  round-trip tests for both; the older one retires on the next bump. The pair
  count stays at two only if retirement actually happens.
- A version bump is a **required-section layout change only**. Adding an optional
  section kind — a filter, an index, a statistic — does **not** bump the version
  (§8's rule). That is what keeps bumps rare enough for N-1 to be a real window
  rather than a treadmill.
- A version outside `{current, current-1}` fails loud, naming the file's version
  and the reader's, and interprets nothing.
- N-1 covers one deploy cycle, not seven days. A file two bumps old inside its
  life is unreadable, so the jobs side needs a defined behaviour — a `.skene`
  result is derived data, so "unreadable version ⟹ re-execute" is the natural
  answer, but it is jobs-side and out of scope here.

---

## 13. Repo boundary, build, and sequencing

**skene imports draken; skene and rugo are disjoint.** Concretely:

- skene includes `core/buffers.h`, `core/string_slot.h`, `core/vector_owner.h`,
  `core/alloc.h`, `core/vector_alloc.h`, `logical_type.h`, `morsels/cxx_morsel.h`.
  It **copies none of them** — §14 of the contract is explicit that `DrakenType`
  and `LogicalType` are draken's, imported never copied, and a hand-maintained
  duplicate of a frozen 40-byte ABI pinned by static_asserts is exactly the drift
  the freeze exists to prevent.
- Mechanically: skene's CMake takes a `DRAKEN_ROOT` cache variable (default: a
  sibling `../opteryx-core` checkout) and adds `${DRAKEN_ROOT}/draken` as an
  include directory. **No copying, no submodule, no symlink.** A submodule of the
  whole engine repo to reach seven headers is a lot of machinery for something
  the merge deletes; an include path is one line and evaporates on merge, when
  the headers simply become in-tree. It fails loud at configure time if the
  headers are not found. **[S-10] settled.**
- skene must **re-assert draken's static_asserts** in its own translation units
  (`sizeof(DrakenVector) == 40` and the per-field offsets). They are in
  `buffers.h`, so this is automatic — but it should be a deliberate, tested
  property of skene's build, because it is the only thing standing between a
  draken ABI change and skene silently writing a different layout.

**Build and test.** C++, no Python, so skene needs its own build (CMake is the
obvious choice) and its own C++ test harness — the round-trip suite cannot be
pytest. One exception: the **IPv4-survives-the-round-trip** check is only
meaningful once opteryx reads a `.skene` file back into a typed column, so it is
an **opteryx-core integration test** written after the wiring, not a skene unit
test. **[S-10]**.

**Sequencing.**

1. skene: format, writer, reader, C++ round-trip tests (this is the bulk).
2. opteryx-core: the draken↔skene adapter and the integration tests, including
   IPv4 and the Parquet-writer benchmark.
3. Spill: `SortSink` emits `.skene` blocks with `value_order = 0` and no stats.
4. jobs.opteryx: `part_NNNN.parquet` → `part_NNNN.skene`, manifest gains
   `format` + `format_version`, the `columns` list carries real draken +
   logical types instead of stringified Arrow types — **so an IPv4 column stops
   being reported as `uint32`** — and the reader switches to tail → footer →
   per-column range GETs. Not touched in this task.

---

## 14. Still open

| # | Decision | Recommendation |
|---|---|---|
| # | Decision | Recommendation |
|---|---|---|
| **S-16** | §11.5.2/3 — `field_id` per column, plus `endianness` / `checksum_algorithm` / `file_uuid` in the header. | All in v1. Each is a handful of bytes now and a version bump plus a migration later. |

### Settled

**S-1** dissolved — `CONSTANT`/`IDENTITY` store zero bytes, so attaching the
shared global is the only construction (§6.1). **S-2** bloom **in scope** — port
the SBBF, but see §9.3 on where it lands. **S-3** — my direction: `BITPACK` +
`DELTA` first, no vendoring, measure. **S-5b** — `ordinalize()` ordinals; string
truncation accepted. **S-10** — CMake + `DRAKEN_ROOT` include path. **S-11** —
widen `DRAKEN_DICT_KEYS_SORTED`, three call sites (§7.3). **S-12** — value
ordering on for results, off for spill. **S-13** — precomputed SUM, **in**.
**S-14** — zone maps, **in v1**. **S-15** — N-1 window plus a `migrate` that
reads N-1 and writes N; multi-version jumps use retained binaries, one hop each
(§11.5.1).

### Flagged, not decisions

- `.claude/CLAUDE.md` §11's `DrakenVector` listing still omits the `uint8_t flags`
  field `buffers.h` defines at offset 36. The doc's own rule says buffers.h wins
  and the section is stale. Not fixed — I am not editing the contract unasked.
- The rugo IPC deserializer's Cython fallback (`kStatusNotHandled` →
  `column_deserializer.pyx` for every string/dict/array tag) remains pre-existing
  Python-on-the-execution-path debt. `.skene` routes around it; called out per §2,
  not fixed.
