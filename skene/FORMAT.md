# The `.skene` File Format

**Version 3 — implemented, not frozen.** The reference implementation writes
v3 and reads v2 and v3. `include/skene/format.h` describes v3, and where this
document and format.h disagree on a byte, format.h is right and this document
is stale. See §13.

v2 is **frozen** as of this bump. The v3 build reads it (§12), so its bytes are
fixed for all time: they are specified verbatim in [FORMAT_v2.md](FORMAT_v2.md)
and pinned by the golden fixtures in `tests/fixtures/v2/`.

v3 (ruled 2026-09-24) is one bump carrying these changes:

1. **Whole-file column-major layout.** Every column's chunks for every row group
   are byte-adjacent, so one projected column is one contiguous byte range for
   the whole file. A row group is no longer contiguous (§3).
2. **One footer.** Row group footers are gone. The file footer carries every
   pruning input, including per-(column, row group) statistics; each column's
   decode metadata sits in a **directory block** immediately before its data,
   checksummed from the file footer (§5).
3. **Block size `G`** — the number of row groups a reader fetches as one unit —
   is recorded in the footer (§3.2, §5.2).
4. **One KMV sketch per column per file**, in draken's `Vector.hash()` family
   and tagged with that family, so it unions with catalog and ANALYZE sketches.
   Per-row-group sketches are removed from the statistics blobs (§8).
5. **Accelerators in the file tail.** Blooms and zone maps live in one INDEX
   region before the footer (§3, §9).
6. **Reader window [2, 3].** v1 files are no longer readable (§12).

The rationale, the measurements behind it and the rulings live in
`opteryx-core/docs/SKENE_V3_FORMAT_DESIGN.md` and are not repeated here. The
pre-v3 rationale lives in `opteryx-core/docs/SKENE_FILE_FORMAT_DESIGN.md`.

This is the **normative specification**: it defines the bytes, and is complete
enough to write an independent reader from.

---

## 1. What it is

`.skene` stores one or more **row groups** of [draken](https://github.com/mabel-dev)
columnar vectors, losslessly. A row group is the unit of statistics, pruning
and decode; the file's bytes are laid out **by column**, not by row group.

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
 ├════════════════════════════════════════════┤
 │ DATA region — one run per column node,     │
 │ in depth-first schema order:               │
 │   column 0  DIRECTORY BLOCK                │  §5.9
 │   column 0  chunk rg0 │ chunk rg1 │ …      │  its required sections, all
 │   column 1  DIRECTORY BLOCK                │  row groups, row group order
 │   column 1  chunk rg0 │ chunk rg1 │ …      │
 │   …  (an ARRAY child's run follows its     │
 │       parent's)                            │
 ├────────────────────────────────────────────┤
 │ INDEX region                               │  optional sections (§9), same
 │   column 0 : rg0 │ rg1 │ …   column 1 : …  │  column-major order
 ├════════════════════════════════════════════┤
 │ FILE FOOTER                footer_bytes    │  §5 — the only footer
 ├────────────────────────────────────────────┤
 │ TAIL                             24 bytes  │  magic LAST
 └────────────────────────────────────────────┘
 EOF
```

Magic appears at **both** ends. The head magic rejects an unrelated or
front-truncated object on the first four bytes read; the tail magic plus
`footer_bytes` locates the FILE footer in one range request with no linear
parse.

A **column node** is one entry of the depth-first schema: every top-level column,
and the element child of each `DRAKEN_ARRAY` column. Every node has its own
directory block, its own run of chunks, its own statistics and its own summary
in the footer.

A **chunk** is one column node's **required** sections for one row group —
exactly the sections v2 wrote for that column inside a row group. Section
kinds, encodings, codecs, the four string lanes and per-section checksums are
unchanged (§5.11, §7).

Layout rules, every one of which a reader validates (§11):

1. Column node runs appear in depth-first schema order and do not overlap. An
   ARRAY column's run is immediately followed by its child's, so a column
   subtree is one contiguous range.
2. Within a run: the node's directory block, then its chunks in row group
   order. Every required section of chunk `i` lies at a lower offset than every
   required section of chunk `i + 1`.
3. The INDEX region follows the last run and holds only optional sections,
   grouped by column node in the same order, and within a node by row group.
4. **Every section body starts at a multiple of 64 bytes** (`kSectionAlign`)
   from byte 0 of the file. The writer pads with zeros; the padding is counted
   in **no** section's bytes. A writer obligation and a validation fact, never a
   quantity a reader computes with — offsets are absolute. Directory blocks carry
   no alignment obligation.

A chunk with no required sections (every chunk of a `DRAKEN_NULL` column)
occupies zero bytes. A node whose every chunk is empty has
`data_bytes == 0`.

### 3.1 Read procedure

1. Read the last 24 bytes. Validate the tail (§4.2).
2. Read `[filesize - 24 - footer_bytes, filesize - 24)`. Verify
   `footer_checksum`. This is the FILE footer — the only footer.
3. Parse it (§5). Validate (§11). Everything a pruning decision needs is now in
   hand: row counts, schema, cluster spec, every column node's extents, block
   extents and file-level sketch, and every (column, row group)'s statistics.
4. Prune row groups on the per-row-group statistics (§5.8). Choose the column
   nodes to read.
5. For each column node to be read, fetch its directory block
   `[directory_offset, directory_offset + directory_bytes)`, verify it against
   the `directory_checksum` recorded in its summary (§5.6), and parse it (§5.9).
   It is immediately followed by the node's chunks, so a reader fetching the
   node's first block of chunks SHOULD extend that request backwards over the
   directory.
6. For each surviving row group and each column node read, fetch that chunk's
   sections, verify each section's checksum, and reconstruct (§7).
7. A reader wanting a column's accelerators fetches its INDEX extent
   `[index_offset, index_offset + index_bytes)` and resolves the optional
   sections through the directory block.

The head MAY be validated at any point; a reader that never reads byte 0 (a
range-GET reader) MUST still validate the tail's `version`, `endianness` and
`checksum_algorithm`, which duplicate the head's for exactly this reason.

### 3.2 Blocks

A **block** is `G` consecutive row groups `[k·G, min((k+1)·G, row_group_count))`,
`G = block_row_groups` from the footer (§5.2). The last block MAY be partial.

Blocks are the fetch unit a reader is expected to use: for block `k` and a column
node, the chunks of every row group in the block are one contiguous range, and
the footer records that range (`block extents`, §5.6) so a fetch can be planned
from the footer alone, before any directory block is read. The layout itself
does not depend on `G` — a node's chunks are contiguous across the whole file —
so a reader MAY fetch several adjacent blocks of a column as one range.

`G` is a written fact about the file and immutable. A reader MUST NOT infer it
from offsets.

---

## 4. Head and tail

### 4.1 Head — 16 bytes at offset 0

| offset | size | field | value |
|---|---|---|---|
| 0 | 4 | `magic` | `0x4E454B53` — ASCII `SKEN` |
| 4 | 2 | `version` | format version, `3` |
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

The footer has its own checksum, each directory block has one recorded in the
footer, and each section has its own. **Nothing checksums the head or the
tail** — they are where a checksum would have to be recorded, so they cannot
cover themselves.

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
> one is rejected or provably unread.

Zero padding — before a section body to reach 64-byte alignment, and between a
directory block and the node's first section — is counted in no section and no
directory block, so no checksum covers it. A flipped bit there is **inert**:
read by nothing and computed with by nothing. Content bytes remain fully
covered.

---

## 5. The footer

There is one footer. It is fetched always, and it is sized to be enough to
decide everything about the file — which row groups survive, which column nodes
to read, and where every block of every node lies — with no second metadata
fetch before data can be requested. What decode alone needs lives in each
column node's directory block (§5.9), fetched with that node's data.

The footer is a flat byte stream, parsed sequentially, containing no internal
offsets and requiring no allocation to walk. It MUST end exactly at the start of
the tail; a reader MUST treat a trailing or short remainder as `kMalformed`.

In order:

1. **File footer header** (§5.2) — 64 bytes
2. **Writer tag** — `writer_tag_bytes` bytes, provenance only
3. **Row group table** (§5.3) — `row_group_count` entries, 16 bytes each
4. **Schema directory** (§5.4) — `column_count` entries, each nesting its child
5. **Cluster spec** (§5.5)
6. **Column summaries** (§5.6) — one per column node, depth-first, each
   carrying its block extents and its sketch (§5.7)
7. **Per-row-group statistics** (§5.8)

### 5.1 Size

The footer grows with (column nodes × row groups): each pair costs a 4-byte
length plus a 56-byte statistics blob, and each node adds a 64-byte summary,
16 bytes per block and at most 264 bytes of sketch. On a 105-column schema at
64k-row row groups in a 4 GiB file (~555 row groups) that is ~3.6 MB. Decode
metadata — chunk records and section entries — is not in the footer; it is in
the directory blocks.

### 5.2 File footer header — 64 bytes

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `footer_magic` | `0x494E4B53` — ASCII `SKNI` |
| 4 | 2 | `footer_version` | file-footer layout version, `3` |
| 6 | 2 | `reserved` | `0`, checked |
| 8 | 8 | `row_count` | **total** logical rows, summed over row groups |
| 16 | 4 | `row_group_count` | at least 1 |
| 20 | 4 | `column_count` | **top-level** columns; ARRAY children are nested, not counted here |
| 24 | 16 | `file_uuid` | all-zero means unset |
| 40 | 8 | `created_at_unix_us` | provenance only, **never load-bearing** |
| 48 | 4 | `writer_tag_bytes` | length of the tag that follows |
| 52 | 4 | `block_row_groups` | `G` (§3.2); `G ≥ 1`. A file with fewer than `G` row groups has one partial block |
| 56 | 8 | `data_region_bytes` | bytes from offset 16 to the start of the footer; MUST equal `filesize − 24 − footer_bytes − 16` |

`footer_magic` has guarded the file footer since row groups were packed into
files (2026-08-08), and a reader MUST still reject a mismatch.

`footer_version` versions this layout independently of the file `version`; the
two move together in practice, but each reader states its own requirement.

### 5.3 Row group table entry — 16 bytes

| offset | size | field |
|---|---|---|
| 0 | 8 | `row_count` — logical rows in this row group |
| 8 | 8 | `first_row` — its first row, in file row order |

A row group is not contiguous in v3, so it has no extent of its own; its bytes
are located per column node (§5.6, §5.10).

A reader MUST check: `first_row` equals the sum of the preceding entries'
`row_count`; the sum of every `row_count` equals the header's `row_count`.

### 5.4 Schema directory entry — 20-byte head, then variable parts

The part of a column that CANNOT vary between row groups. Everything else about
a column — length, `data_length`, selection kind, value order, section extents,
string arena counts — is a property of one row group and lives in that row
group's chunk record (§5.10). Unchanged from v2.

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `field_id` | stable identity across schema evolution; `0` means unassigned |
| 4 | 4 | `name_bytes` | length of the name that follows the head |
| 8 | 4 | `type` | `DrakenType`, verbatim |
| 12 | 1 | `logical_present` | `0`/`1`; a `LogicalTypeDescriptor` follows the name |
| 13 | 1 | `reserved0` | `0` |
| 14 | 2 | `reserved1` | `0` |
| 16 | 4 | `child_count` | `0` except `DRAKEN_ARRAY`, which has `1` |

Then, in order: `name_bytes` of identity, a `LogicalTypeDescriptor` if
`logical_present == 1`, then `child_count` complete child entries — the same
shape and the same order as the column summaries (§5.6).

A writer MUST reject a row group whose columns differ from the first's in name,
type, logical descriptor, `field_id` or nesting. A schema directory that does
not describe every row group is a lie a reader has no way to detect.

### 5.5 Cluster spec

Declares which sort keys, if any, the file's rows are **globally** ordered by —
in file row order, across every row group, seams included. It sits between the
schema directory and the column summaries so that a pruning reader has
it from the footer alone. Unchanged from v2.

```
u16 key_count │ u16 reserved                        (4 bytes; reserved 0, checked)
key_count × { u32 column_ordinal │ u8 descending │ u8 nulls_first │ u16 reserved }
```

The 8-byte key entry is the same `SortKey` §9.2's permutations use.
`column_ordinal` indexes the **top-level** schema order. `key_count == 0` means
**unclustered**, which is what every writer that does not know better MUST
write.

This record is a **verified promise**, never a trusted claim: consumers may act
on it — zone maps become tight, merge readers may skip sorting — so the writer
proves the declared order over the actual rows, every adjacent pair including
the seams between row groups, and fails the write on the first out-of-order
pair. A declared-but-false spec is silent wrong answers in every future
consumer; there is no "probably sorted". `nulls_first` MUST follow draken's
single sort null-ordering rule (NULLS FIRST ascending, NULLS LAST descending) —
any other combination is rejected.


### 5.6 Column summary — 64-byte head, then variable parts

One per column node, in the depth-first order of the schema directory (an ARRAY
column's summary is followed by its child's). Everything a reader needs to
**address** a node without having read its directory block.

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `directory_offset` | absolute; start of this node's directory block (§5.9) |
| 8 | 4 | `directory_bytes` | its length; `> 0` |
| 12 | 4 | `reserved0` | `0`, checked |
| 16 | 8 | `directory_checksum` | XXH3-64 over exactly `directory_bytes` at `directory_offset` |
| 24 | 8 | `data_offset` | absolute; start of the node's first required section, or `directory_offset + directory_bytes` when it has none |
| 32 | 8 | `data_bytes` | through the end of its last required section; `0` when it has none |
| 40 | 8 | `index_offset` | absolute; start of its first optional section, `0` when it has none |
| 48 | 8 | `index_bytes` | through the end of its last optional section; `0` when it has none |
| 56 | 4 | `block_count` | MUST equal `ceil(row_group_count / G)` |
| 60 | 4 | `child_count` | MUST equal the schema directory entry's `child_count` |

Then, in order:

1. `block_count` **block extents**, 16 bytes each:

   | offset | size | field |
   |---|---|---|
   | 0 | 8 | `offset` — absolute start of the first required section holding bytes, of any chunk in block `k` |
   | 8 | 8 | `bytes` — through the end of the last such section |

   Sections of zero stored bytes (a zero-row chunk still has them) contribute
   nothing to a block extent. A block whose sections hold no bytes is
   `offset == 0, bytes == 0`, and a reader fetches nothing for it.
2. The node's **sketch record** (§5.7).
3. `child_count` complete child summaries, recursively.

A node's directory checksum is recorded HERE rather than beside the bytes it
covers, because the footer is the only thing a ranged reader has fetched when
it decides which directory blocks to request — a checksum stored next to its
own bytes could not be validated against anything already trusted.

Block extents are redundant with the directory block, and exist so that a fetch
can be planned from the footer alone. A reader MUST check them against the
directory block when it parses one (§11).

### 5.7 Sketch record

A K-minimum-values sketch of the node's **whole-file** distinct values (§8.1).

```
u8 hash_family │ u8 reserved │ u16 k │ u32 count      (8 bytes; reserved 0, checked)
count × u64 hashes                                    ASCENDING, distinct
```

| field | rule |
|---|---|
| `hash_family` | `2` (draken `Vector.hash()`, §8.1) when `count > 0`; `0` when absent |
| `k` | the K the sketch was built at; `0` when absent |
| `count` | `0..k` |

`count == 0` means NOT TRACKED, never "no distinct values": an all-null or
zero-row column is described by its statistics, not by an empty sketch. A reader
MUST reject any other `hash_family` in a v3 file — one fact, one spelling; a
v2 file's sketches are family `1` and are reported as such by the v2 reader.

### 5.8 Per-row-group statistics

Column-node major, then row group: for each node in depth-first order, for each
row group in order, a `u32` byte length followed by that many bytes of
`ColumnStatistics` (§8). A length of `0` means NOT TRACKED, which is never the
same as zero. A blob longer than a reader understands is read prefix-first and
the remainder skipped.

This is what keeps row group pruning alive once catalog/manifest bounds coarsen.
A file-level bound is necessarily the union over the file's row groups and so
is wider than any one of them; that coarsening is expected and correct. What
recovers it is here, reachable from the footer alone.

### 5.9 Column directory block

One per column node, at the address its summary gives, immediately before the
node's chunks. Holds what decode needs and nothing pruning needs.

**Header — 16 bytes**

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `directory_magic` | `0x434E4B53` — ASCII `SKNC` |
| 4 | 4 | `node_ordinal` | this node's depth-first index; MUST equal the position of the summary that named it |
| 8 | 4 | `chunk_count` | MUST equal `row_group_count` |
| 12 | 4 | `section_count` | section entries that follow the chunk records |

Then `chunk_count` **chunk records** (§5.10), one per row group in order, then
`section_count` **section entries** (§5.11). The block MUST end exactly after
the last section entry.

The section entries are one list for the whole node: every chunk's required
sections and every chunk's optional sections. Chunk records address it by
index.

### 5.10 Chunk record — 64 bytes

The part of a column that varies between row groups — v2's per-row-group column
directory entry, less the identity fields the schema directory already carries
once.

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `length` | logical row count |
| 4 | 4 | `data_length` | physical value count |
| 8 | 1 | `vector_flags` | `DrakenVector.flags`, **verbatim** (§7.5) |
| 9 | 1 | `selection_kind` | §7.2 |
| 10 | 1 | `value_order` | §7.6 |
| 11 | 1 | `string_payloads_elided` | string family only (§7.4); `0` otherwise |
| 12 | 4 | `section_index` | first **required** section entry of this chunk |
| 16 | 4 | `section_count` | required entries belonging to this chunk |
| 20 | 4 | `index_section_index` | first **optional** section entry of this chunk |
| 24 | 4 | `index_section_count` | optional entries belonging to this chunk |
| 28 | 4 | `reserved0` | `0`, checked |
| 32 | 8 | `string_slot_count` | string family only; `0` otherwise |
| 40 | 8 | `string_arena_used` | string family only |
| 48 | 8 | `string_arena_cap` | string family only |
| 56 | 8 | `reserved1` | `0`, checked |

For a **top-level** node, `length` MUST equal row group `i`'s `row_count`. For an
ARRAY child, `length` is its element count, and no offset of its parent's chunk
`i` may address past it (§7.3) — a child MAY hold more elements than its
parent's offsets reach, because a sliced array keeps its whole element vector.

### 5.11 Section entry — 48 bytes

Unchanged from v2.

| offset | size | field |
|---|---|---|
| 0 | 2 | `kind` (§7.1) |
| 2 | 1 | `encoding` (§7.7) — `PLAIN`/`BITPACK`/`DELTA_BITPACK` only; the v1 codec spellings (`3`, `4`) MUST be rejected here |
| 3 | 1 | `codec` (§7.7) — `0` none, `1` zstd, `2` lz4 |
| 4 | 4 | `reserved` — `0`, checked |
| 8 | 8 | `offset` — absolute from file start; MUST be a multiple of 64 (§3) |
| 16 | 8 | `stored_bytes` — length on disk, post-codec |
| 24 | 8 | `encoded_bytes` — length after codec decode, before encoding decode |
| 32 | 8 | `plain_bytes` — length after both stages |
| 40 | 8 | `checksum` — over the **stored** bytes, not the decoded ones |

The three sizes name a **two-stage pipeline**. A body is produced encoding
first (bitpack/delta/plain), then codec (zstd/lz4/none), and decoded in
reverse; `stored_bytes` is the on-disk state, `encoded_bytes` the state between
the stages, `plain_bytes` the fully decoded one. The invariants a reader MUST
enforce: `codec == NONE` ⟹ `stored_bytes == encoded_bytes`;
`encoding == PLAIN` ⟹ `encoded_bytes == plain_bytes`; and for a real encoding,
`encoded_bytes <= plain_bytes`.

`encoded_bytes` is REQUIRED, not derivable: the codec decode needs its exact
destination capacity before any body header can be parsed — the role
`plain_bytes` played in v1 for the LZ4 block, whose block format carries no
length of its own (§7.7).

---


### 5.12 Two slices, because there are two regions

A chunk's required sections live in the DATA region and its optional ones in the
INDEX region, so each chunk carries **two** slices of its node's section list.

A reader MUST reject a **required** section kind appearing in an index slice.
Unknown kinds are skipped there, so a required section in that slice would be
silently ignored — the exact failure the required/optional split exists to
prevent.

Every required entry MUST lie within its node's `[data_offset, data_offset +
data_bytes)`; every optional entry within `[index_offset, index_offset +
index_bytes)`. Bounding a section against its own **column node** rather than
the file is not belt and braces: an entry in column 3 that addresses column 0's
bytes would otherwise pass, and its checksum would pass too, because the
checksum is computed over whatever bytes the offset names. (In v2 the same rule
was per row group; the bound follows the layout's unit of contiguity.)

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
| 4 | `STRING_SLOTS` | **v1 only** — `string_slot_count × 16` bytes of `DrakenStringSlot`, verbatim. A v2 or v3 file MUST NOT carry it, and a reader MUST reject it as malformed. |
| 5 | `STRING_ARENA` | `string_arena_used` bytes, verbatim. Absent when `string_arena_used == 0`. |
| 6 | `SLOT_LANE0` | `string_slot_count × u32`: word 0 of every slot — the length, in both slot forms |
| 7 | `SLOT_LANE1` | word 1: bytes 4–7 — big-endian prefix (long slots) or inline data (short slots) |
| 8 | `SLOT_LANE2` | word 2: bytes 8–11 — the dead `hash32` (long slots, always `0`) or inline data |
| 9 | `SLOT_LANE3` | word 3: bytes 12–15 — `arena_offset` (long slots) or inline data |

**All four lanes are REQUIRED for a string column** (§7.4). Lane *k* holds
`u32` word *k* of every 16-byte `DrakenStringSlot`, `string_slot_count` values
each; the reader reconstructs the slot array by a 4-way interleave. The split
loses nothing and invents nothing — it exists because each lane gets the
encoding that fits its own distribution (lane 2 of an all-long column is all
zeros and collapses to a width-0 bitpack: 8 bytes for the whole lane).

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

- the scalar fields live in the chunk record (§5.10) (`string_slot_count`,
  `string_arena_used`, `string_arena_cap`, `string_payloads_elided`),
- the slot array is the four `SLOT_LANE0..3` sections (§7.1), one `u32` lane
  per slot word,
- the payload bytes are the `STRING_ARENA` section.

A reader decodes the four lanes, interleaves them back into `string_slot_count`
16-byte slots in a fresh block alongside the arena bytes, and rebuilds the two
pointers. `owns_buffers` is **not** carried: it is `0` by construction, because
the reader's own ownership record governs the block.

The lane split is measured, not aesthetic. Interleaved as one verbatim slot
section (v1's `STRING_SLOTS`), the byte distribution changes every 4 bytes —
near-worst-case input for a general compressor: slots reached only 0.43x
against the arena's 0.25x. Planed into lanes, each gets the encoding that fits
it — lengths bit-pack, arena offsets delta-bit-pack, the dead `hash32` lane of
an all-long column collapses to a width-0 bitpack — for **−41%** of compressed
slot bytes on TPC-H lineitem and **−67%** on ClickBench
(`skene/bench/slot_layout.cpp`).

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
| 0 | `PLAIN` | anything; `encoded_bytes == plain_bytes` |
| 1 | `BITPACK` | `u32` arrays (selection codes, slot lanes) at a fixed bit width |
| 2 | `DELTA_BITPACK` | 4- or 8-byte integer arrays |
| 3 | `ZSTD` | **v1-only spelling** — MUST be rejected in a v2 or v3 `encoding` field |
| 4 | `LZ4` | **v1-only spelling** — MUST be rejected in a v2 or v3 `encoding` field |

In v1 the codec was crammed into this enum because the section entry had no
codec field, so "zstd" and "lz4" were spelled as encodings. v2 stores the codec
in its own `SectionEntry.codec` field — `0` NONE, `1` ZSTD, `2` LZ4 — applied
**after** the encoding on write and undone **before** it on read (§5.11), and
REJECTS values `3` and `4` in `encoding`: one fact, one spelling. The values
stay reserved and are never reused; v1 files, the only ones that carry them,
are outside the v3 read window (§12).

There is deliberately no bare `DELTA`: differences stored at the source width are
never smaller than the values, so nothing would produce one. Delta only pays
combined with bit packing.

`BITPACK` width on a `SELECTION` body comes from `data_length`, not from
scanning for a maximum: every code is already `< data_length`, so the bound is
known before the array is read. On a slot lane the maximum is scanned — no
prior bound exists — and a width of `0` (an all-zero lane) is the 8-byte
degenerate case §7.1 describes.

`DELTA_BITPACK` computes differences in **unsigned** arithmetic and wraps
deliberately. For an ascending signed array the wrapping unsigned difference is
the true step magnitude regardless of sign (`-5` → `3` gives `8`), and it cannot
overflow the way signed subtraction does when the array spans more than half the
type's range. The wrapping construction in fact reconstructs **any** integer
sequence exactly, not only ascending ones — a non-monotonic input simply
produces wide deltas and declines on the size test. On `DATA` bodies the writer
applies it ONLY where ascending order is established by construction — a
value-ordered column — never inferred from data that happens to look sorted; on
the slot lanes (§7.1) it is tried on every lane and the size test decides,
which is the natural fit for lane 3's near-sequential arena offsets.

The LZ4 codec is the LZ4 **block** format, not the frame format. A block
carries no header and cannot state its own decoded size, so `encoded_bytes`
supplies it and is load-bearing: a decoder is given that value as its
destination capacity and MUST produce exactly it — the role `plain_bytes`
played in v1, moved one stage earlier now that a codec can sit over a real
encoding. A body that decodes short is as malformed as one that overruns — the
directory decides the section's shape, and a short decode would leave the tail
of the destination holding whatever was there before. Readers MUST NOT narrow
`encoded_bytes`, `plain_bytes` or `stored_bytes` to fit the codec's `int`-sized
API; a value past that ceiling is rejected, never truncated into a plausible
one.

**Which codec is a writer POSTURE, not a per-section choice.** A file uses at
most one of `ZSTD` and `LZ4`. Both are decoded per section independently, so
mixing them within a file would buy a reader nothing while making the file's cost
model unstateable. Readers MUST decode either.

The two answer different questions. Measured on a ClickBench row group, 154.7 MB
of section bytes in 256 KB blocks, Apple Silicon:

| codec | ratio | compress MB/s | decompress MB/s |
|---|---|---|---|
| `LZ4` | 4.49x | 1743 | 8414 |
| `ZSTD` level 1 | 6.47x | 1081 | 2882 |
| `ZSTD` level 9 | 7.34x | 188 | 3078 |
| `ZSTD` level 19 | 7.71x | 9 | 3173 |

zstd's decompression rate does **not** vary with the level that produced the
bytes. A low zstd level therefore gives up ratio and buys nothing back on read,
so writers SHOULD use a high one; level 9 is the knee (9 → 12 costs 7x the
compression time for 1.6% more ratio). LZ4 decodes at roughly the rate the
reader's own uncompressed path runs at on the same file (~8840 MB/s measured),
which makes its decompression close to free relative to work already being done.

`ZSTD` and `LZ4` are applied **per section**, never to the whole file. Whole-file
compression is 0.7–5.7% smaller (measured on TPC-H) but a reader cannot
decompress a slice, so reading one column would mean fetching and decompressing
every column — destroying the property §3 exists for. Per-section keeps each
extent independently fetchable and independently decodable, and that is worth
the few percent.

A writer offers a section to the codec only where it measurably pays. Three
gates, each set from measurement (BENCHMARKS.md; a per-section census of the
v1 ClickBench and TPC-H mirrors, 2026-08-20) rather than intuition:

**Section kind.** Every compressible kind is offered — the exclusions are
`BLOOM` (hash bits; a correctly-sized filter measures 1.27x, incompressible by
construction) and `PERMUTATION` (row ordinals, near-random by nature). **v1's
encoding gate is gone.** v1 offered only `PLAIN` bodies, on the premise that a
bit-packed or delta body had already had its redundancy removed. That premise
was wrong: bit packing removes per-value *width* redundancy, not inter-value
*sequence* redundancy, which is what LZ77 matchers eat. The census measured the
cost — 137.3 MB of a 572.7 MB ClickBench file (24%), recoverable at 3.48x, all
bit-packed selections on high-NDV string columns.

**Size.** Only encoded bodies of at least **10240 bytes**. Sections below that
are 87% of all sections but hold ~1.2% of the recoverable bytes.

**Result.** A `PLAIN` body keeps v1's rule — the compressed form is stored only
when it is smaller at all. A **stacked** body (a codec over `BITPACK` or
`DELTA_BITPACK`) pays a second decode stage on every read, so it MUST be at
most **85%** of the encoded body to be kept; the recovered census sections
clear that floor by miles (3.48x average).

These are writer-side policy, not reader obligations: a reader MUST decode any
codec'd section it is given, on any kind, at any size, stacked or not.

Every encoder DECLINES when the result would not be smaller than plain, and the
writer then emits `PLAIN`. "Not worth it" is a normal outcome measured on actual
size, never a guess — so a compressed file is never larger than an uncompressed
one, section by section.

> Compression is **not** cosmetic for stored data. Measured on TPC-H under the
> v1 layout, skene without it was 1.9–3.8x larger than the equivalent ZSTD
> Parquet; with it, 0.92–1.09x. The cause is the string family: the arena (and,
> in v1, the interleaved 16-byte slots) keeps almost all its redundancy after
> the other encodings have run, and text columns dominate real tables. v2's
> slot lanes shrink the slot half of that further — the arena remains the case
> that makes the codec mandatory. Spill is the exception and stays
> uncompressed — written once, read once, wall-clock bound.
>
> One writer-policy consequence of the codec axis, measured the day it was
> built: a lane (or any body) is stored in whichever **(encoding, codec)** form
> ends smallest, costed to the FINAL stored size — never first-encoding-wins.
> On TPC-H `l_comment`, bit-packing the prefix lane 32 → 31 bits "won" 3% and
> then denied zstd its 57%: packing at a non-byte width misaligns text-like
> bytes so the codec's matcher finds nothing. A smaller intermediate is not a
> smaller file.

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

Per (column node, row group), all optional. **Absent means "not tracked", never
"zero".**

A blob is the length its `u32` prefix states (§5.8). A reader encountering a
blob **longer** than it understands MUST read the prefix it knows and skip the
remainder — this is what lets statistics be added without a version bump.

Current blob — 56 bytes:

| offset | size | field |
|---|---|---|
| 0 | 4 | `flags` — bitmask below |
| 4 | 4 | `reserved` |
| 8 | 8 | `min_ordinal` (`i64`) |
| 16 | 8 | `max_ordinal` (`i64`) |
| 24 | 8 | `null_count` |
| 32 | 8 | `sum_low` — low half of an `i128` |
| 40 | 8 | `sum_high` — high half of an `i128` |
| 48 | 8 | `ndv` — distinct count of the non-null values |

| bit | flag | present when |
|---|---|---|
| 0 | `MIN` | `min_ordinal` is meaningful |
| 1 | `MAX` | `max_ordinal` is meaningful |
| 2 | `NULL_COUNT` | |
| 3 | `SUM` | |
| 4 | `ROW_SORTED` | mirrors `DRAKEN_ROW_SORTED` |
| 5 | `ROW_SORTED_DESC` | direction; meaningful only with bit 4 |
| 6 | `NDV` | `ndv` holds a distinct count |
| 7 | `NDV_EXACT` | …and it is **exact**; never set without bit 6 |
| 8 | `SKETCH` | **v2 only.** A v2 blob appended a per-row-group sketch after the 56 bytes. In v3 the sketch is per file and lives in the column summary (§5.7); this bit MUST be clear and a reader MUST reject a v3 blob that sets it. |

A field whose flag is clear MUST be zero and MUST NOT be read.

`NDV_EXACT` is set when value ordering deduplicated the column, so
`data_length` **is** the exact distinct non-null count. `NDV` alone is a
write-side estimate (±~3% at the writer's K = 1024); a consumer needing a
bound, not an estimate, MUST require `NDV_EXACT`. A per-row-group `ndv` is a
scalar and cannot be combined across row groups; the file-level answer is the
sketch (§8.1).

**`min`/`max` are `ordinalize()` ordinals**, the same dialect the catalog
manifest speaks, so a predicate literal's ordinal compares directly against them
at plan time. Two consequences a reader MUST respect:

- Ordinals are **monotonic but not injective** — string ordinals pack the first 8
  content bytes and collide on a shared prefix. Pruning is therefore
  **conservative**: a row group may be read unnecessarily, never skipped wrongly.
  They MUST NOT be used as an equality proxy or a sort key.
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

### 8.1 The file-level sketch

Each column node MAY carry one KMV sketch (§5.7) over its **whole-file** values:
the `count` smallest distinct hashes, ascending, at most `k` of them.

**Hash family `2` — draken's `Vector.hash()`.** Each hash is exactly the value
draken's `Vector.hash()` produces for the value (per-type seed, then draken's
mix; authority `draken_hash` / `draken/simd/simd_hash.h`). Like every other
draken definition this document relies on, it is referenced, never redefined
(§2). This is the family ANALYZE and the catalog statistics engine sketch with,
so a skene sketch unions with theirs exactly.

Obligations that follow:

- **Nulls.** When the node has at least one null row anywhere in the file, the
  hash draken's `Vector.hash()` emits for a null row (`NULL_HASH` after mixing)
  is inserted **once**, as the catalog's sketches do. A sketch therefore describes
  the column the way every other family-2 sketch in the system does.
- **Canonicalisation is draken's.** Where `Vector.hash()` deliberately collides
  distinct bit patterns (an int64-backed DECIMAL against an equal DECIMAL128,
  fp16 patterns, INTERVAL normalisation), the sketch counts them once. Value
  ordering's deduplication keys on the **bit pattern** (§7.6) and is unaffected;
  so `NDV_EXACT` and the sketch can legitimately disagree on those types. They
  answer different questions.
- **Exact below K.** A node with at most `k` distinct hashes has all of them in
  the sketch, and `count` is the answer. Above `k` it is the standard KMV
  estimator, relative standard error ~`1/sqrt(k − 2)`.
- **Merge only within a family.** The union of two family-2 sketches is the `k`
  smallest of their combined hashes, exactly. A family-2 sketch MUST NOT be
  merged with a family-1 (v2 skene) sketch; the result would be a number with no
  meaning.
- **A persisted contract.** Stored sketches bind `Vector.hash()`'s output for
  every type they cover. A change to draken's hash for any type invalidates
  family 2; such a change MUST introduce a new family value, never reuse `2`.

No sketch is written for `DRAKEN_NULL`, `ARRAY`, `VECTOR_FP16` or a length-only
string column, and none under the spill profile (§10).

---

## 9. Optional sections

Optional sections live in the INDEX region (§3), are addressed through the index
slice of their chunk's record (§5.10, §5.12), and describe **one row group** of
**one column node**. A reader reaches them by fetching the node's INDEX extent
after its directory block.

### 9.1 `BLOOM` (256)

A Split-Block Bloom Filter, byte-compatible with the Parquet SBBF: a sequence of
32-byte blocks, each 8 little-endian `u32` words; XXH64 (seed 0) over the value's
plain bytes; block selection `((hash >> 32) * num_blocks) >> 32`; block count
always a power of two.

Built over the chunk's **`data` array** (`data_length` values), not the logical
rows — on a value-ordered chunk that is the deduplicated dictionary, so the
filter costs NDV insertions rather than row-count insertions and is exactly as
accurate.

Writer policy (not a reader obligation): filters are written for the **string
family only**. Fixed-width columns are pruned by statistics and zone maps.

### 9.2 `PERMUTATION` (257)

A row order of one row group under a multi-column sort specification.

```
u16 key_count │ u16 reserved │ u32 length          (8 bytes)
key_count × { u32 column_ordinal │ u8 descending │ u8 nulls_first │ u16 reserved }
length   × u32 row ordinals
```

`length` MUST equal its row group's `row_count`. `nulls_first` MUST follow draken's
single sort null-ordering rule — NULLS FIRST ascending, NULLS LAST descending — a
permutation written under a different rule is a different order, silently.

An identity permutation MUST NOT be stored; the correct encoding is
`ROW_SORTED` in `vector_flags`.

### 9.3 `ZONE_MAP` (258)

Per-row-chunk **value ordinal** bounds, for skipping rows *within* a row group.

```
u32 chunk_rows │ u32 chunk_count                    (8 bytes)
chunk_count × { i64 min_ordinal │ i64 max_ordinal } (16 bytes each)
```

Bounds are `ordinalize()` ordinals — the same dialect as §8's `min`/`max` and the
catalog manifest — so a predicate's literal ordinal compares against them
directly, whatever the chunk's encoding shape or ordering. `chunk_count` MUST be
`ceil(length / chunk_rows)`.

Nulls are excluded from a chunk's bounds; an **all-null** row chunk is written as
an empty range (`min_ordinal > max_ordinal`), which correctly answers "cannot
contain" for every probe.

Written for every column of an orderable type (§8's `MIN`/`MAX` rule) whose
chunk has **more than one** row chunk of rows — below that, the statistics blob
already says everything a zone map could. `chunk_rows` is `8192` in the reference
writer.

Tightness comes from **clustering**, not from value ordering: ordering sorts the
dictionary and rewrites codes but leaves logical row order untouched, so an
unclustered column's row chunks each span nearly its whole range.

A negative answer from a zone map is PROOF that a row chunk holds no match; a
positive answer is only "cannot rule it out", so a reader must still evaluate
the rows it decodes.

> **Correction carried into v3.** v2's specification described this section as
> `u32` min/max **codes** (8 bytes per entry), written only for value-ordered
> columns. The reference implementation has written 16-byte **ordinal** entries
> for every orderable column since before v2 was released, and v2 files contain
> that form. The byte layout here is the one in the files; the v2 text in
> FORMAT_v2.md is preserved as written, and the v2 reader follows format.h.

---

## 10. Spill profile

The same format with everything optional switched off: `value_order == 0`,
every statistics blob length `0`, every sketch absent, no optional sections.
Spill data is written once, read once, in-process, and wall-clock bound, so no
read acceleration is worth paying for. This is a **profile**, not a variant — a
spill file is an ordinary `.skene` file and any reader reads it.

A spill reader reads **whole row groups**. Under the column-major layout that is
one chunk from each column node's run rather than one contiguous range; on a
local mapping that is no extra IO.

---

## 11. Reader conformance

A conforming reader MUST validate in this order and MUST NOT interpret any
content before all of it passes:

1. Tail `magic`, then head `magic` if the head was read.
2. `version` within the supported window (§12) — otherwise fail naming **both**
   the file's version and the reader's, and the migration route.
3. `endianness` matches the host; `checksum_algorithm` is one this build
   implements.
4. `footer_bytes` is consistent with the object size.
5. `footer_checksum` over the footer.
6. `footer_magic` and `footer_version` (§5.2); `data_region_bytes` against the
   footer's offset; `block_row_groups ≥ 1`.
7. The row group table (§5.3) before any row count is used.
8. Every column summary (§5.6), before any of its offsets is followed:
   `directory_bytes > 0`; the directory block, the data extent and the block
   extents lie inside the DATA region and the index extent inside the INDEX
   region; runs are ordered by node and do not overlap; each directory block
   ends at or before its node's `data_offset`; `block_count` and `child_count`
   are correct; block extents lie inside the data extent, in order, non-
   overlapping; the sketch record's `reserved`, `hash_family`, `k` and `count`
   (§5.7), and its hashes strictly ascending.
9. Per directory block, as it is opened: its checksum against the summary; then
   `directory_magic`, `node_ordinal`, `chunk_count`, and the block ending exactly
   after its section entries. Then, per chunk record: `reserved0`/`reserved1`
   are zero; a top-level node's `length` against the row group table; both slices lie inside the section list; every
   required entry lies inside the node's data extent and every optional entry
   inside its index extent; chunk `i`'s required sections precede chunk
   `i + 1`'s; and the block extents recorded in the summary equal the extents
   the chunk records imply.
10. Each section's `checksum` before that section is used.
11. Structural consistency per chunk: `selection_kind` against
    `data_length`/`length` (§7.2); every selection code `< data_length`;
    `data_length <= string_slot_count` for string columns;
    `string_payloads_elided` against the slots and arena (§7.4); a node has one
    child iff its type is `DRAKEN_ARRAY`; an ARRAY chunk's offsets are
    non-negative, monotonic and never address past its child's `length`.
12. Unrecognised **required** section kind or encoding → reject. Unrecognised
    **optional** section kind → skip.
13. `reserved` in the head, the tail (§4.3), the footer header, every column
    summary, every sketch record, every chunk record, every section entry, the
    cluster spec header and every cluster key is zero.
14. Carried from v2, all rejections: the v1 codec-as-encoding values (3, 4) in
    `encoding`; the v1 `STRING_SLOTS` kind (4); an unrecognised `codec`; the
    §5.11 size invariants; every slot lane present for a string column and
    decoding to exactly `string_slot_count` u32s; cluster-key ordinals within
    the schema and `nulls_first` consistent with draken's rule. v3 additions: the
    `SKETCH` statistics bit (§8) set in any blob.

Section-offset ALIGNMENT is a writer obligation a reader MAY exploit but MUST
NOT require — offsets are absolute.

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
older version. The v3 build reads **v2 and v3** and writes v3; **v1 is outside
its window.**

A file more than one version behind is **migrated, not read**: `binary vX`
migrates `(X-1) → X` and nothing else, so a file at version `F` reaches `N` by
running binaries `v(F+1), v(F+2), … v(N)` in order. Binaries are retained as
releases for this purpose. A v1 file reaches v3 by running the retained v2
binary, then the v3 binary.

```
 v1 file ──[binary v2]──> v2 ──[binary v3]──> v3
```

Migration MUST be verified, not assumed: required-section content MUST round-trip
exactly, so a migrator re-reads its own output and compares decoded buffers
before anything replaces the original. Optional sections and statistics MAY be
dropped or rebuilt — they are reconstructible by definition (§7.1).

`skene::migrate_file` (`include/skene/migrate.h`; `skene.migrate` from Python)
rewrites a v2 file as v3 — exactly one hop, and a file already at the current
version or more than one behind is refused rather than silently copied or skipped
over. It is a rewrite, not a byte transform: each row group is read back through
the retained v2 reader into draken vectors and written by the current writer.
Provenance (`file_uuid`, `created_at_unix_us`, the original `writer_tag`, field
ids) is **carried from the source**, not reissued, and setting any of them on the
migration posture is rejected. Sketches are **recomputed** under family 2 from
the data; a v2 file's family-1 sketches are never carried into a v3 file.
Everything else about the posture (codec, read acceleration, cluster keys,
`block_row_groups`) is the caller's choice, and the writer re-verifies all of it
as it would for any write.

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
| New sketch hash family value | **No** — the family byte exists for this; a reader rejects a family it does not know |
| New required section kind, or a layout change to one | **Yes** |
| New encoding on a required section | **Yes** |
| Any change to the chunk record, directory block or section entry layout | **Yes** |
| Any change to the footer header, row group table, schema directory, column summary or sketch record layout | **Yes**, once frozen — and `footer_version` (§5.2) tracks it independently |
| Any change to bytes 0–5 of the head | **Never permitted** |

While the current version is DRAFT none of the above applies: the layout may
change without a bump. The one obligation a draft change still carries is that
files written before it MUST NOT be misread.

> **Correction carried into v3.** v2's version table did not list a new sketch
> family, because v2 had no family byte; that is why the hash-family change
> (§8.1) rode this bump.

---

## 13. Implementation status

**v3 is implemented** (2026-09-24). The reference implementation writes v3 and
reads v2 and v3. v1 support is removed: `reader_v1` and the v1 fixtures are
deleted, and `migrate_file` goes v2 → v3.

- **Writer**: two-pass. Buffer output stages sections in memory; path output
  stages to a caller-named scratch file and publishes through a partial file.
- **Reader**: whole-buffer (`open_reader`) for v2 and v3; ranged
  (`open_reader_ranged`, `plan_directory_fetch`, `attach_directories`,
  `plan_fetch`, `read_morsel` over fetched ranges) for v3 only. v2 decode is the
  frozen v2 reader over the shared chunk decoder.
- **Engine**: v3 files are read by ranged fetch with block work items; v2 files
  are mapped whole.
- **v2 fixtures** at `tests/fixtures/v2/` pin the v2 read path. They were
  written by the last v2 writer, before the bump; this tree can no longer
  produce v2 bytes, so the committed files are the artifact. The v2
  specification is frozen verbatim in [FORMAT_v2.md](FORMAT_v2.md).

Not implemented in any version: §9.2 permutations — nothing in the engine
produces a stored sort order yet, so the section would have no writer and no
consumer. It is an optional section and adds with no version bump when something
needs it.

Value ordering is not applied to ARRAY children. Ordering one is correct under
the uniform access contract, but it produces a dict-shaped array child, and
draken builds array children dense everywhere else — a storage layer is the wrong
place to hand the engine a shape it has never executed.

A writer MUST fail loud rather than emit a file whose `value_order`, statistics
or sketch claim more than it computed.
