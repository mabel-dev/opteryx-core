# Parquet Bloom Filter Footer Mutator Design

**Last updated:** 2026-03-08  
**Status:** Draft for review  
**Scope:** `rugo` Parquet mutation only

---

## Objective

Add a minimal Parquet mutator in `rugo` that can attach row-group Bloom
filters for selected string columns to an existing Parquet file.

The design goal is intentionally narrow:

- do not rewrite data pages or column chunks
- do not require a new Parquet writer
- do not change query planning in the same change
- produce a valid Parquet file with Bloom filters recorded in footer metadata

This is the enabling step. Once we can write valid Bloom-filter-bearing
Parquet files, Opteryx can decide later whether and how to use those Bloom
filters for pruning.

---

## Current State

`rugo` can already read Bloom filter metadata and test Bloom filter contents.

Confirmed capabilities:

- `read_metadata()` surfaces `bloom_offset` and `bloom_length`
- `test_bloom_filter(path, bloom_offset, bloom_length, value)` evaluates a
  Parquet Bloom filter blob for a single value
- test coverage already exists for reading and probing Bloom filters

What does not exist today:

- a Python-accessible writer for Parquet Bloom filters
- a footer mutator that can update `ColumnMetaData`
- scan-time use of Bloom filters in Opteryx predicate pruning

This design only addresses the missing writer/mutator.

---

## Problem

We want to add Bloom filters to files that were written without them.

PyArrow does not expose Bloom filter writing in the Python API, so we cannot
rely on `pyarrow.parquet.write_table()` or `ParquetWriter` for this.

We also cannot "just append" Bloom filter blobs to the end of a Parquet file.
For the file to remain valid, the active footer must still be the last logical
structure in the file:

```text
[data and metadata payloads]
[footer]
[4-byte footer length]
[PAR1]
```

That means any solution must:

1. preserve all pre-footer data layout
2. place Bloom filter blobs before the new footer
3. write a new footer at the end of the file
4. write a new trailer

This is a footer rewrite, not a raw append.

---

## Non-Goals

Initial implementation will not:

- build Bloom filters for every type
- add page-level Bloom filters
- rewrite column chunks or data pages
- support encrypted Parquet files
- expose planner integration in Opteryx
- solve remote object-store mutation in place
- become a general-purpose Parquet editor

This is a targeted mutator for one metadata feature.

---

## Design Principles

1. Preserve data-page byte offsets.
2. Mutate only the tail of the file.
3. Keep the implementation narrow enough to validate quickly.
4. Prefer safe file replacement over in-place mutation.
5. Reuse `rugo`'s existing Parquet metadata structures where possible.

---

## File Rewrite Model

### Existing File

```text
[column chunks / pages / dictionaries / indexes]
[file metadata footer]
[footer length]
[PAR1]
```

### New File

```text
[column chunks / pages / dictionaries / indexes]
[bloom filter blob 1]
[bloom filter blob 2]
...
[new file metadata footer]
[new footer length]
[PAR1]
```

Important detail:

- everything before the old footer start offset is copied unchanged
- old footer bytes are not preserved as the active footer
- the new footer records the new Bloom filter offsets and lengths

Because the pre-footer region is unchanged, existing column chunk offsets in
the metadata remain valid.

---

## Why Tail Rewrite Is Enough

Parquet column metadata points backwards into column chunks and pages already
written earlier in the file. Adding Bloom filters does not require moving those
column chunks.

Therefore:

- copy bytes `0..old_footer_start`
- append Bloom filter blobs
- append rewritten footer
- append trailer

That is sufficient to create a valid new file image.

We do not need a full Parquet file rewrite.

---

## Proposed Scope Of v1

Support only:

- local files
- row-group Bloom filters
- selected string-like columns
- one Bloom filter per `(row_group, column_chunk)`
- safe rewrite via temp file and rename

String-like means:

- `BYTE_ARRAY` with UTF8/string logical type
- optionally plain `BYTE_ARRAY` when explicitly requested

Skip in v1:

- nested/repeated leaf paths
- fixed-len byte arrays
- binary blobs with unknown semantics
- columns that already have Bloom filters unless `replace=True`

---

## High-Level Architecture

Three pieces are required.

### 1. Metadata Inspection

Read and retain:

- schema
- row groups
- column chunk metadata
- footer start offset
- existing Bloom filter metadata, if any

This can reuse `rugo`'s existing metadata reader for logical inspection, but
the mutator also needs enough footer information to serialize a new footer.

### 2. Bloom Filter Construction

For each selected `(row_group, column)`:

- decode the column chunk
- iterate non-null string values
- build a Parquet split-block Bloom filter bitset
- serialize a Bloom filter header followed by the bitset

The output of this stage is:

- serialized blob bytes
- blob length
- future file offset where the blob will be written

### 3. Footer Rewrite

Clone the parsed footer metadata and update each affected column chunk:

- `bloom_filter_offset`
- `bloom_filter_length`

Then serialize the modified footer and write the new trailer.

---

## Required `rugo` Additions

The missing capability is not reading Bloom filters. The missing capability is
writing footer metadata back out.

### A. Minimal Footer Serializer

Add a small footer serializer in `rugo` that can emit a valid Parquet
`FileMetaData` thrift structure from the in-memory metadata representation.

Required fields:

- schema
- version
- created_by
- num_rows
- row_groups
- key_value_metadata
- column orders if present

For this feature, the critical mutated fields are inside each
`ColumnMetaData`:

- `bloom_filter_offset`
- `bloom_filter_length`

The serializer should preserve all fields that were parsed, even if the
mutator did not change them.

### B. Bloom Filter Serializer

Add a `rugo` helper that can produce a Parquet-format split-block Bloom filter
payload for a sequence of string values.

Required output:

- Bloom filter header
- Bloom filter bitset

The serialized blob should match the format already supported by
`TestBloomFilter`.

### C. Mutation Entry Point

Expose a Python-accessible function, for example:

```python
mutate_add_bloom_filters(
    path,
    columns,
    false_positive_rate=0.01,
    mode="safe",
    replace=False,
)
```

This should be a focused mutator API, not a generic metadata editor.

---

## Proposed Mutation Algorithm

### Stage 1. Inspect Source File

1. Open source file.
2. Read final 8 bytes to determine footer length and validate `PAR1`.
3. Compute:
   - file size
   - footer length
   - footer start offset
4. Parse footer metadata.
5. Validate selected columns:
   - exist
   - are string-like
   - are not nested unsupported shapes
   - do not already have Bloom filters unless `replace=True`

### Stage 2. Build Bloom Filters

For each row group and selected column:

1. Decode only that column chunk.
2. Skip nulls.
3. Encode each value as bytes.
4. Insert values into a Parquet split-block Bloom filter builder.
5. Serialize header + bitset.
6. Record the blob for later append.

### Stage 3. Write New File

In safe mode:

1. Create `path + ".tmp"` in the same directory.
2. Copy source bytes from `0` to `old_footer_start`.
3. Append all Bloom filter blobs.
4. Compute and record each blob's offset in the new file image.
5. Update copied footer metadata with those offsets and lengths.
6. Serialize the new footer.
7. Write:
   - new footer bytes
   - 4-byte footer length
   - `PAR1`
8. `fsync`
9. atomically rename temp file over source file

The original file remains untouched until the rename.

---

## Why Safe Rewrite Should Be The Default

In-place mutation is attractive but fragile:

- partial writes corrupt the file
- a crash after truncation is unrecoverable
- replacing the footer requires careful ordering and rollback

Temp-file rewrite is slower but operationally correct and much easier to trust.

For local storage, correctness is the right default.

If needed later, an in-place local-only fast path can be added behind a
separate flag.

---

## Bloom Filter Construction Policy

### Initial Policy

Use one Bloom filter per row group per selected string column.

Sizing policy:

- estimate item count as non-null row count for the column chunk
- target false positive rate default `1%`
- round bitset size up to Parquet split-block alignment

This is sufficient for a first implementation. It does not need cardinality
estimation or adaptive tuning in v1.

### Null Handling

- nulls are not inserted
- a null predicate cannot be answered from the Bloom filter

### Duplicate Values

- duplicates are inserted normally
- no distinct pass is required

This keeps build cost linear in decoded values.

---

## Metadata Update Rules

For each affected `ColumnMetaData`:

- set `bloom_filter_offset` to the absolute file offset of the appended blob
- set `bloom_filter_length` to the serialized blob length

All other metadata must remain unchanged.

If a column chunk already contains a Bloom filter:

- default behavior: fail
- optional `replace=True`: overwrite metadata to point at the new blob

Even with `replace=True`, v1 does not attempt to reclaim old Bloom filter bytes.

---

## Validation Strategy

The mutator is complete only if it proves three things:

1. the rewritten file is still a valid Parquet file
2. the metadata exposes the new Bloom filter offsets
3. `rugo.test_bloom_filter()` returns expected answers

### Required Tests

#### 1. Smoke Test

- write a small Parquet file without Bloom filters
- run the mutator on one string column
- assert metadata now reports non-null `bloom_offset` and `bloom_length`

#### 2. Membership Test

- known present values return `True`
- known absent values return `False` for at least obvious negatives

#### 3. File Validity Test

- decode the rewritten file normally with `rugo`
- read all row groups and compare content with the original file

#### 4. Offset Stability Test

- verify column chunk offsets before the old footer remain unchanged
- verify only footer-related metadata changed

#### 5. Existing Bloom Filter Guard

- mutating a column that already has a Bloom filter fails unless
  `replace=True`

---

## Failure Handling

The mutator should fail fast on:

- invalid Parquet magic
- unreadable footer
- unsupported schema shapes
- unsupported selected columns
- encrypted files
- malformed existing metadata

Failure behavior in safe mode:

- delete temp file if possible
- leave source file unchanged

This should be a hard requirement.

---

## Integration With Opteryx

This change should not automatically change scan behavior.

Immediate result:

- Opteryx can create Bloom-filter-bearing Parquet files through `rugo`
- test fixtures with Bloom filters become easy to generate

Later query-time use can be added separately:

- for `col = literal`
- for `col IN (...)`
- only when metadata exposes a Bloom filter for the relevant column chunk

That planner/reader integration should be a second design and a second change.

---

## API Sketch

Suggested Python API:

```python
from opteryx.rugo import parquet

parquet.add_bloom_filters(
    "dataset.parquet",
    columns=["user_id", "email"],
    false_positive_rate=0.01,
    replace=False,
)
```

Possible future options:

- `include_binary=False`
- `output_path=None`
- `row_groups=None`

But v1 should stay minimal.

---

## Alternatives Considered

### 1. Full File Rewrite

Pros:

- conceptually simple
- can rebuild everything from a higher-level writer

Cons:

- much more I/O
- requires a real Parquet writer path
- unnecessary when only footer metadata changes

Rejected for v1.

### 2. In-Place Tail Mutation

Pros:

- avoids second file copy

Cons:

- fragile on crash
- difficult to make safe
- hard to recover from partial failure

Rejected as default behavior.

### 3. Sidecar Bloom Filter Files

Pros:

- easy to write
- no Parquet mutation required

Cons:

- not standard Parquet
- portability is worse
- external tools cannot use them

Still useful as a fallback strategy, but not the goal of this design.

---

## Open Questions

1. Should v1 accept plain `BYTE_ARRAY` only when explicitly requested, or
   always treat it as Bloom-filterable?
2. Should the footer serializer live in C++ alongside the parser, or in
   Cython/Python using a minimal thrift writer?
3. Should the mutator overwrite in place only when an explicit
   `unsafe_in_place=True` option is set?

---

## Recommendation

Implement the smallest viable write path in `rugo`:

- a Parquet Bloom filter serializer
- a minimal footer serializer
- a safe temp-file tail rewrite mutator for selected string columns

This is enough to generate real Bloom-filter-bearing Parquet files for local
testing and future Opteryx pruning work, without taking on the complexity of a
general Parquet writer.
