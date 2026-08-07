# skene

A draken-native columnar file format: reader, writer, and specification.

`.skene` stores one row group of [draken](../opteryx-core/draken) vectors
losslessly — including the things Parquet drops. An IPv4 column is a `UINT32`
refined by an `IPV4` logical descriptor; Parquet stores the 32 bits and loses the
refinement on every round trip. `.skene` carries it. It likewise **restores** a
column's dictionary encoding and layout hints rather than re-deriving them.

It is not portable, and no foreign reader is promised. Parquet stays the default
for interchange and for most stored datasets; `.skene` is for the cases where the
draken-native round trip is what matters — query results, sort spill, and
datasets we want optimised rather than interoperable.

- **[FORMAT.md](FORMAT.md)** — the normative specification. Complete enough to
  write an independent reader from.
- **Rationale** — `opteryx-core/docs/SKENE_FILE_FORMAT_DESIGN.md`: why Parquet
  and the existing IPC format were rejected, what was traded, what is still open.

C++17, no dependencies, no Python.

## Build

skene imports draken's headers; it never copies them. Point it at an
`opteryx-core` checkout — a sibling directory by default:

```
make            # library and tests
make test       # build and run every test
make DRAKEN_ROOT=/path/to/opteryx-core
```

## Status

**v1 is a DRAFT and is not frozen.** Byte layouts are implemented and tested, but
fields may still change without a version bump until v1 is released.

Implemented, **writer and reader**: the complete required-section layout in
`AS_WRITTEN` mode — every family (fixed-width, BOOL, the string family including
length-only columns, ARRAY with recursive children, NULL), all three selection
kinds, LogicalType round-trip, per-section and footer checksums, head/tail
framing, column selection, metadata-only reads with per-column byte extents, and
version identification. That is exactly the shape sort spill needs.

Also implemented: **value ordering** (sort + deduplicate each column's values,
with the selection carrying row order, so a predicate becomes a binary search and
`data_length` is the exact distinct count) and **statistics** (min/max ordinals,
null count, exact 128-bit sum).

Also implemented: three encodings (bit packing for selection codes,
delta+bitpack for ascending integer data, and per-section zstd), zone maps, and
bloom filters.

Per-section compression matters more than expected: on TPC-H, skene without it is
1.9-3.8x larger than the equivalent ZSTD Parquet, and with it 0.92-1.09x. See
[BENCHMARKS.md](BENCHMARKS.md).

Not implemented: permutations — deferred because nothing produces a stored sort
order yet — and migration, which needs a v2 to migrate from.

A file never claims more than was computed: a column ineligible for ordering is
written as-written, and a statistic that cannot be defined is absent rather than
zero.

## Versioning

A build reads **two** versions — the one it writes and its predecessor — and
writes exactly one. Older files are migrated forward one hop at a time using
retained release binaries: `binary vX` migrates `(X-1) → X`, so a v1 file reaches
v4 by running v2, then v3, then v4. See [FORMAT.md §12](FORMAT.md).

Because of that, any build can *identify* any file even when it cannot read it —
which is what freezes the first six bytes of the header for all time.
