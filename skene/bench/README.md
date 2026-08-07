# bench — skene vs the rugo Parquet writer

**This directory is NOT part of `libskene.a`.**

The benchmark links rugo's Parquet writer to compare the two formats on the same
data. That does not make skene depend on rugo: the rule is that neither *library*
imports the other, and this is a third-party harness that happens to hold both —
the same shape as opteryx holding both ends of a rugo → morsel → skene pipeline.

`make` builds only the library and tests. `make bench` builds this. The library
target never sees a rugo header, and a `grep` for `rugo` under `src/` or
`include/` must stay empty.

## What it measures

Two comparisons, because they answer different questions:

- **Parquet UNCOMPRESSED vs skene** — isolates the encodings. Neither side gets a
  general-purpose compressor, so this is a like-for-like test of dictionary
  encoding, bit packing and delta against Parquet's own encodings.
- **Parquet ZSTD vs skene** — the production comparison, since job results are
  written as zstd Parquet today. skene has no general-purpose compressor at all,
  so this is deliberately the unfavourable comparison, and it is the one that
  decides whether the format is worth keeping for that path.

Reported for each: bytes and write wall-clock. Read time is measured for skene
only — rugo's reader is a separate pipeline with its own threading, and timing it
against a single-threaded in-memory read would compare the harnesses rather than
the formats.

## Building

Needs an opteryx-core checkout for both draken and rugo:

```
make bench                        # sibling ../opteryx-core
make bench DRAKEN_ROOT=/path/to/opteryx-core
```

Nothing here is built into opteryx-core, and opteryx-core's build files are not
modified. This work is speculative; it has to be deletable without a trace.
