# Draken Morsel Storage IO Design (Spill Read/Write)

## Scope

Design a fast, Draken-native storage format and IO path for persisting/reloading `Morsel` data for spill and replay.

Requirements:

- Draken-native read/write path.
- No Arrow conversion in storage IO hot path.
- High throughput on both write and read.
- Fast compression/decompression to reduce memory and IO pressure.
- Include LZ4 vendoring plan.

This document focuses on storage IO only. It does not define planner/operator integration.

---

## Goals

1. Serialize and deserialize Draken morsels with minimal copies.
2. Support codec-per-block compression.
3. Prefer low-latency codec path for spill (`LZ4` default).
4. Keep format forward compatible (versioned, self-describing).
5. Allow selective read (column-level and block-level skipping in future).

## Non-Goals (v1)

1. Distributed shuffle protocol.
2. External object-store spill format stability guarantees.
3. Cross-language public format commitment.

---

## Format Overview

Introduce a new container format: `DRKM` (Draken Morsel Container).

> **Note:** the **precise on‑disk layout** has been extracted to a
> standalone specification document located at
> `opteryx-core/third_party/mabel/draken/storage/SPECIFICATION.md`.  The
> design discussion below explains the rationale and goals; implementation
> details live in the spec and accompanying tests.


## File Layout

1. File Header
2. Column Directory
3. Data Blocks
4. Footer Index

### 1) File Header

Fixed-size header fields:

- magic: `DRKM`
- format_version: `u16`
- flags: `u16`
- row_count: `u64`
- column_count: `u32`
- block_count: `u32`
- schema_fingerprint: `u64`
- default_codec: `u8`

### 2) Column Directory

One entry per column:

- column_id / name reference
- draken_type enum
- nullability
- encoding kind:
  - fixed-width
  - var-width
- block range (start/end block ids)

### 3) Data Blocks

Blocks are independently compressed and checksummed.

Block header:

- block_id: `u32`
- column_id: `u32`
- chunk_row_start: `u64`
- chunk_row_count: `u32`
- codec: `u8` (`NONE`, `LZ4`, `ZSTD`)
- uncompressed_len: `u32`
- compressed_len: `u32`
- checksum32: `u32`

Payload:

- compressed bytes for exactly one logical segment:
  - fixed-width data
  - null bitmap
  - var-width offsets
  - var-width values

### 4) Footer Index

Footer includes:

- block directory offset/length
- optional min/max metadata placeholders (future)
- footer checksum

---

## Mapping Draken Vectors to Storage

## Fixed-Width Vectors

Persist as:

- null bitmap segment (if present)
- data segment

## String/Binary Vectors (Var-Width)

Persist as:

- null bitmap segment (if present)
- offsets segment (`int32[length+1]`)
- values segment (bytes arena)

Design note:

- Offsets and values are stored as separate blocks so codecs can adapt independently.

## Morsel Schema / Names

Store column names once in metadata section (UTF-8 bytes + length).

---

## Compression Strategy

## Default Policy

- default codec: `LZ4` for spill blocks
- optional override per block/column to `ZSTD` (cold/large blocks)

Why:

- LZ4 gives lowest read/write latency and high decompression throughput.
- ZSTD can be applied where ratio matters more than write latency.

## Adaptive Policy (v1.1 target)

For each column segment type:

1. sample first N blocks with LZ4
2. if ratio below threshold and spill pressure high, switch future blocks to ZSTD-1

---

## Read/Write API

Add dedicated storage module:

- `opteryx/draken/storage/morsel_io.pyx` (Python entrypoints + Cython)
- `opteryx/draken/storage/morsel_io.pxd`
- C++ helpers under `third_party/mabel/draken/storage/`

Core API:

- `write_morsel(path_or_handle, morsel, options) -> MorselHandle`
- `read_morsel(path_or_handle) -> Morsel`
- `iter_morsel_blocks(path_or_handle, columns=None) -> iterator[Morsel]` (future)

Options:

- `codec_default`: `lz4|zstd|none`
- `zstd_level`
- `target_block_bytes`
- `checksum_enabled`

---

## Memory / Copy Semantics

Write path:

1. Read Draken buffers directly.
2. Compress buffer slices into output blocks.
3. Write block bytes sequentially.

Read path:

1. Read block bytes.
2. Decompress into owned buffers.
3. Construct vectors from decompressed buffers.
4. Build `Morsel.from_vectors(...)` without Arrow conversion.

Target: single-copy between decompressed bytes and final vector buffers in most cases.

---

## LZ4 Vendoring Design

Current project already vendors Snappy and ZSTD through `third_party/mabel/rugo/parquet/vendor/*` and builds them via `setup.py`.

Use the same pattern for LZ4.

## Vendor Location

Add:

- `third_party/mabel/rugo/parquet/vendor/lz4/lz4.h`
- `third_party/mabel/rugo/parquet/vendor/lz4/lz4.c`

Optional later:

- `lz4hc.c` (if high-compression mode is needed)
- frame API files are not required for block mode v1

## Build Integration

In `setup.py`:

1. add helper:
- `get_lz4_vendor_sources() -> [ .../vendor/lz4/lz4.c ]`
2. include LZ4 headers path in relevant extension `include_dirs`
3. add LZ4 source to new morsel IO extension sources

Do not depend on system liblz4; keep vendored static compile behavior consistent with zstd/snappy usage.

## Wrapper Module

Create lightweight Cython wrapper:

- `opteryx/third_party/lz4/lz4.pyx`

Expose minimal block API:

- `compress_bound(size)`
- `compress_block(src) -> bytes`
- `decompress_block(src, uncompressed_size) -> bytes`

No frame API in v1.

---

## Integrity and Corruption Handling

Checks:

1. magic/version validation
2. header/footer checksum
3. per-block checksum
4. decompressed length validation

Failure behavior:

- hard fail with structured storage-corruption error
- include block id and column id for diagnostics

---

## Compatibility / Versioning

Format version rules:

- minor-compatible additions allowed with feature flags in header
- breaking changes require version bump and explicit reader gating

Recommended initial:

- `DRKM v1`

---

## Telemetry

Add IO counters:

- `morsel_io_blocks_written`
- `morsel_io_blocks_read`
- `morsel_io_bytes_raw_written`
- `morsel_io_bytes_compressed_written`
- `morsel_io_bytes_compressed_read`
- `morsel_io_bytes_decompressed_read`
- `morsel_io_codec_lz4_blocks`
- `morsel_io_codec_zstd_blocks`
- `morsel_io_write_time_ns`
- `morsel_io_read_time_ns`
- `morsel_io_decompress_time_ns`

---

## Rollout Plan

### Phase 1: Format + LZ4 baseline

- implement `DRKM v1`
- implement `NONE` + `LZ4` codecs
- add unit tests for round-trip and corruption checks

### Phase 2: ZSTD blocks

- add `ZSTD` block codec support
- add codec selection policy hooks

### Phase 3: Performance hardening

- block size tuning
- reduced copy path improvements
- telemetry-driven defaults

---

## Decisions (Captured)

1. Block size default:
- default target block size: `1MB` uncompressed.
- note: larger blocks generally improve compression ratio; the tradeoff is higher tail latency for small/random reads.
- keep configurable (`256KB` / `512KB` / `1MB`) for tuning.

2. Block integrity:
- use `xxhash` per block for fast corruption detection.
- note: this is integrity detection, not cryptographic authenticity.

3. Codec defaults:
- default codec: `LZ4`.
- optional user override to `ZSTD` / `SNAPPY` / `NONE`.

4. Format evolution:
- treat `DRKM` as an evolving internal format.
- maintain strict versioning and reader gating for breaking changes.

5. Spill lifecycle:
- delete spill files eagerly after consumption.
- support retention under explicit debug flag.

6. Encryption in v1:
- no at-rest encryption requirement in version 1.

## Remaining Open Item

1. Optional encryption path (future):
- Option A: filesystem-level encryption (simplest operationally).
- Option B: per-file AEAD envelope in `DRKM` (`XChaCha20-Poly1305` or `AES-GCM`) with key-provider hook.
- Option C: process-local ephemeral key with memory-only key lifecycle for temporary spill files.
