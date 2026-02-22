# DRKM File Format Specification

This document describes the **DRKM** (Draken Morsel) container format used by
`opteryx.draken.storage.morsel_io` for serializing and deserializing
`Morsel` objects.  The format is versioned and forward‑compatible; the
current implementation in `DRKM v1` is defined below.

---

## High‑level layout

A DRKM file consists of four major sections stored sequentially:

1. **File Header** – fixed‑size metadata describing the payload.
2. **Column Directory** – one entry per column in the morsel.
3. **Data Blocks** – a sequence of independently compressed and checksummed
   blocks containing column segments.
4. **Footer Index** – a small trailer that locates the block directory and
   validates the file.

All multi‑byte integers are encoded little‑endian.

---

## File Header (fixed size `HEADER_SIZE` = 48 bytes)

Field                     | Type    | Description
------------------------- | ------- | -----------
magic                     | 4‑byte  | ASCII `DRKM`
format_version            | u16     | currently **1**
flags                     | u16     | reserved for future use
row_count                 | u64     | total number of rows in the morsel
column_count              | u32     | number of columns
block_count               | u32     | total blocks recorded in footer
schema_fingerprint        | u64     | user‑supplied schema hash
default_codec             | u8      | default block codec (`0`=none, `1`=lz4, `2`=zstd)
padding                   | 7 bytes | zeroed

---

## Column Directory

For each column a directory entry records name, type, encoding, and block
range.

Field                  | Type    | Description
---------------------- | ------- | -----------
name_len               | u16     | length of UTF‑8 column name
draken_type            | u16     | value from `DrakenType` enum
encoding               | u8      | `0`=fixed‑width, `1`=var‑width
flags                  | u8      | bit‑flags (`1` = has nulls)
block_start            | u32     | first block id for this column
block_end              | u32     | one‑past‑last block id

Entries are concatenated; names themselves immediately follow the directory
in the on‑disk layout.

---

## Data Blocks

Each logical segment of a column (null bitmap, fixed data, offsets, values)
is stored as one block with its own header and compressed payload.

Block header (`BLOCK_HEADER_SIZE` = 32 bytes):

Field            | Type    | Description
---------------- | ------- | -----------
block_id         | u32     | sequential identifier
column_id        | u32     | zero‑based index into column directory
segment_kind     | u8      | `0`=nulls, `1`=data, `2`=offsets, `3`=values
codec            | u8      | codec id (see above)
row_start        | u64     | starting row index for the segment
row_count        | u32     | number of rows covered
flags            | u8      | reserved
raw_len          | u32     | uncompressed byte length
comp_len         | u32     | compressed byte length
checksum32       | u32     | xxhash32 of uncompressed payload
padding          | 1 byte  | unused

Following the header are exactly `comp_len` bytes of payload.  When read the
payload is decompressed (if compressed) and placed directly into Draken
vector buffers.

### Segment kinds

* `SEG_NULL` – bitmap of null values (one bit per row)
* `SEG_DATA` – fixed‑width element array
* `SEG_OFFSETS` – var‑width offset array (int32)
* `SEG_VALUES` – var‑width byte arena

Compression is per‑block, so offsets and values can use different codecs and
degrees of compression.

---

## Footer Index (`FOOTER_SIZE` = 24 bytes)

Field           | Type    | Description
--------------- | ------- | -----------
dir_offset       | u64     | file offset where block directory begins
dir_len          | u32     | length of block directory (bytes)
footer_checksum  | u32     | xxhash32 of directory
block_count      | u32     | repeat of header block_count
footer_magic     | 4‑byte  | ASCII `DRKF`

The footer is located at the end of the file; readers seek backwards from the
end to locate it and then load the directory to discover block metadata.

---

## Codecs and constants

| Codec id | Name |
|----------|------|
| 0        | none |
| 1        | lz4  | default for spill files
| 2        | zstd |

Constants used by the implementation can be found in
`morsel_io.pyx` (e.g. `CODEC_*`, `ENCODING_*`, `SEG_*`).

---

## Vector mapping

* **Fixed‑width vectors** – null bitmap (optional) followed by a contiguous
  data segment containing raw values.
* **String/binary vectors** – null bitmap (optional), `SEG_OFFSETS` block with
  `int32[length+1]` offsets, then `SEG_VALUES` block containing concatenated
  bytes.

Offsets and values are stored separately to allow independent compression.

---

## Future extensions

* version bump and header flag field for new features
* encryption / AEAD envelope per file
* selective column/block read (using directory offsets)
* additional metadata in footer (min/max, statistics)

This specification is intentionally concise; consult
`docs/draken-morsel-storage-io-design.md` for design reasoning and history.
