# Parquet Row-Group Columnar Read Design

## Motivation

Opteryx currently treats a file as the primary I/O unit. For large
Parquet datasets this is inefficient: most analytical queries require
only a subset of columns and often only a subset of row groups.

To improve cold-read performance, reduce bandwidth pressure (especially
in Cloud Run), and enable effective caching, we shift the storage I/O
unit from:

> **File-level reads**

to:

> **Row-group × column-chunk range reads**

This design targets Parquet exclusively for the hot execution path.
Other formats may be supported at ingestion time but are not part of the
optimized scan path.

------------------------------------------------------------------------

## Architectural Principles

1.  **Parquet-only execution format**
    -   All optimized scans assume Parquet storage.
    -   Other formats should be converted to Parquet at ingest time.
2.  **Footer-first planning**
    -   All scans begin by fetching and parsing the Parquet footer.
    -   Row group and column pruning decisions are made before any data
        pages are read.
3.  **Storage unit ≠ execution unit**
    -   Storage I/O unit: row group (per column chunk).
    -   Execution unit (morsel): \~10k rows (sliced from decoded row
        group).
4.  **I/O layer remains format-agnostic**
    -   I/O provides `read_ranges(object, [(offset,length), ...])`.
    -   Planning and Parquet interpretation remain in EXEC.
    -   Decoding remains in EXEC.
    -   I/O does not understand Parquet semantics.
5.  **Adaptive read strategy**
    -   If projected columns ≈ full schema and selectivity ≈ high, fall
        back to full-object streaming.
    -   Otherwise use selective range reads.

------------------------------------------------------------------------

## Read Units

### 1. Footer

Footer contains:

-   Schema
-   Row group boundaries
-   Column chunk offsets
-   Statistics (min/max, null count, etc.)

Footer read process:

1.  Range GET last 8 bytes.
2.  Determine footer length.
3.  Range GET full footer.
4.  Parse metadata.

Footers are small (typically tens of KB) and highly cacheable.

------------------------------------------------------------------------

### 2. Data Unit: Row-Group × Column-Chunk

The smallest atomic fetch unit:

    (file_uri, row_group_idx, column_id)

This corresponds to a contiguous compressed byte range in the file.

Important constraint:

-   Range size should generally fall in the 1--16MB compressed window.
-   Sub-512KB ranges are likely to become latency-bound in object
    storage.

Row-group sizing must therefore ensure column chunks are not excessively
small.

------------------------------------------------------------------------

## Row Group Sizing Guidance

Row groups should target:

-   4--16MB compressed per row group (typical analytic sweet spot).
-   Not \~200KB total (too latency-sensitive).

Row group size determines:

-   Cache entry size
-   I/O efficiency
-   Scheduling granularity
-   Redis memory pressure

------------------------------------------------------------------------

## Reader API

The reader exposes storage primitives only:

### `fetch_footer(file_uri)`

Returns parsed footer metadata.

Flow:

1.  Check footer cache.
2.  On miss, perform footer range reads.
3.  Parse and cache.

------------------------------------------------------------------------

### `read_ranges(file_uri, ranges)`

Low-level primitive:

    ranges: List[(offset, length)]

Returns raw byte buffers.

This function:

-   May parallelize range reads.
-   May merge adjacent ranges.
-   Enforces concurrency limits.
-   Does not interpret Parquet.

------------------------------------------------------------------------

Higher-level Parquet planner in EXEC:

### `fetch_columns(file_uri, row_group_idx, column_ids)`

Flow:

1.  Use cached footer.
2.  Determine column chunk offsets.
3.  Construct required byte ranges.
4.  Check data cache for each `(file, rg, col)`.
5.  Issue `read_ranges` for misses.
6.  Decode in EXEC.
7.  Optionally populate cache.

------------------------------------------------------------------------

## Adaptive Strategy

To avoid penalizing `SELECT *` or wide scans:

If:

    required_bytes / total_file_bytes > threshold

Then:

-   Use full-object streaming path.
-   Decode sequentially.

Else:

-   Use selective row-group × column range reads.

This prevents excessive request fragmentation.

------------------------------------------------------------------------

## Caching Strategy

Two independent caches:

### 1. Footer Cache

Key:

    file_uri (+ generation/version if available)

Value:

-   Parsed footer metadata.

------------------------------------------------------------------------

### 2. Data Cache

Key:

    (file_uri, row_group_idx, column_id)

Supported return value:

#### Returns LZ4-compressed Draken Vector (Preferred)

-   Store execution-ready column vectors.
-   Avoid network.
-   Avoid Parquet decode.
-   Avoid Arrow conversion.
-   Decompress + use directly.

This shifts bottleneck from network-bound to CPU-bound, which is
desirable.

------------------------------------------------------------------------

## Cache Characteristics

Row group granularity:

-   Avoid file-level cache explosion.
-   Avoid morsel-level key explosion.
-   Balance value size vs key count.

Redis considerations:

-   Prefer fewer, larger entries over many tiny entries.
-   Avoid sub-100KB value sizes when possible.
-   LRU eviction sufficient initially.

------------------------------------------------------------------------

## Concurrency & Range Planning

Optimizations:

-   Merge adjacent column chunks into single range reads.
-   Throttle concurrent requests.
-   Maintain connection reuse.
-   Avoid excessive small range reads.

Parallelism exists at:

-   File level
-   Row-group level
-   Column level (within limits)

------------------------------------------------------------------------

## Execution Flow

Cold read:

1.  Fetch footer.
2.  Determine required row groups.
3.  For each (rg, col):
    -   Cache lookup.
    -   Range read on miss.
    -   Decode → Draken.
    -   Cache store.

Warm read:

-   Mostly cache hits.
-   No network.
-   No Parquet decode.
-   LZ4 decompress → immediate execution.

------------------------------------------------------------------------

## Error Handling

-   Footer corruption → invalidate cache and retry.
-   Data cache corruption → discard and refetch.
-   Range failures → retry with backoff.
-   Partial range mismatch → reissue single-range read.

------------------------------------------------------------------------

## Migration Plan

1.  Implement footer-first planner.
2.  Introduce `read_ranges` primitive.
3.  Build row-group planner in EXEC.
4.  Implement data cache (compressed bytes first).
5.  Add Draken-vector caching.
6.  Enable adaptive strategy.
7.  Benchmark:
    -   Cold narrow scans
    -   Cold wide scans
    -   Warm narrow scans
    -   Warm wide scans

------------------------------------------------------------------------

## Future Work

-   Intelligent range coalescing heuristics.
-   Generation-aware invalidation.
-   Split-level scheduling improvements.
-   Dynamic row-group sizing policy.
-   Workload-driven adaptive caching thresholds.

------------------------------------------------------------------------

## Summary

This design shifts Opteryx from:

> File-scanning engine

to:

> Row-group aware, selective, cache-optimized columnar engine

It preserves clean separation of:

-   I/O (bytes only)
-   Planning (metadata)
-   Decoding (Parquet → Draken)
-   Execution (vectorized operators)

While enabling:

-   Reduced bandwidth pressure
-   Better Cloud Run efficiency
-   Fine-grained caching
-   Improved warm-query performance
