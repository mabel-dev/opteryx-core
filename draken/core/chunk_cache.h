// Process-wide cache of decompressed parquet column chunks (C7, T1).
//
// One cache per process, in draken_native.so only (chunk_cache.cpp is compiled
// into that extension alone); rugo's decoder reaches it through these C symbols,
// resolved at load like draken_mem_*. Design and rulings:
// docs/C7_PAGE_CACHE_DESIGN.md (§12-§23).
//
// An entry is ONE column chunk in one buffer, plus an index from each page's
// offset in the caller's chunk to that page's bytes in the buffer. For parquet
// the buffer is the chunk IMAGE: the chunk re-laid as an uncompressed column
// chunk (rewritten page headers + decompressed bodies), so a hit decodes without
// the fetched bytes and the IO planner can skip the GET
// (docs/C7_GET_SKIP_AND_DECODED_CACHE_DESIGN.md, Part A).
// Only complete chunks are inserted (ruling: partial chunks are not cached).
// Files are immutable for the life of the process (ruling), so the key is the
// file path plus the chunk's first byte offset.
//
// Memory: entry bytes are reported to the process memory account
// (mem_account.h) as CACHE bytes, separately from query bytes. The account
// shrinks the cache when query memory grows (give-way); the cache never
// refuses a query allocation.
#ifndef DRAKEN_CORE_CHUNK_CACHE_H
#define DRAKEN_CORE_CHUNK_CACHE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DrakenChunkEntry DrakenChunkEntry;

// 1 when the cache has a budget. The budget lives in the memory account
// (draken_mem_configure); the caller applies the < 2 GiB "off" rule by
// configuring a budget of 0.
int draken_cc_enabled(void);

// Pinned entry for (path, chunk_offset), or NULL. Every non-NULL result must be
// released with draken_cc_unpin. On a miss, *fill_ok says whether the caller
// should try to fill: 0 when `predicated` and this chunk already came out
// incomplete under a predicate decode (draken_cc_mark_incomplete) — a
// dictionary skip stops after the dictionary page every time, and refilling it
// would only allocate and discard.
const DrakenChunkEntry* draken_cc_lookup(const char* path, size_t path_len, int64_t chunk_offset,
                                         int predicated, int* fill_ok);

// Pinned entry for (path, chunk_offset), or NULL — a probe that counts a hit but
// never a miss. The IO planner asks this before it fetches (a hit skips the GET);
// a miss there is followed by the decoder's own draken_cc_lookup, which counts it.
const DrakenChunkEntry* draken_cc_lookup_hit(const char* path, size_t path_len,
                                             int64_t chunk_offset);

// The whole entry buffer: for parquet, the chunk IMAGE (decode_column.cpp
// ChunkPageSource::commit) — the chunk re-laid as an UNCOMPRESSED column chunk.
void draken_cc_entry_bytes(const DrakenChunkEntry* entry, const uint8_t** data, int64_t* len);

// Remember that a predicate decode of this chunk did not touch every page.
void draken_cc_mark_incomplete(const char* path, size_t path_len, int64_t chunk_offset);

// The bytes of the page indexed at `body_offset` (for parquet: the page HEADER's
// offset within the original chunk; the bytes are the page's rewritten header +
// body in the image). Returns 1 and sets data/len, or 0 if the entry has no such
// page.
int draken_cc_page(const DrakenChunkEntry* entry, int64_t body_offset,
                   const uint8_t** data, int64_t* len);

void draken_cc_unpin(const DrakenChunkEntry* entry);

// Fill path. The buffer comes from malloc (draken_cc_fill_alloc) and is NOT
// charged as query memory. draken_cc_insert takes ownership in every case:
// admitted, it becomes the entry; refused, it is freed. `page_index` is
// 3 * npages int64s: (offset in the caller's chunk, start_in_buffer, length) per page.
// `cost_ns` is the chunk's cost to rebuild: its measured decompression time plus,
// for a remote chunk, a modelled re-fetch. Per byte it sets how many extra CLOCK
// sweeps a hit buys (chunk_cache.cpp). A new chunk goes on probation, or straight
// to MAIN if it was evicted from probation recently. Returns 1 admitted, 0 refused.
uint8_t* draken_cc_fill_alloc(int64_t bytes);
void     draken_cc_fill_discard(uint8_t* buffer);
int      draken_cc_insert(const char* path, size_t path_len, int64_t chunk_offset,
                          uint8_t* buffer, int64_t buffer_bytes,
                          const int64_t* page_index, int32_t npages, int64_t cost_ns);

// As draken_cc_insert, for a filler that goes on reading the bytes (the ORC reader,
// docs/ORC_CHUNK_CACHE_DESIGN.md): admitted, returns the new entry PINNED (release with
// draken_cc_unpin; the bytes are the entry's now); refused — including when another reader
// filled the same key first — returns NULL and the buffer stays the caller's (free it with
// draken_cc_fill_discard).
const DrakenChunkEntry* draken_cc_insert_pinned(const char* path, size_t path_len, int64_t chunk_offset,
                                                uint8_t* buffer, int64_t buffer_bytes,
                                                const int64_t* page_index, int32_t npages, int64_t cost_ns);

// Drop every unpinned entry (pinned ones go as soon as their readers finish).
void draken_cc_flush(void);

// Counters (process lifetime).
typedef struct {
    int64_t bytes;          // currently held by live entries
    int64_t entries;
    int64_t hits;
    int64_t misses;
    int64_t inserts;
    int64_t refused;        // fills offered but not admitted
    int64_t evictions;      // entries dropped to make room for a fill
    int64_t give_way_bytes; // bytes released because query memory grew
    int64_t probation_bytes;  // currently held on probation
    int64_t promotions;       // probation -> MAIN (hit while on probation)
    int64_t remembered_hits;  // fills that skipped probation (recently evicted)
} DrakenChunkCacheStats;
DrakenChunkCacheStats draken_cc_stats(void);

#ifdef __cplusplus
}
#endif

#endif  // DRAKEN_CORE_CHUNK_CACHE_H
