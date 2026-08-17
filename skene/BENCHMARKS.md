# Measured

Apple Silicon, shared dev machine. Reproduce before trusting anything here.

```
make bench    && ./build/bench_vs_parquet 1000000 3          # synthetic
make convert  && SKENE_ZSTD=1 ./build/convert_parquet <in.parquet> <prefix>
make probe    && ./build/probe_parquet <in.parquet>          # decode-path diagnosis
make bakeoff  && ./build/compression_bakeoff <raw.skene>...  # codec / kind / size
make fsst     && ./build/fsst_arena <file.skene>...          # FSST vs zstd on arenas
make slots    && ./build/slot_layout <file.skene>...         # slot array layout
sh bench/skenify.sh out.tsv <parquet>... && python3 bench/summarise.py L out.tsv
```

The design rejected Parquet partly on performance grounds. This is that claim
tested, and it **does not come out uniformly in skene's favour** — the losses are
recorded as prominently as the wins.

---

## TPC-H SF1 — every table

Source files are **ZSTD Parquet**, so this is skene against Parquet with the same
class of compressor on both sides. Default settings: per-section **zstd level 1**,
on the four section kinds that measurably compress, on bodies of 10 KB or more.

| table | rows | parquet-zstd | skene | vs parquet | write | read | pq read |
|---|---:|---:|---:|---|---:|---:|---:|
| lineitem | 6,001,215 | 198,816,957 | 192,765,281 | **0.97×** | 2421 ms | 379 ms | 760 ms |
| orders | 1,500,000 | 47,090,997 | 45,474,992 | **0.97×** | 465 ms | 96 ms | 158 ms |
| supplier | 10,000 | 623,933 | 647,154 | 1.04× | 4 ms | 1 ms | 2 ms |
| customer | 150,000 | 9,596,138 | 10,234,940 | 1.07× | 65 ms | 22 ms | 38 ms |
| partsupp | 800,000 | 31,251,782 | 34,112,523 | 1.09× | 360 ms | 86 ms | 116 ms |
| part | 200,000 | 5,070,908 | 6,000,895 | 1.18× | 51 ms | 14 ms | 22 ms |
| region | 5 | 985 | 1,162 | 1.18× | ~0 | ~0 | ~0 |
| nation | 25 | 2,025 | 3,640 | 1.80× | ~0 | ~0 | ~0 |
| **all** | **8,661,245** | **292,453,725** | **289,240,587** | **0.99×** | 3367 ms | **599 ms** | 1096 ms |

**Parity on size — 0.99× across the whole schema — and reads 1.83× faster than
Parquet.** The read number is conservative: rugo's Parquet reader is a threaded
pipeline, skene's is single-threaded.

The ratio tracks table size, and the two tiny tables are the tell: `nation` at
1.80× is 25 rows, where per-column footer, statistics and zone-map overhead has
nothing to amortise against. That overhead is fixed, so it vanishes by `supplier`
(10k rows, 1.04×) and turns favourable on the two tables anyone actually measures.

Raw, before compression, skene is 1.9–3.8× larger.

---

## ClickBench — 100 million rows

The full `hits` dataset: 100 files, 105 columns, 99,997,497 rows, 14.7 GB.

> **The source here is SNAPPY Parquet, not ZSTD.** Snappy is the weaker
> compressor, so this comparison *flatters* skene and is the softer of the two.
> TPC-H above, against ZSTD Parquet, is the harder test and the one to trust.

| | parquet (snappy) | skene | ratio |
|---|---:|---:|---|
| bytes | 14,737,666,736 | 14,379,832,326 | **0.98×** |

| | time | throughput |
|---|---:|---:|
| skene write | 107,939 ms | 127 MB/s |
| skene read | 28,420 ms | **483 MB/s** |
| parquet read (rugo, threaded) | 41,665 ms | 337 MB/s |

**1.47× faster to read**, again with skene single-threaded against a threaded
Parquet pipeline. Write throughput is *better* here than on TPC-H (127 vs 82
MB/s) because ClickBench is integer-heavy — 90 of its 105 columns arrive
dictionary-encoded — and value ordering over small integer domains is cheap.

### The spread is the interesting part

| ratio | files |
|---|---:|
| <0.90× | 22 |
| 0.90–1.00× | 18 |
| 1.00–1.10× | 34 |
| 1.10–1.50× | 24 |
| >1.50× | 2 |

| | file | source | skene |
|---|---|---:|---:|
| best | hits_54 | 277,305,733 | 190,416,645 (0.69×) |
| worst | hits_47 | 34,336,875 | 73,579,835 (2.14×) |

**The ratio is anti-correlated with source size, and that is not a coincidence.**
The two worst files are the two *smallest* sources — 34 MB where the median is
~150 MB — which means their data is the most repetitive. That is exactly where
Parquet's dictionary and RLE over a whole row group win outright, and exactly
where skene cannot follow: a German-string slot is 16 bytes per value whether the
value is unique or the same string a million times over.

It is the same structural cost the TPC-H section names, seen from the other end.
Compression hides it on ordinary data and cannot hide it on highly repetitive
data. Anyone choosing skene for a very low-cardinality table should measure that
table rather than trust the aggregate.

That gap is the whole story. TPC-H is comment-heavy: `ps_comment` averages ~124
characters, `l_comment` ~44, and both are near-unique. German-string slots are
16 bytes per value and the arena is stored verbatim, so a text column keeps
almost all its redundancy after bit packing and delta have run. Compression is
what closes it.

Write is the weak axis: 82 MB/s against a Parquet reader doing 255 MB/s on the
same data. Value ordering and per-section compression are both paid at write
time, by design — the format trades write cost for read speed, and this is the
bill.

lineitem write cost, 6M rows across 96 row groups:

| | bytes | write | read back |
|---|---:|---:|---:|
| raw | 357.9 MB | 2016 ms | 209 ms |
| per-section zstd-1 | 192.8 MB | 2384 ms | 376 ms |

18% more write time and 80% more read time to halve the file.

---

## Which posture, and why skene and parquet deliberately disagree

Measured 2026-08-11 on the TPC-H SF10 mirror. Three mirrors, one per codec,
swapped in and out of the live path and run interleaved — minimum of three
rounds each, all 22/22:

| posture | codec | size | local |
|---|---|---:|---:|
| — | none | 7.8 GiB | 5823.8 ms |
| `for_fast_reads()` | lz4 | 4.0 GiB | 6041.3 ms |
| `for_storage()` | zstd-7 | 2.7 GiB | 7153.0 ms |

LZ4 and uncompressed **overlap** across runs (6041–6232 against 5824–6250), so
locally they are not distinguishable — and LZ4 is half the size. The marginal
rates make the shape plain: `none → lz4` removes **18.8 MB per millisecond**
spent, `lz4 → zstd-7` removes **1.25**. LZ4 is the knee, which is why there is
no separate "performance" posture: `for_fast_reads()` already is one, and
uncompressed would buy a further 3.7% for double the bytes.

Remotely the ordering inverts. At the ~64 MB/s achieved on the 1 Gbps Cloud Run
link (125 MB/s theoretical), total time is none 137.2 s, lz4 73.7 s, **zstd-7
53.2 s**. LZ4 only overtakes zstd-7 above **~1.25 GB/s (10 Gbps)** — an order of
magnitude beyond the link — so deployed data takes `for_storage()`.

**So the codec a corpus uses states where it is READ, not what it holds.** The
benchmark mirrors (`make clickbench-skene`, `make tpch`) are read locally
off NVMe and use lz4; the parquet corpora are written in the storage posture
(zstd, per-type level, 95% keep floor) because they stand in for deployed data.

⛔ **That gap is intentional, and it means a skene number and a parquet number
from these suites are not a like-for-like format comparison** — they differ by
codec as well as by format, so comparing them measures the codec choice as much
as the format. Any quoted figure should name its posture.

## Which sections, which codec, what size floor

`compression_bakeoff` tries every vendored codec on every `PLAIN` section body of
a raw file, aggregated by kind and by size. Across 35 MB of TPC-H sections:

| section kind | count | plain | zstd-1 | zstd-3 | zstd-9 | lz4 | snappy |
|---|---:|---:|---|---|---|---|---|
| STRING_ARENA | 32 | 23,978,799 | **0.25×** | 0.24× | 0.21× | 0.39× | 0.36× |
| STRING_SLOTS | 36 | 10,563,584 | **0.43×** | 0.39× | 0.38× | 0.54× | 0.54× |
| VALIDITY | 64 | 407,168 | 0.00× | 0.00× | 0.00× | 0.01× | 0.05× |
| ZONE_MAP | 72 | 4,608 | 0.70× | 0.59× | 0.56× | 0.53× | 0.49× |
| DATA / SELECTION | 14 | 138 | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× |
| other (bloom) | 82 | 926 | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× |
| **ALL** | 300 | 34,955,223 | **0.30× / 58 ms** | 0.28× / 66 ms | 0.26× / 356 ms | 0.43× / 44 ms | 0.41× / 66 ms |

**`DATA` and `SELECTION` barely appear** — 138 bytes out of 35 MB. That is not
because they are incompressible; it is because bit packing and delta already
claimed them, so almost nothing reaches this stage as `PLAIN`. The encodings are
doing their job.

**zstd-1 is the choice.** It beats snappy on *both* axes (0.30× in 58 ms against
0.41× in 66 ms), so snappy is dominated outright. lz4 is 24% faster but 43%
worse, which does not buy its way into the format as a second codec. Higher zstd
levels are a size/time trade rather than a win:

| level | vs zstd-1, size | vs zstd-1, whole-table write |
|---|---|---|
| 3 | 0.2–7.8% smaller | ~6% slower |
| 9 | ~12% smaller | ~6× slower on the compress step alone |

Level 1 is the default because the instruction was the fastest option that
works, and it is within 3–4% of level 3 on every table with real volume. `part`
is the outlier at 7.8%; if stored-table size ever matters more than write
throughput, level 3 is a one-field change in `WriteOptions`.

### The 10 KB floor, and what it costs

| section size | count | plain bytes | best compressed | saving |
|---|---:|---:|---:|---|
| <1K | 192 | 9,020 | 4,568 | 49.4% |
| 1K–10K | 68 | 429,100 | 5,232 | 98.8% |
| 10K–100K | 16 | 248,000 | 13,792 | 94.4% |
| 100K–1M | 9 | 5,030,466 | 1,691,545 | 66.4% |
| >1M | 15 | 29,238,637 | 7,312,870 | 75.0% |

Sections under 10 KB are **87% of all sections and ~1.2% of the recoverable
bytes**. Skipping them removes 260 of 300 compression attempts.

The 1K–10K row is not noise, though — 98.8% is nearly all of it, and it is almost
entirely all-ones validity bitmaps sized `rows/8`, which land just under the
floor for a typical row group. **The answer was to stop writing them, not to
compress them:** an absent `VALIDITY` section already means all-valid, so a
redundant bitmap is bytes stating a fact the reader infers. Dropping them takes
8.3 KB off a raw `part` file with no write or read cost at all.

Gate, floor and bitmap drop together are **size-neutral** against compressing
everything ungated — within ±0.4% per table, a small net win on five of six —
while doing 87% less work.

### Per-section vs whole-file

Compressing the file as a single unit is smaller, but a reader cannot decompress
a slice — so reading one column would mean fetching and decompressing all of
them, destroying the property the layout exists for.

| table | per-section | whole-file | cost of independence |
|---|---:|---:|---|
| supplier | 647,154 | 630,314 | 2.7% |
| customer | 10,234,940 | 9,750,855 | 5.0% |
| part | 6,000,895 | 5,511,796 | 8.9% |
| partsupp | 34,112,523 | 32,165,411 | 6.1% |
| orders | 45,474,992 | 41,489,608 | 9.6% |
| lineitem | 192,765,281 | 176,797,780 | 9.0% |

**2.7–9.6% to keep every column a self-contained byte range.** Worth it — a
whole-file frame cannot be sliced, so the alternative is fetching and
decompressing every column to read one.

(The whole-file column is zstd-2 over the raw file and is unchanged; the
per-section column moved because it is now level 1 rather than level 3, which is
most of the widened gap.)

---

## Synthetic shapes

1M rows, best of 3, against the rugo Parquet writer on identical data.

| case | skene | parquet uncompressed | parquet zstd-2 | vs zstd |
|---|---:|---:|---:|---|
| low-cardinality int64 (50 distinct) | 751 KB | 8.0 MB | 961 KB | **0.78×** |
| timestamps (all distinct, ascending) | 1.4 MB | 8.0 MB | 2.1 MB | **0.66×** |
| random int64 (high cardinality) | 8.5 MB | 8.0 MB | 8.0 MB | 1.06× |
| low-cardinality varchar (20 distinct) | 627 KB | 20.5 MB | 1.5 MB | **0.42×** |
| high-cardinality varchar (near-unique) | 38.4 MB | 26.4 MB | 10.2 MB | 3.76× |
| mixed result table (4 columns) | 4.9 MB | 36.0 MB | 7.1 MB | **0.70×** |

| case | skene write | skene read | parquet zstd-2 write |
|---|---:|---:|---:|
| low-cardinality int64 | 6.6 ms | **1.5 ms** | 15.0 ms |
| timestamps | 11.2 ms | **2.1 ms** | 15.5 ms |
| random int64 | 110.6 ms | **6.9 ms** | 5.8 ms |
| low-cardinality varchar | 10.9 ms | **1.4 ms** | 32.2 ms |
| high-cardinality varchar | 6.2 ms | **2.6 ms** | 66.6 ms |
| mixed result table | 146.6 ms | **8.9 ms** | 74.8 ms |

**Reads are 1–9 ms for a million rows.** That is the design goal delivered: the
reader memcpys buffers and rebuilds two pointers, with no decode, no dictionary
reconstruction, no re-derivation of encoding shape.

Parquet read time is deliberately not compared. rugo's reader is a threaded
pipeline with its own I/O; timing it against a single-threaded in-memory read
would measure the harnesses rather than the formats.

### These numbers were misleading, and the correction matters

On this synthetic set, zstd applied to skene's output gained **nothing** on four
of five shapes (751,348 → 751,375 bytes on low-cardinality int64). I concluded
from that a general compressor "would be dead weight except on one column shape,
and is not worth carrying for it."

**That was wrong, and TPC-H showed it.** My generated columns were not
string-heavy enough to be representative. On real tables, compression is the
difference between 3× worse than Parquet and parity. The synthetic set is kept
here because it isolates individual encodings well — but it is not evidence about
whole-table size, and it should not be used as such again.

---

## FSST on the string arena

The arena is 24 MB of the 35 MB measured above, so it is the only place a
string-specific codec could matter. FSST (VLDB 2020, vendored under
`third_party/fsst`) builds a ~255-symbol table per block and compresses each
string against it — so unlike a zstd frame, **any single string decompresses on
its own**.

14 string columns, 342,216 strings, 11.8 MB of arena. Every string is
decompressed and compared before any size is reported.

| column | strings | plain | fsst | zstd-1 | fsst+z1 |
|---|---:|---:|---|---|---|
| c_name | 37,500 | 675,000 | 0.28× | **0.03×** | 0.18× |
| c_address | 33,854 | 899,431 | 0.96× | **0.75×** | 0.77× |
| c_phone | 37,500 | 562,500 | 0.46× | 0.43× | **0.42×** |
| c_comment | 37,500 | 2,713,621 | 0.31× | **0.24×** | 0.25× |
| p_name | 65,536 | 2,145,228 | 0.34× | **0.28×** | 0.29× |
| p_comment | 36,631 | 640,762 | 0.38× | **0.31×** | 0.32× |
| ps_comment | 13,568 | 1,671,511 | 0.29× | **0.23×** | 0.23× |
| o_clerk | 1,000 | 15,000 | 0.28× | **0.04×** | 0.17× |
| o_comment | 41,248 | 1,999,188 | 0.31× | **0.25×** | 0.26× |
| l_comment | 37,721 | 1,057,080 | 0.35× | **0.27×** | 0.29× |
| **all** | **342,216** | **11.8 MB** | **0.37×** | **0.29×** | **0.30×** |

| codec | bytes | ratio | compress | decompress | random access |
|---|---:|---|---:|---:|---|
| fsst, stored extent | 4,608,402 | 0.37× | 385 MB/s | **2710 MB/s** | **per string** |
| fsst, extent inferred | — | — | — | 483 MB/s | per string |
| zstd-1 | 3,541,555 | **0.29×** | **667 MB/s** | 1552 MB/s | whole section |
| fsst + zstd-1 | 3,752,905 | 0.30× | — | — | whole section |

**On disk alone, FSST loses.** 0.37× against 0.29× is 30% more bytes, at half
the compression throughput. A zstd frame spans megabytes and finds redundancy
across the whole arena; a 255-symbol table cannot compete on that ground, and it
is not meant to.

**Stacked, it very nearly does not cost anything: 0.30× against 0.29×, 6%.**
That is the number that matters, because it says FSST has *not* already spent
the redundancy zstd would have claimed. The 6% buys per-string decode at 2.1
GB/s — 65% faster than zstd, and addressable one value at a time.

Two columns show where it fails and why:

- **`c_address` at 0.96×** — near-random text with no repeated substrings for a
  symbol table to find. zstd still gets 0.75× from long-range matching.
- **`o_orderpriority` at 3.07×, `l_shipinstruct` at 2.06×** — a handful of long
  strings, where a ~2 KB symbol table dwarfs the payload. Any adoption needs a
  per-column size floor of its own, exactly like the 10 KB one above.

### What adopting it would actually cost

Not a build change. Two structural ones, so this is stopping here:

**A compressed length per slot — and it is not optional.**

A slot carries a *decoded* length and an arena offset. FSST's *stored* extent is
unrelated to the decoded length and cannot be derived from it, so a slot as it
stands today does not hold enough to read an FSST arena. There are three ways
out, and only one of them keeps the case alive:

| | extent | decode | notes |
|---|---|---:|---|
| dead `hash32` field | exact | **2710 MB/s** | 4 bytes, already zero, no growth |
| inferred, bounded `2n+7` | none | 483 MB/s | correct but **5.6× slower** |
| sorted side array | exact | 2710 MB/s | +1.37 MB, plus an indirection per read |

The inferred variant does work — all 342,216 strings decode byte-correct — but
it is a trap. `fsst_decompress` clamps its *writes* to the output size while its
final loop runs `while (posIn < lenIn)`, so it consumes every input byte it is
handed and returns a `posOut` past the true length. Correctness therefore rests
on an undocumented internal detail, the return value must be discarded, and the
cost is 5.6× — **483 MB/s, slower than zstd's whole-frame 1552 MB/s**. That
erases the only advantage FSST had.

The sorted side array is exact but costs 342,216 × 4 = 1.37 MB here, pushing
0.37× to 0.48×, and slots are not in arena order after dedup or value ordering
so it needs its own sorted index and an indirection on every read.

That leaves `hash32`: dead since E37 removed the equality fast-reject, always
zero, and exactly 4 bytes wide. **Adopting FSST means repurposing it** — there is
no version of this that works without touching the slot, so that is the decision,
not an optimisation on top of it.

Worth noting what stays free either way: `length` remains the decoded length and
`prefix` remains the first four bytes uncompressed, so `str_length()` and every
prefix comparison keep working with **no decode at all**. Only full content
access pays.

**`str_data()` stops being a pointer.** Today it returns a pointer straight into
the arena at zero cost. Against an FSST arena it must decode into scratch first.
That is the real prize — the arena could stay compressed **in memory** at 0.37×
rather than only on disk — and it is also the real cost, because it changes the
zero-copy string contract that every string kernel in draken is written against.

The measurement says the idea is sound. Whether skene wants a compressed
in-memory arena is a design decision, and nothing has been implemented.

---

## The slot array is laid out badly for a compressor

`STRING_SLOTS` is 10.5 MB of the 35 MB and reaches only **0.43×**, against the
arena's 0.25×. A slot is more structured than text, not less, so that gap is a
symptom rather than a fact of life.

A 16-byte long slot is four u32 fields with nothing in common:

| field | shape |
|---|---|
| `length` | small, low entropy, often near-constant within a column |
| `prefix` | first 4 bytes of the string, big-endian — text-like |
| `hash32` | **dead** — always zero since E37 removed the equality fast-reject |
| `arena_offset` | monotonic in arena order |

Interleaved, the distribution changes every 4 bytes, which is close to the worst
input a general compressor can be handed. Four layouts, same zstd-1, 378,487
slots from real TPC-H columns:

| layout | zstd-1 | ratio | vs as-is |
|---|---:|---|---|
| as-is | 2,309,684 | 0.38× | — |
| stripped (drop dead `hash32`) | 2,187,850 | 0.36× | −5.3% |
| planed (fields separated) | 2,222,326 | 0.37× | −3.8% |
| **planed + delta on offsets** | **1,344,219** | **0.22×** | **−41.8%** |

**Neither half does much alone; together they are worth 42%.** Separating the
planes is what lets delta see the offsets at all, and the offsets are where the
structure is: strings are appended in slot order, so successive offsets differ by
the previous string's length — small, repetitive numbers that zstd cannot find
while they are interleaved with text-like prefixes.

Some columns collapse almost entirely:

| column | as-is | planed+delta |
|---|---:|---:|
| c_name | 43,822 | **77** |
| o_clerk | 1,195 | **37** |
| c_phone | 173,943 | 54,275 |
| p_name | 374,585 | 211,694 |
| l_comment | 290,526 | 178,442 |

Scaled to the whole measurement, slots contribute ~4.5 MB of the ~10.5 MB a
compressed file holds, so a 42% cut is **~18% off the total compressed output** —
substantially more than FSST offered, with no new dependency, no ABI change, and
no new codec. Delta and bit packing are already in the format; this is a matter
of handing them a plane instead of an interleaved struct.

Two things to settle before building it. The layout of a **required** section
changes, so it is a version bump, not a free addition. And delta pays here
because the arena is written in slot order — where that does not hold the deltas
are simply wide, the encoder declines by its existing size test, and the result
falls back to planed-only. It degrades to −3.8%, never to wrong.

---

## What each index structure actually prunes

Blooms are ~4-11% of a file, so "does it earn that" is a real question. Probes are
real values taken from ANOTHER row group of the same table — a predicate value
that exists in the table but may not be in this file — with ground truth from
scanning the file.

| file | type | stats | zone | bloom | **bloom-only** | nothing |
|---|---|---:|---:|---:|---:|---:|
| lineitem | STRING | 0.0% | 0.0% | 95.3% | **95.3%** | 4.7% |
| lineitem | NUMERIC | 35.8% | 35.8% | 95.3% | **61.3%** | 2.9% |
| orders | STRING | 0.0% | 0.0% | 95.1% | **95.1%** | 4.9% |
| orders | NUMERIC | 37.8% | 37.8% | 95.3% | **59.2%** | 3.0% |
| customer | NUMERIC | 50.9% | 50.9% | 95.4% | **46.7%** | 2.4% |
| part | NUMERIC | 57.2% | 57.2% | 95.2% | **40.5%** | 2.2% |
| ClickBench | STRING | 0.6% | 0.6% | 97.4% | **96.9%** | 2.6% |
| ClickBench | NUMERIC | 12.0% | 12.0% | 95.7% | **83.9%** | 4.0% |

**On strings, statistics and zone maps reject nothing at all.** String ordinals
truncate — draken packs leading bytes into an int64 — so nearly every string sits
inside the column's range and min/max cannot help. The bloom does 100% of the
work.

**Numerics are not covered by the cheaper structures either.** Statistics reach
only 12-57%, leaving the bloom to uniquely reject 40-84%. Restricting blooms to
string columns would give up most numeric equality pruning.

Bloom rejection lands at 95.1-97.4% against a configured 5% rate, on real
columns — the calibration holds outside the synthetic sweep.

### Zone maps currently prune nothing over the footer's own min/max

Read the `stats` and `zone` columns again: 35.8/35.8, 37.8/37.8, 50.9/50.9,
57.2/57.2, 12.0/12.0, 0.0/0.0. **Identical in every case.** Every value a zone map
rejects, the file-level statistics already rejected.

This is the clustering caveat, measured. Neither corpus is clustered — values are
scattered across rows, so every 8k chunk spans nearly the column's whole range and
any in-range value lands inside some chunk. Zone maps pay only when values vary
slowly across rows.

Two things stop this being a verdict. **Only equality was measured**, and range
predicates are what BRIN is actually for — a range probe against a chunk span is a
different question. And clustered data (time-series ingest, anything sorted on
write) is exactly the shape this corpus lacks. At 0.09-0.14% of a file they are
cheap insurance, but they are not yet shown to earn it.

---

## Where the write time went

Value ordering was ~95% of the write cost, and did not scale with cardinality:
50 distinct values cost the same as a million (98.4 vs 96.9 ms), because a dense
input sorted all N slots before deduplicating.

**Hash-based dedup when cardinality is low** — O(N) hashing plus an O(D log D)
sort over just the distinct values.

**But hashing is wrong at high cardinality.** Hashing a million DISTINCT values
costs ~200 ms of hashtable inserts where sorting them costs ~10 ms — a naive
switch made the timestamp case 20× WORSE. A 4096-row stride sample now picks per
column. The estimate can only choose a slower strategy, never a wrong answer:
both paths emit identical bytes.

**Near-unique columns are not ordered at all.** With no duplicates to remove, a
dense column would gain a full permutation for nothing — 220 ms and a LARGER
file. Delta-capable types are exempt, because ordering is what makes an ascending
timestamp column delta-encodable (8 MB → 1.4 MB); a rule counting only values and
codes would reject the case that pays most.

| case | before | after |
|---|---:|---:|
| low-cardinality int64 | 100.4 ms | **6.6 ms** |
| low-cardinality varchar | 220.5 ms | **10.9 ms** |
| high-cardinality varchar | 222.6 ms | **6.2 ms** |
| mixed result table | 265.5 ms | **146.6 ms** |
| timestamps | 9.7 ms | 11.2 ms |
| random int64 | 97.7 ms | 110.6 ms |

---

## Still unresolved

**Random high-cardinality int64 costs 110 ms and a 6% LARGER file.** It is
delta-capable, so it escapes the near-unique rule and gets ordered; the deltas
are then too wide for delta to pay, leaving a permutation added for nothing.
Catching it needs the sorted max-delta, which means sorting first and discarding
the work.

**The 16-byte German-string slot is the structural cost.** It is what makes the
read fast, and it is why raw skene is 3× Parquet on text-heavy tables.
Compression hides it; nothing removes it.

**Compression is not optional for stored data.** Off, skene is not competitive on
real tables. Level 1 leaves 0.2–7.8% on the table versus level 3; that is a
deliberate choice of write throughput over stored size, and it is one field to
reverse. That reverses the earlier reading of the synthetic numbers, and it
means the spill profile (raw, uncompressed) and the storage profile (compressed)
are genuinely different postures rather than one being a tuning of the other.
