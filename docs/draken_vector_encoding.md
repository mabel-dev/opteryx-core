# How Draken Stores Column Data

Draken is the columnar vector library at the heart of Opteryx's execution engine. Understanding how it represents data in memory explains a lot about why certain queries are fast, why memory use is lower than you might expect, and how the engine avoids redundant work.

---

## The naive approach and its cost

The simplest way to store a column of `N` rows is a flat array of `N` values. Simple, predictable, cache-friendly. It works well when every row has a distinct value — a column of random UUIDs, for example, genuinely needs `N` entries.

But most real analytical columns are not like that. A `country` column across a billion rows might contain fewer than 250 distinct values. A `status` column might have three. A boolean filter result might be `TRUE` for every row in a filtered chunk. Storing `N` copies of the same handful of values wastes memory, wastes cache, and wastes CPU time repeating identical work.

Draken avoids this with three encoding shapes, each a different answer to "how does row `i` map to a value?"

---

## Three shapes, one representation

All three shapes share the same underlying struct. There is no separate data type for each — the same fields describe all three, and the execution engine never needs to know in advance which shape it is reading.

### Dense

The default. Every row has its own value stored in sequence. Used when cardinality is genuinely high or when data has already been fully materialised.

### Dictionary

A high-cardinality column is split into two parts: a small table of unique values (the *dictionary*), and a compact array of integer codes — one per row — each pointing into that table. A billion-row column with 200 distinct strings stores 200 strings plus one billion small integers, rather than one billion full strings.

Predicate evaluation becomes even cheaper than the memory saving suggests. To test whether `country = 'AU'`, the engine checks each of the 200 dictionary entries once, builds a tiny lookup table of matches, then scans the billion codes. No string comparison per row — just a table lookup per code.

Parquet's RLE_DICTIONARY encoding on disk maps directly to this shape in memory. There is no expansion step at read time.

### Constant

When every row in a chunk has the same value — a literal in a SQL expression, a column with a single run of identical values, a scalar broadcast — Draken stores exactly one value and a row count. Reading "all N rows" costs the same as reading one.

Constants arise more often than expected. The right-hand side of `WHERE region = 'APAC'` is a constant. A Parquet page where every row in a row group shares one value becomes a constant when decoded. A `CASE` expression that always takes the same branch produces a constant result.

---

## What this means in practice

**Memory use scales with distinct values, not row count.** A 100-million-row dictionary column with 500 distinct values uses roughly the same memory as 500 values plus 100 million small integers. At 1-byte codes (up to 256 distinct values), that is around 100 MB for the codes and kilobytes for the dictionary — compared to gigabytes for the same data stored flat.

**Work scales with what is unique, not what is repeated.** When evaluating a predicate against a dictionary column, the engine works over the dictionary once. The result for each row is a table lookup. This is why queries filtering on low-cardinality string columns — status codes, categories, country names — are disproportionately fast.

**Constants are zero-copy.** Broadcasting a scalar to N rows costs nothing: the single stored value is read in place for every row. No allocation, no fill loop. This matters particularly for expressions involving literals and for columns that become uniform after an earlier filter step.

**Parquet round-trips cleanly.** Parquet uses dictionary encoding heavily on disk. Draken's dictionary shape is structurally identical, so encoded pages can be handed to the execution engine without decoding to a flat array first. The dictionary stays as a dictionary all the way through query execution.

---

## Where encoding shape comes from

Shape is decided at the scan boundary — the point where raw file bytes become Draken vectors — and stays fixed for the lifetime of that data in the engine.

Rugo (Opteryx's Parquet and JSONL reader) inspects each column chunk as it decodes:

- A chunk that is already dictionary-encoded on disk becomes a dictionary vector directly.
- A chunk where every row has the same value becomes a constant vector.
- Everything else decodes to a dense vector.

Run-length encoded pages are expanded at this boundary. RLE does not propagate into the execution engine; by the time data reaches an operator or expression, it is always one of the three shapes above.

Operators and expressions can also produce new vectors in any shape. A scalar subexpression produces a constant. A join that expands a column by repeating values may produce a dictionary with a selection vector of repeated codes.

---

## What the engine actually sees

From the execution engine's perspective, all three shapes look identical. A single struct describes the data, and reading the value at row `i` is the same operation regardless of shape: follow an optional indirection, read from the data buffer. Constant and dense are both "no indirection" — the difference is just how many entries the data buffer holds.

This means kernels are written once and handle all three shapes correctly. There is no combinatorial explosion of "dense × dense", "dict × constant", and so on for every operation. The shape-awareness happens at the struct level, not in operator code.
