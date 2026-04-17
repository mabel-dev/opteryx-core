Draft design: fast JSONL reader

We are building a faster JSONL reader. It will support single JSON documents, but it is explicitly optimized for files containing thousands to millions of structurally similar JSONL records.

This is not a general-purpose “parse everything into objects” reader. It is a query-aware scan operator designed to push projection and, later, selection into the JSONL read path.

Our target is to exceed the performance of PyArrow’s JSON reader on large JSONL workloads, with an initial goal of 2x throughput at 1M+ rows on representative datasets. We should also consider contributing results to JSON Bench or a similar benchmark suite.

Design principles
	•	Optimize for large scans, not tiny files.
	•	Prefer a small fixed setup cost if it reduces per-row work materially.
	•	Do not fully parse documents when the query only needs a subset of fields.
	•	Treat prediction as speculative: fast when correct, bounded-cost when wrong.
	•	Keep the hot path branch-light, allocation-light, and cache-friendly.
	•	Fall back to brute-force structural lookup when there is insufficient signal.

Reader model

The reader operates in two phases:
	1.	Document mapping
	•	Scan each JSON document and record the positions of structural elements.
	•	Identify object keys and the byte spans of their values.
	•	Classify value shape cheaply where possible.
	2.	Projected extraction
	•	For requested columns, use the document map plus key-position prediction to locate values quickly.
	•	Parse only the values required by the query.
	•	Do not materialize unused keys or intermediate objects.

This allows JSONL to be treated as a table source with projection pushdown, and later predicate pushdown.

Document map

For each row, we perform a SIMD-assisted preparse over the raw bytes.

We record the positions of structural characters:
	•	{
	•	}
	•	[
	•	]
	•	:
	•	,
	•	"
	•	\

The purpose of this stage is not to fully parse the document. It is to build a compact structural map that allows us to locate keys and values without rescanning the whole row repeatedly.

Conceptually:

struct FieldSpan {
    uint32_t key_start;      // inclusive, unquoted
    uint32_t key_end;        // exclusive
    uint32_t value_start;    // inclusive
    uint32_t value_end;      // exclusive when cheaply known
    ValueType type;          // string, number, boolean, null, object, array, unknown
    uint16_t ordinal;        // key position within the object
};

Notes:
	•	key_start / key_end should refer to the key contents, not the surrounding quotes.
	•	value_start / value_end should be raw byte spans in the source buffer.
	•	value_end may not always be known immediately for nested objects/arrays without additional traversal; this should be stated explicitly.
	•	ordinal is important because the predictor works on field position within the object, not byte offset.

Why this helps

Once the document map exists:
	•	key lengths are known before value parsing
	•	non-matching keys can be rejected cheaply by length before byte comparison
	•	value location is already known once the key is matched
	•	the reader can parse only the requested values
	•	strings, nulls, booleans, and many numbers can be classified without building objects

This makes repeated lookups such as doc["name"] much cheaper than reparsing the row or materializing the whole document.

Key index

Because JSONL datasets are typically produced programmatically, key order is often stable across rows. We exploit this by keeping a rolling history of the last 8 observed ordinals for each requested key.
	•	The predictor stores ordinals, not byte offsets.
	•	-1 means the key was not found in that row.
	•	Prediction is used only as a fast path.
	•	Brute-force lookup remains the correctness path.

Example histories:
	•	[5,5,5,5,5,5,5,5] → try position 5 first, then brute force
	•	[5,5,5,5,5,5,5,4] → try position 5 first, then brute force
	•	[5,5,5,5,5,5,4,4] → try position 5 first, then brute force
	•	[5,5,5,5,5,4,4,4] → try position 5 first, then try position 4, then brute force
	•	[5,5,5,5,4,4,4,4] → tie; try the most recent first, then the other, then brute force

Heuristics
	•	a position appearing 5 or more times is probed first
	•	a position appearing 3 or more times is eligible for probing
	•	on ties, probe the most recent first
	•	if no position appears at least 3 times, brute force immediately
	•	if repeated brute-force fallback shows the predictor has no signal, disable prediction for that key and continue with brute-force lookup only

Rationale

This design is optimized for large runs of similar rows:
	•	stable layouts converge to one-probe lookup
	•	shifting layouts degrade gracefully
	•	bimodal layouts still avoid full scans by probing a small candidate set
	•	high-entropy layouts pay a bounded tax, then revert to brute force

The intent is to minimize average per-row cost, not to eliminate transition cost entirely. That is consistent with Opteryx’s broader design philosophy.

Lookup flow

For a projected key:
	1.	consult predictor history for candidate ordinals
	2.	probe candidate ordinals in priority order
	3.	validate key length before byte comparison
	4.	if a candidate matches, return the associated value span
	5.	if no candidate matches, perform brute-force lookup over the document map
	6.	update predictor history with the observed ordinal or -1

This keeps the common path extremely cheap while preserving correctness.

Value parsing

Once a key has been located, value parsing should be separated from key lookup.

Fast paths should exist for:
	•	null
	•	true / false
	•	quoted strings without escapes
	•	integers
	•	simple floating-point numbers

Nested objects and arrays should initially be treated as spans unless the query explicitly requires deeper traversal.

This keeps projection pushdown cheap and avoids paying recursive parse cost for values the query will not inspect.

Scope

In scope
	•	JSONL / NDJSON files
	•	top-level object records
	•	projection pushdown
	•	repeated extraction of the same keys across many rows
	•	large files with similar row structure

Out of scope for the first version
	•	arbitrary JSON streams
	•	full object materialization
	•	generalized schema inference
	•	deep nested-path extraction as the primary optimization target
	•	best-in-class performance on single-document workloads

Performance goals

Primary goals
	•	push projection into the JSONL scan path so projected reads are faster than line-by-line orjson or simdjson
	•	at 1M+ rows on structurally similar JSONL data, exceed the performance of PyArrow’s JSON reader
	•	achieve approximately 2x throughput on representative projected-read workloads
	•	emit spans or typed values directly into Opteryx-native structures without intermediate object materialization

Secondary goals
	•	keep regression on small inputs bounded
	•	avoid materializing Python objects for unused fields
	•	support direct emission into Opteryx-native morsels or columns

Non-goals
	•	beating best-in-class parsers at full-document parsing
	•	being the fastest parser for arbitrary single JSON payloads

Benchmark plan

Benchmarks should be split by workload shape, not reported as a single number.

At minimum:
	•	small rows, few columns projected
	•	wide rows, few columns projected
	•	wide rows, many columns projected
	•	stable key ordering
	•	bimodal key ordering
	•	high-entropy ordering
	•	shallow values only
	•	nested values present but not projected

Competitors / baselines:
	•	PyArrow JSON reader
	•	line-by-line orjson
	•	line-by-line simdjson
	•	our reader with prediction disabled
	•	our reader with prediction enabled

Metrics:
	•	rows/sec
	•	MB/sec
	•	CPU time
	•	allocations
	•	projected-column extraction cost
	•	hit rate of ordinal prediction
	•	brute-force fallback rate

Positioning

The reader should be described as:

a fast, projection-aware JSONL scan operator for repeated extraction from large sets of structurally similar documents

That is a more accurate description than “a faster JSON parser,” and it better explains why it can beat general-purpose parsers in query workloads.
