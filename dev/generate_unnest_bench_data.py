#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generate `testdata/flat/unnest_bench` — the CROSS JOIN UNNEST benchmark corpus.

CROSS JOIN UNNEST does not expand, it EXPLODES: the row count leaving the
operator is the SUM of the array lengths, not the parent row count. The three
optimisations this corpus measures all exist to stop work happening on the far
side of that explosion:

  1. dead PARENT columns replicated across the fan-out then projected away
  2. a value filter on the unnested column applied after the fan-out
  3. a DISTINCT on the unnested column applied after the fan-out

So the corpus is shaped to make each of those visible and separable:

  * `tags` averages ~8 elements over 200,000 parents => ~1.6M rows out. Big
    enough that per-row costs dominate planning noise, small enough to run many
    interleaved rounds.
  * THREE dead parent columns (`payload_a`, `payload_b`, `label`) that a query
    selecting only the unnested tag never reads. `label` is deliberately a
    ~24-byte string: the wasted replication is bytes, not just cells, and a
    VARCHAR carries an arena the fan-out has to drag with it.
  * `csv_tags`, the same tags as one comma-delimited string. Splitting a
    delimited field is the classic computed-source unnest, and it is the shape
    where dead-parent-column replication actually bites: the source string is
    live BELOW the unnest (SPLIT reads it) and dead ABOVE it, so projection
    pushdown cannot prune it at the scan the way it prunes `label` out of a
    plain `UNNEST(tags)`. Measured on the untouched tree, that shape compiles to
    a 2-column unnest output whose second column is discarded by the very next
    operator.
  * a 500-value tag vocabulary, so DISTINCT collapses ~1.6M rows to 500 and a
    single-tag equality filter drops ~99.8% of them. Both reductions are large
    enough that folding them into the operator should show plainly.
  * the vocabulary is Zipf-ish, not uniform: real tag lists are, and a uniform
    distribution would give every dedup bucket the same depth and hide any
    hash-collision behaviour in a folded DISTINCT.
  * array lengths are irregular (including empty and NULL lists) because
    row-count semantics for those are draken's rule, and a corpus where every
    list is the same length makes a broken fan-out look correct.

PyArrow is the writer only — banned inside `opteryx/`, `draken/` and `rugo/`,
sanctioned in `dev/` for test-data generation (CLAUDE.md §4), as
`dev/generate_array_testdata.py` already does.

WARNING: run this in its own process. Importing pyarrow and opteryx into one
interpreter segfaults (OpenSSL 1.0.2k vs 3.x); this script imports neither
opteryx nor rugo.

Run from the repo root:  python dev/generate_unnest_bench_data.py
"""

from __future__ import annotations

import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = os.path.join("testdata", "flat", "unnest_bench")
OUT_FILE = os.path.join(OUT_DIR, "data.parquet")

PARENT_ROWS = 200_000
VOCAB_SIZE = 500
MEAN_LEN = 8
SEED = 20260812  # fixed: the corpus must be byte-identical between A and B arms


def build():
    rng = random.Random(SEED)

    # Zipf-ish vocabulary: index 0 is the most common tag, and the single-tag
    # filter in the benchmark targets a tag from the TAIL so it selects ~0.2%.
    vocab = [f"tag-{i:04d}-{'x' * (i % 7)}" for i in range(VOCAB_SIZE)]
    weights = [1.0 / (i + 1) for i in range(VOCAB_SIZE)]

    ids = []
    payload_a = []
    payload_b = []
    labels = []
    tags = []
    csv_tags = []

    total_elements = 0
    for row in range(PARENT_ROWS):
        ids.append(row)
        payload_a.append(rng.getrandbits(48))
        payload_b.append(rng.random() * 1000.0)
        # ~24 bytes: arena-resident, so replicating it across the fan-out costs
        # real bytes rather than just an inlined slot.
        labels.append(f"label-{row:08d}-padding")

        # Irregular lengths, including the two shapes with special row-count
        # semantics. 1 in 200 NULL, 1 in 200 empty, rest geometric around MEAN_LEN.
        draw = rng.random()
        if draw < 0.005:
            tags.append(None)
            csv_tags.append(None)
            continue
        if draw < 0.010:
            tags.append([])
            csv_tags.append("")
            continue
        length = 1 + int(rng.expovariate(1.0 / MEAN_LEN))
        picks = rng.choices(vocab, weights=weights, k=length)
        total_elements += length
        tags.append(picks)
        # The vocabulary contains no commas, so SPLIT(csv_tags, ',') reproduces
        # `tags` exactly — the two unnest routes are comparable by construction.
        csv_tags.append(",".join(picks))

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "payload_a": pa.array(payload_a, type=pa.int64()),
            "payload_b": pa.array(payload_b, type=pa.float64()),
            "label": pa.array(labels, type=pa.string()),
            "tags": pa.array(tags, type=pa.list_(pa.string())),
            "csv_tags": pa.array(csv_tags, type=pa.string()),
        }
    )

    os.makedirs(OUT_DIR, exist_ok=True)
    pq.write_table(table, OUT_FILE, compression="zstd", row_group_size=50_000)

    print(f"wrote {OUT_FILE}")
    print(f"  parent rows      : {PARENT_ROWS:,}")
    print(f"  unnested rows    : {total_elements:,}  ({total_elements / PARENT_ROWS:.2f}x fan-out)")
    print(f"  tag vocabulary   : {VOCAB_SIZE:,}")
    print(f"  file size        : {os.path.getsize(OUT_FILE):,} bytes")


if __name__ == "__main__":
    build()
