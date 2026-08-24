"""Generate the band-join PERFORMANCE fixtures.

Not test fixtures — `tests/sql/test_band_join_execution.py` uses the tiny
`testdata/band_join` tables, which ARE committed. These two are for measuring, are
regenerated rather than stored (12MB and 1.4MB), and are gitignored.

Both reproduce the shape from docs/BAND_JOIN_PROPOSAL.md: many flows, fewer lookups,
few clients, and — the part that matters — BOTH SIDES SPANNING THE SAME 24h WINDOW,
so range transport derives `[min(flow_start) - width, max(flow_start)]` and prunes
nothing. A fixture whose two sides span different windows measures the range
pushdown instead of the band join.

    band_scale  1.28M x 272k over 57 clients — the proposal's Pair 6 size, ~6.1bn
                pairs for the equi-join to form. The band arm runs in 0.37s; the
                hash-then-filter arm takes ~154s, so this is a HEADLINE fixture, not
                one to sweep parameters on.
    band_small  120k x 25k over 57 clients — sized so EVERY arm finishes at EVERY
                band width. Ratio questions (does a wide band ever lose?) belong
                here; band_scale cannot answer them in reasonable time.

Deterministically seeded, so a regenerated fixture reproduces published numbers.

    python dev/generate_band_fixtures.py
"""

import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 20260823
DAY_US = 24 * 3600 * 1_000_000
BASE_US = 1_700_000_000 * 1_000_000   # a fixed epoch; the absolute value is arbitrary

FIXTURES = {
    "band_scale": (1_280_000, 272_000, 57),
    "band_small": (120_000, 25_000, 57),
}

TIMESTAMP = pa.timestamp("us")
FLOW_SCHEMA = pa.schema([("client", pa.string()), ("flow_start", TIMESTAMP)])
LOOKUP_SCHEMA = pa.schema(
    [("client", pa.string()), ("event_time", TIMESTAMP), ("domain", pa.string())]
)


def build(root: str, n_flows: int, n_lookups: int, n_clients: int) -> None:
    random.seed(SEED)
    clients = ["192.168.4.%d" % (10 + i) for i in range(n_clients)]
    tables = {
        "flows": (
            {
                "client": [clients[random.randrange(n_clients)] for _ in range(n_flows)],
                "flow_start": [BASE_US + random.randrange(DAY_US) for _ in range(n_flows)],
            },
            FLOW_SCHEMA,
        ),
        "lookups": (
            {
                "client": [clients[random.randrange(n_clients)] for _ in range(n_lookups)],
                "event_time": [BASE_US + random.randrange(DAY_US) for _ in range(n_lookups)],
                # A string payload, as the live query carries `domain`. String
                # payloads are what make the build-side consolidation decision
                # (decide_consolidation) worth anything.
                "domain": [
                    "host%05d.example.com" % random.randrange(50_000)
                    for _ in range(n_lookups)
                ],
            },
            LOOKUP_SCHEMA,
        ),
    }
    for name, (data, schema) in tables.items():
        directory = os.path.join(root, name)
        os.makedirs(directory, exist_ok=True)
        pq.write_table(
            pa.table(data, schema=schema),
            os.path.join(directory, name + ".parquet"),
            compression="zstd",
            use_dictionary=True,
        )
        print("%s/%s: %d rows" % (os.path.basename(root), name, len(data["client"])))


if __name__ == "__main__":
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for fixture, (flows, lookups, clients) in FIXTURES.items():
        build(os.path.join(repo, "testdata", fixture), flows, lookups, clients)
