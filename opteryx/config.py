# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import os as _os
import typing
from os import environ
from typing import Optional


def get(key: str, default: Optional[typing.Any] = None) -> Optional[typing.Any]:
    """
    Retrieve a configuration value.

    Parameters:
        key (str): The key to look up.
        default (Optional[Any]): The default value if the key is not found.

    Returns:
        Optional[Any]: The configuration value.
    """
    return environ.get(key, default=default)


_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


def get_bool(key: str, default: bool) -> bool:
    """
    Retrieve a boolean configuration value from the environment.

    `bool()` of any non-empty string is True, so `FLAG=false` and `FLAG=0` must not be
    parsed that way. An unset variable takes the default; anything else is parsed
    case-insensitively and an unrecognised value raises rather than being silently
    misread as its opposite.

    Parameters:
        key (str): The environment variable to look up.
        default (bool): The value used when the variable is unset.

    Returns:
        bool: The configuration value.
    """
    value = environ.get(key)
    if value is None:
        return default
    text = value.strip().lower()
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    raise ValueError(
        f"Invalid boolean for `{key}`: {value!r}. Expected one of "
        f"{sorted(_TRUTHY)} or {sorted(_FALSY)}."
    )


def parse_json(value: typing.Any, default: typing.Any = None) -> typing.Any:
    """
    Parse a JSON value from text or return the object when already structured.
    """
    if value is None:
        return default
    if isinstance(value, (dict, list, tuple)):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

DISABLE_OPTIMIZER: bool = get_bool("DISABLE_OPTIMIZER", False)
"""**DANGEROUS** This will cause most queries to fail."""

OPTERYX_DEBUG: bool = get_bool("OPTERYX_DEBUG", False)
"""**DANGEROUS** Diagnostic and debug mode - generates a lot of log entries."""

MATCH_THRESHOLD: float = float(get("MATCH_THRESHOLD", 0.5))
"""Default cosine similarity at or above which `MATCH (col) AGAINST (str)` is true.

Meaningful only relative to the ACTIVE EMBED capability — the score distributions of two
embedders are not comparable. Under the core static-hash EMBED (lexical, not semantic)
scores are bimodal at 1.0 and ~0, so any value in (0.3, 1.0] makes MATCH a case-insensitive
exact match; under a semantic capability (MiniLM) 0.5 separates related from unrelated
text. Tune per embedder with `SET match_threshold`.
"""

WRITE_COALESCE_ROWS: int = int(get("WRITE_COALESCE_ROWS", 65536))
"""Row-count target the INSERT/CTAS/RMV/OPTIMIZE sink (DataFileStream)
coalesces arriving morsels up to before writing each parquet ROW GROUP of the
open data file. One batch is one row group, so this IS the written row-group
size; the writer then groups row groups into column-major blocks of 4
(docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md). Clamped in the sink to rugo's
`DEFAULT_ROWS_PER_ROW_GROUP` (65536, the engine's measured best morsel size),
so a SET past the ceiling gets the ceiling. Tune per workload with
`SET write_coalesce_rows`."""

LIKE_SELECTIVITY_DECAY: float = float(get("LIKE_SELECTIVITY_DECAY", 0.7))
"""Geometric decay applied per-position when estimating infix `LIKE '%needle%'`
selectivity from a column's char-class statistics (see
opteryx.planner.cost_estimation.selectivity._decayed_char_class_selectivity).
0.7 is the value validated offline against 371K real NVD VARCHAR rows (see
scratch/like_selectivity's report) — lower values discount later needle
characters faster, blunting how far a long needle can drive the estimate
toward zero. Tune per deployment with `SET like_selectivity_decay`."""

DISABLE_RUNTIME_MINMAX_JOIN_FILTER: bool = get_bool(
    "DISABLE_RUNTIME_MINMAX_JOIN_FILTER", False
)
"""Turn OFF runtime min/max join filters — the observed ordinal range of a
join's build keys, handed to the probe-side scan's row-group zone map so row
groups that provably hold no matching row are never read
(docs/RUNTIME_MINMAX_FILTER_DESIGN.md).

Named for the state the caller probably does not want, per variables.py's
convention for feature flags, and mirrored as the session variable
`disable_runtime_minmax_join_filter` so it can be flipped per query.

The filter is pure pruning: disabling it can only make a query read MORE, never
return a different answer. That is exactly what the switch is for — the oracle
gate runs every eligible shape both ways and asserts the results are identical
(tests/integration/test_runtime_minmax_join_filter.py) — and it is also how to
attribute a measurement, since the plan-time correlated filter pushes range
predicates onto the same scans."""

VALIDATE_OPTIMIZER_PLANS: bool = get_bool("VALIDATE_OPTIMIZER_PLANS", False)
"""Debug guardrail: when set, the optimizer checks plan structural invariants
after every strategy and raises (naming the offending strategy) on corruption.
Off by default — adds per-strategy validation cost only when enabled."""

OPTERYX_TRACE: bool = bool(get("OPTERYX_TRACE", "").lower() in ("1", "true", "yes"))
"""Arm the native execution-trace span waterfall for the span of one query
(docs/EXECUTION_TRACING_DESIGN.md). When true, IO/operator spans are recorded
natively and retrievable via :func:`~opteryx.query_session.Session.trace`.
Truncation (per-thread span arena capacity) is controlled by
``OPTERYX_TRACE_ARENA_SPANS``, not a sampling rate — see
draken/core/trace.hpp's trace_arena_capacity()."""
OPTERYX_INSTRUMENT_ENGINE: bool = str(
    get("OPTERYX_INSTRUMENT_ENGINE", "0")
).lower() in (
    "1",
    "true",
    "yes",
)
"""WP-INSTR diagnostic: arm the native execution-engine instrumentation for the
duration of each native run. When enabled, ``execute_native`` measures the
wall-clock nanoseconds spent inside the known execution-time ``with gil`` bodies
(the scan-pull trampoline and the carrier-flip error stash) and records which
worker thread entered which GIL site, surfacing them on the query telemetry as
``gil_held_ns`` and ``worker_gil_sites``. Off by default — the instrumented sites
read a single C flag and pay ~0 when disabled. NOT concurrency-safe across
simultaneous queries in one process (module-global accumulators); it is a
diagnostic, not a production counter."""

OPTERYX_DISABLE_GC_DURING_QUERY: bool = str(
    get("OPTERYX_DISABLE_GC_DURING_QUERY", "0")
).lower() in (
    "1",
    "true",
    "yes",
)
"""Disable Python cyclic GC while tabular query results are being consumed.

Diagnostic setting for stall analysis. When enabled, GC is disabled when query
execution starts and restored when result iteration completes.
"""
MAX_CONSECUTIVE_CACHE_FAILURES: int = int(get("MAX_CONSECUTIVE_CACHE_FAILURES", 10))
"""Maximum number of consecutive cache failures before disabling cache usage."""

MAX_RECURSION_ITERATIONS: int = int(get("MAX_RECURSION_ITERATIONS", 1000))
"""Iteration ceiling for a WITH RECURSIVE fixpoint (docs/RECURSIVE_CTE_DESIGN.md).
A recursive term that has not converged by this many passes fails loud — never a
truncated result. A cycle under UNION ALL recurs forever by definition; this is
what stops it."""

KVSTORE_LOCATION: str = str(get("KVSTORE_LOCATION", "")).strip()
"""Single-store KV location (e.g. file://, valkey://, gs://, memory://)."""

KVSTORE_KEY_PREFIX: str = str(get("KVSTORE_KEY_PREFIX", "")).strip()
"""Optional global key prefix applied to configured KV stores."""

KVSTORE_LAYERS: list[typing.Any] = parse_json(get("KVSTORE_LAYERS", ""), default=[])
"""Optional layered KV definition (JSON list/dict) used by `create_kv_store(None)`."""

KVSTORE_PREWARM_MEMORY_POOLS: bool = str(get("KVSTORE_PREWARM_MEMORY_POOLS", "1")).lower() in (
    "1",
    "true",
    "yes",
)
"""Pre-create global memory:// pools from configured KV layers at startup."""

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = get("GCP_PROJECT_ID")

_parquet_local_io_workers_raw = str(get("PARQUET_LOCAL_IO_WORKERS", "auto")).strip().lower()
if _parquet_local_io_workers_raw not in ("auto", "") and not _parquet_local_io_workers_raw.lstrip(
    "-"
).isdigit():
    raise ValueError(
        f"Invalid PARQUET_LOCAL_IO_WORKERS: {_parquet_local_io_workers_raw!r}. "
        "Expected a positive integer or 'auto'."
    )
PARQUET_LOCAL_IO_WORKERS: int = (
    int(_parquet_local_io_workers_raw)
    if _parquet_local_io_workers_raw.lstrip("-").isdigit()
    else 0
)
"""Worker threads for local-filesystem Parquet reads (mmap path, IO is near-free from OS cache).

**Softcoded by default**: unset / "auto" / an impossible value (0 or less) is stored as
0 here, and `resolve_parquet_local_io_workers` derives the effective width at CALL time
as the larger of `cpu - 2` and 80% of the host, overlapping the execution width. **An explicit positive integer is HONOURED
EXACTLY.**

Override via the env var to tune per deployment, or SET `parquet_local_io_workers` per
query; either is taken as-is."""


def resolve_parquet_local_io_workers(requested: Optional[int] = None) -> int:
    """Effective local-Parquet IO width. Unset/"auto"/<=0 is softcoded as
    ``max(4, cpu - 2, floor(0.8 * cpu))``; an explicit positive request is HONOURED EXACTLY.

    Same derivation as the execution width (`resolve_max_execution_workers`), and the two
    OVERLAP by design (architect ruling 2026-09-23). The reserve is two cores, shrunk to
    20% of the host below 10 vCPU where two cores would be too large a share (architect
    ruling 2026-09-25: plain 80% measured ~7% slower on the 16-vCPU wrenchy-bench box,
    parallel utilisation 11.6 -> 10.7 cores at unchanged CPU-seconds). There is no cap.

    The floor is 4 (architect, 2026-08-28), NOT the historic 8: that floor previously held
    a <=8 vCPU host at 8 IO threads while execution collapsed to 1 — the exact
    oversubscription measured to cost 4.8x on a 2-core box. It is wider than the
    execution floor of 2 because these threads block on the page cache rather than
    burning CPU.

    ⛔ With engine-pool sharing disabled (`pool=None` at the native scan sites) this is
    the scan's own DECODE pool width, not just IO — decompress + materialize run here.
    """
    if requested is None:
        requested = PARQUET_LOCAL_IO_WORKERS
    requested = int(requested)
    if requested <= 0:
        cpu = _os.cpu_count() or 1
        return max(4, cpu - 2, (cpu * 4) // 5)
    return requested


PARQUET_GCS_IO_WORKERS: int = int(get("PARQUET_GCS_IO_WORKERS", 16))
"""Worker threads for GCS/HTTP Parquet reads.

This USED to default to 128, on the reasoning that "each range read pays network
RTT, so high concurrency wins". That reasoning is WRONG for this workload and the
default was measured to be the worst value in its own range.

Production sweep (2026-07-24, `SELECT COUNT(WatchID) FROM benchmarks.clickbench.hits`
— 99 files, 396 row groups, 792 MB, 3 runs/point, identical bytes throughout):

    workers   median   avg dl/row group   throughput
        128   18.66s          4507 ms      42.4 MB/s   <- the old default
         64   16.33s          2188 ms      48.5 MB/s
         32   16.29s          1178 ms      48.6 MB/s
         20   14.67s           761 ms      54.0 MB/s
         16   12.52s           631 ms      63.3 MB/s   <- optimum
          8   13.20s           438 ms      60.0 MB/s

An INTERIOR optimum: below it you are submission-limited against 396 row groups,
above it the extra streams do not add bandwidth — they divide the SAME aggregate
into thinner, more contended slices. Aggregate never exceeded ~64 MB/s at ANY
setting, which looks like the instance's network ceiling rather than anything the
engine controls; more concurrency cannot buy past it, so it only costs.

Caveats before treating 16 as universal. It was measured on ONE query shape
(single narrow int64 column, download-bound — decode stayed at 2-3ms/row group
throughout, so decode parallelism never mattered), and in a window where ~63 MB/s
was reachable; a later session saw the same config yield only ~51 MB/s, and
run-to-run noise of +/-13% has been observed. Re-baseline IN-SESSION before
trusting a small difference. Larger row groups, wider projections, or a bigger
instance could all move the optimum — override via the env var per deployment."""

# HTTP client tuning for remote (GCS/HTTP) Parquet range reads — mirrors the C++
# defaults baked into src/cpp/http_client.cpp so the Python-side code default and
# the native fallback (when no query resolves a SET override) never disagree.
HTTP_MAX_CONNECTIONS_PER_HOST: int = int(get("OPTERYX_HTTP_MAX_HOST_CONNECTIONS", 3))
"""Per-host concurrent-connection cap for get_many() batches. See http_client.cpp's
http_max_host_connections_env() for the empirical justification of the default."""

HTTP_MAX_RETRIES: int = int(get("OPTERYX_HTTP_MAX_RETRIES", 2))
"""Retry budget for transient HTTP/transport failures (5xx, 429, connect/timeout/recv errors)."""

HTTP_MIN_BANDWIDTH_MBPS: float = float(get("OPTERYX_HTTP_MIN_BW_MBPS", 20.0))
"""Assumed floor stream bandwidth (Mbps), used to derive a per-request timeout from the
Range span so a stalled small request times out promptly rather than waiting the full
client timeout."""

HTTP_REQUEST_TIMEOUT_FLOOR_MS: int = int(get("OPTERYX_HTTP_TIMEOUT_FLOOR_MS", 10000))
"""Minimum per-request timeout (ms), regardless of how small the Range span is."""

DISABLE_HTTP_MULTIPLEXING: bool = get_bool("OPTERYX_HTTP_DISABLE_MULTIPLEXING", False)
"""Turn OFF HTTP/2 multiplexing (CURLOPT_PIPEWAIT) for get_many() batches.

Default False — multiplexing ON. Without PIPEWAIT libcurl opens a connection per
range instead of carrying them all on one h2 connection; measured on production
GCS, forcing a single connection was 9.0% faster at 8 columns and 11.5% at 20,
with throughput flat across range counts where a wide cap degraded. See
HttpTuning::use_multiplexing in src/cpp/http_client.hpp for the full numbers.
Set True only to A/B against the old behaviour."""

HTTP_PIPEWAIT: bool = get_bool("OPTERYX_HTTP_PIPEWAIT", False)
"""Enable CURLOPT_PIPEWAIT on get_many() handles.

Default False = historical behaviour. INDEPENDENT of DISABLE_HTTP_MULTIPLEXING:
libcurl already defaults CURLMOPT_PIPELINING to multiplex, so multiplexing was
always on; PIPEWAIT additionally makes each batch WAIT for the first connection
to negotiate h2 before the rest proceed. Because get_many() creates a fresh CURLM
per batch with no CURLOPT_SHARE, connections are never reused across batches, so
PIPEWAIT pays a serialised handshake on every row-group fetch instead of letting
handshakes overlap. Opt-in until measured to be a net win."""

DISABLE_HTTP2: bool = get_bool("OPTERYX_HTTP_DISABLE_HTTP2", False)

PARQUET_IO_COALESCE_WASTE_RATIO: float = float(get("PARQUET_IO_COALESCE_WASTE_RATIO", 0.10))
"""Merge a row group's adjacent column-chunk range GETs while the bytes THROWN
AWAY stay within this fraction of the bytes actually needed.

Parquet stores a row group's column chunks contiguously, so a wide projection is
one unbroken extent that was previously issued as N separate range GETs — 105
columns of ClickBench `hits` = 105 requests per row group, where merging all of
them wastes 0.000%% of the bytes. 0.0 = merge only exactly-touching chunks
(byte-neutral). Sparse projections self-limit: skipping a fat column you did not
select blows the budget and splits the run."""

PARQUET_IO_COALESCE_MAX_BYTES: int = int(get("PARQUET_IO_COALESCE_MAX_BYTES", 8 * 1024 * 1024))
"""Ceiling on a single coalesced range GET; 0 = unbounded. Default 8 MB.

Merging is not free past a point: one huge GET serialises what were concurrent
transfers. Measured against GCS, 1x16MB took 1.05s where 8x2MB took 0.79s for
identical bytes — so this bounds how far a run may merge.

8 MB is the measured safe point. Local bench (8 files x 4 row groups x 100
columns, 1.95 GB) against dev/throttle_server.py's PER-CONNECTION bandwidth
cap — the regime most hostile to merging, since fewer connections means less
aggregate throughput there:
    no coalescing   7.76s   3200 requests
    8 MB cap        7.35s    256 requests   <- fewer requests AND faster
    4 MB cap        7.51s    512 requests
    unbounded      16.49s     32 requests   <- collapses concurrency
Bytes fetched were byte-identical across every row (1,947,017,170), because a
row group's column chunks are contiguous — merging them wastes nothing.
Unbounded wins on an UNCAPPED link (0.60s vs 0.90s) but is 2x worse here, so
the default is bounded until production tells us which regime it is in."""

SKENE_IO_COALESCE_WASTE_RATIO: float = float(get("SKENE_IO_COALESCE_WASTE_RATIO", 0.10))
"""Skene's own range coalescer (design R12, docs/SKENE_V3_FORMAT_DESIGN.md): merge
a v3 scan's planned ranges while the bytes THROWN AWAY stay within this fraction of
the bytes needed.

A v3 file is column-major, so one column over one block of row groups is already
ONE range; this decides whether ranges of DIFFERENT columns (separated by a
directory block or an unread column) or of row groups split by pruning are merged
too. The same rule as the parquet coalescer, with its own knob because the two
formats' layouts leave different gaps. 0.0 = merge only ranges that touch
(byte-neutral). Default: parquet's measured value."""

SKENE_IO_COALESCE_MAX_BYTES: int = int(get("SKENE_IO_COALESCE_MAX_BYTES", 8 * 1024 * 1024))
"""Ceiling on one merged skene range; 0 = unbounded. Default: parquet's measured
8 MB — one huge request serialises what were concurrent transfers."""

PARQUET_IO_IN_FLIGHT_LIMIT: int = int(get("PARQUET_IO_IN_FLIGHT_LIMIT", 0))
"""ABSOLUTE cap on row groups submitted but not yet consumed. 0 = auto
(`workers + 2`, the historical formula).

Deliberately absolute, not a delta: the whole point is to test "many threads,
SHALLOW window", which as a delta requires a NEGATIVE headroom — and that was
exactly the case that silently failed to apply (production cell: workers=128,
headroom=-110 still reported peak concurrency 134, i.e. it fell back to +2).

Worker count currently governs two separate things — how many row groups download
concurrently (threads) and how deep the submission window runs ahead of the
consumer. The 128→16 worker sweep could not tell which of those produced the 33%
win because they move together; this splits them. It also sizes the IO pool
(`est_rg * (in_flight_limit + 1)`), so raising it costs memory linearly."""

PARQUET_IO_FETCH_AHEAD: int = int(get("PARQUET_IO_FETCH_AHEAD", 64))
"""Remote fetch-ahead depth: a dedicated pool of N threads that ONLY issue the
row-group range GETs, so requests in flight are decoupled from the decode thread
count. 0 = off = the coupled path, byte-for-byte. Default 64 (architect, 2026-09-13,
from the production sweep: 48-64 was the optimum, 128 and 256 ran SLOWER than off;
the earlier 128 default from the 2026-09-12 live trial was a regression).
Only remote scans arm it; a local-only scan never starts the pool.

Why: `ParquetIOPipeline::decode_row_group` fetches AND decodes on one thread, so
concurrent fetches == `parquet_gcs_io_workers`, by construction — the submission
window (`parquet_io_in_flight_limit`) never bound. Dev rig, dev/throttle_server.py
at rtt=50ms / 100 Mbps per connection, 240 row groups, 79 MB, A/A floor 0.9996:

    workers=4, window 6 -> 64 (coupled)        4.70s -> 4.45s   (window: inert)
    workers=4 -> 32 (coupled)                  4.70s -> 1.31s   (threads move it)
    workers=4, fetch_ahead=32, window=48       0.74s            (5.5x vs coupled)
    workers=4, fetch_ahead=4                   5.42s            (SLOWER — rejected)

That is the LATENCY regime. Production Cloud Run -> GCS is bandwidth-capped at
~64 MB/s (`parquet_gcs_io_workers` docstring), where more requests in flight buy
much less; the 64 default came from the production sweep, not from this rig.

Rules enforced at plan time (ValueError, never silently inert): the depth must
EXCEED `parquet_gcs_io_workers`, and an explicit `parquet_io_in_flight_limit`
must not be smaller than the depth. With the window on auto it widens to
`max(workers, fetch_ahead) + 2`. Memory: each in-flight row group holds its
COMPRESSED bytes from fetch until decode, on top of the decode pool reservation.
The pipeline reports the depth it actually runs as `fetch_ahead_depth` and the
bytes a cancel threw away as `prefetch_discarded_bytes` (io_scan_diagnostics).

This is the DEPTH only. Whether a given scan is big enough to arm it is
`PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS` below, a separate knob — so a scan whose
`fetch_ahead_depth` reads 0 with a non-zero depth set here was gated, not
ignored."""

PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS: int = int(get("PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS", 48))
"""Minimum number of REMOTE fetch blocks a scan must have (after pruning)
before `PARQUET_IO_FETCH_AHEAD` is armed; 0 = no minimum.

A fetch block is the unit the fetch stage issues — the kept row groups of one
block of one file, fetched as one coalesced batch
(docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md). The production sweep that put the
optimum at 48-64 was made when a fetch unit was one 256k-row row group; counting
blocks keeps that calibration on files written with 64k row groups in blocks of
four (which have four times the row groups for the same data). On a row-major
file every row group is its own block, so the count is unchanged there. Tune
per session with `SET parquet_io_fetch_ahead_min_blocks`."""

PARQUET_IO_MEMORY_BUDGET_BYTES: int = int(get("PARQUET_IO_MEMORY_BUDGET_BYTES", 0))
"""Memory admission budget for one parquet scan's IO pipeline, in BYTES.
0 = auto: half of the cgroup memory limit when one is in effect (Cloud Run),
else half of physical RAM, else off. -1 = off explicitly.

What it bounds is what the pipeline itself HOLDS: decoded row groups from the
moment a worker claims one until the consumer pops its result, plus compressed
row-group bytes from fetch until decode has consumed them. Charges are the
footer's own estimates (Σ total_uncompressed_size / Σ total_compressed_size of
the projected columns), so the ledger is exact by construction and needs no
allocator hook. A worker that would push the total over budget waits at the
gate until a consumer pop releases enough; one is always admitted when nothing
consumer-releasable is held, so the scan can never stall on its own budget.

Why: before this the only bound on in-flight decode was a COUNT — the result
queue capacity (1024) and the submission window — with no relation to bytes. On
an 8 GiB worker a wide-row-group scan that ran ahead of a slow consumer was an
OOM, surfaced as a 503 with no memory log (see docs/ on the worker OOM class).
The pipeline reports the budget it runs (`memory_budget_bytes`), the peak it
held (`memory_held_high_watermark`) and how long tickets waited at the gate
(`admission_blocked_ns`) in io_scan_diagnostics — the read-backs that prove the
knob binds and show whether it ever bit."""

"""Minimum REMOTE row groups a scan must submit before `PARQUET_IO_FETCH_AHEAD`
is armed at all; below it the scan runs the coupled path (depth 0). Counted
after row-group pruning, on the same work-item list the pipeline fetches, so a
scan pruned down to a handful of row groups is gated on what it will actually
read rather than on what the manifest listed.

Why gate at all: the depth is a THREAD COUNT, and `set_fetch_ahead` builds that
pool eagerly (io_pipeline.hpp), while the auto submission window widens to
`max(workers, fetch_ahead) + 2` and sizes the IO arena from it. A small remote
scan therefore pays the whole setup — threads plus roughly double the arena
reservation — for concurrency it has too few row groups to use.

Deliberately a SEPARATE knob from the depth, not derived from it or from the
worker count (architect, 2026-09-13). The two answer different questions: the
depth is how wide to fetch, the gate is how big a scan has to be before that
width pays. Tying them would make one unmeasurable without moving the other,
which is the mistake the worker/window sweep already made once.

0 means no minimum — arm on any remote scan, which is the pre-gate behaviour
exactly. There is no special case for it: the test is `remote row groups >=
this`. Negative is rejected at plan time.

Default 48 against a depth of 64 (architect, 2026-09-13): the gate is set BELOW
the depth deliberately. It is the size at which fetch-ahead starts paying, not
the size at which every fetch thread has its own row group — those are different
questions, which is the whole reason this is not the depth."""
"""Pin HTTP requests to HTTP/1.1. Diagnostic ONLY — this exists to measure what
HTTP/2 contributes (with multiplexing unavailable, a low connection cap should
become catastrophic rather than faster). Leaving it True forfeits multiplexing."""

_max_workers_raw = str(get("MAX_EXECUTION_WORKERS", "auto")).strip().lower()
MAX_EXECUTION_WORKERS: int = (
    int(_max_workers_raw) if _max_workers_raw.lstrip("-").isdigit() else 0
)
"""Central parallel execution scheduler width (M4). **Softcoded by default**:
unset / "auto" / an impossible value (0 or less) is stored as 0 here, and
resolve_worker_count derives the effective width from the core count,
max(2, cpu - 2, floor(0.8 * cpu)). **An explicit positive integer is HONOURED EXACTLY** — never
clamped, never silently overridden, not even to the physical core count; set 128 and
you get 128 workers (oversubscription is warned once, not reduced). Worker count is
degree-of-parallelism only — it never selects a code path (W=1 is one worker, not the
serial engine). GROUP BY parallelises by ROW-ROUTING (disjoint key bins, no merge) —
the only grouped strategy."""


def resolve_max_execution_workers(requested: Optional[int] = None) -> int:
    """Effective execution width. The SINGLE derivation of the auto branch.

    Unset/"auto"/<=0 derives ``max(2, cpu - 2, floor(0.8 * cpu))``; an explicit positive request
    is HONOURED EXACTLY. There is no cap: the old cap of 16 held a 192-vCPU ClickBench
    box to the throughput of a 16-vCPU one (architect ruling 2026-09-23). The reserve is
    two cores, shrunk to 20% of the host below 10 vCPU where two cores would be too large
    a share (architect ruling 2026-09-25: plain 80% measured ~7% slower on the 16-vCPU
    wrenchy-bench box — 12 workers instead of 14). The local Parquet decode pool takes
    the same derivation (`resolve_parquet_local_io_workers`) and the two overlap.

    The floor is 2, not 1, so a 2-vCPU host stays parallel at all.

    This exists because `max_execution_workers` must be able to state the width the
    engine ACTUALLY runs at: the system-variable table stores the resolved value (see
    FromConfig `via=` in opteryx/variables.py), not the 0 sentinel (architect,
    2026-08-28). `resolve_worker_count` in the compiler delegates its auto branch here;
    pre-resolving is idempotent through it.
    """
    if requested is None:
        requested = MAX_EXECUTION_WORKERS
    requested = int(requested)
    if requested <= 0:
        cpu = _os.cpu_count() or 1
        return max(2, cpu - 2, (cpu * 4) // 5)
    return requested

if environ.get("FEATURE_DRAKEN_DICT_EXPR_STRICT") is not None:
    import warnings
    warnings.warn(
        "FEATURE_DRAKEN_DICT_EXPR_STRICT is retired and ignored; "
        "dictionary expression execution is strict-only.",
        DeprecationWarning,
        stacklevel=2,
    )

if environ.get("FEATURE_DRAKEN_DICT_EXPR_FASTPATH") is not None:
    import warnings
    warnings.warn(
        "FEATURE_DRAKEN_DICT_EXPR_FASTPATH is retired and ignored; "
        "dictionary expression execution is strict-only.",
        DeprecationWarning,
        stacklevel=2,
    )


MANIFEST_CACHE_PATH: str = get("OPTERYX_MANIFEST_CACHE_PATH", "")
"""Directory for the on-disk manifest cache. Empty disables the cache.

Must name a real disk (e.g. a Cloud Run ephemeral volume). The container
filesystem is otherwise RAM-backed, where caching would consume the memory it
is meant to conserve, so this has no default path."""

MANIFEST_CACHE_BYTES: int = int(get("OPTERYX_MANIFEST_CACHE_BYTES", 1024 * 1024 * 1024))
"""Byte ceiling for the on-disk manifest cache."""

MANIFEST_REMOTE_LOCATION: str = str(get("OPTERYX_MANIFEST_CACHE_LOCATION", "")).strip()
"""KV store backing the shared (remote) manifest cache, e.g. `valkey://host:6379`.

Deliberately NOT `KVSTORE_LOCATION`/`KVSTORE_LAYERS`: those configure the per-query
shuffle/spill store, whose keys are scoped by query and operator and whose contents
are discarded when the query ends. The manifest cache is the opposite — content-
addressed, shared across queries, and long-lived. They are different caches with
different lifecycles and must be pointed at different places."""

MANIFEST_REMOTE_MAX_VALUE_BYTES: int = int(
    get("OPTERYX_MANIFEST_REMOTE_MAX_VALUE_BYTES", 64 * 1024 * 1024)
)
"""Largest manifest written to the remote manifest cache (`KVSTORE_LOCATION`).

Manifest size scales with a dataset's file count, and the remote tier is reached
over the network: past some size, shipping the payload to and from the cache costs
more than the object-storage read it replaces. Oversized manifests are still served
from origin and still cached on local disk — only the remote write is skipped."""

FOOTER_REMOTE_LOCATION: str = str(get("OPTERYX_FOOTER_CACHE_LOCATION", "")).strip()
"""KV store backing the shared (remote) Parquet footer cache, e.g. `valkey://host:6376`.

Independently configured — deliberately NOT defaulted from `OPTERYX_MANIFEST_CACHE_LOCATION`.
The two caches have different key-population growth rates (footers are per-data-file, a much
larger and faster-growing population than per-snapshot manifests — see below) and a
deployment may legitimately want them on separate Valkey instances with separate eviction
budgets, or even separate infrastructure entirely. Defaulting one from the other would mean
configuring the manifest cache silently also turns on a second, differently-shaped write
load against the same server. Empty disables the tier. Same lifecycle as the manifest cache —
content-addressed by data-file path (a Parquet data file is write-once, so a cached footer can
never be stale), shared across queries, long-lived — and deliberately NOT `KVSTORE_LOCATION`,
which is the per-query spill store.

Entries are written without a TTL and are never invalidated (they cannot go stale), so the key
population grows with every data file ever scanned, including files later removed by
compaction. The deployment is expected to bound it at the server — `maxmemory` with an
`allkeys-lru`/`allkeys-lfu` policy. Size that budget for a per-data-file key population, not
a per-snapshot one — if this points at the same server as the manifest cache, the two policies
must both account for the combined load."""

FOOTER_REMOTE_MAX_VALUE_BYTES: int = int(
    get("OPTERYX_FOOTER_REMOTE_MAX_VALUE_BYTES", 4 * 1024 * 1024)
)
"""Largest footer envelope written to the remote footer cache.

A Parquet footer is small (tens to a few hundred KB); this ceiling only guards against
a pathological wide-schema footer costing more to ship to and from the cache than the
object-storage range read it replaces. Oversized footers are still fetched from origin
and cached in-process — only the remote write is skipped."""

LOCAL_STORE_ROOT: str = get("OPTERYX_LOCAL_STORE", "./.opteryx")
"""Root directory for LocalStoreConnector storage."""

# FEATURE FLAGS
class Features:
    # Feature flags are used to enable or disable experimental features.
    use_draken_ops_kernels = get_bool("FEATURE_USE_DRAKEN_OPS_KERNELS", False)
    disable_predicate_ordering = get_bool("FEATURE_DISABLE_PREDICATE_ORDERING", False)
    disable_predicate_pushdown = get_bool("FEATURE_DISABLE_PREDICATE_PUSHDOWN", False)
    disable_manifest_pruning = get_bool("FEATURE_DISABLE_MANIFEST_PRUNING", False)
    parquet_pool_reader = str(get("FEATURE_PARQUET_POOL_READER", "1")).lower() in ("1", "true", "yes")
    parquet_late_materialization = str(get("FEATURE_PARQUET_LATE_MATERIALIZATION", "1")).lower() in ("1", "true", "yes")
    skene_late_materialization = str(get("FEATURE_SKENE_LATE_MATERIALIZATION", "1")).lower() in ("1", "true", "yes")
    enable_dpccp_join_planning = get_bool("FEATURE_ENABLE_DPCCP_JOIN_PLANNING", True)

    # One kill-switch per optimizer strategy (opteryx/planner/optimizer/__init__.py's
    # OptimizerVisitor.strategies), for A/B testing a strategy against the rest of the
    # pipeline. All default False (every strategy enabled) — this changes no behaviour
    # until a specific one is set. See OptimizerVisitor._STRATEGY_DISABLE_FLAGS for the
    # strategy-class -> flag mapping this wires into.
    disable_aggregate_scan_pushdown = get_bool("FEATURE_DISABLE_AGGREGATE_SCAN_PUSHDOWN", False)
    disable_boolean_simplification = get_bool("FEATURE_DISABLE_BOOLEAN_SIMPLIFICATION", False)
    disable_compaction_planning = get_bool("FEATURE_DISABLE_COMPACTION_PLANNING", False)
    disable_constant_folding = get_bool("FEATURE_DISABLE_CONSTANT_FOLDING", False)
    disable_correlated_filters = get_bool("FEATURE_DISABLE_CORRELATED_FILTERS", False)
    disable_decorrelate_subquery = get_bool("FEATURE_DISABLE_DECORRELATE_SUBQUERY", False)
    disable_semi_join_reducer = get_bool("FEATURE_DISABLE_SEMI_JOIN_REDUCER", False)
    disable_semi_join_pushdown = get_bool("FEATURE_DISABLE_SEMI_JOIN_PUSHDOWN", False)
    disable_cross_join_filter_pushdown = get_bool("FEATURE_DISABLE_CROSS_JOIN_FILTER_PUSHDOWN", False)
    disable_disjunction_simplification = get_bool("FEATURE_DISABLE_DISJUNCTION_SIMPLIFICATION", False)
    disable_disjunctive_domain_pushdown = get_bool("FEATURE_DISABLE_DISJUNCTIVE_DOMAIN_PUSHDOWN", False)
    disable_distinct_pushdown = get_bool("FEATURE_DISABLE_DISTINCT_PUSHDOWN", False)
    disable_distinct_scan_pushdown = get_bool("FEATURE_DISABLE_DISTINCT_SCAN_PUSHDOWN", False)
    disable_filter_implied_group_key_reduction = get_bool("FEATURE_DISABLE_FILTER_IMPLIED_GROUP_KEY_REDUCTION", False)
    disable_function_rewrite = get_bool("FEATURE_DISABLE_FUNCTION_REWRITE", False)
    disable_group_key_reduction = get_bool("FEATURE_DISABLE_GROUP_KEY_REDUCTION", False)
    disable_join_algorithm = get_bool("FEATURE_DISABLE_JOIN_ALGORITHM", False)
    disable_join_condition_hoist = get_bool("FEATURE_DISABLE_JOIN_CONDITION_HOIST", False)
    disable_join_elimination = get_bool("FEATURE_DISABLE_JOIN_ELIMINATION", False)
    disable_join_key_materialization = get_bool(
        "FEATURE_DISABLE_JOIN_KEY_MATERIALIZATION", False
    )
    disable_join_planning = get_bool("FEATURE_DISABLE_JOIN_PLANNING", False)
    disable_join_rewrite = get_bool("FEATURE_DISABLE_JOIN_REWRITE", False)
    disable_length_only_column = get_bool("FEATURE_DISABLE_LENGTH_ONLY_COLUMN", False)
    disable_limit_elimination = get_bool("FEATURE_DISABLE_LIMIT_ELIMINATION", False)
    disable_limit_files_pruning = get_bool("FEATURE_DISABLE_LIMIT_FILES_PRUNING", False)
    disable_limit_pushdown = get_bool("FEATURE_DISABLE_LIMIT_PUSHDOWN", False)
    disable_operator_fusion = get_bool("FEATURE_DISABLE_OPERATOR_FUSION", False)
    disable_predicate_compaction = get_bool("FEATURE_DISABLE_PREDICATE_COMPACTION", False)
    disable_predicate_rewrite = get_bool("FEATURE_DISABLE_PREDICATE_REWRITE", False)
    disable_project_fusion = get_bool("FEATURE_DISABLE_PROJECT_FUSION", False)
    disable_projection_pushdown = get_bool("FEATURE_DISABLE_PROJECTION_PUSHDOWN", False)
    disable_redundant_cast_elimination = get_bool("FEATURE_DISABLE_REDUNDANT_CAST_ELIMINATION", False)
    disable_redundant_operations = get_bool("FEATURE_DISABLE_REDUNDANT_OPERATIONS", False)
    disable_redundant_sort_elimination = get_bool("FEATURE_DISABLE_REDUNDANT_SORT_ELIMINATION", False)
    disable_split_conjunctive_predicates = get_bool("FEATURE_DISABLE_SPLIT_CONJUNCTIVE_PREDICATES", False)
    disable_statistics_only_response = get_bool("FEATURE_DISABLE_STATISTICS_ONLY_RESPONSE", False)
    disable_timestamp_cast_sink = get_bool("FEATURE_DISABLE_TIMESTAMP_CAST_SINK", False)
    disable_topn_manifest_pruning = get_bool("FEATURE_DISABLE_TOPN_MANIFEST_PRUNING", False)
    disable_topn_scan_pushdown = get_bool("FEATURE_DISABLE_TOPN_SCAN_PUSHDOWN", False)
    disable_window_topk_fusion = get_bool("FEATURE_DISABLE_WINDOW_TOPK_FUSION", False)


features = Features()

PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER: int = int(get("PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER", 5))
"""Consecutive fully-passing row groups before abandoning two-pass mode for the rest of the query."""

PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY: float = float(
    get("PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY", 0.7)
)
"""Skip two-pass late materialization when the manifest's cheap, file-stats-based
selectivity estimate for the pushed predicate exceeds this (i.e. the predicate is
expected to prune too little to justify the pass-1/pass-2 split). Two-pass buys
nothing when almost every row survives pass 1 -- it still pays the full cost of
decoding pass-1 columns for the whole table before pass 2 can even start, which
for a wide/string filter column can cost more memory than reading everything in
one single pass would have. Estimation failures fail open (two-pass stays
eligible) rather than silently disabling the optimization for well-behaved
predicates the estimator just doesn't model."""

SKENE_LATE_MATERIALIZATION_MAX_SELECTIVITY: float = float(
    get("SKENE_LATE_MATERIALIZATION_MAX_SELECTIVITY", 0.7)
)
"""Skip two-pass late materialization on a skene scan when the manifest's
file-stats selectivity estimate for the filter exceeds this. Same reasoning and
same default as the parquet knob above; separate because skene's two passes have
a different cost shape (whole-file decodes, no row mask) and should be tunable
without moving parquet. What this bounds on skene specifically is pass 1's live
set: one sort-key value plus one row position per SURVIVING row, held across the
barrier until the top-n boundary is known."""

SKENE_LATE_MATERIALIZATION_MIN_DEFERRED_COLUMNS: int = int(
    get("SKENE_LATE_MATERIALIZATION_MIN_DEFERRED_COLUMNS", 8)
)
"""Minimum number of projected columns NOT read in pass 1 before a skene scan is
worth splitting into two passes. This is the gate that keeps late materialization
away from the narrow-projection shapes the reader-side-filter ruling was about
(ClickBench Q25/26/27: `SELECT SearchPhrase ... ORDER BY EventTime LIMIT 10`,
where deferring one column would cost a second open and a second decode of the
pass-1 columns to save nothing). Late materialization pays when the deferred set
dominates -- the shape it was built for, Q24, defers 103 of 105 columns.

It is also what bounds the downside. Pass 2 re-decodes the pass-1 columns as part
of the projection, so the worst case (a predicate and limit so weak that every
file survives the reduction) is a full single-pass scan PLUS pass 1 -- i.e. the
regression is capped at the pass-1 columns' share of the scan, which this gate
holds small by construction."""

# fmt:on
