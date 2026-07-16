# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import os as _os
import typing
from os import environ
from typing import Optional, Union


def memory_allocation_calculation(allocation: Union[float, int]) -> int:
    """
    Configure the memory allocation for the database based on the input.
    If the allocation is between 0 and 1, it's treated as a percentage of the total system memory.
    If the allocation is greater than 1, it's treated as an absolute value in megabytes.

    Parameters:
        allocation (float|int): Memory allocation value which could be a percentage or an absolute value.

    Returns:
        int: Memory size in bytes to be allocated.
    """

    # Use the compiled platform extension directly. Fail loudly if not present.
    def _get_total_memory_bytes() -> int:
        from opteryx.compiled import platform as _platform  # type: ignore

        # Use physical RAM as the total memory reference
        return int(_platform.physical_memory_total_bytes())

    total_memory = _get_total_memory_bytes()
    if 0 < allocation < 1:  # Treat as a percentage
        return int(total_memory * allocation)
    elif allocation >= 1:  # Treat as an absolute value in MB
        return int(allocation * 1024 * 1024)
    else:
        raise ValueError("Invalid memory allocation value. Must be a positive number.")


def system_gigabytes() -> int:
    """
    Get the total system memory in gigabytes.

    This uses the compiled platform extension lazily to avoid paying the cost at module import time.

    Returns:
        int: Total system memory in gigabytes.
    """
    from opteryx.compiled import platform as _platform  # type: ignore

    return int(_platform.physical_memory_total_bytes()) // (1024 * 1024 * 1024)


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

DISABLE_OPTIMIZER: bool = bool(get("DISABLE_OPTIMIZER", False))
"""**DANGEROUS** This will cause most queries to fail."""

OPTERYX_DEBUG: bool = bool(get("OPTERYX_DEBUG", False))
"""**DANGEROUS** Diagnostic and debug mode - generates a lot of log entries."""

VALIDATE_OPTIMIZER_PLANS: bool = bool(get("VALIDATE_OPTIMIZER_PLANS", False))
"""Debug guardrail: when set, the optimizer checks plan structural invariants
after every strategy and raises (naming the offending strategy) on corruption.
Off by default — adds per-strategy validation cost only when enabled."""

OPTERYX_TRACE: bool = bool(get("OPTERYX_TRACE", "").lower() in ("1", "true", "yes"))
"""Enable IO layer tracing.  When true, events are recorded in memory and
can be retrieved via :func:`~opteryx.query_session.Session.trace`."""
OPTERYX_TRACE_SAMPLE_RATE: float = float(get("OPTERYX_TRACE_SAMPLE_RATE", 1.0))
"""Sampling rate for traced files (0.0–1.0). Defaults to 1.0 (100%).
When tracing is enabled, each event carrying a ``file_id`` will be skipped
with probability ``1 - OPTERYX_TRACE_SAMPLE_RATE``.  This provides a simple
way to reduce overhead on large scans by only recording a fraction of files.
"""
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

# These values are computed lazily via __getattr__ to avoid importing
# psutil (and making expensive system calls) during module import.
# Annotate the names so type checkers know about them, but do not assign
# values here — __getattr__ will compute and cache them on first access.
MAX_LOCAL_BUFFER_CAPACITY: int
"""Local buffer pool size in either bytes or fraction of system memory (lazy)."""

CONCURRENT_READS:int = int(get("CONCURRENT_READS", max(system_gigabytes(), 2)))

ENABLE_ZERO_COPY: bool = bool(get("ENABLE_ZERO_COPY", True))

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = get("GCP_PROJECT_ID")

PARQUET_LOCAL_IO_WORKERS: int = int(
    get("PARQUET_LOCAL_IO_WORKERS", min(16, max(8, (_os.cpu_count() or 8) - 2)))
)
"""Worker threads for local-filesystem Parquet reads (mmap path, IO is near-free from OS cache).

Default scales with the host: ``max(8, cpu_count - 2)`` capped at 16. Decode parallelises
near-linearly (measured ~8.4x at 16 workers on a string-heavy ClickBench scan), so larger
machines use their cores; the floor of 8 means small instances (e.g. <=8 vCPU Cloud Run) are
never reduced below the historic default. Override via the env var to tune per deployment."""

PARQUET_GCS_IO_WORKERS: int = int(get("PARQUET_GCS_IO_WORKERS", 128))
"""Worker threads for GCS/HTTP Parquet reads (each range read pays network RTT, so high concurrency wins)."""

_max_workers_raw = str(get("MAX_EXECUTION_WORKERS", "auto")).strip().lower()
MAX_EXECUTION_WORKERS: int = (
    int(_max_workers_raw) if _max_workers_raw.lstrip("-").isdigit() else 0
)
"""Central parallel execution scheduler width (M4). **Softcoded by default**:
unset / "auto" / an impossible value (0 or less) is stored as 0 here, and
resolve_worker_count derives the effective width from the core count,
max(1, min(cpu-2, 16)). **An explicit positive integer is HONOURED EXACTLY** — never
clamped, never silently overridden, not even to the physical core count; set 128 and
you get 128 workers (oversubscription is warned once, not reduced). Worker count is
degree-of-parallelism only — it never selects a code path (W=1 is one worker, not the
serial engine). GROUP BY parallelises by ROW-ROUTING (disjoint key bins, no merge) —
the only grouped strategy; the ungrouped/stateless paths engage above PARALLEL_MIN_ROWS."""

PARALLEL_MIN_ROWS: int = int(get("PARALLEL_MIN_ROWS", 262_144))
"""Row-floor for the parallel scheduler (M4). A pipeline whose scan yields fewer
buffered rows than this runs through the operator's own (single-producer) path —
below it the per-worker clone + thread setup dominate. Bench-tuned; set to 0 to
force-engage parallel on any input (testing/benchmarking)."""


if environ.get("FEATURE_DRAKEN_DICT_EXPR_STRICT") is not None:
    import warnings
    warnings.warn(
        "FEATURE_DRAKEN_DICT_EXPR_STRICT is retired and ignored; "
        "dictionary expression execution is strict-only.",
        DeprecationWarning,
        stacklevel=2,
    )


# Parquet pool reader (threaded IO worker + MemoryPool transport) configuration
IO_POOL_SLOT_BYTES: int = int(get("IO_POOL_SLOT_BYTES", 32 * 1024 * 1024))
"""Initial per-slot byte budget for the MemoryPool used by the pool reader."""

IO_POOL_SLOT_COUNT: int = int(get("IO_POOL_SLOT_COUNT", 64))
"""Initial slot count for the MemoryPool used by the pool reader."""

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

FOOTER_REMOTE_LOCATION: str = str(
    get("OPTERYX_FOOTER_CACHE_LOCATION", MANIFEST_REMOTE_LOCATION)
).strip()
"""KV store backing the shared (remote) Parquet footer cache, e.g. `valkey://host:6379`.

Defaults to `OPTERYX_MANIFEST_CACHE_LOCATION`: a deployment that already runs a shared
Valkey for manifests gets footer caching on the same server for free, kept apart by a
distinct key prefix. Empty disables the tier. Same lifecycle as the manifest cache —
content-addressed by data-file path (a Parquet data file is write-once, so a cached
footer can never be stale), shared across queries, long-lived — and deliberately NOT
`KVSTORE_LOCATION`, which is the per-query spill store.

Entries are written without a TTL and are never invalidated (they cannot go stale), so the
key population grows with every data file ever scanned, including files later removed by
compaction. The deployment is expected to bound it at the server — `maxmemory` with an
`allkeys-lru`/`allkeys-lfu` policy — exactly as for the shared manifest cache it shares by
default. Footers are per-data-file, a much larger key population than per-snapshot manifests,
so size the eviction budget with that in mind."""

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
    disable_nested_loop_join = bool(get("FEATURE_DISABLE_NESTED_LOOP_JOIN", False))
    force_nested_loop_join = bool(get("FEATURE_FORCE_NESTED_LOOP_JOIN", False))
    use_draken_ops_kernels = bool(get("FEATURE_USE_DRAKEN_OPS_KERNELS", False))
    disable_predicate_ordering = bool(get("FEATURE_DISABLE_PREDICATE_ORDERING", False))
    disable_predicate_pushdown = bool(get("FEATURE_DISABLE_PREDICATE_PUSHDOWN", False))
    disable_manifest_pruning = bool(get("FEATURE_DISABLE_MANIFEST_PRUNING", False))
    parquet_pool_reader = str(get("FEATURE_PARQUET_POOL_READER", "1")).lower() in ("1", "true", "yes")
    parquet_late_materialization = str(get("FEATURE_PARQUET_LATE_MATERIALIZATION", "1")).lower() in ("1", "true", "yes")
    enable_dpccp_join_planning = bool(get("FEATURE_ENABLE_DPCCP_JOIN_PLANNING", True))


features = Features()

PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER: int = int(get("PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER", 5))
"""Consecutive fully-passing row groups before abandoning two-pass mode for the rest of the query."""

# fmt:on
