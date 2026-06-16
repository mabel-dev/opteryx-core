# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
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

PARQUET_LOCAL_IO_WORKERS: int = int(get("PARQUET_LOCAL_IO_WORKERS", 8))
"""Worker threads for local-filesystem Parquet reads (mmap path, IO is near-free from OS cache)."""

PARQUET_GCS_IO_WORKERS: int = int(get("PARQUET_GCS_IO_WORKERS", 128))
"""Worker threads for GCS/HTTP Parquet reads (each range read pays network RTT, so high concurrency wins)."""

MAX_EXECUTION_WORKERS: int = int(get("MAX_EXECUTION_WORKERS", 1))
"""Central parallel execution scheduler width (M4). 1 = the serial engine, byte-
identical to the historic path (the default). >1 routes to the parallel engine,
which parallelises pipeline segments over data partitions on a query-scoped
CppThreadPool. Capped at 8 (the measured regression boundary) inside the engine.
See docs/M4_CENTRAL_SCHEDULER_DESIGN.md."""

PARALLEL_MIN_ROWS: int = int(get("PARALLEL_MIN_ROWS", 262_144))
"""Row-floor for the parallel scheduler (M4). A pipeline whose scan yields fewer
buffered rows than this runs serially — below it the per-worker clone + thread
setup (and the merge) dominate. Bench-tuned; set to 0 to force-engage parallel
on any input (testing/benchmarking)."""

PARALLEL_AGG_STRATEGY: str = get("PARALLEL_AGG_STRATEGY", "roundrobin")
"""Parallel grouped-aggregate strategy (M4). 'roundrobin' = whole morsels to
workers + WP-7 merge() (wins low/medium cardinality). 'shuffle' = hash-partition
rows by key into disjoint bins, NO merge (wins high cardinality). 'auto' (Stage 2)
will select by NDV. Default 'roundrobin' until the NDV selector lands. See
docs/M4_CENTRAL_SCHEDULER_DESIGN.md §11."""



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
    parquet_thread_scheduler = str(get("FEATURE_PARQUET_THREAD_SCHEDULER", "0")).lower() in ("1", "true", "yes")
    parquet_late_materialization = str(get("FEATURE_PARQUET_LATE_MATERIALIZATION", "1")).lower() in ("1", "true", "yes")
    enable_dpccp_join_planning = bool(get("FEATURE_ENABLE_DPCCP_JOIN_PLANNING", True))


features = Features()

PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER: int = int(get("PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER", 5))
"""Consecutive fully-passing row groups before abandoning two-pass mode for the rest of the query."""

# fmt:on
