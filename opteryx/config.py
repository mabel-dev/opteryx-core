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

    # Import psutil lazily to avoid paying the import cost at module import time.
    # Use a small helper so tests or callers that need the value will trigger the
    # import only when this function is called.
    def _get_total_memory_bytes() -> int:
        import psutil

        return psutil.virtual_memory().total

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

    This imports psutil lazily to avoid paying the cost at module import time.

    Returns:
        int: Total system memory in gigabytes.
    """
    import psutil

    return psutil.virtual_memory().total // (1024 * 1024 * 1024)


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


def parse_connector_targets(
    value: typing.Any, default: Optional[typing.Iterable[str]] = None
) -> frozenset[str]:
    """
    Parse a connector selector string such as ``LOCAL,S3`` into uppercase tokens.
    """
    if value is None:
        value = default
    if value is None:
        return frozenset()
    if isinstance(value, str):
        items = [part.strip().upper() for part in value.split(",") if part.strip()]
    else:
        items = [str(part).strip().upper() for part in value if str(part).strip()]
    if not items:
        return frozenset()
    if "ALL" in items:
        return frozenset({"ALL"})
    if "NONE" in items:
        return frozenset()
    return frozenset(items)


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

DISABLE_OPTIMIZER: bool = bool(get("DISABLE_OPTIMIZER", False))
"""**DANGEROUS** This will cause most queries to fail."""

OPTERYX_DEBUG: bool = bool(get("OPTERYX_DEBUG", False))
"""**DANGEROUS** Diagnostic and debug mode - generates a lot of log entries."""

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

# size of morsels to push between steps
# MORSEL_SIZE remains a plain constant
MORSEL_SIZE: int = int(get("MORSEL_SIZE", 64 * 1024 * 1024))

# Parquet row-group scheduler configuration (v2)
PARQUET_FILES_IN_FLIGHT: int = int(get("PARQUET_FILES_IN_FLIGHT", 2))
"""Maximum active parquet files admitted concurrently by the v2 scheduler."""

PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT: int = int(get("PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT", 10))
"""Maximum active row groups per active file for the v2 scheduler."""

PARQUET_ROWGROUPS_IN_FLIGHT: int = int(
    get(
        "PARQUET_ROWGROUPS_IN_FLIGHT",
        24
    )
)
"""Maximum active row groups across the full parquet scan for the v2 scheduler.

Defaults to ``PARQUET_FILES_IN_FLIGHT * PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT`` so
introducing this cap does not silently reduce prior effective concurrency.
"""

PARQUET_GLOBAL_RANGE_READERS: int = int(get("PARQUET_GLOBAL_RANGE_READERS", 64))
"""Hard cap for in-flight column range reads across the full parquet scan."""

PARQUET_RANGE_READERS_PER_ROWGROUP: int = int(get("PARQUET_RANGE_READERS_PER_ROWGROUP", 10))
"""Cap for in-flight column range reads per row group."""

PARQUET_PREFETCH_FOOTER_WORKERS: int = int(get("PARQUET_PREFETCH_FOOTER_WORKERS", 64))
"""Concurrency for parquet footer prefetch in ParquetReadNode preflight."""

PARQUET_ACTIVE_ROWGROUPS_TARGET: int = int(get("PARQUET_ACTIVE_ROWGROUPS_TARGET", 16))
"""Target active row groups for continuous-feed scheduling."""

PARQUET_WARM_START_OPS: int = int(get("PARQUET_WARM_START_OPS", 10))
"""Number of initial dispatch ops reserved for the first admitted row group."""

PARQUET_LOW_COLUMN_THRESHOLD: int = int(get("PARQUET_LOW_COLUMN_THRESHOLD", 3))
"""Enable low-column scheduling strategy when projected columns are below this count."""

PARQUET_LOW_COLUMN_ACTIVE_ROWGROUPS_TARGET: int = int(
    get("PARQUET_LOW_COLUMN_ACTIVE_ROWGROUPS_TARGET", 10)
)
"""Active row-group target for low-column strategy."""

PARQUET_LOW_COLUMN_PER_ROWGROUP_SLOTS: int = int(
    get("PARQUET_LOW_COLUMN_PER_ROWGROUP_SLOTS", 3)
)
"""Per-rowgroup in-flight read cap for low-column strategy."""

PARQUET_READY_ROWGROUP_QUEUE_CAP: int = int(get("PARQUET_READY_ROWGROUP_QUEUE_CAP", 6))
"""Bound on row groups waiting to emit into the ring transport."""

PARQUET_COMPLETED_ROWGROUP_BACKLOG_CAP: int = int(
    get("PARQUET_COMPLETED_ROWGROUP_BACKLOG_CAP", PARQUET_READY_ROWGROUP_QUEUE_CAP * 4)
)
"""Bound on completed row groups retained by scheduler before emitter handoff."""

PARQUET_DECODE_WORKERS: int = int(
    get("PARQUET_DECODE_WORKERS", max(4, PARQUET_GLOBAL_RANGE_READERS // 2))
)
"""Decode worker count for IO-process scheduler's decode stage."""

PARQUET_READ_DECODE_BUFFER_CAP: int = int(
    get("PARQUET_READ_DECODE_BUFFER_CAP", PARQUET_GLOBAL_RANGE_READERS * 4)
)
"""Max pending read-complete items awaiting decode handoff.

Deprecated: use PARQUET_RAW_RING_CAP instead.
"""

PARQUET_RAW_RING_CAP: int = int(
    get("PARQUET_RAW_RING_CAP", PARQUET_GLOBAL_RANGE_READERS * 2)
)
"""Hard cap on raw (decoded) row-group buffers sitting in the ring between
read completion and decode submission.  Replaces PARQUET_READ_DECODE_BUFFER_CAP.
Default = PARQUET_GLOBAL_RANGE_READERS * 2 so downloads run two full waves
ahead of the decode pool without unbounded memory accumulation."""

PARQUET_SMALL_FILE_THRESHOLD: int = int(get("PARQUET_SMALL_FILE_THRESHOLD", 4 * 1024 * 1024))
"""Files whose known size does not exceed this value (bytes) are fetched in a
single whole-file read rather than per-column range requests (§3)."""

PARQUET_SPECULATIVE_RG_BYTES: int = int(get("PARQUET_SPECULATIVE_RG_BYTES", 0))
"""Minimum estimated column-batch size (bytes) for speculative row-group
prefetch (§4).  Set 0 to disable speculative reads entirely."""

# Deprecation check: warn if caller set the old key without setting the new one.
if environ.get("PARQUET_READ_DECODE_BUFFER_CAP") and not environ.get("PARQUET_RAW_RING_CAP"):
    import warnings
    warnings.warn(
        "PARQUET_READ_DECODE_BUFFER_CAP is deprecated; set PARQUET_RAW_RING_CAP instead.",
        DeprecationWarning,
        stacklevel=2,
    )

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
        "dictionary expression fastpath is always enabled for dictionary candidates.",
        DeprecationWarning,
        stacklevel=2,
    )

# IO process row-group ring transport configuration
IO_RING_SLOT_BYTES: int = int(get("IO_RING_SLOT_BYTES", 32 * 1024 * 1024))
"""Shared-memory slot size in bytes for FEATURE_IO_PROCESS_ROWGROUP_RING."""

IO_RING_SLOT_COUNT: int = int(get("IO_RING_SLOT_COUNT", 64))
"""Shared-memory slot count for FEATURE_IO_PROCESS_ROWGROUP_RING."""

IO_MAX_FRAGMENTS_PER_TRANSFER: int = int(get("IO_MAX_FRAGMENTS_PER_TRANSFER", 8))
"""Maximum fragments for one transfer before row-group slicing is applied."""

IO_TARGET_SLICE_BYTES: int = int(get("IO_TARGET_SLICE_BYTES", 16 * 1024 * 1024))
"""Target serialized bytes per row-group slice when slicing is required."""


_serial_reader_setting = get("FEATURE_USE_SERIAL_READER", "LOCAL")



# FEATURE FLAGS
class Features:
    # Feature flags are used to enable or disable experimental features.
    disable_nested_loop_join = bool(get("FEATURE_DISABLE_NESTED_LOOP_JOIN", False))
    force_nested_loop_join = bool(get("FEATURE_FORCE_NESTED_LOOP_JOIN", False))
    enable_free_threading = bool(get("FEATURE_ENABLE_FREE_THREADING", False))
    use_draken_ops_kernels = bool(get("FEATURE_USE_DRAKEN_OPS_KERNELS", False))
    disable_predicate_ordering = bool(get("FEATURE_DISABLE_PREDICATE_ORDERING", False))
    disable_predicate_pushdown = bool(get("FEATURE_DISABLE_PREDICATE_PUSHDOWN", False))
    disable_manifest_pruning = bool(get("FEATURE_DISABLE_MANIFEST_PRUNING", False))
    parquet_rowgroup_scheduler_v2 = str(get("FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2", "1")).lower() in ("1", "true", "yes")
    io_process_rowgroup_ring = str(get("FEATURE_IO_PROCESS_ROWGROUP_RING", "0")).lower() in ("1", "true", "yes")
    use_serial_reader = parse_connector_targets(_serial_reader_setting, default=("LOCAL",))
    parquet_thread_scheduler = str(get("FEATURE_PARQUET_THREAD_SCHEDULER", "0")).lower() in ("1", "true", "yes")
    parquet_late_materialization = str(get("FEATURE_PARQUET_LATE_MATERIALIZATION", "1")).lower() in ("1", "true", "yes")


features = Features()

PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER: int = int(get("PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER", 5))
"""Consecutive fully-passing row groups before abandoning two-pass mode for the rest of the query."""

# fmt:on
