# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import typing
from os import environ
from typing import Optional
from typing import Union


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
    return json.loads(text)


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

DISABLE_OPTIMIZER: bool = bool(get("DISABLE_OPTIMIZER", False))
"""**DANGEROUS** This will cause most queries to fail."""

OPTERYX_DEBUG: bool = bool(get("OPTERYX_DEBUG", False))
"""**DANGEROUS** Diagnostic and debug mode - generates a lot of log entries."""

OPTERYX_TRACE: bool = bool(get("OPTERYX_TRACE", "").lower() in ("1", "true", "yes"))
"""Enable IO layer tracing (records file operations to JSONLines file)."""

OPTERYX_TRACE_FILE: str = str(get("OPTERYX_TRACE_FILE", ""))
"""Path to write IO trace file (.jsonl format). Empty = no tracing."""

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

# IOPS ring buffer configuration
IOPS_SLOT_SIZE: int = int(get("IOPS_SLOT_SIZE", 64 * 1024 * 1024))
"""Size of each shared-memory ring slot in bytes (default: 64 MiB).
Must be >= the largest Parquet blob you expect to read."""

IOPS_MAX_INFLIGHT: int = int(get("IOPS_MAX_INFLIGHT", 0)) or CONCURRENT_READS
"""Maximum number of concurrent blob downloads in the IOPS worker
(default: CONCURRENT_READS)."""

IOPS_SLOT_COUNT: int = int(get("IOPS_SLOT_COUNT", 0)) or max(IOPS_MAX_INFLIGHT * 2, 16)
"""Total ring slots (default: 2 × IOPS_MAX_INFLIGHT, minimum 16).
Must be >= IOPS_MAX_INFLIGHT."""

IOPS_CHUNK_SIZE: int = int(get("IOPS_CHUNK_SIZE", 8 * 1024 * 1024))
"""HTTP streaming chunk size for aiohttp downloads in bytes (default: 8 MiB)."""

IOPS_PREFAULT_MODE: str = str(get("IOPS_PREFAULT_MODE", "adaptive"))
"""Shared-memory pre-fault mode: adaptive, full, first-slot, or none."""

IOPS_REUSE_RING: bool = str(get("IOPS_REUSE_RING", "1")).lower() in ("1", "true", "yes")
"""Reuse a compatible shared-memory ring allocation across readers in-process."""
# size of morsels to push between steps
# MORSEL_SIZE remains a plain constant
MORSEL_SIZE: int = int(get("MORSEL_SIZE", 64 * 1024 * 1024))

# Parquet row-group scheduler configuration (v2)
PARQUET_FILES_IN_FLIGHT: int = int(get("PARQUET_FILES_IN_FLIGHT", 2))
"""Maximum active parquet files admitted concurrently by the v2 scheduler."""

PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT: int = int(get("PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT", 5))
"""Maximum active row groups per active file for the v2 scheduler."""

PARQUET_GLOBAL_RANGE_READERS: int = int(get("PARQUET_GLOBAL_RANGE_READERS", 24))
"""Hard cap for in-flight column range reads across the full parquet scan."""

PARQUET_RANGE_READERS_PER_ROWGROUP: int = int(get("PARQUET_RANGE_READERS_PER_ROWGROUP", 10))
"""Cap for in-flight column range reads per row group."""


# fmt:on


# FEATURE FLAGS
class Features:
    # Feature flags are used to enable or disable experimental features.
    enable_native_aggregator = bool(get("FEATURE_ENABLE_NATIVE_AGGREGATOR", False))
    enable_iops = bool(get("FEATURE_ENABLE_IOPS", True))
    disable_nested_loop_join = bool(get("FEATURE_DISABLE_NESTED_LOOP_JOIN", False))
    force_nested_loop_join = bool(get("FEATURE_FORCE_NESTED_LOOP_JOIN", False))
    enable_free_threading = bool(get("FEATURE_ENABLE_FREE_THREADING", False))
    use_draken_ops_kernels = bool(get("FEATURE_USE_DRAKEN_OPS_KERNELS", False))
    use_draken_aggregator = str(get("FEATURE_USE_DRAKEN_AGGREGATOR", "0")).lower() in (
        "1",
        "true",
        "yes",
    )
    disable_predicate_ordering = bool(get("FEATURE_DISABLE_PREDICATE_ORDERING", False))
    disable_predicate_pushdown = bool(get("FEATURE_DISABLE_PREDICATE_PUSHDOWN", False))
    disable_manifest_pruning = bool(get("FEATURE_DISABLE_MANIFEST_PRUNING", False))
    use_parquet_reader = str(get("FEATURE_USE_PARQUET_READER", "0")).lower() in (
        "1",
        "true",
        "yes",
    )
    parquet_rowgroup_scheduler_v2 = str(
        get("FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2", "1")
    ).lower() in (
        "1",
        "true",
        "yes",
    )


features = Features()
