# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

DISABLE_OPTIMIZER: bool = bool(get("DISABLE_OPTIMIZER", False))
"""**DANGEROUS** This will cause most queries to fail."""

OPTERYX_DEBUG: bool = bool(get("OPTERYX_DEBUG", False))
"""**DANGEROUS** Diagnostic and debug mode - generates a lot of log entries."""
MAX_CONSECUTIVE_CACHE_FAILURES: int = int(get("MAX_CONSECUTIVE_CACHE_FAILURES", 10))
"""Maximum number of consecutive cache failures before disabling cache usage."""

# These values are computed lazily via __getattr__ to avoid importing
# psutil (and making expensive system calls) during module import.
# Annotate the names so type checkers know about them, but do not assign
# values here — __getattr__ will compute and cache them on first access.
MAX_LOCAL_BUFFER_CAPACITY: int
"""Local buffer pool size in either bytes or fraction of system memory (lazy)."""

MAX_READ_BUFFER_CAPACITY: int
"""Read buffer pool size in either bytes or fraction of system memory (lazy)."""

CONCURRENT_READS:int = int(get("CONCURRENT_READS", max(system_gigabytes(), 2)))
READ_BUFFER_CAPACITY:int = memory_allocation_calculation(float(get("MAX_READ_BUFFER_CAPACITY", 0.1)))

# Backwards compatibility alias: some modules expect MAX_READ_BUFFER_CAPACITY
# to be present on the `config` module. Provide an alias to the computed
# `READ_BUFFER_CAPACITY` value so older callers continue to work.
MAX_READ_BUFFER_CAPACITY = READ_BUFFER_CAPACITY

ENABLE_ZERO_COPY: bool = bool(get("ENABLE_ZERO_COPY", True))


# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = get("GCP_PROJECT_ID") 
# size of morsels to push between steps
# MORSEL_SIZE remains a plain constant
MORSEL_SIZE: int = int(get("MORSEL_SIZE", 64 * 1024 * 1024))


# fmt:on


# FEATURE FLAGS
class Features:
    # Feature flags are used to enable or disable experimental features.
    enable_native_aggregator = bool(get("FEATURE_ENABLE_NATIVE_AGGREGATOR", False))
    disable_nested_loop_join = bool(get("FEATURE_DISABLE_NESTED_LOOP_JOIN", False))
    force_nested_loop_join = bool(get("FEATURE_FORCE_NESTED_LOOP_JOIN", False))
    enable_free_threading = bool(get("FEATURE_ENABLE_FREE_THREADING", False))
    use_draken_ops_kernels = bool(get("FEATURE_USE_DRAKEN_OPS_KERNELS", False))
    disable_predicate_ordering = bool(get("FEATURE_DISABLE_PREDICATE_ORDERING", False))
    disable_predicate_pushdown = bool(get("FEATURE_DISABLE_PREDICATE_PUSHDOWN", False))
    disable_manifest_pruning = bool(get("FEATURE_DISABLE_MANIFEST_PRUNING", False))


features = Features()
