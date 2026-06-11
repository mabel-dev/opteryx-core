"""
Internal Opteryx utilities - general-purpose helpers.

This module provides utility functions for Opteryx, eliminating the external
sql dependency while optimizing for Opteryx's actual use cases.

Functions provided:
- random_string(length): Generate random alphanumeric strings
- single_item_cache(func): Cache decorator for functions with single argument
- lru_cache_with_expiry(maxsize, ttl): LRU cache with time-based expiration
"""

import functools
import random
import string
import threading
import time
from typing import Any, Callable, Dict, Optional, TypeVar

__all__ = [
    "random_string",
    "random_int",
    "single_item_cache",
    "lru_cache_with_expiry",
]

# Type variable for decorator functions
F = TypeVar("F", bound=Callable[..., Any])


def random_string(length: int = 8, charset: str = None) -> str:
    """Generate a random alphanumeric string.

    Uses the compiled PCG-based implementation for best performance. This
    function does not fall back — importing the compiled helper is required.
    """
    from opteryx.compiled.functions.random_helper import random_string_c

    return random_string_c(length, charset)


def random_int(min_value: int = 0, max_value: int = 2**31 - 1) -> int:
    """Generate a random integer within range.

    Args:
        min_value: Minimum value (default: 0)
        max_value: Maximum value (default: 2^31 - 1)

    Returns:
        Random integer in range [min_value, max_value]

    Examples:
        >>> n = random_int(1, 100)
        >>> 1 <= n <= 100
        True
    """
    return random.randint(min_value, max_value)


def single_item_cache(func: F) -> F:
    """Cache decorator for functions with a single argument.

    Caches the most recent result for each unique argument value.
    Thread-safe. Useful for expensive lookups that are called repeatedly
    with the same argument.

    Args:
        func: Function to decorate

    Returns:
        Decorated function with caching

    Examples:
        >>> @single_item_cache
        ... def expensive_lookup(key: str) -> str:
        ...     return key.upper()
        >>> expensive_lookup("hello")
        'HELLO'
        >>> expensive_lookup("hello")  # Retrieved from cache
        'HELLO'
    """
    cache: Dict[Any, Any] = {}
    lock = threading.Lock()

    @functools.wraps(func)
    def wrapper(arg: Any) -> Any:
        # Fast path: check cache without lock
        if arg in cache:
            return cache[arg]

        # Slow path: compute and store
        with lock:
            # Double-check after acquiring lock
            if arg in cache:
                return cache[arg]
            result = func(arg)
            cache[arg] = result
            return result

    # Expose cache for inspection/clearing
    wrapper.cache = cache  # type: ignore
    wrapper.cache_clear = cache.clear  # type: ignore

    return wrapper  # type: ignore


def lru_cache_with_expiry(maxsize: int = 128, ttl: Optional[float] = None) -> Callable[[F], F]:
    """LRU cache decorator with optional time-based expiry.

    Combines LRU (Least Recently Used) eviction with optional TTL (Time To Live)
    expiration. Useful for caching expensive operations that should be refreshed
    periodically.

    Args:
        maxsize: Maximum number of cached entries (default: 128)
        ttl: Time to live in seconds; None means no expiration (default: None)

    Returns:
        Decorator function

    Examples:
        >>> @lru_cache_with_expiry(maxsize=32, ttl=60)
        ... def expensive_api_call(endpoint: str) -> str:
        ...     return f"result for {endpoint}"
        >>> expensive_api_call("/api/users")
        'result for /api/users'
    """

    def decorator(func: F) -> F:
        cache: Dict[Any, tuple[Any, float]] = {}
        lock = threading.Lock()
        hit_count = [0]  # Mutable counter for hit tracking
        miss_count = [0]  # Mutable counter for miss tracking

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Create cache key from arguments
            key = (args, tuple(sorted(kwargs.items())))

            # Fast path: check cache without lock
            if key in cache:
                result, timestamp = cache[key]
                # Check if expired
                if ttl is None or (time.time() - timestamp) < ttl:
                    hit_count[0] += 1
                    return result
                # Expired; remove from cache
                with lock:
                    if key in cache:
                        del cache[key]

            # Slow path: compute and store
            miss_count[0] += 1
            result = func(*args, **kwargs)

            with lock:
                # Evict oldest entry if cache is full
                if len(cache) >= maxsize:
                    # Remove the first (oldest) entry
                    oldest_key = next(iter(cache))
                    del cache[oldest_key]

                cache[key] = (result, time.time())

            return result

        # Expose cache introspection
        wrapper.cache_info = lambda: {  # type: ignore
            "hits": hit_count[0],
            "misses": miss_count[0],
            "size": len(cache),
            "maxsize": maxsize,
        }
        wrapper.cache_clear = cache.clear  # type: ignore

        return wrapper  # type: ignore

    return decorator
