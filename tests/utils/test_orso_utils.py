"""Unit tests for inlined orso utilities."""

import threading
import time

import pytest

from opteryx.utils._orso_utils import (
    lru_cache_with_expiry,
    random_int,
    random_string,
    single_item_cache,
)


class TestRandomString:
    """Test random_string() function."""

    def test_default_length(self):
        """Test default length is 8."""
        s = random_string()
        assert len(s) == 8

    def test_custom_length(self):
        """Test custom length."""
        for length in [1, 5, 16, 100]:
            s = random_string(length)
            assert len(s) == length

    def test_default_charset(self):
        """Test default charset is alphanumeric."""
        s = random_string(100)
        # All characters should be letters or digits
        assert all(c.isalnum() for c in s)

    def test_custom_charset(self):
        """Test custom charset."""
        charset = "abc123"
        s = random_string(50, charset=charset)
        assert all(c in charset for c in s)

    def test_randomness(self):
        """Test that different calls generate different strings."""
        s1 = random_string(20)
        s2 = random_string(20)
        # Very unlikely to be the same
        assert s1 != s2

    def test_empty_length(self):
        """Test length 0 produces empty string."""
        s = random_string(0)
        assert s == ""

    def test_special_charset(self):
        """Test with special characters."""
        charset = "!@#$%"
        s = random_string(20, charset=charset)
        assert all(c in charset for c in s)


class TestRandomInt:
    """Test random_int() function."""

    def test_default_range(self):
        """Test default range."""
        for _ in range(100):
            n = random_int()
            assert 0 <= n < 2**31

    def test_custom_range(self):
        """Test custom range."""
        for _ in range(100):
            n = random_int(1, 100)
            assert 1 <= n <= 100

    def test_negative_range(self):
        """Test negative range."""
        for _ in range(50):
            n = random_int(-100, -1)
            assert -100 <= n <= -1

    def test_single_value_range(self):
        """Test range where min == max."""
        n = random_int(42, 42)
        assert n == 42

    def test_randomness(self):
        """Test that random_int produces varied results."""
        values = [random_int(1, 1000) for _ in range(100)]
        # Should have many unique values
        assert len(set(values)) > 50


class TestSingleItemCache:
    """Test single_item_cache decorator."""

    def test_basic_caching(self):
        """Test that results are cached."""
        call_count = [0]

        @single_item_cache
        def expensive_func(x):
            call_count[0] += 1
            return x * 2

        # First call
        result1 = expensive_func(5)
        assert result1 == 10
        assert call_count[0] == 1

        # Second call with same argument should use cache
        result2 = expensive_func(5)
        assert result2 == 10
        assert call_count[0] == 1  # Not incremented

    def test_different_arguments(self):
        """Test that different arguments create different cache entries."""
        call_count = [0]

        @single_item_cache
        def expensive_func(x):
            call_count[0] += 1
            return x * 2

        result1 = expensive_func(5)
        assert call_count[0] == 1

        result2 = expensive_func(10)
        assert call_count[0] == 2

        # Cache for 5 should still work
        result3 = expensive_func(5)
        assert call_count[0] == 2

    def test_cache_clear(self):
        """Test cache_clear method."""
        call_count = [0]

        @single_item_cache
        def expensive_func(x):
            call_count[0] += 1
            return x * 2

        expensive_func(5)
        assert call_count[0] == 1

        expensive_func(5)
        assert call_count[0] == 1  # Cached

        # Clear cache
        expensive_func.cache_clear()

        expensive_func(5)
        assert call_count[0] == 2  # Called again

    def test_none_argument(self):
        """Test caching with None as argument."""
        call_count = [0]

        @single_item_cache
        def func_with_none(x):
            call_count[0] += 1
            return x

        func_with_none(None)
        assert call_count[0] == 1

        func_with_none(None)
        assert call_count[0] == 1  # Cached

    def test_thread_safety(self):
        """Test thread safety of cache."""
        call_count = [0]
        lock = threading.Lock()

        @single_item_cache
        def slow_func(x):
            with lock:
                call_count[0] += 1
            time.sleep(0.01)  # Simulate slow operation
            return x * 2

        results = []
        threads = []

        def worker(value):
            result = slow_func(value)
            results.append(result)

        # Multiple threads calling with same argument
        for i in range(5):
            t = threading.Thread(target=worker, args=(42,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All results should be correct
        assert all(r == 84 for r in results)
        # Function should be called only a few times (not 5)
        assert call_count[0] < 5


class TestLruCacheWithExpiry:
    """Test lru_cache_with_expiry decorator."""

    def test_basic_caching(self):
        """Test basic caching without expiry."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=32, ttl=None)
        def func(x):
            call_count[0] += 1
            return x * 2

        result1 = func(5)
        assert result1 == 10
        assert call_count[0] == 1

        result2 = func(5)
        assert result2 == 10
        assert call_count[0] == 1  # Cached

    def test_different_arguments(self):
        """Test different arguments."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=32)
        def func(x, y):
            call_count[0] += 1
            return x + y

        func(1, 2)
        assert call_count[0] == 1

        func(1, 2)
        assert call_count[0] == 1  # Cached

        func(2, 3)
        assert call_count[0] == 2  # Different args

    def test_ttl_expiration(self):
        """Test TTL expiration."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=32, ttl=0.1)
        def func(x):
            call_count[0] += 1
            return x * 2

        # First call
        result1 = func(5)
        assert result1 == 10
        assert call_count[0] == 1

        # Immediate second call (not expired)
        result2 = func(5)
        assert result2 == 10
        assert call_count[0] == 1

        # Wait for expiration
        time.sleep(0.15)

        # Call after expiration
        result3 = func(5)
        assert result3 == 10
        assert call_count[0] == 2  # Called again

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=3, ttl=None)
        def func(x):
            call_count[0] += 1
            return x * 2

        # Fill cache
        func(1)  # call_count = 1
        func(2)  # call_count = 2
        func(3)  # call_count = 3

        # Cache is full, add another (should evict 1)
        func(4)  # call_count = 4

        # Now calling 1 again should not use cache (was evicted)
        func(1)  # call_count = 5
        assert call_count[0] == 5

    def test_cache_info(self):
        """Test cache_info method."""

        @lru_cache_with_expiry(maxsize=32, ttl=None)
        def func(x):
            return x * 2

        func(1)
        func(1)  # hit
        func(2)
        func(2)  # hit
        func(3)

        info = func.cache_info()
        assert info["hits"] == 2
        assert info["misses"] == 3
        assert info["size"] == 3
        assert info["maxsize"] == 32

    def test_cache_clear(self):
        """Test cache_clear method."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=32)
        def func(x):
            call_count[0] += 1
            return x * 2

        func(5)
        assert call_count[0] == 1

        func(5)
        assert call_count[0] == 1  # Cached

        # Clear cache
        func.cache_clear()

        func(5)
        assert call_count[0] == 2

    def test_kwargs_caching(self):
        """Test caching with keyword arguments."""
        call_count = [0]

        @lru_cache_with_expiry(maxsize=32)
        def func(a, b=2):
            call_count[0] += 1
            return a * b

        func(5, b=3)
        assert call_count[0] == 1

        func(5, b=3)
        assert call_count[0] == 1  # Cached

        func(5, b=4)
        assert call_count[0] == 2  # Different kwargs


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
