"""
Benchmark string sort algorithms on realistic data.
Uses ORDER BY queries to exercise the sort path.
"""
import time
import pytest
import opteryx


@pytest.fixture
def session():
    return opteryx.session()


def timeit_query(session, sql, repeats=3):
    """Execute a query multiple times, return best time."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = session.execute_to_morsels(sql)
        # Consume the result
        for _ in result:
            pass
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return min(times)


class TestStringSortBenchmark:
    """Benchmark string sort on realistic SQL patterns."""

    def test_order_by_url_ascending(self, session):
        """Sort by URL column (long strings, common prefixes)."""
        # Small dataset for quick feedback
        sql = "SELECT URL FROM testdata.clickbench_tiny ORDER BY URL ASC"
        t = timeit_query(session, sql, repeats=2)
        print(f"\nURL ASC (tiny): {t*1000:.2f} ms")
        assert t < 10.0  # sanity bound

    def test_order_by_url_descending(self, session):
        """Sort by URL column descending."""
        sql = "SELECT URL FROM testdata.clickbench_tiny ORDER BY URL DESC"
        t = timeit_query(session, sql, repeats=2)
        print(f"URL DESC (tiny): {t*1000:.2f} ms")
        assert t < 10.0

    def test_order_by_title(self, session):
        """Sort by Title column (short human text)."""
        sql = "SELECT Title FROM testdata.clickbench_tiny ORDER BY Title ASC"
        t = timeit_query(session, sql, repeats=2)
        print(f"Title ASC (tiny): {t*1000:.2f} ms")
        assert t < 10.0

    def test_order_by_referer(self, session):
        """Sort by Referer column (URL-like, shorter)."""
        sql = "SELECT Referer FROM testdata.clickbench_tiny ORDER BY Referer ASC"
        t = timeit_query(session, sql, repeats=2)
        print(f"Referer ASC (tiny): {t*1000:.2f} ms")
        assert t < 10.0

    def test_order_by_useragent(self, session):
        """Sort by UserAgent column (repetitive prefixes)."""
        sql = "SELECT UserAgent FROM testdata.clickbench_tiny ORDER BY UserAgent ASC"
        t = timeit_query(session, sql, repeats=2)
        print(f"UserAgent ASC (tiny): {t*1000:.2f} ms")
        assert t < 10.0

    def test_order_by_multiple_strings(self, session):
        """Multi-column sort: strings then numeric."""
        sql = "SELECT URL, Referer, CounterID FROM testdata.clickbench_tiny ORDER BY URL ASC, Referer ASC"
        t = timeit_query(session, sql, repeats=2)
        print(f"Multi-col sort (tiny): {t*1000:.2f} ms")
        assert t < 10.0

    def test_order_by_with_limit(self, session):
        """ORDER BY with LIMIT (top-K pattern)."""
        sql = "SELECT URL FROM testdata.clickbench_tiny ORDER BY URL ASC LIMIT 100"
        t = timeit_query(session, sql, repeats=2)
        print(f"ORDER BY LIMIT (tiny): {t*1000:.2f} ms")
        assert t < 5.0
