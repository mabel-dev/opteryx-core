from opteryx.connectors.parquet_io.cache import InMemoryParquetCache


def test_inmemory_parquet_cache_stores_footer_metadata():
    cache = InMemoryParquetCache()
    path = "/path/to/file.parquet"
    metadata = {"row_groups": [{"columns": [{"name": "id"}]}], "__footer_bytes__": 123}

    assert cache.get_footer(path) is None
    cache.set_footer(path, metadata)
    assert cache.get_footer(path) is metadata
    assert cache.stats()["footer_entries"] == 1


def test_inmemory_parquet_cache_can_clear():
    cache = InMemoryParquetCache()
    cache.set_footer("file.parquet", {"row_groups": []})
    cache.set_column("file.parquet", 0, "id", b"decoded")

    assert cache.get_footer("file.parquet") is not None
    assert cache.get_column("file.parquet", 0, "id") == b"decoded"

    cache.clear()
    assert cache.get_footer("file.parquet") is None
    assert cache.get_column("file.parquet", 0, "id") is None
    assert cache.stats()["footer_entries"] == 0
    assert cache.stats()["column_entries"] == 0
