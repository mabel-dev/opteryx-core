"""
Test the valkey cache by executing the same query twice. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely
'hit' the cache.
"""

import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests import is_arm, is_mac, is_windows, skip_if

def test_invalid_config():
    from opteryx.managers.kvstores import ValkeyCache

    with pytest.raises(Exception):
        ValkeyCache(server="")

    v = ValkeyCache(server=None)
    assert v._consecutive_failures == 10

@skip_if(is_arm() or is_windows() or is_mac())
def test_skip_on_error():
    from opteryx.managers.kvstores import ValkeyCache
    cache = ValkeyCache()
    cache.set(b"key", b"value")
    read_back = cache.get(b"key")
    assert read_back == b"value", read_back
    assert cache.hits > 0
    cache._consecutive_failures = 10
    assert cache.get(b"key") is None


@skip_if(is_arm() or is_windows() or is_mac())
def test_valkey_delete():
    from opteryx.managers.kvstores import ValkeyCache

    cache = ValkeyCache()
    cache.delete(b"key")
    cache.set(b"key", b"value")
    assert cache.get(b"key") == b"value"
    cache.delete(b"key")
    assert cache.get(b"key") is None

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
