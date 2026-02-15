import re
import opteryx.third_party.cyan4973.xxhash as xxh


def test_xxhash_compiled_backend():
    backend = xxh.get_compiled_xxhash_vector()
    assert isinstance(backend, str)
    assert re.match(r'^(avx2|neon|scalar)$', backend)
