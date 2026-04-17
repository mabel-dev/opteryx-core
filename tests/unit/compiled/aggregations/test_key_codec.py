import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from opteryx.compiled.aggregations.key_codec import deserialize_key_components
from opteryx.compiled.aggregations.key_codec import serialize_key_components


def test_key_codec_roundtrip():
    payload = [None, True, False, 42, -7, 1.5, "north", b"south"]

    encoded = serialize_key_components(payload)
    decoded = deserialize_key_components(encoded)

    assert decoded == [None, True, False, 42, -7, 1.5, b"north", b"south"]
