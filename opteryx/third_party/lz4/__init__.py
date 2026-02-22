"""Minimal LZ4 block codec wrapper."""

from opteryx.third_party.lz4.lz4 import compress_block
from opteryx.third_party.lz4.lz4 import compress_bound
from opteryx.third_party.lz4.lz4 import decompress_block
from opteryx.third_party.lz4.lz4 import is_available

__all__ = ("compress_bound", "compress_block", "decompress_block", "is_available")
