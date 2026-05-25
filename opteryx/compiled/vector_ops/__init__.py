"""Package init for compiled vector ops."""

from .vector_ops import *  # noqa: F401,F403
from opteryx.compiled.nanobind.vector_bitwise import (  # noqa: F401
    vector_bitwise_and,
    vector_bitwise_not,
    vector_bitwise_or,
    vector_bitwise_shift_left,
    vector_bitwise_shift_right,
    vector_bitwise_xor,
)
from opteryx.compiled.nanobind.vector_hash_codec import (  # noqa: F401
    vector_md5,
    vector_sha1,
    vector_sha256,
    vector_sha512,
)


def vector_ltrim(vec):  # noqa: F401
    raise NotImplementedError("vector_ltrim not yet ported to DrakenVector API")


def vector_rtrim(vec):  # noqa: F401
    raise NotImplementedError("vector_rtrim not yet ported to DrakenVector API")


def vector_trim(vec):  # noqa: F401
    raise NotImplementedError("vector_trim not yet ported to DrakenVector API")
