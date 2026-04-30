"""Shared test fixtures and configuration for draken vector tests."""

import pytest
import pyarrow as pa
from draken.encoding import (
    DRAKEN_ENCODING_DENSE,
    DRAKEN_ENCODING_DICTIONARY,
    DRAKEN_ENCODING_RLE,
    DRAKEN_ENCODING_CONSTANT,
)

# Export encoding constants for test matrix
DENSE = DRAKEN_ENCODING_DENSE
DICTIONARY = DRAKEN_ENCODING_DICTIONARY
RLE = DRAKEN_ENCODING_RLE
CONSTANT = DRAKEN_ENCODING_CONSTANT

# Encoding names for readable test output
ENCODING_NAMES = {
    DENSE: "DENSE",
    DICTIONARY: "DICTIONARY",
    RLE: "RLE",
    CONSTANT: "CONSTANT",
}

# All encodings
ALL_ENCODINGS = [DENSE, RLE, CONSTANT, DICTIONARY]


@pytest.fixture
def encoding_constants():
    """Provide encoding constants to tests."""
    return {
        "DENSE": DENSE,
        "RLE": RLE,
        "CONSTANT": CONSTANT,
        "DICTIONARY": DICTIONARY,
    }
