"""Shared test fixtures and configuration for draken vector tests."""

import pytest
import pyarrow as pa
from draken.encoding import (
    DRAKEN_ENCODING_DENSE,
    DRAKEN_ENCODING_DICTIONARY,
    DRAKEN_ENCODING_CONSTANT,
)

# Export encoding constants for test matrix
DENSE = DRAKEN_ENCODING_DENSE
DICTIONARY = DRAKEN_ENCODING_DICTIONARY
CONSTANT = DRAKEN_ENCODING_CONSTANT

# Encoding names for readable test output
ENCODING_NAMES = {
    DENSE: "DENSE",
    DICTIONARY: "DICTIONARY",
    CONSTANT: "CONSTANT",
}

# All encodings
ALL_ENCODINGS = [DENSE, CONSTANT, DICTIONARY]


@pytest.fixture
def encoding_constants():
    """Provide encoding constants to tests."""
    return {
        "DENSE": DENSE,
        "CONSTANT": CONSTANT,
        "DICTIONARY": DICTIONARY,
    }
