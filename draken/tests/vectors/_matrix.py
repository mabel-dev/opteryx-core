"""Test matrix defining vector type/encoding combinations and operations."""

import pyarrow as pa
from conftest import DENSE, RLE, CONSTANT, DICTIONARY

# Vector type inventory with metadata
VECTOR_TYPES = {
    "int64": {
        "vector_class": "Int64Vector",
        "arrow_type": pa.int64(),
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
        "is_numeric": True,
        "is_temporal": False,
        "sample_values": [0, -5, 100, None],
        "sample_constant": 42,
    },
    "float64": {
        "vector_class": "Float64Vector",
        "arrow_type": pa.float64(),
        "supports_encodings": [DENSE, RLE, CONSTANT],
        "is_numeric": True,
        "is_temporal": False,
        "sample_values": [0.0, -1.5, 3.14, float("inf"), None],
        "sample_constant": 2.71,
    },
    "string": {
        "vector_class": "StringVector",
        "arrow_type": pa.string(),
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
        "is_numeric": False,
        "is_temporal": False,
        "sample_values": ["hello", "world", "", None],
        "sample_constant": "test",
    },
    "bool": {
        "vector_class": "BoolVector",
        "arrow_type": pa.bool_(),
        "supports_encodings": [DENSE, RLE, CONSTANT],
        "is_numeric": False,
        "is_temporal": False,
        "sample_values": [True, False, None],
        "sample_constant": True,
    },
    "date32": {
        "vector_class": "Date32Vector",
        "arrow_type": pa.date32(),
        "supports_encodings": [DENSE, RLE, CONSTANT],
        "is_numeric": False,
        "is_temporal": True,
        "sample_values": [0, 10000, -10000, None],
        "sample_constant": 18000,
    },
    "timestamp": {
        "vector_class": "TimestampVector",
        "arrow_type": pa.timestamp("us"),
        "supports_encodings": [DENSE, CONSTANT],
        "is_numeric": False,
        "is_temporal": True,
        "sample_values": [0, 1000000, -1000000, None],
        "sample_constant": 1000000,
    },
    "time": {
        "vector_class": "TimeVector",
        "arrow_type": pa.time64("us"),
        "supports_encodings": [DENSE, CONSTANT],
        "is_numeric": False,
        "is_temporal": True,
        "sample_values": [0, 3600000000, 1800000000, None],
        "sample_constant": 3600000000,
    },
    "decimal": {
        "vector_class": "DecimalVector",
        "arrow_type": pa.decimal128(10, 2),
        "supports_encodings": [DENSE],
        "is_numeric": True,
        "is_temporal": False,
        "sample_values": [0, 100, -50, None],
        "sample_constant": 42,
    },
}

# Operations that can be tested
OPERATIONS = {
    # Creation/conversion
    "from_arrow": {
        "skip_if_not": None,  # All types support this
        "nullable": True,
    },
    "to_arrow": {
        "skip_if_not": None,
        "nullable": True,
    },
    "to_pylist": {
        "skip_if_not": None,
        "nullable": True,
    },
    # Aggregations (numeric only)
    "sum": {
        "skip_if_not": "numeric",
    },
    "min": {
        "skip_if_not": None,  # All types support min
        "requires_non_empty": True,
    },
    "max": {
        "skip_if_not": None,  # All types support max
        "requires_non_empty": True,
    },
    # Transformations
    "take": {
        "skip_if_not": None,
        "requires_non_empty": False,
    },
    "equals": {
        "skip_if_not": None,
        "nullable": True,
    },
    # Iteration
    "length": {
        "skip_if_not": None,
    },
    "null_count": {
        "skip_if_not": None,
    },
    "is_null": {
        "skip_if_not": None,
    },
    "subscript": {  # vec[i]
        "skip_if_not": None,
        "requires_non_empty": True,
    },
}

# Primary test matrix: single-type operations
# Each entry is (type_name, encoding, operation)
TEST_MATRIX_OPERATIONS = [
    # Int64 - all encodings
    ("int64", DENSE, "from_arrow"),
    ("int64", DENSE, "to_arrow"),
    ("int64", DENSE, "sum"),
    ("int64", DENSE, "min"),
    ("int64", DENSE, "max"),
    ("int64", DENSE, "take"),
    ("int64", DENSE, "equals"),
    ("int64", DENSE, "length"),
    ("int64", DENSE, "null_count"),
    ("int64", DENSE, "is_null"),
    ("int64", DENSE, "subscript"),

    ("int64", RLE, "from_arrow"),
    ("int64", RLE, "to_arrow"),
    ("int64", RLE, "sum"),
    ("int64", RLE, "min"),
    ("int64", RLE, "max"),
    ("int64", RLE, "take"),

    ("int64", CONSTANT, "from_arrow"),
    ("int64", CONSTANT, "to_arrow"),
    ("int64", CONSTANT, "sum"),
    ("int64", CONSTANT, "min"),

    ("int64", DICTIONARY, "from_arrow"),
    ("int64", DICTIONARY, "to_arrow"),
    ("int64", DICTIONARY, "equals"),

    # Float64 - dense, RLE, constant
    ("float64", DENSE, "from_arrow"),
    ("float64", DENSE, "to_arrow"),
    ("float64", DENSE, "sum"),
    ("float64", DENSE, "min"),
    ("float64", DENSE, "max"),
    ("float64", DENSE, "take"),
    ("float64", DENSE, "equals"),

    ("float64", RLE, "from_arrow"),
    ("float64", RLE, "sum"),
    ("float64", RLE, "min"),

    ("float64", CONSTANT, "from_arrow"),
    ("float64", CONSTANT, "sum"),

    # String - all encodings
    ("string", DENSE, "from_arrow"),
    ("string", DENSE, "to_arrow"),
    ("string", DENSE, "to_pylist"),
    ("string", DENSE, "take"),
    ("string", DENSE, "equals"),
    ("string", DENSE, "length"),
    ("string", DENSE, "null_count"),

    ("string", RLE, "from_arrow"),
    ("string", RLE, "to_arrow"),
    ("string", RLE, "take"),
    ("string", RLE, "equals"),

    ("string", CONSTANT, "from_arrow"),
    ("string", CONSTANT, "to_arrow"),
    ("string", CONSTANT, "equals"),

    ("string", DICTIONARY, "from_arrow"),
    ("string", DICTIONARY, "to_arrow"),
    ("string", DICTIONARY, "equals"),

    # Bool - dense, RLE, constant
    ("bool", DENSE, "from_arrow"),
    ("bool", DENSE, "to_arrow"),
    ("bool", DENSE, "take"),
    ("bool", DENSE, "equals"),

    ("bool", RLE, "from_arrow"),
    ("bool", RLE, "take"),
    ("bool", RLE, "equals"),

    ("bool", CONSTANT, "from_arrow"),
    ("bool", CONSTANT, "to_arrow"),

    # Date32 - dense, RLE, constant
    ("date32", DENSE, "from_arrow"),
    ("date32", DENSE, "to_arrow"),
    ("date32", DENSE, "min"),
    ("date32", DENSE, "max"),
    ("date32", DENSE, "take"),
    ("date32", DENSE, "to_pylist"),

    ("date32", RLE, "from_arrow"),
    ("date32", RLE, "min"),
    ("date32", RLE, "max"),

    ("date32", CONSTANT, "from_arrow"),
    ("date32", CONSTANT, "min"),

    # Timestamp - dense, constant
    ("timestamp", DENSE, "from_arrow"),
    ("timestamp", DENSE, "to_arrow"),
    ("timestamp", DENSE, "to_pylist"),
    ("timestamp", DENSE, "min"),
    ("timestamp", DENSE, "max"),

    ("timestamp", CONSTANT, "from_arrow"),
    ("timestamp", CONSTANT, "to_arrow"),

    # Time - dense, constant
    ("time", DENSE, "from_arrow"),
    ("time", DENSE, "to_arrow"),
    ("time", DENSE, "to_pylist"),
    ("time", DENSE, "take"),

    ("time", CONSTANT, "from_arrow"),
    ("time", CONSTANT, "to_arrow"),
    ("time", CONSTANT, "to_pylist"),

    # Decimal - dense only
    ("decimal", DENSE, "from_arrow"),
    ("decimal", DENSE, "to_arrow"),
]

from _matrix_generator import generate_matrix_as_list
TEST_MATRIX_COMPARISONS = generate_matrix_as_list()






# Comparison operations metadata
COMPARISON_OPERATIONS = {
    "equals": {
        "skip_if_not": None,  # All types support equals
        "requires_non_empty": False,
    },
    "not_equals": {
        "skip_if_not": None,  # All types support not_equals
        "requires_non_empty": False,
    },
    "less_than": {
        "skip_if_not": "orderable",  # Numeric, string, date, timestamp, time, decimal
        "requires_non_empty": False,
    },
    "less_equal": {
        "skip_if_not": "orderable",
        "requires_non_empty": False,
    },
    "greater_than": {
        "skip_if_not": "orderable",
        "requires_non_empty": False,
    },
    "greater_equal": {
        "skip_if_not": "orderable",
        "requires_non_empty": False,
    },
}

print(f"Loaded test matrix with {len(TEST_MATRIX_OPERATIONS)} operation tests")
print(f"Loaded {len(TEST_MATRIX_COMPARISONS)} cross-type comparison tests")
print(f"Vector types: {list(VECTOR_TYPES.keys())}")
print(f"Coverage: {len(set((t, e) for t, e, _ in TEST_MATRIX_OPERATIONS))} type/encoding combinations")
