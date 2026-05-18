"""Test matrix defining vector type/encoding combinations and operations."""

import pyarrow as pa
from conftest import DENSE, RLE, CONSTANT, DICTIONARY

# Vector type inventory with metadata
VECTOR_TYPES = {
    "int64": {
        "vector_class": "Integer64Vector",
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
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
        "is_numeric": False,
        "is_temporal": True,
        "sample_values": [0, 1000000, -1000000, None],
        "sample_constant": 1000000,
    },
    "time": {
        "vector_class": "TimeVector",
        "arrow_type": pa.time64("us"),
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
        "is_numeric": False,
        "is_temporal": True,
        "sample_values": [0, 3600000000, 1800000000, None],
        "sample_constant": 3600000000,
    },
    "decimal": {
        "vector_class": "DecimalVector",
        "arrow_type": pa.decimal128(10, 2),
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
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
    # Additional vector operations
    "copy": {
        "skip_if_not": None,
    },
    "slice": {
        "skip_if_not": None,
    },
    "count": {  # Non-null count (distinct from null_count)
        "skip_if_not": None,
    },
    "unique": {
        "skip_if_not": None,
    },
    "distinct_count": {
        "skip_if_not": None,
    },
    "any": {  # Any non-null
        "skip_if_not": None,
    },
    "all": {  # All non-null
        "skip_if_not": None,
    },
}

# Primary test matrix: single-type operations
# Generated programmatically from VECTOR_TYPES and OPERATIONS
# This ensures we test ALL supported type/encoding/operation combinations
TEST_MATRIX_OPERATIONS = [
    (type_name, encoding, operation_name)
    for type_name, type_info in VECTOR_TYPES.items()
    for encoding in type_info["supports_encodings"]
    for operation_name, op_info in OPERATIONS.items()
    # Skip operations that don't apply to this type
    if not (
        op_info.get("skip_if_not") == "numeric" and not type_info["is_numeric"]
        or op_info.get("skip_if_not") == "temporal" and not type_info["is_temporal"]
    )
]

from _matrix_generator import generate_matrix_as_list
TEST_MATRIX_COMPARISONS = generate_matrix_as_list()

# Edge case variants for operations
# Each (type, encoding, operation) is tested with different vector conditions
EDGE_CASE_VARIANTS = {
    "standard": {"nullable": False, "size": 100, "description": "100-element vector"},
    "empty": {"nullable": False, "size": 0, "description": "empty vector"},
    "all_null": {"nullable": True, "size": 100, "all_null": True, "description": "all-null 100-element vector"},
    "single_element": {"nullable": False, "size": 1, "description": "single-element vector"},
    "boundary_values": {"nullable": False, "size": 100, "use_boundaries": True, "description": "min/max boundary values"},
}

# Generate edge case test variants
# For each (type, encoding, operation), create variants with different data conditions
TEST_MATRIX_OPERATIONS_WITH_VARIANTS = []
for type_name, encoding, operation_name in TEST_MATRIX_OPERATIONS:
    # Only test edge cases for operations that are relevant
    if operation_name in {"from_arrow", "to_arrow", "to_pylist", "min", "max", "sum"}:
        for variant_name, variant_config in EDGE_CASE_VARIANTS.items():
            TEST_MATRIX_OPERATIONS_WITH_VARIANTS.append(
                (type_name, encoding, operation_name, variant_name, variant_config)
            )
    else:
        # For other operations, just test standard
        TEST_MATRIX_OPERATIONS_WITH_VARIANTS.append(
            (type_name, encoding, operation_name, "standard", EDGE_CASE_VARIANTS["standard"])
        )



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

# Generate comparison variants with edge cases
COMPARISON_VARIANTS = {
    "standard": {"nullable": False, "size": 100, "description": "100-element vectors"},
    "empty": {"nullable": False, "size": 0, "description": "empty vectors"},
    "all_null": {"nullable": True, "size": 100, "all_null": True, "description": "all-null vectors"},
    "single_element": {"nullable": False, "size": 1, "description": "single-element vectors"},
    "mixed_nulls": {"nullable": True, "size": 100, "null_ratio": 0.5, "description": "50% null vectors"},
}

# Generate comparison test variants
TEST_MATRIX_COMPARISONS_WITH_VARIANTS = []
for left_type, left_encoding, right_type, right_encoding, operation_name in TEST_MATRIX_COMPARISONS:
    # Test comparisons with different vector conditions
    for variant_name, variant_config in COMPARISON_VARIANTS.items():
        TEST_MATRIX_COMPARISONS_WITH_VARIANTS.append(
            (left_type, left_encoding, right_type, right_encoding, operation_name, variant_name, variant_config)
        )

# Encoding-specific operation tests
# Test how operations behave with encoding transitions and mixed encodings
ENCODING_OPERATION_TESTS = []
for type_name, type_info in VECTOR_TYPES.items():
    encodings = type_info["supports_encodings"]
    if len(encodings) >= 2:
        # For types that support multiple encodings, test encoding conversions/interactions
        for enc1 in encodings:
            for enc2 in encodings:
                if enc1 != enc2:
                    # Test mixed-encoding operations
                    for operation_name in ["equals", "length", "null_count"]:
                        ENCODING_OPERATION_TESTS.append(
                            (type_name, enc1, type_name, enc2, operation_name, "mixed_encodings")
                        )

print(f"Loaded test matrix with {len(TEST_MATRIX_OPERATIONS)} operation tests")
print(f"Loaded {len(TEST_MATRIX_OPERATIONS_WITH_VARIANTS)} operation tests (with variants)")
print(f"Loaded {len(TEST_MATRIX_COMPARISONS)} cross-type comparison tests")
print(f"Loaded {len(TEST_MATRIX_COMPARISONS_WITH_VARIANTS)} comparison tests (with variants)")
print(f"Loaded {len(ENCODING_OPERATION_TESTS)} encoding-specific operation tests")
print(f"Vector types: {list(VECTOR_TYPES.keys())}")
print(f"Coverage: {len(set((t, e) for t, e, _ in TEST_MATRIX_OPERATIONS))} type/encoding combinations")

# Total test count estimate
total_tests = len(TEST_MATRIX_OPERATIONS_WITH_VARIANTS) + len(TEST_MATRIX_COMPARISONS_WITH_VARIANTS) + len(ENCODING_OPERATION_TESTS) + 29
print(f"\n{'='*60}")
print(f"TOTAL TESTS: ~{total_tests}")
print(f"{'='*60}")
