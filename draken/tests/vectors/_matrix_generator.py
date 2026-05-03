"""
Generate 5D test matrix for vector comparisons based on operator_map.py.

Dimensions:
  1. Left type (8 types)
  2. Right type (8 types)
  3. Operator (comparison ops)
  4. Left encoding (4 encodings)
  5. Right encoding (4 encodings)

Only generates VALID combinations from operator_map.py (source of truth).
Output: ~200-300 test cases (filtered from theoretical 4000+).
"""

import re
import os

try:
    from conftest import DENSE, RLE, CONSTANT, DICTIONARY
except ImportError:
    from draken.tests.vectors.conftest import DENSE, RLE, CONSTANT, DICTIONARY

# Vector type definitions
VECTOR_TYPES = {
    "int64": {
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
    },
    "float64": {
        "supports_encodings": [DENSE, RLE, CONSTANT],
    },
    "string": {
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
    },
    "bool": {
        "supports_encodings": [DENSE, RLE, CONSTANT],
    },
    "date32": {
        "supports_encodings": [DENSE, RLE, CONSTANT],
    },
    "timestamp": {
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
    },
    "time": {
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
    },
    "decimal": {
        "supports_encodings": [DENSE, RLE, CONSTANT, DICTIONARY],
    },
}


def extract_valid_comparisons_from_operator_map():
    """Extract valid comparison combinations from operator_map.py.

    Returns:
        Set of (left_type, right_type, operator) tuples
    """
    # Find operator_map.py
    operator_map_path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..",
        "opteryx", "planner", "binder", "operator_map.py"
    )

    if not os.path.exists(operator_map_path):
        # Fallback: build from hardcoded list (for when path doesn't exist)
        return _get_hardcoded_valid_comparisons()

    # Read operator_map.py
    with open(operator_map_path) as f:
        content = f.read()

    # Map OrsoTypes to draken vector types
    orso_to_draken = {
        "INTEGER": "int64",
        "DOUBLE": "float64",
        "VARCHAR": "string",
        "BLOB": "string",
        "DATE": "date32",
        "TIMESTAMP": "timestamp",
        "TIME": "time",
        "DECIMAL": "decimal",
        "BOOLEAN": "bool",
    }

    # Comparison operators we care about
    comparison_ops = {"Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq"}
    op_map = {
        "Eq": "equals",
        "NotEq": "not_equals",
        "Gt": "greater_than",
        "GtEq": "greater_equal",
        "Lt": "less_than",
        "LtEq": "less_equal",
    }

    # Extract all (type1, type2, operator) tuples from operator_map
    pattern = r'\(OrsoTypes\.([A-Z_]+),\s*OrsoTypes\.([A-Z_]+),\s*"([^"]+)"\)'
    matches = re.findall(pattern, content)

    valid_comparisons = set()
    for type1, type2, op in matches:
        if op not in comparison_ops:
            continue

        draken_type1 = orso_to_draken.get(type1)
        draken_type2 = orso_to_draken.get(type2)

        if draken_type1 and draken_type2:
            draken_op = op_map[op]
            valid_comparisons.add((draken_type1, draken_type2, draken_op))

    return valid_comparisons


def _get_hardcoded_valid_comparisons():
    """Fallback hardcoded list of valid comparisons."""
    # This is extracted from operator_map.py and manually verified
    return {
        # bool
        ("bool", "bool", "equals"),
        ("bool", "bool", "not_equals"),
        # date32
        ("date32", "date32", "equals"),
        ("date32", "date32", "not_equals"),
        ("date32", "date32", "less_than"),
        ("date32", "date32", "less_equal"),
        ("date32", "date32", "greater_than"),
        ("date32", "date32", "greater_equal"),
        ("date32", "int64", "equals"),
        ("date32", "int64", "not_equals"),
        ("date32", "int64", "less_than"),
        ("date32", "int64", "less_equal"),
        ("date32", "int64", "greater_than"),
        ("date32", "int64", "greater_equal"),
        ("date32", "timestamp", "equals"),
        ("date32", "timestamp", "not_equals"),
        ("date32", "timestamp", "less_than"),
        ("date32", "timestamp", "less_equal"),
        ("date32", "timestamp", "greater_than"),
        ("date32", "timestamp", "greater_equal"),
        # decimal
        ("decimal", "decimal", "equals"),
        ("decimal", "decimal", "not_equals"),
        ("decimal", "decimal", "less_than"),
        ("decimal", "decimal", "less_equal"),
        ("decimal", "decimal", "greater_than"),
        ("decimal", "decimal", "greater_equal"),
        ("decimal", "float64", "equals"),
        ("decimal", "float64", "not_equals"),
        ("decimal", "float64", "less_than"),
        ("decimal", "float64", "less_equal"),
        ("decimal", "float64", "greater_than"),
        ("decimal", "float64", "greater_equal"),
        ("decimal", "int64", "equals"),
        ("decimal", "int64", "not_equals"),
        ("decimal", "int64", "less_than"),
        ("decimal", "int64", "less_equal"),
        ("decimal", "int64", "greater_than"),
        ("decimal", "int64", "greater_equal"),
        # float64
        ("float64", "decimal", "equals"),
        ("float64", "decimal", "not_equals"),
        ("float64", "decimal", "less_than"),
        ("float64", "decimal", "less_equal"),
        ("float64", "decimal", "greater_than"),
        ("float64", "decimal", "greater_equal"),
        ("float64", "float64", "equals"),
        ("float64", "float64", "not_equals"),
        ("float64", "float64", "less_than"),
        ("float64", "float64", "less_equal"),
        ("float64", "float64", "greater_than"),
        ("float64", "float64", "greater_equal"),
        ("float64", "int64", "equals"),
        ("float64", "int64", "not_equals"),
        ("float64", "int64", "less_than"),
        ("float64", "int64", "less_equal"),
        ("float64", "int64", "greater_than"),
        ("float64", "int64", "greater_equal"),
        # int64
        ("int64", "date32", "equals"),
        ("int64", "date32", "not_equals"),
        ("int64", "date32", "less_than"),
        ("int64", "date32", "less_equal"),
        ("int64", "date32", "greater_than"),
        ("int64", "date32", "greater_equal"),
        ("int64", "decimal", "equals"),
        ("int64", "decimal", "not_equals"),
        ("int64", "decimal", "less_than"),
        ("int64", "decimal", "less_equal"),
        ("int64", "decimal", "greater_than"),
        ("int64", "decimal", "greater_equal"),
        ("int64", "float64", "equals"),
        ("int64", "float64", "not_equals"),
        ("int64", "float64", "less_than"),
        ("int64", "float64", "less_equal"),
        ("int64", "float64", "greater_than"),
        ("int64", "float64", "greater_equal"),
        ("int64", "int64", "equals"),
        ("int64", "int64", "not_equals"),
        ("int64", "int64", "less_than"),
        ("int64", "int64", "less_equal"),
        ("int64", "int64", "greater_than"),
        ("int64", "int64", "greater_equal"),
        ("int64", "timestamp", "equals"),
        ("int64", "timestamp", "not_equals"),
        ("int64", "timestamp", "less_than"),
        ("int64", "timestamp", "less_equal"),
        ("int64", "timestamp", "greater_than"),
        ("int64", "timestamp", "greater_equal"),
        # string
        ("string", "string", "equals"),
        ("string", "string", "not_equals"),
        ("string", "string", "less_than"),
        ("string", "string", "less_equal"),
        ("string", "string", "greater_than"),
        ("string", "string", "greater_equal"),
        # timestamp
        ("timestamp", "date32", "equals"),
        ("timestamp", "date32", "not_equals"),
        ("timestamp", "date32", "less_than"),
        ("timestamp", "date32", "less_equal"),
        ("timestamp", "date32", "greater_than"),
        ("timestamp", "date32", "greater_equal"),
        ("timestamp", "int64", "equals"),
        ("timestamp", "int64", "not_equals"),
        ("timestamp", "int64", "less_than"),
        ("timestamp", "int64", "less_equal"),
        ("timestamp", "int64", "greater_than"),
        ("timestamp", "int64", "greater_equal"),
        ("timestamp", "timestamp", "equals"),
        ("timestamp", "timestamp", "not_equals"),
        ("timestamp", "timestamp", "less_than"),
        ("timestamp", "timestamp", "less_equal"),
        ("timestamp", "timestamp", "greater_than"),
        ("timestamp", "timestamp", "greater_equal"),
    }

# Load valid comparisons from operator_map.py at module load time
VALID_COMPARISONS = extract_valid_comparisons_from_operator_map()


def generate_matrix():
    """Generate all valid 5D test combinations."""
    test_cases = []
    type_pairs = {}
    operator_counts = {}

    for left_type, right_type, op_name in VALID_COMPARISONS:
        left_info = VECTOR_TYPES[left_type]
        right_info = VECTOR_TYPES[right_type]

        type_pair_key = (left_type, right_type)
        if type_pair_key not in type_pairs:
            type_pairs[type_pair_key] = 0

        if op_name not in operator_counts:
            operator_counts[op_name] = 0

        # Iterate all encoding combinations
        for left_enc in left_info["supports_encodings"]:
            for right_enc in right_info["supports_encodings"]:
                test_cases.append((left_type, left_enc, right_type, right_enc, op_name))
                type_pairs[type_pair_key] += 1
                operator_counts[op_name] += 1

    return test_cases, type_pairs, operator_counts


def generate_matrix_as_list():
    """Generate matrix as list of tuples for TEST_MATRIX_COMPARISONS."""
    test_cases, _, _ = generate_matrix()
    return test_cases


def print_matrix_stats():
    """Print statistics about the generated matrix."""
    test_cases, type_pairs, operator_counts = generate_matrix()

    print("=" * 80)
    print("FILTERED TEST MATRIX GENERATION REPORT")
    print("=" * 80)

    print(f"\nTotal test cases: {len(test_cases)}")
    print(f"Total type pairs: {len(type_pairs)}")
    print(f"Total operators: {len(operator_counts)}")

    print("\n" + "=" * 80)
    print("BREAKDOWN BY OPERATOR")
    print("=" * 80)
    for op in sorted(operator_counts.keys()):
        count = operator_counts[op]
        print(f"  {op:20} {count:4} test cases")

    print("\n" + "=" * 80)
    print("BREAKDOWN BY TYPE PAIR")
    print("=" * 80)

    for left, right in sorted(type_pairs.keys()):
        count = type_pairs[(left, right)]
        print(f"  {left:12} vs {right:12}  {count:4} test cases")

    print("\n" + "=" * 80)
    print("ENCODING COMBINATIONS TESTED")
    print("=" * 80)

    encoding_names = {0: "DENSE", 2: "RLE", 3: "CONSTANT", 1: "DICTIONARY"}

    # Count unique (left_enc, right_enc) pairs
    encoding_pairs = set((tc[1], tc[3]) for tc in test_cases)
    for left_enc, right_enc in sorted(encoding_pairs):
        left_name = encoding_names.get(left_enc, str(left_enc))
        right_name = encoding_names.get(right_enc, str(right_enc))
        count = len([tc for tc in test_cases if tc[1] == left_enc and tc[3] == right_enc])
        print(f"  {left_name:12} vs {right_name:12}  {count:4} test cases")


if __name__ == "__main__":
    print_matrix_stats()
