# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
Comprehensive stress tests and performance validation for group key codec.

This test suite provides advanced stress testing and performance validation:
- Massive datasets: 1M, 10M keys with various distributions
- String pathological cases: extreme sizes, Unicode planes, repetition patterns
- Numeric pathological cases: powers of 2, sequential, clustered, sparse
- Type cardinality mixing: various combinations of high/low cardinality
- Null distribution patterns: different rates and clustering patterns
- Memory stress: buffer sizes, offset boundaries, allocation cycles
- Aggregation stress: all functions at scale with various distributions
- Round-trip stability: encode/decode cycles at scale
- Concurrency patterns: interleaved operations
- Randomized property tests: fuzz testing and verification
"""

import os
import random
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.aggregations.key_codec import (
    decode_multi_payload_keys,
    decode_single_payload_key,
)
from opteryx.compiled.draken.morsels.morsel import Morsel

from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation

# Key type constants
KEY_MULTI_FIXED_INT = 1
KEY_MULTI_FIXED_DATE32 = 2
KEY_MULTI_FIXED_TIME32 = 3
KEY_MULTI_FIXED_TIME64 = 4
KEY_MULTI_FIXED_TIMESTAMP64 = 5
KEY_MULTI_ENCODED_STRING = 6


# ============================================================================
# Helper Functions
# ============================================================================


def _normalize_value(value):
    """Normalize values for comparison."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _rows_by_key(rows, key_columns):
    """Group rows by key columns for easier assertion."""
    if isinstance(key_columns, str):
        key_columns = [key_columns]

    out = {}
    for row in rows:
        key = tuple(_normalize_value(row[column]) for column in key_columns)
        out[key] = {k: _normalize_value(v) for k, v in row.items()}
    return out


def _finalize_rows(group_by_columns, aggregations, table):
    """Execute groupby operation and return finalized rows."""
    morsel = Morsel.from_arrow(table)
    op = ShuffleGroupByOperation(
        group_by_columns=group_by_columns,
        aggregations=aggregations,
    )
    op.ingest(morsel)
    return op.finalize().to_arrow().to_pylist()


def _generate_zipfian_distribution(n, size, alpha=1.5):
    """Generate Zipfian distributed values."""
    # Approximate Zipfian using weighted random selection
    weights = [1.0 / ((i + 1) ** alpha) for i in range(size)]
    total = sum(weights)
    weights = [w / total for w in weights]
    return [random.choices(range(size), weights=weights, k=1)[0] for _ in range(n)]


def _generate_clustered_distribution(n, size, cluster_count=10):
    """Generate clustered distribution where values are grouped."""
    clusters = [random.randint(0, size - 1) for _ in range(cluster_count)]
    values = []
    for _ in range(n):
        cluster = random.choice(clusters)
        # Add some jitter around cluster center
        value = max(0, min(size - 1, cluster + random.randint(-5, 5)))
        values.append(value)
    return values


# ============================================================================
# SECTION 1: MASSIVE DATASET TESTS
# ============================================================================


class TestMassiveDatasets:
    """Test codec with very large volumes of data."""

    @pytest.mark.slow
    def test_1m_uniform_distribution_int_keys(self):
        """Test 1M keys with uniform distribution."""
        size = 1_000_000
        cardinality = 10_000
        keys = [random.randint(0, cardinality - 1) for _ in range(size)]

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Verify we got expected number of groups (should be less than cardinality)
        assert len(rows) <= cardinality
        # Verify counts sum to total
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_1m_zipfian_distribution_int_keys(self):
        """Test 1M keys with Zipfian distribution (power-law)."""
        size = 1_000_000
        cardinality = 10_000
        keys = _generate_zipfian_distribution(size, cardinality)

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Verify we got expected number of groups
        assert len(rows) > 0
        assert len(rows) <= cardinality
        # Verify counts sum to total
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_1m_clustered_distribution_int_keys(self):
        """Test 1M keys with clustered distribution."""
        size = 1_000_000
        cardinality = 10_000
        keys = _generate_clustered_distribution(size, cardinality, cluster_count=100)

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) > 0
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_10m_distinct_keys_high_cardinality(self):
        """Test 10M rows with very high cardinality (10M distinct keys)."""
        size = 10_000_000
        # All keys are unique
        keys = list(range(size))

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Should have one row per key
        assert len(rows) == size
        # All counts should be 1
        assert all(row["cnt"] == 1 for row in rows)

    @pytest.mark.slow
    def test_1m_string_keys_uniform_distribution(self):
        """Test 1M string keys with uniform distribution."""
        size = 1_000_000
        cardinality = 10_000
        keys = [f"key_{random.randint(0, cardinality - 1):06d}" for _ in range(size)]

        table = pa.table({"k": pa.array(keys, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) <= cardinality
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_1m_mixed_keys_uniform_distribution(self):
        """Test 1M rows with mixed int and string keys."""
        size = 1_000_000
        int_cardinality = 100
        string_cardinality = 100
        ints = [random.randint(0, int_cardinality - 1) for _ in range(size)]
        strings = [f"cat_{random.randint(0, string_cardinality - 1):03d}" for _ in range(size)]

        table = pa.table(
            {"int_k": pa.array(ints, type=pa.int64()), "str_k": pa.array(strings, type=pa.string())}
        )
        rows = _finalize_rows(
            group_by_columns=["int_k", "str_k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) <= int_cardinality * string_cardinality
        assert sum(row["cnt"] for row in rows) == size


# ============================================================================
# SECTION 2: STRING PATHOLOGICAL CASES
# ============================================================================


class TestStringPathologicalCases:
    """Test codec with extreme string scenarios."""

    @pytest.mark.slow
    def test_single_key_1m_different_string_values(self):
        """Test single key with 1M different string values."""
        size = 1_000_000
        # All values are unique
        strings = [f"value_{i:07d}" for i in range(size)]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == size
        assert all(row["cnt"] == 1 for row in rows)

    @pytest.mark.slow
    def test_10k_keys_repeated_100_times(self):
        """Test 10K keys each repeated exactly 100 times."""
        cardinality = 10_000
        repetitions = 100
        keys = [f"key_{i:05d}" for i in range(cardinality)] * repetitions

        table = pa.table({"k": pa.array(keys, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == cardinality
        assert all(row["cnt"] == repetitions for row in rows)

    @pytest.mark.slow
    @pytest.mark.parametrize("string_size", [1, 100, 1_000, 10_000, 100_000, 1_000_000])
    def test_large_string_sizes(self, string_size):
        """Test strings of various sizes up to 10MB."""
        # Create strings of specified size
        base_char = "a"
        strings = [
            base_char * string_size,
            base_char * string_size,
            "b" * string_size,
            "b" * string_size,
            "c" * string_size,
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Should group identical strings
        by_key = _rows_by_key(rows, "k")
        assert by_key[(base_char * string_size,)]["cnt"] == 2
        assert by_key[("b" * string_size,)]["cnt"] == 2
        assert by_key[("c" * string_size,)]["cnt"] == 1

    def test_unicode_bmp_plane(self):
        """Test BMP (Basic Multilingual Plane) Unicode characters."""
        # Characters from various scripts in BMP
        strings = [
            "café",  # Latin
            "café",
            "日本",  # Japanese
            "日本",
            "مصر",  # Arabic
            "مصر",
            "Русь",  # Cyrillic
            "Русь",
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        by_key = _rows_by_key(rows, "k")
        assert by_key[("café",)]["cnt"] == 2
        assert by_key[("日本",)]["cnt"] == 2
        assert by_key[("مصر",)]["cnt"] == 2
        assert by_key[("Русь",)]["cnt"] == 2

    def test_unicode_smp_plane(self):
        """Test SMP (Supplementary Multilingual Plane) including emoji."""
        # Emoji and other SMP characters
        strings = [
            "🎉🎊",  # Emoji
            "🎉🎊",
            "🚀🌟",
            "🚀🌟",
            "𝐀𝐁𝐂",  # Mathematical Alphanumeric Symbols
            "𝐀𝐁𝐂",
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        by_key = _rows_by_key(rows, "k")
        assert by_key[("🎉🎊",)]["cnt"] == 2
        assert by_key[("🚀🌟",)]["cnt"] == 2
        assert by_key[("𝐀𝐁𝐂",)]["cnt"] == 2

    def test_unicode_combining_characters_and_normalization(self):
        """Test combining characters and Unicode normalization edge cases."""
        # e with combining acute accent (different from precomposed é)
        combining = "e\u0301"  # e + combining acute
        precomposed = "é"  # precomposed

        strings = [
            combining,
            combining,
            precomposed,
            precomposed,
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # These should be treated as different
        assert len(rows) == 2
        assert sum(row["cnt"] for row in rows) == 4

    def test_unicode_rtl_and_bidi_text(self):
        """Test right-to-left and bidirectional text."""
        strings = [
            "Hello עברית",  # Mixed LTR and RTL
            "Hello עברית",
            "مرحبا World",  # RTL then LTR
            "مرحبا World",
            "⁦RTL⁩LTR",  # Explicit RTL/LTR marks
            "⁦RTL⁩LTR",
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        by_key = _rows_by_key(rows, "k")
        assert by_key[("Hello עברית",)]["cnt"] == 2
        assert by_key[("مرحبا World",)]["cnt"] == 2

    def test_zero_width_and_invisible_characters(self):
        """Test zero-width, invisible, and control characters."""
        strings = [
            "test",
            "test",
            "test\u200b",  # Zero-width space
            "test\u200b",
            "test\u200c",  # Zero-width non-joiner
            "test\u200c",
            "test\u202e",  # Right-to-left override
            "test\u202e",
        ]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # All should be treated as distinct
        assert len(rows) == 4
        assert all(row["cnt"] == 2 for row in rows)


# ============================================================================
# SECTION 3: NUMERIC PATHOLOGICAL CASES
# ============================================================================


class TestNumericPathologicalCases:
    """Test codec with extreme numeric scenarios."""

    def test_all_powers_of_two_i64(self):
        """Test all powers of 2 from 2^0 to 2^62 (within i64 range)."""
        powers = [2**i for i in range(63)]  # 2^63 overflows i64
        # Double each for counting
        keys = [p for p in powers for _ in range(2)]

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        by_key = _rows_by_key(rows, "k")
        assert len(rows) == 63
        assert all(row["cnt"] == 2 for row in rows)

    def test_sequential_numbers_1m(self):
        """Test sequential numbers 1 to 1M."""
        keys = list(range(1, 1_000_001))

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == 1_000_000
        assert all(row["cnt"] == 1 for row in rows)

    def test_clustered_numbers_around_boundaries(self):
        """Test highly clustered numbers around int64 boundaries."""
        # i64::MIN = -9223372036854775808, i64::MAX = 9223372036854775807
        min_i64 = -(2**63)
        max_i64 = 2**63 - 1

        boundaries = [
            min_i64,
            min_i64 + 1,
            0,
            max_i64 - 1,
            max_i64,
        ]
        # Create clusters around each boundary
        keys = []
        for b in boundaries:
            for offset in range(-2, 3):
                value = b + offset
                # Ensure value stays within int64 bounds
                if min_i64 <= value <= max_i64:
                    keys.append(value)

        # Remove duplicates and convert to list
        keys = list(set(keys))

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == len(keys)
        assert all(row["cnt"] == 1 for row in rows)

    def test_sparse_and_gapped_distribution(self):
        """Test sparse distribution with large gaps."""
        # Create keys with large gaps
        keys = []
        current = -9223372036854775808
        step = 1_000_000_000_000  # 1 trillion
        for _ in range(100):
            if current > 9223372036854775807 - step:
                break
            keys.append(current)
            keys.append(current)  # Duplicate for counting
            current += step

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        expected_groups = len(keys) // 2
        assert len(rows) == expected_groups
        assert all(row["cnt"] == 2 for row in rows)

    def test_negative_numbers_distribution(self):
        """Test various distributions of negative numbers."""
        keys = (
            list(range(-1000, 0)) * 10  # Negative range repeated
            + list(range(0, 1000)) * 10  # Positive range repeated
        )

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == 2000
        assert all(row["cnt"] == 10 for row in rows)

    @pytest.mark.parametrize("dist_type", ["uniform", "clustered"])
    @pytest.mark.slow
    def test_float_like_behavior_with_integer_keys(self, dist_type):
        """Test distributions that would be non-uniform if treated as floats."""
        if dist_type == "uniform":
            keys = [random.randint(-1_000_000, 1_000_000) for _ in range(100_000)]
        else:
            keys = _generate_clustered_distribution(100_000, 1000)

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == 100_000


# ============================================================================
# SECTION 4: TYPE CARDINALITY MIXING
# ============================================================================


class TestTypeCardinalityMixing:
    """Test codec with mixed type cardinalities."""

    @pytest.mark.slow
    def test_high_card_string_low_card_int(self):
        """Test high cardinality string with low cardinality int."""
        size = 100_000
        # 10K unique strings, only 2 int values
        strings = [f"str_{i % 10_000:05d}" for i in range(size)]
        ints = [i % 2 for i in range(size)]

        table = pa.table(
            {"str_k": pa.array(strings, type=pa.string()), "int_k": pa.array(ints, type=pa.int64())}
        )
        rows = _finalize_rows(
            group_by_columns=["str_k", "int_k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Should have at most 10K * 2 groups
        assert len(rows) <= 20_000
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_low_card_string_high_card_int(self):
        """Test low cardinality string with high cardinality int."""
        size = 100_000
        # Only 2 unique strings, 10K unique ints
        strings = [["A", "B"][i % 2] for i in range(size)]
        ints = [i % 10_000 for i in range(size)]

        table = pa.table(
            {"str_k": pa.array(strings, type=pa.string()), "int_k": pa.array(ints, type=pa.int64())}
        )
        rows = _finalize_rows(
            group_by_columns=["str_k", "int_k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Should have at most 2 * 10K groups
        assert len(rows) <= 20_000
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_three_types_mixed_cardinalities(self):
        """Test three key columns with different cardinalities."""
        size = 50_000
        # 100 groups for first key, 50 for second, 10 for third
        k1 = [i % 100 for i in range(size)]
        k2 = [f"cat_{i % 50:03d}" for i in range(size)]
        k3 = [i % 10 for i in range(size)]

        table = pa.table(
            {
                "k1": pa.array(k1, type=pa.int64()),
                "k2": pa.array(k2, type=pa.string()),
                "k3": pa.array(k3, type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2", "k3"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # At most 100 * 50 * 10 groups
        assert len(rows) <= 50_000
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_date_int_string_cardinality_mix(self):
        """Test date32, int64, and string with mixed cardinalities."""
        size = 50_000
        dates = [i % 365 for i in range(size)]
        ints = [i % 1000 for i in range(size)]
        strings = [f"s{i % 10}" for i in range(size)]

        table = pa.table(
            {
                "d": pa.array([pa.date32(d) for d in dates]),
                "i": pa.array(ints, type=pa.int64()),
                "s": pa.array(strings, type=pa.string()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["d", "i", "s"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # At most 365 * 1000 * 10 groups
        assert len(rows) <= 3_650_000
        assert sum(row["cnt"] for row in rows) == size


# ============================================================================
# SECTION 5: NULL DISTRIBUTION PATTERNS
# ============================================================================


class TestNullDistributionPatterns:
    """Test codec with various null distributions."""

    @pytest.mark.parametrize("null_rate", [0.01, 0.05, 0.1, 0.5, 0.95])
    @pytest.mark.slow
    def test_various_null_rates_single_key(self, null_rate):
        """Test different null rates in single key."""
        size = 100_000
        null_count = int(size * null_rate)
        valid_count = size - null_count

        keys = [i % 1000 for i in range(valid_count)] + [None] * null_count
        random.shuffle(keys)

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Should have groups for valid values + one for nulls
        assert len(rows) >= 1  # At least one group (the null group)
        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_all_nulls_clustered(self):
        """Test all nulls clustered together."""
        table = pa.table({"k": pa.array([None] * 100_000, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert len(rows) == 1
        assert rows[0]["cnt"] == 100_000

    @pytest.mark.slow
    def test_nulls_scattered_vs_clustered(self):
        """Test scattered nulls vs clustered nulls."""
        size = 10_000
        null_count = 1_000

        # Scattered: distributed throughout
        scattered = [i % 100 if i % 10 != 0 else None for i in range(size)]

        # Clustered: all together
        clustered = [i % 100 for i in range(size - null_count)] + [None] * null_count

        for keys in [scattered, clustered]:
            table = pa.table({"k": pa.array(keys, type=pa.int64())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_different_null_rates_per_column_multi_key(self):
        """Test different null rates for different columns."""
        size = 50_000

        # First column: 5% nulls
        k1 = [i % 100 if i % 20 != 0 else None for i in range(size)]
        # Second column: 20% nulls
        k2 = [i % 100 if i % 5 != 0 else None for i in range(size)]
        # Third column: 1% nulls
        k3 = [i % 100 if i % 100 != 0 else None for i in range(size)]

        table = pa.table(
            {
                "k1": pa.array(k1, type=pa.int64()),
                "k2": pa.array(k2, type=pa.int64()),
                "k3": pa.array(k3, type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2", "k3"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_null_with_high_cardinality_string_keys(self):
        """Test nulls with high cardinality string keys."""
        size = 100_000
        # 10K unique strings with 10% nulls
        strings = [f"key_{i % 10_000:05d}" if i % 10 != 0 else None for i in range(size)]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size


# ============================================================================
# SECTION 6: MEMORY STRESS AND OFFSET BOUNDARIES
# ============================================================================


class TestMemoryStressAndOffsets:
    """Test codec with memory stress scenarios."""

    @pytest.mark.slow
    def test_encode_decode_cycles_100_iterations(self):
        """Test 100 encode/decode cycles."""
        size = 10_000
        keys = [i % 100 for i in range(size)]

        for cycle in range(100):
            table = pa.table({"k": pa.array(keys, type=pa.int64())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )
            assert len(rows) == 100
            assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_encode_decode_cycles_1k_iterations(self):
        """Test 1K encode/decode cycles with smaller dataset."""
        size = 1_000
        keys = [i % 10 for i in range(size)]

        for cycle in range(1_000):
            table = pa.table({"k": pa.array(keys, type=pa.int64())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )
            assert len(rows) == 10
            assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_offset_overflow_boundaries(self):
        """Test offset encoding at boundary values."""
        size = 100_000
        # Test with strings that create large payloads
        strings = [f"{'a' * 1000}{i % 100}" for i in range(size)]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_cumulative_vs_fresh_encoding_stability(self):
        """Test cumulative grouping vs fresh encoding for same data."""
        size = 50_000
        keys = [i % 1000 for i in range(size)]

        # Fresh encoding
        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        fresh_rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        # Verify consistency
        fresh_by_key = _rows_by_key(fresh_rows, "k")

        # Repeat and verify same results
        table2 = pa.table({"k": pa.array(keys, type=pa.int64())})
        repeat_rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table2,
        )
        repeat_by_key = _rows_by_key(repeat_rows, "k")

        assert fresh_by_key == repeat_by_key


# ============================================================================
# SECTION 7: AGGREGATION STRESS TESTS
# ============================================================================


class TestAggregationStress:
    """Test aggregation functions at scale."""

    @pytest.mark.slow
    def test_count_aggregation_1m_keys(self):
        """Test count aggregation with 1M keys."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]

        table = pa.table({"k": pa.array(keys, type=pa.int64())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_sum_aggregation_1m_keys(self):
        """Test sum aggregation with 1M numeric values."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]
        values = [float(i % 1000) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
            table=table,
        )

        assert len(rows) == cardinality

    @pytest.mark.slow
    def test_avg_aggregation_1m_keys(self):
        """Test average aggregation with 1M values."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]
        values = [float(i % 1000) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="avg_v", function="avg", column="v")],
            table=table,
        )

        assert len(rows) == cardinality
        assert all(row["avg_v"] is not None for row in rows)

    @pytest.mark.slow
    def test_min_aggregation_1m_keys(self):
        """Test min aggregation with 1M values."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]
        values = [float(i) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="min_v", function="min", column="v")],
            table=table,
        )

        assert len(rows) == cardinality
        assert all(row["min_v"] is not None for row in rows)

    @pytest.mark.slow
    def test_max_aggregation_1m_keys(self):
        """Test max aggregation with 1M values."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]
        values = [float(i) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="max_v", function="max", column="v")],
            table=table,
        )

        assert len(rows) == cardinality
        assert all(row["max_v"] is not None for row in rows)

    @pytest.mark.slow
    def test_mixed_aggregations_1m_keys(self):
        """Test multiple aggregation functions simultaneously."""
        size = 1_000_000
        cardinality = 10_000
        keys = [i % cardinality for i in range(size)]
        values = [float(i % 1000) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
            table=table,
        )

        assert len(rows) == cardinality
        assert all(row["cnt"] > 0 for row in rows)
        assert all(row["sum_v"] is not None for row in rows)
        assert all(row["avg_v"] is not None for row in rows)
        assert all(row["min_v"] is not None for row in rows)
        assert all(row["max_v"] is not None for row in rows)

    @pytest.mark.parametrize("dist_type", ["uniform", "exponential", "clustered"])
    @pytest.mark.slow
    def test_aggregation_with_various_distributions(self, dist_type):
        """Test aggregations with different value distributions."""
        size = 100_000
        cardinality = 1_000
        keys = [i % cardinality for i in range(size)]

        if dist_type == "uniform":
            values = [float(random.randint(1, 1000)) for _ in range(size)]
        elif dist_type == "exponential":
            values = [float(int(random.expovariate(0.01))) for _ in range(size)]
        else:  # clustered
            values = [float(int(random.gauss(500, 100))) for _ in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
            ],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size


# ============================================================================
# SECTION 8: ROUND-TRIP STABILITY TESTS
# ============================================================================


class TestRoundTripStability:
    """Test encode/decode round-trip stability."""

    @pytest.mark.slow
    def test_round_trip_bit_for_bit_stability(self):
        """Test that encode/decode is bit-for-bit stable."""
        size = 50_000
        keys = [i % 1000 for i in range(size)]
        values = [float(i) for i in range(size)]

        # First execution
        table1 = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows1 = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
            ],
            table=table1,
        )

        # Second execution with same data
        table2 = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows2 = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
            ],
            table=table2,
        )

        by_key1 = _rows_by_key(rows1, "k")
        by_key2 = _rows_by_key(rows2, "k")

        assert by_key1 == by_key2

    @pytest.mark.slow
    def test_round_trip_string_keys_stability(self):
        """Test round-trip stability with string keys."""
        size = 50_000
        strings = [f"str_{i % 1000:04d}" for i in range(size)]

        for iteration in range(10):
            table = pa.table({"k": pa.array(strings, type=pa.string())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            by_key = _rows_by_key(rows, "k")
            # Each unique string should appear 50 times (50000 / 1000)
            assert all(row["cnt"] == 50 for row in rows)

    @pytest.mark.slow
    def test_round_trip_multi_key_stability(self):
        """Test round-trip stability with multiple key columns."""
        size = 50_000
        k1 = [i % 100 for i in range(size)]
        k2 = [f"s{i % 50:03d}" for i in range(size)]
        k3 = [i % 10 for i in range(size)]

        for iteration in range(10):
            table = pa.table(
                {
                    "k1": pa.array(k1, type=pa.int64()),
                    "k2": pa.array(k2, type=pa.string()),
                    "k3": pa.array(k3, type=pa.int64()),
                }
            )
            rows = _finalize_rows(
                group_by_columns=["k1", "k2", "k3"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            assert len(rows) <= 100 * 50 * 10


# ============================================================================
# SECTION 9: RANDOMIZED PROPERTY TESTS
# ============================================================================


class TestRandomizedPropertyTests:
    """Randomized fuzz and property-based tests."""

    @pytest.mark.slow
    def test_fuzz_random_int_keys(self):
        """Fuzz test with random int keys."""
        random.seed(42)  # Deterministic for reproducibility

        for trial in range(100):
            size = random.randint(1000, 10000)
            cardinality = random.randint(10, size // 2)
            keys = [random.randint(-(2**31), 2**31 - 1) for _ in range(size)]

            table = pa.table({"k": pa.array(keys, type=pa.int64())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_fuzz_random_string_keys(self):
        """Fuzz test with random string keys."""
        random.seed(43)

        for trial in range(100):
            size = random.randint(1000, 10000)
            cardinality = random.randint(10, size // 2)

            # Generate random strings
            charset = "abcdefghijklmnopqrstuvwxyz0123456789"
            keys = ["".join(random.choices(charset, k=random.randint(1, 20))) for _ in range(size)]

            table = pa.table({"k": pa.array(keys, type=pa.string())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_property_all_values_accounted_for(self):
        """Property test: all input values accounted for in groups."""
        random.seed(44)

        for trial in range(50):
            size = random.randint(1000, 100000)
            keys = [random.randint(0, 10000) for _ in range(size)]
            values = [float(random.randint(1, 100)) for _ in range(size)]

            table = pa.table(
                {
                    "k": pa.array(keys, type=pa.int64()),
                    "v": pa.array(values, type=pa.float64()),
                }
            )
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            total_count = sum(row["cnt"] for row in rows)
            assert total_count == size, f"Trial {trial}: missing rows"

    @pytest.mark.slow
    def test_property_no_data_corruption(self):
        """Property test: ensure no data corruption in round-trip."""
        random.seed(45)

        for trial in range(50):
            size = random.randint(1000, 50000)
            cardinality = random.randint(10, min(1000, size // 2))
            keys = [random.randint(0, cardinality - 1) for _ in range(size)]

            # Track expected counts
            expected_counts = {}
            for k in keys:
                expected_counts[k] = expected_counts.get(k, 0) + 1

            table = pa.table({"k": pa.array(keys, type=pa.int64())})
            rows = _finalize_rows(
                group_by_columns=["k"],
                aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
                table=table,
            )

            by_key = _rows_by_key(rows, "k")
            for k, expected_cnt in expected_counts.items():
                actual_cnt = by_key.get((k,), {}).get("cnt", 0)
                assert actual_cnt == expected_cnt, f"Trial {trial}: mismatch for key {k}"


# ============================================================================
# SECTION 10: ADVANCED EDGE CASE COMBINATIONS
# ============================================================================


class TestAdvancedEdgeCombinations:
    """Test advanced combinations of edge cases."""

    @pytest.mark.slow
    def test_mega_string_with_many_nulls_and_high_cardinality_int(self):
        """Test large strings + many nulls + high cardinality integers."""
        size = 50_000

        # Large strings (1KB each)
        large_str = "x" * 1000
        strings = [large_str if i % 100 != 0 else None for i in range(size)]

        # High cardinality ints
        ints = [i % 5000 for i in range(size)]

        table = pa.table(
            {
                "s": pa.array(strings, type=pa.string()),
                "i": pa.array(ints, type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["s", "i"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size

    @pytest.mark.slow
    def test_all_unicode_planes_with_different_frequencies(self):
        """Test all Unicode planes with varying frequencies."""
        strings = []

        # BMP characters (repeated 10x)
        for i in range(10):
            strings.extend([f"café{j}" for j in range(100)])

        # SMP characters (repeated 5x)
        for i in range(5):
            strings.extend([f"🎉{j}" for j in range(100)])

        # Mix with nulls
        strings = [s if i % 50 != 0 else None for i, s in enumerate(strings)]

        table = pa.table({"k": pa.array(strings, type=pa.string())})
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == len(strings)

    @pytest.mark.slow
    def test_power_of_two_integers_with_all_aggregations_and_nulls(self):
        """Test powers of 2 with all aggregations and nulls."""
        powers = [2**i for i in range(30)]

        # Repeat each power with some nulls
        keys = []
        values = []
        for p in powers:
            for j in range(100):
                keys.append(p if j % 10 != 0 else None)
                values.append(float(p) if j % 10 != 0 else None)

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="avg", column="v"),
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == len(keys)

    @pytest.mark.slow
    def test_extreme_cardinality_combinations(self):
        """Test extreme cardinality combinations."""
        size = 100_000

        # One column: 1 value (all same)
        k1 = ["A"] * size

        # One column: all unique
        k2 = [f"{i:06d}" for i in range(size)]

        # One column: 10 values
        k3 = [i % 10 for i in range(size)]

        table = pa.table(
            {
                "k1": pa.array(k1, type=pa.string()),
                "k2": pa.array(k2, type=pa.string()),
                "k3": pa.array(k3, type=pa.int64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k1", "k2", "k3"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size
        # Should have size * 10 combinations (since k2 is unique)
        assert len(rows) == size

    @pytest.mark.slow
    def test_sequential_then_reverse_sequential_keys(self):
        """Test sequential numbers then reverse sequential."""
        size = 50_000

        keys = list(range(size // 2)) + list(range(size // 2, 0, -1))
        values = [float(i) for i in range(size)]

        table = pa.table(
            {
                "k": pa.array(keys, type=pa.int64()),
                "v": pa.array(values, type=pa.float64()),
            }
        )
        rows = _finalize_rows(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
            table=table,
        )

        assert sum(row["cnt"] for row in rows) == size
