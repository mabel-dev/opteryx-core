import os
import sys
from functools import cmp_to_key

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.morsels.morsel import Morsel
from opteryx.models.query_properties import QueryProperties
from opteryx.operators.heap_sort_node import HeapSortNode


def _make_heap_sort(limit=2, direction="ASC"):
    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=limit)
    node.mapped_order = [("k", direction)]
    return node


def _top_n_rows(morsel, mapped_order, limit):
    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=limit)
    node.mapped_order = mapped_order
    return node._top_n(morsel).to_arrow().to_pylist()


def _expected_top_n_rows(rows, mapped_order, limit):
    def _compare(left, right):
        for column, direction in mapped_order:
            left_value = left[column]
            right_value = right[column]
            left_null = left_value is None
            right_null = right_value is None
            if left_null and right_null:
                continue
            if left_null:
                return 1
            if right_null:
                return -1
            if left_value == right_value:
                continue
            if direction.upper().startswith("DESC"):
                return -1 if left_value > right_value else 1
            return -1 if left_value < right_value else 1
        return 0

    return sorted(rows, key=cmp_to_key(_compare))[:limit]


def _normalize_rows(rows):
    normalized = []
    for row in rows:
        normalized.append(
            {
                key: value.decode("utf-8") if isinstance(value, bytes) else value
                for key, value in row.items()
            }
        )
    return normalized


def test_dictionary_integer_vector_is_exact_compressible():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 2], type=pa.int8()),
        pa.array([3, 1, 2], type=pa.int32()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": arr}))
    vec = morsel.column(b"k")

    assert HeapSortNode._is_exact_compressible_vector(vec)


def test_dictionary_string_vector_is_not_exact_compressible():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 2], type=pa.int8()),
        pa.array(["c", "a", "b"], type=pa.string()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": arr}))
    vec = morsel.column(b"k")

    assert not HeapSortNode._is_exact_compressible_vector(vec)


def test_dictionary_float_vector_is_not_exact_compressible():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 2], type=pa.int8()),
        pa.array([3.5, 1.5, 2.5], type=pa.float64()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": arr}))
    vec = morsel.column(b"k")

    assert not HeapSortNode._is_exact_compressible_vector(vec)


def test_top_n_single_key_with_integer_dictionary_ordering():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 0, 1, 2], type=pa.int8()),
        pa.array([3, 1, 2], type=pa.int32()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": arr}))

    asc = _make_heap_sort(limit=3, direction="ASC")._top_n(morsel).to_arrow()["k"].to_pylist()
    desc = _make_heap_sort(limit=3, direction="DESC")._top_n(morsel).to_arrow()["k"].to_pylist()

    assert asc == [1, 1, 2]
    assert desc == [3, 3, 2]


def test_top_n_single_key_with_float_dictionary_ordering():
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 0, 1, 2], type=pa.int8()),
        pa.array([3.5, 1.5, 2.5], type=pa.float64()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": arr}))

    asc = _make_heap_sort(limit=3, direction="ASC")._top_n(morsel).to_arrow()["k"].to_pylist()
    desc = _make_heap_sort(limit=3, direction="DESC")._top_n(morsel).to_arrow()["k"].to_pylist()

    assert asc == [1.5, 1.5, 2.5]
    assert desc == [3.5, 3.5, 2.5]


def test_top_n_multi_key_with_numeric_dictionary_columns():
    key_primary = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 0, 2], type=pa.int8()),
        pa.array([2, 1, 3], type=pa.int32()),
    )
    key_secondary = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 3, 4], type=pa.int8()),
        pa.array([9.0, 5.0, 4.0, 1.0, 0.0], type=pa.float64()),
    )
    morsel = Morsel.from_arrow(pa.table({"k1": key_primary, "k2": key_secondary}))

    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=3)
    node.mapped_order = [("k1", "ASC"), ("k2", "ASC")]
    out = node._top_n(morsel).to_arrow()

    assert out["k1"].to_pylist() == [1, 1, 2]
    assert out["k2"].to_pylist() == [4.0, 5.0, 1.0]


def test_top_n_single_key_integer_dictionary_matches_materialized_order():
    key_dict = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 0, 1, 2, None], type=pa.int8()),
        pa.array([30, 10, 20], type=pa.int32()),
    )
    dict_morsel = Morsel.from_arrow(pa.table({"k": key_dict}))
    decoded_rows = pa.table({"k": key_dict.dictionary_decode()}).to_pylist()

    asc_dict = _top_n_rows(dict_morsel, [("k", "ASC")], limit=5)
    desc_dict = _top_n_rows(dict_morsel, [("k", "DESC")], limit=5)
    asc_expected = _expected_top_n_rows(decoded_rows, [("k", "ASC")], limit=5)
    desc_expected = _expected_top_n_rows(decoded_rows, [("k", "DESC")], limit=5)

    assert _normalize_rows(asc_dict) == _normalize_rows(asc_expected)
    assert _normalize_rows(desc_dict) == _normalize_rows(desc_expected)


def test_top_n_multi_key_integer_dictionary_matches_materialized_order():
    key_dict = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 2, 0, 2, None], type=pa.int8()),
        pa.array([30, 10, 20], type=pa.int32()),
    )
    seq = pa.array(["04", "03", "01", "00", "02", "05", "06"], type=pa.string())

    dict_morsel = Morsel.from_arrow(pa.table({"k": key_dict, "seq": seq}))
    decoded_rows = pa.table({"k": key_dict.dictionary_decode(), "seq": seq}).to_pylist()

    asc_dict = _top_n_rows(dict_morsel, [("k", "ASC"), ("seq", "ASC")], limit=4)
    desc_dict = _top_n_rows(dict_morsel, [("k", "DESC"), ("seq", "DESC")], limit=4)
    asc_expected = _expected_top_n_rows(decoded_rows, [("k", "ASC"), ("seq", "ASC")], limit=4)
    desc_expected = _expected_top_n_rows(decoded_rows, [("k", "DESC"), ("seq", "DESC")], limit=4)

    assert _normalize_rows(asc_dict) == _normalize_rows(asc_expected)
    assert _normalize_rows(desc_dict) == _normalize_rows(desc_expected)
