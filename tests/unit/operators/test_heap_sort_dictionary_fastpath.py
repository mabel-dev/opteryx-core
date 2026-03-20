import os
import sys
import types
from functools import cmp_to_key

import numpy
import pyarrow as pa
import pytest
from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.types import OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.int64_vector import Int64Vector
from opteryx.draken.vectors.string_vector import StringVector
from opteryx.expression import NodeType
from opteryx.expression.functions import get_catalog
from opteryx.models import Node
from opteryx.models.query_properties import QueryProperties
from opteryx.operators.heap_sort_node import HeapSortNode
from opteryx.operators.sort_node import SortNode


def _make_heap_sort(limit=2, direction="ASC"):
    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=limit)
    node.mapped_order = [("k", direction)]
    return node


def _top_n_rows(morsel, mapped_order, limit):
    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=limit)
    node.mapped_order = mapped_order
    return node._top_n(morsel).to_arrow().to_pylist()


def _run_sort(table, order_by):
    node = SortNode(QueryProperties("sort-test", {}), order_by=order_by)
    list(node.execute(table))
    outputs = [chunk for chunk in node.execute(EOS) if chunk is not EOS]
    assert len(outputs) == 1
    return outputs[0]


def _identifier(name, value_type=OrsoTypes.VECTOR, element_type=None):
    column = FlatColumn(name=name, type=value_type, element_type=element_type)
    column.identity = name
    return Node(NodeType.IDENTIFIER, schema_column=column)


def _literal_array(value):
    return Node(
        NodeType.LITERAL,
        type=OrsoTypes.VECTOR,
        value=value,
        schema_column=ConstantColumn(
            name="query_vector",
            type=OrsoTypes.VECTOR,
            value=value,
            element_type=OrsoTypes.DOUBLE,
        ),
    )


def _vector_order_node(function_name, source_name="embedding", query_vector=None):
    query_vector = query_vector or [1.0, 0.0]
    schema_column = FunctionColumn(name=function_name.lower(), type=OrsoTypes.DOUBLE)
    schema_column.identity = function_name.lower()
    node = Node(
        NodeType.FUNCTION,
        value=function_name,
        parameters=[_identifier(source_name, element_type=OrsoTypes.DOUBLE), _literal_array(query_vector)],
        schema_column=schema_column,
    )
    node.function_ref = get_catalog().resolve(function_name, node.parameters)
    return node


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


def _decode_strings(values):
    return [value.decode("utf-8") if isinstance(value, bytes) else value for value in values]


def _make_q19_style_chunk(user_ids, minutes, phrases, counts):
    return Morsel.from_vectors(
        ["UserID", "m", "SearchPhrase", "COUNT(*)"],
        [
            Int64Vector.from_arrow(pa.array(user_ids, type=pa.int64())),
            Int64Vector.from_dict(
                list(range(len(minutes))),
                minutes,
            ),
            StringVector.from_arrow(pa.array(phrases, type=pa.binary())),
            Int64Vector.from_arrow(pa.array(counts, type=pa.int64())),
        ],
    )


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


def test_heap_sort_execute_merges_chunked_top_n_before_eos():
    node = HeapSortNode(QueryProperties("heap-sort-test", {}), order_by=[], limit=2)
    node.mapped_order = [("COUNT(*)", "DESC")]

    chunk1 = _make_q19_style_chunk(
        [1, 2],
        [45, 23],
        [b"", b""],
        [37, 31],
    )
    chunk2 = _make_q19_style_chunk(
        [3, 4],
        [3, 50],
        [b"", b""],
        [28, 25],
    )

    list(node.execute(chunk1))
    list(node.execute(chunk2))
    outputs = [chunk for chunk in node.execute(EOS) if chunk is not EOS]

    assert len(outputs) == 1
    assert outputs[0].to_arrow().to_pylist() == [
        {"UserID": 1, "m": 45, "SearchPhrase": b"", "COUNT(*)": 37},
        {"UserID": 2, "m": 23, "SearchPhrase": b"", "COUNT(*)": 31},
    ]


def test_top_n_vector_similarity_uses_native_scoring_path():
    pytest.importorskip("opteryx.nanobind.vector_search")

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
                "embedding": pa.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                    type=pa.list_(pa.float32()),
                ),
            }
        )
    )

    node = HeapSortNode(
        QueryProperties("heap-sort-test", {}),
        order_by=[(_vector_order_node("COSINE_SIMILARITY"), "DESC")],
        limit=2,
        vector_topk_candidate=True,
    )

    out = node._top_n(morsel).to_arrow()

    assert _decode_strings(out["label"].to_pylist()) == ["match", "diagonal"]


def test_top_n_vector_similarity_prefers_exact_native_topk(monkeypatch):
    import opteryx.nanobind as nanobind_pkg

    calls = {"exact": 0, "score": 0}

    def _exact_search_cosine(query_vector, row_ids, vectors, k):
        calls["exact"] += 1
        assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
        assert row_ids.tolist() == [0, 1, 2]
        assert vectors.shape == (3, 2)
        assert k == 2
        return [0, 1], [1.0, 0.70710677]

    def _score_cosine(query_vector, vectors):
        calls["score"] += 1
        raise AssertionError("nearest-neighbor top-k should use exact_search_cosine")

    monkeypatch.setattr(
        nanobind_pkg,
        "vector_search",
        types.SimpleNamespace(
            exact_search_cosine=_exact_search_cosine,
            score_cosine=_score_cosine,
        ),
        raising=False,
    )

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
                "embedding": pa.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                    type=pa.list_(pa.float32()),
                ),
            }
        )
    )

    node = HeapSortNode(
        QueryProperties("heap-sort-test", {}),
        order_by=[(_vector_order_node("COSINE_SIMILARITY"), "DESC")],
        limit=2,
        vector_topk_candidate=True,
    )

    out = node._top_n(morsel).to_arrow()

    assert _decode_strings(out["label"].to_pylist()) == ["match", "diagonal"]
    assert calls == {"exact": 1, "score": 0}


def test_top_n_vector_distance_uses_native_scoring_path():
    pytest.importorskip("opteryx.nanobind.vector_search")

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
                "embedding": pa.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                    type=pa.list_(pa.float32()),
                ),
            }
        )
    )

    node = HeapSortNode(
        QueryProperties("heap-sort-test", {}),
        order_by=[(_vector_order_node("COSINE_DISTANCE"), "ASC")],
        limit=2,
    )

    out = node._top_n(morsel).to_arrow()

    assert _decode_strings(out["label"].to_pylist()) == ["match", "diagonal"]


def test_top_n_vector_similarity_partial_selection_handles_non_nearest_order(monkeypatch):
    import opteryx.nanobind as nanobind_pkg

    calls = {"score": 0}

    def _score_cosine(query_vector, vectors):
        calls["score"] += 1
        return numpy.asarray([1.0, 0.70710677, 0.0], dtype=numpy.float32)

    monkeypatch.setattr(
        nanobind_pkg,
        "vector_search",
        types.SimpleNamespace(score_cosine=_score_cosine),
        raising=False,
    )

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
                "embedding": pa.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                    type=pa.list_(pa.float32()),
                ),
            }
        )
    )

    node = HeapSortNode(
        QueryProperties("heap-sort-test", {}),
        order_by=[(_vector_order_node("COSINE_SIMILARITY"), "ASC")],
        limit=2,
    )

    out = node._top_n(morsel).to_arrow()

    assert _decode_strings(out["label"].to_pylist()) == ["orthogonal", "diagonal"]
    assert calls["score"] == 1


def test_sort_node_evaluates_functional_vector_order_by():
    table = pa.table(
        {
            "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
            "embedding": pa.array(
                [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                type=pa.list_(pa.float32()),
            ),
        }
    )

    out = _run_sort(
        table,
        [(_vector_order_node("COSINE_DISTANCE"), "ascending")],
    )

    assert out["label"].to_pylist() == ["match", "diagonal", "orthogonal"]


def test_top_n_vector_similarity_can_route_through_usearch(monkeypatch):
    import opteryx.nanobind as nanobind_pkg

    calls = {"created": 0, "add_batch": 0, "search": 0}

    class FakeIndex:
        def __init__(self, dimensions, capacity=0, metric="cos", expansion_add=0, expansion_search=0):
            calls["created"] += 1
            assert dimensions == 2
            assert metric == "cos"

        def add_batch(self, row_ids, vectors):
            calls["add_batch"] += 1
            assert row_ids.tolist() == [0, 1, 2]
            assert vectors.shape == (3, 2)

        def search(self, query_vector, k, exact=False):
            calls["search"] += 1
            assert query_vector.tolist() == pytest.approx([1.0, 0.0], abs=1e-6)
            assert k == 2
            assert exact is False
            return [0, 1], [0.0, 0.29289323]

    monkeypatch.setattr(HeapSortNode, "_USEARCH_ENABLED", True)
    monkeypatch.setattr(HeapSortNode, "_USEARCH_MIN_ROWS", 1)
    monkeypatch.setattr(
        nanobind_pkg,
        "usearch_native",
        types.SimpleNamespace(UsearchIndex=FakeIndex),
        raising=False,
    )

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "label": pa.array(["match", "diagonal", "orthogonal"], type=pa.string()),
                "embedding": pa.array(
                    [[1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
                    type=pa.list_(pa.float32()),
                ),
            }
        )
    )

    node = HeapSortNode(
        QueryProperties("heap-sort-test", {}),
        order_by=[(_vector_order_node("COSINE_SIMILARITY"), "DESC")],
        limit=2,
        vector_topk_candidate=True,
    )

    out = node._top_n(morsel).to_arrow()

    assert calls == {"created": 1, "add_batch": 1, "search": 1}
    assert _decode_strings(out["label"].to_pylist()) == ["match", "diagonal"]
