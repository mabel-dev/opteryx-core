import pytest

import pyarrow as pa
from draken.morsels.morsel import Morsel
from draken.vectors.string_vector import StringVector
from opteryx.exceptions import IncorrectTypeError
from opteryx.expression.binary_operators import ArrowOp
from opteryx.expression.binary_operators import LongArrowOp


def _to_list(result):
    if hasattr(result, "to_pylist"):
        return result.to_pylist()
    return list(result)


def _docs(values):
    return Morsel.from_arrow(pa.table({"v": pa.array(values)})).column(b"v")


def _key(value: bytes):
    return StringVector.from_constant(value, 1)


def test_longarrow_returns_stringvector_bytes():
    docs = _docs([b'{"a":1,"b":[2],"c":{"d":3},"s":"x"}', None])

    assert _to_list(LongArrowOp(docs, _key(b"a"))) == [b"1", None]
    assert _to_list(LongArrowOp(docs, _key(b"b"))) == [b"[2]", None]
    assert _to_list(LongArrowOp(docs, _key(b"c"))) == [b'{"d":3}', None]
    assert _to_list(LongArrowOp(docs, _key(b"s"))) == [b"x", None]
    assert _to_list(LongArrowOp(docs, _key(b"missing"))) == [None, None]


def test_arrow_returns_native_scalar_vectors_when_possible():
    docs = _docs([b'{"a":1,"s":"x"}', None])

    assert _to_list(ArrowOp(docs, _key(b"a"))) == [1, None]
    assert _to_list(ArrowOp(docs, _key(b"s"))) == [b"x", None]


def test_arrow_complex_values_fail_without_variant_vector():
    docs = _docs([b'{"arr":[1,2],"obj":{"k":3}}', None])

    with pytest.raises(IncorrectTypeError):
        ArrowOp(docs, _key(b"arr"))
    with pytest.raises(IncorrectTypeError):
        ArrowOp(docs, _key(b"obj"))


def test_json_extract_rejects_non_json_documents():
    docs = _docs([b"not-json"])

    with pytest.raises(IncorrectTypeError):
        ArrowOp(docs, _key(b"a"))
    with pytest.raises(IncorrectTypeError):
        LongArrowOp(docs, _key(b"a"))
