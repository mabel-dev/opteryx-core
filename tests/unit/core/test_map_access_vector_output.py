import os
import sys
from types import SimpleNamespace

import draken.draken_native as dn

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import Node
from opteryx.types.logical_type import INT64, VARCHAR
import opteryx


def _schema(identity: str, value_type):
    return SimpleNamespace(identity=identity.encode(), column_type=value_type, name=identity)


def test_map_access_string_projection_returns_draken_vector():
    morsel = Morsel.from_vectors(
        ["user_name"], [dn.vector_from_string_sequence([b"alice", b"bob", None])]
    )

    user_name = Node(
        NodeType.IDENTIFIER,
        value="user_name",
        schema_column=_schema("user_name", VARCHAR),
    )
    zero = Node(
        NodeType.LITERAL,
        value=0,
        schema_column=_schema("zero", INT64),
    )
    first_char = Node(
        NodeType.EXTRACTION_OPERATOR,
        value="MapAccess",
        left=user_name,
        right=zero,
        schema_column=_schema("a", VARCHAR),
    )

    out = execute_and_append(compile_eval_nodes([first_char]), morsel)
    values = out.column(b"a").to_pylist()
    normalized = [v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v for v in values]

    assert normalized == ["a", "b", None]


def test_hex_encode_projection_returns_draken_vector():
    session = opteryx.session()
    try:
        morsels = list(
            session.execute_to_morsels(
                "SELECT COUNT(*), a FROM (SELECT HEX_ENCODE(name) AS a FROM $planets) GROUP BY a"
            )
        )
        assert len(morsels) > 0
        assert sum(m.num_rows for m in morsels) > 0
    finally:
        session.close()


def test_hex_encode_index_projection_returns_draken_vector():
    session = opteryx.session()
    try:
        morsels = list(
            session.execute_to_morsels(
                "SELECT COUNT(*), a FROM (SELECT HEX_ENCODE(name)[0] AS a FROM $planets) GROUP BY a"
            )
        )
        assert len(morsels) > 0
        assert sum(m.num_rows for m in morsels) > 0
    finally:
        session.close()
