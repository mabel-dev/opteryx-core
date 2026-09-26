# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
OpteryxTable must accept the single-key top-N spec.

Without the capability TopNScanPushdownStrategy never stamps a catalog scan, so
TopNManifestPruningStrategy never runs and `ORDER BY created_at DESC LIMIT 50`
over public.github.events read every file (~238GB) instead of the newest one.
"""

from types import SimpleNamespace

from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.expression import NodeType


def _key(node_type=NodeType.IDENTIFIER, name="created_at", identity="abc"):
    return SimpleNamespace(
        node_type=node_type, schema_column=SimpleNamespace(name=name, identity=identity)
    )


def test_opteryx_table_declares_topn_pushdown():
    assert OpteryxTable.supports_topn_pushdown is True


def test_opteryx_table_accepts_a_single_column_key():
    # can_push_topn does not touch instance state; called unbound so no catalog
    # is needed to construct the table.
    assert OpteryxTable.can_push_topn(None, [(_key(), False)]) is True


def test_opteryx_table_declines_what_the_parquet_reader_cannot_sort_by():
    assert OpteryxTable.can_push_topn(None, [(_key(), False), (_key(name="id"), True)]) is False
    assert OpteryxTable.can_push_topn(None, [(_key(node_type=NodeType.FUNCTION), True)]) is False


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
