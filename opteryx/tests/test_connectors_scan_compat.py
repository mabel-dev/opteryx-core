"""Tests for scan() compatibility fallbacks in OpteryxTable."""

import os
import sys

from opteryx.connectors.opteryx_connector import OpteryxTable

# Add project root to import path for local tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


def test_get_list_of_blob_names_falls_back_on_missing_row_limit():
    class DummyTableObj:
        def __init__(self):
            self.limit = 10
            self.snapshot_id = None

            class T:
                def scan(self, *args, **kwargs):
                    # Simulate older signature that errors on row_limit kw
                    if "row_limit" in kwargs:
                        raise TypeError("unexpected keyword 'row_limit'")

                    class DF:
                        def __init__(self, path):
                            self.file_path = path

                    return [DF("a"), DF("b")]

            self.table = T()

    dummy = DummyTableObj()
    result = OpteryxTable.get_list_of_blob_names(dummy)
    assert result == ["a", "b"]
