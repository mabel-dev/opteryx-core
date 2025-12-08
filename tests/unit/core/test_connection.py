"""
Test the connection example from the documentation
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest



def test_byte_strings():
    import opteryx

    cur = opteryx.query(b"SELECT * FROM $planets")
    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute(b"SELECT * FROM $planets")
    assert cur.rowcount == 9
    assert cur.shape == (9, 20)

    conn = opteryx.connect()
    cur = conn.cursor()
    arrow = cur.execute_to_arrow(b"SELECT * FROM $planets")
    assert arrow.num_rows == 9, cur.rowcount
    assert arrow.shape == (9, 20)

def test_register_errors():
    from opteryx import register_store
    from opteryx.connectors import DiskConnector
    
    with pytest.raises(ValueError):
        register_store(prefix="prefix", connector=DiskConnector(dataset="", telemetry=None))

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
