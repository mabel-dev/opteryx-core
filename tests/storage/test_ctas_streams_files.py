"""CTAS and INSERT write ONE streaming file of many row groups, not a file per
batch - through the local store, end to end.

The sinks used to hand every 262,144-row batch to `write_morsel`, one file per
call, so a 300,000-row CTAS landed as two files and a 3M-row one as twelve. Now
the batches are row groups of one file that rolls only at the 4 GB target.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
ROWS = 300_000  # 262,144 + a remainder: two row groups


def _setup(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session(user="alice", access_policies=_OWNER_POLICY)


def _scalar(session, sql):
    (morsel,) = [m for m in session.execute_to_morsels(sql) if m is not None]
    return morsel[0][0]


def _data_files(tmp_path, relation):
    folder = os.path.join(str(tmp_path), "ws", relation)
    return sorted(f for f in os.listdir(folder) if f.startswith("data-") and f.endswith(".parquet"))


def _row_group_count(path):
    from rugo.parquet import read_parquet

    with open(path, "rb") as f:
        data = f.read()
    with read_parquet(data) as reader:
        return sum(1 for _ in reader)


def test_ctas_lands_as_one_file_of_two_row_groups(tmp_path):
    session = _setup(tmp_path)
    list(
        session.execute_to_morsels(
            f"CREATE TABLE ws.big AS SELECT k FROM generate_series(1, {ROWS}) AS k"
        )
    )

    files = _data_files(tmp_path, "big")
    assert len(files) == 1, files
    assert _row_group_count(os.path.join(str(tmp_path), "ws", "big", files[0])) == 2
    assert not any(f.endswith(".tmp") for f in os.listdir(os.path.join(str(tmp_path), "ws", "big")))

    assert _scalar(session, "SELECT COUNT(*) FROM ws.big") == ROWS
    assert _scalar(session, "SELECT SUM(k) FROM ws.big") == ROWS * (ROWS + 1) // 2


def test_insert_appends_one_file_per_statement(tmp_path):
    session = _setup(tmp_path)
    list(session.execute_to_morsels("CREATE TABLE ws.t (k BIGINT)"))
    list(session.execute_to_morsels(f"INSERT INTO ws.t SELECT k FROM generate_series(1, {ROWS}) AS k"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (-1), (-2)"))

    files = _data_files(tmp_path, "t")
    assert len(files) == 2, files
    assert _scalar(session, "SELECT COUNT(*) FROM ws.t") == ROWS + 2
    assert _scalar(session, "SELECT MIN(k) FROM ws.t") == -2


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
