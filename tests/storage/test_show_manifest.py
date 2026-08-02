# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW MANIFEST FOR - shipped with zero test coverage in 54af4f67, closing that gap."""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
_WRITER_POLICY = [{"pattern": "*", "role": "writer"}]
_READER_POLICY = [{"pattern": "*", "role": "reader"}]


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        n = len(next(iter(pydict.values()))) if pydict else 0
        for i in range(n):
            rows.append({k: vs[i] for k, vs in pydict.items()})
    return rows


def test_show_manifest_returns_file_metadata(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a, 'hello' AS b"))

    rows = _morsels_to_rows(owner.execute_to_morsels("SHOW MANIFEST FOR ws.dst"))

    assert len(rows) == 1
    assert rows[0]["file_path"]
    assert rows[0]["record_count"] == 1


def test_show_manifest_requires_owner_not_writer(tmp_path):
    """Same tier as DROP - a writer can change a table's contents but not
    see its file-level layout."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))

    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)
    with pytest.raises(PermissionError, match="permission to view the manifest"):
        list(writer.execute_to_morsels("SHOW MANIFEST FOR ws.dst"))


def test_show_manifest_requires_owner_not_reader(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))

    reader = opteryx.session(user="rita", access_policies=_READER_POLICY)
    with pytest.raises(PermissionError, match="permission to view the manifest"):
        list(reader.execute_to_morsels("SHOW MANIFEST FOR ws.dst"))
