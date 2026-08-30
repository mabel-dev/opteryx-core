# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""EXECUTE <task> USING <value> AS <name>.

A task is a statement recorded in the catalog. EXECUTE reads that statement at
plan time, binds the USING arguments to its named placeholders, and plans the
result through the same builder table the top level uses - so a task is any
statement the engine can already plan.

On the local store the record is `task.json` next to `dataset.json`, mirroring
where `materialized_view.json` sits.
"""

import json
import os

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import ParameterError
from opteryx.exceptions import UnsupportedSyntaxError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


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
            row = {}
            for k, vs in pydict.items():
                v = vs[i]
                if isinstance(v, bytes):
                    v = v.decode()
                row[k] = v
            rows.append(row)
    return rows


def _write_task(tmp_path, relation, sql):
    """Register a task by writing the sidecar the local store reads."""
    path = tmp_path
    for part in relation.split("."):
        path = path / part
    os.makedirs(path, exist_ok=True)
    with open(path / "task.json", "w") as f:
        json.dump({"sql": sql, "runs-as": "olive"}, f)


def _seed(session, name="ws.src"):
    list(session.execute_to_morsels(f"CREATE TABLE {name} (a BIGINT)"))
    list(session.execute_to_morsels(f"INSERT INTO {name} VALUES (-1), (1), (2), (3)"))


# --- the statement runs


def test_execute_runs_the_recorded_statement(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(tmp_path, "ws.copy_positive", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > 0")

    list(owner.execute_to_morsels("EXECUTE ws.copy_positive"))

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3]


def test_using_arguments_bind_to_named_placeholders(tmp_path):
    """The whole point of the USING clause: the window is bound at fire time."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(
        tmp_path,
        "ws.copy_window",
        "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low AND a <= :high",
    )

    list(
        owner.execute_to_morsels(
            "EXECUTE ws.copy_window USING 0 AS low, 2 AS high"
        )
    )

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert sorted(r["a"] for r in rows) == [1, 2]


def test_arguments_are_matched_by_name_not_position(tmp_path):
    """Reordering the USING clause must not change the window."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(
        tmp_path,
        "ws.copy_window",
        "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low AND a <= :high",
    )

    list(
        owner.execute_to_morsels(
            "EXECUTE ws.copy_window USING 2 AS high, 0 AS low"
        )
    )

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert sorted(r["a"] for r in rows) == [1, 2]


def test_negative_and_string_arguments_bind(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(tmp_path, "ws.t", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low")

    list(owner.execute_to_morsels("EXECUTE ws.t USING -2 AS low"))

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert sorted(r["a"] for r in rows) == [-1, 1, 2, 3]


def test_task_runs_its_current_definition(tmp_path):
    """Redefining a task takes effect on its next execution, not later."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(tmp_path, "ws.t", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > 2")
    list(owner.execute_to_morsels("EXECUTE ws.t"))

    _write_task(tmp_path, "ws.t", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a < -0")
    list(owner.execute_to_morsels("EXECUTE ws.t"))

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert sorted(r["a"] for r in rows) == [-1, 3]


# --- refusals


def test_execute_on_a_non_task_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)

    with pytest.raises(UnsupportedSyntaxError, match="is not a task"):
        list(owner.execute_to_morsels("EXECUTE ws.src"))


def test_missing_argument_is_named(tmp_path):
    """A placeholder with no argument must name the parameter, not fail vaguely."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(tmp_path, "ws.t", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low")

    with pytest.raises(ParameterError, match="low"):
        list(owner.execute_to_morsels("EXECUTE ws.t"))


def test_positional_arguments_are_refused(tmp_path):
    """`EXECUTE t(1, 2)` parses; it must not silently mean something."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="named arguments"):
        list(owner.execute_to_morsels("EXECUTE ws.t(1, 2)"))


def test_unnamed_using_argument_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="must be named"):
        list(owner.execute_to_morsels("EXECUTE ws.t USING 1"))


def test_duplicate_argument_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="more than once"):
        list(owner.execute_to_morsels("EXECUTE ws.t USING 1 AS low, 2 AS low"))


def test_non_constant_argument_is_refused(tmp_path):
    """A task's arguments are bound, never evaluated against a relation."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="not a constant"):
        list(owner.execute_to_morsels("EXECUTE ws.t USING some_column AS low"))


def test_execute_immediate_is_refused(tmp_path):
    """`IMMEDIATE` is not a keyword here - it parses as the task's name."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="EXECUTE IMMEDIATE"):
        list(owner.execute_to_morsels("EXECUTE IMMEDIATE 'SELECT 1'"))


def test_task_noise_word_is_named(tmp_path):
    """`EXECUTE TASK t` parses, binding `TASK` as the name - say so plainly."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="no \\*\\*TASK\\*\\* keyword"):
        list(owner.execute_to_morsels("EXECUTE TASK ws.t"))


def test_a_task_cannot_run_another_task(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.inner", "SELECT 1")
    _write_task(tmp_path, "ws.outer", "EXECUTE ws.inner")

    with pytest.raises(UnsupportedSyntaxError, match="cannot run another task"):
        list(owner.execute_to_morsels("EXECUTE ws.outer"))


def test_task_with_no_statement_is_refused(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    path = tmp_path / "ws" / "t"
    os.makedirs(path, exist_ok=True)
    with open(path / "task.json", "w") as f:
        json.dump({"runs-as": "olive"}, f)

    with pytest.raises(UnsupportedSyntaxError, match="no statement recorded"):
        list(owner.execute_to_morsels("EXECUTE ws.t"))


def test_arguments_cannot_change_how_the_task_parses(tmp_path):
    """Substitution happens after parsing; an argument is a value, never syntax."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.sink (a BIGINT)"))

    _write_task(tmp_path, "ws.t", "INSERT INTO ws.sink SELECT a FROM ws.src WHERE a > :low")

    # The argument is a string containing SQL. It must be bound as a value -
    # producing a comparison against text - not spliced into the statement.
    with pytest.raises(Exception) as excinfo:
        list(owner.execute_to_morsels("EXECUTE ws.t USING '0 OR 1=1' AS low"))
    assert "DROP" not in str(excinfo.value).upper()

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.sink"))
    assert rows == []


# --- symbolic versions: CURRENT / PREVIOUS -------------------------------------
#
# Attended-run sugar, by the invoker's explicit choice: a person typing CURRENT
# is standing there choosing "the head, now" and answers for it. The firing
# path never uses these - it binds literal ids, because an unattended window
# must mean the same thing however late the worker runs it.


def _parse_one(sql):
    from opteryx.third_party import sqloxide

    return sqloxide.parse_sql(sql, _dialect="opteryx")[0]


def test_symbolic_words_resolve_to_real_ids_at_plan_time(monkeypatch):
    """CURRENT/PREVIOUS become literal snapshot ids via the connector's own
    virtual-tag resolver - never the engine's resolve-at-read sentinel."""
    import opteryx.connectors as connectors
    from opteryx.planner.logical_planner.logical_planner import _resolve_symbolic_versions
    from opteryx.planner.logical_planner.logical_planner import _SymbolicVersion

    class _Travels:
        supports_version_travel = True

        def resolve_named_version(self, relation, word):
            return {"CURRENT": 200, "PREVIOUS": 100}[word]

    monkeypatch.setattr(connectors, "connector_factory", lambda name, telemetry=None: _Travels())

    parsed = _parse_one(
        "SELECT * FROM cat.coll.t VERSION AS OF :cur AS c "
        "LEFT ANTI JOIN cat.coll.t VERSION AS OF :par AS p ON c.a = p.a"
    )
    resolved = _resolve_symbolic_versions(
        parsed, {"cur": _SymbolicVersion("CURRENT"), "par": _SymbolicVersion("PREVIOUS")}
    )

    assert resolved == {"cur": 200, "par": 100}


def test_a_symbol_used_outside_version_as_of_is_refused(monkeypatch):
    """A snapshot word in a predicate position has no meaning to resolve."""
    from opteryx.planner.logical_planner.logical_planner import _resolve_symbolic_versions
    from opteryx.planner.logical_planner.logical_planner import _SymbolicVersion

    parsed = _parse_one("SELECT * FROM cat.coll.t WHERE a > :cur")
    with pytest.raises(UnsupportedSyntaxError, match="VERSION AS OF"):
        _resolve_symbolic_versions(parsed, {"cur": _SymbolicVersion("CURRENT")})


def test_a_symbol_spanning_two_relations_is_ambiguous(monkeypatch):
    """One placeholder time-travelling two relations would name a different
    snapshot for each - refused, not guessed."""
    from opteryx.planner.logical_planner.logical_planner import _resolve_symbolic_versions
    from opteryx.planner.logical_planner.logical_planner import _SymbolicVersion

    parsed = _parse_one(
        "SELECT * FROM cat.coll.a VERSION AS OF :v AS x "
        "LEFT JOIN cat.coll.b VERSION AS OF :v AS y ON x.a = y.a"
    )
    with pytest.raises(UnsupportedSyntaxError, match="ambiguous"):
        _resolve_symbolic_versions(parsed, {"v": _SymbolicVersion("CURRENT")})


def test_a_symbol_used_both_in_and_out_of_version_position_is_refused(monkeypatch):
    from opteryx.planner.logical_planner.logical_planner import _resolve_symbolic_versions
    from opteryx.planner.logical_planner.logical_planner import _SymbolicVersion

    parsed = _parse_one("SELECT * FROM cat.coll.t VERSION AS OF :v WHERE a > :v")
    with pytest.raises(UnsupportedSyntaxError, match="outside"):
        _resolve_symbolic_versions(parsed, {"v": _SymbolicVersion("CURRENT")})


def test_symbols_against_a_connector_without_version_travel_are_refused(tmp_path):
    """The local store cannot time-travel, so a symbol cannot resolve there."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed(owner)
    _write_task(tmp_path, "ws.t", "SELECT a FROM ws.src VERSION AS OF :cur")

    with pytest.raises(UnsupportedSyntaxError, match="version travel"):
        list(owner.execute_to_morsels("EXECUTE ws.t USING CURRENT AS cur"))


def test_other_identifiers_are_still_not_constants(tmp_path):
    """Admitting two words must not quietly admit every identifier."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _write_task(tmp_path, "ws.t", "SELECT 1")

    with pytest.raises(UnsupportedSyntaxError, match="not a constant"):
        list(owner.execute_to_morsels("EXECUTE ws.t USING some_column AS cur"))
