# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""ALTER MATERIALIZED VIEW <name> OWNER TO <principal>.

A view refreshes as a pinned identity rather than as whoever's commit fired it.
That identity is set when the view is created and deliberately survives being
edited - so fixing someone's view does not make you responsible for it, nor
hand your authority to whoever edits next. This statement is the only thing
that moves it.
"""

import json
import os

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _setup(tmp_path, user="alice", policies=None):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session(user=user, access_policies=policies or _OWNER_POLICY)


def _seed_view(session):
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (1), (2)"))
    list(session.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src"))


def _record(tmp_path):
    with open(os.path.join(str(tmp_path), "ws", "mv", "materialized_view.json")) as f:
        return json.load(f)


def test_runs_as_is_pinned_to_the_creator(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)
    assert _record(tmp_path)["runs-as"] == "alice"


def test_alter_owner_moves_it(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO svc_etl"))

    record = _record(tmp_path)
    assert record["runs-as"] == "svc_etl"
    # A transfer is not an edit - the definition is untouched.
    assert record["sql"].strip().upper().startswith("SELECT")
    assert record["source_tables"] == ["ws.src"]


def test_editing_a_view_does_not_transfer_it(tmp_path):
    """The reason the statement has to exist: redefining a view records a new
    statement author but must not silently move whose authority refreshes it."""
    session = _setup(tmp_path)
    _seed_view(session)

    bob = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    list(
        bob.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 1"
        )
    )

    record = _record(tmp_path)
    assert record["runs-as"] == "alice"  # unchanged by bob's edit
    assert record["author"] == "bob"  # but bob is on the record


def test_alter_owner_accepts_a_quoted_principal(tmp_path):
    """Principals are often email addresses, which need quoting to survive as
    one token."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO 'someone@example.com'"))
    assert _record(tmp_path)["runs-as"] == "someone@example.com"


def test_only_ownership_is_alterable(tmp_path):
    """Everything else about a view follows from its defining SELECT."""
    session = _setup(tmp_path)
    _seed_view(session)

    with pytest.raises(UnsupportedSyntaxError, match="OWNER TO"):
        list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv SET x = 1"))


def test_other_alter_statements_still_reach_the_parser(tmp_path):
    """The intercept must be narrow: ALTER TABLE is not ours to claim, and a
    too-greedy lead pattern would break every other ALTER in the language."""
    session = _setup(tmp_path)
    _seed_view(session)

    # Not an MV statement, so it goes to the parser and fails on its own terms -
    # not with our "OWNER TO" message.
    with pytest.raises(Exception) as exc:
        list(session.execute_to_morsels("ALTER TABLE ws.src OWNER TO someone"))
    assert "MATERIALIZED VIEW" not in str(exc.value)


def test_alter_owner_rejects_a_plain_table(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)

    with pytest.raises(ValueError, match="not a materialized view"):
        list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.src OWNER TO svc_etl"))


def test_owner_to_current_user_assigns_the_caller(tmp_path):
    """The safest form of the statement: it can only ever point a view at the
    person running it, so no authority can be borrowed."""
    session = _setup(tmp_path)
    _seed_view(session)
    assert _record(tmp_path)["runs-as"] == "alice"

    bob = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    list(bob.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO CURRENT_USER"))
    assert _record(tmp_path)["runs-as"] == "bob"


def test_quoted_current_user_is_a_literal_principal(tmp_path):
    """Quoting asks for a principal literally named CURRENT_USER - the usual
    SQL distinction, and the only way to name one if it ever exists."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO 'CURRENT_USER'"))
    assert _record(tmp_path)["runs-as"] == "CURRENT_USER"


def test_suspend_and_resume(tmp_path):
    """SUSPEND stops the view refreshing without dismantling the machinery that
    does it. Dropping its triggers was the only way before, and left no way to
    tell "deliberately off" from "quietly broken"."""
    session = _setup(tmp_path)
    _seed_view(session)
    assert _record(tmp_path).get("suspended-at-ms") is None

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv SUSPEND"))
    record = _record(tmp_path)
    assert isinstance(record["suspended-at-ms"], int)
    assert record["suspended-by"] == "alice"

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv RESUME"))
    record = _record(tmp_path)
    assert record["suspended-at-ms"] is None
    assert record["suspended-by"] is None


def test_pause_is_not_the_keyword(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)
    with pytest.raises(UnsupportedSyntaxError, match="SUSPEND"):
        list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv PAUSE"))
