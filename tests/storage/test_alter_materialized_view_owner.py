# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""ALTER MATERIALIZED VIEW <name> OWNER TO <principal>.

A view refreshes as a pinned identity rather than as whoever's commit fired it.
The identity lives on each of the view's REFRESH TRIGGERS - the view itself
carries none, exactly as a task carries none - and is pinned when a trigger is
first landed. It deliberately survives the view being edited, so fixing
someone's view does not make you responsible for it, nor hand your authority
to whoever edits next. This statement repoints every refresh trigger of the
view at once; `ALTER TRIGGER ... OWNER TO` moves one.
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


def _runs_as(tmp_path, source="src"):
    """The `runs-as` of the view's refresh trigger on `source` - where the
    identity lives. The trigger sits beside the SOURCE table, not the view."""
    with open(os.path.join(str(tmp_path), "ws", source, "triggers.json")) as f:
        triggers = json.load(f)
    [trigger] = [t for t in triggers if t.get("target-view") == "ws.mv"]
    return trigger.get("runs-as")


def test_runs_as_is_pinned_to_the_creator_on_the_refresh_trigger(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)
    assert _runs_as(tmp_path) == "alice"
    # And nowhere on the view: it carries no identity of its own.
    assert "runs-as" not in _record(tmp_path)


def test_alter_owner_moves_it(tmp_path):
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO svc_etl"))

    assert _runs_as(tmp_path) == "svc_etl"
    record = _record(tmp_path)
    # A transfer is not an edit - the definition is untouched.
    assert record["sql"].strip().upper().startswith("SELECT")
    assert record["source_tables"] == ["ws.src"]
    assert "runs-as" not in record


def test_alter_owner_moves_every_refresh_trigger_of_the_view(tmp_path):
    """The reason the statement names the VIEW: a view over two tables has two
    refresh triggers, and a view whose triggers ran as two identities would
    refresh as whichever one's source was written to last."""
    session = _setup(tmp_path)
    _seed_view(session)
    list(session.execute_to_morsels("CREATE TABLE ws.other (b BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.other VALUES (1)"))
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS "
            "SELECT a FROM ws.src UNION ALL SELECT b FROM ws.other"
        )
    )
    assert (_runs_as(tmp_path, "src"), _runs_as(tmp_path, "other")) == ("alice", "alice")

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO svc_etl"))

    assert (_runs_as(tmp_path, "src"), _runs_as(tmp_path, "other")) == ("svc_etl", "svc_etl")


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

    assert _runs_as(tmp_path) == "alice"  # unchanged by bob's edit
    assert _record(tmp_path)["author"] == "bob"  # but bob is on the record


def test_editing_a_view_pins_only_the_trigger_on_a_newly_read_source(tmp_path):
    """The one place an edit does pin: a source the view did not read before
    has no trigger yet, so the trigger landed for it is the editor's. The
    trigger the view already had keeps its identity."""
    session = _setup(tmp_path)
    _seed_view(session)
    list(session.execute_to_morsels("CREATE TABLE ws.other (b BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.other VALUES (1)"))

    bob = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    list(
        bob.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS "
            "SELECT a FROM ws.src UNION ALL SELECT b FROM ws.other"
        )
    )

    assert _runs_as(tmp_path, "src") == "alice"
    assert _runs_as(tmp_path, "other") == "bob"


def test_alter_owner_accepts_a_quoted_principal(tmp_path):
    """Principals are often email addresses, which need quoting to survive as
    one token."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO 'someone@example.com'"))
    assert _runs_as(tmp_path) == "someone@example.com"


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
    assert _runs_as(tmp_path) == "alice"

    bob = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    list(bob.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO CURRENT_USER"))
    assert _runs_as(tmp_path) == "bob"


def test_quoted_current_user_is_a_literal_principal(tmp_path):
    """Quoting asks for a principal literally named CURRENT_USER - the usual
    SQL distinction, and the only way to name one if it ever exists."""
    session = _setup(tmp_path)
    _seed_view(session)

    list(session.execute_to_morsels("ALTER MATERIALIZED VIEW ws.mv OWNER TO 'CURRENT_USER'"))
    assert _runs_as(tmp_path) == "CURRENT_USER"


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


# ---------------------------------------------------------------------------
# The catalog connector's source accessor.
#
# The ownership check reads a view's sources to judge the incoming owner against
# them. The catalog spells that list `source-tables`; the local store's sidecar
# spells it `source_tables`. Reading the wrong key would not fail - it would
# return an empty list, and a check with nothing to look at is a check that
# cannot refuse anything. Hence a test against the catalog's spelling, not just
# the local store's.
# ---------------------------------------------------------------------------


class _FakeCatalog:
    """Stands in for opteryx_catalog, returning its record shape verbatim."""

    record = None

    def __init__(self, workspace=None, **kwargs):
        pass

    def get_materialized_view(self, identifier):
        if _FakeCatalog.record is None:
            from opteryx_catalog.exceptions import MaterializedViewError

            raise MaterializedViewError(f"{identifier} is not a materialized view")
        return _FakeCatalog.record


def _catalog_connector():
    from opteryx.connectors.opteryx_connector import OpteryxConnector

    return OpteryxConnector(catalog=_FakeCatalog)


def test_catalog_connector_reads_the_kebab_case_source_list():
    _FakeCatalog.record = {"sql": "SELECT 1", "source-tables": ["ops.a", "ops.b"]}
    assert _catalog_connector().materialized_view_sources("ops.mv") == ["ops.a", "ops.b"]


def test_a_record_without_the_key_yields_no_sources():
    """What reading the wrong key would look like. Harmless only because the
    binder refuses to transfer a view whose source list comes back empty."""
    _FakeCatalog.record = {"sql": "SELECT 1"}
    assert _catalog_connector().materialized_view_sources("ops.mv") == []


def test_catalog_connector_rejects_a_relation_that_is_not_a_view():
    _FakeCatalog.record = None
    with pytest.raises(ValueError, match="not a materialized view"):
        _catalog_connector().materialized_view_sources("ops.mv")
