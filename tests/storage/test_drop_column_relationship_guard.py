# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`DROP COLUMN` against a column a relationship runs through.

Until this existed, dropping a column silently orphaned every relationship
declared on it: the rows stayed `active`, `information_schema` kept listing
them, and a BI client kept being handed a NavigationProperty onto a column that
no longer existed. Nothing noticed, which is the failure mode - not the drop.

WARNED AT PLAN TIME, because telling someone before the damage beats recording
it after. The split is by `origin`:

  asserted -> REFUSED. A person declared it, and the column is the entire
              content of the declaration. `RESTRICT` semantics: retract the
              claim, then drop the column.
  inferred -> WARNED. Blocking DDL on a machine's unanswered guess would let
              the inference job veto schema changes, which is worse than
              losing a proposal - and the proposal is not lost, it is marked
              broken after the drop.

This does NOT contradict "nothing is enforced, ever" (design §6.1). That rule
is about DML: a write whose VALUES break a relationship still succeeds. This is
DDL removing the object a declaration names.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError


def _setup(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session()


def _exec(session, sql):
    return list(session.execute_to_morsels(sql))


def _seeded(tmp_path):
    session = _setup(tmp_path)
    _exec(session, "CREATE TABLE ws.tickets (customer_ref INT64, subject VARCHAR)")
    _exec(session, "CREATE TABLE ws.customers (id INT64, name VARCHAR)")
    _exec(
        session,
        "ALTER TABLE ws.tickets ADD CONSTRAINT tickets_customer_fk "
        "FOREIGN KEY (customer_ref) REFERENCES ws.customers (id) NOT ENFORCED",
    )
    return session


def _relationships(connector, relation="ws.tickets"):
    return connector._read_relationships(relation.split("."))


def test_dropping_a_column_a_declared_relationship_runs_through_is_refused(tmp_path):
    session = _seeded(tmp_path)

    with pytest.raises(UnsupportedSyntaxError) as err:
        _exec(session, "ALTER TABLE ws.tickets DROP COLUMN customer_ref")

    # The message has to name the constraint, or the person is told they cannot
    # proceed without being told what to do about it.
    assert "tickets_customer_fk" in str(err.value)
    assert "DROP CONSTRAINT" in str(err.value)


def test_the_refusal_leaves_the_column_and_the_relationship_exactly_as_they_were(tmp_path):
    session = _seeded(tmp_path)
    connector = LocalStoreConnector(dataset="ws.tickets", store_root=str(tmp_path))

    with pytest.raises(UnsupportedSyntaxError):
        _exec(session, "ALTER TABLE ws.tickets DROP COLUMN customer_ref")

    (row,) = _relationships(connector)
    assert row["status"] == "active"
    columns = _column_names(session)
    assert "customer_ref" in columns


def _column_names(session, relation="ws.tickets"):
    for morsel in session.execute_to_morsels(f"SELECT * FROM {relation}"):
        if morsel is not None:
            return list(morsel.to_arrow().to_pydict().keys())
    return []


def test_retracting_the_constraint_first_lets_the_column_go(tmp_path):
    session = _seeded(tmp_path)

    # The escape hatch the refusal points at. Without one, a declared
    # relationship would make a column permanently undroppable.
    _exec(session, "ALTER TABLE ws.tickets DROP CONSTRAINT tickets_customer_fk")
    _exec(session, "ALTER TABLE ws.tickets DROP COLUMN customer_ref")

    assert "customer_ref" not in _column_names(session)


def test_a_column_no_relationship_runs_through_drops_normally(tmp_path):
    session = _seeded(tmp_path)

    _exec(session, "ALTER TABLE ws.tickets DROP COLUMN subject")

    assert "subject" not in _column_names(session)
    # And the relationship on the OTHER column is untouched - the guard is
    # about the column being dropped, not about the table having constraints.
    connector = LocalStoreConnector(dataset="ws.tickets", store_root=str(tmp_path))
    (row,) = _relationships(connector)
    assert row["status"] == "active"


def test_dropping_the_far_end_of_a_relationship_is_not_refused(tmp_path):
    # The refusal is deliberately one-sided. `ws.customers.id` is referenced by
    # a constraint declared on a DIFFERENT dataset, whose owner may hold a
    # grant this caller does not - so refusing here, or naming it in a message,
    # would disclose data the caller cannot read (design §8.2). It is broken
    # after the fact instead, and its owner is told through their own catalog.
    session = _seeded(tmp_path)

    _exec(session, "ALTER TABLE ws.customers DROP COLUMN id")

    assert "id" not in _column_names(session, "ws.customers")
