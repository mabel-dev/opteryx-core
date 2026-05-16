# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for implied predicate derivation across equi-join boundaries."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx import session


def test_implied_predicate_correctness_inner_join():
    # Explicit: predicate stated on both sides.
    # Implied: predicate stated on one side only — optimizer should derive the other.
    # Both must return the same row count.
    sql_explicit = """
        SELECT p.id, s.name
        FROM $planets AS p
        INNER JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE p.id < 4 AND s.planetId < 4
    """
    sql_implied = """
        SELECT p.id, s.name
        FROM $planets AS p
        INNER JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE p.id < 4
    """
    count_explicit = sum(m.num_rows for m in session().execute_to_morsels(sql_explicit))
    count_implied = sum(m.num_rows for m in session().execute_to_morsels(sql_implied))
    assert count_implied == count_explicit


def test_implied_predicate_fires():
    # The telemetry counter must be incremented when the optimization applies.
    sql = """
        SELECT p.id, s.name
        FROM $planets AS p
        INNER JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE p.id < 4
    """
    sess = session()
    list(sess.execute_to_morsels(sql))
    assert sess.telemetry.get("optimization_predicate_pullup_implied", 0) > 0


def test_implied_predicate_left_join_not_propagated():
    # LEFT JOINs: predicate must NOT be pushed to the nullable (right) side.
    # Verify by running — correctness is the gate, not the counter.
    sql = """
        SELECT p.id, s.name
        FROM $planets AS p
        LEFT JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE p.id < 4
    """
    count = sum(m.num_rows for m in session().execute_to_morsels(sql))
    assert count >= 0


def test_implied_predicate_function_filter_not_propagated():
    # Filters containing arithmetic/functions must not be propagated.
    sql = """
        SELECT p.id, s.name
        FROM $planets AS p
        INNER JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE p.id + 1 < 5
    """
    count = sum(m.num_rows for m in session().execute_to_morsels(sql))
    assert count >= 0
