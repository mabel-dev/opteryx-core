# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for NullabilityInferenceStrategy."""

from opteryx import session


def test_nullability_inference_executes():
    """Test that a query with INNER JOIN executes without error."""
    sql = """
    SELECT p.id, s.name
    FROM $planets AS p
    INNER JOIN testdata.satellites AS s ON p.id = s.planetId
    """

    sess = session()
    morsels = sess.execute_to_morsels(sql)
    for _ in morsels:
        pass
    assert True, "Query executed without error"
