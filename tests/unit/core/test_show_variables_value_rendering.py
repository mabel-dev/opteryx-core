# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`SHOW VARIABLES` renders composite values as JSON.

The `value` column is text, and the ARRAY variables (memberships, entitlements,
access_policies, architecture) used to be rendered with `str()` — a Python repr,
single-quoted, which no JSON reader can parse. A client displaying the table could
show it but nothing could read it. Scalars keep their `str()` form deliberately:
JSON-encoding them would wrap every VARCHAR in quotes for no gain.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.managers.virtual_datasets.variables_data import _render

POLICIES = [
    {"pattern": "benchmarks.*", "role": "reader"},
    {"pattern": "opteryx.*", "role": "owner"},
]


def _values(**session_kwargs):
    session = opteryx.session(user="bastian", **session_kwargs)
    for morsel in session.execute_to_morsels("SHOW VARIABLES;"):
        return {r[0]: (r[1], r[2]) for r in morsel}
    return {}


def test_array_variables_are_json():
    shown = _values(
        entitlements=["platform_admin"],
        memberships=["opteryx"],
        access_policies=POLICIES,
    )
    assert json.loads(shown["access_policies"][0]) == POLICIES
    assert json.loads(shown["user_memberships"][0]) == ["opteryx"]
    assert json.loads(shown["user_entitlements"][0]) == ["platform_admin"]
    # architecture is detected from the CPU, so its CONTENT isn't pinned here — that
    # it parses as a JSON list is the contract.
    assert isinstance(json.loads(shown["architecture"][0]), list)


def test_every_array_typed_variable_parses_as_json():
    # Pinned against the TYPE column rather than a hardcoded list of names, so a
    # variable added to the table later cannot quietly reintroduce a Python repr.
    shown = _values(
        entitlements=["platform_admin"],
        memberships=["opteryx"],
        access_policies=POLICIES,
    )
    arrays = {name: value for name, (value, kind) in shown.items() if kind == "ARRAY"}
    assert arrays, "expected some ARRAY variables"
    for name, value in arrays.items():
        assert isinstance(json.loads(value), list), f"{name} is not a JSON array: {value!r}"


def test_scalars_keep_their_plain_text_form():
    # Not JSON: a VARCHAR must not gain quotes, and a number must not gain a type.
    shown = _values()
    assert shown["character_set_client"][0] == "utf8"
    assert shown["sql_select_limit"][0] == "1073741824"


def test_render_does_not_fail_on_an_unserializable_element():
    # An ad-hoc `SET @x = ...` can carry element types with no JSON form. This column
    # is a rendering; one exotic variable must not take the whole listing down.
    import datetime

    assert _render([datetime.date(2026, 8, 10)]) == '["2026-08-10"]'


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
