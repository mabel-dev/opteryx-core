# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SET binds and folds its value to one literal, against the session's variables.

An unset user variable reads NULL in its OWN assignment only, so
`SET @x = COALESCE(@x, <default>)` sets a default without overwriting a value that
is already there (ruled 2026-11-07). Everywhere else an unset variable still raises.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.exceptions import PermissionsError, UnsupportedSyntaxError, VariableNotFoundError

DEFAULT_IP = "SET @ip = COALESCE(@ip, '192.168.4.80'::ipv4)::ipv4"


def _one(session, sql):
    for morsel in session.execute_to_morsels(sql):
        return tuple(next(iter(morsel)))


def _run(session, sql):
    list(session.execute_to_morsels(sql))


def test_unset_self_reference_takes_the_default():
    session = opteryx.session()
    _run(session, DEFAULT_IP)
    assert _one(session, "SELECT CAST(@ip AS VARCHAR)") == ("192.168.4.80",)


def test_set_self_reference_keeps_the_existing_value():
    session = opteryx.session()
    _run(session, "SET @ip = '10.0.0.1'::ipv4")
    _run(session, DEFAULT_IP)
    assert _one(session, "SELECT CAST(@ip AS VARCHAR)") == ("10.0.0.1",)


def test_another_unset_variable_still_raises():
    session = opteryx.session()
    with pytest.raises(VariableNotFoundError):
        _run(session, "SET @ip = COALESCE(@other, '192.168.4.80'::ipv4)")


def test_value_expression_is_folded():
    session = opteryx.session()
    _run(session, "SET @n = 1 + 2")
    assert _one(session, "SELECT @n") == (3,)


def test_value_that_does_not_fold_is_refused():
    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError):
        _run(session, "SET @r = RANDOM()")


def test_set_at_at_name_sets_the_system_variable():
    session = opteryx.session()
    _run(session, "SET @@nprobe = 4")
    assert _one(session, "SELECT @@nprobe") == (4,)
    _run(session, "SET @@nprobe = @@nprobe + 1")
    assert _one(session, "SELECT @@nprobe") == (5,)


def test_set_at_at_name_is_permission_checked():
    session = opteryx.session()
    with pytest.raises(PermissionsError):
        _run(session, "SET @@sql_select_limit = 5")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
