# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import os
import sys

# Ensure tests import the local `opteryx` package from the repo rather than an installed package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import pytest

from opteryx.exceptions import PermissionsError
from opteryx.managers.permissions import can_perform_action
from opteryx.models import ExecutionContext


def test_local_table_allows_read_only():
    ctx = ExecutionContext(user="alice", memberships=[])

    assert can_perform_action(ctx, "local_table")
    assert not can_perform_action(ctx, "local_table", "WRITE")
    assert not can_perform_action(ctx, "local_table", "DELETE")


def test_public_schema_allows_read_only_even_with_policies():
    ctx = ExecutionContext(user="alice", memberships=[])
    # Even if a policy appears to grant owner, public.* must still be READ-only
    ctx.access_policies = [{"pattern": "public.*", "role": "owner"}]

    assert can_perform_action(ctx, "public.some_table", "READ")
    assert not can_perform_action(ctx, "public.some_table", "WRITE")
    assert not can_perform_action(ctx, "public.some_table", "DELETE")


def test_personal_schema_allows_all_actions_for_owner():
    ctx = ExecutionContext(user="bob", memberships=[])

    assert can_perform_action(ctx, "personal.bob.mytable", "READ")
    assert can_perform_action(ctx, "personal.bob.mytable", "WRITE")
    assert can_perform_action(ctx, "personal.bob.mytable", "DELETE")


def test_writer_and_owner_roles_grant_write_and_delete():
    ctx = ExecutionContext(user="carol", memberships=[])

    ctx.access_policies = [{"pattern": "db.schema.table", "role": "writer"}]
    assert can_perform_action(ctx, "db.schema.table", "READ")
    assert can_perform_action(ctx, "db.schema.table", "WRITE")
    assert can_perform_action(ctx, "db.schema.table", "DELETE")

    ctx.access_policies = [{"pattern": "db.schema.table", "role": "owner"}]
    assert can_perform_action(ctx, "db.schema.table", "READ")
    assert can_perform_action(ctx, "db.schema.table", "WRITE")
    assert can_perform_action(ctx, "db.schema.table", "DELETE")


def test_reader_role_only_allows_read():
    ctx = ExecutionContext(user="dave", memberships=[])

    ctx.access_policies = [{"pattern": "db.*", "role": "reader"}]

    assert can_perform_action(ctx, "db.sometable", "READ")
    assert not can_perform_action(ctx, "db.sometable", "WRITE")
    assert not can_perform_action(ctx, "db.sometable", "DELETE")


def test_wildcard_pattern_matching():
    ctx = ExecutionContext(user="erin", memberships=[])

    ctx.access_policies = [{"pattern": "db.*", "role": "writer"}]
    assert can_perform_action(ctx, "db.table1", "WRITE")
    assert not can_perform_action(ctx, "other.table1", "WRITE")

    ctx.access_policies = [{"pattern": "*.table", "role": "reader"}]
    assert can_perform_action(ctx, "db.table", "READ")
    assert not can_perform_action(ctx, "db.other_table", "READ")


def test_missing_role_defaults_to_reader():
    ctx = ExecutionContext(user="frank", memberships=[])

    ctx.access_policies = [{"pattern": "db.table"}]  # no role -> reader
    assert can_perform_action(ctx, "db.table", "READ")
    assert not can_perform_action(ctx, "db.table", "WRITE")


def test_unknown_action_returns_false_even_with_owner():
    ctx = ExecutionContext(user="gina", memberships=[])

    ctx.access_policies = [{"pattern": "*", "role": "owner"}]
    # The action is unknown to the ACTION_MAP, so it should be denied
    assert not can_perform_action(ctx, "any.schema.table", "FLY")


def test_malformed_policy_raises_permissions_error():
    ctx = ExecutionContext(user="hank", memberships=[])

    # A non-dict entry will cause .get(...) to raise and should be converted to PermissionsError
    ctx.access_policies = [None]

    with pytest.raises(PermissionsError):
        can_perform_action(ctx, "db.table", "READ")


def test_role_matching_is_case_sensitive():
    ctx = ExecutionContext(user="irene", memberships=[])

    ctx.access_policies = [{"pattern": "db.table", "role": "WRITER"}]
    # 'WRITER' (uppercase) should NOT match the lowercase 'writer' expected by ACTION_MAP
    assert not can_perform_action(ctx, "db.table", "WRITE")


def test_multiple_policies_any_match():
    ctx = ExecutionContext(user="jules", memberships=[])

    ctx.access_policies = [
        {"pattern": "alpha.*", "role": "reader"},
        {"pattern": "beta.*", "role": "writer"},
    ]

    assert can_perform_action(ctx, "alpha.table", "READ")
    assert not can_perform_action(ctx, "alpha.table", "WRITE")
    assert can_perform_action(ctx, "beta.table", "WRITE")

