# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Billing events are printed only where `EMIT_BILLING_EVENTS` says so.

The switch used to be `K_SERVICE` - set by Cloud Run, so anything else (the VM
fleet) silently billed nothing. It is now one explicit setting, off by default.
"""

import importlib
import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx import config
from opteryx.managers.billing import BillingEventType
from opteryx.managers.billing import write_billing_event


def _emit():
    write_billing_event(
        billing_event=BillingEventType.QUERY_EXECUTION,
        billing_account="acct",
        actor="someone",
        event_details={"user": "someone", "query": "SELECT 1"},
    )


def test_off_prints_nothing(monkeypatch, capsys):
    monkeypatch.setattr(config, "EMIT_BILLING_EVENTS", False)
    # The old gate must no longer turn emission on.
    monkeypatch.setenv("K_SERVICE", "worker")
    _emit()
    assert capsys.readouterr().out == ""


def test_on_prints_one_billing_line(monkeypatch, capsys):
    monkeypatch.setattr(config, "EMIT_BILLING_EVENTS", True)
    monkeypatch.delenv("K_SERVICE", raising=False)
    _emit()
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["severity"] == "BILLING"
    assert record["billing_event"] == "QUERY_EXECUTION"
    assert record["billing_account"] == "acct"
    # RFC 3339 UTC: one zone designator, "Z" - not "+00:00Z".
    assert record["timestamp"].endswith("Z") and "+00:00" not in record["timestamp"]


def test_off_still_validates(monkeypatch):
    """Off means not printed, not unchecked: a malformed event fails everywhere."""
    monkeypatch.setattr(config, "EMIT_BILLING_EVENTS", False)
    with pytest.raises(ValueError):
        write_billing_event(
            billing_event=BillingEventType.QUERY_EXECUTION,
            billing_account="acct",
            event_details={"user": "someone"},
        )


@pytest.mark.parametrize("value, expected", [(None, False), ("true", True), ("0", False)])
def test_setting_is_read_from_the_environment(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("EMIT_BILLING_EVENTS", raising=False)
    else:
        monkeypatch.setenv("EMIT_BILLING_EVENTS", value)
    assert importlib.reload(config).EMIT_BILLING_EVENTS is expected
    monkeypatch.delenv("EMIT_BILLING_EVENTS", raising=False)
    importlib.reload(config)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
