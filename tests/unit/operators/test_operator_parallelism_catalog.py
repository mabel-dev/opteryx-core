"""WP-7 — operator parallelism classification (catalog metadata).

Every registered operator carries an `OperatorParallelism` class stating how a
parallel engine may run it (see docs/EXECUTION_THREAD_SAFETY_CONTRACT.md). This
is metadata only — the serial engine ignores it — but it is the contract a
parallel scheduler will rely on, so it is locked here:

  * every operator has a valid classification;
  * the contract-critical operators match the thread-safety contract;
  * the conservative default (STATEFUL_SERIAL) covers everything not explicitly
    promoted, so a NEW operator is serial-safe until someone classifies it.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from opteryx.operators import OperatorParallelism, get_registry


def _by_name():
    reg = get_registry()
    return {meta.name: meta for meta in (reg.get(c) for c in reg.list())}


def test_every_operator_has_a_valid_parallelism_class():
    metas = _by_name()
    assert metas, "registry is empty"
    for name, meta in metas.items():
        assert isinstance(meta.parallelism, OperatorParallelism), (
            f"{name} has no valid parallelism classification"
        )


# Contract-critical classifications — these drive what a parallel engine may do,
# so a change here must be a conscious contract change, not an accident.
EXPECTED = {
    # stateless — clone/share freely
    "Reader": OperatorParallelism.STATELESS,
    "Parquet Reader": OperatorParallelism.STATELESS,
    "Null Reader": OperatorParallelism.STATELESS,
    "Function Dataset": OperatorParallelism.STATELESS,
    "Filter": OperatorParallelism.STATELESS,
    "Projection": OperatorParallelism.STATELESS,
    # stateful mergeable — clone per worker + merge()
    "Distinct": OperatorParallelism.STATEFUL_MERGEABLE,
    "Aggregate": OperatorParallelism.STATEFUL_MERGEABLE,
    "Aggregate and Group": OperatorParallelism.STATEFUL_MERGEABLE,
    # singleton — one instance joins N inputs / terminal
    "Union": OperatorParallelism.SINGLETON,
    "Exit": OperatorParallelism.SINGLETON,
    # stateful serial — must see all input on one instance (safe default)
    "Sort": OperatorParallelism.STATEFUL_SERIAL,
    "Heap Sort": OperatorParallelism.STATEFUL_SERIAL,
    "Limit": OperatorParallelism.STATEFUL_SERIAL,
    "Window": OperatorParallelism.STATEFUL_SERIAL,
    "Inner Join": OperatorParallelism.STATEFUL_SERIAL,
    "Outer Join": OperatorParallelism.STATEFUL_SERIAL,
    "Cross Join": OperatorParallelism.STATEFUL_SERIAL,
}


@pytest.mark.parametrize("name,expected", list(EXPECTED.items()))
def test_contract_critical_classifications(name, expected):
    meta = _by_name().get(name)
    assert meta is not None, f"operator {name!r} not registered"
    assert meta.parallelism == expected, (
        f"{name}: expected {expected}, got {meta.parallelism} — "
        "this is a thread-safety contract change, make it deliberately"
    )


def test_default_is_conservative_serial():
    # An unclassified operator must default to the serial-safe class, so a new
    # operator can never silently become "parallelisable" without a decision.
    from opteryx.operators.catalog import OperatorMetadata

    assert (
        OperatorMetadata.__dataclass_fields__["parallelism"].default
        == OperatorParallelism.STATEFUL_SERIAL
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
