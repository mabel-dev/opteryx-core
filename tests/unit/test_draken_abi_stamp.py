"""
The draken ABI stamp: every package that bundles draken must agree with the
draken it is sitting next to.

opteryx_core, rugo and libskene are three distributions that install the SAME
`draken/` package to the SAME path, so whichever pip installs LAST wins. The
stamp (build_common.write_draken_abi_modules) gives draken an identity so that
overlay fails loudly at import instead of as an undefined `draken_*` symbol on
the first query — the shape of the 0.9.56 outage.

These tests assert the property the check depends on: a single build produces
one stamp, and every consumer in the tree carries exactly that one.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

CONSUMER_PACKAGES = ("opteryx", "rugo", "skene")


def test_draken_carries_a_stamp():
    """draken must be stamped at all — an unstamped draken cannot be matched."""
    from draken._abi_stamp import DRAKEN_ABI_STAMP

    assert isinstance(DRAKEN_ABI_STAMP, str)
    assert len(DRAKEN_ABI_STAMP) == 16, DRAKEN_ABI_STAMP


@pytest.mark.parametrize("package", CONSUMER_PACKAGES)
def test_consumer_agrees_with_installed_draken(package):
    """Each bundling package must require the stamp the installed draken carries.

    A failure here means the tree holds a draken from one build and a consumer
    from another — exactly the install-overlay state the check exists to catch,
    reproduced locally.
    """
    from draken._abi_stamp import DRAKEN_ABI_STAMP

    module = __import__(f"{package}._draken_abi", fromlist=["REQUIRED_DRAKEN_ABI_STAMP"])

    assert module.REQUIRED_DRAKEN_ABI_STAMP == DRAKEN_ABI_STAMP, (
        f"{package} was built against draken ABI {module.REQUIRED_DRAKEN_ABI_STAMP} "
        f"but the draken in this tree carries {DRAKEN_ABI_STAMP} — rebuild."
    )


@pytest.mark.parametrize("package", CONSUMER_PACKAGES)
def test_check_rejects_a_foreign_draken(package):
    """The check must actually raise on a mismatch, not just compare and shrug."""
    import draken._abi_stamp as stamp_module

    module = __import__(f"{package}._draken_abi", fromlist=["check_draken_abi"])

    original = stamp_module.DRAKEN_ABI_STAMP
    stamp_module.DRAKEN_ABI_STAMP = "0000000000000000"
    try:
        with pytest.raises(ImportError, match="draken ABI mismatch"):
            module.check_draken_abi()
    finally:
        stamp_module.DRAKEN_ABI_STAMP = original


def test_stamp_is_deterministic_for_a_tree_state():
    """Same tree, same stamp — otherwise every build would look like a mismatch."""
    from build_common import draken_abi_stamp

    assert draken_abi_stamp() == draken_abi_stamp()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
