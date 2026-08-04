"""
A C++ exception thrown inside pool_reader.so must arrive in Python as a catchable
RuntimeError — not kill the process.

WHY THIS EXISTS (production crash, 2026-08-04): worker.opteryx returned a Cloud Run
503 with a faulthandler dump whose C stack showed a throw raised in pool_reader.so
being dispatched through __cxa_throw resolved inside *draken_native.so*, then
gsignal — i.e. abort(). Not a SIGSEGV: that is std::terminate() firing because the
unwinder found no handler, so Cython's `except +` never got the chance to turn the
throw into a Python exception. Root cause was every C++ extension carrying its own
statically linked libstdc++/libgcc (LD_EXTRA) while exporting it
(-fvisibility=default), with draken/__init__.py publishing draken_native's copy
process-globally via RTLD_GLOBAL so it interposed on everyone else's. Fixed by
adding -Wl,--exclude-libs,ALL to the Linux LD_EXTRA (see build_common.py, and
docs/NATIVE_EXTENSION_CONSOLIDATION_DESIGN.md).

WHY THE EXISTING TESTS DID NOT CATCH IT: test_http_retry.py already asserts
pytest.raises(RuntimeError) on a C++ throw, and passes — but it imports
opteryx.compiled.http_client, a DIFFERENT .so from pool_reader, and never loads
draken_native. With no RTLD_GLOBAL load there is no interposition and no abort.
Reproducing the failure requires the production import order, which is why the
draken import below is load-bearing and must stay first.

This is a LINUX regression test in substance. On macOS two-level namespaces bind
each image to its own runtime and -static-libgcc is not even applied, so it passes
either way and proves nothing locally — it earns its keep in CI (ubuntu-latest,
regression_suite.yaml). Against a pre-fix Linux build these tests do not fail, they
take the interpreter down.

The throw sites are FetchParquetFooter's guards in rugo/src/parquet/filesystem.hpp,
compiled INTO pool_reader.so — the same function that threw in the crash.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Load order is the point of this test, not incidental. Importing draken runs
# draken/__init__.py, which loads draken_native under RTLD_GLOBAL — exactly what
# production does and what test_http_retry.py does not. pool_reader must be
# imported AFTER it, so its C++ runtime symbol resolution happens in the presence
# of the global-namespace publisher. Do not reorder, and do not let an import
# sorter reorder it.
import draken  # noqa: E402  isort:skip

from opteryx.connectors.parquet_io.pool_reader import (  # noqa: E402
    fetch_column_chunk_info,
)

# (label, bytes) -> each trips a different guard in FetchParquetFooter.
# `fetch_column_chunk_info` is used rather than the more obvious
# `fetch_column_stats` because both reach the same C++ throw through
# _read_footer_payload, and this one is not currently being reworked by the
# plan-time signing change. Any pool_reader entry point that throws would do;
# what is under test is the unwind, not this function.
MALFORMED = [
    ("truncated below the 8-byte trailer", b"PAR1"),
    ("missing PAR1 magic at EOF", b"NOTPARQUET" + b"\x00" * 40),
    ("footer length larger than the file", b"\x00" * 8 + b"\xff\xff\xff\xffPAR1"),
]


def test_draken_native_is_loaded_globally():
    """Guard the precondition. If draken_native is not loaded, the tests below
    still pass but no longer exercise the interposition scenario, and would go on
    passing while silently covering nothing."""
    assert "draken.draken_native" in sys.modules, (
        "draken_native must be imported (RTLD_GLOBAL) before pool_reader for this "
        "module to reproduce the production configuration"
    )


@pytest.mark.parametrize("label, payload", MALFORMED, ids=[m[0] for m in MALFORMED])
def test_malformed_parquet_raises_instead_of_aborting(label, payload):
    """A throw inside pool_reader.so unwinds into Python as RuntimeError.

    Reaching the assert at all is most of the signal: on a broken build the
    process dies here and pytest reports no result for this test.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
        handle.write(payload)
        path = handle.name
    try:
        with pytest.raises(RuntimeError) as excinfo:
            fetch_column_chunk_info(path, 0, ["any_column"])
        # Confirm it is the parquet footer guard that fired, not an unrelated
        # RuntimeError that would make this test vacuous.
        assert path in str(excinfo.value) or "arquet" in str(excinfo.value)
    finally:
        os.unlink(path)


def test_missing_file_raises_instead_of_aborting():
    """The stat() failure path — a throw raised before any file content is read."""
    missing = os.path.join(tempfile.gettempdir(), "definitely_not_here_9f3a1c.parquet")
    assert not os.path.exists(missing)
    with pytest.raises(RuntimeError) as excinfo:
        fetch_column_chunk_info(missing, 0, ["any_column"])
    assert missing in str(excinfo.value)


def test_throw_does_not_poison_later_calls():
    """The process stays usable after an unwind.

    A half-unwound C++ exception can leave the runtime in a state where the NEXT
    throw aborts even though the first appeared to be handled — so one successful
    raise is not sufficient evidence. Throw repeatedly and confirm each one is
    still catchable.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
        handle.write(b"NOTPARQUET" + b"\x00" * 40)
        path = handle.name
    try:
        for _ in range(5):
            with pytest.raises(RuntimeError):
                fetch_column_chunk_info(path, 0, ["any_column"])
    finally:
        os.unlink(path)


if __name__ == "__main__":  # pragma: no cover
    test_draken_native_is_loaded_globally()
    for _label, _payload in MALFORMED:
        test_malformed_parquet_raises_instead_of_aborting(_label, _payload)
    test_missing_file_raises_instead_of_aborting()
    test_throw_does_not_poison_later_calls()
    print("✅ okay")
