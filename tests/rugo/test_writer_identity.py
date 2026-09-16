# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo's writer identity is readable from Python and matches the files it writes.

rugo ships in two distributions that stamp DIFFERENT parquet `created_by` text:

    opteryx_core wheel : "opteryx-rugo version <opteryx ver> (build <n>)"
    standalone rugo    : "rugo version <rugo ver>"

`rugo.__version__` is always the standalone rugo source version, so in the
bundled case it does NOT match the number in the footer — which left a consumer
debugging in the field with two irreconcilable numbers. `rugo.__writer_id__` is
the reconciliation: it is generated at build time from the very string that is
compiled into the writer, and these tests close the loop by reading it back out
of a file rugo really wrote.

PyArrow is used only as a read-side oracle (tests may use pyarrow).
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
import rugo
from rugo.parquet import write_parquet

# The substring IsTrustedRugoWriter (rugo/src/parquet/metadata.cpp) gates
# row-group sorting_columns on. Spelled out here rather than imported because
# the point is to catch the Python and C++ sides disagreeing.
TRUST_MARKER = "rugo"

DISTRIBUTIONS = ("opteryx_core", "rugo")


def _created_by(buf: bytes) -> str:
    import pyarrow.parquet as pq

    return pq.ParquetFile(io.BytesIO(buf)).metadata.created_by


def _write_a_file() -> bytes:
    morsel = list(opteryx.session().execute_to_morsels("SELECT 1 AS a"))[0]
    return write_parquet(morsel)


def test_writer_id_matches_the_footer_it_writes():
    """The whole point: what Python reports IS what lands in the file."""
    assert _created_by(_write_a_file()) == rugo.__writer_id__


def test_distribution_is_one_we_know():
    assert rugo.__distribution__ in DISTRIBUTIONS


def test_writer_id_spelling_follows_the_distribution():
    """Each wheel's stamp is distinguishable from the other's, from Python alone."""
    if rugo.__distribution__ == "opteryx_core":
        assert rugo.__writer_id__.startswith("opteryx-rugo version ")
        # The bundled build stamps the OPTERYX version, which is exactly why it
        # does not match rugo.__version__. Assert the divergence is real rather
        # than letting a future "tidy-up" quietly align them and hide the case
        # this module exists to document.
        assert "(build " in rugo.__writer_id__
    else:
        assert rugo.__writer_id__ == "rugo version %s" % rugo.__version__


def test_writer_id_carries_the_trust_marker():
    """A stamp without 'rugo' makes rugo distrust its own sortedness claims."""
    assert TRUST_MARKER in rugo.__writer_id__
    assert TRUST_MARKER in _created_by(_write_a_file())


def test_version_is_still_the_standalone_rugo_version():
    """__version__ keeps its meaning — __writer_id__ is additive, not a rename."""
    from rugo.__version__ import __version__ as source_version

    assert rugo.__version__ == source_version


if __name__ == "__main__":  # pragma: no cover
    test_writer_id_matches_the_footer_it_writes()
    test_distribution_is_one_we_know()
    test_writer_id_spelling_follows_the_distribution()
    test_writer_id_carries_the_trust_marker()
    test_version_is_still_the_standalone_rugo_version()
    print("✅ okay")
