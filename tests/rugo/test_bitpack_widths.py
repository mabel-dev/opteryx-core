# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
RLE/bit-packed dictionary-code decoding across every code bit-width.

The dictionary code stream is bit-packed at ceil(log2(dict_size)) bits per
code. Widths 9-32 used to be decoded by assembling each value from up to five
source bytes in an inner loop; they now use a single unaligned 64-bit load
(scalar) or a vector gather+variable-shift (NEON / AVX2). Those fast paths
deliberately overread past the current group of 8, so the LAST group of every
run must fall back to the bounds-checked helper — otherwise the decoder reads
past the end of the page buffer.

This exercises each width band with row counts that are deliberately NOT
multiples of 8, so the partial-group remainder path runs too, and oracle-checks
every value against the source list.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

_BASE = "bitpack_widths_tmp"


def _expected_code_width(ndv: int) -> int:
    """Bits per packed dictionary code for a dictionary of `ndv` entries."""
    return max(1, (ndv - 1).bit_length())


# (ndv, rows) — ndv picks the code bit-width, rows is never a multiple of 8.
_CASES = [
    (2, 101),  # bw 1
    (5, 103),  # bw 3
    (256, 251),  # bw 8  (boundary: last width before the fast path)
    (300, 257),  # bw 9  (first width using the fast path)
    (1024, 511),  # bw 10
    (5000, 1013),  # bw 13
    (65536, 4099),  # bw 16 (last NEON/AVX2 wide-kernel width)
    (100_000, 8191),  # bw 17 (first scalar-fast width)
    (1_000_000, 16_381),  # bw 20
]


@pytest.mark.parametrize("ndv, rows", _CASES, ids=[f"ndv{n}_rows{r}" for n, r in _CASES])
@pytest.mark.parametrize("nullable", [False, True], ids=["non_nullable", "nullable"])
def test_bitpacked_dict_codes_roundtrip(ndv, rows, nullable):
    """Every dictionary code width must decode to exactly the written values."""
    # Values cycle through `ndv` distinct entries so the dictionary reaches the
    # target size; spread them so codes are not monotonic within a group.
    vals = [(i * 7919) % ndv for i in range(rows)]
    if nullable:
        vals = [None if i % 13 == 4 else v for i, v in enumerate(vals)]

    folder = os.path.join(_BASE, f"ndv{ndv}_r{rows}_{int(nullable)}")
    os.makedirs(folder, exist_ok=True)
    try:
        pq.write_table(
            pa.table({"v": pa.array(vals, pa.int64())}),
            os.path.join(folder, "p.parquet"),
            use_dictionary=True,
            # Keep the whole column in one chunk so the dictionary really does
            # reach `ndv` entries rather than resetting per row group.
            dictionary_pagesize_limit=16 << 20,
            data_page_size=64 << 10,
        )
        got = []
        for _rg, cols in iter_row_groups_ipc(None, [f"{folder}/p.parquet"], ["v"]):
            got.extend(cols[next(iter(cols))].to_pylist())
        assert got == vals, f"ndv={ndv} (bw~{_expected_code_width(ndv)}) rows={rows}"
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def teardown_module(_module):
    shutil.rmtree(_BASE, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
