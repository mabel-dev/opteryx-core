"""
Performance tests are not intended to be ran as part of the regression set.

This tests the relative performance of different storage formats - results should be
used as instructive with caution - i.e. don't change formats between parquet and orc
based on these results. So many things affect the performance that 10th of a second
differences in this test are unlikely to be meaningful in real world situations.

Best of three runs, lower is better

(results in Debian)

500 cycles  orc_zstd        1.38 seconds    ▏
500 cycles  parquet_zstd    1.57 seconds    ▎
500 cycles  orc_snappy      1.64 seconds    ▎
500 cycles  parquet_lz4     1.72 seconds    ▎
500 cycles  parquet_snappy  1.76 seconds    ▎
500 cycles  arrow_lz4       6.28 seconds    ▊
500 cycles  arrow_zstd      9.67 seconds    █▎
500 cycles  jsonl took      18.6 seconds    ██▍
500 cycles  jsonl_zstd      23.5 seconds    ████

(results on M2 Mac - last updated 20260222)
500 cycles of draken took           20.76932 seconds  # draken doesn't support pushdowns intentionally
500 cycles of jsonl took            165.7076 seconds
500 cycles of parquet took          21.917919 seconds
500 cycles of parquet_snappy took   16.22569 seconds
500 cycles of parquet_lz4 took      14.091697 seconds


"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import time

import opteryx
from opteryx.connectors import DiskConnector


class Timer(object):
    def __init__(self, name="test"):
        self.name = name

    def __enter__(self):
        self.start = time.time_ns()

    def __exit__(self, type, value, traceback):
        print("{} took {} seconds".format(self.name, (time.time_ns() - self.start) / 1e9))


FORMATS = (
    "draken",
    "jsonl",
    "parquet",
    "parquet_snappy",
    "parquet_lz4",
)


if __name__ == "__main__":
    CYCLES = 500

    opteryx.register_workspace("testdata", DiskConnector)

    session = opteryx.session()

    for format in FORMATS:
        with Timer(f"{CYCLES} cycles of {format}"):
            for round in range(CYCLES):
                session.execute_to_arrow(
                    f"SELECT * FROM testdata.flat.formats.{format} WITH(NO_PARTITION);"
                )
