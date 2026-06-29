"""
rugo — a fast, dependency-free file engine for Parquet, CSV, and JSONL.

Reading **and** writing, with zero heavy dependencies: no PyArrow, no NumPy on
any path. Compiled as C++/Cython extensions. Readers emit Draken vectors; the
writers consume Draken Morsels — the bundled ``draken`` columnar substrate.

    from rugo import parquet
    with parquet.read_parquet("data.parquet", columns=["id", "name"]) as r:
        for morsel in r:
            ...
"""

import draken  # load draken_native.so before rugo_native.so resolves its symbols

from rugo.__version__ import __version__

__all__ = ["__version__"]
