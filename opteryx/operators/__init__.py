# isort: skip

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


# Must precede the `._operators` import below: `_operators.so` #includes
# rugo/src/parquet/io_pipeline.hpp directly (native_parquet_scan_source.hpp) and
# needs its rugo decode symbols (e.g. DecodeColumnFromChunk), which are compiled
# only into pool_reader.so. Importing parquet_io here triggers its RTLD_GLOBAL
# load of pool_reader (see opteryx/connectors/parquet_io/__init__.py) before
# `_operators.so` is dlopen'd, regardless of what the caller imports first.
import opteryx.connectors.parquet_io  # noqa: F401

from .catalog import OperatorCategory, OperatorParallelism, ParallelStrategy, get_registry
from ._operators import (
    BasePlanNode,
    JoinNode,
    JoinLeftAdapter,
    JoinRightAdapter,
    PipelineContext,
)
from .read import ReaderNode

__all__ = [
    "BasePlanNode",
    "JoinNode",
    "JoinLeftAdapter",
    "JoinRightAdapter",
    "PipelineContext",
    "OperatorCategory",
    "OperatorParallelism",
    "ParallelStrategy",
    "get_registry",
    "ReaderNode",
]
