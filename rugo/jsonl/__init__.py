# Fast JSONL reader with projection and predicate pushdown
# This module provides optimized JSONL scanning for large, structurally similar datasets

from ._jsonl_reader import get_jsonl_schema, read_jsonl
from ._jsonl_writer import write_jsonl

__all__ = ["read_jsonl", "get_jsonl_schema", "write_jsonl"]
