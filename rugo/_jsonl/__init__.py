# Fast JSONL reader with projection and predicate pushdown
# This module provides optimized JSONL scanning for large, structurally similar datasets

from . import _jsonl_reader

__all__ = ['read_jsonl', 'get_jsonl_schema']

# These will be bound from the compiled extension
try:
    from ._jsonl_reader import read_jsonl, get_jsonl_schema
except ImportError:
    # Extension not yet built or not available
    pass
