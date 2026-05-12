"""
opteryx.shared

Re-exports shared Opteryx infrastructure for use by tests and other modules.
"""

# Re-export compiled structures
from opteryx.compiled.structures.memory_pool import MemoryPool

__all__ = ["MemoryPool"]
