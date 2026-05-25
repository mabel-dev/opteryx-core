"""Draken Vector types — columnar data representation."""

from draken.draken_native import Vector

# E.21a: In new draken all vectors are the same Vector class.
# Per-type subclasses (BoolVector, Integer64Vector, etc.) are aliases until E.21b rewrites.
BoolVector = Vector

__all__ = ["Vector", "BoolVector"]
