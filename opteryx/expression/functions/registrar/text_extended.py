"""Registrar: text_extended group.

This module re-exports the extended text registrar getter from the combined
`text` registrar module.
"""

from opteryx.expression.functions.registrar.text import get_builtin_text_extended_functions

__all__ = ["get_builtin_text_extended_functions"]
