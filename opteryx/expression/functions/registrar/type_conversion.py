# Registrar: type conversion functions
#
# Historically type conversions were exposed as functions in the registrar,
# but CAST / ::type syntax are handled specially by the planner and binder.
# Keep this module present so the registrar package is complete, but do not
# expose any runtime function definitions here — return an empty list.
#
# If in the future CAST is implemented as first-class functions, the helper
# `_make` exported by the registrar package can be used to add entries here.

from __future__ import annotations

from typing import List

from opteryx.expression.functions import FunctionDefinition

# Import registrar helpers for consistency with other domain modules.
# They are currently unused because type conversions are handled elsewhere,
# but keeping the import makes future migrations straightforward.
from opteryx.expression.functions.registrar import _make  # noqa: F401


def get_builtin_type_conversion_functions() -> List[FunctionDefinition]:
    """
    Type casting functions (placeholder - NOT part of the public function catalog).

    Notes:
    - CAST(x AS type) and the shorthand x::type are processed as specialized
      operations by the planner and binder and should not be exposed as regular
      catalog functions. The planner converts cast expressions to internal forms
      that avoid function-call overhead.
    - Returning an empty list ensures the module is discoverable by the top-level
      `get_builtin_functions()` collector while preventing casts from being
      treated as normal functions by name-based checks.
    """
    return []
