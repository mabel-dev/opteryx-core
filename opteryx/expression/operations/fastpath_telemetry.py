"""Fastpath telemetry and performance metrics for filter operations."""

_FASTPATH_TELEMETRY = {
    "draken_dict_expr_fastpath_hits": 0,
    "draken_dict_expr_fastpath_fallbacks": 0,
    "draken_constant_predicate_fastpath_hits": 0,
    "draken_constant_predicate_fastpath_fallbacks": 0,
}


def reset_fastpath_telemetry():
    """Reset fastpath telemetry counters."""
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_hits"] = 0
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_fallbacks"] = 0
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_hits"] = 0
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_fallbacks"] = 0


def get_fastpath_telemetry():
    """Get current fastpath telemetry snapshot."""
    return dict(_FASTPATH_TELEMETRY)


def record_dict_fastpath_hit():
    """Record a dictionary fastpath hit."""
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_hits"] += 1


def record_constant_fastpath_hit():
    """Record a constant fastpath hit."""
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_hits"] += 1


def record_constant_fastpath_fallback():
    """Record a constant fastpath fallback."""
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_fallbacks"] += 1
