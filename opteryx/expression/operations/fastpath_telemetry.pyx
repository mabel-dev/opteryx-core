"""Fastpath telemetry counters for filter operations."""


cdef dict _FASTPATH_TELEMETRY = {
    "draken_dict_expr_fastpath_hits": 0,
    "draken_dict_expr_fastpath_fallbacks": 0,
    "draken_constant_predicate_fastpath_hits": 0,
    "draken_constant_predicate_fastpath_fallbacks": 0,
}


cpdef reset_fastpath_telemetry():
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_hits"] = 0
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_fallbacks"] = 0
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_hits"] = 0
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_fallbacks"] = 0


cpdef dict get_fastpath_telemetry():
    return dict(_FASTPATH_TELEMETRY)


cpdef record_dict_fastpath_hit():
    _FASTPATH_TELEMETRY["draken_dict_expr_fastpath_hits"] += 1


cpdef record_constant_fastpath_hit():
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_hits"] += 1


cpdef record_constant_fastpath_fallback():
    _FASTPATH_TELEMETRY["draken_constant_predicate_fastpath_fallbacks"] += 1
