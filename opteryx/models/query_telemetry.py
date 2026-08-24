# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from collections import defaultdict

from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import compose
from opteryx.exceptions import md_cause
from opteryx.exceptions import md_code


class _QueryTelemetry:
    def __init__(self):
        self.reset()

    def reset(self):
        """Clear all readings back to per-query defaults.

        Called at the start of each query on a long-lived Session: the
        instance is a singleton keyed by query_id (see QueryTelemetry.__new__
        below), so a Session reusing the same query_id across many
        execute_to_morsels() calls must reset here, not reconstruct, or
        messages/optimizer_trace grow unbounded for the life of the process.
        """
        # predefine "messages" and "operations" so all new telemetry default to 0
        self._reading: dict = defaultdict(int)
        self._reading["messages"] = []
        self._reading["operations"] = {}
        self._reading["optimizer_trace"] = []
        self._reading["optimizer_decisions"] = []

    def _ns_to_s(self, nano_seconds: int) -> float:
        """convert elapsed ns to s"""
        if nano_seconds == 0:
            return 0
        return nano_seconds / 1e9

    def __getattr__(self, attr):
        """allow access using telemetry.reading_name"""
        return self._reading[attr]

    def __setattr__(self, attr, value):
        """allow access using telemetry.reading_name"""
        if attr == "_reading":
            super().__setattr__(attr, value)
        else:
            self._reading[attr] = value

    def increase(self, attr: str, amount: float = 1.0):
        self._reading[attr] += amount

    def add_message(self, message: str):
        """collect warnings"""
        self._reading["messages"].append(message)

    def add_decision(self, label: str, detail: str):
        """Record one costed plan choice — an optimizer decision that compared
        concrete alternatives and picked one (or declined to move).

        ``label`` names the decision point (e.g. ``semi join pushdown``);
        ``detail`` states the outcome WITH the numbers it was decided on, so a
        wrong choice is diagnosable from EXPLAIN's text alone. Rendered in the
        OPTIMIZATIONS block alongside the ``optimization_*`` counters."""
        self._reading["optimizer_decisions"].append({"label": label, "detail": detail})

    def add_plan_rewrite(self, phase: str, strategy: str, before: tuple, after: tuple):
        """Record a plan-shape fingerprint change from one strategy application.

        Grade-A structural trace. ``before``/``after`` are ``(node_count,
        edge_count)`` snapshots taken around a single strategy run. Structural
        rewrites (node/edge add, remove, reshape) are visible here; expression-only
        rewrites that leave the graph shape unchanged do not move these counts and
        are surfaced by the per-strategy ``optimization_*`` counters instead."""
        self._reading["optimizer_trace"].append(
            {
                "phase": phase,
                "strategy": strategy,
                "nodes": [before[0], after[0]],
                "edges": [before[1], after[1]],
                "changed": before != after,
            }
        )

    def as_dict(self):
        """
        Return telemetry as a dictionary
        """
        import opteryx

        readings_dict = dict(self._reading)

        # Remove connector-level stats that should only appear in operation/sensor stats.
        # ``bytes_processed`` is deliberately NOT in this list: it is a query-wide total
        # (the dense LOGICAL bytes the plan reads, measured at plan time — see
        # planner/data_processed.py) rather than a per-node reading, it is what the
        # DATA_PROCESSED_BYTES billing event charges on, and popping it here is why the
        # query report could only ever show 0 bytes. Not to be confused with
        # ``io_bytes_fetched``, the COMPRESSED volume the IO pipeline measured.
        connector_only_keys = [
            "rows_read",
            "rows_seen",
            "blobs_read",
            "blobs_seen",
            "bytes_raw",
            "columns_read",
            "bytes_read",
            # per-operator native readings — surfaced via the ``operations`` breakdown
            # (mermaid.get_node_stats), never as a top-level telemetry key.
            "native_op_stats",
            # per-native-scan plan-time facts — overlaid onto the scan's operation
            # row by mermaid.get_node_stats, never a top-level key.
            "native_scan_facts",
        ]
        for key in connector_only_keys:
            readings_dict.pop(key, None)

        for k, v in readings_dict.items():
            # times are recorded in ns but reported in seconds
            if k.startswith("time_"):
                readings_dict[k] = self._ns_to_s(v)
        # `time_total` is the query's wall clock, and it is only computable once the
        # timing window has been CLOSED. `_reading` is a defaultdict(int), so an unset
        # `end_time` read back as 0 and `0 - start_time` was reported as a total of
        # around -1.79 billion seconds - a number no caller can tell apart from a
        # measurement. `Session.telemetry` stamps `end_time` before it calls here and
        # is the only sanctioned way in; anything that reaches as_dict() around it is
        # holding an open window and gets told so rather than handed a bogus reading.
        start_time = readings_dict.pop("start_time", 0)
        end_time = readings_dict.pop("end_time", 0)
        if start_time == 0 or end_time == 0:
            raise InvalidInternalStateError(
                compose(
                    "Query telemetry cannot be reported",
                    md_cause(
                        "the query was never started"
                        if start_time == 0
                        else "the query timing window was never closed"
                    ),
                    f"Read telemetry through {md_code('Session.telemetry')}, which closes"
                    f" the window, rather than calling {md_code('as_dict()')} on the"
                    " readings directly",
                )
            )
        readings_dict["time_total"] = self._ns_to_s(end_time - start_time)
        # sort the keys in the dictionary
        readings_dict = {key: readings_dict[key] for key in sorted(readings_dict)}
        # put messages and edges at the end
        readings_dict["version"] = opteryx.__version__
        readings_dict["messages"] = readings_dict.pop("messages", [])
        readings_dict["edges"] = readings_dict.pop("edges", [])
        return readings_dict


class QueryTelemetry(_QueryTelemetry):
    """Per-query readings, shared BY QUERY ID.

    This is a registry, not a constructor: two calls with the same `query_id`
    return the SAME object, which is how an operator deep in the engine records
    onto the readings the Session will later hand back. It follows that a caller
    who does not supply a query_id is asking for "whatever instance the last
    caller made" — every such call in a process aliases to one shared object and
    its counters accumulate across unrelated queries.

    That produced order-dependent green in the test suite: a test asserting a
    reading is ABSENT passed only while it happened to run before the tests that
    populate the shared instance, and failed — blaming the product — as soon as
    the order changed. A telemetry object with no owner is a bug at the call
    site, so it is refused here rather than silently aliased.

    Need a readings sink that belongs to nobody (constructing a strategy in a
    unit test, a throwaway during expression building)? Ask for one explicitly
    with `QueryTelemetry.detached()` — it is never registered, so nothing else
    can write to it and it cannot leak into another query's numbers.
    """

    slots = "_instances"

    _instances: dict[str, _QueryTelemetry] = {}

    @classmethod
    def detached(cls) -> _QueryTelemetry:
        """A readings sink that is NOT registered against any query id."""
        return _QueryTelemetry()

    def __new__(cls, query_id=""):
        if not query_id:
            raise InvalidInternalStateError(
                compose(
                    "QueryTelemetry requires a query_id — it is a registry keyed by"
                    " that id, and an empty id aliases every caller onto one shared,"
                    " accumulating instance.",
                    md_cause("no query_id was supplied"),
                    f"Pass the query's id, or ask for an unregistered sink with"
                    f" {md_code('QueryTelemetry.detached()')}",
                )
            )
        if cls._instances.get(query_id) is None:
            cls._instances[query_id] = _QueryTelemetry()
            if len(cls._instances.keys()) > 16:
                # find the first key that is not "system"
                key_to_remove = next((key for key in cls._instances if key != "system"), None)
                if key_to_remove:
                    cls._instances.pop(key_to_remove)
        return cls._instances[query_id]
