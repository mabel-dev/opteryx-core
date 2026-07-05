# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from collections import defaultdict


class _QueryTelemetry:
    def __init__(self):
        # predefine "messages" and "operations" so all new telemetry default to 0
        self._reading: dict = defaultdict(int)
        self._reading["messages"] = []
        self._reading["operations"] = {}
        self._reading["optimizer_trace"] = []

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

        # Remove connector-level stats that should only appear in operation/sensor stats
        connector_only_keys = [
            "rows_read",
            "rows_seen",
            "blobs_read",
            "blobs_seen",
            "bytes_raw",
            "bytes_processed",
            "columns_read",
            "bytes_read",
            # per-operator native readings — surfaced via the ``operations`` breakdown
            # (mermaid.get_node_stats), never as a top-level telemetry key.
            "native_op_stats",
        ]
        for key in connector_only_keys:
            readings_dict.pop(key, None)

        for k, v in readings_dict.items():
            # times are recorded in ns but reported in seconds
            if k.startswith("time_"):
                readings_dict[k] = self._ns_to_s(v)
        readings_dict["time_total"] = self._ns_to_s(
            readings_dict.pop("end_time", 0) - readings_dict.pop("start_time", 0)
        )
        # sort the keys in the dictionary
        readings_dict = {key: readings_dict[key] for key in sorted(readings_dict)}
        # put messages and plan at the end
        readings_dict["version"] = opteryx.__version__
        readings_dict["messages"] = readings_dict.pop("messages", [])
        readings_dict["edges"] = readings_dict.pop("edges", [])
        readings_dict["plan"] = readings_dict.pop("plan", None)
        return readings_dict


class QueryTelemetry(_QueryTelemetry):
    slots = "_instances"

    _instances: dict[str, _QueryTelemetry] = {}

    def __new__(cls, query_id=""):
        if cls._instances.get(query_id) is None:
            cls._instances[query_id] = _QueryTelemetry()
            if len(cls._instances.keys()) > 16:
                # find the first key that is not "system"
                key_to_remove = next((key for key in cls._instances if key != "system"), None)
                if key_to_remove:
                    cls._instances.pop(key_to_remove)
        return cls._instances[query_id]
