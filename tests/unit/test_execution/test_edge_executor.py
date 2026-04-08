# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Edge-based executor that can run a physical plan DAG using the edge model.

This demonstrates how to integrate the edge model with Opteryx's PhysicalPlan
to replace the EOS sentinel pattern with explicit edge-driven scheduling.
"""

import threading
import time
import sys
import importlib.util
from typing import Dict, List, Tuple

# Load Edge module directly
spec = importlib.util.spec_from_file_location('edge', './opteryx/execution/edge.py')
edge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(edge_module)
Edge = edge_module.Edge


class EdgeBasedExecutor:
    """
    Executor that runs a physical plan using edges instead of EOS sentinels.

    The executor:
    1. Builds edges for each connection in the plan DAG
    2. Runs scan operators in producer threads
    3. Runs transform operators in consumer threads
    4. Detects edge completion and calls finish() when needed
    """

    def __init__(self, plan, target_queue_depth=10):
        """
        Initialize executor with a physical plan.

        Args:
            plan: A PhysicalPlan instance from Opteryx
            target_queue_depth: Target queue depth for backpressure
        """
        self.plan = plan
        self.target_queue_depth = target_queue_depth

        # Edge storage: (from_nid, to_nid) -> Edge
        self._edges: Dict[Tuple[str, str], Edge] = {}

        # Thread storage
        self._threads: List[threading.Thread] = []

        # Lock for thread safety
        self._lock = threading.RLock()

    def _build_edges(self):
        """Walk the plan and create edges for all connections."""
        print("Building edges for plan DAG...")

        for nid, node in self.plan.depth_first_search_flat():
            outgoing = self.plan.outgoing_edges(nid)
            for edge_label, sink_nid, edge_meta in outgoing:
                pair = (nid, sink_nid)
                edge_name = f"{nid}→{sink_nid}"
                edge = Edge(edge_name, target_queue_depth=self.target_queue_depth)
                self._edges[pair] = edge
                print(f"  Created edge: {edge_name}")

        print(f"Built {len(self._edges)} edges\n")

    def _get_input_edges(self, nid: str) -> List[Edge]:
        """Get all input edges for a node."""
        edges = []
        for (src_nid, sink_nid), edge in self._edges.items():
            if sink_nid == nid:
                edges.append(edge)
        return edges

    def _get_output_edges(self, nid: str) -> List[Edge]:
        """Get all output edges for a node."""
        edges = []
        for (src_nid, sink_nid), edge in self._edges.items():
            if src_nid == nid:
                edges.append(edge)
        return edges

    def _run_scan_operator(self, nid: str, node):
        """Run a scan (source) operator, enqueuing to its output edges."""
        print(f"[{nid}] Starting scan: {node.name}")

        output_edges = self._get_output_edges(nid)
        if not output_edges:
            print(f"[{nid}] No output edges")
            return

        try:
            morsel_count = 0
            # Call the operator as a generator
            for morsel in node(None):
                if morsel is not None:
                    for edge in output_edges:
                        edge.enqueue(morsel)
                    morsel_count += 1

            # Signal that this source is exhausted
            for edge in output_edges:
                edge.close()

            print(f"[{nid}] Scan complete: {morsel_count} morsels")

        except Exception as e:
            print(f"[{nid}] Scan failed: {e}")
            for edge in output_edges:
                edge.close()

    def _run_transform_operator(self, nid: str, node):
        """Run a transform (non-source) operator, consuming and producing."""
        print(f"[{nid}] Starting transform: {node.name}")

        input_edges = self._get_input_edges(nid)
        output_edges = self._get_output_edges(nid)

        if not input_edges:
            print(f"[{nid}] No input edges")
            return

        try:
            morsel_count = 0
            while True:
                # Try to get a morsel from any input edge
                morsel = None
                for edge in input_edges:
                    morsel = edge.dequeue()
                    if morsel is not None:
                        break

                if morsel is None:
                    # No morsel available; check if all inputs are closed
                    if all(not edge.is_open() for edge in input_edges):
                        break
                    time.sleep(0.01)
                    continue

                # Process the morsel
                results = node(morsel)
                if results is not None:
                    for result in (results if isinstance(results, list) else [results]):
                        if result is not None:
                            for edge in output_edges:
                                edge.enqueue(result)
                        morsel_count += 1

                # Mark input as complete
                for edge in input_edges:
                    edge.mark_complete()

            # Close output edges when done
            for edge in output_edges:
                edge.close()

            print(f"[{nid}] Transform complete: {morsel_count} morsels")

        except Exception as e:
            print(f"[{nid}] Transform failed: {e}")
            for edge in output_edges:
                edge.close()

    def _run_exit_operator(self, nid: str, node):
        """Run an exit (sink) operator, consuming all morsels."""
        print(f"[{nid}] Starting exit: {node.name}")

        input_edges = self._get_input_edges(nid)
        if not input_edges:
            print(f"[{nid}] No input edges")
            return

        results = []
        try:
            while True:
                morsel = None
                for edge in input_edges:
                    morsel = edge.dequeue()
                    if morsel is not None:
                        break

                if morsel is None:
                    if all(not edge.is_open() for edge in input_edges):
                        break
                    time.sleep(0.01)
                    continue

                results.append(morsel)
                for edge in input_edges:
                    edge.mark_complete()

            print(f"[{nid}] Exit complete: {len(results)} morsels")

        except Exception as e:
            print(f"[{nid}] Exit failed: {e}")

    def execute(self) -> bool:
        """
        Execute the physical plan using edges.

        Returns True if successful, False otherwise.
        """
        print("\n" + "=" * 60)
        print("EdgeBasedExecutor: Starting execution")
        print("=" * 60 + "\n")

        try:
            # Build edges for the plan
            self._build_edges()

            # Identify exit points
            exit_points = list(set(self.plan.get_exit_points()))
            print(f"Exit points: {exit_points}\n")

            # Start threads for each operator
            print("Starting operator threads...")
            for nid, node in self.plan.depth_first_search_flat():
                if node.is_scan:
                    # Scan operators run as producers
                    thread = threading.Thread(
                        target=self._run_scan_operator,
                        args=(nid, node),
                        name=f"scan-{nid}",
                    )
                elif nid in exit_points:
                    # Exit operators run as sinks
                    thread = threading.Thread(
                        target=self._run_exit_operator,
                        args=(nid, node),
                        name=f"exit-{nid}",
                    )
                else:
                    # Transform operators run as filters
                    thread = threading.Thread(
                        target=self._run_transform_operator,
                        args=(nid, node),
                        name=f"xform-{nid}",
                    )

                self._threads.append(thread)
                thread.start()

            print(f"Started {len(self._threads)} operator threads\n")

            # Wait for all threads to complete
            print("Waiting for execution to complete...")
            for thread in self._threads:
                thread.join(timeout=30.0)
                if thread.is_alive():
                    print(f"Warning: {thread.name} did not complete")

            # Verify all edges are complete
            print("\nVerifying edge completion...")
            for (src, sink), edge in self._edges.items():
                state = edge.get_state()
                print(f"  {edge.name}: {state}")
                if not edge.is_complete():
                    print(f"    WARNING: Edge not complete!")
                    return False

            print("\n" + "=" * 60)
            print("✓ Execution completed successfully")
            print("=" * 60)

            return True

        except Exception as e:
            print(f"\n✗ Execution failed: {e}")
            import traceback
            traceback.print_exc()
            return False


# Test with mock operators if Opteryx is not available
class MockOperator:
    """Mock operator for testing without real Opteryx."""

    def __init__(self, name, is_scan=False, morsel_count=0):
        self.name = name
        self.is_scan = is_scan
        self.morsel_count = morsel_count
        self._counter = 0

    def __call__(self, morsel):
        """Operator callable interface."""
        if self.is_scan:
            # Producer: generate morsels
            for i in range(self.morsel_count):
                yield f"{self.name}_morsel_{i}"
        else:
            # Consumer: pass through
            if morsel is not None:
                yield morsel


class MockPlan:
    """Mock physical plan for testing."""

    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_node(self, nid, node):
        self.nodes[nid] = node

    def add_edge(self, src, sink):
        self.edges.append((src, sink))

    def get_exit_points(self):
        return ["exit"]

    def depth_first_search_flat(self):
        # Return in dependency order
        return [
            ("scan", MockOperator("Scan", is_scan=True, morsel_count=3)),
            ("filter", MockOperator("Filter", is_scan=False)),
            ("exit", MockOperator("Exit", is_scan=False)),
        ]

    def outgoing_edges(self, nid):
        result = []
        for src, sink in self.edges:
            if src == nid:
                result.append((None, sink, None))
        return result

    def ingoing_edges(self, nid):
        result = []
        for src, sink in self.edges:
            if sink == nid:
                result.append((None, src, None))
        return result


def test_mock_plan():
    """Test with a mock plan."""
    print("\nTest: Executing mock physical plan")
    print("=" * 60)

    plan = MockPlan()
    plan.add_edge("scan", "filter")
    plan.add_edge("filter", "exit")

    executor = EdgeBasedExecutor(plan)
    success = executor.execute()

    return success


# Test runner
class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def test(self, name, func):
        try:
            result = func()
            if result:
                print(f"✓ {name}")
                self.passed += 1
            else:
                print(f"✗ {name}")
                self.failed += 1
        except Exception as e:
            print(f"✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            self.failed += 1

    def summary(self):
        print(f"\n{self.passed} passed, {self.failed} failed")
        return self.failed == 0


if __name__ == "__main__":
    runner = TestRunner()
    runner.test("mock_plan_execution", test_mock_plan)

    if not runner.summary():
        sys.exit(1)
