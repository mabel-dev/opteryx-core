#!/usr/bin/env python3
"""
Compare function costs between measurements or against the current catalog.

Usage:
    python compare_function_costs.py --baseline baseline.json --current current.json
    python compare_function_costs.py --catalog --current current.json

Useful for:
- Detecting performance regressions
- Tracking optimization improvements
- Validating cost estimates
"""

import argparse
import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional

import numpy as np


class CostComparator:
    """Compare cost measurements."""

    def __init__(self, baseline_path: Optional[Path] = None, current_path: Optional[Path] = None):
        """Initialize comparator with baseline and current cost data.

        Args:
            baseline_path: Path to baseline JSON file (or use catalog)
            current_path: Path to current JSON file
        """
        self.baseline: Dict[str, Any] = {}
        self.current: Dict[str, Any] = {}

        if baseline_path and baseline_path.exists():
            with open(baseline_path) as f:
                self.baseline = json.load(f)

        if current_path and current_path.exists():
            with open(current_path) as f:
                self.current = json.load(f)

    def load_from_catalog(self) -> None:
        """Load baseline costs from the current function catalog."""
        try:
            from opteryx.expression.functions.catalog import get_catalog

            catalog = get_catalog()
            baseline = {}

            for func_def in catalog.list_functions():
                for overload in func_def.overloads:
                    key = f"{func_def.name}:{overload.kernel.id}"
                    baseline[key] = {
                        "cost_us_per_million": overload.kernel.cost_us_per_million,
                        "engine": str(overload.kernel.engine),
                    }

            # Convert to JSON-like structure
            self.baseline = {
                "functions": {
                    name: [
                        {
                            "kernel_id": kid,
                            "cost_us_per_million": data["cost_us_per_million"],
                            "engine": data["engine"],
                        }
                        for (n, kid), data in baseline.items()
                        if n == name
                    ]
                    for name in set(k.split(":")[0] for k in baseline)
                }
            }
        except Exception as e:
            print(f"Warning: Could not load catalog: {e}")

    def extract_costs(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Extract cost values from cost data.

        Args:
            data: Cost data dictionary

        Returns:
            Dictionary mapping "function:kernel" to cost
        """
        costs = {}

        for func_name, kernels in data.get("functions", {}).items():
            for kernel in kernels:
                if kernel.get("success", True):  # Include even if not explicitly marked success
                    key = f"{func_name}:{kernel['kernel_id']}"
                    costs[key] = kernel.get("cost_us_per_million", 0.0)

        return costs

    def compare_costs(
        self,
        threshold_percent: float = 10.0,
    ) -> Dict[str, Any]:
        """Compare baseline and current costs.

        Args:
            threshold_percent: Highlight changes above this percentage

        Returns:
            Dictionary with comparison results
        """
        baseline_costs = self.extract_costs(self.baseline)
        current_costs = self.extract_costs(self.current)

        results = {
            "baseline_count": len(baseline_costs),
            "current_count": len(current_costs),
            "identical": [],
            "improved": [],  # Lower cost (faster)
            "regressed": [],  # Higher cost (slower)
            "new": [],  # Only in current
            "removed": [],  # Only in baseline
        }

        # Compare common kernels
        for key in baseline_costs:
            if key not in current_costs:
                results["removed"].append({
                    "kernel": key,
                    "baseline_cost": baseline_costs[key],
                })
                continue

            baseline = baseline_costs[key]
            current = current_costs[key]
            diff = current - baseline
            percent_change = (diff / baseline * 100) if baseline != 0 else 0

            comparison = {
                "kernel": key,
                "baseline_cost": baseline,
                "current_cost": current,
                "absolute_change": diff,
                "percent_change": percent_change,
            }

            if abs(percent_change) < 0.01:
                results["identical"].append(comparison)
            elif diff < 0:
                results["improved"].append(comparison)
            elif percent_change > threshold_percent:
                results["regressed"].append(comparison)
            else:
                results["identical"].append(comparison)

        # Find new kernels
        for key in current_costs:
            if key not in baseline_costs:
                results["new"].append({
                    "kernel": key,
                    "current_cost": current_costs[key],
                })

        return results

    def generate_report(self, results: Dict[str, Any], threshold_percent: float = 10.0) -> str:
        """Generate a comparison report.

        Args:
            results: Results from compare_costs()
            threshold_percent: Threshold for highlighting changes

        Returns:
            Formatted report string
        """
        lines = []
        lines.append("=" * 100)
        lines.append("FUNCTION COST COMPARISON REPORT")
        lines.append("=" * 100 + "\n")

        lines.append(f"Baseline kernels: {results['baseline_count']}")
        lines.append(f"Current kernels: {results['current_count']}")
        lines.append(f"Threshold for highlighting: {threshold_percent}%\n")

        # Improvements
        if results["improved"]:
            lines.append("✓ IMPROVEMENTS (Lower cost = Faster)")
            lines.append("-" * 100)
            sorted_improved = sorted(
                results["improved"],
                key=lambda x: x["percent_change"],
            )
            for item in sorted_improved[:20]:  # Top 20
                lines.append(
                    f"{item['kernel']:<50} "
                    f"{item['baseline_cost']:>12.2f} → {item['current_cost']:>12.2f} "
                    f"({item['percent_change']:>7.1f}%)"
                )
            if len(results["improved"]) > 20:
                lines.append(f"... and {len(results['improved']) - 20} more improvements")
            lines.append()

        # Regressions
        if results["regressed"]:
            lines.append("✗ REGRESSIONS (Higher cost = Slower)")
            lines.append("-" * 100)
            sorted_regressed = sorted(
                results["regressed"],
                key=lambda x: x["percent_change"],
                reverse=True,
            )
            for item in sorted_regressed[:20]:  # Top 20
                lines.append(
                    f"{item['kernel']:<50} "
                    f"{item['baseline_cost']:>12.2f} → {item['current_cost']:>12.2f} "
                    f"({item['percent_change']:>7.1f}%)"
                )
            if len(results["regressed"]) > 20:
                lines.append(f"... and {len(results['regressed']) - 20} more regressions")
            lines.append()

        # New kernels
        if results["new"]:
            lines.append("⊕ NEW KERNELS")
            lines.append("-" * 100)
            for item in results["new"][:20]:
                lines.append(f"{item['kernel']:<50} {item['current_cost']:>12.2f} μs/million")
            if len(results["new"]) > 20:
                lines.append(f"... and {len(results['new']) - 20} more new kernels")
            lines.append()

        # Removed kernels
        if results["removed"]:
            lines.append("⊖ REMOVED KERNELS")
            lines.append("-" * 100)
            for item in results["removed"][:20]:
                lines.append(f"{item['kernel']:<50} (was {item['baseline_cost']:>10.2f} μs/million)")
            if len(results["removed"]) > 20:
                lines.append(f"... and {len(results['removed']) - 20} more removed kernels")
            lines.append()

        # Statistics
        lines.append("STATISTICS")
        lines.append("-" * 100)
        total_changed = len(results["improved"]) + len(results["regressed"])
        lines.append(f"Unchanged: {len(results['identical'])}")
        lines.append(f"Improved: {len(results['improved'])}")
        lines.append(f"Regressed: {len(results['regressed'])}")
        lines.append(f"New: {len(results['new'])}")
        lines.append(f"Removed: {len(results['removed'])}")

        if results["improved"] or results["regressed"]:
            all_changes = results["improved"] + results["regressed"]
            changes = [item["percent_change"] for item in all_changes]
            lines.append(f"\nAverage change: {np.mean(changes):+.1f}%")
            lines.append(f"Median change: {np.median(changes):+.1f}%")

        lines.append("\n" + "=" * 100)

        return "\n".join(lines)

    def get_detailed_comparison(self, kernel_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed comparison for a specific kernel.

        Args:
            kernel_name: Kernel name in "function:kernel_id" format

        Returns:
            Comparison details or None if not found
        """
        baseline_costs = self.extract_costs(self.baseline)
        current_costs = self.extract_costs(self.current)

        if kernel_name not in baseline_costs or kernel_name not in current_costs:
            return None

        baseline = baseline_costs[kernel_name]
        current = current_costs[kernel_name]
        diff = current - baseline
        percent = (diff / baseline * 100) if baseline != 0 else 0

        return {
            "kernel": kernel_name,
            "baseline": baseline,
            "current": current,
            "absolute_change": diff,
            "percent_change": percent,
            "impact": "improvement" if diff < 0 else "regression" if diff > 0 else "unchanged",
        }


def main():
    parser = argparse.ArgumentParser(
        description="Compare function costs between measurements"
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Baseline cost JSON file",
    )
    parser.add_argument(
        "--current",
        type=Path,
        help="Current cost JSON file",
        required=True,
    )
    parser.add_argument(
        "--catalog",
        action="store_true",
        help="Use current catalog as baseline",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Percentage threshold for highlighting changes (default: 10%%)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Save report to file",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of formatted text",
    )

    args = parser.parse_args()

    if not args.current.exists():
        print(f"Error: Current file not found: {args.current}")
        return 1

    # Initialize comparator
    comparator = CostComparator(baseline_path=args.baseline, current_path=args.current)

    # Load from catalog if requested
    if args.catalog:
        print("Loading costs from function catalog...")
        comparator.load_from_catalog()
    elif not args.baseline or not args.baseline.exists():
        print("Error: Provide either --baseline file or --catalog flag")
        return 1

    # Compare
    print("Comparing costs...")
    results = comparator.compare_costs(threshold_percent=args.threshold)

    # Generate output
    if args.json:
        output = json.dumps(results, indent=2)
    else:
        output = comparator.generate_report(results, threshold_percent=args.threshold)

    # Write or print
    if args.output:
        args.output.write_text(output)
        print(f"Report written to: {args.output}")
    else:
        print(output)

    return 0


if __name__ == "__main__":
    main()
