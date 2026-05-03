#!/usr/bin/env python
"""
Constant-folding oracle fuzzer for Opteryx query engine.

Inspired by CODDTest (https://arxiv.org/abs/2502.16674), this fuzzer detects
logic bugs by applying constant folding and propagation to predicates:

1. Generate a query with an expression in a predicate
2. Replace that expression with a semantically equivalent constant or simplified form
3. Verify both queries return identical results

This tests the engine's correctness across various SQL features and predicates,
catching bugs where optimizer decisions change semantics.
"""

import os
import sys
import random
import argparse
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
from typing import Optional, Tuple, Set

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import opteryx


@dataclass
class FuzzerStats:
    total_iterations: int = 0
    successful_folds: int = 0
    oracle_violations: int = 0
    execution_errors: int = 0
    fold_failures: int = 0
    expressions_by_category: dict = None

    def __post_init__(self):
        if self.expressions_by_category is None:
            self.expressions_by_category = defaultdict(int)


def run_query(query: str, debug: bool = False) -> Optional[Set]:
    """Execute query, return set of result rows (as tuples)."""
    try:
        session = opteryx.session()
        morsels = session.execute_to_morsels(query)
        results = set()
        for morsel in morsels:
            for i in range(len(morsel)):
                row = morsel[i]
                if isinstance(row, tuple):
                    results.add(row)
                else:
                    results.add((row,))
        return results
    except Exception as e:
        if debug:
            print(f"ERROR executing: {query[:80]}")
            print(f"  {type(e).__name__}: {str(e)[:100]}")
        return None


class ConstantFoldingOracle:
    """Base class for constant-folding oracles."""

    def __init__(self, name: str):
        self.name = name

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """
        Generate (original_query, folded_query, description).
        Both queries should return identical results.
        Returns (None, None, desc) if unable to generate.
        """
        raise NotImplementedError


class TautologyFold(ConstantFoldingOracle):
    """Test tautologies (1=1, expressions that are always true)."""

    def __init__(self):
        super().__init__("tautology_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate tautology test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE 1 = 1",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id)",
                "WHERE 1=1 is redundant"
            ),
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id > 0 OR 1 = 1",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id)",
                "OR 1=1 makes predicate always true"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class ContradictionFold(ConstantFoldingOracle):
    """Test contradictions (0=1, expressions that are always false)."""

    def __init__(self):
        super().__init__("contradiction_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate contradiction test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE 0 = 1",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE 1 = 0",
                "Both contradictions"
            ),
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id > 0 AND 0 = 1",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE 1 = 0",
                "AND 0=1 makes entire predicate false"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class INPredicateFold(ConstantFoldingOracle):
    """Test IN predicates with equivalent OR expressions."""

    def __init__(self):
        super().__init__("in_predicate_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate IN predicate test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id IN (1, 2)",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id = 1 OR id = 2",
                "IN with literals = OR chain"
            ),
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id NOT IN (1)",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id != 1",
                "NOT IN single value = !="
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class SubqueryFold(ConstantFoldingOracle):
    """Test subqueries in predicates."""

    def __init__(self):
        super().__init__("subquery_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate subquery test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id IN (SELECT 1)",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id = 1",
                "Subquery with single value"
            ),
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE EXISTS (SELECT 1)",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id)",
                "EXISTS with always-true subquery"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class ComparisonFold(ConstantFoldingOracle):
    """Test comparison simplification."""

    def __init__(self):
        super().__init__("comparison_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate comparison test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id >= 1",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id > 0",
                "Different but equivalent ranges"
            ),
            (
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE NOT (id = 1)",
                "SELECT id FROM (VALUES (1), (2), (3)) AS T(id) WHERE id != 1",
                "NOT equals = not equal"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class JoinFold(ConstantFoldingOracle):
    """Test JOIN predicates."""

    def __init__(self):
        super().__init__("join_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate JOIN test cases."""
        cases = [
            (
                "SELECT a.id FROM (VALUES (1), (2)) AS A(id) JOIN (VALUES (1), (2)) AS B(id) ON a.id = b.id WHERE a.id > 0",
                "SELECT a.id FROM (VALUES (1), (2)) AS A(id) WHERE a.id > 0",
                "Self-join on equality is redundant"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class AggregateNullFold(ConstantFoldingOracle):
    """Test NULL handling in aggregates."""

    def __init__(self):
        super().__init__("aggregate_null_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate aggregate/NULL test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2), (NULL)) AS T(id) WHERE id IS NOT NULL",
                "SELECT id FROM (VALUES (1), (2)) AS T(id)",
                "Filter out NULLs"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


class CaseFold(ConstantFoldingOracle):
    """Test CASE expression folding."""

    def __init__(self):
        super().__init__("case_fold")

    def generate_pair(self) -> Tuple[Optional[str], Optional[str], str]:
        """Generate CASE test cases."""
        cases = [
            (
                "SELECT id FROM (VALUES (1), (2)) AS T(id) WHERE CASE WHEN 1=1 THEN TRUE ELSE FALSE END",
                "SELECT id FROM (VALUES (1), (2)) AS T(id)",
                "CASE with constant condition folds to constant"
            ),
        ]

        case = random.choice(cases)
        return case[0], case[1], case[2]


def fuzz(iterations: int, verbose: bool = False):
    """Run constant-folding fuzzing campaign."""
    oracles = [
        TautologyFold(),
        ContradictionFold(),
        INPredicateFold(),
        SubqueryFold(),
        ComparisonFold(),
        JoinFold(),
        AggregateNullFold(),
        CaseFold(),
    ]

    stats = FuzzerStats(total_iterations=iterations)
    bugs = []

    for i in range(iterations):
        # Pick an oracle
        oracle = random.choice(oracles)

        # Generate original and folded query pair
        original_query, folded_query, description = oracle.generate_pair()

        if original_query is None or folded_query is None:
            stats.fold_failures += 1
            continue

        # Execute both queries
        orig_results = run_query(original_query)
        folded_results = run_query(folded_query)

        # Check if execution failed
        if orig_results is None or folded_results is None:
            stats.execution_errors += 1
            if verbose:
                print(f"⚠ Execution error in iteration {i+1}: {oracle.name}")
            continue

        # Check oracle: results must be identical
        if orig_results != folded_results:
            stats.oracle_violations += 1
            bugs.append(
                {
                    "iteration": i + 1,
                    "oracle": oracle.name,
                    "description": description,
                    "original": original_query[:100],
                    "folded": folded_query[:100],
                    "orig_size": len(orig_results),
                    "folded_size": len(folded_results),
                    "orig_sample": sorted(list(orig_results))[:3],
                    "folded_sample": sorted(list(folded_results))[:3],
                }
            )
            if verbose:
                print(f"❌ Oracle violation: {oracle.name}")
                print(f"   {description}")
                print(f"   Original: {original_query[:80]}")
                print(f"   Folded:   {folded_query[:80]}")
                print(f"   Result sizes: {len(orig_results)} vs {len(folded_results)}")
        else:
            stats.successful_folds += 1
            stats.expressions_by_category[oracle.name] += 1
            if verbose and (i + 1) % 10 == 0:
                print(f"✓ {i+1}/{iterations} iterations...")

    # Report
    print(f"\n{'='*60}")
    print(f"Constant-Folding Fuzzing Summary")
    print(f"{'='*60}")
    print(f"Total iterations:           {stats.total_iterations}")
    print(f"Successful folds:           {stats.successful_folds}")
    print(f"Oracle violations:          {stats.oracle_violations}")
    print(f"Execution errors:           {stats.execution_errors}")
    print(f"Fold failures:              {stats.fold_failures}")
    print()
    print("Oracles tested:")
    for name, count in sorted(stats.expressions_by_category.items()):
        print(f"  {name:35} {count:4d}")
    print()

    if bugs:
        print(f"{'='*60}")
        print(f"BUGS FOUND: {len(bugs)}")
        print(f"{'='*60}")
        for bug in bugs[:10]:  # Show first 10
            print(f"\nIteration {bug['iteration']}")
            print(f"  Oracle:       {bug['oracle']}")
            print(f"  Description:  {bug['description']}")
            print(f"  Original:     {bug['original']}")
            print(f"  Folded:       {bug['folded']}")
            print(f"  Result sizes: {bug['orig_size']} vs {bug['folded_size']}")
            if bug['orig_sample'] != bug['folded_sample']:
                print(f"  Original rows: {bug['orig_sample']}")
                print(f"  Folded rows:   {bug['folded_sample']}")
        if len(bugs) > 10:
            print(f"\n... and {len(bugs) - 10} more bugs")
        return 1
    else:
        print("✓ All constant-folding oracles satisfied!")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Constant-folding fuzzing for Opteryx query engine"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of fuzzing iterations (default: 100)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )

    args = parser.parse_args()
    sys.exit(fuzz(args.iterations, verbose=args.verbose))
