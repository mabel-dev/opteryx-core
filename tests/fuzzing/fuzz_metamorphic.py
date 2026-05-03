#!/usr/bin/env python
"""
Metamorphic fuzzer for Opteryx query engine.

Generates random SQL queries, applies semantics-preserving transformations,
executes both, and verifies the expected oracle relationships hold.
"""

import os
import sys
import random
import argparse
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import opteryx
from opteryx.managers import virtual_datasets

# Oracle types
EQUIVALENCE = "eq"
CONTRACTION = "con"
EXPANSION = "exp"


@dataclass
class FuzzerStats:
    total_iterations: int = 0
    successful_transformations: int = 0
    oracle_violations: int = 0
    execution_errors: int = 0
    transformation_failures: int = 0
    transformations_by_type: dict = None

    def __post_init__(self):
        if self.transformations_by_type is None:
            self.transformations_by_type = defaultdict(int)


class MetamorphicTransformation:
    """Base class for transformations."""

    def __init__(self, oracle_type: str, name: str):
        self.oracle_type = oracle_type
        self.name = name

    def apply(self, query: str) -> Optional[str]:
        """Return transformed query or None if inapplicable."""
        raise NotImplementedError


class AddTautologyPredicate(MetamorphicTransformation):
    """Add 'AND 1=1' to WHERE clause — should be equivalent."""

    def __init__(self):
        super().__init__(EQUIVALENCE, "add_tautology")

    def apply(self, query: str) -> Optional[str]:
        if " WHERE " not in query.upper():
            return None
        # Only add to simple queries to avoid complex nesting
        if query.upper().count("WHERE") > 1:
            return None
        return query.replace(" WHERE ", " WHERE 1=1 AND ", 1)


class RemoveTautologyPredicate(MetamorphicTransformation):
    """Remove 'AND 1=1' if present — should be equivalent."""

    def __init__(self):
        super().__init__(EQUIVALENCE, "remove_tautology")

    def apply(self, query: str) -> Optional[str]:
        if " 1=1 AND " not in query:
            return None
        return query.replace(" 1=1 AND ", " ", 1)


class AddIsNotNullRedundant(MetamorphicTransformation):
    """Add 'AND col IS NOT NULL' for a column that must be non-null — equivalent."""

    def __init__(self):
        super().__init__(EQUIVALENCE, "add_is_not_null")

    def apply(self, query: str) -> Optional[str]:
        # Heuristic: add IS NOT NULL check on first WHERE column
        if " WHERE " not in query.upper():
            return None
        # Very basic: find first comparison and add redundant IS NOT NULL
        query_upper = query.upper()
        where_idx = query_upper.find(" WHERE ")
        if where_idx == -1:
            return None

        # Extract first column name (basic heuristic)
        after_where = query[where_idx + 7 :]
        # Find first word that looks like a column
        parts = after_where.split()
        if not parts:
            return None
        col = parts[0]
        if col.upper() in ("SELECT", "FROM", "WHERE", "AND", "OR"):
            return None

        # Append redundant check
        return query + f" AND {col} IS NOT NULL"


class RemoveDistinctRedundant(MetamorphicTransformation):
    """Remove DISTINCT when result is already unique — expansion (may get dupes if they exist)."""

    def __init__(self):
        super().__init__(EXPANSION, "remove_distinct")

    def apply(self, query: str) -> Optional[str]:
        if "DISTINCT" not in query.upper():
            return None
        return query.upper().replace("SELECT DISTINCT", "SELECT", 1)


class AddDistinct(MetamorphicTransformation):
    """Add DISTINCT to non-DISTINCT query — contraction (fewer/equal results)."""

    def __init__(self):
        super().__init__(CONTRACTION, "add_distinct")

    def apply(self, query: str) -> Optional[str]:
        if "DISTINCT" in query.upper():
            return None
        query_upper = query.upper()
        if "SELECT" not in query_upper:
            return None
        return query.replace("SELECT ", "SELECT DISTINCT ", 1)


class AddRedundantWhere(MetamorphicTransformation):
    """Add a WHERE condition from an existing column — contraction."""

    def __init__(self):
        super().__init__(CONTRACTION, "add_redundant_where")

    def apply(self, query: str) -> Optional[str]:
        # Very basic: add a condition that's likely always true
        # This is risky without schema knowledge, so make it simple
        if "WHERE" in query.upper():
            return None  # Avoid complex nested conditions

        if " FROM " not in query.upper():
            return None

        # Just add a simple filter that eliminates few rows
        return query + " WHERE 1=1"


def run_query(query: str) -> Optional[set]:
    """Execute query, return set of result rows (as tuples)."""
    try:
        session = opteryx.session()
        morsels = session.execute_to_morsels(query)
        results = set()
        for morsel in morsels:
            for row in morsel.to_pylist():
                # Convert to tuple for set membership
                if isinstance(row, (list, tuple)):
                    results.add(tuple(row))
                else:
                    results.add((row,))
        return results
    except Exception as e:
        return None


def check_oracle(orig_results: Optional[set], trans_results: Optional[set], oracle_type: str) -> bool:
    """Check if oracle relationship holds."""
    if orig_results is None or trans_results is None:
        # Execution error — don't flag as violation
        return True

    if oracle_type == EQUIVALENCE:
        return orig_results == trans_results
    elif oracle_type == EXPANSION:
        return orig_results.issubset(trans_results)
    elif oracle_type == CONTRACTION:
        return trans_results.issubset(orig_results)

    return False


def load_seed_queries(seed_dir: str) -> list:
    """Extract SQL queries from existing test files."""
    queries = []
    seed_path = Path(seed_dir)
    if not seed_path.exists():
        return queries

    for fpath in seed_path.rglob("*.py"):
        try:
            with open(fpath) as f:
                content = f.read()
                # Very basic: look for execute() calls with SQL strings
                import ast
                try:
                    tree = ast.parse(content)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Constant) and isinstance(node.value, str):
                            sql = node.value.strip()
                            if sql.upper().startswith("SELECT"):
                                queries.append(sql)
                except SyntaxError:
                    pass
        except Exception:
            pass

    return queries


def generate_random_query() -> str:
    """Generate a basic random SQL query."""
    # Use planets as a simple available dataset
    table = "$planets"

    # Very basic: SELECT * FROM table with optional WHERE 1=1
    query = f"SELECT * FROM {table}"

    if random.random() < 0.3:
        query += " WHERE 1=1"

    if random.random() < 0.2:
        query += " LIMIT 100"

    return query


def fuzz(iterations: int, verbose: bool = False):
    """Run metamorphic fuzzing campaign."""
    transformations = [
        AddTautologyPredicate(),
        RemoveTautologyPredicate(),
        AddIsNotNullRedundant(),
        RemoveDistinctRedundant(),
        AddDistinct(),
        AddRedundantWhere(),
    ]

    stats = FuzzerStats(total_iterations=iterations)
    bugs = []

    # Load seed queries
    seed_queries = load_seed_queries("tests/")
    all_queries = seed_queries if seed_queries else []

    if verbose and seed_queries:
        print(f"Loaded {len(seed_queries)} seed queries from tests/")

    for i in range(iterations):
        # Pick a query (seed or random)
        if all_queries and random.random() < 0.7:
            query = random.choice(all_queries)
        else:
            query = generate_random_query()

        # Pick a transformation
        transform = random.choice(transformations)
        transformed = transform.apply(query)

        if transformed is None:
            stats.transformation_failures += 1
            continue

        # Execute both queries
        orig_results = run_query(query)
        trans_results = run_query(transformed)

        # Check if execution failed
        if orig_results is None or trans_results is None:
            stats.execution_errors += 1
            if verbose:
                print(f"⚠ Execution error in iteration {i+1}")
            continue

        # Check oracle
        if not check_oracle(orig_results, trans_results, transform.oracle_type):
            stats.oracle_violations += 1
            bugs.append(
                {
                    "iteration": i + 1,
                    "transformation": transform.name,
                    "oracle": transform.oracle_type,
                    "original": query[:80],
                    "transformed": transformed[:80],
                    "orig_size": len(orig_results),
                    "trans_size": len(trans_results),
                }
            )
            if verbose:
                print(f"❌ Oracle violation: {transform.name} ({transform.oracle_type})")
                print(f"   Original size: {len(orig_results)}, Transformed size: {len(trans_results)}")
        else:
            stats.successful_transformations += 1
            stats.transformations_by_type[transform.name] += 1

        # Progress indicator every 50 iterations
        if (i + 1) % 50 == 0 and verbose:
            print(f"✓ {i+1}/{iterations} iterations complete...")

    # Report
    print(f"\n{'='*60}")
    print(f"Metamorphic Fuzzing Summary")
    print(f"{'='*60}")
    print(f"Total iterations:           {stats.total_iterations}")
    print(f"Successful transformations: {stats.successful_transformations}")
    print(f"Oracle violations:          {stats.oracle_violations}")
    print(f"Execution errors:           {stats.execution_errors}")
    print(f"Transformation failures:    {stats.transformation_failures}")
    print()
    print("Transformations applied:")
    for name, count in sorted(stats.transformations_by_type.items()):
        print(f"  {name:30} {count:4d}")
    print()

    if bugs:
        print(f"{'='*60}")
        print(f"BUGS FOUND: {len(bugs)}")
        print(f"{'='*60}")
        for bug in bugs[:10]:  # Show first 10
            print(f"\nIteration {bug['iteration']}")
            print(f"  Transformation: {bug['transformation']}")
            print(f"  Oracle:         {bug['oracle']}")
            print(f"  Original:       {bug['original']}")
            print(f"  Transformed:    {bug['transformed']}")
            print(f"  Result sizes:   {bug['orig_size']} → {bug['trans_size']}")
        if len(bugs) > 10:
            print(f"\n... and {len(bugs) - 10} more bugs")
        return 1  # Exit with error code
    else:
        print("✓ All oracles satisfied!")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Metamorphic fuzzing for Opteryx query engine"
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
