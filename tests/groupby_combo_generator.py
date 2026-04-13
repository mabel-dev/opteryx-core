#!/usr/bin/env python
"""
GROUP BY Column Combination Test Generator

Generates and executes GROUP BY tests for all combinations of columns
based on a configuration file. This systematically tests which column
combinations trigger bugs.

Each test runs in an isolated subprocess to survive crashes.

Usage:
    python groupby_combo_generator.py --config tests/groupby_combo_tests_config.json
    python groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --suite satellites_comprehensive
    python groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --suite missions_comprehensive --verbose
    python groupby_combo_generator.py --config tests/groupby_combo_tests_config.json --output results.json --limit 50
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx


class TestStatus(Enum):
    """Status of a generated test."""

    PASSED = "passed"
    FAILED = "failed"
    CRASHED = "crashed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestResult:
    """Result of executing a single test."""

    pattern_id: str
    table: str
    columns_used: List[str]
    sql: str
    status: TestStatus
    error_message: Optional[str] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    error_type: Optional[str] = None
    traceback_str: Optional[str] = None


# Worker script that runs in a subprocess
WORKER_SCRIPT = """
import json
import sys
import time
import traceback

import opteryx

test_data = json.loads(sys.argv[1])
pattern_id = test_data['pattern_id']
table = test_data['table']
sql = test_data['sql']

result = {
    'pattern_id': pattern_id,
    'table': table,
    'sql': sql,
    'status': 'failed',
    'error_message': None,
    'error_type': None,
    'row_count': None,
    'execution_time_ms': None,
}

try:
    session = opteryx.session(memberships=["Apollo 11", "opteryx"])
    start = time.time()
    arrow_result = session.execute(sql)
    row_count = len(arrow_result)
    elapsed_ms = (time.time() - start) * 1000

    result['status'] = 'passed'
    result['row_count'] = row_count
    result['execution_time_ms'] = elapsed_ms
except Exception as e:
    result['status'] = 'failed'
    result['error_message'] = str(e)
    result['error_type'] = type(e).__name__
    result['traceback_str'] = traceback.format_exc()

print(json.dumps(result))
"""


class ComboTestGenerator:
    """Generates and executes GROUP BY tests for column combinations."""

    def __init__(self, config_path: str, verbose: bool = False):
        """Initialize generator with config file."""
        self.config = self._load_config(config_path)
        self.verbose = verbose
        self.results: List[TestResult] = []
        self.test_count = 0
        self.passed_count = 0
        self.failed_count = 0
        self.crashed_count = 0

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load JSON configuration file."""
        with open(config_path, "r") as f:
            return json.load(f)

    def generate_tests(self, suite_name: Optional[str] = None) -> List[Tuple[str, str, str]]:
        """
        Generate all test combinations based on config.

        Returns list of (pattern_id, table, sql) tuples.
        """
        tests = []
        suites = self.config["test_suites"]

        if suite_name:
            suites = [s for s in suites if s["name"] == suite_name]
            if not suites:
                raise ValueError(f"Suite '{suite_name}' not found in config")

        for suite in suites:
            table = suite["table"]
            columns = suite["columns"]
            patterns = suite["test_patterns"]

            # Generate column combinations for each pattern
            for pattern in patterns:
                sqls = self._generate_sqls_for_pattern(pattern, table, columns, suite["name"])
                for sql, columns_used, pattern_id in sqls:
                    tests.append((pattern_id, table, sql))

        return tests

    def _generate_sqls_for_pattern(
        self, pattern: Dict[str, Any], table: str, columns: List[Dict[str, Any]], suite_name: str
    ) -> List[Tuple[str, List[str], str]]:
        """Generate SQL statements for a given pattern with all column combinations."""

        pattern_id = pattern["pattern_id"]
        template = pattern["template"]
        required_columns = pattern["required_columns"]

        sqls = []

        # Generate combinations based on required columns
        if len(required_columns) == 1:
            # P1: Single column combinations
            col_a = required_columns[0]
            for column in columns:
                if self._matches_constraint(column, pattern, col_a):
                    sql = template.format(column_a=column["name"], table=table)
                    sqls.append((sql, [column["name"]], pattern_id))

        elif len(required_columns) == 2:
            # P2 or P3: Two column combinations
            col_a, col_b = required_columns[0], required_columns[1]

            # Check if it's pattern P2 (one agg, one group) or P3 (both group)
            is_p2 = (
                "column_b_must_be" in pattern and pattern.get("column_b_must_be") == "aggregatable"
            )

            for col_a_candidate in columns:
                for col_b_candidate in columns:
                    # Skip if same column required (for P3)
                    if (
                        pattern.get("columns_must_be_different", False)
                        and col_a_candidate["name"] == col_b_candidate["name"]
                    ):
                        continue

                    if self._matches_constraint(
                        col_a_candidate, pattern, col_a
                    ) and self._matches_constraint(col_b_candidate, pattern, col_b):
                        sql = template.format(
                            column_a=col_a_candidate["name"],
                            column_b=col_b_candidate["name"],
                            table=table,
                        )
                        sqls.append(
                            (sql, [col_a_candidate["name"], col_b_candidate["name"]], pattern_id)
                        )

        elif len(required_columns) == 3:
            # P4: Three column combinations (two group, one agg)
            col_a, col_b, col_c = required_columns

            for col_a_candidate in columns:
                for col_b_candidate in columns:
                    for col_c_candidate in columns:
                        # Skip if group columns are same (P4 requires different)
                        if col_a_candidate["name"] == col_b_candidate["name"]:
                            continue

                        if (
                            self._matches_constraint(col_a_candidate, pattern, col_a)
                            and self._matches_constraint(col_b_candidate, pattern, col_b)
                            and self._matches_constraint(col_c_candidate, pattern, col_c)
                        ):
                            sql = template.format(
                                column_a=col_a_candidate["name"],
                                column_b=col_b_candidate["name"],
                                column_c=col_c_candidate["name"],
                                table=table,
                            )
                            sqls.append(
                                (
                                    sql,
                                    [
                                        col_a_candidate["name"],
                                        col_b_candidate["name"],
                                        col_c_candidate["name"],
                                    ],
                                    pattern_id,
                                )
                            )

        return sqls

    def _matches_constraint(
        self, column: Dict[str, Any], pattern: Dict[str, Any], column_role: str
    ) -> bool:
        """Check if column matches constraints for its role."""
        constraint_key = f"{column_role}_must_be"

        if constraint_key in pattern:
            required_property = pattern[constraint_key]
            return column.get(required_property, False)

        return True

    def _run_test_subprocess(self, pattern_id: str, table: str, sql: str) -> TestResult:
        """Run a single test in an isolated subprocess."""
        test_data = {
            "pattern_id": pattern_id,
            "table": table,
            "sql": sql,
        }

        try:
            # Run the worker script in a subprocess
            result = subprocess.run(
                [sys.executable, "-c", WORKER_SCRIPT, json.dumps(test_data)],
                capture_output=True,
                timeout=30,
                text=True,
            )

            if result.returncode == 0:
                # Parse the JSON output
                result_data = json.loads(result.stdout)
                return TestResult(
                    pattern_id=result_data["pattern_id"],
                    table=result_data["table"],
                    columns_used=self._extract_columns_from_sql(result_data["sql"]),
                    sql=result_data["sql"],
                    status=TestStatus(result_data["status"]),
                    error_message=result_data.get("error_message"),
                    row_count=result_data.get("row_count"),
                    execution_time_ms=result_data.get("execution_time_ms"),
                    error_type=result_data.get("error_type"),
                    traceback_str=result_data.get("traceback_str"),
                )
            else:
                # Subprocess crashed or failed
                return TestResult(
                    pattern_id=pattern_id,
                    table=table,
                    columns_used=self._extract_columns_from_sql(sql),
                    sql=sql,
                    status=TestStatus.CRASHED,
                    error_message=f"Subprocess crash: {result.stderr}",
                    error_type="SubprocessCrash",
                )
        except subprocess.TimeoutExpired:
            return TestResult(
                pattern_id=pattern_id,
                table=table,
                columns_used=self._extract_columns_from_sql(sql),
                sql=sql,
                status=TestStatus.FAILED,
                error_message="Test execution timed out",
                error_type="TimeoutError",
            )
        except Exception as e:
            return TestResult(
                pattern_id=pattern_id,
                table=table,
                columns_used=self._extract_columns_from_sql(sql),
                sql=sql,
                status=TestStatus.FAILED,
                error_message=str(e),
                error_type=type(e).__name__,
            )

    def run_tests(
        self, suite_name: Optional[str] = None, limit: Optional[int] = None
    ) -> List[TestResult]:
        """Execute all generated tests."""

        # Generate test combinations
        tests = self.generate_tests(suite_name)

        if limit:
            tests = tests[:limit]

        print(f"\n{'=' * 80}")
        print(f"Generated {len(tests)} test combinations")
        print(f"{'=' * 80}\n")

        for idx, (pattern_id, table, sql) in enumerate(tests, 1):
            self.test_count += 1

            if self.verbose:
                print(f"[{idx}/{len(tests)}] Pattern {pattern_id}: {sql[:100]}...")

            test_result = self._run_test_subprocess(pattern_id, table, sql)

            if test_result.status == TestStatus.PASSED:
                self.passed_count += 1
                if self.verbose:
                    print(
                        f"  ✓ Passed ({test_result.row_count} rows, {test_result.execution_time_ms:.1f}ms)\n"
                    )
                else:
                    print(".", end="", flush=True)
            elif test_result.status == TestStatus.CRASHED:
                self.crashed_count += 1
                if self.verbose:
                    print(f"  ✗ CRASHED: {test_result.error_message}\n")
                else:
                    print("C", end="", flush=True)
            else:
                self.failed_count += 1
                if self.verbose:
                    print(f"  ✗ Failed: {test_result.error_message}\n")
                else:
                    print("F", end="", flush=True)

            self.results.append(test_result)

        print("\n")
        return self.results

    def _extract_columns_from_sql(self, sql: str) -> List[str]:
        """Extract column names from SQL (simple parsing)."""
        # Basic extraction - finds names after FROM/GROUP BY
        columns = []

        # Extract from GROUP BY
        if "GROUP BY" in sql:
            parts = sql.split("GROUP BY")[1]
            col_str = parts.split(")")[0] if ")" in parts else parts
            for col in col_str.split(","):
                col = col.strip()
                if col and not col.startswith("SELECT"):
                    columns.append(col)

        return columns

    def print_summary(self):
        """Print test execution summary."""
        print(f"\n{'=' * 80}")
        print("TEST EXECUTION SUMMARY")
        print(f"{'=' * 80}\n")

        print(f"Total Tests:      {self.test_count}")
        print(f"Passed:           {self.passed_count} ✓")
        print(f"Failed:           {self.failed_count} ✗")
        print(f"Crashed:          {self.crashed_count} 🔴")
        print(f"Pass Rate:        {100 * self.passed_count / max(1, self.test_count):.1f}%")
        print()

    def print_failures(self):
        """Print details of all failures."""
        failures = [r for r in self.results if r.status != TestStatus.PASSED]

        if not failures:
            print("All tests passed! ✓\n")
            return

        print(f"\n{'=' * 80}")
        print(f"FAILED/CRASHED TESTS ({len(failures)})")
        print(f"{'=' * 80}\n")

        for result in failures:
            print(f"Pattern: {result.pattern_id}")
            print(f"Status: {result.status.value.upper()}")
            print(f"Table: {result.table}")
            print(f"Columns: {', '.join(result.columns_used)}")
            print(f"SQL: {result.sql}")
            if result.error_type:
                print(f"Error Type: {result.error_type}")
            if result.error_message:
                print(f"Error: {result.error_message}")
            print()

    def export_results(self, output_path: str):
        """Export results to JSON file."""
        results_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_tests": self.test_count,
                "passed": self.passed_count,
                "failed": self.failed_count,
                "crashed": self.crashed_count,
                "pass_rate": 100 * self.passed_count / max(1, self.test_count),
            },
            "results": [
                {
                    "pattern_id": r.pattern_id,
                    "table": r.table,
                    "columns_used": r.columns_used,
                    "sql": r.sql,
                    "status": r.status.value,
                    "error_type": r.error_type,
                    "error_message": r.error_message,
                    "row_count": r.row_count,
                    "execution_time_ms": r.execution_time_ms,
                }
                for r in self.results
            ],
        }

        with open(output_path, "w") as f:
            json.dump(results_data, f, indent=2)

        print(f"\nResults exported to: {output_path}")

    def get_problematic_combinations(self) -> List[Dict[str, Any]]:
        """Get list of column combinations that caused failures."""
        problematic = []

        for result in self.results:
            if result.status in (TestStatus.FAILED, TestStatus.CRASHED):
                problematic.append(
                    {
                        "pattern": result.pattern_id,
                        "table": result.table,
                        "columns": result.columns_used,
                        "status": result.status.value,
                        "error_type": result.error_type,
                        "sql": result.sql,
                    }
                )

        return problematic


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate and execute GROUP BY column combination tests"
    )
    parser.add_argument("--config", required=True, help="Path to configuration JSON file")
    parser.add_argument("--suite", help="Specific test suite to run (default: all suites)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--limit", type=int, help="Limit number of tests to run")
    parser.add_argument("--output", help="Export results to JSON file")

    args = parser.parse_args()

    # Create generator and run tests
    generator = ComboTestGenerator(args.config, verbose=args.verbose)

    try:
        generator.run_tests(suite_name=args.suite, limit=args.limit)
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")

    # Print summary and failures
    generator.print_summary()
    generator.print_failures()

    # Print problematic combinations
    problematic = generator.get_problematic_combinations()
    if problematic:
        print(f"\n{'=' * 80}")
        print(f"PROBLEMATIC COLUMN COMBINATIONS ({len(problematic)})")
        print(f"{'=' * 80}\n")

        for item in problematic:
            print(
                f"Pattern {item['pattern']} | Table: {item['table']} | Columns: {', '.join(item['columns'])} | Status: {item['status']}"
            )

        print()

    # Export results if requested
    if args.output:
        generator.export_results(args.output)

    # Return appropriate exit code
    sys.exit(0 if generator.failed_count + generator.crashed_count == 0 else 1)


if __name__ == "__main__":
    main()
