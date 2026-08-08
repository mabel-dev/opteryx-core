"""Helpers for exporting a generated aggregate catalog for documentation."""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.operators.aggregate.helpers import AGGREGATORS

_GLOBAL_SUPPORTED = frozenset(
    {
        "CORR",
        "COUNT",
        "SUM",
        "MIN",
        "MAX",
        "AVG",
        "MEDIAN",
        "STDDEV",
        "COUNT_DISTINCT",
        "APPROX_COUNT_DISTINCT",
        "APPROX_PERCENTILE",
        "ARRAY_AGG",
        "CIDR_AGG",
    }
)

_GROUPED_SUPPORTED = _GLOBAL_SUPPORTED | frozenset({"ANY_VALUE"})

_STRICT_GROUPED_SUPPORTED = frozenset(
    {
        "CORR",
        "COUNT",
        "SUM",
        "MIN",
        "MAX",
        "AVG",
        "MEDIAN",
        "STDDEV",
        "COUNT_DISTINCT",
        "APPROX_COUNT_DISTINCT",
        "APPROX_PERCENTILE",
        "ARRAY_AGG",
        "ANY_VALUE",
        "CIDR_AGG",
    }
)

_FRIENDLY_NAMES = {
    "ANY_VALUE": "Any Value",
    "APPROX_COUNT_DISTINCT": "Approximate Count Distinct",
    "APPROX_PERCENTILE": "Approximate Percentile",
    "ARRAY_AGG": "Array Aggregate",
    "CIDR_AGG": "CIDR Aggregate",
    "AVG": "Average",
    "CORR": "Correlation",
    "COUNT": "Count",
    "COUNT_DISTINCT": "Count Distinct",
    "MAX": "Maximum",
    "MEDIAN": "Median",
    "MIN": "Minimum",
    "SUM": "Sum",
    "STDDEV": "Standard Deviation",
}

_CATEGORIES = {
    "ANY_VALUE": "selection",
    "APPROX_COUNT_DISTINCT": "approximate",
    "APPROX_PERCENTILE": "approximate",
    "ARRAY_AGG": "collection",
    "CIDR_AGG": "collection",
    "AVG": "numeric",
    "CORR": "numeric",
    "COUNT": "counting",
    "COUNT_DISTINCT": "counting",
    "MAX": "extrema",
    "MEDIAN": "numeric",
    "MIN": "extrema",
    "SUM": "numeric",
    "STDDEV": "numeric",
}

_SUMMARIES = {
    "ANY_VALUE": "Returns one non-null value from the input set.",
    "APPROX_COUNT_DISTINCT": "Estimates the number of distinct input values.",
    "APPROX_PERCENTILE": "Estimates a percentile using sketch-based aggregation.",
    "ARRAY_AGG": "Collects input values into an array.",
    "CIDR_AGG": "Collects IPv4 addresses into the smallest list of CIDR blocks that covers exactly those addresses.",
    "AVG": "Computes the arithmetic mean of the input values.",
    "CORR": "Computes the Pearson correlation coefficient between two numeric columns.",
    "COUNT": "Counts rows or non-null input values.",
    "COUNT_DISTINCT": "Counts distinct non-null input values.",
    "MAX": "Returns the largest non-null input value.",
    "MEDIAN": "Computes the exact median (middle value) of the input values.",
    "MIN": "Returns the smallest non-null input value.",
    "SUM": "Sums the input values.",
    "STDDEV": "Computes the population standard deviation of the input values.",
}

_DOCUMENTATION = {
    "ANY_VALUE": "Useful when a grouped query only needs one representative value from each group.",
    "APPROX_COUNT_DISTINCT": "Uses a sketch-based estimator instead of exact deduplication.",
    "APPROX_PERCENTILE": "Accepts an input expression and a percentile literal between 0.0 and 1.0.",
    "ARRAY_AGG": "Supports DISTINCT, ORDER BY, and LIMIT forms in the aggregate surface.",
    "CIDR_AGG": "Returns ARRAY<VARCHAR> of CIDR blocks, ascending and non-overlapping. The cover is MINIMAL and unique: adjacent addresses fold into the largest aligned block, so 10.0.0.0-10.0.0.7 becomes a single 10.0.0.0/29. The operand must be IPV4 (a plain integer column is rejected). Duplicate addresses are free - the set deduplicates on insert - and NULLs are not members, so a group with no addresses returns an empty array rather than NULL. Works with and without GROUP BY. Bounded by two independent budgets, one on the collected address set and one on the emitted text: see @@cidr_agg_state_budget_bytes and @@cidr_agg_emit_budget_bytes.",
    "AVG": "Ignores nulls and divides the running sum by the number of non-null values.",
    "CORR": "Pearson correlation over (x, y) pairs where both values are non-null. Returns DOUBLE in [-1, 1]; NULL when undefined (no pairs, or zero variance in either input). DECIMAL inputs must be CAST to DOUBLE first.",
    "COUNT": "COUNT(*) counts rows, while COUNT(expr) counts non-null values.",
    "COUNT_DISTINCT": "Exact distinct count over the non-null input values.",
    "MAX": "Returns the greatest comparable non-null value encountered.",
    "MEDIAN": "Buffers all non-null values per group and selects the middle. Even-count inputs interpolate; result type is FLOAT. Buffering is bounded by a global 512MB memory budget — exceeding it raises an error. Decimal inputs must be CAST to FLOAT.",
    "MIN": "Returns the smallest comparable non-null value encountered.",
    "SUM": "Nulls are ignored; non-null values are accumulated.",
    "STDDEV": "Population standard deviation (N denominator, not N-1/sample). Ignores nulls. DECIMAL inputs must be CAST to DOUBLE first.",
}

_SQL_FORMS = {
    "ANY_VALUE": ["ANY_VALUE(expr)"],
    "APPROX_COUNT_DISTINCT": ["APPROX_COUNT_DISTINCT(expr)"],
    "APPROX_PERCENTILE": ["APPROX_PERCENTILE(expr, percentile)"],
    "CIDR_AGG": ["CIDR_AGG(ipv4_expr)"],
    "ARRAY_AGG": [
        "ARRAY_AGG(expr)",
        "ARRAY_AGG(DISTINCT expr)",
        "ARRAY_AGG(expr LIMIT n)",
        "ARRAY_AGG(expr ORDER BY expr [ASC|DESC] LIMIT n)",
    ],
    "AVG": ["AVG(expr)"],
    "CORR": ["CORR(x, y)"],
    "COUNT": ["COUNT(*)", "COUNT(expr)", "COUNT(DISTINCT expr)"],
    "COUNT_DISTINCT": ["COUNT_DISTINCT(expr)", "COUNT(DISTINCT expr)"],
    "MAX": ["MAX(expr)"],
    "MEDIAN": ["MEDIAN(expr)"],
    "MIN": ["MIN(expr)"],
    "SUM": ["SUM(expr)"],
    "STDDEV": ["STDDEV(expr)"],
}


def _friendly_name(aggregate: str) -> str:
    return _FRIENDLY_NAMES.get(aggregate, aggregate.replace("_", " ").title())


def _aggregate_support(aggregate: str) -> dict[str, bool]:
    return {
        "global": aggregate in _GLOBAL_SUPPORTED,
        "grouped": aggregate in _GROUPED_SUPPORTED,
        "strict_grouped": aggregate in _STRICT_GROUPED_SUPPORTED,
    }


def _aggregate_status(aggregate: str) -> str:
    support = _aggregate_support(aggregate)
    if support["grouped"] or support["global"]:
        if support["strict_grouped"]:
            return "active"
        return "fallback"
    return "unsupported"


def export_aggregate_catalog() -> OrderedDict[str, dict[str, Any]]:
    exported: dict[str, dict[str, Any]] = {}
    for aggregate in sorted(AGGREGATORS):
        exported[aggregate] = {
            "ast_symbol": aggregate,
            "friendly_name": _friendly_name(aggregate),
            "kernel_name": AGGREGATORS[aggregate],
            "category": _CATEGORIES.get(aggregate, "other"),
            "status": _aggregate_status(aggregate),
            "summary": _SUMMARIES.get(aggregate, aggregate),
            "description": _SUMMARIES.get(aggregate, aggregate),
            "documentation": _DOCUMENTATION.get(aggregate, aggregate),
            "sql_forms": _SQL_FORMS.get(aggregate, [f"{aggregate}(expr)"]),
            "support": _aggregate_support(aggregate),
        }

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_aggregate_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_aggregate_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
