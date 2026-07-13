"""
Turn a pytest --report-log JSONL file into a short markdown summary for a PR comment.

Requires the pytest-reportlog plugin (`--report-log` is not a pytest built-in).

Usage: python pytest_report.py <results.jsonl> <report.md>
"""

import json
import sys
from collections import defaultdict

from rugo import jsonl

MARKER = "<!-- pytest-report -->"
OUTCOME_PRIORITY = {"failed": 2, "skipped": 1, "passed": 0}
SLOWEST_N = 10


def short_message(longrepr_json):
    if not longrepr_json:
        return ""
    parsed = json.loads(longrepr_json)
    if isinstance(parsed, dict):
        return parsed.get("reprcrash", {}).get("message", "")
    if isinstance(parsed, list) and len(parsed) >= 3:
        return parsed[2]
    return str(parsed)


def collect_results(results_path):
    """Reduce pytest's per-phase (setup/call/teardown) TestReport rows to one
    outcome per test. A test can fail or be skipped in any phase, so we can't
    just filter to when == "call" - we take the worst outcome across phases.
    """
    phases_by_test = defaultdict(list)

    with jsonl.read_jsonl(
        results_path,
        columns=["nodeid", "when", "outcome", "duration", "longrepr"],
        predicates=[("$report_type", "==", "TestReport")],
    ) as reader:
        for morsel in reader:
            for nodeid, when, outcome, duration, longrepr in zip(
                morsel.column("nodeid").to_pylist(),
                morsel.column("when").to_pylist(),
                morsel.column("outcome").to_pylist(),
                morsel.column("duration").to_pylist(),
                morsel.column("longrepr").to_pylist(),
            ):
                phases_by_test[nodeid].append((when, outcome, duration, longrepr))

    results = {}
    for nodeid, phases in phases_by_test.items():
        outcome = max(phases, key=lambda p: OUTCOME_PRIORITY.get(p[1], 0))[1]
        duration = sum(p[2] for p in phases)
        message = ""
        for _when, o, _d, longrepr in phases:
            if o != "passed" and longrepr:
                message = short_message(longrepr)
        results[nodeid] = (outcome, duration, message)
    return results


def render_report(results):
    counts = defaultdict(int)
    for outcome, _duration, _message in results.values():
        counts[outcome] += 1

    failures = sorted(
        (nodeid, message) for nodeid, (outcome, _d, message) in results.items() if outcome == "failed"
    )
    slowest = sorted(
        ((nodeid, duration) for nodeid, (_o, duration, _m) in results.items()),
        key=lambda x: -x[1],
    )[:SLOWEST_N]

    lines = [
        MARKER,
        f"**{counts['passed']} passed**, **{counts['failed']} failed**, "
        f"**{counts['skipped']} skipped**",
    ]

    if failures:
        lines.append("\n<details><summary>Failures</summary>\n")
        for nodeid, message in failures:
            lines.append(f"- `{nodeid}`: {message}")
        lines.append("\n</details>")

    if slowest:
        lines.append("\n<details><summary>Slowest tests</summary>\n")
        lines.append("| test | duration |")
        lines.append("|---|---|")
        for nodeid, duration in slowest:
            lines.append(f"| {nodeid} | {duration:.3f}s |")
        lines.append("\n</details>")

    return "\n".join(lines)


if __name__ == "__main__":
    results_path, report_path = sys.argv[1], sys.argv[2]
    results = collect_results(results_path)
    report = render_report(results)
    with open(report_path, "w") as f:
        f.write(report)
    print(report)
