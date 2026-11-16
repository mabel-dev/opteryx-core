"""
OData Benchmark (opteryx.app)
=============================
Runs a benchmark's queries against opteryx.app as OData reads of its views:

    --collection clickbench     benchmarks.clickbench.q_00 .. q_42 (dev/clickbench/web_views.sql)
    --collection tpch_sf<N>     benchmarks.tpch_sf<N>.q_01 .. q_22 (dev/tpch/web_views.sql)

    GET https://odata.opteryx.app/api/v4/benchmarks/<collection>/q_NN

Each try is timed from request to the last page of the response body parsed
(following @odata.nextLink), so it measures what an OData client sees: auth
excluded, network and serialisation included.

Credentials never touch the repo or the result file - they are read from
Vaultwarden (dev/secret_store.py):

    CLIENT_ID      opteryx.app client id
    CLIENT_SECRET  opteryx.app client secret

Output: odata/results.local.json for clickbench, odata/results.local.<collection>.json
otherwise (ClickBench result format; a failed try is null and the run exits non-zero).

Usage:
    python tests/performance/clickbench/odata/runner.py [--collection tpch_sf10]
"""

import argparse
import datetime
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "dev"))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
from _common import format_ratio  # noqa: E402
from secret_store import get_secret  # noqa: E402

AUTH_URL = "https://authenticate.opteryx.app/token"
ODATA_BASE = "https://odata.opteryx.app/api/v4/benchmarks"
TPCH_SCALES = ("001", "01", "1", "10", "100", "1000")
TRIES = 3
TIMEOUT = 600
# Re-authenticate this many seconds before the token says it expires.
TOKEN_EXPIRY_SKEW = 60
# Same threshold as the opteryx runner: (max - min) / min above this is UNSTABLE.
UNSTABLE_SPREAD = 0.45


class Token:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._header = None
        self._expires_at = 0.0

    def header(self) -> str:
        if self._header is None or time.monotonic() >= self._expires_at:
            self._refresh()
        return self._header

    def _refresh(self) -> None:
        body = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
        ).encode()
        req = urllib.request.Request(
            AUTH_URL,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
        token = payload["access_token"]
        token_type = payload.get("token_type", "bearer")
        self._header = f"{token_type[0].upper()}{token_type[1:].lower()} {token}"
        self._expires_at = time.monotonic() + float(payload["expires_in"]) - TOKEN_EXPIRY_SKEW


def view_numbers(collection: str) -> list:
    """The q_NN view numbers a benchmarks collection holds: ClickBench is 0-based, TPC-H 1-based."""
    if collection == "clickbench":
        return list(range(43))
    if collection in {f"tpch_sf{scale}" for scale in TPCH_SCALES}:
        return list(range(1, 23))
    raise ValueError(f"unknown benchmarks collection {collection!r}")


def default_output(here: str, collection: str) -> str:
    name = "results.local.json" if collection == "clickbench" else f"results.local.{collection}.json"
    return os.path.join(here, name)


def read_view(url: str, token: Token) -> int:
    """Read every page of an OData entity set; returns the row count."""
    rows = 0
    while url is not None:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "Authorization": token.header()}
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            payload = json.loads(resp.read())
        rows += len(payload["value"])
        # nextLink may be relative (OData allows it; opteryx.app sends `/api/v4/...?$skip=N`).
        next_link = payload.get("@odata.nextLink")
        url = None if next_link is None else urllib.parse.urljoin(url, next_link)
    return rows


def describe_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        detail = exc.read().decode(errors="replace")[:500]
        return f"HTTP {exc.code}: {detail}"
    return f"{type(exc).__name__}: {exc}"


def run_benchmark(token: Token, collection: str) -> tuple[list, list]:
    """Returns (per-query try times in seconds, None for a failed try; failure messages)."""
    results = []
    failures = []
    for idx in view_numbers(collection):
        query_num = f"Q{idx:02d}"
        url = f"{ODATA_BASE}/{collection}/q_{idx:02d}"
        times = []
        errors = []
        # Each try's time is printed as it lands; the finished line is then
        # rewritten in place (CR) with the fastest/slowest coloured.
        print(f"  {query_num} {TRIES} runs (", end="", flush=True)
        for attempt in range(TRIES):
            t0 = time.monotonic()
            try:
                read_view(url, token)
                elapsed = round(time.monotonic() - t0, 3)
            except (urllib.error.URLError, TimeoutError) as exc:
                errors.append(f"{query_num} try {attempt + 1}: {describe_error(exc)}")
                elapsed = None
            times.append(elapsed)
            status = f"{elapsed:5.2f}" if elapsed is not None else "  ERR"
            print(f"{', ' if attempt else ''}{status}", end="", flush=True)
        results.append(times)
        if errors:
            # A query with any failed try is a failure, never a fast time.
            failures.extend(errors)
            print(f"\r\033[K  {query_num} FAILED: {errors[0][:90]}")
            continue
        fastest = times.index(min(times))
        slowest = times.index(max(times))
        cells = []
        for try_ix, seconds in enumerate(times):
            cell = f"{seconds:5.2f}"
            if try_ix == fastest:
                cell = f"\033[38;2;34;197;94m{cell}\033[0m"
            elif try_ix == slowest:
                cell = f"\033[38;2;255;69;69m{cell}\033[0m"
            cells.append(cell)
        print(f"\r\033[K  {query_num} {TRIES} runs ({', '.join(cells)})")
    return results, failures


def print_summary(
    results: list, failures: list, wall_seconds: float, numbers: list, baselines: tuple = ()
) -> None:
    """Same summary shape as the opteryx runner: per-query table, unstable list, SUM OF MINIMUMS.

    `baselines` is (label, result) pairs: other systems' ClickBench-format `result` for the same
    queries. Each adds a column with its min and the coloured [ratio], like the DuckDB-compared
    runners, and a VS line whose total ratio is over the queries that passed in BOTH runs.
    """
    for label, baseline in baselines:
        assert len(baseline) == len(results), f"{label} baseline has {len(baseline)} queries, run has {len(results)}"
    print()
    header = f"{'Query':<7} {'Min':>11} {'Median':>11} {'Max':>11} {'Spread':>9}  {'Rounds':<8}"
    for label, _ in baselines:
        header += f" {label:>11}  {'vs':<9}"
    print(header)
    print("-" * (len(header) + 4))

    passed = 0
    failed = 0
    sum_min_ms = 0.0
    unstable = []
    query_count = len(numbers)
    # per baseline: [queries compared, our ms over them, its ms over them]
    compared = [[0, 0.0, 0.0] for _ in baselines]
    for row, (idx, times) in enumerate(zip(numbers, results)):
        query_num = f"Q{idx:02d}"
        if None in times:
            print(f"{query_num:<7} {'FAILED':>11}")
            failed += 1
            continue
        ms = [t * 1000 for t in times]
        min_ms = min(ms)
        max_ms = max(ms)
        med_ms = statistics.median(ms)
        spread = (max_ms - min_ms) / min_ms if min_ms > 0 else 0.0
        spread_str = f"{spread * 100:>8.1f}%"
        if spread > UNSTABLE_SPREAD:
            unstable.append((query_num, spread))
            spread_str = f"\033[38;2;255;165;0m{spread * 100:>8.1f}%\033[0m"
        line = f"{query_num:<7} {min_ms:>9.2f}ms {med_ms:>9.2f}ms {max_ms:>9.2f}ms {spread_str}  {len(ms):<8}"
        for (_, baseline), tally in zip(baselines, compared):
            if None in baseline[row]:
                line += f" {'FAILED':>11}{'':11}"
                continue
            base_ms = min(baseline[row]) * 1000
            ratio = format_ratio(min_ms, base_ms)
            # pad on the uncoloured width so the next baseline's column lines up
            line += f" {base_ms:>9.2f}ms  {ratio}{'':{9 - len(re.sub(r'\033\[[0-9;]*m', '', ratio))}}"
            tally[0] += 1
            tally[1] += min_ms
            tally[2] += base_ms
        print(line)
        sum_min_ms += min_ms
        passed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if unstable:
        print(
            f"\n\033[38;2;255;165;0m{len(unstable)} UNSTABLE QUERIES\033[0m "
            f"(spread > {UNSTABLE_SPREAD * 100:.0f}% of their own minimum — the service moved "
            f"under them, so their minimum is not a usable signal):"
        )
        for query_num, spread in sorted(unstable, key=lambda x: -x[1]):
            print(f"  {query_num}  {spread * 100:.1f}%")

    if failures:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for err in failures:
            print(err)

    # Headline total: the sum of each query's BEST (minimum) time — OUR metric,
    # not the published ClickBench one. Failed queries contribute nothing, so a
    # run with failures is not comparable to a clean one.
    summary = (
        f"\n\033[38;2;139;233;253m\033[3mSUM OF MINIMUMS\033[0m  {sum_min_ms / 1000:.2f}s "
        f"({sum_min_ms:.0f}ms) over {passed}/{query_count} queries"
    )
    if failed:
        summary += f"\n  \033[0;31m{failed} queries failed and contribute 0ms — this total is not comparable to a clean run\033[0m"
    for (label, _), (count, ours_ms, theirs_ms) in zip(baselines, compared):
        if count:
            summary += (
                f"\n\033[38;2;139;233;253m\033[3mVS {label.upper()}\033[0m  "
                f"{format_ratio(ours_ms, theirs_ms)}  {ours_ms:.0f}ms vs {theirs_ms:.0f}ms "
                f"over the {count}/{query_count} queries that passed in both"
            )
    print(summary)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({wall_seconds:.2f} seconds wall)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // query_count}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="OData benchmark against opteryx.app")
    parser.add_argument("--collection", default="clickbench", help="benchmarks.<collection> to read")
    parser.add_argument("--output", help="Output JSON path (default: results.local[.<collection>].json here)")
    args = parser.parse_args()
    numbers = view_numbers(args.collection)
    output = args.output or default_output(os.path.dirname(__file__), args.collection)

    token = Token(get_secret("CLIENT_ID"), get_secret("CLIENT_SECRET"))
    token.header()  # authenticate up front: a credential failure stops the run before any timing

    print(f"Opteryx {args.collection} — OData (opteryx.app)")
    print(f"  Endpoint       : {ODATA_BASE}/{args.collection}/q_NN")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {output}")
    print()

    start = time.monotonic()
    results, failures = run_benchmark(token, args.collection)
    wall_seconds = time.monotonic() - start

    payload = {
        "system": "Opteryx (opteryx.app, OData)",
        "date": datetime.date.today().isoformat(),
        "machine": "opteryx.app",
        "cluster_size": "serverless",
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["C++", "column-oriented", "serverless", "managed", "no-cold"],
        "load_time": 0,
        "data_size": 0,
        "result": results,
    }
    with open(output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nresults written to {output}")

    print_summary(results, failures, wall_seconds, numbers)

    if failures:
        print(f"ERROR: {len(failures)} of {len(numbers) * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
