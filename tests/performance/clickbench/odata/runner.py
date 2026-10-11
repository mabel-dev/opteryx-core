"""
OData ClickBench Benchmark (opteryx.app)
========================================
Runs the 43 ClickBench queries against opteryx.app as OData reads of the views
benchmarks.clickbench.q_00 .. q_42 (created by dev/clickbench/web_views.sql):

    GET https://odata.opteryx.app/api/v4/benchmarks/clickbench/q_NN

Each try is timed from request to the last page of the response body parsed
(following @odata.nextLink), so it measures what an OData client sees: auth
excluded, network and serialisation included.

Credentials follow the ClickBench convention for hosted systems - they come
from the environment, never from the repo or the result file:

    CLIENT_ID      opteryx.app client id
    CLIENT_SECRET  opteryx.app client secret

`--env-file PATH` loads them from a dotenv-style file first (the make target
passes the gitignored repo-root .env). A variable already set in the
environment is not overridden.

Output: odata/results.local.json (ClickBench result format; a failed try is
null and the run exits non-zero).

Usage:
    CLIENT_ID=... CLIENT_SECRET=... python tests/performance/clickbench/odata/runner.py
    python tests/performance/clickbench/odata/runner.py --env-file .env
"""

import argparse
import datetime
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

AUTH_URL = "https://authenticate.opteryx.app/token"
ODATA_BASE = "https://odata.opteryx.app/api/v4/benchmarks/clickbench"
QUERY_COUNT = 43
TRIES = 3
TIMEOUT = 600
# Re-authenticate this many seconds before the token says it expires.
TOKEN_EXPIRY_SKEW = 60
# Same threshold as the opteryx runner: (max - min) / min above this is UNSTABLE.
UNSTABLE_SPREAD = 0.45


def load_env_file(path: str) -> None:
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            os.environ.setdefault(key.strip(), val)


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
        url = payload.get("@odata.nextLink")
    return rows


def describe_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        detail = exc.read().decode(errors="replace")[:500]
        return f"HTTP {exc.code}: {detail}"
    return f"{type(exc).__name__}: {exc}"


def run_benchmark(token: Token) -> tuple[list, list]:
    """Returns (per-query try times in seconds, None for a failed try; failure messages)."""
    results = []
    failures = []
    for idx in range(QUERY_COUNT):
        query_num = f"Q{idx:02d}"
        url = f"{ODATA_BASE}/q_{idx:02d}"
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


def print_summary(results: list, failures: list, wall_seconds: float) -> None:
    """Same summary shape as the opteryx runner: per-query table, unstable list, SUM OF MINIMUMS."""
    print()
    header = f"{'Query':<7} {'Min':>11} {'Median':>11} {'Max':>11} {'Spread':>9}  {'Rounds':<8}"
    print(header)
    print("-" * (len(header) + 4))

    passed = 0
    failed = 0
    sum_min_ms = 0.0
    unstable = []
    for idx, times in enumerate(results):
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
        print(f"{query_num:<7} {min_ms:>9.2f}ms {med_ms:>9.2f}ms {max_ms:>9.2f}ms {spread_str}  {len(ms):<8}")
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
        f"({sum_min_ms:.0f}ms) over {passed}/{QUERY_COUNT} queries"
    )
    if failed:
        summary += f"\n  \033[0;31m{failed} queries failed and contribute 0ms — this total is not comparable to a clean run\033[0m"
    print(summary)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({wall_seconds:.2f} seconds wall)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // QUERY_COUNT}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="OData ClickBench benchmark against opteryx.app")
    parser.add_argument("--env-file", default=None, help="dotenv file to load CLIENT_ID/CLIENT_SECRET from")
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__), "results.local.json"),
        help="Output JSON path (default: results.local.json next to this script)",
    )
    args = parser.parse_args()

    if args.env_file is not None:
        load_env_file(args.env_file)
    missing = [v for v in ("CLIENT_ID", "CLIENT_SECRET") if not os.environ.get(v)]
    if missing:
        print(f"ERROR: {', '.join(missing)} not set", file=sys.stderr)
        return 1

    token = Token(os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])
    token.header()  # authenticate up front: a credential failure stops the run before any timing

    print("Opteryx ClickBench — OData (opteryx.app)")
    print(f"  Endpoint       : {ODATA_BASE}/q_NN")
    print(f"  Tries per query: {TRIES}")
    print(f"  Output         : {args.output}")
    print()

    start = time.monotonic()
    results, failures = run_benchmark(token)
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
    with open(args.output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nresults written to {args.output}")

    print_summary(results, failures, wall_seconds)

    if failures:
        print(f"ERROR: {len(failures)} of {QUERY_COUNT * TRIES} tries failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
