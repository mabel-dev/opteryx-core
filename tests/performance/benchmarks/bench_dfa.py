"""
Benchmark: DFA regex_replace vs RE2 — per-layer cost isolation.

Measures each layer of the call stack independently so the bottleneck
is unambiguous rather than guessed at from code reading.

Layers tested
─────────────
  L0  Python-side setup per morsel
        _normalise_replacement()
        RegexToDFACompiler().compile()
        proc.to_cython_args()

  L1  Cython ops-struct build
        The malloc + Python-tuple → C-struct loop inside execute_regex_procedure,
        isolated by calling it with a zero-row StringVector.

  L2  Cython per-row DFA execution
        execute_regex_procedure on N rows (full path minus L0/L1 overhead).

  L3  RE2 reference
        vector_regex_replace on the same N rows.

  L4  Full Python wrapper
        regex_replace() end-to-end including all overhead.

  L5  _dfa_replace alias
        Same path but entered via the _DFA_REPLACE evaluator alias.

Usage
─────
    python bench_dfa.py [--rows N] [--morsels M] [--pattern PATTERN]

Defaults: 65_536 rows per morsel, 64 morsels (≈ 4M total rows).
"""

import argparse
import gc
import statistics
import time
from typing import Callable

import numpy
import pyarrow

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="DFA regex_replace benchmark")
parser.add_argument("--rows", type=int, default=65_536, help="Rows per morsel")
parser.add_argument("--morsels", type=int, default=64, help="Number of morsels to simulate")
parser.add_argument(
    "--pattern",
    default="url",
    choices=["url", "prefix"],
    help="Pattern family to benchmark (url=domain-extract, prefix=M?(.+)$)",
)
parser.add_argument("--warmup", type=int, default=4, help="Warm-up iterations")
parser.add_argument("--repeat", type=int, default=16, help="Timed iterations")
args = parser.parse_args()

ROWS = args.rows
MORSELS = args.morsels
WARMUP = args.warmup
REPEAT = args.repeat

# ---------------------------------------------------------------------------
# Pattern / replacement selection
# ---------------------------------------------------------------------------

if args.pattern == "url":
    PATTERN_BYTES = b"^https?://(?:www\\.)?([^/]+)/.*$"
    REPL_BYTES = b"\\1"  # canonical form (already normalised)
    REPL_SQL_FORM = b"\\\\1"  # SQL r'\\1' 3-byte form

    def _make_url(i: int) -> str:
        hosts = [
            "www.example.com",
            "foo.bar",
            "sub.domain.net",
            "www.google.com",
            "plain.example.com",
            "www.with-prefix.example.com",
            "news.ycombinator.com",
            "cdn.static.io",
        ]
        return f"https://{hosts[i % len(hosts)]}/path/to/page?q={i}"

    RAW_STRINGS = [_make_url(i) for i in range(ROWS)]

else:  # prefix
    PATTERN_BYTES = b"^M?(.+)$"
    REPL_BYTES = b"\\1"
    REPL_SQL_FORM = b"\\\\1"

    PLANET_BASES = ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"]
    RAW_STRINGS = [PLANET_BASES[i % len(PLANET_BASES)] for i in range(ROWS)]

# ---------------------------------------------------------------------------
# Build test data once
# ---------------------------------------------------------------------------

print(f"\nBuilding test data: {ROWS:,} rows × {MORSELS} morsels")
print(f"Pattern : {PATTERN_BYTES.decode()}")
print(f"Repl    : {REPL_BYTES!r}\n")

ARROW_ARRAY = pyarrow.array(RAW_STRINGS)
PAT_ARR = numpy.array([PATTERN_BYTES], dtype=object)
REPL_ARR = numpy.array([REPL_BYTES], dtype=object)
REPL_SQL_ARR = numpy.array([REPL_SQL_FORM], dtype=object)

# ---------------------------------------------------------------------------
# Import everything we'll benchmark
# ---------------------------------------------------------------------------

from opteryx.draken.vectors.string_vector import StringVector

from opteryx.compiled import regex_procedures as _rp
from opteryx.compiled import vector_ops as _vops
from opteryx.expression.functions.implementations.text import (
    _as_string_vector,
    _dfa_replace,
    _normalise_replacement,
    regex_replace,
)
from opteryx.expression.functions.regex_compiler import RegexToDFACompiler

vector_regex_replace = getattr(_vops, "vector_regex_replace")

# Compile once for direct Cython calls
_normed_repl = _normalise_replacement(REPL_BYTES)
_compiler = RegexToDFACompiler()
_proc = _compiler.compile(PATTERN_BYTES, _normed_repl)
assert not _proc.fallback_to_re2, "Pattern must compile to DFA for this benchmark"
_OPS, _OPS_LEN, _FALLBACK = _proc.to_cython_args()

# Build StringVector once for Cython-direct tests
_SV = _as_string_vector(ARROW_ARRAY)

# Zero-row StringVector for ops-overhead isolation
_SV_EMPTY = StringVector.from_arrow(pyarrow.array([], type=pyarrow.string()))


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def _bench(label: str, fn: Callable, n_rows_per_call: int) -> None:
    """Run fn WARMUP+REPEAT times, report median throughput and per-row latency."""
    gc.disable()
    try:
        # warm up
        for _ in range(WARMUP):
            fn()

        times = []
        for _ in range(REPEAT):
            t0 = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t0)
    finally:
        gc.enable()

    med_s = statistics.median(times)
    med_us = med_s * 1e6
    ns_per_row = (med_s / n_rows_per_call) * 1e9 if n_rows_per_call > 0 else 0
    mrps = n_rows_per_call / med_s / 1e6 if med_s > 0 else 0

    # coefficient of variation
    cv = statistics.stdev(times) / statistics.mean(times) * 100 if len(times) > 1 else 0

    print(
        f"  {label:<52s}  {med_us:>9.1f} µs/call   "
        f"{ns_per_row:>7.1f} ns/row   "
        f"{mrps:>6.2f} Mrow/s   "
        f"cv={cv:4.1f}%"
    )


# ---------------------------------------------------------------------------
# L0 — Python-side setup cost per morsel (no data processing)
# ---------------------------------------------------------------------------

print("=" * 80)
print("L0  Python-side setup overhead per morsel (no row processing)")
print("=" * 80)


def _l0_normalise():
    _normalise_replacement(REPL_SQL_FORM)


def _l0_compile_only():
    c = RegexToDFACompiler()
    c.compile(PATTERN_BYTES, _normed_repl)


def _l0_compile_and_args():
    c = RegexToDFACompiler()
    p = c.compile(PATTERN_BYTES, _normed_repl)
    p.to_cython_args()


def _l0_imports_compile_args():
    from opteryx.compiled import regex_procedures as _rp2  # noqa: F401
    from opteryx.expression.functions.regex_compiler import RegexToDFACompiler as _C

    c = _C()
    p = c.compile(PATTERN_BYTES, _normed_repl)
    p.to_cython_args()


_bench("_normalise_replacement() only", _l0_normalise, 0)
_bench("RegexToDFACompiler().compile()", _l0_compile_only, 0)
_bench("compile() + to_cython_args()", _l0_compile_and_args, 0)
_bench("imports + compile() + to_cython_args()", _l0_imports_compile_args, 0)

# ---------------------------------------------------------------------------
# L1 — Cython ops-struct build cost (zero rows)
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print("L1  Cython ops-struct build cost (zero rows — pure overhead)")
print("=" * 80)


def _l1_ops_build_zero_rows():
    _rp.execute_regex_procedure(_SV_EMPTY, _OPS, _OPS_LEN, False)


_bench("execute_regex_procedure(0 rows)", _l1_ops_build_zero_rows, 0)

# ---------------------------------------------------------------------------
# L2 — Cython DFA execution (full, direct, bypassing Python wrapper)
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print(f"L2  Cython DFA execution — direct (bypasses Python compile overhead)")
print("=" * 80)


def _l2_cython_dfa():
    _rp.execute_regex_procedure(_SV, _OPS, _OPS_LEN, False)


_bench(f"execute_regex_procedure({ROWS:,} rows)", _l2_cython_dfa, ROWS)


# Also time the to_arrow() conversion
def _l2_cython_dfa_with_arrow():
    sv = _rp.execute_regex_procedure(_SV, _OPS, _OPS_LEN, False)
    sv.to_arrow()


_bench(f"execute_regex_procedure + to_arrow()", _l2_cython_dfa_with_arrow, ROWS)

# ---------------------------------------------------------------------------
# L3 — RE2 reference (vector_regex_replace, same data)
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print(f"L3  RE2 reference — vector_regex_replace (same {ROWS:,} rows)")
print("=" * 80)


def _l3_re2():
    vector_regex_replace(_SV, PATTERN_BYTES, _normed_repl)


def _l3_re2_with_arrow():
    sv = vector_regex_replace(_SV, PATTERN_BYTES, _normed_repl)
    sv.to_arrow()


_bench("vector_regex_replace()", _l3_re2, ROWS)
_bench("vector_regex_replace() + to_arrow()", _l3_re2_with_arrow, ROWS)

# ---------------------------------------------------------------------------
# L4 — Full Python wrapper (regex_replace) — canonical \\1 form
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print(f"L4  Full Python wrapper — regex_replace() — {ROWS:,} rows")
print("=" * 80)


def _l4_wrapper_canonical():
    regex_replace(ARROW_ARRAY, PAT_ARR, REPL_ARR)


def _l4_wrapper_sql_form():
    regex_replace(ARROW_ARRAY, PAT_ARR, REPL_SQL_ARR)


_bench("regex_replace() — canonical b'\\\\1'", _l4_wrapper_canonical, ROWS)
_bench("regex_replace() — SQL form b'\\\\\\\\1'", _l4_wrapper_sql_form, ROWS)

# ---------------------------------------------------------------------------
# L4b — Simulate multiple morsels (shows total per-query overhead)
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print(f"L4b  Multi-morsel simulation — {MORSELS} × {ROWS:,} rows")
print("=" * 80)


def _l4b_multi_morsel():
    for _ in range(MORSELS):
        regex_replace(ARROW_ARRAY, PAT_ARR, REPL_ARR)


_bench(f"regex_replace() × {MORSELS} morsels", _l4b_multi_morsel, ROWS * MORSELS)

# ---------------------------------------------------------------------------
# L5 — _dfa_replace alias (used by optimizer-rewritten _DFA_REPLACE nodes)
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print(f"L5  _dfa_replace alias — {ROWS:,} rows")
print("=" * 80)


def _l5_dfa_replace():
    _dfa_replace(ARROW_ARRAY, PAT_ARR, REPL_ARR)


_bench("_dfa_replace()", _l5_dfa_replace, ROWS)

# ---------------------------------------------------------------------------
# L6 — Overhead breakdown: L4 minus L2 = Python wrapper tax
# ---------------------------------------------------------------------------

print()
print("=" * 80)
print("Summary: overhead analysis")
print("=" * 80)


def _time_median(fn: Callable) -> float:
    for _ in range(WARMUP):
        fn()
    gc.disable()
    try:
        times = [
            (lambda t0: time.perf_counter() - t0)(time.perf_counter())
            for _ in [fn() for _ in range(REPEAT)]
        ]
        # Re-run properly
        times = []
        for _ in range(REPEAT):
            t0 = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t0)
    finally:
        gc.enable()
    return statistics.median(times)


t_l0 = _time_median(_l0_imports_compile_args)
t_l1 = _time_median(_l1_ops_build_zero_rows)
t_l2 = _time_median(_l2_cython_dfa)
t_l3 = _time_median(_l3_re2)
t_l4 = _time_median(_l4_wrapper_canonical)

print(f"\n  Cython DFA execution   (L2) : {t_l2 * 1e6:>8.1f} µs")
print(f"  RE2 execution          (L3) : {t_l3 * 1e6:>8.1f} µs")
print(f"  DFA speedup vs RE2         : {t_l3 / t_l2:>8.2f}x" if t_l2 > 0 else "  (no data)")
print()
print(f"  Python wrapper total   (L4) : {t_l4 * 1e6:>8.1f} µs")
print(f"  Python setup overhead  (L0) : {t_l0 * 1e6:>8.1f} µs  ({t_l0 / t_l4 * 100:.0f}% of L4)")
print(f"  Cython ops-build       (L1) : {t_l1 * 1e6:>8.1f} µs  ({t_l1 / t_l4 * 100:.0f}% of L4)")
print(f"  Data processing        (L2) : {t_l2 * 1e6:>8.1f} µs  ({t_l2 / t_l4 * 100:.0f}% of L4)")
print(f"  Unaccounted (cast etc)      : {max(0, t_l4 - t_l0 - t_l1 - t_l2) * 1e6:>8.1f} µs")
print()

multi_morsel_overhead = t_l0 * MORSELS
print(f"  Per-query Python overhead across {MORSELS} morsels:")
print(f"    Compile × {MORSELS}            : {multi_morsel_overhead * 1e3:>8.2f} ms")
print(f"    Data processing × {MORSELS}    : {t_l2 * MORSELS * 1e3:>8.2f} ms")
print(
    f"    Compile fraction            : {multi_morsel_overhead / (multi_morsel_overhead + t_l2 * MORSELS) * 100:>7.1f}%"
)
print()

# Verify correctness of both paths
_r_dfa = _rp.execute_regex_procedure(_SV, _OPS, _OPS_LEN, False).to_arrow().to_pylist()
_r_re2 = vector_regex_replace(_SV, PATTERN_BYTES, _normed_repl).to_arrow().to_pylist()
_r_wrap = regex_replace(ARROW_ARRAY, PAT_ARR, REPL_ARR).to_pylist()


def _norm(v):
    return v.decode("utf-8") if isinstance(v, bytes) else v


_match_dfa_re2 = all(_norm(a) == _norm(b) for a, b in zip(_r_dfa[:100], _r_re2[:100]))
_match_dfa_wrap = all(_norm(a) == _norm(b) for a, b in zip(_r_dfa[:100], _r_wrap[:100]))

print(f"  Correctness: DFA == RE2      : {'✓' if _match_dfa_re2 else '✗ MISMATCH'}")
print(f"  Correctness: DFA == wrapper  : {'✓' if _match_dfa_wrap else '✗ MISMATCH'}")

if not _match_dfa_re2:
    print("\n  First 5 DFA results :", [_norm(v) for v in _r_dfa[:5]])
    print("  First 5 RE2 results :", [_norm(v) for v in _r_re2[:5]])

print()
