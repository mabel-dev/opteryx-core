"""Verify and compare the preserved ORC fixed-width benchmark JSON samples."""

import argparse
import json
from pathlib import Path
import statistics


def load(path):
    return json.loads(Path(path).read_text())


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("baseline")
    ap.add_argument("post")
    ap.add_argument("output")
    args = ap.parse_args()
    require(Path(args.output) not in (Path(args.baseline), Path(args.post)),
            "comparison output must not overwrite either sample file")
    base, post = load(args.baseline), load(args.post)
    for key in ("rows", "rounds", "iterations", "pyarrow", "python", "platform", "checksum"):
        require(base[key] == post[key], f"metadata mismatch for {key}: {base[key]!r} != {post[key]!r}")
    require(len(base["cases"]) == len(post["cases"]) == 64, "expected exactly 64 cases in each run")
    baseline_cases = {(c["name"], c["null_permille"]): c for c in base["cases"]}
    post_cases = {(c["name"], c["null_permille"]): c for c in post["cases"]}
    require(len(baseline_cases) == len(post_cases) == 64, "duplicate case keys")
    require(baseline_cases.keys() == post_cases.keys(), "case names/densities differ")
    require([(c["name"], c["null_permille"]) for c in base["cases"]] ==
            [(c["name"], c["null_permille"]) for c in post["cases"]], "case order differs")

    lines = []
    regressions = []
    improvements = []
    for before in base["cases"]:
        key = (before["name"], before["null_permille"])
        after = post_cases[key]
        for field in ("name", "null_permille", "nulls", "bytes", "sha256"):
            require(before[field] == after[field], f"{key}: mismatch in {field}")
        a, b = before["samples"], after["samples"]
        require(len(a) == len(b) == base["rounds"] and all(x > 0 for x in a + b),
                f"{key}: invalid sample count or value")
        am, bm = statistics.median(a), statistics.median(b)
        delta = (bm / am - 1) * 100
        overlap = max(min(a), min(b)) <= min(max(a), max(b))
        label = f"{key[0]} ({key[1] / 10:g}% null)"
        if delta > 5:
            regressions.append((label, delta, overlap))
        if delta < -5:
            improvements.append((label, delta, overlap))
        lines.append(f"| {key[0]} | {key[1] / 10:g}% | {am:.3f} | {min(a):.3f}–{max(a):.3f} | "
                     f"{bm:.3f} | {min(b):.3f}–{max(b):.3f} | {delta:+.1f}% | "
                     f"{'overlap' if overlap else 'separate'} |")

    report = [
        "# ORC fixed-width decode: post-change comparison — 2026-10-27", "",
        "## Commands", "",
        "```sh",
        "make bench-orc-fixed ORC_FIXED_ARGS='--rows 65536 --rounds 9 --iterations 64 --output dev/bench_orc_fixed_baseline_20261027.json'",
        "make bench-orc-fixed ORC_FIXED_ARGS='--rows 65536 --rounds 9 --iterations 64 --output dev/bench_orc_fixed_post_20261027.json'",
        "python3.14 dev/compare_orc_fixed.py dev/bench_orc_fixed_baseline_20261027.json dev/bench_orc_fixed_post_20261027.json dev/bench_orc_fixed_comparison_20261027.md",
        "```", "",
        f"Baseline samples: `{args.baseline}`. Post-change samples: `{args.post}`.",
        "The first command was run before the reader change; the second was run after rebuilding.", "",
        "## Matching and methodology", "",
        "All 64 case keys and their order, target densities, actual null counts, file sizes and",
        "SHA-256 fixture hashes matched. Row count, iteration/round budget, PyArrow version,",
        "Python version, platform and final output checksum matched. Both benchmark runs checked",
        "every validity bit and valid value, plus source/output types, ORC kind, row count,",
        "single-stripe layout and lack of compression, before timing. The JSON does not retain",
        "those individual metadata assertions; they were checked by the native bridge each run.", "",
        f"Reader SHA-256: baseline `{base['rugo_native_sha256']}`; post-change `{post['rugo_native_sha256']}`.",
        "Draken binary SHA-256 was `3d64e1a4e516c4eea9971c492b4314ad8eb5b8e8b3b7fcdfdbfa3edbb041dd1c`",
        "in both runs. Apple M5 Pro, macOS 26.6.2 arm64; PyArrow 25.0.1; bridge compiled by",
        "Apple clang 21.0.0, `-O3 -std=c++20 -fPIC`. Reader binary build options were not",
        "independently established. See `dev/bench_orc_fixed_baseline_20261027.md` for fixture",
        "generation, measurement methodology and limitations.", "",
        "Before the post-change run, a process-list check found no matching test processes. A",
        "separate monitor sampled `ps -axo pid,ppid,etime,command` every 0.5 seconds while the",
        "benchmark process existed and reported no matching pytest, sqllogictest, make-test,",
        "test-script or native `*_test` processes. This is sampling, not process isolation:",
        "short-lived or differently named tests could have been missed; the monitor itself can",
        "add system noise. The prior baseline did not use this monitor.", "",
        "The runs were sequential, not interleaved A/B. Ranges are minima/maxima of nine rounds,",
        "not confidence intervals; overlapping ranges signal ambiguity, and separate ranges do",
        "not prove causality. Background activity, frequency and run-to-run effects can affect",
        "results. Synthetic hot in-memory decoding is not end-to-end or representative of x86.", "",
        "## Summary", "",
        f"Using an arbitrary 5% median-change reporting threshold, {len(improvements)} cases improved",
        f"and {len(regressions)} regressed; the other {64 - len(improvements) - len(regressions)} were within ±5%.",
        "Positive delta means the post-change decode is slower; negative means faster.",
        "Regressions over 5%:",
    ]
    for name, delta, overlap in sorted(regressions, key=lambda x: -x[1]):
        report.append(f"- {name}: +{delta:.1f}% ({'sample ranges overlap' if overlap else 'sample ranges separate'}).")
    report.extend([
        "", "## All cases", "",
        "Times are ns per logical row; each range is min–max across nine rounds.", "",
        "| Case | Target nulls | Baseline median | Baseline range | Post median | Post range | Delta | Ranges |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
        *lines, "",
    ])
    Path(args.output).write_text("\n".join(report))
    print(f"Verified 64 matching cases; wrote {args.output}")
    print(f">5% faster: {len(improvements)}; >5% slower: {len(regressions)}; within ±5%: {64 - len(improvements) - len(regressions)}")
    for name, delta, overlap in sorted(regressions, key=lambda x: -x[1]):
        print(f"  {name}: +{delta:.1f}%; ranges {'overlap' if overlap else 'separate'}")


if __name__ == "__main__":
    main()
