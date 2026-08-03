# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A0 native SELECT-path residual census (developer tooling).

The native C++ engine runs plain ``SELECT`` end-to-end EXCEPT for parquet scans
that fall back to the per-morsel Python trampoline (``StreamingScanSource``).
Every such fallback is one ``return None`` guard in
``opteryx/managers/execution/compiler.py::_native_scan_plan`` (``_Compiler``),
and each records a stable machine-readable reason code on query telemetry
(``scan_residual_reasons``, keyed by scan identity, parallel to ``scan_sources``).

This module is the measurement front-end for that frontier:

  * :func:`scan_residuals` — run one query and read back its per-scan Source
    selection + residual reason codes from telemetry. Used by the A0 acceptance
    gate (``tests/unit/operators/test_native_scan_residual_gate.py``).

  * :data:`HAND_SET` — one canonical query per residual reason that still has a
    single-file SQL trigger, so the reachability test can prove each code stays
    wired (and did not silently drift / die).

  * :func:`census` / :func:`main` — tally ``scan_residual_reasons`` over the
    ``.run_tests`` SQL battery (clickbench + tpch), the corpus
    ``docs/NATIVE_RESIDUAL_PLAN.md`` reports against. Re-running after a close-out
    chip shows the closed reason's count fall.

Nothing here changes query behaviour; it only reads what the engine records.
"""

from __future__ import annotations

import os
import sys
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

# Run-from-tree: this file lives in dev/, so the repo root is one level up.
_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _REPO_ROOT not in sys.path:
    sys.path.insert(1, _REPO_ROOT)

# The .run_tests battery the plan censuses against.
_BATTERY_FILES = (
    "tests/integration/sql_battery/test_data/tests/clickbench.run_tests",
    "tests/integration/sql_battery/test_data/tests/tpch_data.run_tests",
)


def _read_battery() -> List[str]:
    """Parse the ``.run_tests`` battery files into a flat list of SQL statements.

    Statements are semicolon-terminated; ``#`` comment lines and blanks are
    skipped. Mirrors the sql_battery loader closely enough for a census tally.
    """
    statements: List[str] = []
    for rel in _BATTERY_FILES:
        path = os.path.join(_REPO_ROOT, rel)
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                statements.append(stripped.rstrip(";").strip())
    return statements


def scan_residuals(sql: str) -> Tuple[Dict, Dict, Optional[BaseException]]:
    """Run ``sql`` to completion and read its scan-Source census from telemetry.

    Returns ``(scan_sources, scan_residual_reasons, err)``:
      * ``scan_sources``          — {scan_identity: "NativeParquetScanSource" |
                                     "StreamingScanSource"} (plan-time fact,
                                     always present).
      * ``scan_residual_reasons`` — {scan_identity: reason_code} for the trampoline
                                     scans only ({} when every scan went native).
      * ``err``                   — the exception if the query raised, else None
                                     (with empty dicts).
    """
    import opteryx

    session = opteryx.session()
    try:
        for morsel in session.execute_to_morsels(sql):
            _ = morsel.num_rows
    except BaseException as exc:  # noqa: BLE001 — the census records, never swallows
        return {}, {}, exc
    telemetry = session._telemetry.as_dict()
    sources = dict(telemetry.get("scan_sources", {}))
    reasons = dict(telemetry.get("scan_residual_reasons", {}))
    return sources, reasons, None


# ---------------------------------------------------------------------------
# Canonical single-file triggers, one per residual reason that is still an open
# frontier with a reachable SQL trigger. The A0 acceptance gate parametrizes its
# reachability test over these; a reason may carry a `:<detail>` suffix, so the
# gate matches on the prefix.
#
# `footer_gate` is the integer/narrow/unsigned admission gate (A1 closed the
# integer widths). It stays reachable here via a SCHEMA-EVOLUTION dataset (a
# projected column absent from one of the files) — the native scan does not
# support schema evolution, so native_scan_supported returns False and the scan
# stays on the trampoline, byte-for-byte the same R7b guard the integer case hit.
# ---------------------------------------------------------------------------
_FLAT = "testdata/flat/formats/parquet"
_TEN = "testdata/flat/ten_files"
_EVOLVING = "testdata/flat/different"

HAND_SET: Dict[str, str] = {
    # (R2 `pushed_limit` is RETIRED — no longer reachable, so it has no hand-set
    # entry. A scan-pushed LIMIT used to fail closed because LIMIT semantics lived
    # in the trampoline's `_records_to_read` slice. NativeParquetScanSource now
    # carries `row_limit`: it claims each morsel's share under the Source's global
    # mutex, truncates the morsel that crosses the boundary, and caps the submit
    # frontier from the footer's per-row-group row counts so row groups that cannot
    # contribute are never decoded (LIMIT 5 over tpch_1.lineitem: 96 row groups →
    # 1). The scan MUST enforce this itself — LimitPushdownStrategy removes the
    # Limit node from the plan when it pushes. See test_pushed_limit_now_native.)
    # (R3 `fused_topn` is RETIRED — no longer reachable, so it has no hand-set entry.
    # A3 had already admitted the NO-predicate scan-fused `ORDER BY ... LIMIT`; the
    # composed shape (fused TopN WITH a pushed predicate — ClickBench Q24, `SELECT *
    # ... WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`) stayed on the
    # trampoline only because admitting it as a plain single-pass scan lost the
    # two-pass late-materialization decode-skip and measured ~400% slower on Q24.
    # That decode-skip is now NATIVE: src/cpp/engine/native_latmat_scan_source.hpp's
    # LatmatScanSource runs pass 1 (predicate columns + sort key) -> a top-n boundary
    # reduction over draken's own sort comparator -> masked pass-2 decode of the
    # remaining projected columns, entirely in C++. The reduction is the one new
    # piece; everything else (masked decode, the pushed pass-1 predicate on rugo's
    # decode workers, gather) already existed and was only ever driven from Python.
    # Shapes where the trampoline would NOT have late-materialized (no pass-2-only
    # columns, or the selectivity estimate says the predicate does not prune enough)
    # fall through to the ordinary single-pass native scan — the same work the
    # trampoline would have done. See test_wp_r3_latmat_scan.py,
    # test_fused_topn_with_predicate_now_native, and docs/NATIVE_RESIDUAL_PLAN.md
    # item 6. Same retirement convention as R2 / R5 / R5b / R6.)
    # (R4 `unlowerable_predicate` is RETIRED — no longer reachable, so it has no
    # hand-set entry. Its canonical trigger was `WHERE text RLIKE 'a'`: a pushed
    # predicate whose bytecode was not all-c-native, so `bytecode_is_all_c_native`
    # declined and the whole scan fell back. The native regex work closed it
    # INCIDENTALLY — nobody wrote an R4 close-out chip — which is why the marker
    # outlived the category. Confirmed closed by (a) the battery census: 165/165
    # scans native, zero residual reasons of any kind, and (b) a 43-shape hand
    # sweep over the regex family (RLIKE / NOT RLIKE / SIMILAR TO / ~ / !~,
    # composed with AND / OR / NOT), string transforms, hashing/encoding, SPLIT,
    # SOUNDEX, LEVENSHTEIN, ARRAY_CONTAINS, CASE, COALESCE/NULLIF, casts and
    # arithmetic — every one either goes native or raises. None tags R4.
    #
    # ⚠ The `return None` guard in compiler.py STAYS, defensively: it is the
    # structural fail-closed for any future non-lowerable predicate, exactly like
    # R6's. What is retired is the claim that SQL can still reach it. R4 was the
    # last entry in the test's `_OPEN_CATEGORIES` frontier, but NOT the last
    # trampoline trigger overall — `footer_gate` (schema evolution) below still is.
    # Note also
    # that a non-lowerable predicate which never PUSHES is a different class — it
    # becomes a standalone Filter and hard-errors in `_lower_expression` ("outside
    # the c-native kernel set ... no fallback engine"), which R4 never tagged. See
    # test_regex_predicate_now_native, and docs/NATIVE_RESIDUAL_PLAN.md item 7.)
    # (R5 `bool_predicate_input` is RETIRED — no longer reachable, so it has no
    # hand-set entry. A BOOL column used as a predicate input used to fail closed
    # because draken_compare_dv's type switch had no DRAKEN_BOOL branch: every bool
    # comparison declined to nullptr, which on the relocated ExprFilter (no fallback)
    # raised err_op=11. BOOL is BIT-PACKED, so no fixed-width kernel can read it —
    # draken/ops/bool_compare.h supplies its own, reading bit `selection[i]` of the
    # bitmap per logical row (uniform §11 access, no shape discriminant), ordering
    # FALSE < TRUE, result NULL if either operand row is NULL. See
    # test_bool_predicate_input_now_native. Same convention as R2 / R5b above.)
    # (R6 `non_admissible_kind` is RETIRED — it has no reachable SQL trigger left,
    # so it has no hand-set entry. ARRAY was the only kind the reason code was ever
    # observed with, and ARRAY is now decoded natively: a parquet LIST column always
    # lands DK_POOL (repetition levels ⇒ no direct kind) and serializes as TAG_ARRAY,
    # which src/cpp/engine/native_array_pool_decode.hpp now parses in C++ — a
    # faithful port of the trampoline's Cython `_build_array_vector*`, nested
    # list<list<...>> and the ARRAY<TIMESTAMP> child retag included. See
    # test_array_column_now_native. The two OTHER nested kinds stay fail-closed but
    # do NOT reach this guard: STRUCT binds as json/VARCHAR and MAP is refused by
    # the footer gate (`footer_gate`), both verified against real files. What is
    # left behind this guard — VARIANT, INTERVAL, VECTOR_FP16, a DECIMAL/temporal
    # column with no usable logical descriptor — has no parquet-scan trigger in the
    # test corpus, making it a defensive check like `no_manifest`/R7a. Same
    # retirement convention as R2 / R5 / R5b.)
    # R7b — the footer gate; still reachable via schema evolution (missing column).
    "footer_gate": "SELECT followers FROM '%s'" % _EVOLVING,
    # (R5b (A1) `unsigned_predicate_input` is RETIRED — no longer reachable, so it has
    # no hand-set entry. An unsigned predicate input used to fail closed because the
    # relocated ExprFilter's compare requires both operands to share a DrakenType and
    # the literal was always INT64. The schema now declares the column's real width so
    # the literal is coerced to match, and draken_compare_dv dispatches the unsigned
    # compare kernels. See test_unsigned_predicate_input_now_native.)
}


#: Scan Source classes that run with NO Python on the execution path.
#: ``LatmatScanSource`` (R3) is the two-pass late-materialization scan — a different
#: Source class from the single-pass one, but equally native: it never constructs a
#: PyObject and never calls back into Python while executing. The trampoline is
#: ``StreamingScanSource``, and only that.
_NATIVE_SOURCES = frozenset({"NativeParquetScanSource", "LatmatScanSource"})


def census(verbose: bool = False) -> Dict[str, int]:
    """Tally ``scan_residual_reasons`` over the ``.run_tests`` battery.

    Returns ``{reason_code: count}`` plus the aggregate keys ``__native__`` and
    ``__trampoline__`` (scan counts) and ``__raised__`` (queries that raised).
    """
    tally: Dict[str, int] = {}
    native = trampoline = raised = queries = scans = 0
    for sql in _read_battery():
        queries += 1
        sources, reasons, err = scan_residuals(sql)
        if err is not None:
            raised += 1
            if verbose:
                print("  RAISED: %s\n    %s" % (type(err).__name__, sql))
            continue
        for identity, source in sources.items():
            scans += 1
            if source in _NATIVE_SOURCES:
                native += 1
            else:
                trampoline += 1
                reason = reasons.get(identity, "unknown")
                tally[reason] = tally.get(reason, 0) + 1
    tally["__queries__"] = queries
    tally["__scans__"] = scans
    tally["__native__"] = native
    tally["__trampoline__"] = trampoline
    tally["__raised__"] = raised
    return tally


def main(argv: Optional[List[str]] = None) -> int:
    verbose = bool(argv) and ("-v" in argv or "--verbose" in argv)
    tally = census(verbose=verbose)
    print("== native SELECT-path residual census (clickbench + tpch battery) ==")
    print("  queries        : %d" % tally.pop("__queries__"))
    print("  parquet scans  : %d" % tally.pop("__scans__"))
    print("  native         : %d" % tally.pop("__native__"))
    print("  trampoline     : %d" % tally.pop("__trampoline__"))
    print("  raised         : %d" % tally.pop("__raised__"))
    if tally:
        print("  residual reasons:")
        for reason in sorted(tally, key=lambda r: (-tally[r], r)):
            print("    %-28s %d" % (reason, tally[reason]))
    else:
        print("  residual reasons: none — every scan went native")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
