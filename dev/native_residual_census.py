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
# reachability test over these; `non_admissible_kind` carries a `:<DrakenType>`
# suffix (ARRAY → :NONE) so the gate matches on the prefix.
#
# `footer_gate` is the integer/narrow/unsigned admission gate (A1 closed the
# integer widths). It stays reachable here via a SCHEMA-EVOLUTION dataset (a
# projected column absent from one of the files) — the native scan does not
# support schema evolution, so native_scan_supported returns False and the scan
# stays on the trampoline, byte-for-byte the same R7b guard the integer case hit.
# ---------------------------------------------------------------------------
_FLAT = "testdata/flat/formats/parquet"
_TEN = "testdata/flat/ten_files"
_ARRAY = "testdata/flat/struct_array"
_EVOLVING = "testdata/flat/different"

HAND_SET: Dict[str, str] = {
    # R2 — a scan-pushed LIMIT (no ORDER BY, so not a fused TopN).
    "pushed_limit": "SELECT followers FROM '%s' LIMIT 5" % _FLAT,
    # R3 (fused_topn) — PARTIALLY closed (A3). The NO-predicate scan-fused
    # ORDER BY ... LIMIT is admitted natively (measured no-regression). WITH a
    # predicate it still fails closed — measured ~400% regression on ClickBench
    # Q24 (`SELECT * ... WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`)
    # from losing the trampoline's two-pass late-mat decode-skip. See
    # tests/unit/operators/test_wp_a3_fused_topn_scan.py and
    # docs/NATIVE_RESIDUAL_PLAN.md item 6.
    "fused_topn":
        "SELECT * FROM testdata.clickbench_tiny WHERE URL LIKE '%google%' "
        "ORDER BY EventTime LIMIT 10",
    # R4 — a pushed predicate that does not lower to a c-native span (regex).
    "unlowerable_predicate": "SELECT followers FROM '%s' WHERE text RLIKE 'a'" % _FLAT,
    # R5 — a BOOL column used as a predicate input (WP-11 fail-closed).
    "bool_predicate_input": "SELECT userid FROM '%s' WHERE user_verified = TRUE" % _TEN,
    # R6 — a read-set column of a not-yet-admissible kind (ARRAY → :NONE).
    "non_admissible_kind": "SELECT data FROM '%s'" % _ARRAY,
    # R7b — the footer gate; still reachable via schema evolution (missing column).
    "footer_gate": "SELECT followers FROM '%s'" % _EVOLVING,
    # R5b (A1) — an UNSIGNED integer column used as a c-native predicate input. The
    # decode preserves the exact DK_UINT width, which the relocated ExprFilter's
    # bytecode VM cannot read (err_op=11), so the scan fails closed to the trampoline.
    # EventDate is parquet int32 / logical uint16.
    "unsigned_predicate_input":
        "SELECT EventDate FROM testdata.clickbench_tiny WHERE EventDate > 0",
}


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
            if source == "NativeParquetScanSource":
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
