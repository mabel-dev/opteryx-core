# P9.1 — Unify Binary Ops onto the Registry kernel_fn ABI (complete, null-correct, atomic flip)

Status: **design, awaiting architect sign-off. No code cut.**
Date: 2026-06-16
Parent: `docs/M4_P9_EXECUTOR_FLIP_DESIGN.md` (P9). Decision locked 2026-06-16:
"Unify binops onto the registry kernel_fn ABI; flip only when complete + null-correct,
as a wholesale replacement — no beside-fallback." This honours the standing
2026-05-28 ruling (`docs/tickets/correctness-restore-binop-single-path.md`): one
path, complete or fails loud, no fallback.

---

## 1. The complete binop surface (the bar = "all cases")

From `resolve_binary_op` (`opteryx/expression/evaluator/arithmetic.pyx:213`), the
live closure path covers ALL of:

| Family | Ops | Type combos | Current compute |
|---|---|---|---|
| Arithmetic | + − × ÷ % DIV | int8/16/32/64, float32/64, narrow→wide & int→float promotion, DECIMAL(int64), DECIMAL128, decimal×float→float | `_build_arithmetic_closure` → nanobind `vector_math` + decimal kernels; **executor fast-path = `draken_arithmetic_dv` (int64/float64 only)** |
| String concat | `\|\|` | VARCHAR/NVARCHAR/VARBINARY | `_build_string_concat_closure` (nanobind) |
| Bitwise | OR AND XOR SHL SHR | integer types | `_build_bitwise_closure` (nanobind) |
| IP-in-CIDR | `\|` on VARCHAR | VARCHAR | `vector_ip_in_cidr` (nanobind) |
| Date/Timestamp ± Interval | + − | DATE/TIMESTAMP ± INTERVAL | `_date_interval_op_draken` |
| Date − Date | − | DATE−DATE, TIMESTAMP−TIMESTAMP | `_date_minus_date_draken` |
| Interval ± Interval | + − | INTERVAL±INTERVAL | `INTERVAL_KERNELS` |

"Complete" (the flip gate) = every cell above produces a byte-identical,
null-correct result via a registered C `kernel_fn`, for all three vector shapes
(dense/constant/dict) via the uniform `data[selection[i]]` path. Failing loud on
a case that works today is a **regression**, not acceptable — the bar is all cases.

## 2. Gap map — existing draken kernels vs what must be wired

Most compute already exists as typed draken C++ kernels; P9.1 is mostly *wiring +
null/shape coverage + dispatch*, not green-field compute:

| Need | Existing draken kernel | Gap |
|---|---|---|
| int64 arith | `int64_arithmetic.h` (`i64_add`…) | wired in `draken_arithmetic_dv`; needs null-merge audit |
| narrow-int arith | `fixed_int_ops.h` | NOT wired into the binop entry |
| float32/64 arith | `float_ops.h` (`float_add<…>`) | float64 wired; float32 + int/float promotion to confirm |
| DECIMAL / DECIMAL128 arith | `decimal_arith.h` | NOT wired into the binop entry (scale/precision correctness-critical) |
| bitwise | `int_bitwise.h` | NOT wired (registry stubs removed in P9.0) |
| temporal ± interval, date−date | `temporal_arith.h` | NOT wired |
| interval ± interval | `interval_ops.h` | NOT wired |
| string concat | string ops (`draken_native` concat) | needs a binop-shaped C entry |
| IP-in-CIDR | nanobind `vector_ip_in_cidr` only | **no draken kernel** — must extract compute into draken (C′ pattern) |

Null-correctness: the registry `draken_add` already does per-row validity-bitmap
merge (`binary_op_arithmetic.cpp` — the 9c gap is closed there), and handles the
DRAKEN_NULL all-null short-circuit. The complete entry must apply that pattern
uniformly across every family (the 9c SIGBUS was missing per-row null merge).

## 3. Strategy — build-behind, flip-once (forced by the no-fallback ruling)

The ruling forbids a partial C path running beside the closure. The only way to
build something this large while keeping the suite green is to keep the closure as
THE single live path until the kernels are complete, then flip atomically:

- **Develop the complete binop C entry + its kernels NOT wired into the executor.**
  The live path stays `draken_arithmetic_dv` + closure throughout development —
  unchanged, single, correct. The new kernels are exercised only by their own
  tests. This is not a beside-fallback: nothing in the live query path calls them
  until the flip.
- **Gate each family by its own value-checked + null + shape tests** (the
  `make kernel-parity` / `make dt` model: correct answers, null propagation, and
  dense/constant/dict inputs), built up family by family.
- **Flip once, atomically, only when all families pass:** one change that (a) routes
  every BC_BINARY_OP through `kernel_fn` in the executor, (b) deletes the
  `draken_arithmetic_dv` fast-path branch, (c) stops the binder setting the
  `resolve_binary_op` closure for binops. Removes both old mechanisms in the same
  commit. Gate: `make q` 182 / tpch 22 / clickbench 43 identical + the binop
  differential test (every covered combo == prior closure output, incl. nulls).
- **Binder:** add a per-(op, left_type, right_type) resolver — the binop analogue of
  `_c_native_cast` — that returns the kernel name + ctx for a covered combo. Until
  the flip, this resolver is *defined but not consulted by the executor*; at the
  flip it becomes the single dispatch. (Unlike casts, there is no permanent closure
  fallback — post-flip, an uncovered combo is a bind-time loud failure, and "all
  cases" means there are none for supported SQL.)

## 4. Sub-stages (build order; live path unchanged until P9.1-FLIP)

Each Pn builds + tests kernels only; NONE wire the executor:
- **P9.1a — Arithmetic core:** one C entry `draken_binop_arith(ctx{op}, l, r)` over
  int8/16/32/64 + float32/64 with promotion + per-row null merge + 3 shapes, wiring
  `int64_arithmetic.h`/`fixed_int_ops.h`/`float_ops.h`. Kernel-parity tests.
- **P9.1b — Decimal:** DECIMAL(int64) + DECIMAL128 + decimal×float promotion via
  `decimal_arith.h` (scale/precision exactness vs DuckDB — highest correctness risk).
- **P9.1c — Bitwise** (`int_bitwise.h`) + **string concat** (binop-shaped entry).
- **P9.1d — Temporal:** date/ts ± interval, date−date (`temporal_arith.h`),
  interval±interval (`interval_ops.h`).
- **P9.1e — IP-in-CIDR:** extract compute from nanobind `vector_ip_in_cidr` into a
  draken kernel (C′ pattern — compute in draken, nanobind becomes thin shim).
- **P9.1-FLIP — atomic executor + binder flip** (see §3), only when a–e all pass.
  Removes `draken_arithmetic_dv` and the binop closure in one commit.
- **P9.1-CLEANUP:** delete the now-dead `resolve_binary_op` closure path,
  `_build_arithmetic_closure`/`_build_bitwise_closure`/`_build_string_concat_closure`,
  and `arithmetic_dv.{h,cpp,pxd}` (§11 anti-duplication).

## 4b. P9.1a findings — result-type model corrected (2026-06-16)

Empirical probe (engine output types) falsified the "widen narrow→int64" assumption
and surfaced closure bugs. Authoritative result-type rules live in
`opteryx/planner/binder/operator_map.py` + `reference/operators.json` — the kernel
must MATCH the binder's declared result type, derived from there (not reverse-engineered).

- **Narrow-int arithmetic is WIDTH-PRESERVING.** `$planets.id` is INT16, and
  `id + id`, `id - id`, `id DIV id` all return **INT16** (not INT64). But
  `fixed_int_ops.h` explicitly has **no narrow-int arithmetic kernels**. So P9.1a is
  NOT merely "wire existing kernels" for narrow widths — `draken_binop` must produce
  the narrow result type, either via new narrow-int arithmetic compute (with the
  repo's existing overflow/wrap semantics) or widen→compute→narrow. The exact
  overflow semantics must be matched and tested.
- **The closure has pre-existing bugs** the byte-identical gate will expose:
  `id / id` (INT16 true-divide) currently ERRORS (`to_float64: expected DECIMAL,
  DECIMAL128, INT64, …` — the float-promotion path doesn't accept INT16). A correct
  `draken_binop` would return FLOAT64 here — i.e. the kernel would be MORE correct
  than the closure, so a literal "== closure" differential would flag it as a diff.
- **DECIMAL is pervasive**: `$planets.gravity` is DECIMAL128, so even simple test
  data exercises the P9.1b decimal path early.

Implication: P9.1a must implement (not just wire) narrow-int arithmetic to the
correct width, and the differential gate is fix-forward (kernel correct vs DuckDB;
closure bugs adjudicated, not auto-fail) — LOCKED 2026-06-16.

**Result-type is resolved at BIND time in two steps** (`operator_map.py` static
category map → `determine_type()` width refinement). Rather than re-implement
`determine_type` in C (divergence risk), the clean design is: **the binder passes the
already-resolved result `DrakenType` to the kernel via `ctx`** (extend `binary_op_ctx`
to `{op_code, result_type}`; the binder fills it — it already knows the type). The
kernel then produces exactly that width. For P9.1a (unwired), kernel-parity tests pass
the expected result type directly; the binder ctx-fill lands at flip time.

**Narrow-int overflow semantics: match DuckDB (error on overflow)** per the locked
fix-forward decision — native narrow kernels detect overflow and return an error
sentinel; differential-verified (if current Opteryx wraps, that's the adjudicated
closure bug).

## 5. Decisions to surface

- **D1 — Strategy confirm.** §3 build-behind + flip-once is, I believe, the only way
  to honour "no beside-fallback" while staying green. Confirm, or prefer a different
  shape (e.g. a worktree/branch for the whole effort)?
- **D2 — Completeness bar.** Per the ruling I am treating "all cases" literally:
  every combo the closure handles today (incl. DECIMAL128, interval×interval,
  IP-in-CIDR) must be C-native before the flip — fail-loud on a today-working case
  is a regression. Confirm that bar (it sets P9.1's size), vs. an agreed narrower
  scope where some rare family stays on the closure *as the sole path for that op*
  (a per-family split of mechanisms, not a per-row fallback) until a later stage.
- **D3 — Single entry vs per-family kernels.** One `draken_binop(ctx{op,...}, l, r)`
  dispatching internally, or one registered kernel per family (`draken_binop_arith`,
  `draken_binop_bitwise`, …) selected by the binder. Leaning per-family (smaller,
  testable units; binder already knows the family from types) — confirm.
- **D4 — Decimal/temporal correctness gate.** These are historically bug-prone
  (decimal scale, temporal units). Propose gating P9.1b/d behind a value-checked
  differential vs the current closure on a broad generated corpus before they count
  as "covered." Confirm acceptable.
