# E.32 — DECIMAL(p ≤ 18) arithmetic kernels (`draken/ops/decimal_arith.h`)

> **Status:** TODO.
>
> **Goal:** add scale-aware arithmetic kernels for the existing
> int64-backed `DRAKEN_DECIMAL` type. Add / sub / mul / div / mod / neg.
> Wire into the OpsTable dispatch so `a + b` on decimal columns runs in
> native C++ rather than going through int64 paths (which mis-handle
> scale alignment) or Python `decimal.Decimal` (slow path).
>
> **Why now:** decimals are storable, hashable, ingestible — they just
> can't be added together natively. Every analytical query with a money
> column hits this. Smallest concrete piece of "decimal pt2" with the
> highest hit rate.
>
> **Scope:** kernels for the **existing 64-bit storage only**. Precision
> ≤ 18. 128-bit storage is a separate ticket if/when needed.

---

## 1. What's being delivered

1. **`draken/ops/decimal_arith.h`** — new file, mirrors
   `draken/ops/int64_arithmetic.h`'s shape:
   - `decimal_add(a, b, scale_a, scale_b)` → result with descriptor
   - `decimal_sub(a, b, scale_a, scale_b)`
   - `decimal_mul(a, b, scale_a, scale_b)`
   - `decimal_div(a, b, scale_a, scale_b)`
   - `decimal_mod(a, b, scale_a, scale_b)`
   - `decimal_neg(a)` (no scale interaction)
2. **Scale-aware semantics** per §3 below.
3. **Overflow detection** using `__int128` for intermediates, with
   explicit failure (loud, not silent-wrap) when the int64 result
   doesn't fit.
4. **OpsTable wiring** so `DRAKEN_DECIMAL` arithmetic dispatches to
   these kernels, **de-aliasing from int64**. (Today, `hash.h:357` has
   `entries[DRAKEN_DECIMAL] = entries[DRAKEN_INT64]` — that aliasing
   stays for **hash** but must NOT extend to arithmetic.)
5. **Native tests** under `draken/tests/native/test_decimal_arith.py`
   (or extend `test_decimal.py` if cleaner) covering each op against
   the cases in §4.

## 2. Architect calls needed BEFORE implementation

The agent must surface these and get answers before writing kernel
code. Picking them inline is the E.24 anti-pattern — design questions
get architect closure, not agent guessing.

### 2.1 Result scale rules

SQL standards vary. The two common conventions:

- **PostgreSQL / "expand-precision" rule:** add/sub result scale =
  max(s1, s2); mul result scale = s1 + s2; div result scale =
  max(s1+s2+6, 6) or similar precision-padding rule.
- **DuckDB / "fit-the-output" rule:** result type computed to avoid
  overflow within int64 storage; promotes to wider type if needed.

Recommendation pending architect: **PostgreSQL rules for v1.** Simpler,
matches the SQL standard, and our int64 storage means precision-ceiling
overflow is already a known failure mode regardless. Decide.

### 2.2 Division-by-zero semantics

- **SQL standard:** raise (`ERROR: division by zero`).
- **`int64_arithmetic.h` precedent:** `div/mod by zero: result = 0`.
  Silent — bug-compatible with `draken_old`.
- **Postgres/DuckDB:** raise.

Recommendation pending architect: **raise**, breaking from the existing
int64 silent-zero behaviour. The silent-zero is a bug we inherited; not
extending it to decimal. (May then also be the right time to fix the
int64 behaviour — but that's a separate ticket.) Decide.

### 2.3 Overflow on multiplication

Multiplying two `DECIMAL(18, s)` values gives an intermediate up to
~10^36 — fits in `__int128`, overflows int64. After scale-down by
`10^s_combined`, the int64 result may or may not fit.

- **Option A:** detect overflow via int128 path, raise on int64
  underflow at result.
- **Option B:** detect overflow, silently wrap (matches int64's
  current behaviour).
- **Option C:** detect overflow, return null with a counter.

Recommendation pending architect: **(A) raise.** Silent wrap on
financial data is unconscionable. Decide.

### 2.4 Result scale storage

The kernel needs to know input scales and produce an output descriptor
(precision, scale). Where do scales come from at call time?

- The logical-type descriptor on the `DrakenVector` (per draken's
  parameterized-type design).
- Passed explicitly as kernel arguments.

Recommendation pending architect: **logical-type descriptor**, with
the kernel signature taking `(const DrakenVector& a, const
DrakenVector& b)` and pulling scales from the descriptors. Matches the
existing pattern for parameterized types (timestamp unit/offset).
Decide.

## 3. Semantic rules (assuming §2 recommendations approved)

| Op | Result scale | Result precision (logical) | Storage check |
|---|---|---|---|
| `a + b` | `max(s_a, s_b)` | `max(p_a − s_a, p_b − s_b) + max(s_a, s_b) + 1` | int128 intermediate, raise if int64 result overflows |
| `a − b` | `max(s_a, s_b)` | `max(p_a − s_a, p_b − s_b) + max(s_a, s_b) + 1` | int128 intermediate, raise on int64 overflow |
| `a × b` | `s_a + s_b` | `p_a + p_b` | int128 multiply, scale-down with rounding, raise on int64 overflow |
| `a ÷ b` | `max(s_a + 6, 6)` (TBD per §2.1) | TBD | int128 numerator scaled up by 10^extra, raise on div-by-zero (§2.2) |
| `a mod b` | `s_a` (TBD) | `p_a` | int128 intermediate, raise on div-by-zero |
| `-a` | `s_a` | `p_a` | raise if `a == INT64_MIN` |

Mixed-scale alignment for add/sub: lower-scale operand multiplied by
`10^(scale_diff)` before the operation. Pre-computed scale_factor
lookup (`pow10_table[19]` of int64) — values fit comfortably for
`s ≤ 18`.

Null propagation: standard — `result_valid[i] = a_valid[i] AND b_valid[i]`
(unary: `result_valid[i] = a_valid[i]`). Matches int64_arithmetic.h
exactly.

## 4. Test matrix (native)

For each op, exercise:

1. **Same scale, same precision** — base case.
2. **Different scales** — alignment correctness.
3. **Mixed precision** — `DECIMAL(8, 2) + DECIMAL(12, 4)`.
4. **Constant-shape × dense-shape** — kernel reads through `selection`
   uniformly per §11.
5. **Dense × dense, dict × dense** — encoding shape transparency.
6. **Null propagation** — at least one null on each side.
7. **All-null inputs** — degenerate case.
8. **Overflow case** — chosen to trigger the int128→int64 fail path;
   assert raise (or NULL, per §2.3).
9. **Division by zero** — assert raise (or NULL, per §2.2).
10. **`neg(INT64_MIN)`** — overflow corner.

Reuse the existing `test_decimal.py` patterns for ingestion / readback;
this ticket adds the arithmetic-side coverage.

## 5. What is explicitly NOT in scope

- **128-bit DECIMAL storage** for precision > 18. Separate ticket.
- **Decimal comparison kernels** (`<`, `=`, `>`, etc.). Comparison
  needs scale alignment too — same shape as arithmetic but a separate
  file (`decimal_compare.h`) and separate ticket. Without it, `WHERE
  price > 10.00` still fails or routes through Python. Acknowledged
  follow-up.
- **Decimal aggregation** (SUM, AVG, MIN, MAX on decimal columns).
  Aggregations are a layer above element-wise kernels. Out of scope.
- **Decimal × non-decimal arithmetic** (`decimal_col + 5`). Requires
  type-promotion rules. Surface if you hit it; don't implement in this
  ticket — that's a promotion-layer ticket, not a kernel ticket.
- **Rounding modes other than the default for division.** SQL has
  several rounding modes (half-up, half-even, etc.). v1 picks one
  (recommend half-even per `draken-boost-math` memory's
  2^52-trick precedent), documents it, doesn't expose configuration.
- **`draken/ops/*` for any other type.** Don't refactor
  `int64_arithmetic.h`'s silent-zero div-by-zero — that's its own
  ticket if approved.
- **Python-level wrapping changes.** If a nanobind extension or
  evaluator dispatch needs updating to route decimal arithmetic through
  the new kernels, that's downstream consumer-side work, not this
  ticket. Surface the dispatch hook; let the consumer fix the call.

## 6. STOP conditions

- File count > 4: `decimal_arith.h`, test file, possibly one OpsTable
  registration line, possibly one `setup.py` line if a new compile
  target is needed. Past 4 → drifting.
- You start implementing before §2 architect calls are closed. **STOP
  and surface** with the questions.
- You catch yourself extending `int64_arithmetic.h` or modifying any
  existing kernel file. Decimal arithmetic is its **own** file. Don't
  bolt onto int64 — that's the alias-from-int64 violation already
  present for hash; we're not extending it.
- You catch yourself adding decimal handling to `draken_native.cpp`'s
  type-check branches at L3287/3354/3464/3510/3557. Those exist for
  ingestion/readback; the arithmetic path goes through the OpsTable,
  not through ad-hoc branches in the nanobind layer.
- `make dt` regresses below the post-this-ticket expected count (≥2801
  before, plus however many tests you add).
- You introduce `cdef object` or `object` parameters anywhere. §3
  violation.
- You add `DRAKEN_DECIMAL128` or any new enum value to support this
  work. **128-bit is a separate ticket.** This is int64-storage only.

## 7. Acceptance

Run and report verbatim:

1. `ls draken/ops/decimal_arith.h` — file exists.
2. `make draken 2>&1 | tail -5` — build succeeds.
3. `make dt 2>&1 | tail -3` — all tests pass (≥2801 + new tests).
4. Each of the §4 test categories shows a passing test for each of
   add/sub/mul/div/mod/neg.
5. `grep -c "decimal_arith" draken/ops/ops_table.cpp` (or wherever the
   OpsTable lives) — non-zero, showing the dispatch is wired.
6. `grep "DRAKEN_DECIMAL = DRAKEN_INT64\|entries\[DRAKEN_DECIMAL\] = entries\[DRAKEN_INT64\]" draken/ops/hash.h` — still present (hash alias stays; this ticket de-aliases arithmetic only).
7. `git diff --stat HEAD` — ≤4 files changed (plus any new test files
   created).

## 8. Reporting back

- §2 architect-call answers as received.
- §7 acceptance outputs.
- A short note on whether the overflow-detection path (int128
  intermediate, int64-check at the end) measurably affected throughput
  versus the int64 path. Pure benchmarking, no decisions to make —
  just record.
- Any surface that came close to triggering a STOP condition, with
  a one-line note on why you didn't stop.
- Surfaced gaps: if anything in §5's "explicitly NOT in scope" turned
  out to be a hard blocker on completing the in-scope work, surface as
  a follow-up ticket recommendation.

## 9. After this lands

Decimal arithmetic works natively at the kernel layer. The next steps
(separate tickets, scoped if/when needed):

- **Decimal comparison kernels** (`decimal_compare.h`) — same shape,
  smaller scope. Probably the next ticket after this.
- **Decimal aggregation** — SUM/AVG/MIN/MAX, builds on element
  kernels.
- **Decimal × non-decimal type promotion** — needs a promotion layer.
- **128-bit storage** for `DECIMAL(p ≤ 38)` — separate ticket, separate
  type tag, separate kernel suffix.

None of these block each other. Each is independently scoped if and
when the engine actually needs it.
