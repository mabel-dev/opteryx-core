# String Kernel Results Borrowing the Input Arena — Design

Status: **Stage 1 and Stages 2/3 are BUILT and LANDED in main (uncommitted).**

- **Stage 1:** landed 2026-10-26.
- **Stages 2/3:** landed 2026-10-27.
- **Tests:** `make q`, `make slt`, `make dt`, `make st` and `make rt` pass.
- **x86:** not yet measured.

Architect decisions are in §9.

## 1. Problem

A string kernel whose output is a substring of its input still copies the bytes. Kernels that do this include:

- the REGEXP_REPLACE extract (`_DFA_EXTRACT`): capture → substring; no match → the whole value;
- `SUBSTRING`, `LEFT`/`RIGHT`, `TRIM`/`LTRIM`/`RTRIM`.

These kernels copy every long value (more than 12 bytes) into a new arena. The result cannot point at the input's bytes because nothing would keep those bytes alive.

The expression VM then copies the result a **second** time when it leaves the span.

Measured on the ClickBench REGEXP_REPLACE query (`/* 28 */`, Mac, 2026-10-26, before the compression-aware extract change):

- kernel output build/copy: 9.3% of busy CPU;
- memcpy under the expression-eval trampoline: 2.5% + 1.7% self.

The compression-aware extract kernel (landed the same day) already removed the per-row copy for dictionary input, which is 90% of that query's rows. Rows that arrive one value per row (dense) still copy every passthrough value. Every string result, of any shape, still pays the span-exit copy.

## 2. Where the bytes are copied today

| # | Where | What is copied |
|---|---|---|
| C1 | The kernel itself, e.g. `_dfa_extract_kernel` in `opteryx/compiled/vector_ops/vector_dfa_extract.pyx` | Every long output value goes into a new arena. Passthrough values are copied whole. |
| C2 | VM span exit: `_dv_copy_result_string` / `_dv_copy_result_string_preserve` in `opteryx/expression/evaluator/evaluation.pyx`, called from `_dv_eval_span_cxx` | The slots and every live long value are copied a second time into caller-owned buffers. This happens even though the kernel's buffers are already individually `draken_malloc`'d and tracked by the frame arena. |

C2 applies to every computed string column on the engine path (`ExprMultiProjectOperator`, `src/cpp/engine/native_expression.hpp`), whichever kernel produced it.

## 3. Mechanisms that already exist

- **`VectorOwner::data_source`** (`draken/core/vector_owner.h`): a `shared_ptr<VectorOwner>` that keeps a **whole borrowed payload** alive.
  - It is used by the join's build-side dict emit and by lazy filter (`cxx_morsel_ops.cpp`), and it never chains.
  - It is mutually exclusive with `data_buf`, and `draken_vector_owner_nbytes` already skips borrowed bytes.
  - It covers "this vector's data AND arena belong to someone else". It does not cover "my slots are mine, my arena bytes are someone else's".
- **The frame arena** (`draken/core/frame_arena.h`): every intermediate buffer is its own `draken_malloc` allocation, tracked by pointer.
  - `draken_frame_arena_release` hands one out without a copy.
  - `draken_frame_arena_contains` tells an independently-owned buffer from an interior pointer.
  - The Python path (`_slot_to_pyobj`) already transfers ownership this way instead of copying.
- **`VecResult::arena`**: a separately owned arena. It is owned only; there is no borrowed variant.

## 4. Proposal: three stages, each measured on its own

### Stage 1: the span exit transfers instead of copying (removes C2; no ABI change)

At `_dv_eval_span_cxx` exit:

1. **If every buffer behind the result is tracked by the frame arena**, release those buffers and hand them out instead of deep-copying. "Every buffer" means:
   - the data block;
   - the separately allocated `sa->arena`, when there is one;
   - the codes, when the selection is owned;
   - the validity, when it is not embedded.
2. **Validity embedded in the data block** (`vecresult_from_string_buffers` / `_block` results): copy only the bitmap out, because `out_validity` must be a separate allocation. That bitmap is n/8 bytes.
3. **Otherwise copy as today.** This covers:
   - a result that *is* a morsel column or a literal (a pass-through `LOAD_COL`);
   - selections that point at the global identity or zero arrays. These are handled by allocating a fresh identity array, as the copy path already does.

Notes:

- The `preserve_shape` variant gets the same treatment: a dictionary result is handed out with its own codes.
- This is a pure ownership hand-off with no new state, and it benefits **every** string-producing expression, not just the extract.
- Both shared string-result constructors already set `owns_buffers = 0` (`draken/ops/kernels/result_helpers.cpp:81` and `:203`), so a released block from them is valid as-is under a `VectorOwner`. Producers that write the header by hand need auditing; see §7.

### Stage 2: kernels may borrow an input's arena (removes C1; VecResult ABI addition)

Append one field to `VecResult` (`draken/ops/vec_result.h`), default-initialised like every earlier appended field so producers that don't know about it stay correct:

```c
uint8_t arena_lender = 0xFF;   // 0xFF = none. Otherwise: index of the args[]
                               // entry whose arena this result's slots point into.
```

Kernel contract when `arena_lender = i`:

1. **Arena pointer:** the result's `DrakenStringArena.arena` is **exactly** `args[i]`'s `sa->arena` pointer, and `VecResult::arena` is NULL. A result never owns some long values and borrows others: one arena per vector is the existing invariant, and it stays.
2. **Slot offsets:** every long output slot's offset is an offset into the lender's arena.
   - A passthrough is `str_clone_with_offset(dst, src, src_offset)`.
   - A long capture is `str_init_extern(dst, cap_ptr, len, src_offset + (cap_ptr - src_ptr))`.
   - Short output values are inline, as today.
3. **Arena bookkeeping:** `arena_used` and `arena_cap` are copied from the lender, so any reader that bounds offsets by `arena_used` stays correct. `owns_buffers` is 0.
4. **When not to borrow:** a kernel must not set the field when the lender has `payloads_elided = 1`, or when the lender's arena is NULL (every value inline). In those cases the output is necessarily all-inline or the kernel errors, as today.
5. **Opt-in:** only kernels that opt in set the field; the default is a copy. The first candidates are `_DFA_EXTRACT`, `draken_substring[_dynamic]`, `draken_left_dynamic`/`draken_right_dynamic` and `draken_trim`/`ltrim`/`rtrim`.

> **Implementation note (2026-10-27, bench tree):** the built version differs from the per-register provenance described below.
>
> - **How the lender is found:** the VM finds it **at span exit** by pointer equality. The result's `sa->arena` is compared against the arena of every column the program loaded (`BC_LOAD_COL` entries of the span's operand cache).
> - **Why that is sufficient:** every operand stays alive until the frame is destroyed, so borrowing inside a span needs no tracking at all.
> - **What still gets copied:** a lender that is not a loaded column, such as a literal or an intermediate whose arena is inside its own block. That case falls back to today's copy, so the answer is always correct; it just isn't zero-copy.
> - **Callers that can't keep the lender alive:** a caller that passes `out_arena_col = NULL` (the join and unnest residual spans) never receives a borrowed result.
> - **Every non-VM consumer compacts first:** `vecresult_to_owner`, the Python bridge `_slot_to_pyobj`, and spill via `cxx_morsel_own_arenas_c`.

### Stage 3: the VM keeps the lender alive (needed by Stage 2)

Inside a span, every lender is alive until the frame is destroyed, so borrowing between registers is safe as is. The work is at the span exit. It needs:

- **Per-register provenance for the arena.** For each `dv_store` slot, record one of:
  1. the arena is in a frame-tracked allocation;
  2. the arena is borrowed from morsel column `c`;
  3. the arena is borrowed from another register `r`.

  Chains collapse to the root lender when recorded, as `data_source` does: a borrow from a register that itself borrows records that register's root.

- **A new owner edge**, `VectorOwner::arena_source` (`shared_ptr<VectorOwner>`). It means the slots and validity are owned here and the arena bytes belong to that owner.
  - `arena_buf` and `arena_source` are mutually exclusive, the same rule as `data_buf`/`data_source`.
  - `data_source` already implies the arena too, so the two are never set together.

- **Resolution at span exit:**
  - Root is a frame allocation: release it and hand it over as the result's owned arena. That is Stage 1.
  - Root is morsel column `c`: the operator sets `own->arena_source = col.own` (or `col.own->data_source` if `c` itself borrows).
  - Root is a literal: copy, as today. Literal owners belong to the plan, not to the output morsel, and their values are constant-shaped, so they go through the dictionary path anyway.

- **`ExprEvalFn` gains one out-parameter**, `int* out_arena_col`: −1 for none, otherwise the morsel column index whose owner the result's arena borrows. `ExprMultiProjectOperator` turns that into `arena_source`.

## 5. Invariants (fail loudly; never degrade silently)

- `arena_buf` and `arena_source` are never both set; `data_source` and `arena_source` are never both set. A `debug_assert` goes in the `VectorOwner` constructors and setters.
- A result with `arena_lender != 0xFF` whose `sa->arena` differs from `args[arena_lender]`'s arena is a kernel bug. The VM rejects it with a span error (a new rc), not by copying.
- `draken_vector_owner_nbytes` counts a borrowed arena as 0 bytes for the borrower, the same rule as `data_source`.
- Shape is unaffected: borrowing changes where the bytes live, never which value a row reads (CLAUDE.md §11).

## 6. Lifetime and memory

- **A borrower pins the lender's whole arena.** For example, a selective filter followed by an extract keeps the full unfiltered arena alive for as long as the result lives. The pin is per morsel and ends when the downstream consumer drops the column.
  - GROUP BY and DISTINCT copy key bytes into their own arena (`GBArena::put`), so group state does not extend the pin.
  - Sinks that retain morsels would: sort, spill, and result collection back to Python.
- **Spill and serialization** (the skene spill writer and anything else that writes `arena[0..arena_used)`) would write the lender's full arena for a borrowed vector. Two options:
  1. compact on spill: walk the slots and copy only the referenced bytes;
  2. treat a borrowed arena as an error at the spill boundary until the spill writer is audited.

  Option 1 is recommended.
- **The Python bridge** (`_slot_to_pyobj`, `draken_vector_own_string`) must either understand `arena_source` or compact when crossing into a Python `Vector`.

## 7. Audit list before Stage 2/3 lands

- About 43 source files read `arena_used` and about 23 read `owns_buffers` (draken, src/cpp, rugo, skene, opteryx; generated files excluded). Each one must be classified as one of:
  - "reads via slots": safe;
  - "frees or serializes the arena": must learn about `arena_source`;
  - "assumes `arena_used` bounds this vector's own bytes": safe if the lender's value is copied, which §4 Stage 2 rule 3 does.
- Every producer of string `VecResult`s that Stage 1 would hand out unchanged: check that its header has `owns_buffers = 0` and that its validity is embedded or separate exactly as `validity_embedded` says.
- `kernel_preserve_shape`: it copies the selection and validity and never touches the arena, so it is compatible.

## 8. Expected gain and measurement plan (not yet measured)

- **Stage 1** removes C2 for every computed string column. On the REGEXP_REPLACE query that was the 2.5% + 1.7% memcpy under the expression VM. It also covers every `SUBSTRING`/`TRIM`/`CONCAT` projection.
- **Stage 2+3** removes C1. After the compression-aware extract change, C1 on that query is only the 10% of rows that arrive dense, so the gain there is small. Dense-input substring kernels gain more.
- **Plan:** A/B each stage separately in a private bench tree, interleaved, with controls, as for the DFA work. Use:
  - the REGEXP_REPLACE query;
  - a dense `SUBSTRING(URL, 1, 40)` projection;
  - a `TRIM` projection;
  - a non-string projection as the control.

  Then repeat on x86. Stage 1 lands only if it measures faster; it does not depend on Stage 2.

## 9. Decisions for the architect

1. ~~Build **Stage 1 alone first**?~~ **DECIDED 2026-10-26: Stage 1 first; Stage 2 is decided after it.**
   - Stage 1 is built and landed in main (`_dv_handover_result_string`, `evaluation.pyx`); `make q` and `make slt` pass.
   - Mac A/B results:
     - `MIN/MAX(LOWER(URL))`: 0.760
     - `ORDER BY … LIMIT` over `LOWER(URL)`: 0.779
     - `GROUP BY TRIM` / `GROUP BY SUBSTRING`: 0.95
     - REGEXP_REPLACE query: 0.964
     - controls: 1.00
   - x86 has not been measured yet.
**DECIDED 2026-10-27 (decisions 2–5):**

- **ABI:** `VecResult::arena_lender` (an index into `args`).
- **Owner edge:** the new `VectorOwner::arena_source`.
- **Spill:** compact on spill.
- **First kernels to opt in:** `_DFA_EXTRACT` plus `SUBSTRING`, `LEFT`/`RIGHT` and `TRIM`/`LTRIM`/`RTRIM`.

The original options are kept below for the record.

2. Stage 2's ABI shape: `VecResult::arena_lender` (an index into `args`), as proposed, or a pointer-valued `arena_lender_ptr` that the VM maps back to a register.
3. The new owner edge `VectorOwner::arena_source`, or widen `data_source` to mean "borrowed bytes", with a flag saying which parts are borrowed.
4. Spill and serialization of a borrowed arena: compact on spill (recommended), or reject until the spill writer is audited.
5. Which kernels opt in first: the extract alone, or the extract plus `SUBSTRING`/`LEFT`/`RIGHT`/`TRIM` together.
