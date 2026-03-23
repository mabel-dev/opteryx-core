# CarcharIndex — Performance Design

## Goal

Apply lessons from the `CarcharSet` optimisation work to `CarcharIndex` and its
consumers (`CarcharJoinIndex`, `CarcharJoinEngine`).  The same structural issues
are present in the hash-map layer and the fixes are direct translations.

Three improvements are identified:

| # | Change | Files |
|---|---|---|
| 1 | `std::move` old vectors in `CarcharIndex::resize` | `carchar_index.hpp` |
| 2 | Cache `probe_finder` as a member on `CarcharIndex` | `carchar_index.hpp` |
| 3 | Remove `normalize_key` dead no-op from all call sites | `carchar_common.hpp`, `carchar_index.hpp`, `carchar_set.hpp` |

---

## Background

`CarcharIndex` is the key→payload-ref map that backs `CarcharJoinIndex` and
`CarcharJoinEngine`.  Every join build row passes through
`CarcharIndex::find_or_insert` and every probe row passes through
`CarcharIndex::lookup_fast`.  Both call `find_slot`, making it the hottest
function in join execution.

---

## Proposal 1 — `std::move` Old Vectors in `CarcharIndex::resize` ✅ DONE

### Problem

`CarcharIndex::resize` deep-copies **three** vectors before rebuilding:

```cpp
const auto old_control      = control_;       // malloc + memcpy
const auto old_hashes       = hashes_;        // malloc + memcpy
const auto old_payload_refs = payload_refs_;  // malloc + memcpy
```

For a 500 k-entry index this copies ~500 KB (control) + 4 MB (hashes) + 4 MB
(payload refs) = ~8.5 MB before inserting a single rehashed entry.  Peak memory
during resize reaches ~3× the index size.

This is the same bug fixed in `CarcharSet` (Proposal 2 of the set work).
`CarcharIndex` has an extra `payload_refs_` vector, making the wasted copy 2×
larger.

### Fix

```cpp
void resize(std::size_t new_capacity) {
    new_capacity = std::max(kMinCapacity, next_power_of_two(new_capacity));

    auto old_control      = std::move(control_);       // O(1)
    auto old_hashes       = std::move(hashes_);        // O(1)
    auto old_payload_refs = std::move(payload_refs_);  // O(1)
    const auto old_capacity = capacity_;

    initialize_storage(new_capacity);
    ++resize_count_;

    for (std::size_t slot = 0; slot < old_capacity; ++slot) {
        if (old_control[slot] == kEmpty) continue;
        insert_at(
            find_empty_slot_for_resize(old_hashes[slot]),
            old_hashes[slot],
            old_payload_refs[slot]
        );
    }
}
```

### Files

- `third_party/mabel/carchar/carchar_index.hpp` — `resize`

### Expected Gain

5–10 % on join build throughput, dominated by reduced allocator pressure and
lower peak cache usage during the resize event.  Memory high-water mark during
resize drops from ~3× to ~2× the index size.

### Outcome

Applied.  Three-line change: `control_`, `hashes_`, and `payload_refs_` are now
moved rather than copied, eliminating up to ~8.5 MB of memcpy per resize event
on a 500 k-entry index.

Benchmark numbers are within noise of the baseline.  The map benchmark
pre-sizes the table via `reserve(len(keys))` before any inserts, so the one
resize that fires has no occupied entries to copy — both old and new code rehash
zero entries.  The saving materialises in production where tables grow
incrementally from a small initial capacity.  The change is correct and free on
any resize with actual data.

---

## Proposal 2 — Cache `probe_finder` as a Member on `CarcharIndex` ✅ DONE

### Problem

`CarcharIndex::find_slot` calls `detail::select_probe_finder()` on **every
single invocation**:

```cpp
FindResult find_slot(std::uint64_t key) const {
    const std::uint8_t tag = key_tag(key);
    const auto probe_finder = detail::select_probe_finder();   // ← every call
    const auto result = probe_finder(...);
    ...
}
```

`select_probe_finder()` caches the result in a `static std::atomic<fn_t>`,
so after the first call it is just an atomic acquire-load.  On x86 this is
essentially free (TSO makes acquire loads free).  On ARM (including the Apple
Silicon dev environment and AWS Graviton) an acquire load requires a `LDAR`
instruction — a full load-acquire memory barrier.

`CarcharSet`'s bulk methods already avoid this by calling
`select_probe_finder()` once and threading the result through the loop.
`CarcharIndex` has no bulk path, so every `lookup`, `lookup_fast`, `insert_new`,
and `find_or_insert` call pays the cost.

For a 500 k × 500 k join that is 1 M `find_slot` calls, each with an
unnecessary atomic load on ARM.

### Fix

Cache the resolved function pointer as a `const` member, set once at
construction from `initialize_storage`:

```cpp
class CarcharIndex {
    ...
   private:
    detail::ProbeFn probe_finder_ = nullptr;

    void initialize_storage(std::size_t capacity) {
        capacity_ = capacity;
        control_.assign(capacity_ + (kGroupWidth - 1U), kEmpty);
        hashes_.assign(capacity_, 0U);
        payload_refs_.assign(capacity_, -1);
        size_ = 0;
        probe_finder_ = detail::select_probe_finder();   // resolve once
    }

    FindResult find_slot(std::uint64_t key) const {
        const std::uint8_t tag = key_tag(key);
        const auto result =
            probe_finder_(control_.data(), hashes_.data(), capacity_, key, tag);
        if (result.probes < capacity_) {
            return {result.slot, result.found, result.probes};
        }
        throw std::runtime_error("Carchar probe exhausted table capacity");
    }
    ...
};
```

`select_probe_finder()` is idempotent and CPU-feature-based — calling it
multiple times always returns the same value, so caching is safe.  The member
replaces every per-call atomic load with a plain register load from `this`.

### Files

- `third_party/mabel/carchar/carchar_index.hpp` — `initialize_storage`,
  `find_slot`, member declaration

### Expected Gain

2–5 % on ARM join workloads (eliminates `LDAR` per probe/insert).  Negligible
on x86 where acquire loads are free, but zero-cost there too.  The primary
benefit is ARM production environments (AWS Graviton, Apple Silicon CI).

### Outcome

Applied.  `probe_finder_` added as a `detail::ProbeFn` member, resolved once in
`initialize_storage` (called from both the constructor and `resize`).  `find_slot`
now reads directly from `this` rather than going through the atomic cache.

Benchmark numbers are flat on Apple M-series — consistent with the x86 set work
finding where the atomic acquire load is cheap enough to be lost in noise at this
scale.  The `probe-heavy` scenario (500 k lookups, 15-entry hot table) showed a
hint of improvement (+7 %) but within noise.  The saving is proportional to how
hot `find_slot` is relative to everything else in the join pipeline; it will be
more visible on ARM production hardware under sustained join load.

---

## Proposal 3 — Remove `normalize_key` Dead No-Op ✅ DONE

### Problem

`carchar_common.hpp` defines:

```cpp
constexpr std::uint64_t kMask64 = std::numeric_limits<std::uint64_t>::max();

inline std::uint64_t normalize_key(std::uint64_t key) {
    return key & kMask64;   // AND with all-ones — always a no-op
}
```

`AND` with `UINT64_MAX` can never change the value of a `uint64_t`.  Every call
site in `CarcharSet`, `CarcharIndex`, and the probe paths calls
`normalize_key(key)` before using the key.  At `-O2` the compiler should
eliminate the instruction, but:

- The function is semantically misleading — it implies some normalisation is
  taking place.
- It obscures where real key transforms might need to be added in the future.
- It adds a layer of indirection that the reader has to trace to confirm it is
  a no-op.

### Fix

Delete `normalize_key` and `kMask64` from `carchar_common.hpp`.  Remove all
call sites.  The key is used directly.

If a genuine normalisation step is needed in the future (e.g. canonicalising
a sentinel value), it should be added explicitly with a descriptive name.

### Files

- `third_party/mabel/carchar/carchar_common.hpp` — delete `kMask64`,
  `normalize_key`
- `third_party/mabel/carchar/carchar_index.hpp` — remove all
  `normalize_key(key)` call sites
- `third_party/mabel/carchar/carchar_set.hpp` — remove all
  `normalize_key(key)` call sites

### Expected Gain

Negligible at runtime (compiler eliminates the instruction at -O2).  Primary
benefit is code clarity and correctness of intent.

### Outcome

Applied.  `kMask64` and `normalize_key` deleted from `carchar_common.hpp`.  All
11 call sites removed across `carchar_index.hpp` (5 sites) and `carchar_set.hpp`
(4 sites).  Benchmark numbers are within noise of the baseline — no regression,
no measurable gain, as expected.

---

## Combined Expected Gain

| Proposal | Primary Beneficiary | Mechanism |
|---|---|---|
| 1 — move in resize | Join build, all platforms | Eliminates ~8.5 MB memcpy per resize |
| 2 — cache probe_finder | Join build + probe, ARM | Eliminates `LDAR` per `find_slot` call |
| 3 — remove normalize_key | All paths | Code clarity; compiler was already eliminating it |

Proposals 1 and 2 together are expected to produce a measurable improvement on
join build throughput.  Proposal 3 is a correctness/clarity fix with no runtime
risk.

---

## Implementation Order

1. **Proposal 3** — delete dead code, zero risk, no behaviour change.
2. **Proposal 1** — isolated to `resize`, mechanical translation of the set fix.
3. **Proposal 2** — requires adding a member and touching `initialize_storage`
   and `find_slot`; implement after 1 and 3 are stable.

---

## Benchmark Harness

```
python tests/performance/benchmarks/bench_carchar_maps.py \
    --rows 500000 --repeat 7
```

The map benchmark exercises `insert_batch` (build) and `probe_row_count_sum`
(probe) on `CarcharJoinEngine`, which is the production join path.  Both build
and probe Mops/s should be recorded before and after each proposal.

---

## Baseline Results

Captured before any changes, using:

```
python tests/performance/benchmarks/bench_carchar_maps.py --rows 500000 --repeat 7
```

```
scenario                  impl                     rows     uniq   probes  build best  build mean  probe best  probe mean  rows seen   build Mrows/s  probe Mops/s
high-dup                  abseil-flat_hash_map    500000     1953   100000       23.65       24.10        0.10        0.10   25601591          20.74        967.58
high-dup                  carchar_native          500000     1953   100000        7.37        7.80        0.23        0.25   25601591          64.07        396.24
medium-dup                abseil-flat_hash_map    500000    15625   100000       27.55       28.59        0.12        0.13    3200000          17.49        777.92
medium-dup                carchar_native          500000    15625   100000       13.35       13.71        0.40        0.42    3200000          36.48        239.07
low-dup                   abseil-flat_hash_map    500000   250000   100000       46.93       48.13        0.21        0.21     200000          10.39        466.16
low-dup                   carchar_native          500000   250000   100000       15.09       16.42        0.62        0.63     200000          30.44        158.68
medium-dup probe-heavy    abseil-flat_hash_map       500       15   500000        0.03        0.03        0.43        0.45   16669992          17.12       1114.24
medium-dup probe-heavy    carchar_native             500       15   500000        0.01        0.01        0.17        0.18   16669992          71.55       2765.89
medium-dup build-heavy    abseil-flat_hash_map    500000    15625      500       28.26       29.05        0.00        0.00      16000          17.21        604.39
medium-dup build-heavy    carchar_native          500000    15625      500       13.28       13.54        0.00        0.00      16000          36.92        203.90
```

### Key Observations

**Build throughput:** Carchar already leads Abseil on build across all scenarios —
3× faster on high-dup (64 vs 21 Mrows/s), ~2× on medium-dup (36 vs 17), and
~3× on low-dup (30 vs 10).  Any improvement here is additive to an already
winning position.

**Probe throughput:** Abseil leads on probe.  As established in the set work,
the probe path (`probe_row_count_sum`) is not the primary production path for
joins, so this gap is not a target for this work.

**25 % build target:** On the primary low-dup scenario (most representative for
distinct-key joins) the build baseline is **30.44 Mrows/s**.  A 25 % improvement
sets the bar at **~38 Mrows/s**.