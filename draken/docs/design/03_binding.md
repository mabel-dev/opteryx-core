# Draken Python Binding (DRAFT)

> Status: DRAFT. The binding is a thin shell. It owns the C++ object, marshals at
> the Python edge, and calls C++ functions. No logic, no loops, no `object`
> mid-pipeline, no dispatch.

## Technology

**nanobind (locked)**, consistent with the existing
`opteryx/compiled/nanobind/carchar_native` (already in the build, already proven for
a C++-first structure here). Cython is not used for the new core — the binding wraps
the C++ library directly. Rationale: the whole point is "C++ library + thin
binding," and Cython's cdef-class/vtable machinery is exactly what produced the
dispatch crutches.

**One module (resolved).** The binding is a single nanobind extension module — the
vector handle, the dumb `Morsel` container, and interop all live in it, not split
into `vectors`/`morsels`/`interop` submodules. (Internal C++ organisation can still
be split across files; the *Python* surface is one module.)

## What the binding does (only these)

1. **Owns the C++ vector** — a Python handle wrapping the C++ object; its
   destructor hands the C++ object back to RAII. No `owns_*` flags.
2. **Marshals at the edge, once** —
   - in: a Python sequence → owned native buffer (copy at the boundary);
     a Python IN-set → a built `CarcharSet`.
   - out: `to_pylist()` / `__getitem__` → boxed Python values (the *only* place
     boxing happens), via a single type-switch conversion in C++.
3. **Calls C++ free functions** — `hash`, `compare_*`, `in_list`, `sum`, … .

It does **not** loop over rows, dispatch on type, hold Python keep-alives, or touch
`object` between the edges.

<!--
/opus/ BIGGEST GAP IN THE WHOLE SET — read before the rest of this doc.

This doc frames draken's integration contract as "import draken" + "marshal at the
Python edge." That describes the *Python* consumers. But the dominant, hot integration
today is COMPILED, not Python:

  99 opteryx/rugo .pyx/.pxd files `cimport draken.core.buffers` (DrakenVector,
  DrakenVarBuffer, DrakenStringArena, DRAKEN_INT64, str_data/str_length inline
  helpers, …) and read the struct layout DIRECTLY at the C level. Reference counts:
  draken.core.buffers ~170, draken.vectors.vector ~162, plus string_vector/bool_vector
  /integer/float each cimported in the hundreds.

nanobind produces a C++ extension. It does NOT emit a Cython-cimportable `.pxd`. So the
moment draken's core stops being a Cython module, those 99 modules cannot
`cimport draken.core.buffers` anymore. This is not covered anywhere in the design.

The clean answer is good news but must be MADE EXPLICIT and SCOPED:
 - opteryx/rugo compiled code should bind to draken via the C++ HEADER
   (`#include "draken/core/buffers.h"`) — a true C++-to-C++ ABI, which is cleaner than
   today's cimport and fits the "C++-first" thesis. For the Cython modules that remain
   Cython, hand-write a thin `buffers.pxd` of `extern from "buffers.h"` declarations
   mirroring the struct, so `cimport` keeps working against the C++-defined struct.
 - This migration touches ~99 files and is a hard prerequisite for de-shimming any
   type. It is currently invisible in the bring-up plan (04), which only talks about
   the per-type Python shim. The Python shim does nothing for a compiled module that
   binds the struct ABI at COMPILE time.

Knock-on for the per-type shim (see also /opus/ note at "Fallthrough shim"): a compiled
opteryx operator reads `vec.data[vec.selection[i]]` for whatever vector it's handed.
That only works for BOTH new-draken and draken_old vectors if the struct ABI is
byte-identical between them. Carrying buffers.h forward keeps it identical — UNTIL 05
(stats ptr) / 06 (logical-type ptr) add struct fields. If they do, old and new structs
diverge and the "mixed-type morsel, some columns old some new" claim in 04 becomes
unsafe for the compiled consumers. Resolve the canonical struct (see 00 /opus/) and
keep logical-type + stats OUT of the struct so the ABI stays stable through bring-up.
-->

## Compiled consumers bind the C++ struct, not Python (resolved)

The dominant integration is **compile-time**, not the Python edge: ~99 opteryx/rugo
`.pyx/.pxd` modules `cimport draken.core.buffers` and read `DrakenVector` /
`DrakenStringArena` / inline helpers at the C level. nanobind emits **no**
cimportable `.pxd`, so when the core becomes C++ those `cimport`s break unless we
plan for it. Plan:

- **C++ consumers** (`rugo`, any C++ op) bind via the **header**:
  `#include "draken/core/buffers.h"` — a true C++↔C++ ABI, cleaner than today's
  `cimport` and squarely "C++-first."
- **Remaining Cython consumers** bind via a **hand-written `buffers.pxd`** of
  `cdef extern from "buffers.h"` declarations that mirror the C++ struct — so
  `cimport draken.core.buffers` keeps working against the C++-defined layout, byte
  for byte.
- **ABI is FROZEN during bring-up.** Because the struct is the contract for both
  new-draken and `draken_old` (mixed morsels), the layout must stay byte-identical
  across them through the whole migration — which is exactly why logical-type and
  stats stay **out-of-band** (`00`). This struct/header migration touches the ~99
  sites and is a **hard prerequisite** for de-shimming any type (`04`); the per-type
  *Python* shim does nothing for a compiled module that binds the ABI at compile time.

## The consumer API contract

`import draken` is the integration contract — but the contract is **function, not
signature.** We must preserve every *capability* opteryx + rugo rely on (hashing,
compare, arithmetic, in_list, take, ingestion, to_arrow, …) — we are **free to
redesign the interfaces** and update the call sites accordingly. The enumeration
below is the list of functions/capabilities to keep (and the dead ones to drop), not
a set of frozen APIs to mirror.

<!--
/opus/ For a "final review before implementation," leaving the contract as a TODO is
the single largest scoping risk — you cannot build a "thin shell exporting exactly the
consumed surface" without first knowing the surface. A first-pass grep already shows
the magnitude: ~20 distinct draken submodule paths consumed, the heavy hitters being
draken.vectors.{string_vector,bool_vector,integer,float,date,timestamp_vector},
draken.core.buffers, draken.vectors.vector, draken.interop.{vector_sequence,arrow},
draken.morsels.morsel, draken.vectors.{scalar_constructors,array_vector,
decimal_vector,interval_vector,null_vector,arithmetic_kernels,time_vector,vector_vector},
draken.core.string_arena. The "one module" decision means EVERY one of these dotted
paths must be rewritten at the call sites (Python imports) and re-homed behind a flat
namespace — and the ~99 cimport sites additionally need the header/extern-pxd path
above. Do this enumeration as step 0, not as a TODO, because it sizes the whole
project and will surface dead surface to drop AND surface like `arithmetic_kernels`
that the op catalog in 02 currently forgets.
-->

> TODO: generate the enumerated contract by grepping opteryx + rugo for everything
> imported/called from draken (`from draken…`, `draken.*`, `cimport`s of draken
> modules). That list is the spec. Known major surface to cover:
> - `Morsel` (construct from native vectors, column access, `nbytes`, hashing,
>   `take`, slicing, `to_arrow` export)
> - the vector handle + ops above; `vector_from_sequence`, `from_decoded`-style
>   ingestion; `to_arrow` (kept — export only)
> - interop entry points the evaluator/operators use

## Fallthrough shim (incremental bring-up)

To stay always-green while the new layer fills in: the new `draken` package
delegates not-yet-implemented types/ops to `draken_old`.

- New `draken/__init__` exposes the same names; for a type not yet ported, it
  constructs/uses the `draken_old` implementation.
- Each ported type is A/B-checked against `draken_old` for parity + speed, then its
  shim entry is removed.
- When the shim is empty, `draken_old` is dropped from the build and deleted.

**Granularity (resolved): per vector type.** A type is either fully on the new layer
or fully on `draken_old` — bring up one type end-to-end (all its ops) at a time, not
one op across all types. (Pilot type first, then the rest; see `04_testing.md` for
the sequence.)

<!--
/opus/ The Python-level per-type shim is sound for Python consumers. But it has a blind
spot for the COMPILED consumers (the 99 cimport sites): those bind ONE struct ABI at
compile time and dispatch on `vec.type` at runtime. So a compiled opteryx operator can
transparently handle a mix of new+old vectors ONLY IF (a) the struct ABI is identical
between new and old, and (b) both register the same DrakenType tag values. (a) holds
while buffers.h is carried forward unchanged; it BREAKS if 05/06 add struct fields to
one and not the other. Make the shim contract explicit: during bring-up the struct
layout is FROZEN and identical across new/old; logical-type and stats stay out-of-band
so they don't fork the ABI mid-migration. Otherwise "mixed-type morsel" (04) is a
latent segfault in compiled code, not a clean fallthrough.

Also confirm draken_old stays BUILDABLE throughout: it's present on disk (good — the
parity oracle is real), but the "removing python from draken" commit gutted draken/.
The build must keep compiling draken_old under its own name until the shim empties.
-->


## Build

- `setup.py`: compile the new `draken` C++ + nanobind targets; `draken_old`
  compiled under its own name only while the shim needs it (else reference-only).
- Include paths: `third_party/mabel/carchar` etc. already global — reuse.
- Hard rule: the new `draken` core has **zero upward dependencies** (no
  `import opteryx`). Fix the old leaks (`vector.pyx` → `relation_statistics`,
  `arrow.pyx` → `OrsoTypes`) by inverting them (pass values in, or move the shared
  type into draken/third_party).

## Open questions

- [ ] nanobind vs pybind11 vs Cython-shell — lock it. /JJ/ nano bind
- [ ] One module or submodules (vectors / morsels / interop)? /JJ/ one module
- [ ] Shim granularity (per type vs per op). /JJ/ per type
- [x] How does `Morsel` live? **Resolved (`01_ownership.md`): a thin, dumb
      container** — groups related vectors for convenience, owns nothing itself; the
      vectors own their own memory. Not a meaningful/heavy construct.
