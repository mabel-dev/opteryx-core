# Embeddings & the EMBED Capability

How `EMBED` works in Opteryx, what the default gives you, and how to replace it with
something stronger.

The short version: **`EMBED` always works, out of the box, with zero dependencies — but
by default it is _lexical_, not _semantic_.** If you need real semantic meaning
(`'dog'` ≈ `'puppy'`), you install a capability. Nothing is downloaded, and nothing
changes what `EMBED` means, unless you explicitly ask for it.

---

## 1. The functions

| Function | Signature | Returns |
|---|---|---|
| `EMBED(text)` | string → vector | `VECTOR(n)`, where `n` is the active capability's width |
| `COSINE_SIMILARITY(a, b)` | two vectors, **or** two strings | `FLOAT64` |
| `COSINE_DISTANCE(a, b)` | two vectors, **or** two strings | `FLOAT64` |
| `CAST(x AS VECTOR(n))` | array → vector | `VECTOR(n)` |

`COSINE_SIMILARITY` and `COSINE_DISTANCE` each have two overloads — a vector one and a
text one. The text overload is sugar: it embeds both operands with the **same**
bind-time-resolved `EMBED` kernel, so these two queries always agree:

```sql
SELECT COSINE_SIMILARITY('dog', 'puppy');
SELECT COSINE_SIMILARITY(EMBED('dog'), EMBED('puppy'));
```

That equivalence is load-bearing and was a real bug once: the text overload used to carry
its own private embedder, so with a capability installed the first query answered `0.0`
(lexically) while the second answered `0.80` (semantically) — the same question, two
answers. Both now route through the active `draken_embed`.

`COSINE_DISTANCE` is `1 - COSINE_SIMILARITY`.

All of the above execute as native kernels. There is no Python on the execution path and
no Python fallback: if something cannot be planned, it fails at plan time rather than
silently degrading.

---

## 2. The default capability: static hashed projection

**Name:** `static-hash` **Width:** 256 **Dependencies:** none.

This is the built-in `draken_embed` kernel
(`draken/ops/kernels/function_vector_distance.cpp`). It is part of the zero-dependency
core, it is a total function of its input, and it is therefore **always present** — the
engine can never fail to plan an `EMBED` because an optional dependency is missing.

### What it actually computes

A signed random projection ("hashing trick") over lexical features:

1. **Normalise** — lowercase, collapse whitespace.
2. **Tokenise** — drop tokens with no alphanumeric character, drop 41 stopwords, drop
   single-character tokens.
3. **Extract features** per token, each weighted:
   - unigram (`u:token`) — weight `1.0`
   - bigram (`b:token next`) — weight `0.5`
   - character n-grams of the wrapped token `<token>`, sizes 3–4 (`g:...`) — weight `0.25`
4. **Project** — each feature is hashed twice (XXH3-64). Each hash picks a slot
   (`hash % 256`) and a **sign** (from bit 63), contributing `±2^-0.5 × weight`.
5. **L2-normalise** the accumulated 256-dim fp32 vector.

Determinism matters here: the kernel is bit-exact with the Python
`_StaticHashEmbeddingProvider` (verified over 357 real column values plus 13 literal edge
cases, 0 mismatches). Getting there required fp32 accumulation *in emission order* — fp32
addition is not associative — and `norm = (float)sqrt((double)norm_sq)`.

### What you get — real measured values

These are actual outputs from this build, not illustrations:

```sql
SELECT name, COSINE_SIMILARITY(EMBED(name), EMBED('Earth')) AS sim
  FROM $planets ORDER BY sim DESC LIMIT 5;
```

| name | sim |
|---|---|
| Earth | 1.0000 |
| Mars | 0.0426 |
| Mercury | 0.0180 |
| Venus | 0.0000 |
| Jupiter | 0.0000 |

And text-to-text:

| a | b | similarity | why |
|---|---|---|---|
| `'Jupiter'` | `'Jupiter'` | **1.0000** | identical |
| `'New York'` | `'New York City'` | **0.8197** | shared tokens |
| `'Jupiter'` | `'Jupter'` | **0.2457** | typo — rescued by character n-grams |
| `'dog'` | `'dogs'` | **−0.2275** | see the caveat below |
| `'dog'` | `'puppy'` | **0.0000** | no shared surface form |
| `'dog'` | `'asparagus'` | **0.0000** | no shared surface form |

### Why this default is useful

- **It always works.** No network, no weights, no optional build, no "embeddings
  unavailable" at 3am. Every deployment can plan every `EMBED`.
- **It is fast.** Measured ~4.15 µs/row for `EMBED` vs ~10.80 µs/row for the old Python
  provider (~6.9× on a 357-row corpus).
- **It is deterministic and immutable.** Same input, same bytes, forever, on every
  machine and every architecture. Answers do not depend on a model version.
- **It is genuinely good at lexical similarity** — near-duplicate detection, typo and
  misspelling tolerance, fuzzy string matching, shared-token overlap, clustering by
  surface form. The character n-grams are what make `'Jupiter'` ≈ `'Jupter'` work.

### What this default is *not*

**It has no idea what words mean.** `'dog'` vs `'puppy'` is `0.0`, and that is correct
behaviour, not a bug — it is measuring shared character and token features, and those two
strings share none. If you need meaning, you need a model; see §4.

### Two caveats worth knowing

**Similarity can be negative.** Signed hashing means colliding features can cancel.
`'dog'` vs `'dogs'` scores **−0.2275** despite being obviously related — the shared
n-grams landed with opposing signs. Cosine over signed random projections has range
`[-1, 1]`, and low-dimensional collisions are real. Do not assume `>= 0`. If you are
thresholding, threshold on a measured distribution, not on intuition.

**Zero-magnitude vectors produce `NaN`, deliberately.** Empty or stopword-only text
embeds to a zero vector, and cosine is then `0/0`:

```sql
SELECT COSINE_SIMILARITY('', 'earth');   -- nan
```

This is IEEE-correct and intentional. The retired Python implementation answered `0.0`,
which conflated "undefined" with "orthogonal". `NaN` says "undefined". Filter with
`IS NOT NULL`-style guards or a magnitude check if that matters to you.

---

## 3. The capability model — how extension works

`EMBED`'s kernel is looked up in a registry under the name `draken_embed`. A *capability*
replaces that entry. This is the **only** sanctioned way to change what `EMBED` means.

```
install_minilm_capability()
        │
        ├─→ minilm_native.install_embed_capability(model, vocab, max_length)
        │        └─→ returns (kernel_ptr, dims)
        │
        └─→ register_embedding_capability(name, dims, kernel_ptr)
                 └─→ swaps `draken_embed` in the kernel registry
```

### Three rules that are not negotiable

**1. It never auto-installs.** Building the extension must *not* silently change what
`EMBED` means. If it did, a query's answers would depend on how the wheel was compiled —
the same SQL returning different numbers on two machines, with nothing in the query to
explain it. You call `install_minilm_capability()` explicitly, or you get the default.

**2. Width has a single source.** `EMBED` returns `VECTOR(n)` and `n` is fixed into the
plan at bind time, because the projection boundary copies rows at exactly that stride. So
the width cannot be discovered late:

```
capability declares dimensions
   → _embed_return_type() returns VECTOR(n)
   → binder hands the SAME n to the kernel via vector_dim_ctx
   → kernel produces exactly that width, or fails loud
```

One number, one source, no way for the two to disagree. There is **no width constant in
C++**. The hashed projection honours any width (`slot = hash % dims`); a model-backed
embedder whose width is fixed must *reject* a width it cannot produce rather than quietly
return a differently-shaped vector.

**3. Register at startup, before any query using `EMBED` is planned.** Once a width has
been observed by the binder, changing it is refused — plans already compiled carry the
old stride.

### The guards

| Situation | Behaviour |
|---|---|
| Width change after `EMBED` has been planned | **Refused** — `InvalidConfigurationError` |
| Re-register at the *same* width | Allowed |
| Width `0`, or outside 1–65535 | **Refused** |
| Null / `0` kernel pointer | **Refused** |
| Runtime or weights absent | **Fails loud** — `MissingDependencyError` |

Absence never falls back to the hashed embedder. That would make `EMBED`'s meaning depend
on whether a download happened — exactly the split-brain the capability design exists to
prevent.

---

## 4. Option A — install MiniLM (the built-in capability)

Gives you `all-MiniLM-L6-v2`: **real semantic embeddings at 384 dimensions**.

### Why the artifacts are not in this repo

Opteryx has **zero installed dependencies** (`CLAUDE.md` §4). ONNX Runtime (~157MB) and
the MiniLM weights (~87MB) were briefly vendored and have been removed — ~244MB of
third-party binaries and model weights do not belong in the source tree. You supply them
out-of-band; the repo and the build stay hermetic, and the cost is carried by whoever
actually wants the capability.

### Step 1 — obtain an ONNX Runtime SDK

You need an **extracted SDK directory containing `include/` and `lib/`** — the layout of
the official release tarballs from
[microsoft/onnxruntime releases](https://github.com/microsoft/onnxruntime/releases).

The previously-vendored copy was **version 1.22.0**, for two platforms:

| Platform | Release artifact |
|---|---|
| macOS Apple Silicon (dev) | `onnxruntime-osx-arm64-1.22.0.tgz` |
| Linux x86-64 (prod) | `onnxruntime-linux-x64-1.22.0.tgz` |

```bash
# adjust the artifact for your platform
curl -LO https://github.com/microsoft/onnxruntime/releases/download/v1.22.0/onnxruntime-osx-arm64-1.22.0.tgz
tar xzf onnxruntime-osx-arm64-1.22.0.tgz -C "$HOME/opt/"
export OPTERYX_ONNXRUNTIME_HOME="$HOME/opt/onnxruntime-osx-arm64-1.22.0"

ls "$OPTERYX_ONNXRUNTIME_HOME"      # must show: include/  lib/
```

Other versions may work but are unverified — 1.22.0 is what this code was built and
tested against.

### Step 2 — obtain the MiniLM weights

You need a directory containing **`model.onnx`** and **`vocab.txt`**, from
[sentence-transformers/all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2)
(the ONNX export lives under `onnx/` in that repository).

```bash
mkdir -p "$HOME/opt/all-MiniLM-L6-v2"
# place model.onnx and vocab.txt in that directory, then:
export OPTERYX_MINILM_MODEL_DIR="$HOME/opt/all-MiniLM-L6-v2"

ls "$OPTERYX_MINILM_MODEL_DIR"      # must show: model.onnx  vocab.txt
```

### Step 3 — build the extension

```bash
export OPTERYX_BUILD_EMBEDDINGS=1
export OPTERYX_ONNXRUNTIME_HOME="$HOME/opt/onnxruntime-osx-arm64-1.22.0"
make compile
```

This builds `opteryx.compiled.nanobind.minilm_native`. The SDK's `lib/` is baked in as an
rpath so the extension can load the shared library at runtime.

If the SDK is missing, the build **refuses** rather than quietly skipping the extension
(which would resurface later as a baffling `ImportError`):

```
OPTERYX_BUILD_EMBEDDINGS=1 but the ONNX Runtime SDK was not found. Set
OPTERYX_ONNXRUNTIME_HOME to a locally-obtained, extracted ONNX Runtime SDK directory
containing include/ and lib/ (the SDK is not vendored — see CLAUDE.md §4).
Looked under: None.
```

### Step 4 — install the capability at startup

```python
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.types.vectors.embedding_capability import (
    install_minilm_capability,
    active_embedding_capability,
)

print(active_embedding_capability())
# EmbeddingCapability(name='static-hash', dimensions=256)

# Explicit. Before any query using EMBED is planned.
install_minilm_capability()

print(active_embedding_capability())
# EmbeddingCapability(name='minilm-l6-v2', dimensions=384)

session = opteryx.session()
sql = """
SELECT COSINE_SIMILARITY('dog', 'puppy')     AS dog_puppy,
       COSINE_SIMILARITY('king', 'queen')    AS king_queen,
       COSINE_SIMILARITY('dog', 'asparagus') AS dog_asparagus
  FROM $planets LIMIT 1
"""
for morsel in session.execute_to_morsels(sql):
    for i in range(morsel.num_rows):
        print(morsel[i])
```

`install_minilm_capability(max_length=256)` — `max_length` is the **tokenizer** sequence
limit, not the vector width. The width always comes from the model (384).

### What the capability gets you

| | default `static-hash` | `minilm-l6-v2` |
|---|---|---|
| Width | 256 | 384 |
| Dependencies | **none** | ONNX Runtime + ~90MB weights |
| `'dog'` vs `'puppy'` | `0.0000` | **≈ 0.80** |
| `'king'` vs `'queen'` | `0.0000` | **≈ 0.68** |
| `'dog'` vs `'asparagus'` | `0.0000` | **≈ 0.28** |
| Understands meaning | no | **yes** |
| Typo tolerance | yes (char n-grams) | partial (wordpiece) |
| Deterministic across versions | yes | tied to model version |
| Speed | ~4.15 µs/row | model inference — far slower |

> The MiniLM figures above (`0.80` / `0.68` / `0.28`, and the 256→384 width flip) are
> **previously-verified reference values**, recorded when the capability was landed and
> the artifacts were in-tree. They were *not* re-verified after the artifacts were
> removed, because doing so requires an out-of-band SDK and weights. Treat them as
> expected values to check against, not as a fresh test result.

Because the capability replaces `draken_embed` itself, **everything downstream follows
automatically** — `EMBED(col)`, both `COSINE_*` text overloads, and both vector
overloads. There is no second place to configure.

---

## 5. Option B — register your own kernel

MiniLM is just *a* capability, not a special case. Any native kernel can take over
`EMBED`.

```python
from opteryx.types.vectors.embedding_capability import register_embedding_capability

register_embedding_capability(
    name="my-embedder",   # diagnostics only
    dimensions=768,       # what your kernel WILL produce, every time, for every input
    kernel_ptr=0x...,     # address of a C-ABI kernel symbol
)
```

### The kernel contract

```c
VecResult my_embed(void* ctx, const DrakenVector* const* args, uint32_t nargs);
```

- **Lifetime.** The pointer must live for the process lifetime — i.e. be a symbol in a
  loaded extension module. The registry holds a bare address; it does not own it.
- **`ctx`** is a `const struct vector_dim_ctx*` carrying the bind-time `dimension`. Honour
  it exactly, or return an error. If your width is fixed and `ctx->dimension` differs, the
  plan was built against a different capability — **reject it**, do not return a
  differently-shaped vector.
- **`args[0]`** is the string operand (`DRAKEN_VARCHAR`, `DRAKEN_NVARCHAR` or
  `DRAKEN_VARBINARY`). Read it uniformly: `data[selection[i]]` for `i in [0, length)`.
- **Errors** are returned as a `VecResult` with `data = nullptr` and a `error_msg` that
  outlives the reader (a static literal is safest — do **not** use
  `draken_error_sentinel` from a different extension; its thread-local buffer is owned by
  whichever copy of `error_handling.cpp` was linked in).

### Two things worth copying from the MiniLM kernel

**Embed the `k` physical values, then gather through `selection`.** Do not embed `n`
logical rows. A constant operand — `COSINE_SIMILARITY(col, 'literal')` — then costs *one*
inference instead of `n`. For a model, that is the whole ballgame.

**Be shape-preserving.** Return `data_length = k` plus an **owned** copy of the operand's
selection, rather than gathering to `n` dense rows. The operand's selection is borrowed and
your result outlives it, so the copy is required. Densifying is the projection boundary's
job, not yours. (Measured at 200k ClickBench rows: cosine marginal 45.0ms → 42.7ms, ~5%.
Modest, because that corpus repeats 190×, so both variants already embedded only `k`
distinct values — the delta was purely the `n × 512B` gather memcpy.)

Reference implementation: `src/cpp/minilm_native.cpp` (`draken_embed_minilm`).

---

## 6. Reference

### Environment variables

| Variable | When | Purpose |
|---|---|---|
| `OPTERYX_BUILD_EMBEDDINGS` | build | `1` builds `minilm_native`. Default **off**. |
| `OPTERYX_ONNXRUNTIME_HOME` | build | Extracted ONNX Runtime SDK (`include/` + `lib/`). Also baked as the runtime rpath. |
| `OPTERYX_MINILM_MODEL_DIR` | runtime | Directory holding `model.onnx` + `vocab.txt`. |

None of these change what `EMBED` means on their own. Only
`register_embedding_capability` does.

> **Not part of this system:** `OPTERYX_EMBEDDING_PROVIDER` (`static` / `hybrid`) belongs
> to the older Python *provider* mechanism, which is not on the native execution path.
> Setting it does **not** change what `EMBED` computes. Do not reach for it expecting to
> switch embedders.

### API — `opteryx.types.vectors.embedding_capability`

| Function | Purpose |
|---|---|
| `active_embedding_capability()` | The capability `EMBED` currently resolves to. Never `None`. |
| `embedding_dimensions()` | Width `EMBED` will produce. Called by the binder; **marks the width as committed**. |
| `register_embedding_capability(name, dimensions, kernel_ptr)` | Install a kernel as `EMBED`. |
| `install_minilm_capability(max_length=256)` | Convenience: build + register MiniLM. |

### Error messages you may hit

| Message | Meaning |
|---|---|
| `the MiniLM model is not configured — set OPTERYX_MINILM_MODEL_DIR…` | No model directory configured. |
| `the MiniLM model is not present at <dir> (expected model.onnx and vocab.txt)` | Directory configured but incomplete. |
| `opteryx.compiled.nanobind.minilm_native — rebuild with OPTERYX_BUILD_EMBEDDINGS=1` | Extension not built. |
| `a capability of width N — EMBED has already been planned at that width` | Registered too late. Move it to startup. |
| `draken_embed: plan declared a width this minilm capability cannot produce` | Plan built against a different capability. |

### Known restrictions

- `SELECT CAST([1.0, 0.0] AS VECTOR(2))` as a **bare projection** is refused — literal
  `ARRAY`/`VECTOR` projections are ambiguous with `VALUES`. The same cast works inside
  `COSINE_*` and `ORDER BY`.
- `TRY_CAST(... AS VECTOR(n))` is `NotSupportedError` — there is no Python fallback on the
  native engine.
- `CAST(identifier AS VECTOR(n))` requires an explicit width. Bare `VECTOR` is refused
  loud: an `ARRAY` column's row lengths vary per row, so a width cannot be inferred.
- Vector top-k (`ORDER BY <distance> LIMIT k`) does **not** currently fuse into an index
  scan. It is additionally blocked by a pre-existing, vector-unrelated gap: any `ARRAY`
  column in an `ORDER BY` query fails in `gather_rows`. Workaround: compute the distance
  in a subquery so the `ARRAY` is dropped before the sort.

---

## 7. Choosing

**Use the default if** you want near-duplicate detection, typo tolerance, fuzzy matching,
or surface-form clustering — and you want it to work everywhere, always, at ~4 µs/row,
with byte-identical answers across machines and releases.

**Install a capability if** you need the model to know that a dog is a puppy. Accept in
exchange: a ~250MB out-of-band artifact story, a build flag, an explicit startup call, an
inference cost per distinct value, and answers tied to a model version.

The engine will not make that trade for you, quietly, based on how it was compiled. That
is the entire point.
