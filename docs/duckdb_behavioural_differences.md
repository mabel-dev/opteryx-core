# Behavioural Differences from DuckDB

DuckDB is a useful reference oracle for validating Opteryx's results — we cross-check
query output against it (for example, across the ClickBench suite). Most queries agree
exactly. This document records the cases where Opteryx **intentionally** behaves
differently, where a difference is **inherent** (floating point, unspecified ordering),
and where a difference is a **known limitation**. It is meant as a compatibility
reference for anyone porting queries between the two engines.

Each entry states what Opteryx does, what DuckDB does, whether the difference is
deliberate, and how to get matching behaviour where that is possible.

---

## Strings are byte-oriented by default

Opteryx has a three-member string family, and the default type is **byte-oriented**:

| Type        | Storage          | Semantics                                  |
|-------------|------------------|--------------------------------------------|
| `VARCHAR`   | raw bytes        | default; ASCII-oriented, no UTF-8 decode   |
| `NVARCHAR`  | raw bytes        | opt-in UTF-8; decoded for character ops    |
| `VARBINARY` | raw bytes        | opaque bytes                               |

DuckDB has a single string type that is always UTF-8.

A consequence is that `VARCHAR` may legitimately hold bytes that are **not valid UTF-8**
(for example, CP1251-encoded text), where a "character count" is undefined. Opteryx
treats `VARCHAR` as bytes precisely so it never has to assume — or pay to decode — an
encoding it cannot guarantee.

### `LENGTH` / `CHAR_LENGTH` count bytes on `VARCHAR`

| Expression                  | Opteryx | DuckDB |
|-----------------------------|---------|--------|
| `LENGTH('abcde')`           | 5       | 5      |
| `LENGTH('ффф')` (`VARCHAR`) | **6** (bytes) | **3** (codepoints) |
| `LENGTH(x)` where `x` is `NVARCHAR` | codepoints* | codepoints |
| `OCTET_LENGTH('ффф')`       | 6       | 6      |

- `LENGTH` / `CHAR_LENGTH` / `CHARACTER_LENGTH` count **bytes on `VARCHAR` / `VARBINARY`**
  and **codepoints on `NVARCHAR`** — "length in the type's natural unit".
- `OCTET_LENGTH` (alias `BYTE_LENGTH`) always counts **bytes**, regardless of type.
- DuckDB's `length`/`char_length` always count codepoints; `octet_length` counts bytes.

**To match DuckDB:** `CAST` the value to `NVARCHAR` for character-count / Unicode-aware
semantics, or use `OCTET_LENGTH` when you specifically want bytes.

> **Getting an `NVARCHAR` value.** `NVARCHAR` is reachable via `CAST(x AS NVARCHAR)`,
> which validates the bytes as UTF-8 (vendored `utf8h`) and re-tags the value — plain
> `CAST` raises on invalid UTF-8, `TRY_CAST` maps invalid rows to `NULL`. Columns are
> still read as `VARCHAR` by the parquet/scan path (readers don't yet emit `NVARCHAR`),
> so today you opt in per expression with a cast. On `NVARCHAR`, `LENGTH`/`CHAR_LENGTH`
> count codepoints and `UPPER`/`LOWER` are Unicode-aware; `VARCHAR` stays byte-oriented
> and ASCII-cheap.

> The same byte-orientation applies to other string operations (substring, position,
> pattern matching). On ASCII data the two engines agree; they can diverge on
> multibyte or non-UTF-8 input.

---

## Pattern matching (`LIKE`, `REGEXP`, `REGEXP_REPLACE`)

Opteryx uses **RE2** (the same engine DuckDB uses) with default options, so on clean
ASCII/UTF-8 input the two agree. Because Opteryx's string operations are byte-oriented
(see above), results can differ on **non-UTF-8 or otherwise dirty byte data** — the
match still operates correctly over bytes, but "the same logical string" may not be the
same byte sequence the other engine sees.

This is not a bug in the regex engine; it is the byte-vs-codepoint model surfacing
through pattern matching.

---

## Arithmetic

### `/` is true division; integer division is `DIV`

| Expression  | Opteryx | DuckDB |
|-------------|---------|--------|
| `7 / 2`     | `3.5`   | `3.5`  |
| `7.0 / 2`   | `3.5`   | `3.5`  |
| `7 DIV 2`   | `3`     | `7 // 2` → `3` |

`/` always returns a floating-point result, matching DuckDB. The **integer (truncating)
division operator is spelled `DIV`** in Opteryx, where DuckDB uses `//`. Cross-type
arithmetic (`int` with `float`) promotes the integer operand to float.

### Decimal arithmetic stays decimal

`DECIMAL / DECIMAL` returns `DECIMAL`; `DECIMAL` mixed with an integer keeps decimal
semantics rather than promoting to float. (Note: very precise decimal results may still
differ from DuckDB in trailing digits depending on scale handling.)

### Floating-point aggregates are not bit-identical

`SUM` / `AVG` over floating-point columns are computed in double precision on both
engines, but the **summation order differs**, so results typically agree to ~13–15
significant figures rather than exactly. This is inherent to floating-point addition,
not a correctness issue. (`SUM` over `INT64` uses exact 128-bit accumulation and does
match exactly.)

When comparing results programmatically, use a **relative** tolerance for float
aggregates rather than exact equality.

---

## Temporal values require explicit typing

Opteryx does not infer temporal types as eagerly as DuckDB.

- **Integer-encoded temporal columns are surfaced as `INT64`.** A column physically
  stored as epoch seconds/days (common in raw datasets like ClickBench) reads back as an
  integer unless you cast it. To get a date or timestamp you must cast explicitly:
  `EventDate::DATE`, `EventTime::TIMESTAMP[s]`.

- **Timestamp casts carry an explicit unit.** `::TIMESTAMP[s]`, `[ms]`, `[us]`, `[ns]`
  select how the underlying integer is interpreted. Casting epoch **seconds** data with
  `[ms]` (or vice versa) silently mis-scales the values. DuckDB infers the unit from the
  column's native type or via functions like `to_timestamp` / `epoch_ms`.

- **Timestamps render as UTC-aware.** Opteryx tags timestamps with a UTC offset
  (`2013-07-15T12:40:00+00:00`); DuckDB returns a naive datetime
  (`2013-07-15T12:40:00`). These denote the **same instant** — the difference is only in
  the rendered representation. When comparing, normalise to UTC-naive (convert to UTC and
  drop the tzinfo) on both sides.

- **`DATE_TRUNC` is not available;** use `TRUNC(ts, 'minute')` for truncation to a unit.

---

## Ordering and `LIMIT`

### Tie-breaking among equal sort keys is unspecified

For `ORDER BY k ... LIMIT n` (and especially `... LIMIT n OFFSET m`) where many rows
share the same value of `k`, **which of the tied rows survive at the boundary is not
defined** by SQL, and Opteryx and DuckDB may keep a different subset. Both produce a
valid ordering of `k`; only the arbitrary choice among ties differs. This is most
visible with a deep `OFFSET` into a long run of ties.

**To get deterministic, matching output:** add a tie-breaking column to `ORDER BY` so
the ordering is total (e.g. `ORDER BY k, id`).

### `LIMIT` without `ORDER BY` returns an arbitrary subset

`SELECT ... LIMIT n` with no `ORDER BY` returns *some* `n` rows, and the set is not
guaranteed to match DuckDB (or to be stable across runs). Add an `ORDER BY` if you need
a defined result.

---

## NULL handling

NULL **values within a column** propagate correctly through scalar functions
(three-valued logic: `f(NULL) = NULL`), matching DuckDB.

A **bare untyped `NULL` literal** passed directly to some functions (e.g.
`LENGTH(NULL)`, `OCTET_LENGTH(NULL)`) currently raises an error rather than returning
`NULL`, because the argument has no resolvable type. This is a known limitation, not a
deliberate choice. Cast the literal (`CAST(NULL AS VARCHAR)`) or rely on column data,
which carries a type.

---

## Function and operator availability

Opteryx implements a large but not identical set of functions and operators. Notable
points relevant to porting from DuckDB:

- Integer division is `DIV`, not `//` (see Arithmetic).
- `OCTET_LENGTH` / `BYTE_LENGTH` are available for explicit byte length.
- `DATE_TRUNC` is not available; use `TRUNC(value, unit)`.

When a function is missing, Opteryx fails fast with an explicit "unknown function"
error rather than silently degrading.

---

## How we track this

Result correctness against DuckDB is validated by a golden-comparison harness (see the
ClickBench result battery under `tests/`). Differences that are deliberate design
choices or inherent (ties, float precision, byte-vs-codepoint, encoding-dependent data)
are treated as **expected** and excluded from the pass/fail assertions; anything else is
treated as an engine bug to fix. This document should be updated whenever a new
deliberate difference is introduced or an existing limitation is resolved.
