# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True

"""
Plan-time compiler for the quantified LIKE forms (`LIKE ANY`, `LIKE ALL`,
`NOT LIKE ALL`, and their ILIKE spellings).

Mirrors the RLIKE split (`vector_dfa_compile.pyx` compiles, `draken_rlike`
walks): this module is Python, plan-time only. It partitions the N literal glob
patterns by shape and emits a single self-describing blob that the C++ kernel
`draken_like_any` (draken/ops/kernels/function_like_any.cpp) walks with no Python
and no RE2 — satisfying .claude/CLAUDE.md §1/§2 (Draken matches without Python).

The measured decision (scratchpad REPORT_like_any.md): glob-N-passes is the
matcher; the alternation-DFA is structurally dead at scale; the profiled `.slt`
workload is contains-dominant with the N=50 case ALL contains — so the contains
bucket is an Aho-Corasick automaton (states LINEAR in total needle length, no
determinisation blow-up), one pass per row regardless of pattern count.

Pattern classification (byte-level, matches fk_like_match: only `%` and `_` are
special, NO backslash escape):
  - ``''``               -> exact "" (matches only the empty subject)
  - only `%` chars       -> always_true (matches every non-null row)
  - no `%`, no `_`       -> exact literal
  - ``X%`` (X wildcard-free) -> prefix X
  - ``%X`` (X wildcard-free) -> suffix X
  - ``%X%`` (X wildcard-free, non-empty) -> contains X  (Aho-Corasick)
  - anything else (interior `%`, any `_`, multiple segments) -> residual glob,
    matched per-row by fk_like_match — always correct, never a wrong fast path.

Case-insensitive (`ILIKE`): every literal/needle is simple-casefolded to lower
here, and the kernel folds subject bytes at match time with the SAME fold
(draken_utf8ci::casefold — Unicode simple-fold, which reduces to ASCII fold on
the ASCII bytes a VARCHAR holds). Both sides call one implementation so the
automaton alphabet and the subject bytes are guaranteed identical — no drift.
NVARCHAR subjects therefore fold correctly for the case-changing codepoints the
utf8h table covers (Latin-1/Latin-ext/Greek/Cyrillic). A NULL pattern in the
list sets the has_null flag (SQL three-valued ANY: a non-match becomes NULL).

Blob format (little-endian):
    u8  version (=1)
    u8  flags   (bit0 = ci, bit1 = negate, bit2 = require_all)
    u8  always_true
    u8  has_null
    u32 n_exact ; then n_exact * (u32 len, len bytes)
    u32 n_prefix; then n_prefix * (u32 len, len bytes)
    u32 n_suffix; then n_suffix * (u32 len, len bytes)
    u32 n_glob  ; then n_glob  * (u32 len, len bytes)   # raw glob patterns
    u32 ac_n_states                                     # 0 => empty contains bucket
    if ac_n_states > 0:
        accept_bitmap: ceil(ac_n_states/8) bytes        # bit s set => state s accepts
        next: ac_n_states * 256 * u32                   # goto with fail links folded in
"""


from libc.stdint cimport uint8_t, uint32_t


cdef extern from "ops/kernels/utf8_ci_match.h" namespace "draken_utf8ci" nogil:
    void casefold(const uint8_t* src, uint32_t n, uint8_t* dst)


cdef bytes _uni_lower(bytes b):
    """Unicode simple-casefold, byte-identical to draken_utf8ci::casefold (the
    kernel's subject fold) — MUST be the same fold or needle and subject bytes
    diverge and ILIKE ANY silently under-matches. Length-preserving."""
    cdef Py_ssize_t n = len(b)
    if n == 0:
        return b
    cdef bytearray out = bytearray(n)
    cdef uint8_t[::1] mv = out
    casefold(<const uint8_t*>(<const char*>b), <uint32_t>n, &mv[0])
    return bytes(out)


def _classify(bytes p):
    """Return (kind, payload). kind in exact/prefix/suffix/contains/glob/always."""
    if len(p) == 0:
        return ("exact", b"")
    if p.count(b"%") == len(p):          # only '%' chars -> matches everything
        return ("always", None)
    cdef bint has_us = b"_" in p
    cdef int npct = p.count(b"%")
    if not has_us and npct == 0:
        return ("exact", p)
    if not has_us and npct == 1:
        if p.endswith(b"%") and not p.startswith(b"%"):
            return ("prefix", p[:-1])
        if p.startswith(b"%") and not p.endswith(b"%"):
            return ("suffix", p[1:])
        return ("glob", p)               # single '%' in the middle: a%b
    if not has_us and npct == 2 and p.startswith(b"%") and p.endswith(b"%"):
        inner = p[1:-1]
        if b"%" not in inner and len(inner) > 0:
            return ("contains", inner)
        return ("glob", p)
    return ("glob", p)


# --- Aho-Corasick over the contains needles -> deterministic byte automaton ----
def _build_ac(list needles):
    """Build an Aho-Corasick automaton with fail links folded into a dense
    `next` table (goto-with-fail), so the kernel walk is one array index per
    byte — like the RLIKE DFA, but states are LINEAR in total needle length.
    Canonical construction: build the trie, then a BFS that fills both fail[]
    and the deterministic next[] together (next[fail[s]][c] is always ready
    before state s is processed, because fail[s] is nearer the root). Returns
    (n_states, accept[bool], next_flat[n_states*256])."""
    from collections import deque

    goto = [dict()]          # goto[state][byte] -> child state (trie edges only)
    accept = [False]
    for ndl in needles:
        s = 0
        for b in ndl:
            nxt = goto[s].get(b)
            if nxt is None:
                nxt = len(goto)
                goto.append(dict())
                accept.append(False)
                goto[s][b] = nxt
            s = nxt
        accept[s] = True

    cdef int n = len(goto)
    fail = [0] * n
    nxt = [0] * (n * 256)

    # Root: next[0][c] = trie edge or stay at root. Seed BFS with depth-1 states.
    q = deque()
    for b in range(256):
        t = goto[0].get(b, 0)
        nxt[b] = t
        if t != 0:
            fail[t] = 0
            q.append(t)
    # BFS: for each state s and byte b, fill fail[child] and next[s][b].
    while q:
        s = q.popleft()
        for b in range(256):
            t = goto[s].get(b)
            if t is not None:
                fail[t] = nxt[fail[s] * 256 + b]
                if accept[fail[t]]:
                    accept[t] = True
                nxt[s * 256 + b] = t
                q.append(t)
            else:
                nxt[s * 256 + b] = nxt[fail[s] * 256 + b]
    return n, accept, nxt


cdef void _put_u32(bytearray out, unsigned int v):
    out.append(v & 0xFF)
    out.append((v >> 8) & 0xFF)
    out.append((v >> 16) & 0xFF)
    out.append((v >> 24) & 0xFF)


cdef void _put_strs(bytearray out, list items):
    _put_u32(out, len(items))
    for it in items:
        _put_u32(out, len(it))
        out += it


cpdef bytes compile_like_any(object patterns, bint ci, bint negate=False,
                             bint require_all=False):
    """Compile a literal quantified-LIKE pattern set into a matcher blob (never
    None — the residual glob bucket catches everything, so this always yields a
    valid, correct matcher). `patterns` is an iterable of str/bytes/None.

    The two flags spell the three reachable quantifier forms; they are NOT
    independent knobs, and `negate` does NOT mean "NOT LIKE ANY":

        x LIKE ANY (a,b)      negate=0 require_all=0   a OR b
        x NOT LIKE ALL (a,b)  negate=1 require_all=0   NOT(a OR b)
        x LIKE ALL (a,b)      negate=0 require_all=1   a AND b

    `NOT LIKE ALL` is the De Morgan dual of `LIKE ANY`, which is why it rides the
    ANY bucketing with a flipped verdict and costs nothing extra. The fourth
    combination would be `NOT LIKE ANY` = NOT(a AND b); the planner rejects that
    spelling outright (see `pattern_match` in logical_planner_builders.py), so it
    never reaches here and is refused below rather than silently compiled.

    `negate` does not change bucketing. `require_all` does: a conjunction cannot
    use the ANY short-circuits (see the comments at each site below)."""
    if negate and require_all:
        raise ValueError(
            "compile_like_any: negate and require_all together spell NOT LIKE ANY, "
            "which the planner rejects and this compiler cannot express."
        )
    cdef list exact = []
    cdef list prefix = []
    cdef list suffix = []
    cdef list contains = []
    cdef list glob = []
    cdef bint always_true = False
    cdef bint has_null = False

    for p in patterns:
        if p is None:
            has_null = True
            continue
        if isinstance(p, str):
            p = p.encode("utf-8")
        elif not isinstance(p, bytes):
            p = str(p).encode("utf-8")
        if ci:
            p = _uni_lower(<bytes>p)
        kind, payload = _classify(<bytes>p)
        if kind == "always":
            # In a disjunction a match-everything pattern makes the whole set
            # true; in a CONJUNCTION it constrains nothing, so it is dropped.
            # Setting always_true under require_all would answer TRUE for rows
            # the other patterns reject — the kernel refuses that blob.
            if not require_all:
                always_true = True
        elif kind == "exact":
            exact.append(payload)
        elif kind == "prefix":
            prefix.append(payload)
        elif kind == "suffix":
            suffix.append(payload)
        elif kind == "contains":
            # The contains bucket is an Aho-Corasick automaton, which answers
            # "SOME needle occurs in the subject" — it cannot answer "EVERY
            # needle occurs" (a single accepting walk does not say which needles
            # were seen, and folding fail links loses that per-needle identity).
            # Under require_all the needle goes back to its `%X%` glob form and
            # is matched per-pattern. LIKE ALL is a rare predicate and the ALL
            # walk short-circuits on the first non-match, so this is the honest
            # trade rather than growing a second automaton to count needles.
            if require_all:
                glob.append(b"%" + payload + b"%")
            else:
                contains.append(payload)
        else:
            glob.append(payload)

    cdef bytearray out = bytearray()
    out.append(1)                                   # version
    # flags: bit0=ci bit1=negate bit2=require_all
    out.append((1 if ci else 0) | (2 if negate else 0) | (4 if require_all else 0))
    out.append(1 if always_true else 0)
    out.append(1 if has_null else 0)
    _put_strs(out, exact)
    _put_strs(out, prefix)
    _put_strs(out, suffix)
    _put_strs(out, glob)

    if always_true or len(contains) == 0:
        _put_u32(out, 0)                            # ac_n_states = 0
        return bytes(out)

    n_states, accept, nxt = _build_ac(contains)
    _put_u32(out, n_states)
    accept_bitmap = bytearray((n_states + 7) // 8)
    for i in range(n_states):
        if accept[i]:
            accept_bitmap[i >> 3] |= (1 << (i & 7))
    out += bytes(accept_bitmap)
    for v in nxt:
        _put_u32(out, v)
    return bytes(out)
