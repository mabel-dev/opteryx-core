# Aside Parser — Opteryx-only Statements Parsed in Rust

**Status:** COMPLETE 2026-09-21. All six steps delivered.

`src/aside/` (mod, cursor, task, trigger, view, listen, grant, admin — 1,955
lines of Rust), wired into `src/lib.rs` in place of `Parser::parse_sql`.
**`opteryx/planner/pre_parse.py` is DELETED** — 1,315 lines, fourteen
interceptors, ~25 statement forms, all of it. There is one front door.

`resolve_slot_value` moved into `logical_planner.py`, which is its only
consumer; `_slot_value`, `_PLACEHOLDER`, `_PRINCIPAL_SLOT` and `_OBJECT_SLOT`
went with the module, because the classification they did by regex is
`ValueSlot::classify` now. The
planner builders read `ObjectName` slots; grammar refusals are re-typed from the
`OPTERYX-SYNTAX:` channel in `opteryx.planner.parse_statement`.

Tests: `tests/unit/planner/test_aside_parser.py` (114),
`test_relation_name_variables.py` (46), and the existing DDL suites
(`test_create_task_ddl.py` 77, `test_triggers_ddl.py` 62) unchanged and
passing. `tests/storage` green (1,270). `make q` green, clippy clean.

**Two defects found and fixed during the migration**, both now pinned:

- `OWNER TO 'CURRENT_USER'` (quoted — a principal literally named that) was read
  as the bare keyword. Introduced in step 2: the regex decided this from the RAW
  text and `ValueSlot::is_current_user` decided it after unquoting, by which
  point the two forms are identical. Now decided at classification, on the raw
  text, and carried on `Slot`.
- `analyze_query(...)["tables"]` went EMPTY for `DROP TRIGGER` between steps 2
  and 3. `_SYNTHESIZED_TARGETS` read the target as a dotted STRING, and a moved
  form carries an `ObjectName`. `tables` is what the jobs API pre-flights
  permissions against, so this is a pre-flight that silently checks nothing —
  the reader now accepts both shapes, and every moved form asserts its target.

Existing tests that called `pre_parse` directly for a moved form were MOVED to
`parse_statement`, not weakened — the trigger case now covers two slots the
regex never checked (the trigger's own name, and the task it fires).
`test_grant_statements.py` gained a `parse_one` helper for this and keeps its
`pre_parse` import beside it, because `DROP STATISTICS` has NOT moved and its
test must keep exercising the layer that still owns it.

**One asymmetry, preserved on purpose:** `@@external_user` resolves in every
relation name the aside parser reads — except a GRANT's object, which is a
VALUE slot (it takes placeholders) and stays text. That is what the regex did.
Changing it means ruling that a grant's object is a NAME after all, which is a
decision about that surface, not about parsing. Pinned by a test so it cannot
drift either way unnoticed.

§9.1 is RULED, and NOT the way this document recommended — the evidence went the
other way (see the ruling). §9.2 is RULED with a correction: four keys, not
three. §9.3 stands as proposed.

**Three defects were found and fixed during the migration**, each caught by a
test rather than by review, and each now pinned:

1. `OWNER TO 'CURRENT_USER'` (quoted — a principal literally named that) read
   as the bare keyword. The regex decided this from the RAW text; the first
   implementation decided it after unquoting, by which point the two forms are
   identical.
2. `analyze_query(...)["tables"]` went EMPTY for `DROP TRIGGER`.
   `_SYNTHESIZED_TARGETS` read a target as a dotted STRING and a moved form
   carries an `ObjectName`. That field is what the jobs API pre-flights
   permissions against, so an unrecognised target reads as "touches nothing".
3. `SAVE RESULTS OF :job` was accepted. The handle is read as TEXT because a
   job id is not identifier-shaped, but it is an IDENTIFIER slot semantically —
   it names whose results land in the caller's workspace. Going through the
   value-slot reader silently gave it placeholder support.

All three are the same shape of mistake: a property the regex enforced
incidentally, lost when the slot changed representation. The lesson for any
future production is that the tests which caught them were the ones asserting
BEHAVIOUR (what a pre-flight is told, what a refusal says), not AST shape.

**What this is:** move the statements `opteryx/planner/pre_parse.py`
recognises by regex into a Rust state machine that sits *beside* sqlparser — running over
sqlparser's tokens, handing each recursive slot (a task body, a defining query)
back to sqlparser's `Parser`, returning our own statement type, and serialising
to the same dict shapes the planner already reads. Not a fork of sqlparser. Not
more productions on the `Dialect::parse_statement` hook. Not a tokenizer of our
own (§3.2).

**Why now:** `personal.@@external_user.dataset` resolves in every statement
sqlparser parses (the AST rewriter substitutes name parts, one place — see
`opteryx/planner/ast_rewriter/relation_variables.py`) and in none of the
~25 forms pre-parse synthesises, because those never become an AST. That gap
is the fourth instance of one defect, not a new one (§2).

---

## 1. The seam

Parsing has three front doors today:

```
clean_sql ─► pre_parse.py (regex, Python) ─► synthesized dict ─┐
           └► sqlparser ─► Dialect::parse_statement hook ───────┤─► planner
                         └► upstream grammar ───────────────────┘
```

`pre_parse.py` exists because sqlparser has no grammar for these statements,
or has the wrong one (its `LISTEN` is a Postgres channel; ours is a durable
subscription). Each regex captures text and hand-builds the dict the planner
expects. It was 1,315 lines, 14 interceptors, ~25 statement forms; steps 1-2
have taken it to 822 lines and 12 interceptors.

Everything a regex-synthesised statement bypasses, the module has to
re-implement by hand — and the ledger shows it:

| Parser facility | State in pre-parse |
|---|---|
| Parameter placeholders | Paid by hand: `_PLACEHOLDER` / `_slot_value` / `resolve_slot_value`. Paid *after* `GRANT ... TO USER :username` created a grant for a principal literally named `:username`. (In the aside parser this is `Cursor::value_slot_tail` returning a `ValueSlot`, one reader for every form.) |
| Quoted / hyphenated identifiers | Unpaid. Name slots are `[A-Za-z_][\w.$]*`; `` CREATE TASK personal.ada.`my-task` `` is refused (measured 2026-09-21). Backticks are the ONLY way to write a hyphenated name in this dialect. Paid for the moved forms. |
| Source positions | Unpaid. No `span` anywhere in the module, so `attach_source_position` can underline nothing in these statements. Every `Ident` the aside parser builds carries one. |
| `@@name` inside a relation name | Unpaid. The subject of this design. |

The regex layer is not careless — its comments reason correctly about every
choice it makes. It is the wrong *layer*: it re-derives, per statement, what
one tokenizer provides for free.

## 2. Why not the two obvious alternatives

### 2.1 Not a fork of sqlparser

A fork buys real `Statement` variants. But the Python boundary is
`pythonize(py, &statements)` in `src/lib.rs` — serde's externally-tagged
dicts. **Python never sees a Rust type.** A variant on our own enum serialises
identically to one on theirs, so the fork buys nothing the planner can
distinguish, and costs: ~100k vendored lines in `third_party/`, its CVE
surface, and a rebase on every upgrade against an AST that churns (this tree
already records `[#2376]` reverted, `AlterTable` growing `end_token` and
`table_type`, `dialect_of!` gates with no trait flag). Vendoring also needs
agreement under CLAUDE.md §4.

### 2.2 Not more of the `Dialect::parse_statement` hook

The hook returns `Result<sqlparser::ast::Statement, ParserError>`. It **must**
produce an upstream variant. That return type — not a stylistic choice — is why
`ALTER TABLE ... CREATE TAG` travels as `AlterTable` +
`SetTblProperties` with dotted-key "transport" properties
(`src/opteryx_dialect.rs`, `parse_tag_ddl`). Acceptable for three productions
that *are* `ALTER TABLE` with a twist. `CREATE TASK` is not an `ALTER TABLE` in
any sense; twenty statements in costume and a planner unpacking properties to
find out what it was handed is the outcome to avoid.

**The three existing hook productions stay exactly where they are.** Tag DDL,
`ROLLBACK TO VERSION` and guarded `ADD COLUMN IF NOT EXISTS` genuinely belong
to upstream statements. Un-costuming them is a separate, later change.

## 3. The design

### 3.1 Shape

```rust
// src/aside/mod.rs
#[derive(Serialize)]
#[serde(untagged)]
pub enum OpteryxStatement {
    Sql(Statement),        // serialises as {"Query": …} — byte-identical to today
    Opteryx(OpteryxOnly),  // serialises as {"CreateTask": …}
}

#[derive(Serialize)]       // externally tagged, like sqlparser's own enum
pub enum OpteryxOnly {
    CreateTask(CreateTask),
    DropTask(DropTask),
    AlterTask(AlterTask),
    // … one per migrated form
}
```

Untagged outer, externally-tagged inner: upstream statements reach Python
unchanged; ours arrive as new top-level keys. Every field under a new key
reuses sqlparser's types — `ObjectName`, `Ident`, `Expr`, `Query`, `Statement`
— so the planner's existing builders read the sub-nodes. **Same vocabulary,
new words. Not the same type.**

### 3.2 Tokens in, state machine over them (ARCHITECT 2026-09-21)

The Opteryx-only grammars are FLAT — keyword sequences with name and literal
slots (`CREATE [OR REPLACE] TASK [IF NOT EXISTS] name [ON table] AS …`,
`LISTEN TO task [FOR outcome]`, `ALTER TRIGGER name ON table SUSPEND|RESUME`).
That is a regular language, and the honest tool for it is a state machine over
a token vector, not a recursive-descent parser borrowed for the job.

Prior art: classic opteryx's `planner/sql_rewriter.py` — a four-state machine
over a token list that lifted temporal `FOR` clauses off relations. The machine
is the part to keep. Its tokenizer (`_QUOTED_STRINGS_REGEX` + `_KEYWORDS_REGEX`)
is the part to leave behind: this repo's rewriter docstring records that
splitting on a keyword regex and rejoining with single spaces destroyed
positions, and the tokenizer is exactly where the four taxes of §1 live.

So tokens come from **sqlparser's tokenizer**, which is public and already
applies our dialect's identifier rules:

```
sql ─► Tokenizer::new(&dialect, sql).tokenize_with_location()   → Vec<TokenWithSpan>
        ├─ leading tokens name an Opteryx form ─► state machine ─► OpteryxOnly
        │        └─ a recursive slot, where one exists ─► Parser::with_tokens_with_locations(tail)
        └─ otherwise ─► Parser::with_tokens_with_locations(tail).parse_statement()
```

Three public entry points, all checked in 0.62: `Tokenizer::tokenize_with_location`,
`Parser::with_tokens_with_locations`, `Parser::parse_statement`, plus
`Parser::index()` to learn how many tokens sqlparser consumed so the loop can
resume. No `maybe_parse`, no rewinding into sqlparser: a flat grammar knows from
its first two tokens whether it applies, exactly as the regex `_LEAD` patterns
do today.

**Whitespace and comments are filtered out** of the token vector before either
side sees it. `Parser::advance_token` skips them anyway, so handing it the
filtered vector is equivalent — and it makes `Parser::index()` count the same
tokens the cursor does, which is what lets the loop resume. (Not filtering was
the one real bug in building this: every `peek_word` saw a `Whitespace` token
and no production ever matched.)

`lib.rs::parse_sql` calls this instead of `Parser::parse_sql`. Nothing else in
`lib.rs` changes shape.

**No form has needed the recursive slot yet.** §9.1 ruled a task body stays
source text, and `SAVE RESULTS OF <job> AS <dataset>` carries a job handle, not
a query — this document said otherwise, and was wrong. Every slot so far is a
name, a literal, or text. The machinery stays described here because a form
that carries a real sub-query may still turn up; nothing has.

### 3.3 One production

A cursor over `&[TokenWithSpan]` (`src/aside/cursor.rs`) with a handful of slot
readers, written once: `peek_word` / `take_word` / `take_words` (all-or-nothing,
so an optional `IF NOT EXISTS` cannot half-match), `expect_word`, `object_name`,
`statement_tail`. Then one function per form:

```rust
fn create_task(cursor: &mut Cursor, or_replace: bool) -> Result<CreateTask, ParserError> {
    // entered because the first tokens read CREATE [OR REPLACE] TASK
    let if_not_exists = cursor.take_words(&["IF", "NOT", "EXISTS"]);
    if or_replace && if_not_exists { return Err(grammar_error(CREATE_BOTH_GUARDS)); }
    let name = cursor.object_name(CREATE_GRAMMAR)?;       // quoting, spans, @@ — free
    let table = if cursor.take_word("ON") { Some(cursor.object_name(CREATE_GRAMMAR)?) } else { None };
    cursor.expect_word("AS", CREATE_GRAMMAR)?;
    let statement = body(cursor, CREATE_GRAMMAR)?;        // source text to the statement's end
    Ok(CreateTask { name, table, or_replace, if_not_exists, statement })
}
```

The dispatch gate is the opening keyword pair, read without consuming: a
statement that is not a task statement costs two peeks and rewinds. Once the
pair matches, the statement IS that statement — every later failure is a grammar
refusal quoting the form back, never a fallthrough to sqlparser, which knows no
TASK and would point at the word after it.

The state machine never parses an expression. `statement_tail` ends a statement
at the next top-level semicolon, by TOKEN — so a `;` inside a string literal is
a `SingleQuotedString` token and ends nothing.

**Refusals keep their exact wording.** The four messages the regex raised are
now `const`s in `src/aside/task.rs`, raised through `grammar_error`, which
prefixes `OPTERYX-SYNTAX: `. That prefix is the only way a grammar refusal and a
parse failure stay distinguishable across the single `ValueError` channel to
Python; `opteryx.planner.parse_statement` strips it and re-raises
`UnsupportedSyntaxError`, so the refusals are the same type and the same text
they were before the move. They now also carry a position, which they never did.

### 3.4 Keywords — extended by matching, not by enum

sqlparser's `Keyword` enum already holds `TASK`, `REFRESH`, `LISTEN`,
`UNLISTEN`, `STATISTICS`, `SNAPSHOT`, `TAG`, `SECURE`, `OVER`; none is in a
reserved list. Missing entirely: `SIGNAL`, `FORK`, `EFFECTIVE`, `LISTENERS`,
`SAVE`, `SCHEDULE`. Those six are matched on `Token::Word` text,
case-insensitively, through one helper (`parse_word("SCHEDULE")`) so the
difference never leaks into a production.

**We do not want them in the enum.** Matching on text keeps them
non-reserved: `SELECT fork, task FROM t` keeps working, and promoting a word to
reserved is a silent break of every column that carries the name.

### 3.5 What `@@name` gets, for free

`parse_object_name` yields `ObjectName(Vec<Ident>)`; `is_identifier_start`
already admits `@`; so `personal.@@external_user.add_body` arrives as three
`Identifier` parts under `name` — the exact shape the AST rewriter's
`OBJECT_NAME_KEYS` walk already substitutes. **No second resolution point.**
`relation_variables.py` gains new keys only where a production chooses a field
name it does not already cover, which is a one-line frozenset edit per key,
pinned by the sweep test. Step 1 added exactly one: `table` (§9.2). sqlparser
also uses `table`, for MERGE's `TableFactor` DICT — the walk's ObjectName shape
test (a LIST of `Identifier` parts) tells the two apart, so the key is safe to
share.

## 4. Migration order

Per statement, never big-bang. Each form is DELETED from `pre_parse.py` the
moment its production lands — one statement, one front door, never a fallback.
When the last form moves, `pre_parse.py`, its `_INTERCEPTORS` list, the
`_PLACEHOLDER`/`resolve_slot_value` apparatus and the `query_parser.py`
"synthesized statements" preflight entries go with it.

| Order | Forms | Why here |
|---|---|---|
| 1 ✅ | `CREATE / DROP / ALTER TASK` | DELIVERED 2026-09-21. Blocked the engineer.md walkthrough; hardest representation case (§9.1). |
| 2 ✅ | `CREATE / DROP / ALTER TRIGGER` | DELIVERED 2026-09-21. Three event forms and three ALTER branches; `_TRIGGER_EVENT_LEAD` and `_CREATE_TRIGGER_COMMIT_MODIFIER_RE` — two regexes whose only job was to make a failure name the right form — became the ordinary `else` of a `match`. |
| 3 ✅ | `REFRESH`, `ALTER MATERIALIZED VIEW`, `SAVE` | DELIVERED 2026-09-21. `ALTER MATERIALIZED VIEW` is the first non-greedy gate — `ALTER` opens statements this parser does not own, so all three words are read before anything is claimed. `SAVE`'s job handle needed `value_slot_until`, the bounded sibling of `value_slot_tail`. |
| 4 ✅ | `LISTEN TO / UNLISTEN / SHOW LISTENERS` | DELIVERED 2026-09-21. `SHOW LISTENERS` is the second non-greedy gate: two words, because sqlparser owns `SHOW COLUMNS`/`SHOW CREATE TABLE`, `pre_parse` still owns `SHOW CREATE TASK`, and everything else is the parser's `ShowVariable` catch-all. |
| 5 ✅ | `GRANT / REVOKE / SHOW GRANTS ON / SHOW EFFECTIVE` | DELIVERED 2026-09-21. Four value slots, two per statement. The third non-greedy gate, and the fussiest: bare `SHOW GRANTS` is the session's own and belongs to the parser's catch-all, so the gate reads as far as `ON` before claiming anything — while `SHOW EFFECTIVE <anything>` IS ours to refuse, because sqlparser knows no EFFECTIVE. |
| 6 ✅ | `DROP STATISTICS`, `ALTER WORKSPACE SET/DROP SECURE`, `ALTER FORK`, `SHOW CREATE MV/TASK/TRIGGER` | DELIVERED 2026-09-21, and `pre_parse.py` deleted with it. `ALTER TABLE ... RESYNC\|DETACH` is the deepest rewind: the action word is only visible PAST the table name, which is what the regex layer needed a second lookahead pattern for — and got wrong once (`RESYNC FORCE` does not end in `RESYNC`). |

Already fine, no move: `EXECUTE` (native sqlparser; resolves `@@` today).

## 5. Python-side changes

Small and mechanical per form:

- `logical_planner.py` builders keyed on the same top-level key (`"CreateTask"`
  stays `"CreateTask"`) read `name` as an `ObjectName` list instead of a
  string — join it as every other builder does (`".".join(part["Identifier"]["value"] ...)`).
- `query_parser.describe_statement` reads the new shapes for `tables` where it
  reads them at all. (The task forms needed no change: they were never in
  `_SYNTHESIZED_TARGETS`, so `tables` was empty for them before and after.)
- `pre_parse.py` loses the form.

What step 1 did NOT do, against this section as written: `plan_create_task`
still re-parses the body text with `sqloxide`, because §9.1 ruled the body stays
text. That re-parse is the existing one, unchanged — the "one statement only"
and "not another task" rules it enforces are unaffected.

No new Python machinery. The rewriter, binder and everything after them see
nodes they already know.

## 6. What stays where

| Layer | Owns |
|---|---|
| sqlparser (crates.io, unforked) | Tokenizer, expression grammar, every standard statement. |
| `Dialect` impl | Tokenizer policy (`@` in identifiers, quoting, `-`), operator precedence, `supports_*` flags, and the three `ALTER TABLE` costume productions. |
| Aside parser (new, `src/aside/`) | The token cursor and its slot readers; one state machine per Opteryx-only statement; the tokenize-then-dispatch entry; `OpteryxStatement`. |
| `pre_parse.py` | Nothing, once migration completes. |
| SQL rewriter (Python, text) | Unchanged — temporal clause extraction runs before parsing as now. |

## 7. Costs, honestly

- **Public-API coupling.** Three entry points: `Tokenizer::tokenize_with_location`,
  `Parser::with_tokens_with_locations`, `Parser::parse_statement`. Plus the
  `Token` / `TokenWithSpan` / `Span` types the machine reads. An upgrade may
  break one — as compile errors, not merge conflicts.
- **The cursor's readers are ours to get right.** `object_name` in particular:
  `Word (. Word)*` with quoted parts, and it must refuse what sqlparser's own
  `parse_object_name` refuses (a trailing dot, a keyword where a name is
  expected). Pinned by tests; not delegated, because the whole point is not to
  borrow the parser for flat grammar.
- **Two front doors in Rust** (ours, then upstream's). Net one fewer than
  today: the Python regex door closes.
- ~~**`restore_ast`** needs `impl Display for OpteryxOnly`.~~ Checked at step 1:
  it does not. Every caller (`binder/view.py`, `logical_planner`) hands it a
  hand-built `[{"Query": ...}]`, never a statement list from the parser, so an
  `OpteryxOnly` never reaches it. Re-check when a form moves whose own body is
  an AST.
- **Error text.** The regex layer rejects malformed statements "by name" with a
  grammar in the message. Each production must do the same — a bare "expected
  AS" is a regression, not parity. Pin every message the regex emits before
  deleting it. Step 1 carried its four across verbatim, as `const`s.
- **One error channel.** Rust reaches Python as a single `ValueError`, so a
  grammar refusal has to mark itself (`OPTERYX-SYNTAX: `) to be re-typed as
  `UnsupportedSyntaxError` rather than rendered as a parse failure. A sentinel
  in a message is not lovely; the alternative is a second pyo3 exception type
  per error class, which is worse. The prefix is asserted absent from what the
  reader sees.

## 8. Tests

- `tests/unit/planner/test_relation_name_variables.py` sweep grows one case per
  migrated form (`@@external_user` resolves in it).
- Per form: parse-shape tests in Rust? No — this crate has none and adding a
  Rust test harness is out of scope. Shape is pinned from Python through
  `sqloxide.parse_sql`, next to the existing planner tests.
- ~~Round-trip: every stored-text form is pinned `parse → restore_ast → parse`
  equal.~~ Superseded by §9.1: stored text is now a source slice, so what is
  pinned instead is that the slice IS the reader's bytes, including interior
  spacing, and that a `;` inside a literal does not truncate it.
- Every refusal message the regex emits today is asserted before its regex is
  deleted.

## 9. Rulings needed

### 9.1 A task body — RULED C, 2026-09-21, ON EVIDENCE

This document recommended **A** (store the body regenerated from the resolved
AST, so `@@external_user` means the author) gated on `ast_to_sql` round-tripping
every body shape. **The gate failed, so A is dead.** Measured, 12 body shapes:

- `SELECT a FROM ws.src FOR TODAY` — the SQL rewriter lifts a temporal clause
  out of the TEXT before the parser sees it, so it is not in the AST at all. A
  regenerated body loses the clause **silently**, and the task then reads the
  whole relation forever.
- `SELECT a FROM ws.src VERSION AS OF PREVIOUS` — the rewriter maps `PREVIOUS`
  to a sentinel, which renders back as `VERSION AS OF 0` and refuses to
  re-parse.

The other 10 round-tripped clean, which is the point: a lossy path that works
for most inputs is worse than one that does not exist, because nothing tells
the author which kind of body they wrote.

**Ruled: C.** The body is the reader's own bytes, sliced from the source between
the first body token and the statement's end — semantically identical to what
the regex captured, so nothing about task bodies changed. `@@name` inside a body
is REFUSED at plan time (`_reject_variables_in_task_body`), because the two
alternatives are pinning the author into a statement designed to run as somebody
else, or leaving the meaning to whoever re-parses it later. The task's own name
and its `ON <table>` are ordinary `ObjectName`s and DO resolve.

Consequence for the docs: `learn/engineer.md` step 5 writes the name out inside
`CREATE TASK ... AS`, and uses the variable everywhere else. That is a visible
seam, and an honest one — it is also reversible, since C can become A or B later
without re-parsing anything already stored.

### 9.2 Field naming — RULED, with a correction: FOUR keys

Settled as proposed, plus one the proposal missed:

| Key | Holds |
|---|---|
| `name` | the object the statement defines, alters or drops |
| `table` | the relation it acts over — a task's `ON`, a trigger's HOLDER |
| `task` | a task it fires |
| `window_source` | the dataset a clock- or signal-fired run is windowed over |

Three was not enough because a schedule trigger names **two** relations that
mean different things: the holder it lives under and the dataset its runs are
windowed over. Collapsing them onto `table` would have made the planner guess
which it had.

All four are in `relation_variables.OBJECT_NAME_KEYS`, so `@@name` resolves in
every one — pinned by the sweep. `table` is shared with sqlparser's MERGE, which
puts a `TableFactor` DICT there; the walk's ObjectName shape test (a LIST of
`Identifier` parts) tells them apart.

### 9.3 Un-costuming tag DDL and rollback

Leave on the hook for now (§2.2), or fold into `OpteryxOnly` during step 6?
Recommendation: leave. They work, and bundling them makes step 1 harder to
judge.

## 10. What the migration cost, measured

| | Before | After |
|---|---|---|
| Python regex front door | 1,315 lines, 14 interceptors | deleted |
| Rust productions | — | 1,955 lines, 8 modules |
| Front doors for one statement | up to 3 | 1 |
| Quoted/hyphenated names in these forms | refused | accepted |
| Source positions on these statements | none | every `Ident` |
| `@@name` in these relation names | never | everywhere but a GRANT object |
| Placeholder machinery | `_PLACEHOLDER` + `_slot_value` + `resolve_slot_value` | `ValueSlot::classify` + `resolve_slot_value` |

The Rust is larger than the Python it replaces, and that is the trade: a
grammar written out is longer than a regex that approximates it, and it is the
approximating that cost the four taxes in §1.

## 11. Not in scope

- Forking or vendoring sqlparser.
- Any change to the SQL rewriter's text-level temporal handling.
- A Rust test harness for the crate.
- Reserving new keywords.
- Un-costuming the three `Dialect::parse_statement` productions (§9.3) — tag
  DDL, `ROLLBACK TO VERSION` and guarded `ADD COLUMN` genuinely belong to
  `ALTER TABLE`, and they still work.
- Making a GRANT's object a NAME rather than a value (§ status note).
