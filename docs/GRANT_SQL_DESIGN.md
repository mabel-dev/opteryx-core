# GRANT / REVOKE / SHOW GRANTS ON — SQL Access-Administration Surface

**Status:** DELIVERED 2026-08-27 — both sides implemented. opteryx-access:
`grants.revoke_grant`, `patterns.pattern_level`, capability
`apply_grant`/`apply_revoke`/`grants_on`, `_actions_for` flip. opteryx-core:
pre-parse grammar (`planner/pre_parse.py`), logical plan builders + kind→
pattern arity mapping, binder gates (`binder/relation.py`), Relation
Management `grant_access`/`revoke_access` actions, `ShowGrantsNode` operator,
`_REQUIRED_MEMBERS` + `PermitAll` refusal, `$grants` `level` column,
`query_parser` preflight entries. Tests:
`tests/storage/test_grant_statements.py` (engine),
`tests/test_revoke_grant.py` + `tests/test_capability_administration.py`
(library).

Architect rulings (2026-08-27): `SHOW GRANTS ON` spelling; owner-on-the-object
required to see or act on grants; users cannot act on themselves; roles are
ranked; effective ownership is resolved by the `opteryx-access` library, never
the engine; the whole surface is gated to billing accounts.

**Motivation:** the engine already reports grants in SQL (`SHOW GRANTS` /
`$grants`) but the write side lives behind REST-like APIs — a different client,
auth surface, and audit trail. DDL already routes through the engine with the
catalog and permissions capability as the authority; grants become the same
shape: the engine is the syntax and the gate, the deployment is the store.

**Implementation substrate (read 2026-08-27):** `opteryx-access`
(`../opteryx-access`) already implements the whole rule set — `grants.grant()`
/ `grants.revoke()` with owner-authority (`checks.can_administer_pattern`,
`GRANT` action in `ACTION_ROLES` = owner), self-action refusal
(`SelfAccessError` on grant, update, and revoke), ranked roles (`roles.py`),
conflict/redundancy detection (`find_conflict`), pattern/principal validation
with reserved workspaces (`public`, `personal`, `information_schema`
non-grantable), built-in audit records, and the engine-facing
`capability.py` opteryx-core already registers. The engine work is wiring:
parser, binder gates, and three new capability members. §7 lists the real
impedance mismatches.

---

## 1. SQL surface (all of it)

```sql
SHOW GRANTS ON [WORKSPACE|COLLECTION|DATASET] <object>
GRANT [READER|WRITER|OWNER] ON [WORKSPACE|COLLECTION|DATASET] <object> TO USER <user>
REVOKE [READER|WRITER|OWNER] ON [WORKSPACE|COLLECTION|DATASET] <object> FROM USER <user>
```

- `ON` takes the object in all three statements; `TO`/`FROM` take the
  principal. The existing bare `SHOW GRANTS` (the session's own grants, via
  `$grants`) is unchanged and remains the no-arg form.
- Principals are `USER <user>` only. The mandatory `USER` keyword reserves the
  grammar for `TO ROLE`/groups later without ambiguity.
- No other verbs, options, or `WITH GRANT OPTION`-style modifiers in v1.

## 2. Authority model

- **Caller must hold `owner` on the named object** — to grant, to revoke, and
  to *see* grants (`SHOW GRANTS ON` is owner-only; grant listings are not
  readable by non-owners).
- **Effective ownership** — whether ownership at an enclosing scope (workspace
  → collection → dataset) confers owner on the object, and what a scoped grant
  means for contained objects — is decided entirely by `opteryx-access`. The
  engine never interprets the hierarchy; it hands the capability the caller,
  the object (kind + name), and the action, and acts on the boolean.
- **Users cannot act on themselves.** A caller can neither `GRANT ... TO USER
  <self>` nor `REVOKE ... FROM USER <self>`. This is the last-owner solution:
  the sole owner cannot revoke their own `owner` role, so an object can never
  be orphaned from inside SQL — and self-escalation is structurally
  unwritable. Refused with an explicit error, not a no-op.
- **Billing gate.** The whole surface exists only for billing accounts:
  personal spaces cannot be granted on at all, and a shared space requires a
  billing account. The gate is answered by `opteryx-access` (it knows what a
  billing account is; the engine does not) and refusal is a loud, specific
  error naming the gate — not a generic permission failure.

## 3. Statement semantics (ruled 2026-08-27)

- **GRANT = add one policy. REVOKE = delete one policy. Strictly 1:1 and
  exact.** A statement maps to exactly one stored policy document at exactly
  the level named — no upgrades, no merging, no acting-at-a-distance.
- **There is no ALTER for grants.** Changing someone's role on an object is
  `REVOKE` then `GRANT` — two statements, issued by the caller. `GRANT` onto
  an existing exact-pattern policy is refused (the library's
  `PolicyConflictError`, whose message already says to revoke first).
- **REVOKE must report level mismatches.** If the user holds `reader` via a
  workspace-level policy, `REVOKE READER ON DATASET w.c.d` deletes nothing —
  it errors, naming the policy that actually confers the access and its
  level. Never a silent no-op, never a stealth-narrowing of the broader
  policy.
- Roles are **ranked** (`owner ⊇ writer ⊇ reader`) for redundancy detection:
  granting `reader` on a dataset under an existing broader `writer` is
  refused as redundant (the library's `find_conflict`). A *higher* role on a
  narrower pattern is legitimate elevation and allowed.
- How ranks combine across scopes (a workspace-level `reader` plus a
  dataset-level `writer`) is `opteryx-access`'s effective-ownership problem,
  not the engine's.
- Granting `owner` is ownership sharing. Whether a given principal may hold
  `owner` (notably platform automation identities — the same hazard
  `can_principal_own_materialized_view` exists for) is refused inside the
  capability's apply path, not by an engine-side list.

## 4. Engine mechanics

- **Parse** — dialect work: sqlparser has GRANT/REVOKE grammar but the
  `ON WORKSPACE|COLLECTION|DATASET <object>` object-kind keywords and the
  three-role vocabulary need Opteryx dialect handling (possible upstream-PR
  candidate, as with previous dialect work). `SHOW GRANTS ON ...` extends the
  existing SHOW family.
- **Bind** — the binder resolves the object name (same resolution as DDL),
  then gates on the capability: caller-is-owner, not-self, billing gate. All
  refusals surface with statement positions per the standard error contract.
- **Apply** — the permissions capability grows a write side alongside
  `grants()`:

  ```
  apply_grant(execution_context, pattern, role, principal)  -> policy id | raises
  apply_revoke(execution_context, pattern, role, principal) -> policy id | raises
  grants_on(execution_context, pattern) -> list[dict]       # SHOW GRANTS ON
  ```

  **DELIVERED in opteryx-access 2026-08-27** (with
  `grants.revoke_grant` and `patterns.pattern_level`). The binder maps the
  object kind to its pattern before calling; the capability speaks
  pattern-world only. `grants_on` rows are `(user, pattern, level, role)`.
  In `opteryx_access.capability.PermissionsCapability` these are thin
  delegations to `opteryx_access.grants.grant()` / `revoke()` and
  `store.list_policies()` — every rule (owner authority via
  `can_administer_pattern`, `SelfAccessError`, validation, conflicts, audit)
  already lives in `grants.py` and stays there. The engine stores no policy,
  holds no identities, and interprets nothing: it parses, binds, gates, and
  hands off. All three require the capability to have been constructed with a
  `PolicyStore`; without one they raise `PolicyStoreRequiredError`, exactly
  as `can_principal_perform_action` already does.
- **Object kind → pattern.** The SQL object kinds are arity assertions mapped
  to opteryx-access patterns: `WORKSPACE w` → `w.*`, `COLLECTION w.c` →
  `w.c.*`, `DATASET w.c.d` → `w.c.d`. The binder checks the name's segment
  count matches the declared kind (a `DATASET` with two segments is an
  error, not a guess); everything after the mapping is pattern-world.
- **`PermitAll` refuses all three statements.** Embedded/CLI mode has no
  policy service; a GRANT that "succeeds" there would be fake green. The
  intrinsic capability raises (no permissions capability registered that can
  administer grants), never no-ops.
- **Session semantics unchanged** — policies are issued at session
  construction; a GRANT affects new sessions, exactly as the REST path does
  today. No mid-session policy mutation.
- **`SHOW GRANTS ON` result shape** — `(user, role)` rows for the named
  object, distinct from `$grants`' `(pattern, role, actions)`. Rendered via
  the same virtual-dataset machinery, sourced from `grants_on`.

## 5. Relationship to the REST-like APIs

The REST path does not die; it becomes another client of the same policy
service. SQL is the sanctioned user-facing surface; both routes converge on
`opteryx-access`, so there is one enforcement/reporting implementation and no
second interpretation of what a policy means.

## 6. Audit

Already solved: `opteryx_access.grants` emits one structured
`policy.created`/`policy.deleted` record per successful change, from inside
the write path (not the caller), on the field contract the existing
`opteryx.ops.policy_changes` transforms parse. The engine adds nothing here.
Refused attempts are deliberately not recorded (see the opteryx-access
README's "Not recorded").

## 7. Wiring gaps — RULED 2026-08-27

The rules exist in opteryx-access; these were the shape mismatches, now all
ruled:

1. **REVOKE-by-value resolution lives in opteryx-access.** New
   `grants.revoke_grant(store, actor=..., workspace=..., principal=...,
   role=..., pattern=...)`: looks up the policy whose (principal, pattern,
   role) match **exactly**, delegates to `revoke()`. No exact match →
   error. If the principal holds the role only via a policy at a
   **different level** (`w.*` when revoking on `w.c.d`), the error names
   that policy and its level — the mirror of `find_conflict`'s messages.
   REVOKE deletes exactly one policy or deletes nothing and says why.
2. **GRANT-on-existing is refused, not upgraded.** No ALTER for grants;
   change = caller issues REVOKE then GRANT. The library's existing
   `PolicyConflictError` behaviour is the ruling — no wiring translation to
   `update_grant()`. (`update_grant` remains a REST/console affordance
   only.)
3. **SHOW GRANTS ON lists stored policies, mirroring the console's ACCESS
   LIST screen.** Gate = `can_administer_pattern` over the actor's policies
   against the object's pattern (NOT the weaker `has_workspace_access`).
   `SHOW GRANTS ON WORKSPACE w` = every policy in the workspace, one row
   per policy: `(user, pattern, level, role)` — `level` derived from
   pattern arity (`w.*`→WORKSPACE, `w.c.*`→COLLECTION, `w.c.d`→DATASET),
   grouped/ordered by user then pattern, exactly the console's rows.
   `ON COLLECTION` / `ON DATASET` = the same shape filtered to policies at
   exactly that object (1:1 with what GRANT/REVOKE there would act on);
   broader covering grants are visible via `ON WORKSPACE`.
4. **Bare `SHOW GRANTS` adopts the new vocabulary.** `$grants` gains the
   `level` column, rendered consistently with `SHOW GRANTS ON`
   (implicit grants included, evaluation order preserved). And the
   capability's `_actions_for` stops hiding `GRANT`/`REVOKE` — its "no SQL
   statement performs them" justification inverts the moment this surface
   ships; both must land in the same change.

**Billing gate — likely already solved structurally, confirm:** opteryx-access
has no billing notion by design; but `validate_pattern` refuses policies over
`personal` (and `public`), so personal spaces are non-grantable at the
library layer, and a shared workspace only exists after billing-gated genesis
(`bootstrap_workspace`, billing checked by the creating service). If that
chain is accepted as the gate, the engine needs no billing check of its own —
the refusal for personal spaces just needs a message naming the rule.

**Self-action, last-owner, owner-authority:** already exact matches for the
rulings — `SelfAccessError` on grant/update/revoke, `bootstrap_workspace`
refuses an ownerless genesis, `owned_by()` exists for offboarding, and
`can_administer_pattern` requires owner *covering* the pattern. No work.
