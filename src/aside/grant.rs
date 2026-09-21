// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Access administration.
//!
//! ```text
//! GRANT  READER|WRITER|OWNER ON WORKSPACE|COLLECTION|DATASET <object> TO USER <user>
//! REVOKE READER|WRITER|OWNER ON WORKSPACE|COLLECTION|DATASET <object> FROM USER <user>
//! SHOW [EFFECTIVE] GRANTS ON WORKSPACE|COLLECTION|DATASET <object>
//! ```
//!
//! sqlparser HAS a `GRANT` grammar and it is not used: it speaks in privileges
//! over tables (`GRANT SELECT ON t TO r`), which is a different statement
//! wearing the same first word. Handing ours to it would get a statement this
//! engine does not run either accepted or misreported.
//!
//! ## Two slots that are VALUES, not names
//!
//! The object and the principal both take placeholders, so both are read as
//! source text and classified (`Cursor::value_slot_until`). That is a
//! deliberate property of this surface and NOT an oversight to tidy up: the
//! role and the object KIND stay literal keywords from a closed set, because a
//! parameter there would make the SHAPE of the statement — which authority it
//! confers, how many parts its object name has — depend on runtime data.
//!
//! A consequence worth naming: because the object is a value, `@@external_user`
//! does NOT resolve inside it, while it resolves in every relation name
//! elsewhere. That is the behaviour the regex had and this move preserves it
//! exactly; changing it means deciding that a grant's object is a NAME after
//! all, which is a ruling about this surface, not about parsing.
//!
//! Bare `SHOW GRANTS` — the session's own — is NOT ours. It falls through to
//! the parser's `ShowVariable` catch-all, so the gate reads far enough to be
//! sure an `ON` follows.

use serde::Serialize;

use sqlparser::parser::ParserError;

use super::cursor::{grammar_error, Cursor, ValueSlot};
use super::OpteryxOnly;

const GRANT_GRAMMAR: &str = "Expected: **GRANT** READER|WRITER|OWNER **ON** \
WORKSPACE|COLLECTION|DATASET <object> **TO USER** <user>, or **REVOKE** \
READER|WRITER|OWNER **ON** WORKSPACE|COLLECTION|DATASET <object> **FROM USER** \
<user>.";

const LISTING_GRAMMAR: &str = "Expected: **SHOW GRANTS ON** \
WORKSPACE|COLLECTION|DATASET <object> (the grants stored on the object), or \
**SHOW EFFECTIVE GRANTS ON** WORKSPACE|COLLECTION|DATASET <object> (those, plus \
the grants above it that cover it). For the session's own grants, use bare \
**SHOW GRANTS**.";

const ROLES: [&str; 3] = ["READER", "WRITER", "OWNER"];
const OBJECT_KINDS: [&str; 3] = ["WORKSPACE", "COLLECTION", "DATASET"];

#[derive(Debug, Serialize)]
pub struct GrantAccess {
    pub role: String,
    pub object_kind: String,
    pub object_name: ValueSlot,
    pub principal: ValueSlot,
}

/// Same fields as `GrantAccess`; a separate type only so the two serialise
/// under their own keys, which is what the planner dispatches on.
#[derive(Debug, Serialize)]
pub struct RevokeAccess {
    pub role: String,
    pub object_kind: String,
    pub object_name: ValueSlot,
    pub principal: ValueSlot,
}

#[derive(Debug, Serialize)]
pub struct ShowGrantsOn {
    pub object_kind: String,
    pub object_name: ValueSlot,
}

/// The listing that includes every policy ABOVE the object that covers it.
/// A separate key rather than a flag, because the planner keys on the name.
#[derive(Debug, Serialize)]
pub struct ShowEffectiveGrantsOn {
    pub object_kind: String,
    pub object_name: ValueSlot,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    for (verb, preposition) in [("GRANT", "TO"), ("REVOKE", "FROM")] {
        if !cursor.peek_word(verb) {
            continue;
        }
        cursor.advance(1);
        let role = keyword_from(cursor, &ROLES, GRANT_GRAMMAR)?;
        cursor.expect_word("ON", GRANT_GRAMMAR)?;
        let object_kind = keyword_from(cursor, &OBJECT_KINDS, GRANT_GRAMMAR)?;

        // GRANT pairs with TO and REVOKE with FROM. The crossed forms are
        // refused by their own message: a statement whose preposition
        // disagrees with its verb was not the statement anyone meant to run,
        // and "expected TO" would not say that.
        let crossed = if verb == "GRANT" { "FROM" } else { "TO" };
        let object_name = read_object(cursor, preposition, crossed, verb)?;

        cursor.expect_word("USER", GRANT_GRAMMAR)?;
        let principal = cursor.value_slot_tail(GRANT_GRAMMAR)?.value;

        let (role, object_kind) = (role, object_kind);
        return Ok(Some(if verb == "GRANT" {
            OpteryxOnly::GrantAccess(GrantAccess {
                role,
                object_kind,
                object_name,
                principal,
            })
        } else {
            OpteryxOnly::RevokeAccess(RevokeAccess {
                role,
                object_kind,
                object_name,
                principal,
            })
        }));
    }

    if cursor.peek_word("SHOW") {
        cursor.advance(1);
        let effective = cursor.take_word("EFFECTIVE");
        if !cursor.take_word("GRANTS") {
            // `SHOW EFFECTIVE <anything else>` IS ours to refuse - sqlparser
            // knows no EFFECTIVE and would point at a token several words away.
            // Anything else rewinds.
            if effective {
                return Err(grammar_error(LISTING_GRAMMAR));
            }
            cursor.seek(start);
            return Ok(None);
        }
        if !cursor.peek_word("ON") {
            // Bare `SHOW GRANTS` is the session's own, and belongs to the
            // parser's catch-all. Only `... ON` is ours.
            if effective {
                return Err(grammar_error(LISTING_GRAMMAR));
            }
            cursor.seek(start);
            return Ok(None);
        }
        cursor.advance(1);
        let object_kind = keyword_from(cursor, &OBJECT_KINDS, LISTING_GRAMMAR)?;
        let object_name = cursor.value_slot_tail(LISTING_GRAMMAR)?.value;
        return Ok(Some(if effective {
            OpteryxOnly::ShowEffectiveGrantsOn(ShowEffectiveGrantsOn {
                object_kind,
                object_name,
            })
        } else {
            OpteryxOnly::ShowGrantsOn(ShowGrantsOn {
                object_kind,
                object_name,
            })
        }));
    }

    Ok(None)
}

/// The object slot, terminated by the verb's own preposition.
///
/// Reads to `preposition`, and if the WRONG one is there instead says so by
/// name rather than reporting a missing keyword.
fn read_object(
    cursor: &mut Cursor,
    preposition: &str,
    crossed: &str,
    verb: &str,
) -> Result<ValueSlot, ParserError> {
    let probe = cursor.index();
    match cursor.value_slot_until(preposition, GRANT_GRAMMAR) {
        Ok(slot) => Ok(slot.value),
        Err(err) => {
            cursor.seek(probe);
            if cursor.value_slot_until(crossed, GRANT_GRAMMAR).is_ok() {
                return Err(grammar_error(&format!(
                    "**GRANT** grants **TO USER** and **REVOKE** revokes **FROM USER** - \
                     '{verb} ... {crossed} USER' mixes the two."
                )));
            }
            cursor.seek(probe);
            Err(err)
        }
    }
}

/// One keyword from a closed set, lowercased for the planner.
///
/// Literal, never a value slot: the role decides which authority the statement
/// confers and the kind decides how many parts its object name has, so a
/// parameter in either would make the statement's own shape a runtime
/// decision.
fn keyword_from(
    cursor: &mut Cursor,
    allowed: &[&str],
    grammar: &str,
) -> Result<String, ParserError> {
    for word in allowed {
        if cursor.take_word(word) {
            return Ok(word.to_lowercase());
        }
    }
    Err(grammar_error(grammar))
}
