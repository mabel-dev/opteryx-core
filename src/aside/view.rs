// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! The materialized-view surface, and `SAVE`.
//!
//! ```text
//! REFRESH MATERIALIZED VIEW <name>
//! ALTER MATERIALIZED VIEW <name> OWNER TO <principal> | CURRENT_USER
//! ALTER MATERIALIZED VIEW <name> SUSPEND | RESUME
//! SAVE RESULTS OF <job> AS <dataset>
//! ```
//!
//! `ALTER MATERIALIZED VIEW` is the one dispatch here that must NOT be greedy:
//! `ALTER` opens several statements this parser does not own (`ALTER TABLE`,
//! `ALTER WORKSPACE`, and `ALTER TABLE ... CREATE TAG`, which belongs to the
//! dialect hook), so the gate is all three words before anything is claimed.
//!
//! `SAVE` is recognised here and planned NOWHERE. The engine has no idea its
//! results are written to a bucket, so it cannot be the thing that copies them.
//! But the jobs API pre-flights every statement through `analyze_query` to
//! authorize it, and a statement the parser rejects cannot be submitted at all.
//! So the parser classifies it and the service that owns the results bucket
//! does the work.

use serde::Serialize;

use sqlparser::ast::ObjectName;
use sqlparser::parser::ParserError;

use super::cursor::{grammar_error, Cursor, ValueSlot};
use super::OpteryxOnly;

const REFRESH_GRAMMAR: &str = "Expected: **REFRESH MATERIALIZED VIEW** <name>. \
It is the only **REFRESH** statement, and it takes no options.";

const SAVE_GRAMMAR: &str = "Expected: **SAVE RESULTS OF** <job> **AS** <dataset>. \
It is the only **SAVE** statement.";

const ALTER_GRAMMAR: &str = "Expected: **ALTER MATERIALIZED VIEW** <name> \
**OWNER TO** <principal>, or **ALTER MATERIALIZED VIEW** <name> \
**SUSPEND**|**RESUME**. Everything else about a view follows from its defining \
SELECT, so change it with **CREATE OR REPLACE MATERIALIZED VIEW**.";

#[derive(Debug, Serialize)]
pub struct RefreshMaterializedView {
    pub name: ObjectName,
}

#[derive(Debug, Serialize)]
pub struct AlterMaterializedViewOwner {
    pub name: ObjectName,
    /// None when the reader wrote the bare keyword: there is no principal to
    /// record, only "whoever runs this". Shaped this way because the planner
    /// already reads it this way - unlike the trigger form beside it, which
    /// keeps the value and flags it.
    pub owner: Option<ValueSlot>,
    pub current_user: bool,
}

#[derive(Debug, Serialize)]
pub struct AlterMaterializedViewSuspended {
    pub name: ObjectName,
    pub suspended: bool,
}

#[derive(Debug, Serialize)]
pub struct SaveResults {
    /// The job whose results are copied. Read as text because a job id is not
    /// identifier-shaped (`Cursor::value_slot_until`), but an identifier slot
    /// in every other sense - it takes no placeholder.
    pub handle: ValueSlot,
    pub name: ObjectName,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.peek_word("REFRESH") {
        cursor.advance(1);
        // Every REFRESH is ours: there is no other. A malformed one is refused
        // by name rather than handed to a parser with no REFRESH statement.
        cursor.expect_word("MATERIALIZED", REFRESH_GRAMMAR)?;
        cursor.expect_word("VIEW", REFRESH_GRAMMAR)?;
        let name = cursor.object_name(REFRESH_GRAMMAR)?;
        end_of_statement(cursor, REFRESH_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::RefreshMaterializedView(
            RefreshMaterializedView { name },
        )));
    }

    if cursor.peek_word("SAVE") {
        cursor.advance(1);
        cursor.expect_word("RESULTS", SAVE_GRAMMAR)?;
        cursor.expect_word("OF", SAVE_GRAMMAR)?;
        let handle = cursor.value_slot_until("AS", SAVE_GRAMMAR)?;
        // Read as text, but an IDENTIFIER slot all the same: the handle names
        // WHOSE results get copied into the caller's own workspace, and a
        // parameterised one would let runtime data make that choice. Every
        // relation-shaped slot in this parser refuses a placeholder for the
        // same reason; this one has to say so explicitly because its shape
        // forced it through the value-slot reader.
        if handle.is_placeholder() {
            return Err(grammar_error(SAVE_GRAMMAR));
        }
        let name = cursor.object_name(SAVE_GRAMMAR)?;
        end_of_statement(cursor, SAVE_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::SaveResults(SaveResults {
            handle: handle.value,
            name,
        })));
    }

    if cursor.peek_word("ALTER") {
        cursor.advance(1);
        // All three words, or nothing: `ALTER TABLE` and `ALTER WORKSPACE` are
        // not ours, and `MATERIALIZED` alone is not a statement.
        if !cursor.take_words(&["MATERIALIZED", "VIEW"]) {
            cursor.seek(start);
            return Ok(None);
        }
        return alter(cursor).map(Some);
    }

    Ok(None)
}

fn alter(cursor: &mut Cursor) -> Result<OpteryxOnly, ParserError> {
    let name = cursor.object_name(ALTER_GRAMMAR)?;

    for (word, suspended) in [("SUSPEND", true), ("RESUME", false)] {
        if cursor.take_word(word) {
            end_of_statement(cursor, ALTER_GRAMMAR)?;
            return Ok(OpteryxOnly::AlterMaterializedViewSuspended(
                AlterMaterializedViewSuspended { name, suspended },
            ));
        }
    }

    if cursor.take_words(&["OWNER", "TO"]) {
        let owner = cursor.value_slot_tail(ALTER_GRAMMAR)?;
        return Ok(OpteryxOnly::AlterMaterializedViewOwner(
            AlterMaterializedViewOwner {
                name,
                owner: if owner.is_current_user {
                    None
                } else {
                    Some(owner.value)
                },
                current_user: owner.is_current_user,
            },
        ));
    }

    Err(grammar_error(ALTER_GRAMMAR))
}

fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&sqlparser::tokenizer::Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}
