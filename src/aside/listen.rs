// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Notification subscriptions.
//!
//! ```text
//! LISTEN TO <task> [FOR ERROR | SUCCESS | EVERYTHING]
//! UNLISTEN <task>
//! SHOW LISTENERS
//! ```
//!
//! ## Why not sqlparser's LISTEN
//!
//! sqlparser HAS `LISTEN`/`UNLISTEN`/`NOTIFY`, behind the one-line
//! `Dialect::supports_listen_notify()` flag. Turning that flag on would be the
//! cheapest possible change and it would be wrong three times over, which is
//! why this production exists instead:
//!
//!   - its grammar is Postgres's `LISTEN <channel>` — a bare identifier, no
//!     `TO`, no `FOR` clause;
//!   - its AST reports `Span::empty()`, forfeiting the position every error
//!     this engine raises carries;
//!   - its SEMANTICS are a session-scoped channel subscription. These
//!     subscriptions are durable, owned by a user, and fire when nobody is
//!     connected.
//!
//! That argument is not new — it is the `pre_parse` comment this module
//! replaces, and it cuts FOR writing our own production rather than against
//! it. The span objection in particular applies to upstream's parser, not to a
//! grammar we control: every `Ident` below carries one.
//!
//! `SHOW LISTENERS` takes no arguments, deliberately: it lists the tasks YOU
//! listen to, and there is no form that lists anyone else's.
//!
//! `UNLISTEN` has no wildcard. A statement that silently empties every
//! subscription a user holds is not something to be one keystroke away from
//! `UNLISTEN t`.

use serde::Serialize;

use sqlparser::ast::ObjectName;
use sqlparser::parser::ParserError;

use super::cursor::{grammar_error, Cursor};
use super::OpteryxOnly;

const LISTEN_GRAMMAR: &str = "Expected: **LISTEN TO** <task> \
[**FOR ERROR**|**SUCCESS**|**EVERYTHING**]. A subscription is to a TASK - not \
to a trigger or a table - and without **FOR** it covers every outcome.";

const UNLISTEN_GRAMMAR: &str = "Expected: **UNLISTEN** <task>. The task is \
named - there is no wildcard form that drops every subscription at once. \
**UNLISTEN** takes no **FOR** clause either: a subscription is removed whole, \
and changing which outcomes you hear about is **UNLISTEN** then **LISTEN TO**.";

const SHOW_GRAMMAR: &str = "Expected: **SHOW LISTENERS**, which takes no \
arguments. It lists the tasks YOU listen to; there is no form that lists \
anyone else's subscriptions.";

#[derive(Debug, Serialize)]
pub struct Listen {
    pub name: ObjectName,
    /// One of ERROR / SUCCESS / EVERYTHING. A missing `FOR` clause is resolved
    /// to EVERYTHING here rather than left null, so one spelling reaches the
    /// catalog.
    pub outcome: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Unlisten {
    pub name: ObjectName,
}

/// Serialises as `{}` — the statement carries nothing, because it answers for
/// the session and takes no arguments.
#[derive(Debug, Serialize)]
pub struct ShowListeners {}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.take_word("LISTEN") {
        cursor.expect_word("TO", LISTEN_GRAMMAR)?;
        let name = cursor.object_name(LISTEN_GRAMMAR)?;
        let outcome = if cursor.take_word("FOR") {
            if cursor.take_word("ERROR") {
                "ERROR"
            } else if cursor.take_word("SUCCESS") {
                "SUCCESS"
            } else if cursor.take_word("EVERYTHING") {
                "EVERYTHING"
            } else {
                return Err(grammar_error(LISTEN_GRAMMAR));
            }
        } else {
            "EVERYTHING"
        };
        end_of_statement(cursor, LISTEN_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::Listen(Listen { name, outcome })));
    }

    if cursor.take_word("UNLISTEN") {
        let name = cursor.object_name(UNLISTEN_GRAMMAR)?;
        end_of_statement(cursor, UNLISTEN_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::Unlisten(Unlisten { name })));
    }

    if cursor.peek_word("SHOW") {
        cursor.advance(1);
        // Only this one SHOW. `SHOW COLUMNS` and `SHOW CREATE TABLE` are
        // sqlparser's, `SHOW CREATE TASK` is still pre-parse's, and every
        // unrecognised `SHOW <words>` is the parser's own catch-all - so a
        // second word that is not LISTENERS rewinds and claims nothing.
        if !cursor.take_word("LISTENERS") {
            cursor.seek(start);
            return Ok(None);
        }
        end_of_statement(cursor, SHOW_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ShowListeners(ShowListeners {})));
    }

    Ok(None)
}

fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&sqlparser::tokenizer::Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}
