// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! The remainder: statistics, the egress exemption, `SHOW CREATE` for the
//! object types sqlparser cannot spell, and fork maintenance.
//!
//! ```text
//! DROP STATISTICS ON <table> [FOR COLUMNS <c1>, <c2>, ...]
//! ALTER WORKSPACE <source> SET SECURE <object> TO <workspace>[, <workspace>...]
//! ALTER WORKSPACE <source> DROP SECURE <object>
//! SHOW CREATE MATERIALIZED VIEW | TASK <name>
//! SHOW CREATE TRIGGER <name> ON <table>
//! ALTER TABLE <fork> RESYNC [FORCE] | DETACH
//! ```
//!
//! Four unrelated statements in one module because each is a handful of lines
//! and none has a family to join. What they share is that all four must decide
//! NOT to claim a statement, and three of them have to read some distance to
//! know:
//!
//!   `SHOW CREATE TABLE`/`VIEW` are sqlparser's and parse natively - only the
//!   three object types its `ShowCreateObject` enum cannot spell are ours.
//!
//!   `ALTER TABLE ... RESYNC|DETACH` shares its first three tokens with every
//!   other `ALTER TABLE`, so the action word is only visible PAST the table
//!   name. The cursor reads the name, looks, and rewinds if it is not ours -
//!   which is exactly what the regex layer needed a second lookahead pattern
//!   for, and got subtly wrong once (`RESYNC FORCE` does not end in `RESYNC`,
//!   so a tail test handed it back to the parser).
//!
//!   `ALTER WORKSPACE ... SET|DROP SECURE` is reached only when the SQL
//!   rewriter has NOT already turned the statement into `ALTER FUNCTION` - it
//!   looks ahead for SECURE and leaves these alone. The property forms never
//!   arrive here.

use serde::Serialize;

use sqlparser::ast::{Ident, ObjectName};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use super::cursor::{grammar_error, Cursor};
use super::OpteryxOnly;

const STATS_GRAMMAR: &str = "Expected: DROP STATISTICS ON <table> \
[FOR COLUMNS <col>, ...]";

const SECURE_GRAMMAR: &str = "Expected: **ALTER WORKSPACE** <source> \
**SET SECURE** <object> **TO** <workspace>[, <workspace>...], or **ALTER \
WORKSPACE** <source> **DROP SECURE** <object>. <source> is the workspace whose \
data the object copies out; <object> is the fully-qualified task or \
materialized view doing the copying; the destinations are workspace names, not \
relations.";

const SHOW_CREATE_GRAMMAR: &str = "Expected: **SHOW CREATE MATERIALIZED VIEW** \
<name> or **SHOW CREATE TASK** <name>. The statement takes one object name and \
nothing else.";

const SHOW_CREATE_TRIGGER_GRAMMAR: &str = "Expected: **SHOW CREATE TRIGGER** \
<name> **ON** <table>. A trigger name is only unique per table, so the table \
must be named.";

const FORK_GRAMMAR: &str = "Expected: **ALTER TABLE** <dataset> **RESYNC** \
[**FORCE**], or **ALTER TABLE** <dataset> **DETACH**.";

#[derive(Debug, Serialize)]
pub struct DropStatistics {
    pub table_name: ObjectName,
    /// Empty means every column's statistics.
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct AlterWorkspaceSecure {
    pub workspace: Ident,
    pub object: ObjectName,
    /// `None` is DROP SECURE — withdraw the sanction, rather than grant it to
    /// nobody.
    pub destinations: Option<Vec<Ident>>,
}

/// Deliberately the SAME key sqlparser uses for `SHOW CREATE TABLE`/`VIEW`, so
/// the planner has one path for all five object types rather than one per
/// spelling. `trigger_name` is absent on the other forms.
#[derive(Debug, Serialize)]
pub struct ShowCreate {
    pub obj_type: &'static str,
    /// The object, or for a trigger the HOLDER it hangs off — the convention
    /// `ALTER`/`DROP TRIGGER` use, so connector resolution is the same.
    pub obj_name: ObjectName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_name: Option<Ident>,
}

#[derive(Debug, Serialize)]
pub struct ResyncRelation {
    pub relation: ObjectName,
    pub force: bool,
}

#[derive(Debug, Serialize)]
pub struct DetachRelation {
    pub relation: ObjectName,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.peek_word("DROP") {
        cursor.advance(1);
        if !cursor.take_word("STATISTICS") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::DropStatistics(drop_statistics(cursor)?)));
    }

    if cursor.peek_word("SHOW") {
        cursor.advance(1);
        if !cursor.take_word("CREATE") {
            cursor.seek(start);
            return Ok(None);
        }
        return match show_create(cursor)? {
            Some(statement) => Ok(Some(statement)),
            None => {
                cursor.seek(start);
                Ok(None)
            }
        };
    }

    if cursor.peek_word("ALTER") {
        cursor.advance(1);
        if cursor.take_word("WORKSPACE") {
            return match workspace_secure(cursor)? {
                Some(statement) => Ok(Some(statement)),
                None => {
                    cursor.seek(start);
                    Ok(None)
                }
            };
        }
        if cursor.take_word("TABLE") {
            return match fork(cursor)? {
                Some(statement) => Ok(Some(statement)),
                None => {
                    cursor.seek(start);
                    Ok(None)
                }
            };
        }
        cursor.seek(start);
        return Ok(None);
    }

    Ok(None)
}

fn drop_statistics(cursor: &mut Cursor) -> Result<DropStatistics, ParserError> {
    cursor.expect_word("ON", STATS_GRAMMAR)?;
    let table_name = cursor.object_name(STATS_GRAMMAR)?;
    let mut columns: Vec<String> = Vec::new();
    if cursor.take_word("FOR") {
        cursor.expect_word("COLUMNS", STATS_GRAMMAR)?;
        loop {
            // The VALUE, not the spelling: a quoted column arrives unquoted,
            // as it did when the regex stripped the quote characters by hand.
            columns.push(cursor.identifier(STATS_GRAMMAR)?.value);
            if !cursor.take_token(&Token::Comma) {
                break;
            }
        }
    }
    end_of_statement(cursor, STATS_GRAMMAR)?;
    Ok(DropStatistics {
        table_name,
        columns,
    })
}

/// `None` when this is some other `ALTER WORKSPACE` — the property forms, which
/// the SQL rewriter has already turned into `ALTER FUNCTION` by now, and
/// anything else that should reach the parser.
fn workspace_secure(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let workspace = match cursor.peek() {
        Some(_) => cursor.identifier(SECURE_GRAMMAR)?,
        None => return Ok(None),
    };
    let dropping = if cursor.take_word("SET") {
        false
    } else if cursor.take_word("DROP") {
        true
    } else {
        return Ok(None);
    };
    if !cursor.take_word("SECURE") {
        return Ok(None);
    }

    let object = cursor.object_name(SECURE_GRAMMAR)?;
    if dropping {
        end_of_statement(cursor, SECURE_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::AlterWorkspaceSecure(
            AlterWorkspaceSecure {
                workspace,
                object,
                destinations: None,
            },
        )));
    }

    cursor.expect_word("TO", SECURE_GRAMMAR)?;
    let mut destinations = Vec::new();
    loop {
        destinations.push(cursor.identifier(SECURE_GRAMMAR)?);
        if !cursor.take_token(&Token::Comma) {
            break;
        }
    }
    end_of_statement(cursor, SECURE_GRAMMAR)?;
    Ok(Some(OpteryxOnly::AlterWorkspaceSecure(
        AlterWorkspaceSecure {
            workspace,
            object,
            destinations: Some(destinations),
        },
    )))
}

/// `None` for `SHOW CREATE TABLE`/`VIEW`, which sqlparser parses natively. The
/// fewer spellings that come through here, the fewer places the grammar lives.
fn show_create(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    if cursor.take_words(&["MATERIALIZED", "VIEW"]) {
        let obj_name = cursor.object_name(SHOW_CREATE_GRAMMAR)?;
        end_of_statement(cursor, SHOW_CREATE_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ShowCreate(ShowCreate {
            obj_type: "MaterializedView",
            obj_name,
            trigger_name: None,
        })));
    }

    if cursor.take_word("TASK") {
        let obj_name = cursor.object_name(SHOW_CREATE_GRAMMAR)?;
        end_of_statement(cursor, SHOW_CREATE_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ShowCreate(ShowCreate {
            obj_type: "Task",
            obj_name,
            trigger_name: None,
        })));
    }

    if cursor.take_word("TRIGGER") {
        // sqlparser's `ShowCreateObject` DOES list TRIGGER, and it is still
        // ours: upstream has no per-object `ON <table>` clause to go with it
        // and rejects the suffix, while a trigger name here is unique only per
        // holder.
        let trigger_name = cursor.identifier(SHOW_CREATE_TRIGGER_GRAMMAR)?;
        cursor.expect_word("ON", SHOW_CREATE_TRIGGER_GRAMMAR)?;
        let obj_name = cursor.object_name(SHOW_CREATE_TRIGGER_GRAMMAR)?;
        end_of_statement(cursor, SHOW_CREATE_TRIGGER_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ShowCreate(ShowCreate {
            obj_type: "Trigger",
            obj_name,
            trigger_name: Some(trigger_name),
        })));
    }

    Ok(None)
}

/// `None` for every other `ALTER TABLE` — which is most of them.
///
/// The action word is only visible past the table name, so this reads the name
/// before it can tell. Cheap, and the alternative is the second lookahead
/// pattern the regex layer needed.
fn fork(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let probe = cursor.index();
    let relation = match cursor.object_name(FORK_GRAMMAR) {
        Ok(name) => name,
        Err(_) => {
            cursor.seek(probe);
            return Ok(None);
        }
    };

    if cursor.take_word("DETACH") {
        end_of_statement(cursor, FORK_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::DetachRelation(DetachRelation {
            relation,
        })));
    }

    if cursor.take_word("RESYNC") {
        let force = cursor.take_word("FORCE");
        end_of_statement(cursor, FORK_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::ResyncRelation(ResyncRelation {
            relation,
            force,
        })));
    }

    cursor.seek(probe);
    Ok(None)
}

fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}
