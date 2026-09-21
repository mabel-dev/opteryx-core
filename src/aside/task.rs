// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! `CREATE TASK`, `DROP TASK`, `ALTER TASK`.
//!
//! Three flat grammars:
//!
//! ```text
//! CREATE [OR REPLACE] TASK [IF NOT EXISTS] <name> [ON <table>] AS <statement>
//! DROP TASK [IF EXISTS] <name>
//! ALTER TASK <name> AS <statement>
//! ```
//!
//! ## The body is TEXT, not a parsed node
//!
//! `statement` carries the task's body as the reader wrote it, and the planner
//! parses it separately. That looks like a missed opportunity — the tokens are
//! right there — but regenerating the text from a parsed body is MEASURABLY
//! lossy (2026-09-21, see `docs/ASIDE_PARSER_DESIGN.md` §9.1):
//!
//!   - a temporal clause (`FOR TODAY`) is lifted out of the text by the SQL
//!     rewriter BEFORE the parser sees it, so it is not in the AST and a
//!     regenerated body loses it silently;
//!   - `VERSION AS OF PREVIOUS` is rewritten to a sentinel the renderer emits
//!     as `VERSION AS OF 0`, which then refuses to re-parse.
//!
//! A task's body is stored as text by the catalog and re-parsed when it fires,
//! so the text is the artifact that matters. Keeping the reader's own bytes is
//! the only version that cannot quietly differ from what they wrote.
//!
//! The consequence, ruled with it: a `@@name` inside a body is NOT resolved -
//! the body never becomes an AST here, so the rewriter never walks it. The
//! planner refuses one rather than storing text whose meaning depends on who
//! re-parses it later. The task's own NAME and its `ON <table>` are ordinary
//! `ObjectName`s and do resolve.

use serde::Serialize;

use sqlparser::ast::ObjectName;
use sqlparser::dialect::Dialect;
use sqlparser::parser::ParserError;

use super::cursor::{grammar_error, Cursor};
use super::{source_slice, OpteryxOnly};

const CREATE_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **TASK** \
[**IF NOT EXISTS**] <name> [**ON** <table>] **AS** <statement>. A task is a \
statement the platform runs for you; the statement is what it runs.";

const CREATE_BOTH_GUARDS: &str = "**CREATE TASK** cannot combine **OR REPLACE** \
and **IF NOT EXISTS** - the first always redefines, the second only ever no-ops.";

const DROP_GRAMMAR: &str = "Expected: **DROP TASK** [**IF EXISTS**] <name>. \
**DROP TASK** takes no other options - a task owns no storage, so there is \
nothing for CASCADE or RESTRICT to decide.";

const ALTER_GRAMMAR: &str = "Expected: **ALTER TASK** <name> **AS** <statement>. \
This redefines what the task runs and nothing else - it takes no **ON** <table>. \
Who a task runs as, and whether it runs, belong to the trigger that fires it: \
**ALTER TRIGGER** <name> **ON** <table> **OWNER TO** <principal>, or \
**... SUSPEND**|**RESUME**. To repoint or create the trigger too, use \
**CREATE OR REPLACE TASK** <name> **ON** <table> **AS** <statement>.";

#[derive(Debug, Serialize)]
pub struct CreateTask {
    pub name: ObjectName,
    /// The dataset whose commits fire this task, if one was named. Declared,
    /// never derived from the body - see `pre_parse`'s note on why a task
    /// differs from a materialized view here.
    pub table: Option<ObjectName>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    /// The body, as written. See the module note.
    pub statement: String,
}

#[derive(Debug, Serialize)]
pub struct DropTask {
    pub name: ObjectName,
    pub if_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct AlterTask {
    pub name: ObjectName,
    /// The body, as written. See the module note.
    pub statement: String,
}

/// Dispatch: is this one of the three, and if so, parse it.
///
/// The gate is the opening keyword pair, read without consuming anything, so a
/// statement that is not a task statement costs two peeks. Once the pair
/// matches, this statement IS that statement: every later failure is a grammar
/// refusal quoting the form back, never a fallthrough to sqlparser, which knows
/// no TASK at all and would point at the word after it.
pub fn parse(
    cursor: &mut Cursor,
    _dialect: &dyn Dialect,
) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.peek_word("CREATE") {
        cursor.advance(1);
        let or_replace = cursor.take_words(&["OR", "REPLACE"]);
        if !cursor.take_word("TASK") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::CreateTask(create_task(
            cursor, or_replace,
        )?)));
    }

    if cursor.peek_word("DROP") {
        cursor.advance(1);
        if !cursor.take_word("TASK") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::DropTask(drop_task(cursor)?)));
    }

    if cursor.peek_word("ALTER") {
        cursor.advance(1);
        if !cursor.take_word("TASK") {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::AlterTask(alter_task(cursor)?)));
    }

    Ok(None)
}

fn create_task(cursor: &mut Cursor, or_replace: bool) -> Result<CreateTask, ParserError> {
    let if_not_exists = cursor.take_words(&["IF", "NOT", "EXISTS"]);
    if or_replace && if_not_exists {
        return Err(grammar_error(CREATE_BOTH_GUARDS));
    }

    let name = cursor.object_name(CREATE_GRAMMAR)?;
    let table = if cursor.take_word("ON") {
        Some(cursor.object_name(CREATE_GRAMMAR)?)
    } else {
        None
    };
    cursor.expect_word("AS", CREATE_GRAMMAR)?;

    let statement = body(cursor, CREATE_GRAMMAR)?;

    Ok(CreateTask {
        name,
        table,
        or_replace,
        if_not_exists,
        statement,
    })
}

fn drop_task(cursor: &mut Cursor) -> Result<DropTask, ParserError> {
    let if_exists = cursor.take_words(&["IF", "EXISTS"]);
    let name = cursor.object_name(DROP_GRAMMAR)?;
    // Nothing may follow. `DROP TASK t CASCADE` is refused BY NAME here rather
    // than as a stray token, because CASCADE is a thing a reader can reasonably
    // expect to work and the answer is "there is nothing for it to decide".
    if !cursor.at_end() && !cursor.peek_is(&sqlparser::tokenizer::Token::SemiColon) {
        return Err(grammar_error(DROP_GRAMMAR));
    }
    Ok(DropTask { name, if_exists })
}

fn alter_task(cursor: &mut Cursor) -> Result<AlterTask, ParserError> {
    let name = cursor.object_name(ALTER_GRAMMAR)?;
    // `ALTER TASK t ON s AS ...` is refused by the same message that explains
    // where ON belongs, rather than by a bare "expected AS".
    cursor.expect_word("AS", ALTER_GRAMMAR)?;
    let statement = body(cursor, ALTER_GRAMMAR)?;
    Ok(AlterTask { name, statement })
}

/// Everything from here to the end of the statement, as source text.
fn body(cursor: &mut Cursor, grammar: &str) -> Result<String, ParserError> {
    let sql = cursor.sql();
    let tokens = cursor.statement_tail();
    if tokens.is_empty() {
        return Err(grammar_error(grammar));
    }
    Ok(source_slice(sql, tokens))
}
