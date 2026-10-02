// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Vector-index DDL sqlparser has no grammar for (docs/VECTOR_INDEX_DESIGN.md).
//!
//! ```text
//! ALTER INDEX <name> ON <relation> SET (BUILD = 'sync' | 'async')
//! ```
//!
//! `CREATE INDEX ... USING IVF (col) WITH (...)` and `DROP INDEX <name> ON <relation>`
//! are sqlparser's own statements (the dialect enables CREATE INDEX's WITH clause).
//! sqlparser's ALTER INDEX knows only `RENAME`, so the gate here is
//! `ALTER INDEX <name> ON` — anything else rewinds and is sqlparser's to accept or refuse.
//!
//! The build mode is the only alterable property (ruled 2026-10-02): everything else
//! about an index changes what it contains, which is a DROP and CREATE.

use serde::Serialize;

use sqlparser::ast::{Ident, ObjectName};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use super::cursor::{grammar_error, Cursor};
use super::OpteryxOnly;

const ALTER_GRAMMAR: &str = "Expected: **ALTER INDEX** <name> **ON** <relation> \
**SET** (**BUILD** = 'sync' | 'async'). The build mode is the only property of an index \
that can be altered; to change anything else, drop and re-create it.";

#[derive(Debug, Serialize)]
pub struct AlterIndexBuild {
    pub name: Ident,
    pub relation: ObjectName,
    pub build: String,
}

pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();
    if !cursor.take_words(&["ALTER", "INDEX"]) {
        return Ok(None);
    }
    // `ALTER INDEX <name> ON` or nothing: `ALTER INDEX a RENAME TO b` is sqlparser's.
    let name = match cursor.identifier(ALTER_GRAMMAR) {
        Ok(name) if cursor.peek_word("ON") => name,
        _ => {
            cursor.seek(start);
            return Ok(None);
        }
    };
    cursor.expect_word("ON", ALTER_GRAMMAR)?;
    let relation = cursor.object_name(ALTER_GRAMMAR)?;
    cursor.expect_word("SET", ALTER_GRAMMAR)?;
    if !cursor.take_token(&Token::LParen) {
        return Err(grammar_error(ALTER_GRAMMAR));
    }
    cursor.expect_word("BUILD", ALTER_GRAMMAR)?;
    if !cursor.take_token(&Token::Eq) {
        return Err(grammar_error(ALTER_GRAMMAR));
    }
    let build = cursor.string_literal(ALTER_GRAMMAR)?.to_lowercase();
    if build != "sync" && build != "async" {
        return Err(grammar_error(ALTER_GRAMMAR));
    }
    if !cursor.take_token(&Token::RParen) {
        return Err(grammar_error(ALTER_GRAMMAR));
    }
    if !(cursor.at_end() || cursor.peek_is(&Token::SemiColon)) {
        return Err(grammar_error(ALTER_GRAMMAR));
    }
    Ok(Some(OpteryxOnly::AlterIndexBuild(AlterIndexBuild { name, relation, build })))
}
