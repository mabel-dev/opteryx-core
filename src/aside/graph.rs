// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! Property graphs (SQL/PGQ, ISO/IEC 9075-16): `CREATE` and `DROP`.
//! `SHOW CREATE PROPERTY GRAPH` is admin.rs's, beside the other `SHOW CREATE`s.
//!
//! ```text
//! CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] <name>
//!   VERTEX TABLES ( <element> [, ...] )
//!   [EDGE TABLES ( <element> [, ...] )]
//!
//! <element>  ::= <relation> [AS <name>] KEY ( <col> [, ...] )
//!                [SOURCE <endpoint> DESTINATION <endpoint>]      -- edges only
//!                [LABEL <label>] [<properties>]
//! <endpoint> ::= KEY ( <col> [, ...] ) REFERENCES <vertex> ( <col> [, ...] )
//!              | <vertex>                                        -- from a FOREIGN KEY
//! <properties> ::= PROPERTIES ( <col> [AS <name>] [, ...] )
//!                | PROPERTIES ALL COLUMNS | NO PROPERTIES
//!
//! DROP PROPERTY GRAPH [IF EXISTS] <name>
//! ```
//!
//! A deliberate subset of SQL/PGQ's DDL (docs/PROPERTY_GRAPH_DESIGN.md §1). The
//! parts it leaves out are refused here BY NAME - `DEFAULT LABEL`, a second
//! `LABEL`, expression properties, a missing `KEY` - rather than reaching
//! sqlparser, which has no PGQ grammar and would point at the word after
//! `PROPERTY`. Whether the tables, columns and foreign keys exist is the
//! planner's question, not the grammar's.

use serde::Serialize;

use sqlparser::ast::{Ident, ObjectName};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use super::cursor::{grammar_error, Cursor};
use super::OpteryxOnly;

const CREATE_GRAMMAR: &str = "Expected: **CREATE** [**OR REPLACE**] **PROPERTY GRAPH** \
[**IF NOT EXISTS**] <name> **VERTEX TABLES** (<table> [**AS** <name>] **KEY** (<column>, ...) \
[**LABEL** <label>] [**PROPERTIES** (<column> [**AS** <name>], ...) | **PROPERTIES ALL COLUMNS** \
| **NO PROPERTIES**], ...) [**EDGE TABLES** (<table> [**AS** <name>] **KEY** (<column>, ...) \
**SOURCE KEY** (<column>, ...) **REFERENCES** <vertex table> (<column>, ...) \
**DESTINATION KEY** (<column>, ...) **REFERENCES** <vertex table> (<column>, ...) \
[**LABEL** <label>] [<properties>], ...)].";

const CREATE_BOTH_GUARDS: &str = "**CREATE PROPERTY GRAPH** cannot combine **OR REPLACE** \
and **IF NOT EXISTS** - the first always redefines, the second only ever no-ops.";

const KEY_REQUIRED: &str = "Every element table of a property graph needs **KEY** \
(<column>, ...). SQL/PGQ would default it to the table's primary key, and Opteryx tables \
have none.";

const DEFAULT_LABEL: &str = "**DEFAULT LABEL** is not supported. Write **LABEL** <label>, \
or leave the label out and the element table's name is used.";

const ONE_LABEL: &str = "An element table takes one **LABEL** - more than one is not \
supported. Use a separate element table (**AS** <name>) for each label.";

const COLUMN_PROPERTIES: &str = "A property is a column, optionally renamed: \
<column> [**AS** <name>]. Expressions as properties are not supported.";

const DROP_GRAMMAR: &str = "Expected: **DROP PROPERTY GRAPH** [**IF EXISTS**] <name>. \
A property graph owns no data, so there is nothing for CASCADE or RESTRICT to decide.";

#[derive(Debug, Serialize)]
pub struct CreatePropertyGraph {
    pub name: ObjectName,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub vertex_tables: Vec<GraphElement>,
    pub edge_tables: Vec<GraphElement>,
}

#[derive(Debug, Serialize)]
pub struct GraphElement {
    pub relation: ObjectName,
    /// The element's name: `AS <name>`, or absent for the relation's last part.
    pub alias: Option<Ident>,
    pub key: Vec<Ident>,
    /// Edges only.
    pub source: Option<GraphEndpoint>,
    pub destination: Option<GraphEndpoint>,
    /// Absent means the element's name, as SQL/PGQ defines.
    pub label: Option<Ident>,
    pub properties: GraphProperties,
}

#[derive(Debug, Serialize)]
pub struct GraphEndpoint {
    /// The edge table's columns; absent when a FOREIGN KEY is to supply them.
    pub key: Option<Vec<Ident>>,
    pub vertex: Ident,
    /// The vertex table's columns; absent with `key`.
    pub references: Option<Vec<Ident>>,
}

#[derive(Debug, Serialize)]
pub enum GraphProperties {
    /// No properties clause: SQL/PGQ reads that as every column.
    Unspecified,
    AllColumns,
    NoProperties,
    Listed(Vec<GraphProperty>),
}

#[derive(Debug, Serialize)]
pub struct GraphProperty {
    pub column: Ident,
    pub alias: Option<Ident>,
}

#[derive(Debug, Serialize)]
pub struct DropPropertyGraph {
    pub name: ObjectName,
    pub if_exists: bool,
}

/// Claimed on `CREATE [OR REPLACE] PROPERTY GRAPH` and `DROP PROPERTY GRAPH`;
/// anything else rewinds. Once claimed, every failure is a refusal quoting the
/// grammar.
pub fn parse(cursor: &mut Cursor) -> Result<Option<OpteryxOnly>, ParserError> {
    let start = cursor.index();

    if cursor.take_word("CREATE") {
        let or_replace = cursor.take_words(&["OR", "REPLACE"]);
        if !cursor.take_words(&["PROPERTY", "GRAPH"]) {
            cursor.seek(start);
            return Ok(None);
        }
        return Ok(Some(OpteryxOnly::CreatePropertyGraph(create(cursor, or_replace)?)));
    }

    if cursor.take_word("DROP") {
        if !cursor.take_words(&["PROPERTY", "GRAPH"]) {
            cursor.seek(start);
            return Ok(None);
        }
        let if_exists = cursor.take_words(&["IF", "EXISTS"]);
        let name = cursor.object_name(DROP_GRAMMAR)?;
        end_of_statement(cursor, DROP_GRAMMAR)?;
        return Ok(Some(OpteryxOnly::DropPropertyGraph(DropPropertyGraph {
            name,
            if_exists,
        })));
    }

    Ok(None)
}

fn create(cursor: &mut Cursor, or_replace: bool) -> Result<CreatePropertyGraph, ParserError> {
    let if_not_exists = cursor.take_words(&["IF", "NOT", "EXISTS"]);
    if or_replace && if_not_exists {
        return Err(grammar_error(CREATE_BOTH_GUARDS));
    }
    let name = cursor.object_name(CREATE_GRAMMAR)?;

    cursor.expect_word("VERTEX", CREATE_GRAMMAR)?;
    cursor.expect_word("TABLES", CREATE_GRAMMAR)?;
    let vertex_tables = element_list(cursor, false)?;

    let mut edge_tables = Vec::new();
    if cursor.take_word("EDGE") {
        cursor.expect_word("TABLES", CREATE_GRAMMAR)?;
        edge_tables = element_list(cursor, true)?;
    }

    end_of_statement(cursor, CREATE_GRAMMAR)?;
    Ok(CreatePropertyGraph {
        name,
        or_replace,
        if_not_exists,
        vertex_tables,
        edge_tables,
    })
}

fn element_list(cursor: &mut Cursor, edges: bool) -> Result<Vec<GraphElement>, ParserError> {
    expect_token(cursor, &Token::LParen)?;
    let mut elements = vec![element(cursor, edges)?];
    while cursor.take_token(&Token::Comma) {
        elements.push(element(cursor, edges)?);
    }
    expect_token(cursor, &Token::RParen)?;
    Ok(elements)
}

fn element(cursor: &mut Cursor, edge: bool) -> Result<GraphElement, ParserError> {
    let relation = cursor.object_name(CREATE_GRAMMAR)?;
    let alias = if cursor.take_word("AS") {
        Some(cursor.identifier(CREATE_GRAMMAR)?)
    } else {
        None
    };
    if !cursor.take_word("KEY") {
        return Err(grammar_error(KEY_REQUIRED));
    }
    let key = column_list(cursor)?;

    let (source, destination) = if edge {
        cursor.expect_word("SOURCE", CREATE_GRAMMAR)?;
        let source = endpoint(cursor)?;
        cursor.expect_word("DESTINATION", CREATE_GRAMMAR)?;
        (Some(source), Some(endpoint(cursor)?))
    } else {
        (None, None)
    };

    if cursor.peek_word("DEFAULT") {
        return Err(grammar_error(DEFAULT_LABEL));
    }
    let label = if cursor.take_word("LABEL") {
        Some(cursor.identifier(CREATE_GRAMMAR)?)
    } else {
        None
    };
    let properties = properties(cursor)?;
    if cursor.peek_word("LABEL") || cursor.peek_word("DEFAULT") {
        return Err(grammar_error(ONE_LABEL));
    }

    Ok(GraphElement {
        relation,
        alias,
        key,
        source,
        destination,
        label,
        properties,
    })
}

fn endpoint(cursor: &mut Cursor) -> Result<GraphEndpoint, ParserError> {
    if cursor.take_word("KEY") {
        let key = column_list(cursor)?;
        cursor.expect_word("REFERENCES", CREATE_GRAMMAR)?;
        let vertex = cursor.identifier(CREATE_GRAMMAR)?;
        let references = column_list(cursor)?;
        return Ok(GraphEndpoint {
            key: Some(key),
            vertex,
            references: Some(references),
        });
    }
    Ok(GraphEndpoint {
        key: None,
        vertex: cursor.identifier(CREATE_GRAMMAR)?,
        references: None,
    })
}

fn properties(cursor: &mut Cursor) -> Result<GraphProperties, ParserError> {
    if cursor.take_words(&["NO", "PROPERTIES"]) {
        return Ok(GraphProperties::NoProperties);
    }
    if !cursor.take_word("PROPERTIES") {
        return Ok(GraphProperties::Unspecified);
    }
    if cursor.take_words(&["ALL", "COLUMNS"]) {
        return Ok(GraphProperties::AllColumns);
    }
    expect_token(cursor, &Token::LParen)?;
    let mut listed = Vec::new();
    loop {
        let column = cursor.identifier(COLUMN_PROPERTIES)?;
        let alias = if cursor.take_word("AS") {
            Some(cursor.identifier(COLUMN_PROPERTIES)?)
        } else {
            None
        };
        listed.push(GraphProperty { column, alias });
        if !cursor.take_token(&Token::Comma) {
            break;
        }
    }
    if !cursor.take_token(&Token::RParen) {
        return Err(grammar_error(COLUMN_PROPERTIES));
    }
    Ok(GraphProperties::Listed(listed))
}

fn column_list(cursor: &mut Cursor) -> Result<Vec<Ident>, ParserError> {
    expect_token(cursor, &Token::LParen)?;
    let mut columns = vec![cursor.identifier(CREATE_GRAMMAR)?];
    while cursor.take_token(&Token::Comma) {
        columns.push(cursor.identifier(CREATE_GRAMMAR)?);
    }
    expect_token(cursor, &Token::RParen)?;
    Ok(columns)
}

fn expect_token(cursor: &mut Cursor, token: &Token) -> Result<(), ParserError> {
    if cursor.take_token(token) {
        Ok(())
    } else {
        Err(grammar_error(CREATE_GRAMMAR))
    }
}

fn end_of_statement(cursor: &Cursor, grammar: &str) -> Result<(), ParserError> {
    if cursor.at_end() || cursor.peek_is(&Token::SemiColon) {
        Ok(())
    } else {
        Err(grammar_error(grammar))
    }
}
