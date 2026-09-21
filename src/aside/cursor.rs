// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

//! A read cursor over a token vector, with the slot readers the flat grammars
//! need. This is the whole of the "state machine" apparatus: a position, a
//! handful of peek/take operations, and one reader per kind of slot.
//!
//! Deliberately NOT `sqlparser::Parser`. The grammars here are regular, so they
//! need no backtracking, no precedence climbing and no expression parsing —
//! and borrowing the parser for them would bind us to a far wider public API
//! surface than the three entry points this module actually uses.

use serde::Serialize;

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

/// Marks a refusal as a GRAMMAR refusal rather than a parse failure.
///
/// A statement opening with `CREATE TASK` is a `CREATE TASK`; if the rest of it
/// does not fit, the reader needs the grammar quoted back, not sqlparser's
/// "expected: X, found: Y" pointing at a token several words away. Python
/// re-types anything carrying this prefix as `UnsupportedSyntaxError` and
/// strips it — see `opteryx.planner.parse_statement`. The message after the
/// prefix is markdown, as every Opteryx error message is.
pub const GRAMMAR_ERROR_PREFIX: &str = "OPTERYX-SYNTAX: ";

pub struct Cursor<'a> {
    tokens: &'a [TokenWithSpan],
    sql: &'a str,
    index: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(tokens: &'a [TokenWithSpan], sql: &'a str) -> Self {
        Cursor {
            tokens,
            sql,
            index: 0,
        }
    }

    pub fn sql(&self) -> &'a str {
        self.sql
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn seek(&mut self, index: usize) {
        self.index = index;
    }

    pub fn advance(&mut self, by: usize) {
        self.index = (self.index + by).min(self.tokens.len());
    }

    pub fn remaining(&self) -> &'a [TokenWithSpan] {
        &self.tokens[self.index.min(self.tokens.len())..]
    }

    /// The tokenizer emits no EOF token, so "at end" is the index running out.
    /// A trailing `EOF` token is tolerated in case that ever changes.
    pub fn at_end(&self) -> bool {
        match self.tokens.get(self.index) {
            None => true,
            Some(t) => t.token == Token::EOF,
        }
    }

    pub fn peek(&self) -> Option<&'a TokenWithSpan> {
        self.tokens.get(self.index)
    }

    pub fn peek_is(&self, token: &Token) -> bool {
        matches!(self.peek(), Some(t) if &t.token == token)
    }

    pub fn take_token(&mut self, token: &Token) -> bool {
        if self.peek_is(token) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    /// Whether the next token is the bare word `word`, ignoring case.
    ///
    /// Matched on the word's TEXT rather than through `Keyword`, because six of
    /// the words these grammars need (`SIGNAL`, `FORK`, `EFFECTIVE`,
    /// `LISTENERS`, `SAVE`, `SCHEDULE`) are not in sqlparser's `Keyword` enum
    /// at all, and adding them would mean forking. Matching on text also keeps
    /// every one of them NON-RESERVED, which is the behaviour we want: a column
    /// called `task` or `fork` has to keep working.
    ///
    /// A quoted word is not a keyword: `` `TASK` `` is an identifier the reader
    /// deliberately escaped, so `quote_style` must be absent to match.
    pub fn peek_word(&self, word: &str) -> bool {
        matches!(
            self.peek().map(|t| &t.token),
            Some(Token::Word(Word { value, quote_style: None, .. })) if value.eq_ignore_ascii_case(word)
        )
    }

    pub fn take_word(&mut self, word: &str) -> bool {
        if self.peek_word(word) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    /// Consume `words` in order, or consume nothing at all.
    ///
    /// All-or-nothing so an optional multi-word clause (`IF NOT EXISTS`) cannot
    /// half-match and leave the cursor stranded mid-clause.
    pub fn take_words(&mut self, words: &[&str]) -> bool {
        let start = self.index;
        for word in words {
            if !self.take_word(word) {
                self.index = start;
                return false;
            }
        }
        true
    }

    pub fn expect_word(&mut self, word: &str, grammar: &str) -> Result<(), ParserError> {
        if self.take_word(word) {
            Ok(())
        } else {
            Err(grammar_error(grammar))
        }
    }

    /// A dotted object name: `Word ('.' Word)*`.
    ///
    /// Quoting and the `@` in `@@external_user` are the tokenizer's business,
    /// already settled by `OpteryxDialect::is_identifier_start` - which is why a
    /// name part here can be a backtick-quoted `` `my-task` `` or a `@@name`
    /// that the AST rewriter resolves later, both of which the regex this
    /// replaces rejected.
    ///
    /// Refuses what `Parser::parse_object_name` refuses: an empty part, and a
    /// trailing dot.
    pub fn object_name(&mut self, grammar: &str) -> Result<ObjectName, ParserError> {
        let mut parts: Vec<ObjectNamePart> = Vec::new();
        loop {
            let ident = match self.peek().map(|t| (&t.token, t.span)) {
                Some((Token::Word(word), span)) => Ident {
                    value: word.value.clone(),
                    quote_style: word.quote_style,
                    span,
                },
                _ => return Err(grammar_error(grammar)),
            };
            self.index += 1;
            parts.push(ObjectNamePart::Identifier(ident));
            if !self.take_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(parts))
    }

    /// Every token from here to the end of this statement — the next top-level
    /// semicolon, or the end of input.
    ///
    /// Top-level is by token, not by text, so a semicolon inside a string
    /// literal is a `SingleQuotedString` token and does not end anything.
    pub fn statement_tail(&mut self) -> &'a [TokenWithSpan] {
        let start = self.index;
        while !self.at_end() && !self.peek_is(&Token::SemiColon) {
            self.index += 1;
        }
        &self.tokens[start..self.index]
    }

    /// Exactly one identifier part, refusing a dotted name.
    ///
    /// Some names are deliberately NOT dotted: a trigger's name is unique only
    /// within the holder it hangs off, so `ALTER TRIGGER a.b ON t` is a reader
    /// mistaking a trigger for a relation, and saying so beats silently
    /// addressing something else.
    pub fn identifier(&mut self, grammar: &str) -> Result<Ident, ParserError> {
        let ident = match self.peek().map(|t| (&t.token, t.span)) {
            Some((Token::Word(word), span)) => Ident {
                value: word.value.clone(),
                quote_style: word.quote_style,
                span,
            },
            _ => return Err(grammar_error(grammar)),
        };
        self.index += 1;
        if self.peek_is(&Token::Period) {
            return Err(grammar_error(grammar));
        }
        Ok(ident)
    }

    /// A single-quoted string. The tokenizer has already collapsed doubled
    /// quotes, so what comes back is the value, not the literal.
    pub fn string_literal(&mut self, grammar: &str) -> Result<String, ParserError> {
        match self.peek().map(|t| &t.token) {
            Some(Token::SingleQuotedString(value)) => {
                let value = value.clone();
                self.index += 1;
                Ok(value)
            }
            _ => Err(grammar_error(grammar)),
        }
    }

    /// A non-negative whole number.
    pub fn unsigned_integer(&mut self, grammar: &str) -> Result<u64, ParserError> {
        match self.peek().map(|t| &t.token) {
            Some(Token::Number(digits, _)) => {
                let parsed = digits.parse::<u64>().map_err(|_| grammar_error(grammar))?;
                self.index += 1;
                Ok(parsed)
            }
            _ => Err(grammar_error(grammar)),
        }
    }

    /// A VALUE slot running to the end of the statement, read as SOURCE TEXT.
    ///
    /// A principal is not an identifier: `svc:account` and
    /// `justin.joyce@joocer.com` are one name each, but the tokenizer splits
    /// both (`@` and `.` are identifier characters here, `:` is not), so
    /// rebuilding one from tokens means re-deciding where it ends. The text is
    /// the name. See `ValueSlot` for how it is classified.
    pub fn value_slot_tail(&mut self, grammar: &str) -> Result<Slot, ParserError> {
        let sql = self.sql;
        let tokens = self.statement_tail();
        if tokens.is_empty() {
            return Err(grammar_error(grammar));
        }
        Ok(Slot::classify(crate::aside::source_slice(sql, tokens).trim()))
    }

    /// A VALUE slot running up to (but not including) `terminator`, read as
    /// SOURCE TEXT — the bounded sibling of `value_slot_tail`.
    ///
    /// `SAVE RESULTS OF <job> AS <dataset>` needs this: a job handle is
    /// `YYYYMMDDHHMMSS-<random>`, which opens with a digit and carries a
    /// hyphen, so the tokenizer sees a number, a minus and a word. It is one
    /// name, and the text is that name.
    pub fn value_slot_until(&mut self, terminator: &str, grammar: &str) -> Result<Slot, ParserError> {
        let sql = self.sql;
        let start = self.index;
        while !self.at_end()
            && !self.peek_is(&Token::SemiColon)
            && !self.peek_word(terminator)
        {
            self.index += 1;
        }
        if self.index == start || !self.peek_word(terminator) {
            return Err(grammar_error(grammar));
        }
        let tokens = &self.tokens[start..self.index];
        self.index += 1; // the terminator
        Ok(Slot::classify(crate::aside::source_slice(sql, tokens).trim()))
    }

    /// sqlparser's own "expected X, found Y", for the fallthrough path.
    pub fn expected(&self, what: &str) -> ParserError {
        match self.peek() {
            Some(t) => ParserError::ParserError(format!(
                "Expected: {}, found: {} at Line: {}, Column: {}",
                what, t.token, t.span.start.line, t.span.start.column
            )),
            None => ParserError::ParserError(format!("Expected: {}, found: EOF", what)),
        }
    }
}

/// A refusal that quotes the grammar back. See `GRAMMAR_ERROR_PREFIX`.
pub fn grammar_error(message: &str) -> ParserError {
    ParserError::ParserError(format!("{GRAMMAR_ERROR_PREFIX}{message}"))
}

/// One VALUE slot, as the planner reads it back.
///
/// Mirrors `pre_parse._slot_value`, which this replaces: a placeholder becomes
/// the node the AST rewriter binds, a quoted literal is unquoted with doubled
/// quote characters collapsing to one, anything else is the bare text. The
/// literal charsets exclude a leading `:`, so a slot starting with one is
/// unambiguously a placeholder and can never be read back as a literal.
///
/// Untagged: a literal serialises as a bare string and a placeholder as
/// `{"Placeholder": ":name"}`, which is the shape
/// `ast_rewriter.parameter_dict_binder` looks for.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ValueSlot {
    Literal(String),
    Placeholder(Placeholder),
}

#[derive(Debug, Serialize)]
pub struct Placeholder {
    #[serde(rename = "Placeholder")]
    pub name: String,
}

/// A classified value slot, plus the one thing that cannot be recovered from
/// the classified value.
pub struct Slot {
    pub value: ValueSlot,
    /// Whether the reader wrote the bare keyword `CURRENT_USER`.
    ///
    /// Decided from the RAW text, before unquoting, and that is the whole
    /// reason this is not a method on `ValueSlot`: bare `CURRENT_USER` means
    /// "me", while `'CURRENT_USER'` asks for a principal literally named that,
    /// the usual SQL distinction, and the only way to name such a principal if
    /// one ever exists. Both unquote to the same string, so a check made
    /// after classification cannot tell them apart. (It did not, briefly:
    /// `OWNER TO 'CURRENT_USER'` was read as the keyword.) A placeholder is
    /// never the keyword either - what a parameter carries is a value.
    pub is_current_user: bool,
}

impl Slot {
    /// Whether the reader wrote a parameter here.
    ///
    /// A slot can be read as TEXT for tokenizing reasons and still be an
    /// IDENTIFIER slot semantically - a job handle is the case: it opens with
    /// a digit and carries a hyphen, so it cannot be an `ObjectName`, but it
    /// names WHICH results get copied and that is not a runtime decision. Such
    /// a slot checks this and refuses.
    pub fn is_placeholder(&self) -> bool {
        matches!(self.value, ValueSlot::Placeholder(_))
    }

    fn classify(raw: &str) -> Slot {
        let is_current_user = raw.eq_ignore_ascii_case("CURRENT_USER");
        let value = match raw.chars().next() {
            Some(':') | Some('?') => ValueSlot::Placeholder(Placeholder {
                name: raw.to_string(),
            }),
            Some('\'') if raw.len() >= 2 && raw.ends_with('\'') => {
                ValueSlot::Literal(raw[1..raw.len() - 1].replace("''", "'"))
            }
            Some('"') if raw.len() >= 2 && raw.ends_with('"') => {
                ValueSlot::Literal(raw[1..raw.len() - 1].replace("\"\"", "\""))
            }
            _ => ValueSlot::Literal(raw.to_string()),
        };
        Slot {
            value,
            is_current_user,
        }
    }
}
