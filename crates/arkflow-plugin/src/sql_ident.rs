/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! SQL identifier quoting shared by the `sql` output and the pgvector
//! components.
//!
//! Column names come from the batch schema and may be inferred from data, so
//! every identifier interpolated into SQL text is wrapped in the dialect's
//! quote character with embedded quote characters doubled. A wrapped
//! identifier can never end early: a hostile name becomes one (very
//! strangely named) identifier instead of a change to the statement.

/// Why a dotted object name cannot be quoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InvalidObjectName {
    Empty,
    NulByte,
    EmptyPart { part: usize },
    UnterminatedQuote { quote: char },
    TextAfterQuote { character: char },
}

impl std::fmt::Display for InvalidObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "the name is empty"),
            Self::NulByte => write!(f, "it contains a NUL byte"),
            Self::EmptyPart { part } => write!(f, "part {part} is empty"),
            Self::UnterminatedQuote { quote } => {
                write!(f, "an opening {quote:?} is never closed")
            }
            Self::TextAfterQuote { character } => {
                write!(f, "`{character}` follows a closing quote")
            }
        }
    }
}

/// Quoting style of a SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuoteStyle {
    /// ANSI double quotes: `"name"` (PostgreSQL).
    DoubleQuote,
    /// Backticks: `` `name` `` (MySQL).
    Backtick,
}

impl QuoteStyle {
    fn quote_char(self) -> char {
        match self {
            QuoteStyle::DoubleQuote => '"',
            QuoteStyle::Backtick => '`',
        }
    }

    /// Quotes one identifier, doubling embedded quote characters.
    pub(crate) fn quote(self, name: &str) -> String {
        let quote = self.quote_char();
        let mut out = String::with_capacity(name.len() + 2);
        out.push(quote);
        for c in name.chars() {
            if c == quote {
                out.push(quote);
            }
            out.push(c);
        }
        out.push(quote);
        out
    }

    /// Quotes a possibly schema-qualified object name (`analytics.events`),
    /// one identifier per dot-separated part.
    ///
    /// A part already wrapped in this dialect's quote character passes
    /// through unchanged once validated; a bare part is quoted, and on
    /// PostgreSQL lower-cased first so it resolves exactly like the
    /// unquoted identifier it replaces. Surrounding whitespace is ignored.
    pub(crate) fn quote_qualified(self, name: &str) -> Result<String, InvalidObjectName> {
        let name = name.trim();
        if name.is_empty() {
            return Err(InvalidObjectName::Empty);
        }
        if name.contains('\0') {
            return Err(InvalidObjectName::NulByte);
        }
        let quote = self.quote_char();
        let chars: Vec<char> = name.chars().collect();
        let mut parts: Vec<String> = Vec::new();
        let mut i = 0;
        loop {
            if chars.get(i) == Some(&quote) {
                // A quoted part: find its closing quote, honouring doubling.
                let start = i;
                i += 1;
                loop {
                    match chars.get(i) {
                        None => {
                            return Err(InvalidObjectName::UnterminatedQuote { quote });
                        }
                        Some(found) if *found == quote => {
                            if chars.get(i + 1) == Some(&quote) {
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(_) => i += 1,
                    }
                }
                parts.push(chars[start..i].iter().collect());
            } else {
                let start = i;
                while i < chars.len() && chars[i] != '.' {
                    i += 1;
                }
                if i == start {
                    return Err(InvalidObjectName::EmptyPart {
                        part: parts.len() + 1,
                    });
                }
                let bare: String = chars[start..i].iter().collect();
                let bare = match self {
                    QuoteStyle::DoubleQuote => bare.to_lowercase(),
                    QuoteStyle::Backtick => bare,
                };
                parts.push(self.quote(&bare));
            }
            match chars.get(i) {
                None => break,
                Some('.') => i += 1,
                Some(character) => {
                    return Err(InvalidObjectName::TextAfterQuote {
                        character: *character,
                    });
                }
            }
        }
        Ok(parts.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Names that would change the structure of a statement if they were
    /// interpolated raw. Every one must survive as a single identifier under
    /// both quoting styles.
    const HOSTILE: &[&str] = &[
        "a\"b",
        "a`b",
        "x) VALUES ('injected'); DROP TABLE events; --",
        "'); DROP TABLE events; --",
        "with space",
        "`)",
        "\"\"",
        "a.b",
        "Ünïcödé;表",
        "'quoted'",
        "\\",
    ];

    /// Removes the wrapping quotes and undoes the doubling.
    fn unquote(quoted: &str, quote: char) -> String {
        let inner = &quoted[1..quoted.len() - 1];
        let mut out = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == quote && chars.peek() == Some(&quote) {
                chars.next();
            }
            out.push(c);
        }
        out
    }

    #[test]
    fn quote_wraps_every_hostile_name_as_one_identifier() {
        for style in [QuoteStyle::DoubleQuote, QuoteStyle::Backtick] {
            let quote = style.quote_char();
            for name in HOSTILE {
                let quoted = style.quote(name);
                assert_eq!(quoted.chars().next(), Some(quote), "{name:?}");
                assert_eq!(quoted.chars().last(), Some(quote), "{name:?}");
                assert_eq!(unquote(&quoted, quote), *name, "{style:?} {name:?}");
                // Only doubled quotes appear inside.
                let inner = &quoted[1..quoted.len() - 1];
                let mut rest = inner;
                while let Some(at) = rest.find(quote) {
                    assert!(rest[at + 1..].starts_with(quote), "{quoted}");
                    rest = &rest[at + 2..];
                }
            }
        }
    }

    #[test]
    fn quote_doubles_the_dialect_quote_character() {
        assert_eq!(QuoteStyle::DoubleQuote.quote("a\"b"), "\"a\"\"b\"");
        assert_eq!(QuoteStyle::DoubleQuote.quote("a`b"), "\"a`b\"");
        assert_eq!(QuoteStyle::Backtick.quote("a`b"), "`a``b`");
        assert_eq!(QuoteStyle::Backtick.quote("a\"b"), "`a\"b`");
        assert_eq!(
            QuoteStyle::Backtick.quote("x) VALUES ('injected'); DROP TABLE events; --"),
            "`x) VALUES ('injected'); DROP TABLE events; --`"
        );
    }

    #[test]
    fn quote_qualified_splits_on_dots_and_quotes_each_part() {
        assert_eq!(
            QuoteStyle::DoubleQuote.quote_qualified("events").unwrap(),
            "\"events\""
        );
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("analytics.events")
                .unwrap(),
            "\"analytics\".\"events\""
        );
        assert_eq!(
            QuoteStyle::Backtick
                .quote_qualified("analytics.events")
                .unwrap(),
            "`analytics`.`events`"
        );
        // Semicolons name one table; they do not end the statement. The bare
        // PostgreSQL part folds to lower case, MySQL keeps it as written.
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("events; DROP TABLE users; --")
                .unwrap(),
            "\"events; drop table users; --\""
        );
        assert_eq!(
            QuoteStyle::Backtick
                .quote_qualified("events; DROP TABLE users; --")
                .unwrap(),
            "`events; DROP TABLE users; --`"
        );
        assert_eq!(
            QuoteStyle::Backtick.quote_qualified("  events  ").unwrap(),
            "`events`"
        );
    }

    #[test]
    fn quote_qualified_keeps_unquoted_postgresql_names_case_insensitive() {
        // An unquoted PostgreSQL identifier folds to lower case: a bare part
        // keeps resolving to the same object once it is quoted.
        assert_eq!(
            QuoteStyle::DoubleQuote.quote_qualified("Events").unwrap(),
            "\"events\""
        );
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("Analytics.Events")
                .unwrap(),
            "\"analytics\".\"events\""
        );
        // A part the user already quoted keeps its case, verbatim.
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("\"Events\"")
                .unwrap(),
            "\"Events\""
        );
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("analytics.\"Events\"")
                .unwrap(),
            "\"analytics\".\"Events\""
        );
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("\"My\"\"Table\".events")
                .unwrap_or_default(),
            "\"My\"\"Table\".\"events\""
        );
        // MySQL backticks preserve case, like an unquoted name there.
        assert_eq!(
            QuoteStyle::Backtick.quote_qualified("Events").unwrap(),
            "`Events`"
        );
        assert_eq!(
            QuoteStyle::Backtick
                .quote_qualified("`my table`.events")
                .unwrap(),
            "`my table`.`events`"
        );
        // A quote inside a bare part is content, not the start of a quoted part.
        assert_eq!(
            QuoteStyle::DoubleQuote.quote_qualified("ev\"ents").unwrap(),
            "\"ev\"\"ents\""
        );
    }

    #[test]
    fn quote_qualified_rejects_malformed_names() {
        for style in [QuoteStyle::DoubleQuote, QuoteStyle::Backtick] {
            assert_eq!(style.quote_qualified(""), Err(InvalidObjectName::Empty));
            assert_eq!(style.quote_qualified("   "), Err(InvalidObjectName::Empty));
            assert_eq!(
                style.quote_qualified("a\0b"),
                Err(InvalidObjectName::NulByte)
            );
            assert_eq!(
                style.quote_qualified("a..b"),
                Err(InvalidObjectName::EmptyPart { part: 2 })
            );
            assert_eq!(
                style.quote_qualified(".a"),
                Err(InvalidObjectName::EmptyPart { part: 1 })
            );
            assert_eq!(
                style.quote_qualified("a."),
                Err(InvalidObjectName::EmptyPart { part: 2 })
            );
        }
        assert_eq!(
            QuoteStyle::DoubleQuote.quote_qualified("\"events"),
            Err(InvalidObjectName::UnterminatedQuote { quote: '"' })
        );
        assert_eq!(
            QuoteStyle::Backtick.quote_qualified("a.`events"),
            Err(InvalidObjectName::UnterminatedQuote { quote: '`' })
        );
        assert_eq!(
            QuoteStyle::DoubleQuote.quote_qualified("\"events\"x"),
            Err(InvalidObjectName::TextAfterQuote { character: 'x' })
        );
        assert_eq!(
            QuoteStyle::Backtick.quote_qualified("`a` `b`"),
            Err(InvalidObjectName::TextAfterQuote { character: ' ' })
        );
        assert_eq!(
            QuoteStyle::DoubleQuote
                .quote_qualified("\"a\"\"")
                .unwrap_err(),
            InvalidObjectName::UnterminatedQuote { quote: '"' }
        );
        let reason = QuoteStyle::DoubleQuote
            .quote_qualified(".a")
            .unwrap_err()
            .to_string();
        assert_eq!(reason, "part 1 is empty");
    }
}
