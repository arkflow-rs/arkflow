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

//! Component metadata must describe the configuration the component really
//! accepts.
//!
//! Every component registers a hand-written JSON Schema next to a
//! serde-derived config struct, and the two lists are maintained separately.
//! Because the root schemas are `additionalProperties: false`, drift in
//! either direction is user-visible: the IDE rejects a legal field, or blesses
//! a config the component then rejects or silently ignores.
//!
//! This gate reads each component's config struct from source — so the field
//! list can never go stale — and checks the registered schema against it:
//!
//! * R1 coverage — at a node with `additionalProperties: false`, every serde
//!   input field appears in `properties`.
//! * R2 no ghosts — every documented key is a serde input field.
//! * R3 — every schema-`required` key is a field serde requires (or is
//!   registered as builder-enforced in [`BUILDER_REQUIRED`]); a required key
//!   nothing reads is reported too.
//! * R4 — every serde-required field is demanded by every configuration the
//!   schema accepts (a key only some `anyOf`/`oneOf` branches demand does not
//!   count), and every `oneOf`/`anyOf` branch of an untagged enum demands
//!   all the keys of some variant.
//! * R5 — a documented `default` belongs to a field serde lets the user omit,
//!   and equals the value serde produces: the type's zero behind
//!   `#[serde(default)]`, or the literal a default function returns.
//! * R6 — a string-enum schema lists exactly the variants serde decodes, and
//!   every tag value a tagged enum's schema offers names such a variant.
//! * R0 — a referenced type the gate cannot read, or a bare name that is
//!   ambiguous, is reported instead of silently skipped — flattened types
//!   included.
//!
//! `#[serde(flatten)]` fields are inlined, nested object/array properties are
//! checked against their own types, generic types resolve by their name, and
//! enums are read the way serde tags them. A schema node without
//! `properties`/`required` that is not closed is opaque (it accepts any key),
//! so nothing is checked there.
//!
//! Deliberate exceptions go through [`HIDDEN_FIELDS`], [`BUILDER_REQUIRED`],
//! and [`NO_CONFIG`], and an exemption that is no longer needed fails the
//! gate so the lists cannot rot. A [`BUILDER_REQUIRED`] entry is also probed:
//! the component's builder has to refuse a config without the key.

use arkflow_core::component::{list_components, ComponentKind, ComponentMetadata};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Exemptions = &'static [(&'static str, &'static str, &'static str, &'static str)];

/// `(kind, registered name, source file under src/, config struct the builder
/// deserializes)` for every component with a configuration surface.
///
/// Adding a component with a schema? Add a row — the completeness check below
/// fails until you do, and the row is bound to the builder registered under
/// that name (its `impl` block must deserialize the struct).
const CONFIG_TYPES: &[(&str, &str, &str, &str)] = &[
    // inputs
    ("input", "file", "input/file.rs", "FileInputConfig"),
    (
        "input",
        "generate",
        "input/generate.rs",
        "GenerateInputConfig",
    ),
    ("input", "http", "input/http.rs", "HttpInputConfig"),
    ("input", "kafka", "input/kafka.rs", "KafkaInputConfig"),
    ("input", "memory", "input/memory.rs", "MemoryInputConfig"),
    ("input", "modbus", "input/modbus.rs", "ModbusInputConfig"),
    ("input", "mqtt", "input/mqtt.rs", "MqttInputConfig"),
    (
        "input",
        "multiple_inputs",
        "input/multiple_inputs.rs",
        "MultipleInputsConfig",
    ),
    ("input", "nats", "input/nats.rs", "NatsInputConfig"),
    ("input", "pulsar", "input/pulsar.rs", "PulsarInputConfig"),
    ("input", "redis", "input/redis.rs", "RedisInputConfig"),
    ("input", "sql", "input/sql.rs", "SqlInputConfig"),
    (
        "input",
        "websocket",
        "input/websocket.rs",
        "WebSocketInputConfig",
    ),
    // outputs
    ("output", "http", "output/http.rs", "HttpOutputConfig"),
    (
        "output",
        "influxdb",
        "output/influxdb.rs",
        "InfluxDBOutputConfig",
    ),
    ("output", "kafka", "output/kafka.rs", "KafkaOutputConfig"),
    ("output", "milvus", "output/milvus.rs", "MilvusOutputConfig"),
    (
        "output",
        "mongodb",
        "output/mongodb.rs",
        "MongoDBOutputConfig",
    ),
    ("output", "mqtt", "output/mqtt.rs", "MqttOutputConfig"),
    ("output", "nats", "output/nats.rs", "NatsOutputConfig"),
    (
        "output",
        "pgvector",
        "output/pgvector.rs",
        "PgVectorOutputConfig",
    ),
    ("output", "pulsar", "output/pulsar.rs", "PulsarOutputConfig"),
    ("output", "qdrant", "output/qdrant.rs", "QdrantOutputConfig"),
    ("output", "redis", "output/redis.rs", "RedisOutputConfig"),
    ("output", "sql", "output/sql.rs", "SqlOutputConfig"),
    ("output", "stdout", "output/stdout.rs", "StdoutOutputConfig"),
    // processors
    (
        "processor",
        "arrow_to_json",
        "processor/json.rs",
        "JsonProcessorConfig",
    ),
    (
        "processor",
        "arrow_to_protobuf",
        "processor/protobuf.rs",
        "ArrowToProtobufProcessorConfig",
    ),
    (
        "processor",
        "batch",
        "processor/batch.rs",
        "BatchProcessorConfig",
    ),
    (
        "processor",
        "embedding",
        "processor/embedding.rs",
        "EmbeddingProcessorConfig",
    ),
    (
        "processor",
        "json_to_arrow",
        "processor/json.rs",
        "JsonProcessorConfig",
    ),
    ("processor", "llm", "processor/llm.rs", "LlmProcessorConfig"),
    ("processor", "lookup", "processor/lookup.rs", "LookupConfig"),
    (
        "processor",
        "milvus_search",
        "processor/milvus_search.rs",
        "MilvusSearchProcessorConfig",
    ),
    (
        "processor",
        "pgvector_search",
        "processor/pgvector_search.rs",
        "PgVectorSearchProcessorConfig",
    ),
    (
        "processor",
        "protobuf_to_arrow",
        "processor/protobuf.rs",
        "ProtobufToArrowProcessorConfig",
    ),
    (
        "processor",
        "python",
        "processor/python.rs",
        "PythonProcessorConfig",
    ),
    ("processor", "sql", "processor/sql.rs", "SqlProcessorConfig"),
    (
        "processor",
        "vector_search",
        "processor/vector_search.rs",
        "VectorSearchProcessorConfig",
    ),
    ("processor", "vrl", "processor/vrl.rs", "VrlProcessorConfig"),
    // buffers
    ("buffer", "memory", "buffer/memory.rs", "MemoryBufferConfig"),
    (
        "buffer",
        "session_window",
        "buffer/session_window.rs",
        "SessionWindowConfig",
    ),
    (
        "buffer",
        "sliding_window",
        "buffer/sliding_window.rs",
        "SlidingWindowConfig",
    ),
    (
        "buffer",
        "tumbling_window",
        "buffer/tumbling_window.rs",
        "TumblingWindowConfig",
    ),
    // codecs
    ("codec", "json", "codec/json.rs", "JsonCodecConfig"),
    (
        "codec",
        "protobuf",
        "codec/protobuf.rs",
        "ProtobufCodecConfig",
    ),
    (
        "codec",
        "schema_registry",
        "codec/schema_registry.rs",
        "SchemaRegistryCodecConfig",
    ),
    // temporary
    (
        "temporary",
        "redis",
        "temporary/redis.rs",
        "RedisTemporaryConfig",
    ),
];

/// A config-struct field that is deliberately absent from the metadata
/// schema: `(kind/name, config struct, field, reason)`.
///
/// The field is exempt from R1 (coverage) and R4 (required). An entry stops
/// being needed the moment the gate no longer relies on it, and then fails
/// the gate — remove it.
const HIDDEN_FIELDS: Exemptions = &[];

/// A field the schema lists as `required` although serde accepts its
/// omission, because the component builder enforces it with a
/// domain-specific error: `(kind/name, config struct, field, reason)`.
///
/// Exempt from R3 only. The entry must stay true (the schema marks it
/// required, serde lets the user omit it) or it fails the gate as stale.
const BUILDER_REQUIRED: Exemptions = &[(
    "output/milvus",
    "MilvusOutputConfig",
    "id_field",
    "the builder rejects a missing id_field: the REST upsert API needs a primary key per row",
)];

/// Components whose schema is a closed object that documents nothing,
/// because their builder reads no configuration: `(kind/name, reason)`.
///
/// They need no [`CONFIG_TYPES`] row. The schema must stay that way: once it
/// documents a key, the entry is stale and the component needs a row.
const NO_CONFIG: &[(&str, &str)] = &[(
    "codec/debezium_json",
    "the builder ignores its configuration object",
)];

// ---------------------------------------------------------------------------
// Lexical helpers
// ---------------------------------------------------------------------------

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether the word `word` starts at `i`.
fn word_at(s: &[char], i: usize, word: &str) -> bool {
    let letters: Vec<char> = word.chars().collect();
    s.len() >= i + letters.len() && s[i..i + letters.len()] == letters[..]
}

/// Index just past the identifier starting at `i` (or `i` when none does).
fn ident_end(s: &[char], i: usize) -> usize {
    let mut j = i;
    while j < s.len() && is_ident_continue(s[j]) {
        j += 1;
    }
    j
}

fn skip_ws(s: &[char], mut i: usize) -> usize {
    while i < s.len() && s[i].is_whitespace() {
        i += 1;
    }
    i
}

/// `r"…"`, `r#"…"#`, `br#"…"#`: the number of `#` between `r` and the
/// opening quote, or `None` when `i` does not start a raw string.
fn raw_string_hashes(s: &[char], i: usize) -> Option<usize> {
    let mut j = i;
    if s.get(j) == Some(&'b') {
        j += 1;
    }
    if s.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let start = j;
    while s.get(j) == Some(&'#') {
        j += 1;
    }
    (s.get(j) == Some(&'"')).then_some(j - start)
}

/// Index of the closing quote of the string literal opening at `open`
/// (`s.len()` when unterminated).
fn string_close(s: &[char], open: usize) -> usize {
    let mut j = open + 1;
    while j < s.len() {
        if s[j] == '\\' {
            j += 2;
            continue;
        }
        if s[j] == s[open] {
            return j;
        }
        j += 1;
    }
    s.len()
}

/// Whether the text ends with an odd number of backslashes.
fn escaped(text: &[char]) -> bool {
    text.iter().rev().take_while(|c| **c == '\\').count() % 2 == 1
}

/// Index just past the char literal starting at `i`, or `None` when `i` is a
/// lifetime (a lone `'` followed by an identifier).
fn char_literal_end(s: &[char], i: usize) -> Option<usize> {
    if s.get(i) != Some(&'\'') {
        return None;
    }
    if s.get(i + 1) == Some(&'\\') {
        if s.get(i + 2) == Some(&'\'') {
            return Some(i + 4);
        }
        let mut j = i + 2;
        while j < s.len() && s[j] != '\'' {
            j += 1;
        }
        return Some((j + 1).min(s.len()));
    }
    (s.get(i + 2) == Some(&'\'')).then_some(i + 3)
}

/// Index just past the literal (string, raw string, or char) starting at `i`.
fn literal_end(s: &[char], i: usize) -> Option<usize> {
    if let Some(hashes) = raw_string_hashes(s, i) {
        let close: Vec<char> = format!("\"{}", "#".repeat(hashes)).chars().collect();
        let mut j = i + 1;
        while j + close.len() <= s.len() && s[j..j + close.len()] != close[..] {
            j += 1;
        }
        return Some((j + close.len()).min(s.len()));
    }
    if let Some(end) = char_literal_end(s, i) {
        return Some(end);
    }
    (s.get(i) == Some(&'"')).then(|| (string_close(s, i) + 1).min(s.len()))
}

/// Replaces comments with spaces (keeping newlines and every other byte, so
/// positions are preserved). String, char, and raw-string literals are copied
/// verbatim so attribute values such as `rename = "…"` stay readable.
fn strip_comments(src: &str) -> Vec<char> {
    let s: Vec<char> = src.chars().collect();
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if let Some(end) = literal_end(&s, i) {
            out.extend_from_slice(&s[i..end]);
            i = end;
        } else if s[i] == '/' && s.get(i + 1) == Some(&'/') {
            while i < s.len() && s[i] != '\n' {
                out.push(' ');
                i += 1;
            }
        } else if s[i] == '/' && s.get(i + 1) == Some(&'*') {
            let mut depth = 0usize;
            while i < s.len() {
                if s[i] == '/' && s.get(i + 1) == Some(&'*') {
                    depth += 1;
                    out.extend([' ', ' ']);
                    i += 2;
                } else if s[i] == '*' && s.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    out.extend([' ', ' ']);
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    out.push(if s[i] == '\n' { '\n' } else { ' ' });
                    i += 1;
                }
            }
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    out
}

/// Index just past the bracket matching `s[at]` (`open`).
fn matching(s: &[char], open: char, close: char, at: usize) -> Result<usize, ParseError> {
    let mut depth = 0usize;
    let mut i = at;
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        if s[i] == open {
            depth += 1;
        } else if s[i] == close {
            depth -= 1;
            if depth == 0 {
                return Ok(i + 1);
            }
        }
        i += 1;
    }
    Err(ParseError(format!("unbalanced `{open}`")))
}

/// Splits at commas that are not nested in `<>`, `()`, `[]`, or `{}`.
fn split_top_level(body: &[char]) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    let mut i = 0;
    while i < body.len() {
        if let Some(end) = literal_end(body, i) {
            current.extend(&body[i..end]);
            i = end;
            continue;
        }
        let c = body[i];
        let arrow = c == '>' && i > 0 && body[i - 1] == '-';
        match c {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' if !arrow => depth -= 1,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if c == ',' && depth <= 0 {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
        i += 1;
    }
    parts.push(current);
    parts
        .into_iter()
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// The `#[…]` attributes at the start of `text`, plus the text after them.
fn leading_attrs(text: &str) -> (Vec<String>, String) {
    let s: Vec<char> = text.chars().collect();
    let mut attrs = Vec::new();
    let mut i = skip_ws(&s, 0);
    while s.get(i) == Some(&'#') && s.get(i + 1) == Some(&'[') {
        let Ok(end) = matching(&s, '[', ']', i + 1) else {
            break;
        };
        attrs.push(s[i + 2..end - 1].iter().collect());
        i = skip_ws(&s, end);
    }
    let rest = s[i..].iter().collect::<String>();
    (attrs, rest.trim().to_string())
}

/// The `#[…]` attributes ending exactly at `end` (comments in between are
/// already blank).
fn attrs_ending_at(s: &[char], end: usize) -> Vec<String> {
    let mut attrs = Vec::new();
    let mut j = end;
    loop {
        while j > 0 && s[j - 1].is_whitespace() {
            j -= 1;
        }
        if j == 0 || s[j - 1] != ']' {
            break;
        }
        let mut depth = 0i32;
        let mut k = j;
        while k > 0 {
            k -= 1;
            match s[k] {
                ']' => depth += 1,
                '[' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                '"' => {
                    // Closing quote of a literal: jump to its opening quote.
                    let mut open = k;
                    while open > 0 {
                        open -= 1;
                        if s[open] == '"' && !escaped(&s[..open]) {
                            break;
                        }
                    }
                    k = open;
                }
                _ => {}
            }
        }
        if depth != 0 || k == 0 || s[k - 1] != '#' || (k >= 2 && s[k - 2] == '!') {
            break;
        }
        attrs.insert(0, s[k + 1..j - 1].iter().collect());
        j = k - 1;
    }
    attrs
}

/// `#[…]` attributes on the item whose `struct`/`enum` keyword starts at
/// `keyword`, looking across a visibility (`pub`, `pub(crate)`,
/// `pub(super)`, `pub(in path)`).
fn item_attrs(s: &[char], keyword: usize) -> Vec<String> {
    let mut head = keyword;
    let mut j = keyword;
    while j > 0 && s[j - 1].is_whitespace() {
        j -= 1;
    }
    // A visibility ends at `j`: `pub` followed by an optional `(…)`
    // restriction.
    let mut k = j;
    if s.get(j.wrapping_sub(1)) == Some(&')') {
        let mut depth = 0i32;
        while k > 0 {
            k -= 1;
            match s[k] {
                ')' => depth += 1,
                '(' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    let is_pub =
        k >= 3 && s[k - 3..k] == ['p', 'u', 'b'] && (k == 3 || !is_ident_continue(s[k - 4]));
    if is_pub {
        head = k - 3;
    }
    attrs_ending_at(s, head)
}

// ---------------------------------------------------------------------------
// serde attributes and config-shape parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct SerdeAttrs {
    rename: Option<String>,
    /// `default` on its own: the field is `Default::default()` when omitted.
    default: bool,
    /// `default = "path"`: a function supplies the value.
    default_fn: Option<String>,
    /// `deserialize_with = "…"` / `with = "…"`: runs only for a key that is
    /// present, so an `Option` behind it is still required unless `default`
    /// says otherwise.
    custom_deserializer: bool,
    skip: bool,
    flatten: bool,
    rename_all: Option<String>,
    /// An enum's rule for the fields of its struct variants.
    rename_all_fields: Option<String>,
    tag: Option<String>,
    untagged: bool,
    content: bool,
}

fn unquote(value: &str) -> String {
    let value = value.trim();
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
        .to_string()
}

fn parse_serde(attrs: &[String]) -> SerdeAttrs {
    let mut out = SerdeAttrs::default();
    for attr in attrs {
        let attr = attr.trim();
        let Some(open) = attr.find('(') else {
            continue;
        };
        if attr[..open].trim_end() != "serde" {
            continue;
        }
        let inner: Vec<char> = attr[open + 1..attr.len().saturating_sub(1)]
            .chars()
            .collect();
        for part in split_top_level(&inner) {
            let (key, value) = match part.find('=') {
                Some(eq) => (
                    part[..eq].trim().to_string(),
                    Some(part[eq + 1..].trim().to_string()),
                ),
                None => (part.clone(), None),
            };
            match key.as_str() {
                "rename" => out.rename = value.map(|v| unquote(&v)),
                "default" => match value {
                    Some(function) => out.default_fn = Some(unquote(&function)),
                    None => out.default = true,
                },
                "deserialize_with" | "with" => out.custom_deserializer = true,
                "skip" | "skip_deserializing" => out.skip = true,
                "flatten" => out.flatten = true,
                "rename_all" => out.rename_all = value.map(|v| unquote(&v)),
                "rename_all_fields" => out.rename_all_fields = value.map(|v| unquote(&v)),
                "tag" => out.tag = value.map(|v| unquote(&v)),
                "untagged" => out.untagged = true,
                "content" => out.content = true,
                _ => {}
            }
        }
    }
    out
}

/// serde's `rename_all` conventions, named as serde names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
enum RenameRule {
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

fn rename_rule(rule: &str) -> Option<RenameRule> {
    Some(match rule {
        "lowercase" => RenameRule::LowerCase,
        "UPPERCASE" => RenameRule::UpperCase,
        "PascalCase" => RenameRule::PascalCase,
        "camelCase" => RenameRule::CamelCase,
        "snake_case" => RenameRule::SnakeCase,
        "SCREAMING_SNAKE_CASE" => RenameRule::ScreamingSnakeCase,
        "kebab-case" => RenameRule::KebabCase,
        "SCREAMING-KEBAB-CASE" => RenameRule::ScreamingKebabCase,
        _ => return None,
    })
}

/// serde's `PascalCase` for a field: words are split at `_`, each word
/// starts with an ASCII upper-case letter and keeps the rest as written.
fn pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut at_word_start = true;
    for c in name.chars() {
        if c == '_' {
            at_word_start = true;
        } else if at_word_start {
            out.push(c.to_ascii_uppercase());
            at_word_start = false;
        } else {
            out.push(c);
        }
    }
    out
}

impl RenameRule {
    /// The name serde derives for a *variant*, character for character.
    ///
    /// serde's case folding is ASCII (`Échec` stays `Échec` under
    /// `lowercase`), while its word boundaries follow Unicode case (`aÉb`
    /// splits before `É`). The [`tests::rename_rules_match_serde`]
    /// cross-check keeps this honest.
    fn apply_to_variant(self, name: &str) -> String {
        match self {
            RenameRule::LowerCase => name.to_ascii_lowercase(),
            RenameRule::UpperCase => name.to_ascii_uppercase(),
            RenameRule::PascalCase => name.to_string(),
            // Only the first character is lowered; the rest is untouched.
            RenameRule::CamelCase => {
                let mut chars = name.chars();
                match chars.next() {
                    Some(first) => {
                        let mut out = first.to_ascii_lowercase().to_string();
                        out.push_str(chars.as_str());
                        out
                    }
                    None => String::new(),
                }
            }
            separated => {
                let (separator, upper) = match separated {
                    RenameRule::SnakeCase => ('_', false),
                    RenameRule::ScreamingSnakeCase => ('_', true),
                    RenameRule::KebabCase => ('-', false),
                    _ => ('-', true),
                };
                let mut out = String::with_capacity(name.len() * 2);
                for c in name.chars() {
                    if c.is_uppercase() {
                        // Every upper-case letter starts a word — an
                        // acronym splits letter by letter (`XML` → `x_m_l`).
                        if !out.is_empty() {
                            out.push(separator);
                        }
                    } else if !c.is_lowercase() && !c.is_numeric() {
                        // Any other character becomes a separator.
                        out.push(separator);
                        continue;
                    }
                    out.push(if upper {
                        c.to_ascii_uppercase()
                    } else {
                        c.to_ascii_lowercase()
                    });
                }
                out
            }
        }
    }
}

impl RenameRule {
    /// The name serde derives for a *field*. serde treats the Rust
    /// conventions as the identity and otherwise works on the `_` word
    /// boundaries — unlike the per-letter rule it applies to variants.
    fn apply_to_field(self, name: &str) -> String {
        match self {
            RenameRule::LowerCase | RenameRule::SnakeCase => name.to_string(),
            RenameRule::UpperCase | RenameRule::ScreamingSnakeCase => name.to_ascii_uppercase(),
            RenameRule::PascalCase => pascal_case(name),
            RenameRule::CamelCase => {
                let pascal = pascal_case(name);
                let mut chars = pascal.chars();
                match chars.next() {
                    Some(first) => {
                        let mut out = first.to_ascii_lowercase().to_string();
                        out.push_str(chars.as_str());
                        out
                    }
                    None => String::new(),
                }
            }
            RenameRule::KebabCase => name.replace('_', "-"),
            RenameRule::ScreamingKebabCase => name.to_ascii_uppercase().replace('_', "-"),
        }
    }
}

/// The name serde reads a field from.
fn field_name(ident: &str, attrs: &SerdeAttrs, rule: Option<RenameRule>) -> String {
    match (&attrs.rename, rule) {
        (Some(rename), _) => rename.clone(),
        (None, Some(rule)) => rule.apply_to_field(ident),
        (None, None) => ident.to_string(),
    }
}

/// The name serde reads a variant from.
fn variant_name(ident: &str, attrs: &SerdeAttrs, rule: Option<RenameRule>) -> String {
    match (&attrs.rename, rule) {
        (Some(rename), _) => rename.clone(),
        (None, Some(rule)) => rule.apply_to_variant(ident),
        (None, None) => ident.to_string(),
    }
}

fn type_is_option(ty: &str) -> bool {
    const PREFIXES: [&str; 3] = ["Option<", "std::option::Option<", "core::option::Option<"];
    ty.ends_with('>') && PREFIXES.iter().any(|p| ty.starts_with(p))
}

/// The value a plain `#[serde(default)]` produces for a type whose `Default`
/// is fixed: `false`, `0`, `""`, `[]`. `None` when it is not known
/// statically (`Option`, enums, maps, function-backed defaults).
fn plain_default_value(ty: &str) -> Option<Value> {
    let base = ty.strip_prefix("std::").unwrap_or(ty);
    match base {
        "bool" => Some(Value::Bool(false)),
        "u8" | "u16" | "u32" | "u64" | "usize" | "i8" | "i16" | "i32" | "i64" | "isize" => {
            Some(Value::from(0))
        }
        "f32" | "f64" => Some(serde_json::json!(0.0)),
        "String" => Some(Value::String(String::new())),
        _ if base.starts_with("Vec<") => Some(serde_json::json!([])),
        _ => None,
    }
}

/// Whether two JSON values mean the same default (`0` and `0.0` agree).
fn same_default(advertised: &Value, actual: &Value) -> bool {
    match (advertised.as_f64(), actual.as_f64()) {
        (Some(a), Some(b)) => a == b,
        _ => advertised == actual,
    }
}

/// Removes a leading visibility (`pub`, `pub(crate)`, `pub(super)`,
/// `pub(in path)`).
fn strip_visibility(text: &str) -> &str {
    let text = text.trim_start();
    if let Some(tail) = text.strip_prefix("pub") {
        if let Some(inner) = tail.strip_prefix('(') {
            let end = inner.find(')').map_or(0, |i| i + 2);
            return tail[end..].trim_start();
        }
        if tail.starts_with(is_ident_start) {
            return text;
        }
        return tail.trim_start();
    }
    text
}

/// One serde input field of a config shape.
#[derive(Debug, Clone)]
struct Field {
    /// Name serde reads the field from (after `rename` / `rename_all`).
    name: String,
    /// Type text, whitespace-collapsed.
    ty: String,
    /// Omitting the key is valid: `#[serde(default …)]`, a container
    /// `#[serde(default)]`, or a plain `Option<_>` (not behind a custom
    /// deserializer).
    optional: bool,
    /// The value of an absent key is the type's zero and can be compared
    /// with the schema: a bare `#[serde(default)]` on the field, or a
    /// derived container default behind which the field has no default of
    /// its own.
    plain_default: bool,
    /// `#[serde(default = "path")]`: the function that supplies the value.
    /// A default of the field's own, which wins over the container's.
    default_fn: Option<String>,
    /// `#[serde(skip)]` / `#[serde(skip_deserializing)]`: not an input.
    skip: bool,
    /// `#[serde(flatten)]`: the target type's fields are inlined.
    flatten: bool,
    /// The file the field is declared in: a default function written as a
    /// bare name is the one that file sees, also once the field is
    /// flattened into another file's struct.
    file: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariantKind {
    Struct,
    Newtype,
    Other,
}

#[derive(Debug, Clone)]
struct Variant {
    name: String,
    kind: VariantKind,
    skip: bool,
    /// Struct-variant fields.
    fields: Vec<Field>,
    /// Inner type text of a newtype variant.
    inner: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefKind {
    Struct,
    Enum,
}

/// How serde lays out the variants of an enum in the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnumLayout {
    /// serde's default: `{"variant": …}`.
    External,
    /// `#[serde(tag = "…")]`: the variant's fields sit next to the tag.
    Internal,
    /// `#[serde(untagged)]`: the variant's fields sit directly in the object.
    Untagged,
    /// `#[serde(tag = "…", content = "…")]`: fields nest under the content key.
    Adjacent,
}

/// A struct or enum definition, with the serde attributes that shape input.
#[derive(Debug, Clone)]
struct Def {
    kind: DefKind,
    /// Named fields (structs only).
    fields: Vec<Field>,
    /// Variants (enums only).
    variants: Vec<Variant>,
    /// `#[serde(tag = "…")]`: the tag key of an internally tagged enum.
    tag: Option<String>,
    layout: EnumLayout,
    file: String,
    name: String,
}

struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a container-level `#[serde(default …)]` does for the struct's keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerDefault {
    /// No container default.
    None,
    /// A bare `#[serde(default)]` on a struct that derives `Default`: every
    /// key may be omitted and takes its type's zero.
    Derived,
    /// A function-backed container default, or a bare one on a struct with
    /// a hand-written `Default`: every key may be omitted, but its value is
    /// whatever that `Default` returns.
    Custom,
}

fn parse_fields(
    body: &[char],
    container_default: ContainerDefault,
    rule: Option<RenameRule>,
    file: &str,
) -> Result<Vec<Field>, ParseError> {
    let mut fields = Vec::new();
    for part in split_top_level(body) {
        let (attrs, rest) = leading_attrs(&part);
        if rest.is_empty() {
            continue;
        }
        let serde = parse_serde(&attrs);
        let rest = strip_visibility(&rest);
        let rest = rest.strip_prefix("r#").unwrap_or(rest);
        let chars: Vec<char> = rest.chars().collect();
        let name_end = ident_end(&chars, 0);
        let name: String = chars[..name_end].iter().collect();
        if name.is_empty() {
            return Err(ParseError(format!("cannot read a field name in `{part}`")));
        }
        let after: String = chars[name_end..].iter().collect();
        let Some(ty) = after.trim().strip_prefix(':') else {
            return Err(ParseError(format!(
                "field `{name}` has no type in `{part}`"
            )));
        };
        let ty = ty.split_whitespace().collect::<Vec<_>>().join(" ");
        if ty.is_empty() || ty.contains(['{', '}']) {
            return Err(ParseError(format!(
                "field `{name}` has an unreadable type `{ty}`"
            )));
        }
        // A default of the field's own wins over the container's: a function
        // supplies its value, a bare `#[serde(default)]` is the field
        // type's zero. Behind a derived container default an absent key
        // takes the type's zero as well.
        let (plain_default, default_fn) = match &serde.default_fn {
            Some(function) => (false, Some(function.clone())),
            None => (
                serde.default || container_default == ContainerDefault::Derived,
                None,
            ),
        };
        fields.push(Field {
            name: field_name(&name, &serde, rule),
            optional: plain_default
                || default_fn.is_some()
                || container_default != ContainerDefault::None
                || (type_is_option(&ty) && !serde.custom_deserializer),
            plain_default,
            default_fn,
            skip: serde.skip,
            flatten: serde.flatten,
            file: file.to_string(),
            ty,
        });
    }
    Ok(fields)
}

fn parse_def_body(
    kind: DefKind,
    body: &[char],
    serde: &SerdeAttrs,
    derives_default: bool,
    file: &str,
    name: &str,
) -> Result<Def, ParseError> {
    let mut def = Def {
        kind,
        fields: Vec::new(),
        variants: Vec::new(),
        tag: serde.tag.clone(),
        layout: if serde.untagged {
            EnumLayout::Untagged
        } else if serde.tag.is_some() && serde.content {
            EnumLayout::Adjacent
        } else if serde.tag.is_some() {
            EnumLayout::Internal
        } else {
            EnumLayout::External
        },
        file: file.to_string(),
        name: name.to_string(),
    };
    let rule = serde.rename_all.as_deref().and_then(rename_rule);
    match kind {
        DefKind::Struct => {
            // Either container default lets every key be omitted; only a
            // derived one is known to yield zeroes.
            let container_default = if serde.default && derives_default {
                ContainerDefault::Derived
            } else if serde.default || serde.default_fn.is_some() {
                ContainerDefault::Custom
            } else {
                ContainerDefault::None
            };
            def.fields = parse_fields(body, container_default, rule, file)?;
        }
        DefKind::Enum => {
            // An enum's `rename_all` renames its variants. The fields of a
            // struct variant follow the variant's own `rename_all`, else the
            // enum's `rename_all_fields` — checked by
            // `tests::rename_rules_match_serde`.
            for part in split_top_level(body) {
                let (attrs, rest) = leading_attrs(&part);
                let vserde = parse_serde(&attrs);
                let chars: Vec<char> = rest.chars().collect();
                let name_end = ident_end(&chars, 0);
                let vname: String = chars[..name_end].iter().collect();
                if vname.is_empty() {
                    return Err(ParseError(format!(
                        "cannot read a variant name in `{part}`"
                    )));
                }
                let after = chars[name_end..]
                    .iter()
                    .collect::<String>()
                    .trim()
                    .to_string();
                let mut variant = Variant {
                    name: variant_name(&vname, &vserde, rule),
                    kind: VariantKind::Other,
                    skip: vserde.skip,
                    fields: Vec::new(),
                    inner: None,
                };
                if after.starts_with('{') && after.ends_with('}') {
                    variant.kind = VariantKind::Struct;
                    let inner: Vec<char> = after[1..after.len() - 1].chars().collect();
                    let field_rule = vserde
                        .rename_all
                        .as_deref()
                        .or(serde.rename_all_fields.as_deref())
                        .and_then(rename_rule);
                    variant.fields =
                        parse_fields(&inner, ContainerDefault::None, field_rule, file)?;
                } else if after.starts_with('(') && after.ends_with(')') {
                    let types: Vec<char> = after[1..after.len() - 1].chars().collect();
                    let mut pieces = split_top_level(&types);
                    if pieces.len() == 1 {
                        variant.kind = VariantKind::Newtype;
                        variant.inner = Some(pieces.remove(0));
                    }
                }
                def.variants.push(variant);
            }
        }
    }
    Ok(def)
}

/// Whether the attributes include `#[derive(Default)]`.
fn derives_default(attrs: &[String]) -> bool {
    attrs.iter().any(|attr| {
        let attr = attr.trim();
        let Some(open) = attr.find('(') else {
            return false;
        };
        attr[..open].trim_end() == "derive" && {
            let inner: Vec<char> = attr[open + 1..attr.len().saturating_sub(1)]
                .chars()
                .collect();
            split_top_level(&inner).iter().any(|item| item == "Default")
        }
    })
}

/// `Ok(None)` for a tuple or unit struct (no named fields to read),
/// `Err` for a braced definition whose body does not parse.
fn parse_def_at(
    s: &[char],
    keyword: usize,
    keyword_len: usize,
    kind: DefKind,
    file: &str,
) -> Result<Option<Def>, ParseError> {
    // A keyword is followed by whitespace and the type name.
    let Some(after_keyword) = s.get(keyword + keyword_len) else {
        return Ok(None);
    };
    if !after_keyword.is_whitespace() {
        return Ok(None);
    }
    let name_start = skip_ws(s, keyword + keyword_len);
    if !s.get(name_start).is_some_and(|c| is_ident_start(*c)) {
        return Ok(None);
    }
    let name_end = ident_end(s, name_start);
    let name: String = s[name_start..name_end].iter().collect();
    // Skip generics and a where clause: the body starts at the first `{` at
    // nesting depth zero.
    let mut depth = 0i32;
    let mut i = name_end;
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        let arrow = s[i] == '>' && i > 0 && s[i - 1] == '-';
        match s[i] {
            '<' | '(' | '[' => depth += 1,
            '>' if !arrow => depth -= 1,
            ')' | ']' => depth -= 1,
            '{' | ';' if depth <= 0 => break,
            _ => {}
        }
        i += 1;
    }
    if s.get(i) != Some(&'{') {
        return Ok(None);
    }
    let attrs = item_attrs(s, keyword);
    let serde = parse_serde(&attrs);
    let derives_default = derives_default(&attrs);
    let end = matching(s, '{', '}', i)?;
    parse_def_body(
        kind,
        &s[i + 1..end - 1],
        &serde,
        derives_default,
        file,
        &name,
    )
    .map(Some)
}

/// Definitions in a comment-stripped source, in order of appearance, plus
/// the braced definitions whose body could not be read: `(name, reason)`.
fn parse_defs(s: &[char], file: &str) -> (Vec<Def>, Vec<(String, String)>) {
    const KEYWORDS: [(&str, DefKind); 2] = [("struct", DefKind::Struct), ("enum", DefKind::Enum)];
    let mut defs = Vec::new();
    let mut unparsed = Vec::new();
    let mut i = 0;
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        if is_ident_start(s[i]) && (i == 0 || !is_ident_continue(s[i - 1])) {
            let end = ident_end(s, i);
            let word: String = s[i..end].iter().collect();
            if let Some((keyword, kind)) = KEYWORDS.iter().find(|(kw, _)| *kw == word) {
                let name_start = skip_ws(s, end);
                let name: String = s[name_start..ident_end(s, name_start)].iter().collect();
                match parse_def_at(s, i, keyword.len(), *kind, file) {
                    Ok(Some(def)) => defs.push(def),
                    Ok(None) => {}
                    Err(error) => unparsed.push((name, error.to_string())),
                }
            }
            i = end;
            continue;
        }
        i += 1;
    }
    (defs, unparsed)
}

/// The value of a simple default expression: a number, `true`/`false`, a
/// string literal (optionally followed by `.to_string()`, `.to_owned()`, or
/// `.into()`), `Some(…)`, `None`, or `String::from(…)`. `None` for anything
/// else: such a default cannot be compared with the schema.
fn literal_value(expr: &str) -> Option<Value> {
    let expr = expr.trim().trim_end_matches(';').trim();
    let inner = |prefix: &str| -> Option<&str> {
        expr.strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(')'))
    };
    match expr {
        "true" => return Some(Value::Bool(true)),
        "false" => return Some(Value::Bool(false)),
        "None" => return Some(Value::Null),
        _ => {}
    }
    if let Some(inner) = inner("Some(") {
        return literal_value(inner);
    }
    if let Some(inner) = inner("String::from(") {
        return literal_value(inner);
    }
    if expr.starts_with('"') {
        let chars: Vec<char> = expr.chars().collect();
        let close = string_close(&chars, 0);
        let rest: String = chars[close + 1..].iter().collect();
        let rest = rest.trim();
        let plain = rest.is_empty() || [".to_string()", ".to_owned()", ".into()"].contains(&rest);
        if !plain {
            return None;
        }
        let text: String = chars[1..close].iter().collect();
        return Some(Value::String(unescape(&text)?));
    }
    let negative = expr.starts_with('-');
    let digits = expr.trim_start_matches(['-', '+']).replace('_', "");
    let digits = digits.as_str();
    let numeric = !digits.is_empty()
        && digits.chars().all(|c| c.is_ascii_digit() || c == '.')
        && digits.bytes().filter(|b| *b == b'.').count() <= 1;
    if !numeric {
        return None;
    }
    if digits.contains('.') {
        return expr
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number);
    }
    if negative {
        return expr.parse::<i64>().ok().map(|n| Value::Number(n.into()));
    }
    digits.parse::<u64>().ok().map(|n| Value::Number(n.into()))
}

/// The body of a Rust string literal as the text it denotes: the escape
/// sequences are decoded in one pass. `None` for one that cannot be decoded,
/// so such a default stays unverified instead of being compared with the
/// wrong text.
fn unescape(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '\\' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        match chars.get(i) {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('0') => out.push('\0'),
            Some('\\') | Some('"') | Some('\'') => out.push(chars[i]),
            Some('x') => {
                let digit = |k: usize| chars.get(k).and_then(|c| c.to_digit(16));
                let (high, low) = (digit(i + 1)?, digit(i + 2)?);
                out.push(char::from_u32(high * 16 + low)?);
                i += 2;
            }
            Some('u') => {
                if chars.get(i + 1) != Some(&'{') {
                    return None;
                }
                let end = (i + 2..chars.len()).find(|&k| chars[k] == '}')?;
                let digits: String = chars[i + 2..end].iter().collect();
                out.push(char::from_u32(u32::from_str_radix(&digits, 16).ok()?)?);
                i = end;
            }
            // A backslash before a newline continues the line: the newline
            // and the next line's indentation are not part of the text.
            Some('\n') | Some('\r') => {
                i = skip_ws(&chars, i + 1);
                continue;
            }
            _ => return None,
        }
        i += 1;
    }
    Some(out)
}

/// The parameterless functions of a source whose body is one expression:
/// `(name, value)`; the value is `None` when the body is not a literal.
fn parse_fn_defaults(s: &[char]) -> Vec<(String, Option<Value>)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        if word_at(s, i, "fn") && (i == 0 || !is_ident_continue(s[i - 1])) {
            let name_start = skip_ws(s, i + 2);
            let mut j = ident_end(s, name_start);
            let name: String = s[name_start..j].iter().collect();
            let name_len = name.chars().count();
            if s.get(j) == Some(&'<') {
                j = matching(s, '<', '>', j).unwrap_or(j);
            }
            j = skip_ws(s, j);
            if s.get(j) == Some(&'(') && s.get(j + 1) == Some(&')') {
                // Skip the return type: the body starts at `{` or `=`.
                j = skip_ws(s, j + 2);
                if word_at(s, j, "->") {
                    while j < s.len() && !matches!(s[j], '{' | '=' | ';') {
                        j += 1;
                    }
                }
                let expr: String = if s.get(j) == Some(&'{') {
                    match matching(s, '{', '}', j) {
                        Ok(end) => s[j + 1..end - 1].iter().collect(),
                        Err(_) => String::new(),
                    }
                } else if s.get(j) == Some(&'=') {
                    match s[j + 1..].iter().position(|c| *c == ';') {
                        Some(end) => s[j + 1..j + 1 + end].iter().collect(),
                        None => String::new(),
                    }
                } else {
                    String::new()
                };
                out.push((name, literal_value(&expr)));
            }
            i = name_start + name_len;
            continue;
        }
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Type index
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Resolved<'a> {
    def: &'a Def,
    /// The type was reached through a `Vec<…>` wrapper.
    array: bool,
}

/// What a field type refers to, once its wrappers are peeled.
#[derive(Debug)]
enum Resolution<'a> {
    Resolved(Resolved<'a>),
    /// A scalar or a type of another crate: nothing of ours to read.
    Foreign,
    /// A local definition whose body the parser could not read.
    Unparsed {
        name: String,
        reason: String,
    },
    /// A bare name that several local definitions share.
    Ambiguous {
        name: String,
        files: Vec<String>,
    },
}

/// Parsed definitions of every source file under `src/`.
struct Index {
    /// `file → type name → definition`; the first definition wins.
    defs: BTreeMap<String, BTreeMap<String, Def>>,
    /// `type name → files defining it`.
    files_by_name: BTreeMap<String, Vec<String>>,
    /// Braced definitions whose body did not parse: `(file, name) → reason`.
    unparsed: BTreeMap<(String, String), String>,
    /// `use` imports per file: `(local name, path as written)` in order.
    imports: BTreeMap<String, Vec<(String, String)>>,
    /// Glob imports per file (`use a::b::*;` → `a::b`), in order.
    globs: BTreeMap<String, Vec<String>>,
    /// Parameterless functions whose body is one expression:
    /// `function name → (defining file, literal value)`.
    fn_defaults: BTreeMap<String, Vec<(String, Option<Value>)>>,
    /// Comment-stripped text of each file.
    cleaned: BTreeMap<String, Vec<char>>,
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|entry| entry.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Splits a path at `::` separators that are not nested in a group.
fn split_path(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == ':' && chars.get(i + 1) == Some(&':') && depth <= 0 {
            parts.push(std::mem::take(&mut current));
            i += 2;
            continue;
        }
        match c {
            '{' | '(' | '[' | '<' => depth += 1,
            '}' | ')' | ']' | '>' => depth -= 1,
            _ => {}
        }
        current.push(c);
        i += 1;
    }
    parts.push(current);
    parts
        .into_iter()
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Records the named imports and glob prefixes of one `use` tree (the text
/// between `use` and `;`), expanding nested groups.
fn expand_use(tree: &str, named: &mut Vec<(String, String)>, globs: &mut Vec<String>) {
    let segments = split_path(tree);
    let mut path = String::new();
    for (index, segment) in segments.iter().enumerate() {
        if let Some(group) = segment
            .strip_prefix('{')
            .and_then(|group| group.strip_suffix('}'))
        {
            // A group is the last segment; each item continues the path.
            for item in split_top_level(&group.chars().collect::<Vec<_>>()) {
                expand_use(&format!("{path}{item}"), named, globs);
            }
            return;
        }
        if index + 1 < segments.len() {
            path.push_str(segment);
            path.push_str("::");
            continue;
        }
        if segment == "*" {
            globs.push(path.trim_end_matches("::").to_string());
            continue;
        }
        // A leaf: `name`, or `name as alias`.
        let mut words = segment.split_whitespace();
        let name = words.next().unwrap_or_default().to_string();
        let alias = if words.next() == Some("as") {
            words.next().unwrap_or(&name).to_string()
        } else {
            name.clone()
        };
        named.push((alias, path.clone() + &name));
    }
}

/// The `use` items of a comment-stripped source.
fn parse_uses(s: &[char]) -> (Vec<(String, String)>, Vec<String>) {
    let (mut named, mut globs) = (Vec::new(), Vec::new());
    let (mut depth, mut i) = (0i32, 0);
    while i < s.len() {
        if let Some(end) = literal_end(s, i) {
            i = end;
            continue;
        }
        match s[i] {
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => depth -= 1,
            _ => {}
        }
        let starts_word = is_ident_start(s[i]) && (i == 0 || !is_ident_continue(s[i - 1]));
        if depth == 0 && starts_word && word_at(s, i, "use") {
            let end = ident_end(s, i);
            if end == i + 3 {
                let (mut j, mut inner) = (end, 0i32);
                while j < s.len() {
                    match s[j] {
                        '{' | '(' | '[' => inner += 1,
                        '}' | ')' | ']' => inner -= 1,
                        ';' if inner <= 0 => break,
                        _ => {}
                    }
                    j += 1;
                }
                let tree: String = s[end..j].iter().collect();
                expand_use(&tree, &mut named, &mut globs);
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }
    (named, globs)
}

/// The module a source file declares: `lib.rs` is the crate root, `a/b.rs`
/// is `a::b`, and `a/b/mod.rs` is `a::b` as well.
fn module_of(file: &str) -> Vec<String> {
    let parts: Vec<&str> = file.split('/').collect();
    let last = parts[parts.len() - 1];
    let mut dir: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|part| part.to_string())
        .collect();
    if last != "lib.rs" && last != "mod.rs" {
        dir.push(last.trim_end_matches(".rs").to_string());
    }
    dir
}

/// `lib.rs` for the crate root, else `dirs/mod.rs`.
fn module_file_name(dir: &[String]) -> String {
    if dir.is_empty() {
        "lib.rs".to_string()
    } else {
        format!("{}/mod.rs", dir.join("/"))
    }
}

/// The files of the modules enclosing `file`, nearest first.
fn parent_modules(file: &str) -> Vec<String> {
    let dir = module_of(file);
    (0..dir.len())
        .rev()
        .map(|depth| module_file_name(&dir[..depth]))
        .collect()
}

impl Index {
    fn from_sources(sources: &[(String, String)]) -> Index {
        let mut index = Index {
            defs: BTreeMap::new(),
            files_by_name: BTreeMap::new(),
            unparsed: BTreeMap::new(),
            imports: BTreeMap::new(),
            globs: BTreeMap::new(),
            fn_defaults: BTreeMap::new(),
            cleaned: BTreeMap::new(),
        };
        for (file, source) in sources {
            let cleaned = strip_comments(source);
            let (defs, unparsed) = parse_defs(&cleaned, file);
            for def in defs {
                let first = index
                    .defs
                    .entry(file.clone())
                    .or_default()
                    .insert(def.name.clone(), def.clone())
                    .is_none();
                if first {
                    index
                        .files_by_name
                        .entry(def.name.clone())
                        .or_default()
                        .push(file.clone());
                }
            }
            for (name, reason) in unparsed {
                index.unparsed.entry((file.clone(), name)).or_insert(reason);
            }
            let (named, globs) = parse_uses(&cleaned);
            index.imports.insert(file.clone(), named);
            index.globs.insert(file.clone(), globs);
            for (function, value) in parse_fn_defaults(&cleaned) {
                index
                    .fn_defaults
                    .entry(function)
                    .or_default()
                    .push((file.clone(), value));
            }
            index.cleaned.insert(file.clone(), cleaned);
        }
        index
    }

    fn load() -> Index {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut paths = Vec::new();
        collect_rust_files(&root, &mut paths);
        let sources = paths
            .iter()
            .map(|path| {
                let file = path
                    .strip_prefix(&root)
                    .expect("paths are collected under the root")
                    .to_string_lossy()
                    .into_owned();
                let source = std::fs::read_to_string(path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                (file, source)
            })
            .collect::<Vec<_>>();
        Index::from_sources(&sources)
    }

    fn get(&self, file: &str, name: &str) -> Option<&Def> {
        self.defs.get(file)?.get(name)
    }

    fn cleaned_text(&self, file: &str) -> String {
        self.cleaned
            .get(file)
            .map(|chars| chars.iter().collect())
            .unwrap_or_default()
    }

    /// The file declaring the module at `dir`, when the crate has one
    /// (`a/b.rs`, else `a/b/mod.rs`).
    fn module_file(&self, dir: &[String]) -> Option<String> {
        if dir.is_empty() {
            return self
                .cleaned
                .contains_key("lib.rs")
                .then(|| "lib.rs".to_string());
        }
        let base = dir.join("/");
        [format!("{base}.rs"), format!("{base}/mod.rs")]
            .into_iter()
            .find(|file| self.cleaned.contains_key(file))
    }

    /// The file a path written in `from_file` points at (its last segment is
    /// the item name). `self::` starts at the file's own module and
    /// `super::` steps up through the enclosing ones; the remaining segments
    /// descend from there. `None` for another crate's path or a bare name.
    fn path_file(&self, path: &str, from_file: &str) -> Option<String> {
        let segments: Vec<&str> = path.split("::").collect();
        if segments.len() < 2 {
            return None;
        }
        let (base, consumed) = match segments[0] {
            "crate" => (Vec::new(), 1),
            "self" => (module_of(from_file), 1),
            "super" => {
                let supers = segments.iter().take_while(|s| **s == "super").count();
                let here = module_of(from_file);
                if supers > here.len() {
                    return None;
                }
                (here[..here.len() - supers].to_vec(), supers)
            }
            _ => return None,
        };
        let mut dir = base;
        dir.extend(
            segments[consumed..segments.len() - 1]
                .iter()
                .map(|part| part.to_string()),
        );
        self.module_file(&dir)
    }

    /// The definition `name` in `file`, when it is one of ours.
    fn definition(&self, file: &str, name: &str, array: bool) -> Option<Resolution<'_>> {
        if let Some(def) = self.get(file, name) {
            return Some(Resolution::Resolved(Resolved { def, array }));
        }
        self.unparsed
            .get(&(file.to_string(), name.to_string()))
            .map(|reason| Resolution::Unparsed {
                name: name.to_string(),
                reason: reason.clone(),
            })
    }

    /// The value the function behind `#[serde(default = "path")]` returns,
    /// when its body is one expression the gate can read. The path is read
    /// the way the field's own file sees it: a path rooted in a module
    /// (`crate::`, `self::`, `super::`) is the function that module
    /// declares and nothing else; a bare name is a function of that file,
    /// then one the file imports under that name, then one declared only
    /// once in the crate. Whatever cannot be found that way stays
    /// unverified.
    fn default_fn_value(&self, path: &str, from_file: &str) -> Option<Value> {
        let (qualifiers, name) = path.rsplit_once("::").unwrap_or(("", path));
        let declared_in = |file: &str| {
            self.fn_defaults
                .get(name)?
                .iter()
                .find(|(defined, _)| defined == file)
                .and_then(|(_, value)| value.clone())
        };
        let rooted = qualifiers
            .split("::")
            .next()
            .is_some_and(|first| ["crate", "self", "super"].contains(&first));
        if rooted {
            return declared_in(&self.path_file(path, from_file)?);
        }
        let local = declared_in(from_file);
        if !qualifiers.is_empty() {
            // A type's associated function: only its name is followed.
            return local.or_else(|| self.unique_fn_value(name));
        }
        local
            .or_else(|| {
                let (_, imported) = self
                    .imports
                    .get(from_file)?
                    .iter()
                    .find(|(local, _)| local.as_str() == name)?;
                let file = self.path_file(imported, from_file)?;
                let target = imported.rsplit("::").next()?;
                self.fn_defaults
                    .get(target)?
                    .iter()
                    .find(|(defined, _)| *defined == file)
                    .and_then(|(_, value)| value.clone())
            })
            .or_else(|| self.unique_fn_value(name))
    }

    /// The value of the function `name` when the crate declares exactly one.
    fn unique_fn_value(&self, name: &str) -> Option<Value> {
        // `None` when the crate declares no function of that name, or several.
        match self.fn_defaults.get(name)?.as_slice() {
            [only] => only.1.clone(),
            _ => None,
        }
    }

    /// What a field type refers to: wrappers (`Option`, `Vec`, …) are peeled
    /// and a generic type is resolved by its name.
    ///
    /// A bare name is bound the way the file sees it — the file itself, its
    /// `use` imports, its enclosing modules, its glob imports — and only then
    /// by a crate-wide definition. A name several local definitions share is
    /// reported instead of guessed.
    fn resolve(&self, ty: &str, from_file: &str) -> Resolution<'_> {
        const PEEL: [(&str, bool); 9] = [
            ("Option<", false),
            ("std::option::Option<", false),
            ("core::option::Option<", false),
            ("Box<", false),
            ("std::boxed::Box<", false),
            ("Arc<", false),
            ("std::sync::Arc<", false),
            ("Vec<", true),
            ("std::vec::Vec<", true),
        ];
        let mut t = ty.trim();
        let mut array = false;
        while let Some((prefix, is_array)) = PEEL.iter().find(|(prefix, _)| t.starts_with(*prefix))
        {
            if !t.ends_with('>') {
                break;
            }
            t = t[prefix.len()..t.len() - 1].trim();
            array |= *is_array;
        }
        let (base, generic) = match t.split_once('<') {
            Some((base, _)) => (base.trim(), true),
            None => (t, false),
        };
        if base.is_empty()
            || base.contains(['(', ')', '&', ' '])
            || (!generic && base.contains('>'))
        {
            return Resolution::Foreign;
        }
        // 1. The file itself.
        if let Some(resolution) = self.definition(from_file, base, array) {
            return resolution;
        }
        // 2. What the file imports by that name.
        if !base.contains(':') {
            if let Some(imports) = self.imports.get(from_file) {
                for (local, path) in imports {
                    if local != base {
                        continue;
                    }
                    // An alias renames the type: the path still names it.
                    let target = path.rsplit("::").next().unwrap_or(path);
                    if let Some(file) = self.path_file(path, from_file) {
                        if let Some(resolution) = self.definition(&file, target, array) {
                            return resolution;
                        }
                    }
                }
            }
        } else if let Some(file) = self.path_file(base, from_file) {
            let type_name = base.rsplit("::").next().unwrap_or(base);
            if let Some(resolution) = self.definition(&file, type_name, array) {
                return resolution;
            }
            return Resolution::Foreign;
        }
        // 3. The enclosing modules (`use super::*;`).
        for file in parent_modules(from_file) {
            if let Some(resolution) = self.definition(&file, base, array) {
                return resolution;
            }
        }
        // 4. Glob imports.
        for glob in self.globs.get(from_file).into_iter().flatten() {
            if let Some(file) = self.path_file(&format!("{glob}::{base}"), from_file) {
                if let Some(resolution) = self.definition(&file, base, array) {
                    return resolution;
                }
            }
        }
        // 5. A name defined once in the crate, through an import we cannot
        //    see. Several definitions are an ambiguity, not a choice.
        let files = self
            .files_by_name
            .get(base)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        match files {
            [] => Resolution::Foreign,
            [only] => self
                .definition(only, base, array)
                .unwrap_or(Resolution::Foreign),
            many => Resolution::Ambiguous {
                name: base.to_string(),
                files: many.to_vec(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

/// A node plus every subschema nested through `oneOf`/`anyOf`/`allOf`.
fn schemas(node: &Value) -> Vec<&Value> {
    let mut out = vec![node];
    for key in ["oneOf", "anyOf", "allOf"] {
        if let Some(branches) = node.get(key).and_then(Value::as_array) {
            for branch in branches {
                out.extend(schemas(branch));
            }
        }
    }
    out
}

/// Documented properties of a node (the first branch mentioning a key wins).
fn props(node: &Value) -> BTreeMap<&str, &Value> {
    let mut out = BTreeMap::new();
    for schema in schemas(node) {
        if let Some(entries) = schema.get("properties").and_then(Value::as_object) {
            for (name, sub) in entries {
                out.entry(name.as_str()).or_insert(sub);
            }
        }
    }
    out
}

fn required(node: &Value) -> BTreeSet<&str> {
    let mut out = BTreeSet::new();
    for schema in schemas(node) {
        if let Some(names) = schema.get("required").and_then(Value::as_array) {
            out.extend(names.iter().filter_map(Value::as_str));
        }
    }
    out
}

/// `false` for `additionalProperties`: keys outside `properties` are rejected.
fn is_strict(node: &Value) -> bool {
    schemas(node)
        .iter()
        .any(|schema| schema.get("additionalProperties") == Some(&Value::Bool(false)))
}

/// Whether a node documents anything (an opaque node accepts any key).
fn has_docs(node: &Value) -> bool {
    schemas(node).iter().any(|schema| {
        schema
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|entries| !entries.is_empty())
            || schema
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|names| !names.is_empty())
    })
}

fn find_prop<'a>(node: &'a Value, name: &str) -> Option<&'a Value> {
    schemas(node)
        .into_iter()
        .find_map(|schema| schema.get("properties")?.get(name))
}

/// The string values a node allows through `enum`/`const`, when it lists any.
fn string_values(node: &Value) -> Option<BTreeSet<&str>> {
    let mut out = BTreeSet::new();
    for schema in schemas(node) {
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            out.extend(values.iter().filter_map(Value::as_str));
        }
        if let Some(value) = schema.get("const").and_then(Value::as_str) {
            out.insert(value);
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Direct `oneOf`/`anyOf` branches of a node, or the node itself.
fn direct_branches(node: &Value) -> Vec<&Value> {
    for key in ["oneOf", "anyOf"] {
        if let Some(branches) = node.get(key).and_then(Value::as_array) {
            if !branches.is_empty() {
                return branches.iter().collect();
            }
        }
    }
    vec![node]
}

/// Whether a branch describes an enum variant: by the tag's `const` (or
/// membership in the tag's `enum`) for tagged enums, otherwise by containing
/// every field of the variant.
fn branch_matches(branch: &Value, tag: Option<&str>, variant: &str, fields: &[Field]) -> bool {
    let Some(entries) = branch.get("properties").and_then(Value::as_object) else {
        return false;
    };
    if let Some(discriminator) = tag.and_then(|tag| entries.get(tag)) {
        if let Some(value) = discriminator.get("const").and_then(Value::as_str) {
            return value == variant;
        }
        if let Some(values) = discriminator.get("enum").and_then(Value::as_array) {
            return values.iter().any(|value| value.as_str() == Some(variant));
        }
    }
    fields.iter().all(|field| entries.contains_key(&field.name))
}

/// Whether a node says anything about the keys it accepts: documented
/// properties or requirements, or a closed object — which rejects every key
/// it does not list, even when it lists none.
fn is_documented(node: &Value) -> bool {
    has_docs(node) || is_strict(node)
}

/// The `default` a node advertises for `name`, from whichever schema mentions
/// it.
fn advertised_default<'a>(node: &'a Value, name: &str) -> Option<&'a Value> {
    schemas(node)
        .into_iter()
        .find_map(|schema| schema.get("properties")?.get(name)?.get("default"))
}

/// How many of `fields` a branch documents.
fn branch_overlap(branch: &Value, fields: &[Field]) -> usize {
    let Some(entries) = branch.get("properties").and_then(Value::as_object) else {
        return 0;
    };
    fields
        .iter()
        .filter(|field| entries.contains_key(&field.name))
        .count()
}

/// `s` with every string, character, and raw literal blanked, so text
/// inside a diagnostic is not mistaken for code.
fn blank_literals(s: &[char]) -> Vec<char> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match literal_end(s, i) {
            Some(end) => {
                out.extend(std::iter::repeat_n(' ', end - i));
                i = end;
            }
            None => {
                out.push(s[i]);
                i += 1;
            }
        }
    }
    out
}

/// The keys the schema demands in every configuration it accepts: the
/// node's own `required`, plus everything the `allOf` branches demand (they
/// all apply), plus what every `anyOf`/`oneOf` alternative demands — one
/// lenient alternative is enough to let a configuration through without the
/// key.
fn always_required(node: &Value) -> BTreeSet<String> {
    let mut demanded: BTreeSet<String> = node
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if let Some(branches) = node.get("allOf").and_then(Value::as_array) {
        for branch in branches {
            demanded.extend(always_required(branch));
        }
    }
    for key in ["anyOf", "oneOf"] {
        let mut common: Option<BTreeSet<String>> = None;
        if let Some(branches) = node.get(key).and_then(Value::as_array) {
            for branch in branches {
                let branch_demands = always_required(branch);
                common = Some(match common {
                    None => branch_demands,
                    Some(previous) => previous.intersection(&branch_demands).cloned().collect(),
                });
            }
        }
        demanded.extend(common.unwrap_or_default());
    }
    demanded
}

/// Whether a newtype variant's payload stands for any object — a JSON
/// `Value` or a map — so that no object schema is too lenient for it.
fn decodes_any_object(ty: &str) -> bool {
    let base = ty.split('<').next().unwrap_or(ty);
    matches!(
        base.rsplit("::").next(),
        Some("Value" | "HashMap" | "BTreeMap" | "Map" | "IndexMap")
    )
}

/// The values a node's `name` property selects through `const` or `enum`.
fn selector_values(node: &Value, name: &str) -> Vec<String> {
    let Some(selector) = node.get("properties").and_then(|props| props.get(name)) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    values.extend(selector.get("const"));
    values.extend(
        selector
            .get("enum")
            .and_then(Value::as_array)
            .into_iter()
            .flatten(),
    );
    values
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect()
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExemptionKind {
    /// `HIDDEN_FIELDS`: exempt from R1 and R4.
    Hidden,
    /// `BUILDER_REQUIRED`: exempt from R3.
    BuilderRequired,
}

/// What the gate relied on while checking: the exemption entries it needed,
/// as `(component, struct, field)`, and how many advertised defaults it
/// managed to compare (so "no mismatch" cannot mean "nothing compared").
#[derive(Debug, Default)]
struct ExemptionUsage {
    hidden: BTreeSet<(String, String, String)>,
    builder_required: BTreeSet<(String, String, String)>,
    /// Advertised defaults whose value the gate could compare.
    defaults_compared: usize,
    /// Of those, the ones supplied by a function.
    function_defaults_compared: usize,
}

struct Gate<'a> {
    index: &'a Index,
    /// `kind/name` of the component being checked.
    component: &'a str,
    hidden: Exemptions,
    builder_required: Exemptions,
    /// Receives every exemption the gate relies on, so an entry nobody
    /// needed is reported as stale.
    usage: Option<&'a std::cell::RefCell<ExemptionUsage>>,
    problems: Vec<String>,
}

impl<'a> Gate<'a> {
    fn new(index: &'a Index, component: &'a str) -> Gate<'a> {
        Gate {
            index,
            component,
            hidden: HIDDEN_FIELDS,
            builder_required: BUILDER_REQUIRED,
            usage: None,
            problems: Vec::new(),
        }
    }

    /// Whether `kind` exempts `owner`.`field` for this component. Called only
    /// where a finding is about to be reported, so a hit means the exemption
    /// was needed.
    fn exempt(&self, kind: ExemptionKind, owner: &str, field: &str) -> bool {
        let table = match kind {
            ExemptionKind::Hidden => self.hidden,
            ExemptionKind::BuilderRequired => self.builder_required,
        };
        let hit = table.iter().any(|(component, name, field_name, _)| {
            *component == self.component && *name == owner && *field_name == field
        });
        if hit {
            if let Some(usage) = &self.usage {
                let key = (
                    self.component.to_string(),
                    owner.to_string(),
                    field.to_string(),
                );
                let mut usage = usage.borrow_mut();
                match kind {
                    ExemptionKind::Hidden => {
                        usage.hidden.insert(key);
                    }
                    ExemptionKind::BuilderRequired => {
                        usage.builder_required.insert(key);
                    }
                }
            }
        }
        hit
    }

    fn problem(&mut self, def: &str, path: &str, message: String) {
        self.problems
            .push(format!("{} [{def}] {path}: {message}", self.component));
    }

    /// The local definition the type `ty` (written in `from_file`) refers
    /// to. Foreign types give `None`; a definition the gate cannot read is
    /// reported as R0 under `subject` and gives `None` too.
    fn local_def(
        &mut self,
        owner: &str,
        path: &str,
        subject: &str,
        ty: &str,
        from_file: &str,
    ) -> Option<Resolved<'a>> {
        let resolution = self.index.resolve(ty, from_file);
        match resolution {
            Resolution::Resolved(target) => Some(target),
            Resolution::Foreign => None,
            Resolution::Unparsed { name, reason } => {
                self.problem(
                    owner,
                    path,
                    format!(
                        "R0 `{subject}` cannot be checked: the definition of `{name}` does not \
                         parse ({reason})"
                    ),
                );
                None
            }
            Resolution::Ambiguous { name, files } => {
                let files: Vec<String> = files.iter().map(|file| format!("src/{file}")).collect();
                self.problem(
                    owner,
                    path,
                    format!(
                        "R0 `{subject}` cannot be checked: the bare name `{name}` is defined in {} \
                         — write the field type as a qualified path",
                        files.join(" and ")
                    ),
                );
                None
            }
        }
    }

    /// The serde input fields of a shape, with `flatten` targets inlined. A
    /// flattened type the gate cannot read is reported, never dropped.
    fn shape_fields(
        &mut self,
        owner: &str,
        path: &str,
        fields: &'a [Field],
        from_file: &str,
        depth: usize,
    ) -> Vec<Field> {
        let mut out = Vec::new();
        if depth > 8 {
            return out;
        }
        for field in fields {
            if field.skip {
                continue;
            }
            if field.flatten {
                let subject = format!("{path}.{} (flattened)", field.name);
                if let Some(target) = self.local_def(owner, path, &subject, &field.ty, from_file) {
                    if target.def.kind == DefKind::Struct {
                        out.extend(self.shape_fields(
                            owner,
                            path,
                            &target.def.fields,
                            &target.def.file,
                            depth + 1,
                        ));
                    }
                }
                continue;
            }
            out.push(field.clone());
        }
        out
    }

    fn check_def(&mut self, def: &'a Def, node: &Value, path: &str) {
        if !is_documented(node) {
            return;
        }
        match def.kind {
            DefKind::Struct => {
                let fields = self.shape_fields(&def.name, path, &def.fields, &def.file, 0);
                self.check_shape(&def.name, &fields, node, path, true, &BTreeSet::new());
                self.check_members(&def.name, &def.fields, &def.file, node, path);
            }
            DefKind::Enum => self.check_enum(def, node, path),
        }
    }

    /// Recurses into the nested types behind a shape's fields.
    fn check_members(
        &mut self,
        owner: &str,
        fields: &'a [Field],
        from_file: &str,
        node: &Value,
        path: &str,
    ) {
        for field in fields {
            if field.skip {
                continue;
            }
            if field.flatten {
                let subject = format!("{path}.{} (flattened)", field.name);
                if let Some(target) = self.local_def(owner, path, &subject, &field.ty, from_file) {
                    if target.def.kind == DefKind::Struct {
                        self.check_members(owner, &target.def.fields, &target.def.file, node, path);
                    }
                }
                continue;
            }
            self.check_property(owner, field, node, path, from_file);
        }
    }

    fn check_property(
        &mut self,
        owner: &str,
        field: &Field,
        node: &Value,
        path: &str,
        from_file: &str,
    ) {
        let subject = format!("{path}.{}", field.name);
        let Some(target) = self.local_def(owner, path, &subject, &field.ty, from_file) else {
            return;
        };
        let def = target.def;
        let Some(mut sub) = find_prop(node, &field.name) else {
            return;
        };
        if target.array {
            let Some(items) = sub.get("items").filter(|items| items.is_object()) else {
                return;
            };
            sub = items;
        }
        let deserializable: Vec<&Variant> = def.variants.iter().filter(|v| !v.skip).collect();
        let string_enum = def.kind == DefKind::Enum
            && def
                .variants
                .iter()
                .all(|variant| variant.kind == VariantKind::Other);
        if string_enum {
            if let Some(values) = string_values(sub) {
                // Both directions look at the variants serde can decode: a
                // skipped one is neither expected nor accepted.
                for value in &values {
                    if !deserializable.iter().any(|variant| variant.name == *value) {
                        self.problem(
                            &def.name,
                            &subject,
                            format!(
                                "R6 the schema enum lists `{value}`, which is not a variant of `{}`",
                                def.name
                            ),
                        );
                    }
                }
                for variant in deserializable {
                    if !values.contains(variant.name.as_str()) {
                        self.problem(
                            &def.name,
                            &subject,
                            format!(
                                "R6 variant `{}` of `{}` is missing from the schema enum, so the \
                                 IDE rejects it",
                                variant.name, def.name
                            ),
                        );
                    }
                }
                return;
            }
        }
        self.check_def(def, sub, &subject);
    }

    /// R1–R5 for one field set against one documented schema node. `ghosts`
    /// is off when the node is a merged view whose keys were already checked
    /// against the union of every variant. `outer` lists the keys the node
    /// enclosing a branch demands of every configuration; they bind the
    /// branch too.
    fn check_shape(
        &mut self,
        def: &str,
        fields: &[Field],
        node: &Value,
        path: &str,
        ghosts: bool,
        outer: &BTreeSet<String>,
    ) {
        let props = props(node);
        let required = required(node);
        // A key the schema demands only under `anyOf`/`oneOf` can still be
        // omitted through a lenient alternative — unless the enclosing node
        // demands it.
        let demanded: BTreeSet<String> = outer.union(&always_required(node)).cloned().collect();
        let by_name: BTreeMap<&str, &Field> = fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        if is_strict(node) {
            for field in fields {
                if !props.contains_key(field.name.as_str())
                    && !self.exempt(ExemptionKind::Hidden, def, &field.name)
                {
                    self.problem(
                        def,
                        path,
                        format!(
                            "R1 field `{}` is read from config but missing from the schema, whose \
                             `additionalProperties: false` rejects it — add it to `properties` \
                             (or register it in HIDDEN_FIELDS with a reason)",
                            field.name
                        ),
                    );
                }
            }
        }
        if ghosts {
            for name in props.keys() {
                if !by_name.contains_key(name) {
                    self.problem(
                        def,
                        path,
                        format!(
                            "R2 the schema documents key `{name}` but the config struct never \
                             reads it (serde drops it silently) — remove it from the schema or \
                             add the field"
                        ),
                    );
                }
            }
        }
        for name in required
            .iter()
            .copied()
            .chain(outer.iter().map(String::as_str))
        {
            let Some(field) = by_name.get(name) else {
                // A merged view may list keys that belong to other variants.
                if ghosts && required.contains(name) {
                    self.problem(
                        def,
                        path,
                        format!(
                            "R3 the schema requires `{name}`, which the config struct never reads — no \
                                                     configuration can satisfy it"
                        ),
                    );
                } else if outer.contains(name) {
                    // No branch can lift a requirement of the enclosing node.
                    self.problem(
                        def,
                        path,
                        format!(
                            "R1 the enclosing schema requires `{name}`, which this variant never \
                             reads — it rejects every configuration of the variant"
                        ),
                    );
                }
                continue;
            };
            if field.optional && !self.exempt(ExemptionKind::BuilderRequired, def, name) {
                self.problem(
                    def,
                    path,
                    format!(
                        "R3 the schema marks `{name}` required but serde accepts a config \
                             without it — make the struct require it, drop it from `required`, \
                             or register it in BUILDER_REQUIRED if the builder enforces it"
                    ),
                );
            }
        }
        for field in fields.iter().filter(|field| !field.optional) {
            if !demanded.contains(&field.name)
                && !self.exempt(ExemptionKind::Hidden, def, &field.name)
            {
                let message = if required.contains(field.name.as_str()) {
                    format!(
                        "R4 serde requires `{}` but the schema demands it only in some of its \
                         `anyOf`/`oneOf` branches, so a configuration without it still validates — \
                         demand it in every branch",
                        field.name
                    )
                } else {
                    format!(
                        "R4 serde requires `{}` but the schema does not list it in `required`, so \
                         the IDE passes configs that fail at build — add it to `required` or give \
                         the field a default",
                        field.name
                    )
                };
                self.problem(def, path, message);
            }
        }
        for (name, field) in &by_name {
            let Some(advertised) = advertised_default(node, name) else {
                continue;
            };
            if !field.optional {
                self.problem(
                    def,
                    path,
                    format!(
                        "R5 the schema advertises a default for `{name}` but serde requires the \
                         field — add `#[serde(default …)]` or remove `default`"
                    ),
                );
                continue;
            }
            // Only a default whose value is known can be compared: a
            // function whose body is a literal (a default of the field's
            // own comes first), or a plain `#[serde(default)]` (the type's
            // zero). A default function is looked up from the file the
            // field is declared in.
            let actual = if let Some(function) = &field.default_fn {
                self.index.default_fn_value(function, &field.file)
            } else if field.plain_default {
                plain_default_value(&field.ty)
            } else {
                None
            };
            let Some(actual) = actual else {
                continue;
            };
            if let Some(usage) = &self.usage {
                let mut usage = usage.borrow_mut();
                usage.defaults_compared += 1;
                if field.default_fn.is_some() {
                    usage.function_defaults_compared += 1;
                }
            }
            let source = match &field.default_fn {
                Some(function) => format!("`{function}` returns"),
                None => "`#[serde(default)]` yields".to_string(),
            };
            if !same_default(advertised, &actual) {
                self.problem(
                    def,
                    path,
                    format!(
                        "R5 the schema advertises `default: {advertised}` for `{name}`, but \
                         {source} {actual}"
                    ),
                );
            }
        }
    }

    /// How an enum lays out its variants decides how its schema is read:
    /// externally tagged enums (`{"variant": {…}}`, serde's default) get one
    /// property per variant, internally tagged ones one object per variant
    /// selected by the tag, untagged ones the variant's fields directly.
    /// Coverage is checked on the union of variants first, then each variant
    /// with a decodable payload is checked against the branch that
    /// describes it, and every branch must be a variant serde can decode.
    fn check_enum(&mut self, def: &'a Def, node: &Value, path: &str) {
        struct Shape<'a> {
            name: String,
            /// Fields serde reads for the variant, `flatten` targets inlined.
            fields: Vec<Field>,
            /// The variant's own fields and where their types are written,
            /// for recursion.
            own: &'a [Field],
            file: String,
            /// A struct or struct-valued newtype variant has a field set to
            /// check; unit and tuple variants do not.
            checkable: bool,
            /// A newtype variant whose payload (`Value`, a map) decodes any
            /// object whatever.
            catch_all: bool,
        }
        // Adjacent tagging nests the fields under a content key: not modelled.
        if def.layout == EnumLayout::Adjacent {
            return;
        }
        let tag_name = def.tag.clone().unwrap_or_default();
        let mut shapes: Vec<Shape<'a>> = Vec::new();
        for variant in &def.variants {
            if variant.skip {
                continue;
            }
            let (mut checkable, mut catch_all, mut own, mut file): (bool, bool, &'a [Field], &str) =
                (true, false, &[] as &[Field], "");
            match variant.kind {
                VariantKind::Struct => {
                    own = &variant.fields;
                    file = def.file.as_str();
                }
                VariantKind::Newtype => {
                    // A payload the gate cannot read is reported (R0) and
                    // leaves the variant without a field set to check.
                    let subject = format!("{path}[{}] payload", variant.name);
                    let inner = variant.inner.as_deref();
                    let payload = inner.and_then(|inner| {
                        self.local_def(&def.name, path, &subject, inner, &def.file)
                    });
                    match payload {
                        Some(target) if !target.array && target.def.kind == DefKind::Struct => {
                            own = &target.def.fields;
                            file = target.def.file.as_str();
                        }
                        _ => {
                            checkable = false;
                            catch_all = inner.is_some_and(decodes_any_object);
                        }
                    }
                }
                VariantKind::Other => checkable = false,
            }
            let mut fields = self.shape_fields(&def.name, path, own, file, 0);
            if def.layout == EnumLayout::Internal {
                fields.push(Field {
                    name: tag_name.clone(),
                    ty: "String".to_string(),
                    optional: false,
                    plain_default: false,
                    default_fn: None,
                    skip: false,
                    flatten: false,
                    file: def.file.clone(),
                });
            }
            shapes.push(Shape {
                name: variant.name.clone(),
                fields,
                own,
                file: file.to_string(),
                checkable,
                catch_all,
            });
        }
        let props = props(node);
        if def.layout == EnumLayout::External {
            for key in props.keys() {
                if !shapes.iter().any(|shape| shape.name == *key) {
                    self.problem(
                        &def.name,
                        path,
                        format!(
                            "R2 the schema documents key `{key}` under `{}` but the enum has no \
                             such variant",
                            def.name
                        ),
                    );
                }
            }
            for shape in shapes.iter().filter(|shape| shape.checkable) {
                let Some(sub) = find_prop(node, &shape.name) else {
                    if is_strict(node) {
                        self.problem(
                            &def.name,
                            path,
                            format!(
                                "R1 externally tagged variant `{}` has no property of that name, \
                                 so the schema rejects it",
                                shape.name
                            ),
                        );
                    }
                    continue;
                };
                let variant_path = format!("{path}.{}", shape.name);
                self.check_shape(
                    &def.name,
                    &shape.fields,
                    sub,
                    &variant_path,
                    true,
                    &BTreeSet::new(),
                );
                self.check_members(&def.name, shape.own, &shape.file, sub, path);
            }
            return;
        }
        let mut union: Vec<Field> = Vec::new();
        for shape in &shapes {
            for field in &shape.fields {
                if !union.iter().any(|known| known.name == field.name) {
                    union.push(field.clone());
                }
            }
        }
        if is_strict(node) {
            for field in &union {
                if !props.contains_key(field.name.as_str())
                    && !self.exempt(ExemptionKind::Hidden, &def.name, &field.name)
                {
                    self.problem(
                        &def.name,
                        path,
                        format!(
                            "R1 variant field `{}` is read from config but missing from the \
                             schema, whose `additionalProperties: false` rejects it",
                            field.name
                        ),
                    );
                }
            }
        }
        for name in props.keys() {
            if !union.iter().any(|field| field.name == *name) {
                self.problem(
                    &def.name,
                    path,
                    format!(
                        "R2 the schema documents key `{name}` but no variant of `{}` reads it",
                        def.name
                    ),
                );
            }
        }
        let tag = (def.layout == EnumLayout::Internal).then_some(tag_name.as_str());
        let branches = direct_branches(node);
        // A node without `oneOf` branches is a merged view of every variant:
        // its keys were already checked against the union.
        let merged = branches.len() == 1 && std::ptr::eq(branches[0], node);
        // The node's own `required` binds every branch it has.
        let outer: BTreeSet<String> = if merged {
            BTreeSet::new()
        } else {
            always_required(node)
        };
        // Every discriminator the schema offers must name a variant serde
        // decodes, whatever its shape.
        if tag.is_some() {
            let offered: Vec<String> = if merged {
                selector_values(node, &tag_name)
            } else {
                branches
                    .iter()
                    .flat_map(|branch| selector_values(branch, &tag_name))
                    .collect()
            };
            for value in offered {
                if !shapes.iter().any(|shape| shape.name == value) {
                    self.problem(
                        &def.name,
                        path,
                        format!(
                            "R6 the schema accepts `{tag_name}: {value:?}`, which is not a variant \
                             serde decodes — remove it or correct it"
                        ),
                    );
                }
            }
        }
        for shape in &shapes {
            // Nothing is checked of a unit or tuple variant of an untagged
            // enum: it has no keys.
            if tag.is_none() && !shape.checkable {
                continue;
            }
            // Every branch that selects the variant is checked. For a tagged
            // enum that is each branch whose selector names it: a second,
            // lenient one would let a configuration through that serde
            // rejects. For an untagged enum it is each branch that documents
            // all of its fields, or else the one that documents the most, so
            // a field the schema left out is still reported.
            let mut selecting: Vec<&Value> = branches
                .iter()
                .copied()
                .filter(|branch| branch_matches(branch, tag, &shape.name, &shape.fields))
                .collect();
            if tag.is_none() && selecting.is_empty() {
                if let Some(partial) = branches
                    .iter()
                    .copied()
                    .max_by_key(|branch| branch_overlap(branch, &shape.fields))
                    .filter(|branch| branch_overlap(branch, &shape.fields) > 0)
                {
                    selecting.push(partial);
                }
            }
            let Some(..) = selecting.first() else {
                let how = if tag.is_some() {
                    format!("no `oneOf` entry selects it through `properties.{tag_name}`")
                } else {
                    "no `oneOf` entry lists its fields".to_string()
                };
                self.problem(
                    &def.name,
                    path,
                    format!(
                        "R1 variant `{}` has no schema branch that describes it ({how})",
                        shape.name
                    ),
                );
                continue;
            };
            let variant_path = format!("{path}[{}]", shape.name);
            for branch in selecting {
                if !shape.checkable {
                    // Only the tag is left to demand, and serde requires it
                    // to select any variant — a unit one too.
                    if !outer.contains(&tag_name) && !always_required(branch).contains(&tag_name) {
                        self.problem(
                            &def.name,
                            &variant_path,
                            format!(
                                "R4 serde requires `{tag_name}` to select `{}`, but a schema branch \
                                 that selects it does not demand it — add `{tag_name}` to that \
                                 branch's `required`",
                                shape.name
                            ),
                        );
                    }
                    continue;
                }
                self.check_shape(
                    &def.name,
                    &shape.fields,
                    branch,
                    &variant_path,
                    !merged,
                    &outer,
                );
                self.check_members(&def.name, shape.own, &shape.file, branch, path);
            }
        }
        // Untagged, with real branches: a branch that demands less than any
        // variant requires admits objects serde rejects. While a variant
        // decodes any object, no branch is too lenient.
        if tag.is_none() && !merged && !shapes.iter().any(|shape| shape.catch_all) {
            for (position, branch) in branches.iter().enumerate() {
                let demanded: BTreeSet<String> =
                    outer.union(&always_required(branch)).cloned().collect();
                let decodable = shapes.iter().filter(|shape| shape.checkable).any(|shape| {
                    shape
                        .fields
                        .iter()
                        .filter(|field| !field.optional)
                        .all(|field| demanded.contains(&field.name))
                });
                if !decodable {
                    let keys: Vec<&str> = demanded.iter().map(String::as_str).collect();
                    let demands = if keys.is_empty() {
                        "nothing".to_string()
                    } else {
                        format!("only `{}`", keys.join("`, `"))
                    };
                    self.problem(
                        &def.name,
                        path,
                        format!(
                            "R4 schema branch {} demands {demands}: no variant of `{}` \
                             requires just those keys, so the schema accepts objects serde rejects",
                            position + 1,
                            def.name
                        ),
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The gate over the live registry
// ---------------------------------------------------------------------------

fn row_key(kind: &str, name: &str) -> String {
    format!("{kind}/{name}")
}

fn registered_components() -> BTreeMap<String, Arc<ComponentMetadata>> {
    arkflow_plugin::initialize().expect("component catalogue registers");
    list_components()
        .into_iter()
        .map(|(kind, meta)| (row_key(kind.as_str(), &meta.name), meta))
        .collect()
}

/// Skips whitespace, then consumes `token` if it is next: an identifier
/// (consumed to its boundary), a string literal, or punctuation.
fn take_token(s: &[char], i: &mut usize, token: &str) -> bool {
    *i = skip_ws(s, *i);
    if !word_at(s, *i, token) {
        return false;
    }
    let ends_in_ident = token.chars().last().is_some_and(is_ident_continue);
    let next_is_ident = s
        .get(*i + token.chars().count())
        .is_some_and(|c| is_ident_continue(*c));
    if ends_in_ident && next_is_ident {
        return false;
    }
    *i += token.chars().count();
    true
}

/// The identifier starting after any whitespace at `i`.
fn ident_after_ws(s: &[char], i: usize) -> Option<String> {
    let start = skip_ws(s, i);
    is_ident_start(*s.get(start)?).then(|| s[start..ident_end(s, start)].iter().collect())
}

/// The text of the `impl …Builder` block of the builder a component
/// registers, when both are written in the usual shape:
/// `register_<kind>_builder("<name>", Arc::new(XBuilder))`.
fn builder_block(source: &[char], kind: &str, name: &str) -> Option<String> {
    let trait_name = match kind {
        "input" => "InputBuilder",
        "output" => "OutputBuilder",
        "processor" => "ProcessorBuilder",
        "buffer" => "BufferBuilder",
        "codec" => "CodecBuilder",
        _ => "TemporaryBuilder",
    };
    let registration = format!("register_{kind}_builder");
    let literal = format!("\"{name}\"");
    let starts_word =
        |i: usize| is_ident_start(source[i]) && (i == 0 || !is_ident_continue(source[i - 1]));

    let mut builder = None;
    let mut i = 0;
    while i < source.len() {
        if let Some(end) = literal_end(source, i) {
            i = end;
            continue;
        }
        if starts_word(i) && word_at(source, i, &registration) {
            let mut j = i + registration.len();
            if take_token(source, &mut j, "(")
                && take_token(source, &mut j, &literal)
                && take_token(source, &mut j, ",")
                && take_token(source, &mut j, "Arc::new(")
            {
                builder = ident_after_ws(source, j);
                if builder.is_some() {
                    break;
                }
            }
        }
        i += 1;
    }
    let builder = builder?;

    let mut i = 0;
    while i < source.len() {
        if let Some(end) = literal_end(source, i) {
            i = end;
            continue;
        }
        if starts_word(i) && word_at(source, i, "impl") {
            let mut j = i + "impl".len();
            if take_token(source, &mut j, trait_name)
                && take_token(source, &mut j, "for")
                && take_token(source, &mut j, &builder)
            {
                let open = skip_ws(source, j);
                if source.get(open) == Some(&'{') {
                    let end = matching(source, '{', '}', open).ok()?;
                    return Some(source[open..end].iter().collect());
                }
            }
        }
        i += 1;
    }
    None
}

/// Whether a builder body deserializes `config`: a `let` binding typed as
/// it (`let config: Config = …`) or a turbofish (`from_value::<Config>(…)`).
/// The bound name, the colon, the type, and the `=` are matched as tokens,
/// so the whitespace between them does not matter. String and character
/// literals are blanked first, so a diagnostic that merely mentions the
/// type does not count.
fn reads_config(body: &[char], config: &str) -> bool {
    let s = blank_literals(body);
    let len = config.chars().count();
    // Index just before the whitespace that precedes `at`.
    let back = |mut at: usize| {
        while at > 0 && s[at - 1].is_whitespace() {
            at -= 1;
        }
        at
    };
    // The word that ends just before `at`, and where it starts.
    let word_before = |at: usize| -> Option<(Vec<char>, usize)> {
        let end = back(at);
        let start = (0..end)
            .rev()
            .take_while(|&k| is_ident_continue(s[k]))
            .last()?;
        is_ident_start(s[start]).then(|| (s[start..end].to_vec(), start))
    };
    let word_at_start = |at: usize, word: &[char]| s.get(at..at + word.len()) == Some(word);

    let mut i = 0;
    while i < s.len() {
        let names_it = is_ident_start(s[i])
            && (i == 0 || !is_ident_continue(s[i - 1]))
            && word_at(&s, i, config)
            && !s.get(i + len).is_some_and(|c| is_ident_continue(*c));
        if names_it {
            // What follows: the `=` of a binding, or `>(` closing a turbofish.
            let after = skip_ws(&s, i + len);
            let initializer =
                s.get(after) == Some(&'=') && !matches!(s.get(after + 1), Some('=') | Some('>'));
            let turbofish_close =
                s.get(after) == Some(&'>') && s.get(skip_ws(&s, after + 1)) == Some(&'(');
            // What precedes: `<` and `::` for a turbofish; for a binding a
            // type annotation (`:`, not a path separator) over the `module::`
            // prefixes, then the bound name and a `let` that starts the
            // statement.
            let mut before = back(i);
            let turbofish = s.get(before.wrapping_sub(1)) == Some(&'<') && {
                before = back(before - 1);
                before >= 2 && s[before - 2..before] == [':', ':']
            };
            let mut binding = false;
            if !turbofish {
                while before >= 2 && s[before - 2..before] == [':', ':'] {
                    match word_before(before - 2) {
                        Some((_, earlier)) => before = earlier,
                        None => break,
                    }
                }
                let annotated = back(before);
                if s.get(annotated.wrapping_sub(1)) == Some(&':') {
                    if let Some((_, name_start)) = word_before(annotated - 1) {
                        // `let mut config: …`
                        let statement = match word_before(name_start) {
                            Some((word, earlier)) if word == ['m', 'u', 't'] => earlier,
                            _ => name_start,
                        };
                        // `let` opens the statement: before it is the
                        // start, `;`, `{`, or `}` — not `if` or `while`.
                        let keyword_end = back(statement);
                        let keyword_start = keyword_end.saturating_sub(3);
                        binding =
                            keyword_end >= 3 && word_at_start(keyword_start, &['l', 'e', 't']) && {
                                let before_keyword = back(keyword_start);
                                before_keyword == 0
                                    || matches!(s[before_keyword - 1], ';' | '{' | '}')
                            };
                    }
                }
            }
            if (binding && initializer) || (turbofish && turbofish_close) {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Builds a component from `config` through its real builder.
fn build_component(kind: &str, name: &str, config: serde_json::Value) -> Result<(), String> {
    let resource = arkflow_core::Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    };
    let config = Some(config);
    let kind = match kind {
        "input" => ComponentKind::Input,
        "output" => ComponentKind::Output,
        "processor" => ComponentKind::Processor,
        "buffer" => ComponentKind::Buffer,
        "codec" => ComponentKind::Codec,
        _ => return Err("temporary components are not built here".to_string()),
    };
    match kind {
        ComponentKind::Input => arkflow_core::input::InputConfig {
            input_type: name.to_string(),
            name: None,
            codec: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Output => arkflow_core::output::OutputConfig {
            output_type: name.to_string(),
            name: None,
            codec: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Processor => arkflow_core::processor::ProcessorConfig {
            processor_type: name.to_string(),
            name: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Buffer => arkflow_core::buffer::BufferConfig {
            buffer_type: name.to_string(),
            name: None,
            config: config.clone(),
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Codec => arkflow_core::codec::CodecConfig {
            codec_type: name.to_string(),
            name: None,
            config,
        }
        .build(&resource)
        .map(|_| ()),
        ComponentKind::Temporary => unreachable!("handled above"),
    }
    .map_err(|error| format!("{error}"))
}

/// Probes the claim behind a `BUILDER_REQUIRED` entry: the component's
/// builder must refuse a config that omits the exempt key, and only that.
/// The example itself has to build first — an error it already carries must
/// not pass for the key's omission — and the refusal has to name the key.
/// `Err` explains why the claim does not hold.
fn builder_enforces(
    component: &str,
    meta: &ComponentMetadata,
    field: &str,
) -> Result<String, String> {
    let Some(intact) = meta.config_example.clone() else {
        return Err("the component has no `config_example` to probe its builder with".to_string());
    };
    let mut example = intact.clone();
    match example.as_object_mut() {
        None => return Err("`config_example` is not an object".to_string()),
        Some(object) if !object.contains_key(field) => {
            return Err(format!(
                "`config_example` does not set `{field}`, so the probe cannot take it away"
            ));
        }
        Some(object) => {
            object.remove(field);
        }
    }
    let (kind, name) = component.split_once('/').unwrap_or(("", component));
    judged_probe(
        field,
        build_component(kind, name, intact),
        build_component(kind, name, example),
    )
}

/// What the two builds of the probe show. `intact` has to succeed: an error
/// the example already carries says nothing about the key. Without the key
/// the builder has to refuse, and naming the key — any other refusal is not
/// evidence that the key is what it wants.
fn judged_probe(
    field: &str,
    intact: Result<(), String>,
    without: Result<(), String>,
) -> Result<String, String> {
    if let Err(error) = intact {
        return Err(format!(
            "the builder rejects the intact `config_example` as well ({error}), so nothing shows \
             that the key's omission is what it refuses — fix the example or the builder"
        ));
    }
    match without {
        Ok(()) => Err(format!(
            "the builder accepts a config without `{field}` (probed with `config_example` minus \
             the key), so nothing makes the key required — drop it from the schema's `required` \
             or make the builder reject it"
        )),
        Err(error) if error.contains(field) => Ok(error),
        Err(error) => Err(format!(
            "the builder refuses a config without `{field}` for a reason that does not name it \
             ({error})"
        )),
    }
}

/// Runs the gate over the registered catalogue: every finding, plus the
/// exemption entries it relied on.
fn run_gate() -> (Vec<String>, ExemptionUsage) {
    let registered = registered_components();
    let index = Index::load();
    let usage = std::cell::RefCell::new(ExemptionUsage::default());
    let mut problems = Vec::new();

    for (key, meta) in &registered {
        let mapped = CONFIG_TYPES
            .iter()
            .any(|(kind, name, ..)| row_key(kind, name) == *key);
        let no_config = NO_CONFIG
            .iter()
            .any(|(component, _)| *component == key.as_str());
        if is_documented(&meta.config_schema) && !mapped && !no_config {
            problems.push(format!(
                "{key}: has a configuration schema but no CONFIG_TYPES row in this test — add \
                 `(kind, name, file, config struct)` so the schema is checked"
            ));
        }
    }

    for (kind, name, file, config) in CONFIG_TYPES {
        let key = row_key(kind, name);
        let Some(meta) = registered.get(&key) else {
            problems.push(format!(
                "{key}: CONFIG_TYPES row names a component that is not registered"
            ));
            continue;
        };
        let Some(def) = index.get(file, config) else {
            let why = match index.unparsed.get(&(file.to_string(), config.to_string())) {
                Some(reason) => format!("whose definition does not parse ({reason})"),
                None => "which does not exist — update the row".to_string(),
            };
            problems.push(format!(
                "{key}: CONFIG_TYPES points at `{config}` in `src/{file}`, {why}"
            ));
            continue;
        };
        // Bind the row to the builder registered under this name: that
        // builder's own `impl` block must deserialize the struct. A
        // registration written in another shape keeps the file-level check.
        let source = index.cleaned_text(file);
        let source: Vec<char> = source.chars().collect();
        match builder_block(&source, kind, name)
            .map(|block| reads_config(&block.chars().collect::<Vec<_>>(), config))
        {
            Some(true) => {}
            Some(false) => {
                problems.push(format!(
                    "{key}: the builder registered as `{name}` no longer deserializes `{config}` \
                     — point CONFIG_TYPES at the struct that builder reads"
                ));
                continue;
            }
            None if !reads_config(&source, config) => {
                problems.push(format!(
                    "{key}: `{config}` is no longer deserialized in `src/{file}` — point \
                     CONFIG_TYPES at the struct the builder reads"
                ));
                continue;
            }
            None => {}
        }
        // A mapped component's root must say something about its keys: an
        // opaque root would let the gate check nothing at all.
        if !is_documented(&meta.config_schema) {
            problems.push(format!(
                "{key}: the schema documents none of the keys `{config}` reads — an opaque root \
                 lets the gate check nothing; declare its `properties` and `required` (a \
                 component that takes no configuration is not mapped, see NO_CONFIG)"
            ));
            continue;
        }
        let mut gate = Gate {
            index: &index,
            component: key.as_str(),
            hidden: HIDDEN_FIELDS,
            builder_required: BUILDER_REQUIRED,
            usage: Some(&usage),
            problems: Vec::new(),
        };
        gate.check_def(def, &meta.config_schema, config);
        problems.append(&mut gate.problems);
    }

    // A no-config exemption must still be earned: the schema stays a closed
    // object that documents nothing.
    for (component, reason) in NO_CONFIG {
        match registered.get(*component) {
            None => problems.push(format!(
                "NO_CONFIG names `{component}`, which is not registered"
            )),
            Some(meta) if !(is_strict(&meta.config_schema) && !has_docs(&meta.config_schema)) => {
                problems.push(format!(
                    "stale NO_CONFIG entry `{component}` ({reason}): the schema documents keys \
                     now — add a CONFIG_TYPES row"
                ))
            }
            Some(_) => {}
        }
    }
    // A builder-enforced exemption must still be earned: the builder has to
    // refuse a config that omits the key.
    for (component, owner, field, reason) in BUILDER_REQUIRED {
        if let Some(meta) = registered.get(*component) {
            if let Err(why) = builder_enforces(component, meta, field) {
                problems.push(format!(
                    "stale BUILDER_REQUIRED entry `{component}` / `{owner}.{field}` ({reason}): \
                     {why}"
                ));
            }
        }
    }
    (problems, usage.into_inner())
}

const RULES: &str = "rules: R0 a referenced type cannot be read or is ambiguous · R1 struct \
                     field missing from a strict schema, or demanded by the enclosing schema of a \
                     branch whose variant never reads it · R2 schema key the struct never reads \
                     · R3 schema-required but serde-optional, or required key nothing reads · \
                     R4 serde-required but not demanded by every schema branch, or a tag a \
                     branch does not demand · R5 advertised \
                     default serde never applies, or whose value differs · R6 string enum or \
                     tag value that is not a decodable variant.\n \
                     Exemptions are explicit and verified: HIDDEN_FIELDS (R1/R4), \
                     BUILDER_REQUIRED (R3, the builder must still refuse a config without the \
                     key), and NO_CONFIG in this file, each with a reason.";

#[test]
fn registered_schemas_match_config_structs() {
    let (problems, _) = run_gate();
    assert!(
        problems.is_empty(),
        "component metadata drifted from the config structs:\n{}\n{RULES}",
        problems.join("\n")
    );
}

#[test]
fn exemptions_stay_accurate() {
    let (mut problems, usage) = run_gate();
    let key = |component: &str, owner: &str, field: &str| {
        (component.to_string(), owner.to_string(), field.to_string())
    };
    for (component, owner, field, reason) in HIDDEN_FIELDS {
        if !usage.hidden.contains(&key(component, owner, field)) {
            problems.push(format!(
                "stale HIDDEN_FIELDS entry `{component}` / `{owner}.{field}` ({reason}): nothing \
                 relied on it — the schema documents the field and marks it required where it \
                 applies; remove the entry"
            ));
        }
    }
    for (component, owner, field, reason) in BUILDER_REQUIRED {
        if !usage
            .builder_required
            .contains(&key(component, owner, field))
        {
            problems.push(format!(
                "stale BUILDER_REQUIRED entry `{component}` / `{owner}.{field}` ({reason}): \
                 nothing relied on it — the schema no longer marks it required or serde requires \
                 it now; remove the entry"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "metadata exemption lists drifted:\n{}",
        problems.join("\n")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const DEMO: &str = r##"
/// Demo configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemoConfig {
    pub brokers: Vec<String>,
    #[serde(default)]
    pub flag: bool,
    #[serde(rename = "renamed")]
    pub original: Option<std::collections::HashMap<String, Vec<u8>>>,
    #[serde(skip)]
    pub internal: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
    pub nested: Option<Extra>,
    pub many: Vec<Extra>,
    pub mode: Mode,
    pub kind: Kind,
    pub destination: Expr<String>,
    /// Commas and `->` inside the type must not split the field.
    pub handler: Option<fn(a: u8, b: u8) -> bool>,
    /// A custom deserializer runs only for a key that is present.
    #[serde(deserialize_with = "read_id")]
    pub custom: Option<String>,
    #[serde(flatten)]
    pub(crate) crate_wide: Crate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Extra {
    pub host: String,
    pub port: Option<u16>,
    /// A fixed-size array: its type text carries a `;`.
    nonce: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Mode {
    Cluster { urls: Vec<String> },
    Single { url: String, #[serde(default)] insecure: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Fail,
    Skip,
    #[serde(skip_deserializing)]
    Legacy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Expr<T> {
    Expr { expr: String },
    Value { value: T },
}

/// A restricted visibility must not hide the container attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Crate {
    enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WithUnit {
    Off,
    File { path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum Untagged {
    File { path: String, format: String },
    Socket { port: u16 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "UPPERCASE")]
enum Upper {
    File {
        #[allow(non_snake_case)]
        pathName: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Camel {
    retry_count: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default = "make_loud")]
struct LoudContainer {
    retries: u8,
}

struct Borrowed<'a> {
    value: &'a str,
}

struct Tuple(String);

struct Unit;
"##;

    const ENDPOINT: &str = r#"
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum Endpoint {
    Tcp { host: String, port: u16 },
    Unix { host: String, path: String },
}
"#;

    fn demo_index() -> Index {
        Index::from_sources(&[("input/demo.rs".to_string(), DEMO.to_string())])
    }

    fn field<'a>(def: &'a Def, name: &str) -> &'a Field {
        def.fields
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("field `{name}` is parsed"))
    }

    fn conforming() -> Value {
        let extra = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "host": {"type": "string"}, "port": {"type": "integer"},
                "nonce": {"type": "array"}
            },
            "required": ["host", "nonce"]
        });
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "brokers": {"type": "array", "items": {"type": "string"}},
                "flag": {"type": "boolean", "default": false},
                "renamed": {"type": "object"},
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "nonce": {"type": "array"},
                "nested": extra,
                "many": {"type": "array", "items": extra},
                "handler": {},
                "custom": {"type": "string"},
                "enabled": {"type": "boolean", "default": false},
                "mode": {"type": "object", "oneOf": [
                    {"properties": {"type": {"const": "cluster"}, "urls": {"type": "array"}},
                     "required": ["type", "urls"]},
                    {"properties": {"type": {"const": "single"}, "url": {"type": "string"},
                                    "insecure": {"type": "boolean", "default": false}},
                     "required": ["type", "url"]}
                ]},
                "kind": {"type": "string", "enum": ["fail", "skip"]},
                "destination": {"type": "object", "oneOf": [
                    {"properties": {"type": {"const": "expr"}, "expr": {"type": "string"}},
                     "required": ["type", "expr"]},
                    {"properties": {"type": {"const": "value"}, "value": {"type": "string"}},
                     "required": ["type", "value"]}
                ]}
            },
            // `flag`, `port`, and `enabled` are omitted: `enabled` through the
            // container default of a `pub(crate)` struct.
            "required": ["brokers", "host", "nonce", "many", "mode", "kind", "destination", "custom"]
        })
    }

    fn problems(index: &Index, config: &str, schema: &Value) -> Vec<String> {
        let def = index
            .get("input/demo.rs", config)
            .unwrap_or_else(|| panic!("`{config}` is parsed"));
        let mut gate = Gate::new(index, "input/demo");
        gate.check_def(def, schema, config);
        gate.problems
    }

    fn reported(problems: &[String], needle: &str) -> bool {
        problems.iter().any(|problem| problem.contains(needle))
    }

    #[test]
    fn parser_reads_fields_with_their_serde_attributes() {
        let index = demo_index();
        let demo = index.get("input/demo.rs", "DemoConfig").unwrap();
        let names: Vec<&str> = demo.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "brokers",
                "flag",
                "renamed",
                "internal",
                "extra",
                "nested",
                "many",
                "mode",
                "kind",
                "destination",
                "handler",
                "custom",
                "crate_wide",
            ]
        );
        // Option<_> and #[serde(default …)] make a field optional; a bare
        // `bool`/`Vec<_>` does not.
        for name in ["flag", "renamed", "nested", "handler"] {
            assert!(field(demo, name).optional, "{name} is optional");
        }
        for name in [
            "brokers",
            "extra",
            "many",
            "mode",
            "kind",
            "destination",
            "custom",
        ] {
            assert!(!field(demo, name).optional, "{name} is required");
        }
        // `deserialize_with` only runs for a key that is present.
        assert_eq!(field(demo, "custom").ty, "Option<String>");
        assert!(field(demo, "flag").plain_default);
        assert!(!field(demo, "nested").plain_default);
        assert!(field(demo, "internal").skip);
        assert!(field(demo, "extra").flatten);
        assert_eq!(
            field(demo, "handler").ty,
            "Option<fn(a: u8, b: u8) -> bool>"
        );
        assert_eq!(
            field(demo, "renamed").ty,
            "Option<std::collections::HashMap<String, Vec<u8>>>"
        );
        // A fixed-size array type is read like any other.
        let extra = index.get("input/demo.rs", "Extra").unwrap();
        assert_eq!(extra.fields[2].name, "nonce");
        assert_eq!(extra.fields[2].ty, "[u8; 16]");
        // The container attributes of a `pub(crate)` struct survive.
        let crate_wide = index.get("input/demo.rs", "Crate").unwrap();
        assert!(field(crate_wide, "enabled").optional);
        // The value of a container default is the struct's `Default`, which
        // the gate cannot read: it is not compared with a zero.
        assert!(!field(crate_wide, "enabled").plain_default);
        // serde's field conversion differs from its variant conversion …
        let camel = index.get("input/demo.rs", "Camel").unwrap();
        assert_eq!(camel.fields[0].name, "retryCount");
        // … and a function-backed container default lets every key be
        // omitted just like a bare one.
        let loud = index.get("input/demo.rs", "LoudContainer").unwrap();
        assert!(field(loud, "retries").optional);
        assert!(!field(loud, "retries").plain_default);

        let mode = index.get("input/demo.rs", "Mode").unwrap();
        assert_eq!(mode.kind, DefKind::Enum);
        assert_eq!(mode.tag.as_deref(), Some("type"));
        assert_eq!(mode.layout, EnumLayout::Internal);
        let names: Vec<&str> = mode.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["cluster", "single"]);
        assert!(!mode.variants[1].fields[0].optional);
        assert!(mode.variants[1].fields[1].optional);

        let kind = index.get("input/demo.rs", "Kind").unwrap();
        assert_eq!(kind.layout, EnumLayout::External);
        assert_eq!(
            kind.variants
                .iter()
                .map(|v| v.name.as_str())
                .collect::<Vec<_>>(),
            ["fail", "skip", "legacy"]
        );
        assert!(kind.variants[2].skip);

        // An enum's `rename_all` renames variants, never variant fields.
        let upper = index.get("input/demo.rs", "Upper").unwrap();
        assert_eq!(upper.variants[0].name, "FILE");
        assert_eq!(upper.variants[0].fields[0].name, "pathName");

        assert_eq!(
            index.get("input/demo.rs", "Untagged").unwrap().layout,
            EnumLayout::Untagged
        );

        let borrowed = index.get("input/demo.rs", "Borrowed").unwrap();
        assert_eq!(borrowed.fields[0].ty, "&'a str");
        // Tuple and unit structs have no named fields: they are not indexed.
        assert!(index.get("input/demo.rs", "Tuple").is_none());
        assert!(index.get("input/demo.rs", "Unit").is_none());
    }

    #[test]
    fn comments_and_literals_never_become_definitions() {
        let source = r##"
// struct Fake { a: u8 }
/* enum Block { A } */
const TEXT: &str = "struct Quoted { b: u8 }";
const RAW: &str = r#"enum RawEnum { C }"#;
const CHARS: (char, char, char) = ('x', '\'', '"');
fn lifetime<'a>(value: &'a str) -> &'a str { value }
#[derive(Debug)]
pub struct Real { pub x: u8 } // trailing struct Trailing { }
"##;
        let cleaned = strip_comments(source);
        // Positions are preserved: only comment bytes are blanked.
        assert_eq!(cleaned.len(), source.chars().count());
        assert!(!cleaned.iter().collect::<String>().contains("Fake {"));
        let index = Index::from_sources(&[("lib.rs".to_string(), source.to_string())]);
        let names: Vec<&str> = index.defs["lib.rs"].keys().map(String::as_str).collect();
        assert_eq!(names, ["Real"]);
        assert_eq!(index.get("lib.rs", "Real").unwrap().fields[0].name, "x");
    }

    #[test]
    fn rename_rules_match_serde() {
        macro_rules! rule_enum {
            ($name:ident, $rule:expr) => {
                #[derive(serde::Serialize)]
                #[serde(rename_all = $rule)]
                #[allow(non_camel_case_types, dead_code, clippy::enum_variant_names)]
                enum $name {
                    HTTPServer,
                    KeyShared,
                    Field2Name,
                    ABc,
                    AlreadySnake,
                    Snake_Input,
                    Kebab_Input,
                    A,
                }
            };
        }
        rule_enum!(Lower, "lowercase");
        rule_enum!(Upper, "UPPERCASE");
        rule_enum!(Pascal, "PascalCase");
        rule_enum!(Camel, "camelCase");
        rule_enum!(Snake, "snake_case");
        rule_enum!(ScreamingSnake, "SCREAMING_SNAKE_CASE");
        rule_enum!(Kebab, "kebab-case");
        rule_enum!(ScreamingKebab, "SCREAMING-KEBAB-CASE");
        let name_of = |value: serde_json::Value| value.as_str().unwrap().to_string();

        for (rule, cases) in [
            (
                "lowercase",
                vec![
                    ("HTTPServer", name_of(json!(Lower::HTTPServer))),
                    ("Snake_Input", name_of(json!(Lower::Snake_Input))),
                    ("Kebab_Input", name_of(json!(Lower::Kebab_Input))),
                ],
            ),
            (
                "UPPERCASE",
                vec![
                    ("HTTPServer", name_of(json!(Upper::HTTPServer))),
                    ("Snake_Input", name_of(json!(Upper::Snake_Input))),
                    ("Kebab_Input", name_of(json!(Upper::Kebab_Input))),
                ],
            ),
            (
                "PascalCase",
                vec![
                    ("HTTPServer", name_of(json!(Pascal::HTTPServer))),
                    ("Snake_Input", name_of(json!(Pascal::Snake_Input))),
                ],
            ),
            (
                "camelCase",
                vec![
                    ("HTTPServer", name_of(json!(Camel::HTTPServer))),
                    ("Snake_Input", name_of(json!(Camel::Snake_Input))),
                    ("Field2Name", name_of(json!(Camel::Field2Name))),
                ],
            ),
            (
                "snake_case",
                vec![
                    ("HTTPServer", name_of(json!(Snake::HTTPServer))),
                    ("KeyShared", name_of(json!(Snake::KeyShared))),
                    ("Field2Name", name_of(json!(Snake::Field2Name))),
                    ("ABc", name_of(json!(Snake::ABc))),
                    ("AlreadySnake", name_of(json!(Snake::AlreadySnake))),
                    ("Snake_Input", name_of(json!(Snake::Snake_Input))),
                    ("A", name_of(json!(Snake::A))),
                ],
            ),
            (
                "SCREAMING_SNAKE_CASE",
                vec![
                    ("HTTPServer", name_of(json!(ScreamingSnake::HTTPServer))),
                    ("Snake_Input", name_of(json!(ScreamingSnake::Snake_Input))),
                ],
            ),
            (
                "kebab-case",
                vec![
                    ("HTTPServer", name_of(json!(Kebab::HTTPServer))),
                    ("KeyShared", name_of(json!(Kebab::KeyShared))),
                    ("Snake_Input", name_of(json!(Kebab::Snake_Input))),
                ],
            ),
            (
                "SCREAMING-KEBAB-CASE",
                vec![
                    ("HTTPServer", name_of(json!(ScreamingKebab::HTTPServer))),
                    ("Snake_Input", name_of(json!(ScreamingKebab::Snake_Input))),
                ],
            ),
        ] {
            let rule = rename_rule(rule).unwrap();
            for (rust, serde_name) in cases {
                assert_eq!(
                    rule.apply_to_variant(rust),
                    serde_name,
                    "{rule:?} applied to the variant {rust}"
                );
            }
        }
        assert!(rename_rule("Title Case").is_none());

        // Fields follow another conversion: serde treats the Rust conventions
        // as the identity and works on `_` word boundaries for the rest.
        macro_rules! rule_struct {
            ($name:ident, $rule:expr) => {
                #[derive(serde::Serialize)]
                #[serde(rename_all = $rule)]
                #[allow(non_snake_case, dead_code)]
                struct $name {
                    HTTPServer: u8,
                    retry_count: u8,
                    Snake_Input: u8,
                    pathName: u8,
                    a: u8,
                }
            };
        }
        rule_struct!(FieldLower, "lowercase");
        rule_struct!(FieldUpper, "UPPERCASE");
        rule_struct!(FieldPascal, "PascalCase");
        rule_struct!(FieldCamel, "camelCase");
        rule_struct!(FieldSnake, "snake_case");
        rule_struct!(FieldScreamingSnake, "SCREAMING_SNAKE_CASE");
        rule_struct!(FieldKebab, "kebab-case");
        rule_struct!(FieldScreamingKebab, "SCREAMING-KEBAB-CASE");
        let names = ["HTTPServer", "retry_count", "Snake_Input", "pathName", "a"];
        // The key of the field whose value is `n`.
        let key_of = |value: &serde_json::Value, n: u64| {
            value
                .as_object()
                .unwrap()
                .iter()
                .find(|(_, field)| field.as_u64() == Some(n))
                .expect("every fixture field is serialized")
                .0
                .clone()
        };
        for (rule, value) in [
            (
                "lowercase",
                json!(FieldLower {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "UPPERCASE",
                json!(FieldUpper {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "PascalCase",
                json!(FieldPascal {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "camelCase",
                json!(FieldCamel {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "snake_case",
                json!(FieldSnake {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "SCREAMING_SNAKE_CASE",
                json!(FieldScreamingSnake {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "kebab-case",
                json!(FieldKebab {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
            (
                "SCREAMING-KEBAB-CASE",
                json!(FieldScreamingKebab {
                    HTTPServer: 1,
                    retry_count: 2,
                    Snake_Input: 3,
                    pathName: 4,
                    a: 5
                }),
            ),
        ] {
            let rule = rename_rule(rule).unwrap();
            for (index, rust) in names.iter().enumerate() {
                assert_eq!(
                    rule.apply_to_field(rust),
                    key_of(&value, index as u64 + 1),
                    "{rule:?} applied to the field {rust}"
                );
            }
        }

        // An enum's `rename_all` renames its variants. The fields of a
        // struct variant follow the variant's own rule, else the enum's
        // `rename_all_fields`.
        #[derive(serde::Serialize)]
        #[serde(tag = "t", rename_all = "UPPERCASE", rename_all_fields = "kebab-case")]
        #[allow(dead_code)]
        enum VariantRules {
            File {
                path_name: String,
            },
            #[serde(rename_all = "camelCase")]
            Socket {
                port_number: String,
            },
        }
        assert_eq!(
            serde_json::to_value(VariantRules::File {
                path_name: "x".to_string()
            })
            .unwrap(),
            json!({"t": "FILE", "path-name": "x"})
        );
        assert_eq!(
            serde_json::to_value(VariantRules::Socket {
                port_number: "x".to_string()
            })
            .unwrap(),
            json!({"t": "SOCKET", "portNumber": "x"})
        );
    }

    #[test]
    fn a_conforming_schema_reports_nothing() {
        assert!(problems(&demo_index(), "DemoConfig", &conforming()).is_empty());
    }

    #[test]
    fn every_drift_class_is_reported() {
        let index = demo_index();

        let mut schema = conforming();
        schema["properties"].as_object_mut().unwrap().remove("flag");
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R1 field `flag`"
        ));

        let mut schema = conforming();
        schema["properties"]["ghost"] = json!({"type": "string"});
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R2 the schema documents key `ghost`"
        ));

        let mut schema = conforming();
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("port"));
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R3 the schema marks `port` required"
        ));

        // A required key nothing reads can never be satisfied.
        let mut schema = conforming();
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("typo"));
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R3 the schema requires `typo`"
        ));

        // An `Option` behind a custom deserializer is still required.
        let mut schema = conforming();
        schema["required"].as_array_mut().unwrap().remove(7);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R4 serde requires `custom`"
        ));

        let mut schema = conforming();
        schema["required"].as_array_mut().unwrap().remove(0);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R4 serde requires `brokers`"
        ));

        let mut schema = conforming();
        schema["properties"]["brokers"]["default"] = json!([]);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R5 the schema advertises a default for `brokers`"
        ));

        // An advertised default must be the value serde produces.
        let mut schema = conforming();
        schema["properties"]["flag"]["default"] = json!(true);
        let found = problems(&index, "DemoConfig", &schema);
        assert!(
            reported(
                &found,
                "R5 the schema advertises `default: true` for `flag`"
            ),
            "{found:?}"
        );

        let mut schema = conforming();
        schema["properties"]["kind"]["enum"] = json!(["fail"]);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R6 variant `skip` of `Kind` is missing"
        ));

        let mut schema = conforming();
        schema["properties"]["kind"]["enum"] = json!(["fail", "skip", "bogus"]);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R6 the schema enum lists `bogus`"
        ));

        // A variant serde skips is neither expected nor accepted.
        let mut schema = conforming();
        schema["properties"]["kind"]["enum"] = json!(["fail", "skip", "legacy"]);
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R6 the schema enum lists `legacy`"
        ));

        // A flattened field is part of the root shape.
        let mut schema = conforming();
        schema["properties"].as_object_mut().unwrap().remove("host");
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R1 field `host`"
        ));

        // Losing the container default of a `pub(crate)` struct would make
        // `enabled` required, and requiring it would then be correct: the
        // gate must still object.
        let mut schema = conforming();
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("enabled"));
        assert!(
            reported(
                &problems(&index, "DemoConfig", &schema),
                "R3 the schema marks `enabled` required"
            ),
            "the container `#[serde(default)]` of a `pub(crate)` struct is kept"
        );
        // A container default's value is not compared with a zero.
        let mut schema = conforming();
        schema["properties"]["enabled"]["default"] = json!(true);
        assert!(problems(&index, "DemoConfig", &schema).is_empty());

        // Nested nodes recurse, including through array `items`.
        let mut schema = conforming();
        schema["properties"]["many"]["items"]["properties"]
            .as_object_mut()
            .unwrap()
            .remove("host");
        let found = problems(&index, "DemoConfig", &schema);
        assert!(
            reported(&found, "DemoConfig.many: R1 field `host`"),
            "{found:?}"
        );

        // Each tagged variant is checked against its own branch.
        let mut schema = conforming();
        schema["properties"]["mode"]["oneOf"][1]["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("insecure"));
        let found = problems(&index, "DemoConfig", &schema);
        assert!(
            reported(
                &found,
                "DemoConfig.mode[single]: R3 the schema marks `insecure` required"
            ),
            "{found:?}"
        );

        // A generic type is resolved by its name, so a tagged expression
        // typed `Expr<String>` is checked like any other enum.
        let mut schema = conforming();
        schema["properties"]["destination"] = json!({
            "type": "object",
            "properties": {"type": {"enum": ["expr", "value"]},
                           "expr": {"type": "string"}, "value": {"type": "string"}},
            "required": ["type"]
        });
        let found = problems(&index, "DemoConfig", &schema);
        assert!(
            reported(
                &found,
                "DemoConfig.destination[expr]: R4 serde requires `expr`"
            ) && reported(
                &found,
                "DemoConfig.destination[value]: R4 serde requires `value`"
            ),
            "{found:?}"
        );
    }

    #[test]
    fn enum_edges_are_still_checked() {
        let index = demo_index();
        let check = |config: &str, schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(index.get("input/demo.rs", config).unwrap(), &schema, config);
            gate.problems
        };

        // A unit sibling does not switch off the checks for the other variants.
        let found = check(
            "WithUnit",
            json!({"oneOf": [
                {"properties": {"type": {"const": "off"}}},
                {"properties": {"type": {"const": "file"}, "path": {"type": "string"}},
                 "required": ["type"]}
            ]}),
        );
        assert!(
            reported(&found, "WithUnit[file]: R4 serde requires `path`"),
            "{found:?}"
        );

        // Untagged: a branch that documents most of a variant is still its
        // branch, so the field it left out is reported.
        let found = check(
            "Untagged",
            json!({"oneOf": [
                {"properties": {"path": {"type": "string"}}, "required": ["path"]},
                {"properties": {"port": {"type": "integer"}}, "required": ["port"]}
            ]}),
        );
        assert!(
            reported(&found, "Untagged[File]: R4 serde requires `format`"),
            "{found:?}"
        );
        // A closed branch also reports the field it does not document.
        let mut closed = json!({"oneOf": [
            {"properties": {"path": {"type": "string"}}, "required": ["path", "format"],
             "additionalProperties": false},
            {"properties": {"port": {"type": "integer"}}, "required": ["port"]}
        ]});
        let found = check("Untagged", closed.clone());
        assert!(
            reported(&found, "Untagged[File]: R1 field `format`"),
            "{found:?}"
        );
        closed["oneOf"][0]["properties"]["format"] = json!({"type": "string"});
        let found = check("Untagged", closed);
        assert!(found.is_empty(), "{found:?}");
        let found = check(
            "Untagged",
            json!({"anyOf": [
                {"properties": {"path": {"type": "string"}, "format": {"type": "string"}},
                 "required": ["path", "format"]},
                {"properties": {"port": {"type": "integer"}}, "required": ["port"]}
            ]}),
        );
        assert!(found.is_empty(), "{found:?}");
        // No branch describes a variant at all.
        let found = check(
            "Untagged",
            json!({"anyOf": [
                {"properties": {"port": {"type": "integer"}}, "required": ["port"]}
            ]}),
        );
        assert!(
            reported(&found, "Untagged: R1 variant `File` has no schema branch"),
            "{found:?}"
        );

        // An enum's `rename_all` renames variants only: the discriminator is
        // `FILE`, the field stays `pathName`.
        let found = check(
            "Upper",
            json!({"oneOf": [{"properties": {"t": {"const": "FILE"}, "pathName": {"type": "string"}},
                              "required": ["t", "pathName"]}]}),
        );
        assert!(found.is_empty(), "{found:?}");
        let found = check(
            "Upper",
            json!({"oneOf": [{"properties": {"t": {"const": "FILE"}, "pathname": {"type": "string"}},
                              "required": ["t", "pathname"]}]}),
        );
        assert!(
            reported(&found, "Upper: R2 the schema documents key `pathname`")
                && reported(&found, "Upper[FILE]: R3 the schema requires `pathname`")
                && reported(&found, "Upper[FILE]: R4 serde requires `pathName`"),
            "{found:?}"
        );

        // Without a tag the same enum is externally tagged: one property per
        // variant, named by the (renamed) variant.
        let index = Index::from_sources(&[(
            "input/demo.rs".to_string(),
            "#[serde(rename_all = \"UPPERCASE\")]\nenum Plain { File { pathName: String } }"
                .to_string(),
        )]);
        let mut gate = Gate::new(&index, "input/demo");
        let schema = json!({"properties": {"FILE": {"properties": {"pathName": {"type": "string"}},
                                                    "required": ["pathName"]}}});
        gate.check_def(
            index.get("input/demo.rs", "Plain").unwrap(),
            &schema,
            "Plain",
        );
        assert!(gate.problems.is_empty(), "{:?}", gate.problems);

        // A closed schema that documents nothing rejects every field: it is
        // not the same as taking no configuration.
        let found = check(
            "Upper",
            json!({"type": "object", "additionalProperties": false}),
        );
        assert!(
            reported(&found, "R1 variant `FILE` has no schema branch"),
            "{found:?}"
        );
    }

    #[test]
    fn unreadable_and_ambiguous_types_are_reported_not_skipped() {
        // A local definition whose body does not parse is reported where a
        // documented field refers to it.
        let index = Index::from_sources(&[(
            "input/demo.rs".to_string(),
            r#"
struct Holder { broken: Option<Broken> }
struct Broken { broken_field, }
"#
            .to_string(),
        )]);
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Holder").unwrap(),
            &json!({"type": "object", "properties": {"broken": {"type": "object", "properties": {"x": {}}}}}),
            "Holder",
        );
        assert!(
            reported(
                &gate.problems,
                "R0 `Holder.broken` cannot be checked: the definition of `Broken` does not parse"
            ),
            "{:?}",
            gate.problems
        );
        assert!(index.get("input/demo.rs", "Broken").is_none());
        assert!(index
            .unparsed
            .contains_key(&("input/demo.rs".to_string(), "Broken".to_string())));

        // A bare name shared by two definitions is an ambiguity, not a choice.
        let sources: Vec<(String, String)> = [
            (
                "a.rs".to_string(),
                "pub struct Options { a: u8 }".to_string(),
            ),
            (
                "z.rs".to_string(),
                "pub struct Options { b: u8 }".to_string(),
            ),
            (
                "input/demo.rs".to_string(),
                "struct Holder { options: Options }".to_string(),
            ),
        ]
        .to_vec();
        let index = Index::from_sources(&sources);
        let schema = json!({"type": "object", "properties": {"options": {"type": "object", "properties": {"b": {}},
            "required": ["b"]}}, "required": ["options"]});
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Holder").unwrap(),
            &schema,
            "Holder",
        );
        assert!(
            reported(&gate.problems, "is defined in src/a.rs and src/z.rs"),
            "{:?}",
            gate.problems
        );

        // A `use` import binds the name.
        let mut sources = sources;
        sources[2].1 = "use crate::z::Options;\nstruct Holder { options: Options }".to_string();
        let index = Index::from_sources(&sources);
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Holder").unwrap(),
            &schema,
            "Holder",
        );
        assert!(gate.problems.is_empty(), "{:?}", gate.problems);

        // `use` trees are expanded: groups, nested groups, and aliases.
        let index = Index::from_sources(&[(
            "lib.rs".to_string(),
            "use a::b::{X, c::{Y, Z as W}};\nuse d::e::*;\nuse f::g::V as U;\nstruct S { v: u8 }"
                .to_string(),
        )]);
        let named = &index.imports["lib.rs"];
        for (local, path) in [
            ("X", "a::b::X"),
            ("Y", "a::b::c::Y"),
            ("W", "a::b::c::Z"),
            ("U", "f::g::V"),
        ] {
            assert!(
                named
                    .iter()
                    .any(|(name, imported)| name == local && imported == path),
                "{local} → {path} is imported ({named:?})"
            );
        }
        assert_eq!(index.globs["lib.rs"], vec!["d::e".to_string()]);
    }

    #[test]
    fn exemptions_apply_and_are_scoped() {
        let index = demo_index();
        let def = index.get("input/demo.rs", "DemoConfig").unwrap();
        let mut schema = conforming();
        schema["properties"]
            .as_object_mut()
            .unwrap()
            .remove("brokers");
        schema["required"].as_array_mut().unwrap().remove(0);

        // Without an exemption both drifts are reported (R1 and R4).
        let mut plain = Gate::new(&index, "input/demo");
        plain.check_def(def, &schema, "DemoConfig");
        assert_eq!(plain.problems.len(), 2, "{:?}", plain.problems);

        // A hidden field is exempt from R1 and R4 …
        let mut hidden = Gate::new(&index, "input/demo");
        hidden.hidden = &[("input/demo", "DemoConfig", "brokers", "test")];
        hidden.check_def(def, &schema, "DemoConfig");
        assert!(hidden.problems.is_empty(), "{:?}", hidden.problems);

        // … but only for the component and struct it names.
        let mut stranger = Gate::new(&index, "input/other");
        stranger.hidden = hidden.hidden;
        stranger.check_def(def, &schema, "DemoConfig");
        assert_eq!(stranger.problems.len(), 2, "{:?}", stranger.problems);

        // A builder-enforced key is exempt from R3.
        let mut schema = conforming();
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("port"));
        let mut builder = Gate::new(&index, "input/demo");
        builder.builder_required = &[("input/demo", "DemoConfig", "port", "test")];
        builder.check_def(def, &schema, "DemoConfig");
        assert!(builder.problems.is_empty(), "{:?}", builder.problems);
    }

    #[test]
    fn externally_tagged_enums_read_one_property_per_variant() {
        let source = r#"
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Backend {
    Http(HttpCfg),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HttpCfg {
    url: String,
    #[serde(default = "default_method")]
    method: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Db {
    Mysql(Conn),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Conn {
    uri: String,
    ssl: Option<Ssl>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ssl {
    mode: String,
}
"#;
        let index = Index::from_sources(&[("lookup.rs".to_string(), source.to_string())]);
        let backend = index.get("lookup.rs", "Backend").unwrap();
        let run = |schema: &Value| {
            let mut gate = Gate::new(&index, "processor/lookup");
            gate.check_def(backend, schema, "LookupConfig.backend");
            gate.problems
        };

        let conforming = json!({
            "type": "object",
            "oneOf": [{"type": "object", "additionalProperties": false, "properties": {
                "http": {"type": "object", "additionalProperties": false, "properties": {
                    "url": {"type": "string"}, "method": {"type": "string", "default": "GET"}
                }, "required": ["url"]}
            }, "required": ["http"]}]
        });
        assert!(run(&conforming).is_empty());

        let mut ghost = conforming.clone();
        ghost["oneOf"][0]["properties"]["https"] = ghost["oneOf"][0]["properties"]["http"].clone();
        assert!(reported(
            &run(&ghost),
            "R2 the schema documents key `https`"
        ));

        let mut required = conforming.clone();
        required["oneOf"][0]["properties"]["http"]["required"] = json!(["url", "method"]);
        assert!(reported(
            &run(&required),
            "R3 the schema marks `method` required"
        ));

        // A function-backed default cannot be compared: `GET` is not
        // second-guessed.
        let mut other = conforming.clone();
        other["oneOf"][0]["properties"]["http"]["properties"]["method"]["default"] = json!("POST");
        assert!(run(&other).is_empty());

        // A tagged enum documented as one flat object selects the variant
        // through the tag's `enum`.
        let db = index.get("lookup.rs", "Db").unwrap();
        let flat = json!({
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["mysql"]},
                "uri": {"type": "string"},
                "ssl": {"type": "object", "properties": {"mode": {"type": "string"}},
                        "required": ["mode"]}
            },
            "required": ["type", "uri"]
        });
        let mut gate = Gate::new(&index, "output/sql");
        gate.check_def(db, &flat, "SqlOutputConfig.output_type");
        assert!(gate.problems.is_empty(), "{:?}", gate.problems);
    }

    #[test]
    fn a_row_is_bound_to_the_builder_registered_under_its_name() {
        let source: Vec<char> = r#"
struct OldConfig { a: u8 }
struct NewConfig { b: u8 }
fn helper() {
    let config: OldConfig = parse_config(None, "elsewhere");
    let _ = config;
}
pub struct DemoBuilder;
impl InputBuilder for DemoBuilder {
    fn build(&self) -> Result<(), ()> {
        let config: NewConfig = parse_config(None, "demo")?;
        let _ = config;
        Ok(())
    }
}
pub fn init() {
    register_input_builder(
        "demo",
        Arc::new(DemoBuilder),
    )?;
    Ok(())
}
"#
        .chars()
        .collect();
        // Both spellings appear in the file, so only the builder's own
        // block can tell which struct it reads.
        assert!(reads_config(&source, "OldConfig"));
        let block: Vec<char> = builder_block(&source, "input", "demo")
            .expect("the registration and the impl block are found")
            .chars()
            .collect();
        assert!(!reads_config(&block, "OldConfig"));
        assert!(reads_config(&block, "NewConfig"));
        // A name that is not registered, or another kind, has no block.
        assert!(builder_block(&source, "input", "other").is_none());
        assert!(builder_block(&source, "output", "demo").is_none());
    }

    #[test]
    fn the_real_index_resolves_types_across_modules() {
        let index = Index::load();
        let resolved = |ty: &str, file: &str| match index.resolve(ty, file) {
            Resolution::Resolved(inner) => {
                (inner.def.file.clone(), inner.def.name.clone(), inner.array)
            }
            other => panic!("{ty} in {file} resolves, got {other:?}"),
        };
        assert_eq!(
            resolved("crate::pulsar::common::PulsarTlsConfig", "input/pulsar.rs"),
            (
                "pulsar/common.rs".to_string(),
                "PulsarTlsConfig".to_string(),
                false
            )
        );
        assert_eq!(
            resolved("Option<Vec<TemporaryConfig>>", "processor/sql.rs"),
            (
                "processor/sql.rs".to_string(),
                "TemporaryConfig".to_string(),
                true
            )
        );
        assert_eq!(resolved("Expr<String>", "output/pulsar.rs").1, "Expr");
        // `MysqlConfig` exists in both the SQL input and output: the file the
        // type is written in wins.
        assert_eq!(resolved("MysqlConfig", "output/sql.rs").0, "output/sql.rs");
        assert_eq!(resolved("MysqlConfig", "input/sql.rs").0, "input/sql.rs");
        assert!(matches!(
            index.resolve("String", "output/sql.rs"),
            Resolution::Foreign
        ));
        assert!(matches!(
            index.resolve("std::time::Duration", "output/sql.rs"),
            Resolution::Foreign
        ));
        assert!(matches!(
            index.resolve("HashMap<String, String>", "output/sql.rs"),
            Resolution::Foreign
        ));
    }

    // ---- third review round --------------------------------------------------

    const DEFAULTS: &str = r#"
struct Defaults {
    #[serde(default = "four")]
    count: usize,
    #[serde(default = "default_name")]
    name: String,
    #[serde(default = "default_on")]
    on: Option<bool>,
    #[serde(default = "cpus")]
    cpus: usize,
}
fn four() -> usize {
    4
}
fn default_name() -> String {
    "flow".to_string()
}
fn default_on() -> Option<bool> {
    Some(true)
}
fn cpus() -> usize {
    num_cpus::get()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct Derived {
    flag: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct Custom {
    flag: bool,
}
impl Default for Custom {
    fn default() -> Self {
        Self { flag: true }
    }
}

// A default of the field's own comes before the derived container default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct DerivedWithFunction {
    plain: bool,
    #[serde(default = "default_on")]
    flag: bool,
}

// A newtype variant whose payload decodes any object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum Loose {
    Raw(serde_json::Value),
    File { path: String },
}

struct Child {
    nonce: [u8; {16}],
}
struct Parent {
    enabled: bool,
    #[serde(flatten)]
    child: Child,
}
"#;

    #[test]
    fn default_values_are_compared_with_their_source() {
        let index = Index::from_sources(&[("input/demo.rs".to_string(), DEFAULTS.to_string())]);
        let conforming = json!({"type": "object", "properties": {
            "count": {"default": 4}, "name": {"default": "flow"},
            "on": {"default": true}, "cpus": {"default": 8}}});
        assert!(problems(&index, "Defaults", &conforming).is_empty());

        // A function-backed default is compared with what the function returns.
        for (key, value, needle) in [
            (
                "count",
                json!(5),
                "R5 the schema advertises `default: 5` for `count`, but `four` returns 4",
            ),
            (
                "name",
                json!("other"),
                "R5 the schema advertises `default: \"other\"` for `name`, but `default_name` \
                 returns \"flow\"",
            ),
            (
                "on",
                json!(false),
                "R5 the schema advertises `default: false` for `on`, but `default_on` returns \
                 true",
            ),
        ] {
            let mut schema = conforming.clone();
            schema["properties"][key]["default"] = value;
            let found = problems(&index, "Defaults", &schema);
            assert!(reported(&found, needle), "{key}: {found:?}");
        }
        // A body that is not a literal cannot be compared: `cpus` stays quiet.

        // A derived container default is the field type's zero …
        let advertised = json!({"type": "object", "properties": {"flag": {"default": true}}});
        let found = problems(&index, "Derived", &advertised);
        assert!(
            reported(
                &found,
                "R5 the schema advertises `default: true` for `flag`, but `#[serde(default)]` \
                 yields false"
            ),
            "{found:?}"
        );
        // … a hand-written one is whatever its `Default` returns: not compared.
        assert!(problems(&index, "Custom", &advertised).is_empty());

        // A default of the field's own wins over the derived container's.
        let both = json!({"type": "object", "properties": {
            "plain": {"default": false}, "flag": {"default": true}}});
        assert!(problems(&index, "DerivedWithFunction", &both).is_empty());
        let mut wrong = both.clone();
        wrong["properties"]["flag"]["default"] = json!(false);
        let found = problems(&index, "DerivedWithFunction", &wrong);
        assert!(
            reported(
                &found,
                "R5 the schema advertises `default: false` for `flag`, but `default_on` returns \
                 true"
            ),
            "{found:?}"
        );

        // A comparison is counted where it happens — in this fixture, not
        // by a number the live registry has to keep.
        let usage = std::cell::RefCell::new(ExemptionUsage::default());
        let seen = |config: &str, schema: &Value| {
            let def = index
                .get("input/demo.rs", config)
                .unwrap_or_else(|| panic!("`{config}` is parsed"));
            let mut gate = Gate {
                index: &index,
                component: "input/demo",
                hidden: HIDDEN_FIELDS,
                builder_required: BUILDER_REQUIRED,
                usage: Some(&usage),
                problems: Vec::new(),
            };
            gate.check_def(def, schema, config);
            gate.problems
        };
        assert!(seen("Defaults", &conforming).is_empty());
        assert_eq!(
            (
                usage.borrow().defaults_compared,
                usage.borrow().function_defaults_compared
            ),
            (3, 3),
            "`cpus` is not a literal: checked to be omittable, not compared"
        );
        assert!(!seen("Derived", &advertised).is_empty());
        assert_eq!(
            usage.borrow().defaults_compared,
            4,
            "a mismatch is a comparison too"
        );

        // The expressions a default function is written with.
        for (expr, value) in [
            ("3", json!(3)),
            ("60_000", json!(60000)),
            ("-1", json!(-1)),
            ("0.5", json!(0.5)),
            ("\"x\"", json!("x")),
            ("\"x\".to_string()", json!("x")),
            ("\"x\".to_owned()", json!("x")),
            ("\"x\".into()", json!("x")),
            ("String::from(\"x\")", json!("x")),
            ("\"a\\\"b\"", json!("a\"b")),
            ("Some(true)", json!(true)),
            ("None", Value::Null),
            (" 4 ", json!(4)),
            // Escape sequences are decoded to the text they denote.
            ("\"\\n\"", json!("\n")),
            ("\"a\\\\b\"", json!("a\\b")),
            ("\"\\t\\r\\0\"", json!("\t\r\0")),
            ("\"\\x41\\u{42}\".to_string()", json!("AB")),
            ("\"line \\\n       continued\"", json!("line continued")),
        ] {
            assert_eq!(literal_value(expr), Some(value), "{expr}");
        }
        for expr in [
            "num_cpus::get()",
            "vec![1]",
            "10u64",
            "{ 1; 2 }",
            "",
            "\"x\".trim()",
            // A literal whose text cannot be decoded stays unverified.
            "\"\\q\"",
            "\"\\u{110000}\"",
            "\"\\u{4_1}\"",
            "r#\"x\"#",
        ] {
            assert_eq!(literal_value(expr), None, "{expr}");
        }
    }

    #[test]
    fn default_functions_are_read_from_the_real_sources() {
        let index = Index::load();
        let value = |function: &str, file: &str| index.default_fn_value(function, file);
        assert_eq!(
            value("default_concurrency", "processor/milvus_search.rs"),
            Some(json!(4))
        );
        assert_eq!(
            value("default_method", "processor/lookup.rs"),
            Some(json!("GET"))
        );
        assert_eq!(
            value("default_hostname_verification", "pulsar/common.rs"),
            Some(json!(true))
        );
        assert_eq!(
            value("default_timeout_ms", "processor/llm.rs"),
            Some(json!(30000))
        );
        // `num_cpus::get()` is not a literal.
        assert_eq!(value("default_thread_num", "buffer/join.rs"), None);
    }

    #[test]
    fn a_key_demanded_by_one_alternative_only_is_not_required() {
        let index = demo_index();
        let required = conforming()["required"].clone();

        // `anyOf`: the empty branch accepts a config without `brokers`.
        let mut schema = conforming();
        schema["required"] = json!([]);
        schema["anyOf"] = json!([{"required": ["brokers"]}, {}]);
        let found = problems(&index, "DemoConfig", &schema);
        assert!(
            reported(
                &found,
                "R4 serde requires `brokers` but the schema demands it only in some of its \
                 `anyOf`/`oneOf` branches"
            ),
            "{found:?}"
        );
        let mut schema = conforming();
        schema["required"] = json!([]);
        schema["anyOf"] = json!([{}, {"required": ["brokers"]}]);
        assert!(
            reported(
                &problems(&index, "DemoConfig", &schema),
                "R4 serde requires `brokers`"
            ),
            "branch order does not matter"
        );

        // `allOf`: every branch applies, so the demand stands.
        let mut schema = conforming();
        schema.as_object_mut().unwrap().remove("required");
        schema["allOf"] = json!([{"required": required}, {}]);
        assert!(problems(&index, "DemoConfig", &schema).is_empty());

        // Demanded everywhere, nested included.
        let mut schema = conforming();
        schema.as_object_mut().unwrap().remove("required");
        schema["anyOf"] = json!([
            {"required": required},
            {"allOf": [{"required": required}]}
        ]);
        assert!(problems(&index, "DemoConfig", &schema).is_empty());
    }

    #[test]
    fn every_untagged_branch_must_be_a_variant_serde_can_decode() {
        let index = demo_index();
        let check = |schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(
                index.get("input/demo.rs", "Untagged").unwrap(),
                &schema,
                "Untagged",
            );
            gate.problems
        };
        let file = json!({"properties": {"path": {"type": "string"}, "format": {"type": "string"}}, "required": ["path", "format"]});
        let socket = json!({"properties": {"port": {"type": "integer"}}, "required": ["port"]});

        // The complete branches alone pass.
        assert!(check(json!({"anyOf": [file, socket]})).is_empty());

        // A branch that overlaps a variant but demands less admits objects
        // serde rejects.
        let found = check(json!({"anyOf": [
            file, {"properties": {"path": {"type": "string"}}, "required": ["path"]}, socket
        ]}));
        assert!(
            reported(
                &found,
                "R4 schema branch 2 demands only `path`: no variant of `Untagged` requires just \
                 those keys, so the schema accepts objects serde rejects"
            ),
            "{found:?}"
        );
        // So does an empty alternative.
        let found = check(json!({"anyOf": [file, socket, {}]}));
        assert!(
            reported(&found, "R4 schema branch 3 demands nothing"),
            "{found:?}"
        );
    }

    #[test]
    fn a_discriminator_must_name_a_decodable_variant() {
        let index = demo_index();
        let check = |schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(
                index.get("input/demo.rs", "WithUnit").unwrap(),
                &schema,
                "WithUnit",
            );
            gate.problems
        };
        let off = json!({"properties": {"type": {"const": "off"}}, "required": ["type"]});
        let file = json!({"properties": {"type": {"const": "file"}, "path": {"type": "string"}},
                          "required": ["type", "path"]});
        // A unit variant's value is a valid discriminator.
        assert!(check(json!({"oneOf": [off.clone(), file.clone()]})).is_empty());

        // The tag is what serde selects by, so every branch that offers a
        // variant has to demand it: `{}` matches a branch that does not, and
        // serde rejects `{}` before it selects anything. A unit variant is
        // no exception.
        let found =
            check(json!({"oneOf": [{"properties": {"type": {"const": "off"}}}, file.clone()]}));
        assert!(
            reported(
                &found,
                "R4 serde requires `type` to select `off`, but a schema branch that selects it \
                 does not demand it — add `type` to that branch's `required`"
            ),
            "{found:?}"
        );
        // The node's own `required` demands the tag of every branch.
        let found = check(json!({"required": ["type"],
                                 "oneOf": [{"properties": {"type": {"const": "off"}}}, file.clone()]}));
        assert!(found.is_empty(), "{found:?}");
        // A variant no branch offers cannot be written.
        let found = check(json!({"oneOf": [file.clone()]}));
        assert!(
            reported(
                &found,
                "R1 variant `off` has no schema branch that describes it"
            ),
            "{found:?}"
        );

        let found = check(
            json!({"oneOf": [off, file, {"properties": {"type": {"const": "bogus"}}, "required": ["type"]}]}),
        );
        assert!(
            reported(
                &found,
                "R6 the schema accepts `type: \"bogus\"`, which is not a variant serde decodes"
            ),
            "{found:?}"
        );

        // The same through an `enum` on a merged view.
        let found = check(
            json!({"properties": {"type": {"enum": ["off", "file", "ghost"]},
                                             "path": {"type": "string"}},
                                 "required": ["type"]}),
        );
        assert!(
            reported(&found, "R6 the schema accepts `type: \"ghost\"`"),
            "{found:?}"
        );
    }

    #[test]
    fn an_unreadable_flattened_type_is_reported() {
        let index = Index::from_sources(&[("input/demo.rs".to_string(), DEFAULTS.to_string())]);
        // A closed schema that documents only `enabled` must not pass while
        // `Child`'s fields are invisible to the gate.
        let closed = json!({"type": "object", "additionalProperties": false,
                            "properties": {"enabled": {"type": "boolean"}},
                            "required": ["enabled"]});
        let found = problems(&index, "Parent", &closed);
        assert!(
            reported(
                &found,
                "R0 `Parent.child (flattened)` cannot be checked: the definition of `Child` does \
                 not parse"
            ),
            "{found:?}"
        );
    }

    #[test]
    fn relative_paths_resolve_their_module_suffix() {
        let index = Index::from_sources(&[
            (
                "lib.rs".to_string(),
                "pub struct Root { n: u8 }".to_string(),
            ),
            (
                "input/mod.rs".to_string(),
                "pub struct Shared { n: u8 }".to_string(),
            ),
            (
                "input/common.rs".to_string(),
                "pub struct Port { n: u8 }".to_string(),
            ),
            (
                "input/demo.rs".to_string(),
                "struct Holder { a: super::common::Port }".to_string(),
            ),
        ]);
        let file = |ty: &str, from: &str| match index.resolve(ty, from) {
            Resolution::Resolved(inner) => inner.def.file.clone(),
            other => panic!("{ty} in {from} resolves, got {other:?}"),
        };
        // `super` selects the enclosing module; the rest of the path descends
        // from there.
        assert_eq!(
            file("super::common::Port", "input/demo.rs"),
            "input/common.rs"
        );
        assert_eq!(file("super::Shared", "input/demo.rs"), "input/mod.rs");
        assert_eq!(file("super::super::Root", "input/demo.rs"), "lib.rs");
        assert_eq!(
            file("crate::input::common::Port", "lib.rs"),
            "input/common.rs"
        );
        // `self` is the module the file itself declares: `input` for
        // `input/mod.rs`.
        assert_eq!(file("self::Shared", "input/mod.rs"), "input/mod.rs");
        assert_eq!(
            file("self::common::Port", "input/mod.rs"),
            "input/common.rs"
        );
        // Nothing above the crate root, and no file for a path that leaves
        // the tree.
        assert!(index
            .path_file("super::super::super::Root", "input/demo.rs")
            .is_none());
        assert!(index
            .path_file("self::missing::Port", "input/demo.rs")
            .is_none());
    }

    #[test]
    fn only_real_deserialization_sites_count() {
        let body = |text: &str| text.chars().collect::<Vec<char>>();
        assert!(reads_config(
            &body("let config: OldConfig = parse_config(None, \"demo\")?;"),
            "OldConfig"
        ));
        assert!(reads_config(
            &body("let mut config: OldConfig = next();"),
            "OldConfig"
        ));
        assert!(reads_config(
            &body("let c = serde_json::from_value::<OldConfig>(value)?;"),
            "OldConfig"
        ));
        // A diagnostic that mentions the type is not a deserialization.
        assert!(!reads_config(
            &body("return Err(format!(\"expected: OldConfig = {:?}\", x));"),
            "OldConfig"
        ));
        // Neither is an annotated expression that is not a `let` binding.
        assert!(!reads_config(
            &body("typed: OldConfig = other;"),
            "OldConfig"
        ));
        // The type has to be named exactly.
        assert!(!reads_config(
            &body("let c = serde_json::from_value::<OldConfigExtra>(value)?;"),
            "OldConfig"
        ));
        assert!(!reads_config(
            &body("let c: OldConfigExtra = x;"),
            "OldConfig"
        ));
        // The tokens are matched whatever whitespace separates them, and
        // through the module path of the type.
        for text in [
            "let config:OldConfig=parse_config(None)?;",
            "let  mut  config\n    :\n    OldConfig\n    = next();",
            "let config: crate::input::demo::OldConfig = x;",
            "let c = serde_json::from_value::< OldConfig >(value)?;",
        ] {
            assert!(reads_config(&body(text), "OldConfig"), "{text}");
        }
        for text in [
            "if let c: OldConfig = x {}",
            "let c = OldConfig::default();",
            "let c = HashMap<String, OldConfig>::new();",
            "let c = parse::<OldConfig, Error>(x);",
        ] {
            assert!(!reads_config(&body(text), "OldConfig"), "{text}");
        }
    }

    #[test]
    fn a_builder_enforced_exemption_is_probed_against_the_real_builder() {
        let registered = registered_components();
        let milvus = &registered["output/milvus"];
        // The exemption holds: the builder refuses a config without the key.
        let refusal = builder_enforces("output/milvus", milvus, "id_field")
            .expect("the milvus builder refuses a config without id_field");
        assert!(refusal.contains("'id_field' is required"), "{refusal}");
        // A key the builder does not enforce is reported as such.
        let why = builder_enforces("output/milvus", milvus, "vector_field").unwrap_err();
        assert!(
            why.contains("the builder accepts a config without `vector_field`"),
            "{why}"
        );
        // A key the example does not set cannot be probed.
        let why = builder_enforces("output/milvus", milvus, "headers").unwrap_err();
        assert!(why.contains("does not set `headers`"), "{why}");

        // An example the builder already rejects proves nothing: without a
        // `collection` it refuses before it looks at `id_field`.
        let mut rejected = (**milvus).clone();
        rejected.config_example.as_mut().unwrap()["collection"] = json!("");
        let why = builder_enforces("output/milvus", &rejected, "id_field").unwrap_err();
        assert!(
            why.contains("rejects the intact `config_example` as well")
                && why.contains("'collection' must not be empty"),
            "{why}"
        );
        // Neither does a component that is not built at all.
        let why = builder_enforces("temporary/milvus", milvus, "id_field").unwrap_err();
        assert!(why.contains("rejects the intact `config_example`"), "{why}");

        // A refusal that does not name the key is not about the key.
        let why = judged_probe(
            "id_field",
            Ok(()),
            Err("milvus output: 'collection' must not be empty".to_string()),
        )
        .unwrap_err();
        assert!(why.contains("for a reason that does not name it"), "{why}");
    }

    // ---- fourth review round --------------------------------------------------

    #[test]
    fn a_default_function_is_found_through_its_path_and_its_own_file() {
        let index = Index::from_sources(&[
            (
                "input/demo.rs".to_string(),
                r#"
struct Qualified {
    #[serde(default = "crate::input::shared::count")]
    a: u8,
    #[serde(default = "super::shared::count")]
    b: u8,
    #[serde(default = "crate::missing::count")]
    c: u8,
    #[serde(default = "count")]
    d: u8,
    #[serde(default = "Config::count")]
    e: u8,
}
fn count() -> u8 {
    3
}
struct Parent {
    #[serde(flatten)]
    child: super::shared::Child,
}
"#
                .to_string(),
            ),
            (
                "input/shared.rs".to_string(),
                r#"
pub fn count() -> u8 {
    4
}
pub struct Child {
    #[serde(default = "count")]
    n: u8,
}
"#
                .to_string(),
            ),
        ]);
        // A path rooted in a module is the function that module declares.
        let value = |function: &str| index.default_fn_value(function, "input/demo.rs");
        assert_eq!(value("crate::input::shared::count"), Some(json!(4)));
        assert_eq!(value("super::shared::count"), Some(json!(4)));
        // A bare name is the file's own function.
        assert_eq!(value("count"), Some(json!(3)));
        assert_eq!(
            index.default_fn_value("count", "input/shared.rs"),
            Some(json!(4))
        );
        // A path that leads nowhere names nothing: the same-file `count`
        // does not stand in for it.
        assert_eq!(value("crate::missing::count"), None);
        // A type's associated function is followed by its name.
        assert_eq!(value("Config::count"), Some(json!(3)));

        // A flattened field keeps the file its default function lives in:
        // `Parent`'s file has a `count` of its own, and it is not the one.
        let advertised = json!({"type": "object", "properties": {"n": {"default": 4}}});
        assert!(problems(&index, "Parent", &advertised).is_empty());
        let mut wrong = advertised;
        wrong["properties"]["n"]["default"] = json!(3);
        let found = problems(&index, "Parent", &wrong);
        assert!(
            reported(
                &found,
                "R5 the schema advertises `default: 3` for `n`, but `count` returns 4"
            ),
            "{found:?}"
        );
    }

    #[test]
    fn every_branch_that_selects_a_variant_is_checked() {
        let index = demo_index();
        let check = |schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(index.get("input/demo.rs", "Mode").unwrap(), &schema, "Mode");
            gate.problems
        };
        let cluster = json!({"properties": {"type": {"const": "cluster"},
                                            "urls": {"type": "array"}},
                             "required": ["type", "urls"]});
        let single = json!({"properties": {"type": {"const": "single"},
                                           "url": {"type": "string"},
                                           "insecure": {"type": "boolean", "default": false}},
                            "required": ["type", "url"]});
        assert!(check(json!({"anyOf": [cluster.clone(), single.clone()]})).is_empty());

        // A second branch that selects `single` but demands only the tag
        // lets a configuration without `url` through the schema.
        let lenient = json!({"properties": {"type": {"const": "single"}}, "required": ["type"]});
        let found = check(json!({"anyOf": [cluster.clone(), single.clone(), lenient.clone()]}));
        assert!(
            reported(&found, "[Mode] Mode[single]: R4 serde requires `url`"),
            "{found:?}"
        );
        // So does one that advertises another default for a field of the
        // variant.
        let mut other_default = single.clone();
        other_default["properties"]["insecure"]["default"] = json!(true);
        let found = check(json!({"anyOf": [cluster, single, other_default, lenient]}));
        assert!(
            reported(
                &found,
                "R5 the schema advertises `default: true` for `insecure`, but `#[serde(default)]` \
                 yields false"
            ),
            "{found:?}"
        );
    }

    #[test]
    fn a_requirement_of_the_enclosing_schema_reaches_every_branch() {
        let index =
            Index::from_sources(&[("input/demo.rs".to_string(), format!("{DEMO}{ENDPOINT}"))]);
        let check = |config: &str, schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(index.get("input/demo.rs", config).unwrap(), &schema, config);
            gate.problems
        };
        let port = json!({"properties": {"port": {"type": "integer"}}, "required": ["port"]});
        let path = json!({"properties": {"path": {"type": "string"}}, "required": ["path"]});
        // `host` is demanded by the node itself, so of both variants.
        let enclosing = json!({"properties": {"host": {"type": "string"}}, "required": ["host"],
                               "anyOf": [port.clone(), path.clone()]});
        let found = check("Endpoint", enclosing.clone());
        assert!(found.is_empty(), "{found:?}");

        // Once the node stops demanding it, a branch that leaves it out
        // lets a configuration without `host` through.
        let mut loose = enclosing;
        loose["required"] = json!([]);
        let found = check("Endpoint", loose);
        assert!(
            reported(&found, "[Endpoint] Endpoint[Tcp]: R4 serde requires `host`"),
            "{found:?}"
        );
        assert!(
            reported(&found, "R4 schema branch 1 demands only `port`"),
            "{found:?}"
        );

        // A key only one variant reads cannot be demanded of every branch:
        // the other variant's configurations would be rejected.
        let file = json!({"properties": {"path": {"type": "string"}}, "required": ["path"]});
        let found = check(
            "Untagged",
            json!({"required": ["format"], "anyOf": [file, port.clone()]}),
        );
        assert!(
            !reported(&found, "Untagged[File]: R4 serde requires `format`"),
            "the branch inherits the enclosing demand: {found:?}"
        );
        assert!(
            reported(
                &found,
                "[Untagged] Untagged[Socket]: R1 the enclosing schema requires `format`, which \
                 this variant never reads"
            ),
            "{found:?}"
        );
    }

    #[test]
    fn a_merged_view_still_checks_each_variant_against_the_union() {
        let index = demo_index();
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Mode").unwrap(),
            &json!({"type": "object", "properties": {
                "type": {"enum": ["cluster", "single"]},
                "urls": {"type": "array"}, "url": {"type": "string"},
                "insecure": {"type": "boolean", "default": false}},
                "required": ["type"]}),
            "Mode",
        );
        // A key of another variant is not a ghost here — the merged view
        // documents every variant — while a key the shared `required` leaves
        // out is still reported for the variant that needs it.
        assert!(
            !reported(&gate.problems, "R2"),
            "no ghost on a merged view: {:?}",
            gate.problems
        );
        assert!(
            reported(
                &gate.problems,
                "[Mode] Mode[cluster]: R4 serde requires `urls` but the schema does not list it"
            ),
            "{:?}",
            gate.problems
        );
        assert!(
            reported(
                &gate.problems,
                "[Mode] Mode[single]: R4 serde requires `url` but the schema does not list it"
            ),
            "{:?}",
            gate.problems
        );
        // With the keys demanded, nothing is left to report.
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Mode").unwrap(),
            &json!({"type": "object", "properties": {
                "type": {"enum": ["cluster", "single"]},
                "urls": {"type": "array"}, "url": {"type": "string"},
                "insecure": {"type": "boolean", "default": false}},
                "required": ["type", "urls", "url"]}),
            "Mode",
        );
        assert!(gate.problems.is_empty(), "{:?}", gate.problems);
    }

    #[test]
    fn a_variant_that_decodes_any_object_excuses_every_branch() {
        let index =
            Index::from_sources(&[("input/demo.rs".to_string(), format!("{DEMO}{}", DEFAULTS))]);
        let check = |schema: Value| {
            let mut gate = Gate::new(&index, "input/demo");
            gate.check_def(
                index.get("input/demo.rs", "Loose").unwrap(),
                &schema,
                "Loose",
            );
            gate.problems
        };
        let file = json!({"properties": {"path": {"type": "string"}}, "required": ["path"]});
        // `Raw` decodes any object, so the schema may accept anything too.
        assert!(check(json!({"anyOf": [file.clone(), {}]})).is_empty());
        // Without such a variant an empty branch admits what serde rejects.
        let mut gate = Gate::new(&index, "input/demo");
        gate.check_def(
            index.get("input/demo.rs", "Untagged").unwrap(),
            &json!({"anyOf": [file, {}]}),
            "Untagged",
        );
        assert!(
            reported(&gate.problems, "R4 schema branch 2 demands nothing"),
            "{:?}",
            gate.problems
        );
    }

    #[test]
    fn rename_rules_match_serde_for_non_ascii_names() {
        let name_of = |value: serde_json::Value| value.as_str().unwrap().to_string();
        macro_rules! rule_struct {
            ($name:ident, $rule:expr) => {
                #[derive(serde::Serialize)]
                #[serde(rename_all = $rule)]
                #[allow(non_snake_case, dead_code)]
                struct $name {
                    port_élevé: u8,
                    aÉb_c: u8,
                    déjà: u8,
                    x_Élé: u8,
                }
            };
        }
        macro_rules! rule_enum {
            ($name:ident, $rule:expr) => {
                #[derive(serde::Serialize)]
                #[serde(rename_all = $rule)]
                #[allow(non_camel_case_types, dead_code)]
                enum $name {
                    Échec,
                    HTTPServer,
                    ÉtéLu,
                    déjàVu,
                    aÉb,
                    Ab,
                }
            };
        }
        rule_struct!(FieldLower, "lowercase");
        rule_struct!(FieldUpper, "UPPERCASE");
        rule_struct!(FieldPascal, "PascalCase");
        rule_struct!(FieldCamel, "camelCase");
        rule_struct!(FieldSnake, "snake_case");
        rule_struct!(FieldScreamingSnake, "SCREAMING_SNAKE_CASE");
        rule_struct!(FieldKebab, "kebab-case");
        rule_struct!(FieldScreamingKebab, "SCREAMING-KEBAB-CASE");
        rule_enum!(VariantLower, "lowercase");
        rule_enum!(VariantUpper, "UPPERCASE");
        rule_enum!(VariantPascal, "PascalCase");
        rule_enum!(VariantSnake, "snake_case");
        rule_enum!(VariantScreamingSnake, "SCREAMING_SNAKE_CASE");
        rule_enum!(VariantKebab, "kebab-case");
        rule_enum!(VariantScreamingKebab, "SCREAMING-KEBAB-CASE");
        // serde's derive panics when `camelCase` meets a name that starts
        // with a non-ASCII letter, so that rule is compared on the rest.
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        #[allow(non_camel_case_types, dead_code)]
        enum VariantCamel {
            HTTPServer,
            déjàVu,
            aÉb,
            Ab,
        }

        let fields = ["port_élevé", "aÉb_c", "déjà", "x_Élé"];
        let key_of = |value: &serde_json::Value, n: u64| {
            value
                .as_object()
                .unwrap()
                .iter()
                .find(|(_, field)| field.as_u64() == Some(n))
                .expect("every fixture field is serialized")
                .0
                .clone()
        };
        for (rule, value) in [
            (
                "lowercase",
                json!(FieldLower {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "UPPERCASE",
                json!(FieldUpper {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "PascalCase",
                json!(FieldPascal {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "camelCase",
                json!(FieldCamel {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "snake_case",
                json!(FieldSnake {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "SCREAMING_SNAKE_CASE",
                json!(FieldScreamingSnake {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "kebab-case",
                json!(FieldKebab {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
            (
                "SCREAMING-KEBAB-CASE",
                json!(FieldScreamingKebab {
                    port_élevé: 1,
                    aÉb_c: 2,
                    déjà: 3,
                    x_Élé: 4
                }),
            ),
        ] {
            let rule = rename_rule(rule).unwrap();
            for (index, rust) in fields.iter().enumerate() {
                assert_eq!(
                    rule.apply_to_field(rust),
                    key_of(&value, index as u64 + 1),
                    "{rule:?} applied to the field {rust}"
                );
            }
        }

        // ASCII folding, Unicode word boundaries.
        for (rule, cases) in [
            (
                "lowercase",
                vec![
                    ("Échec", name_of(json!(VariantLower::Échec))),
                    ("ÉtéLu", name_of(json!(VariantLower::ÉtéLu))),
                    ("aÉb", name_of(json!(VariantLower::aÉb))),
                ],
            ),
            (
                "UPPERCASE",
                vec![
                    ("ÉtéLu", name_of(json!(VariantUpper::ÉtéLu))),
                    ("déjàVu", name_of(json!(VariantUpper::déjàVu))),
                ],
            ),
            (
                "PascalCase",
                vec![("Échec", name_of(json!(VariantPascal::Échec)))],
            ),
            (
                "snake_case",
                vec![
                    ("Échec", name_of(json!(VariantSnake::Échec))),
                    ("ÉtéLu", name_of(json!(VariantSnake::ÉtéLu))),
                    ("déjàVu", name_of(json!(VariantSnake::déjàVu))),
                    ("aÉb", name_of(json!(VariantSnake::aÉb))),
                ],
            ),
            (
                "SCREAMING_SNAKE_CASE",
                vec![
                    ("ÉtéLu", name_of(json!(VariantScreamingSnake::ÉtéLu))),
                    ("aÉb", name_of(json!(VariantScreamingSnake::aÉb))),
                ],
            ),
            (
                "kebab-case",
                vec![
                    ("ÉtéLu", name_of(json!(VariantKebab::ÉtéLu))),
                    ("déjàVu", name_of(json!(VariantKebab::déjàVu))),
                ],
            ),
            (
                "SCREAMING-KEBAB-CASE",
                vec![
                    ("Échec", name_of(json!(VariantScreamingKebab::Échec))),
                    ("ÉtéLu", name_of(json!(VariantScreamingKebab::ÉtéLu))),
                ],
            ),
        ] {
            let rule = rename_rule(rule).unwrap();
            for (rust, serde_name) in cases {
                assert_eq!(
                    rule.apply_to_variant(rust),
                    serde_name,
                    "{rule:?} applied to the variant {rust}"
                );
            }
        }
        let camel = rename_rule("camelCase").unwrap();
        assert_eq!(
            camel.apply_to_variant("HTTPServer"),
            name_of(json!(VariantCamel::HTTPServer))
        );
        assert_eq!(
            camel.apply_to_variant("déjàVu"),
            name_of(json!(VariantCamel::déjàVu))
        );
        assert_eq!(
            camel.apply_to_variant("Ab"),
            name_of(json!(VariantCamel::Ab))
        );
    }
}
