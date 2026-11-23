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
//! * R3 — every schema-`required` key is required by serde (or registered as
//!   builder-enforced in [`BUILDER_REQUIRED`]).
//! * R4 — every serde-required field is schema-`required`.
//! * R5 — a documented `default` belongs to a field serde lets the user omit.
//! * R6 — a string-enum schema lists exactly the variants of a unit-variant
//!   enum.
//!
//! `#[serde(flatten)]` fields are inlined, nested object/array properties are
//! checked against their own types, and tagged-enum branches are matched to
//! their variants. A schema node without `properties`/`required` is opaque
//! (it accepts any key), so nothing is checked there.
//!
//! Deliberate exceptions go through [`HIDDEN_FIELDS`] and [`BUILDER_REQUIRED`],
//! and an exemption that is no longer needed fails the gate so the lists
//! cannot rot.

use arkflow_core::component::{list_components, ComponentMetadata};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Exemptions = &'static [(&'static str, &'static str, &'static str, &'static str)];

/// `(kind, registered name, source file under src/, config struct the builder
/// deserializes)` for every component with a configuration surface.
///
/// Adding a component with a schema? Add a row — the completeness check below
/// fails until you do.
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
/// being needed the moment the field shows up in the schema, and then fails
/// the gate — remove it.
const HIDDEN_FIELDS: Exemptions = &[];

/// A field the schema lists as `required` although serde accepts its
/// omission, because the component builder enforces it with a
/// domain-specific error: `(kind/name, config struct, field, reason)`.
///
/// Exempt from R3 only. The entry must stay true (field in the schema's
/// `required`, optional for serde) or it fails the gate as stale.
const BUILDER_REQUIRED: Exemptions = &[(
    "output/milvus",
    "MilvusOutputConfig",
    "id_field",
    "the builder rejects a missing id_field: the REST upsert API needs a primary key per row",
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
/// `keyword`, skipping a directly preceding visibility.
fn item_attrs(s: &[char], keyword: usize) -> Vec<String> {
    let mut head = keyword;
    let mut j = head;
    while j > 0 && s[j - 1].is_whitespace() {
        j -= 1;
    }
    let is_pub =
        j >= 3 && s[j - 3..j] == ['p', 'u', 'b'] && (j == 3 || !is_ident_continue(s[j - 4]));
    if is_pub {
        head = j - 3;
        if head > 0 && s[head - 1] == ')' {
            let mut depth = 0i32;
            let mut k = head;
            while k > 0 {
                k -= 1;
                if s[k] == ')' {
                    depth += 1;
                } else if s[k] == '(' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
            head = k;
        }
    }
    attrs_ending_at(s, head)
}

// ---------------------------------------------------------------------------
// serde attributes and config-shape parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct SerdeAttrs {
    rename: Option<String>,
    default: bool,
    skip: bool,
    flatten: bool,
    rename_all: Option<String>,
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
                "default" => out.default = true,
                "skip" | "skip_deserializing" => out.skip = true,
                "flatten" => out.flatten = true,
                "rename_all" => out.rename_all = value.map(|v| unquote(&v)),
                "tag" => out.tag = value.map(|v| unquote(&v)),
                "untagged" => out.untagged = true,
                "content" => out.content = true,
                _ => {}
            }
        }
    }
    out
}

/// serde's `rename_all = "snake_case"` for a Rust identifier: a `_` goes
/// before an upper-case letter that follows a lower-case letter or digit, or
/// that ends an upper-case run, and everything is lower-cased.
fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev_lower = chars[i - 1].is_lowercase() || chars[i - 1].is_numeric();
            let run_ends =
                chars[i - 1].is_uppercase() && chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if prev_lower || run_ends {
                out.push('_');
            }
        }
        out.extend(c.to_lowercase());
    }
    out
}

fn renamed(ident: &str, attrs: &SerdeAttrs, rename_all: Option<&str>) -> String {
    if let Some(rename) = &attrs.rename {
        return rename.clone();
    }
    match rename_all {
        Some("snake_case") => snake_case(ident),
        Some("lowercase") => ident.to_lowercase(),
        Some("UPPERCASE") => ident.to_uppercase(),
        // Conventions the config surface does not use are left verbatim:
        // if one is ever adopted the mismatch shows up as drift.
        _ => ident.to_string(),
    }
}

fn type_is_option(ty: &str) -> bool {
    const PREFIXES: [&str; 3] = ["Option<", "std::option::Option<", "core::option::Option<"];
    ty.ends_with('>') && PREFIXES.iter().any(|p| ty.starts_with(p))
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
    /// `Option<_>`, `#[serde(default ...)]`, or the container has
    /// `#[serde(default)]`.
    optional: bool,
    /// `#[serde(skip)]` / `#[serde(skip_deserializing)]`: not an input.
    skip: bool,
    /// `#[serde(flatten)]`: the target type's fields are inlined.
    flatten: bool,
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
    /// `#[serde(tag = "...")]`: the tag key of an internally tagged enum.
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

fn parse_fields(
    body: &[char],
    container_default: bool,
    rename_all: Option<&str>,
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
        if ty.is_empty() || ty.contains(['{', '}', ';']) {
            return Err(ParseError(format!(
                "field `{name}` has an unreadable type `{ty}`"
            )));
        }
        fields.push(Field {
            name: renamed(&name, &serde, rename_all),
            optional: serde.default || container_default || type_is_option(&ty),
            skip: serde.skip,
            flatten: serde.flatten,
            ty,
        });
    }
    Ok(fields)
}

fn parse_def_body(
    kind: DefKind,
    body: &[char],
    serde: &SerdeAttrs,
    file: &str,
    name: &str,
) -> Result<Def, ParseError> {
    let rename_all = serde.rename_all.as_deref();
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
    match kind {
        DefKind::Struct => def.fields = parse_fields(body, serde.default, rename_all)?,
        DefKind::Enum => {
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
                    name: renamed(&vname, &vserde, rename_all),
                    kind: VariantKind::Other,
                    skip: vserde.skip,
                    fields: Vec::new(),
                    inner: None,
                };
                if after.starts_with('{') && after.ends_with('}') {
                    variant.kind = VariantKind::Struct;
                    let inner: Vec<char> = after[1..after.len() - 1].chars().collect();
                    variant.fields = parse_fields(&inner, false, rename_all)?;
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

fn parse_def_at(
    s: &[char],
    keyword: usize,
    keyword_len: usize,
    kind: DefKind,
    file: &str,
) -> Option<Def> {
    // A keyword is followed by whitespace and the type name.
    if !s.get(keyword + keyword_len)?.is_whitespace() {
        return None;
    }
    let name_start = skip_ws(s, keyword + keyword_len);
    if !is_ident_start(*s.get(name_start)?) {
        return None;
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
        // Tuple/unit struct or forward declaration: no named fields to read.
        return None;
    }
    let serde = parse_serde(&item_attrs(s, keyword));
    let end = matching(s, '{', '}', i).ok()?;
    parse_def_body(kind, &s[i + 1..end - 1], &serde, file, &name).ok()
}

/// Definitions in a comment-stripped source, in order of appearance.
fn parse_defs(s: &[char], file: &str) -> Vec<Def> {
    const KEYWORDS: [(&str, DefKind); 2] = [("struct", DefKind::Struct), ("enum", DefKind::Enum)];
    let mut defs = Vec::new();
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
                if let Some(def) = parse_def_at(s, i, keyword.len(), *kind, file) {
                    defs.push(def);
                }
            }
            i = end;
            continue;
        }
        i += 1;
    }
    defs
}

// ---------------------------------------------------------------------------
// Type index
// ---------------------------------------------------------------------------

struct Resolved<'a> {
    def: &'a Def,
    /// The type was reached through a `Vec<…>` wrapper.
    array: bool,
}

/// Parsed definitions of every source file under `src/`.
struct Index {
    /// `file → type name → definition`; the first definition wins.
    defs: BTreeMap<String, BTreeMap<String, Def>>,
    /// `type name → files defining it`.
    files_by_name: BTreeMap<String, Vec<String>>,
    /// Comment-stripped text of each file, for the deserialization guard.
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

/// `dirs/mod.rs` for a module path (`lib.rs` for the crate root).
fn module_path(dirs: &[&str]) -> String {
    if dirs.is_empty() {
        "lib.rs".to_string()
    } else {
        format!("{}/mod.rs", dirs.join("/"))
    }
}

/// The files of the modules enclosing `file`, nearest first.
fn parent_modules(file: &str) -> Vec<String> {
    if file == "lib.rs" {
        return Vec::new();
    }
    let parts: Vec<&str> = file.split('/').collect();
    let dirs = &parts[..parts.len() - 1];
    if parts.last() == Some(&"mod.rs") {
        // `a/b/mod.rs` is module `a::b` itself: its parent is `a`.
        (0..dirs.len())
            .rev()
            .map(|depth| module_path(&dirs[..depth]))
            .collect()
    } else {
        // `a/b/c.rs` declares `c` inside module `a::b`.
        (0..=dirs.len())
            .rev()
            .map(|depth| module_path(&dirs[..depth]))
            .collect()
    }
}

impl Index {
    fn from_sources(sources: &[(String, String)]) -> Index {
        let mut index = Index {
            defs: BTreeMap::new(),
            files_by_name: BTreeMap::new(),
            cleaned: BTreeMap::new(),
        };
        for (file, source) in sources {
            let cleaned = strip_comments(source);
            for def in parse_defs(&cleaned, file) {
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

    /// The file declaring the module at `dirs`.
    fn module_file(&self, dirs: &[&str]) -> Option<String> {
        if dirs.is_empty() {
            return self
                .defs
                .contains_key("lib.rs")
                .then(|| "lib.rs".to_string());
        }
        let base = dirs.join("/");
        [format!("{base}.rs"), format!("{base}/mod.rs")]
            .into_iter()
            .find(|file| self.defs.contains_key(file))
    }

    /// Resolves a type path written in `from_file` to `(file, type name)`.
    fn locate(&self, ty: &str, from_file: &str) -> Option<(String, String)> {
        let segments: Vec<&str> = ty.split("::").collect();
        let name = (*segments.last()?).to_string();
        let mut candidates: Vec<String> = Vec::new();
        match segments[0] {
            "crate" => {
                if segments.len() == 2 {
                    candidates.push("lib.rs".to_string());
                } else {
                    candidates.extend(self.module_file(&segments[1..segments.len() - 1]));
                }
            }
            "self" => candidates.push(from_file.to_string()),
            "super" => {
                let supers = segments.iter().take_while(|s| **s == "super").count();
                if supers == 0 {
                    return None;
                }
                candidates.extend(parent_modules(from_file).into_iter().nth(supers - 1));
            }
            _ if segments.len() == 1 => {
                // A bare name may arrive through a `use` we cannot see, so the
                // enclosing modules and every definition of that name are
                // tried after the current file.
                candidates.push(from_file.to_string());
                candidates.extend(parent_modules(from_file));
                if let Some(files) = self.files_by_name.get(&name) {
                    candidates.extend(files.iter().cloned());
                }
            }
            // Another crate's path: nothing of ours to parse.
            _ => return None,
        }
        candidates
            .into_iter()
            .find_map(|file| self.get(&file, &name).map(|_| (file, name.clone())))
    }

    /// The config shape behind a field type: wrappers (`Option`, `Vec`, …)
    /// are peeled; scalars and foreign types resolve to `None`.
    fn resolve(&self, ty: &str, from_file: &str) -> Option<Resolved<'_>> {
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
        if t.is_empty() || t.contains(['<', '>', '(', ')', '&', ' ']) {
            return None;
        }
        let (file, name) = self.locate(t, from_file)?;
        let def = self.get(&file, &name)?;
        Some(Resolved { def, array })
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

/// Every name listed under `key` (`properties` keys or `required` entries)
/// anywhere inside a schema.
fn all_named(node: &Value, key: &str) -> BTreeSet<String> {
    fn walk(value: &Value, key: &str, out: &mut BTreeSet<String>) {
        match value {
            Value::Object(map) => {
                match map.get(key) {
                    Some(Value::Object(entries)) => out.extend(entries.keys().cloned()),
                    Some(Value::Array(names)) => {
                        out.extend(names.iter().filter_map(Value::as_str).map(str::to_string))
                    }
                    _ => {}
                }
                for child in map.values() {
                    walk(child, key, out);
                }
            }
            Value::Array(items) => items.iter().for_each(|child| walk(child, key, out)),
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(node, key, &mut out);
    out
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

struct Gate<'a> {
    index: &'a Index,
    /// `kind/name` of the component being checked.
    component: &'a str,
    hidden: Exemptions,
    builder_required: Exemptions,
    problems: Vec<String>,
}

impl<'a> Gate<'a> {
    fn new(index: &'a Index, component: &'a str) -> Gate<'a> {
        Gate {
            index,
            component,
            hidden: HIDDEN_FIELDS,
            builder_required: BUILDER_REQUIRED,
            problems: Vec::new(),
        }
    }

    fn exempt(&self, table: Exemptions, def: &str, field: &str) -> bool {
        table.iter().any(|(component, owner, name, _)| {
            *component == self.component && *owner == def && *name == field
        })
    }

    fn problem(&mut self, def: &str, path: &str, message: String) {
        self.problems
            .push(format!("{} [{def}] {path}: {message}", self.component));
    }

    /// The serde input fields of a shape, with `flatten` targets inlined.
    fn shape_fields(&self, fields: &'a [Field], from_file: &str, depth: usize) -> Vec<Field> {
        let index = self.index;
        let mut out = Vec::new();
        if depth > 8 {
            return out;
        }
        for field in fields {
            if field.skip {
                continue;
            }
            if field.flatten {
                if let Some(target) = index.resolve(&field.ty, from_file) {
                    if target.def.kind == DefKind::Struct {
                        out.extend(self.shape_fields(
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
        if !has_docs(node) {
            return;
        }
        match def.kind {
            DefKind::Struct => {
                let fields = self.shape_fields(&def.fields, &def.file, 0);
                self.check_shape(&def.name, &fields, node, path, true);
                self.check_members(&def.fields, &def.file, node, path);
            }
            DefKind::Enum => self.check_enum(def, node, path),
        }
    }

    /// Recurses into the nested types behind a shape's fields.
    fn check_members(&mut self, fields: &'a [Field], from_file: &str, node: &Value, path: &str) {
        let index = self.index;
        for field in fields {
            if field.skip {
                continue;
            }
            if field.flatten {
                if let Some(target) = index.resolve(&field.ty, from_file) {
                    if target.def.kind == DefKind::Struct {
                        self.check_members(&target.def.fields, &target.def.file, node, path);
                    }
                }
                continue;
            }
            self.check_property(field, node, path, from_file);
        }
    }

    fn check_property(&mut self, field: &Field, node: &Value, path: &str, from_file: &str) {
        let index = self.index;
        let Some(target) = index.resolve(&field.ty, from_file) else {
            return;
        };
        let Some(mut sub) = find_prop(node, &field.name) else {
            return;
        };
        if target.array {
            let Some(items) = sub.get("items").filter(|items| items.is_object()) else {
                return;
            };
            sub = items;
        }
        let def = target.def;
        let sub_path = format!("{path}.{}", field.name);
        let unit_enum = def.kind == DefKind::Enum
            && !def.variants.is_empty()
            && def
                .variants
                .iter()
                .all(|variant| variant.kind == VariantKind::Other);
        if unit_enum {
            if let Some(values) = string_values(sub) {
                for value in &values {
                    if !def
                        .variants
                        .iter()
                        .any(|variant| variant.name.as_str() == *value)
                    {
                        self.problem(
                            &def.name,
                            &sub_path,
                            format!(
                                "R6 the schema enum lists `{value}`, which is not a variant of `{}`",
                                def.name
                            ),
                        );
                    }
                }
                for variant in def.variants.iter().filter(|variant| !variant.skip) {
                    if !values.contains(variant.name.as_str()) {
                        self.problem(
                            &def.name,
                            &sub_path,
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
        self.check_def(def, sub, &sub_path);
    }

    /// R1–R5 for one field set against one documented schema node. `ghosts`
    /// is off when the node is a merged view whose keys were already checked
    /// against the union of every variant.
    fn check_shape(&mut self, def: &str, fields: &[Field], node: &Value, path: &str, ghosts: bool) {
        let props = props(node);
        let required = required(node);
        let by_name: BTreeMap<&str, &Field> = fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        if is_strict(node) {
            for field in fields {
                if self.exempt(self.hidden, def, &field.name) {
                    continue;
                }
                if !props.contains_key(field.name.as_str()) {
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
        for name in &required {
            if let Some(field) = by_name.get(name) {
                if field.optional && !self.exempt(self.builder_required, def, name) {
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
        }
        for field in fields.iter().filter(|field| !field.optional) {
            if self.exempt(self.hidden, def, &field.name) {
                continue;
            }
            if !required.contains(field.name.as_str()) {
                self.problem(
                    def,
                    path,
                    format!(
                        "R4 serde requires `{}` but the schema does not list it in `required`, so \
                         the IDE passes configs that fail at build — add it to `required` or give \
                         the field a default",
                        field.name
                    ),
                );
            }
        }
        for (name, field) in &by_name {
            if field.optional {
                continue;
            }
            let advertised = schemas(node).iter().any(|schema| {
                schema
                    .get("properties")
                    .and_then(|entries| entries.get(*name))
                    .and_then(|sub| sub.get("default"))
                    .is_some()
            });
            if advertised {
                self.problem(
                    def,
                    path,
                    format!(
                        "R5 the schema advertises a default for `{name}` but serde requires the \
                         field — add `#[serde(default …)]` or remove `default`"
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
    /// against the branch that describes it.
    fn check_enum(&mut self, def: &'a Def, node: &Value, path: &str) {
        struct Shape<'a> {
            name: String,
            /// Fields serde reads for the variant, `flatten` targets inlined.
            fields: Vec<Field>,
            /// The variant's own fields and where their types are written,
            /// for recursion.
            own: &'a [Field],
            file: String,
        }
        // Adjacent tagging nests the fields under a content key: not modelled.
        if def.layout == EnumLayout::Adjacent {
            return;
        }
        let index = self.index;
        let mut shapes: Vec<Shape<'a>> = Vec::new();
        let mut simple = true;
        for variant in &def.variants {
            if variant.skip {
                continue;
            }
            let (own, file): (&'a [Field], &str) = match variant.kind {
                VariantKind::Struct => (&variant.fields, def.file.as_str()),
                VariantKind::Newtype => {
                    match variant
                        .inner
                        .as_deref()
                        .and_then(|inner| index.resolve(inner, &def.file))
                    {
                        Some(target) if !target.array && target.def.kind == DefKind::Struct => {
                            (&target.def.fields, target.def.file.as_str())
                        }
                        _ => {
                            // A scalar or foreign payload: no field set to check.
                            simple = false;
                            (&[] as &[Field], "")
                        }
                    }
                }
                VariantKind::Other => {
                    simple = false;
                    (&[] as &[Field], "")
                }
            };
            let mut fields = self.shape_fields(own, file, 0);
            if def.layout == EnumLayout::Internal {
                fields.push(Field {
                    name: def.tag.clone().unwrap_or_default(),
                    ty: "String".to_string(),
                    optional: false,
                    skip: false,
                    flatten: false,
                });
            }
            shapes.push(Shape {
                name: variant.name.clone(),
                fields,
                own,
                file: file.to_string(),
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
            for shape in &shapes {
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
                self.check_shape(&def.name, &shape.fields, sub, &variant_path, true);
                self.check_members(shape.own, &shape.file, sub, path);
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
                    && !self.exempt(self.hidden, &def.name, &field.name)
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
        if !simple {
            return;
        }
        let tag = (def.layout == EnumLayout::Internal)
            .then_some(def.tag.as_deref())
            .flatten();
        let tag_name = tag.unwrap_or_default();
        let branches = direct_branches(node);
        // A node without `oneOf` branches is a merged view of every variant:
        // its keys were already checked against the union.
        let merged = branches.len() == 1 && std::ptr::eq(branches[0], node);
        for shape in &shapes {
            let Some(branch) = branches
                .iter()
                .find(|branch| branch_matches(branch, tag, &shape.name, &shape.fields))
            else {
                if tag.is_some() {
                    self.problem(
                        &def.name,
                        path,
                        format!(
                            "R1 tagged variant `{}` has no schema branch (no `oneOf` entry \
                             whose `properties.{tag_name}.const` or `enum` selects it)",
                            shape.name
                        ),
                    );
                }
                continue;
            };
            let variant_path = format!("{path}[{}]", shape.name);
            self.check_shape(&def.name, &shape.fields, branch, &variant_path, !merged);
            self.check_members(shape.own, &shape.file, branch, path);
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

const RULES: &str = "rules: R1 struct field missing from a strict schema · R2 schema key the \
                     struct never reads · R3 schema-required but serde-optional · R4 \
                     serde-required but not schema-required · R5 advertised default serde \
                     never applies · R6 string enum vs. variants.\nExemptions are explicit: \
                     HIDDEN_FIELDS (R1/R4) and BUILDER_REQUIRED (R3) in this file, each with a \
                     reason.";

#[test]
fn registered_schemas_match_config_structs() {
    let registered = registered_components();
    let index = Index::load();
    let mut problems = Vec::new();
    for (key, meta) in &registered {
        let mapped = CONFIG_TYPES
            .iter()
            .any(|(kind, name, ..)| row_key(kind, name) == *key);
        if has_docs(&meta.config_schema) && !mapped {
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
            problems.push(format!(
                "{key}: CONFIG_TYPES points at `{config}` in `src/{file}`, which does not exist — \
                 update the row"
            ));
            continue;
        };
        let source = index.cleaned_text(file);
        if !source.contains(&format!(": {config} =")) && !source.contains(&format!("::<{config}>"))
        {
            problems.push(format!(
                "{key}: `{config}` is no longer deserialized in `src/{file}` — point \
                 CONFIG_TYPES at the struct the builder reads"
            ));
            continue;
        }
        let mut gate = Gate::new(&index, key.as_str());
        gate.check_def(def, &meta.config_schema, config);
        problems.append(&mut gate.problems);
    }
    assert!(
        problems.is_empty(),
        "component metadata drifted from the config structs:\n{}\n{RULES}",
        problems.join("\n")
    );
}

fn exemption_target<'i>(
    index: &'i Index,
    registered: &'i BTreeMap<String, Arc<ComponentMetadata>>,
    component: &str,
    def_name: &str,
) -> Result<(&'i Def, &'i Value), String> {
    let Some((_, _, file, _)) = CONFIG_TYPES
        .iter()
        .find(|(kind, name, ..)| row_key(kind, name) == component)
    else {
        return Err(format!(
            "names `{component}`, which has no CONFIG_TYPES row"
        ));
    };
    let Some(def) = index.get(file, def_name) else {
        return Err(format!(
            "names `{def_name}`, which is not defined in `src/{file}`"
        ));
    };
    let Some(meta) = registered.get(component) else {
        return Err(format!("names `{component}`, which is not registered"));
    };
    Ok((def, &meta.config_schema))
}

#[test]
fn exemptions_stay_accurate() {
    let registered = registered_components();
    let index = Index::load();
    let mut problems = Vec::new();
    for (component, def_name, field, reason) in HIDDEN_FIELDS {
        match exemption_target(&index, &registered, component, def_name) {
            Err(why) => problems.push(format!(
                "HIDDEN_FIELDS entry `{component}` / `{def_name}.{field}` {why}"
            )),
            Ok((def, schema)) => {
                let is_input = def.fields.iter().any(|f| f.name == *field && !f.skip);
                if !is_input {
                    problems.push(format!(
                        "stale HIDDEN_FIELDS entry `{component}` / `{def_name}.{field}` \
                         ({reason}): the struct has no such input field"
                    ));
                } else if all_named(schema, "properties").contains(*field) {
                    problems.push(format!(
                        "stale HIDDEN_FIELDS entry `{component}` / `{def_name}.{field}` \
                         ({reason}): the schema documents the field now — remove the entry"
                    ));
                }
            }
        }
    }
    for (component, def_name, field, reason) in BUILDER_REQUIRED {
        match exemption_target(&index, &registered, component, def_name) {
            Err(why) => problems.push(format!(
                "BUILDER_REQUIRED entry `{component}` / `{def_name}.{field}` {why}"
            )),
            Ok((def, schema)) => match def.fields.iter().find(|f| f.name == *field && !f.skip) {
                None => problems.push(format!(
                    "stale BUILDER_REQUIRED entry `{component}` / `{def_name}.{field}` \
                         ({reason}): the struct has no such input field"
                )),
                Some(field_def) if !field_def.optional => problems.push(format!(
                    "stale BUILDER_REQUIRED entry `{component}` / `{def_name}.{field}` \
                         ({reason}): serde requires the field now — remove the entry"
                )),
                Some(_) if !all_named(schema, "required").contains(*field) => {
                    problems.push(format!(
                        "stale BUILDER_REQUIRED entry `{component}` / \
                             `{def_name}.{field}` ({reason}): the schema no longer lists the \
                             field as required — remove the entry"
                    ))
                }
                Some(_) => {}
            },
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
    /// Brokers. A doc comment may quote `r#"struct Fake { a: u8 }"#`.
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
    /// Commas and `->` inside the type must not split the field.
    pub handler: Option<fn(a: u8, b: u8) -> bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Extra {
    pub host: String,
    pub port: Option<u16>,
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
}

struct Borrowed<'a> {
    value: &'a str,
}

struct Tuple(String);

struct Unit;
"##;

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
            "properties": {"host": {"type": "string"}, "port": {"type": "integer"}},
            "required": ["host"]
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
                "nested": extra,
                "many": {"type": "array", "items": extra},
                "handler": {},
                "mode": {"type": "object", "oneOf": [
                    {"properties": {"type": {"const": "cluster"}, "urls": {"type": "array"}},
                     "required": ["type", "urls"]},
                    {"properties": {"type": {"const": "single"}, "url": {"type": "string"},
                                    "insecure": {"type": "boolean", "default": false}},
                     "required": ["type", "url"]}
                ]},
                "kind": {"type": "string", "enum": ["fail", "skip"]}
            },
            "required": ["brokers", "host", "many", "mode", "kind"]
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
                "brokers", "flag", "renamed", "internal", "extra", "nested", "many", "mode",
                "kind", "handler",
            ]
        );
        // Option<_> and #[serde(default …)] make a field optional; a bare
        // `bool`/`Vec<_>` does not.
        for name in ["flag", "renamed", "nested", "handler"] {
            assert!(field(demo, name).optional, "{name} is optional");
        }
        for name in ["brokers", "extra", "many", "mode", "kind"] {
            assert!(!field(demo, name).optional, "{name} is required");
        }
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

        let mode = index.get("input/demo.rs", "Mode").unwrap();
        assert_eq!(mode.kind, DefKind::Enum);
        assert_eq!(mode.tag.as_deref(), Some("type"));
        assert_eq!(mode.layout, EnumLayout::Internal);
        let names: Vec<&str> = mode.variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["cluster", "single"]);
        assert!(!mode.variants[1].fields[0].optional);
        let insecure = &mode.variants[1].fields[1];
        assert_eq!(
            (insecure.name.as_str(), insecure.optional),
            ("insecure", true)
        );

        let kind = index.get("input/demo.rs", "Kind").unwrap();
        assert_eq!(kind.layout, EnumLayout::External);
        assert_eq!(
            kind.variants
                .iter()
                .map(|v| v.name.as_str())
                .collect::<Vec<_>>(),
            ["fail", "skip"]
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
    fn snake_case_follows_serde() {
        for (rust, serde) in [
            ("Cluster", "cluster"),
            ("KeyShared", "key_shared"),
            ("HTTPServer", "http_server"),
            ("Field2Name", "field2_name"),
            ("already_snake", "already_snake"),
        ] {
            assert_eq!(snake_case(rust), serde, "{rust}");
        }
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

        // A flattened field is part of the root shape.
        let mut schema = conforming();
        schema["properties"].as_object_mut().unwrap().remove("host");
        assert!(reported(
            &problems(&index, "DemoConfig", &schema),
            "R1 field `host`"
        ));

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

        let mut renamed = conforming.clone();
        renamed["oneOf"][0]["properties"]["http"]["required"] = json!(["url", "method"]);
        assert!(reported(
            &run(&renamed),
            "R3 the schema marks `method` required"
        ));

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
    fn the_real_index_resolves_types_across_modules() {
        let index = Index::load();
        let tls = index
            .resolve("crate::pulsar::common::PulsarTlsConfig", "input/pulsar.rs")
            .expect("crate paths resolve");
        assert_eq!(
            (tls.def.file.as_str(), tls.def.name.as_str()),
            ("pulsar/common.rs", "PulsarTlsConfig")
        );
        assert!(!tls.array);

        let temporaries = index
            .resolve("Option<Vec<TemporaryConfig>>", "processor/sql.rs")
            .expect("wrappers are peeled");
        assert!(temporaries.array);
        assert_eq!(temporaries.def.name, "TemporaryConfig");

        // `MysqlConfig` exists in both the SQL input and output: the file the
        // type is written in wins.
        assert_eq!(
            index
                .resolve("MysqlConfig", "output/sql.rs")
                .unwrap()
                .def
                .file,
            "output/sql.rs"
        );
        assert_eq!(
            index
                .resolve("MysqlConfig", "input/sql.rs")
                .unwrap()
                .def
                .file,
            "input/sql.rs"
        );
        assert!(index.resolve("String", "output/sql.rs").is_none());
        assert!(index
            .resolve("std::time::Duration", "output/sql.rs")
            .is_none());
    }
}
