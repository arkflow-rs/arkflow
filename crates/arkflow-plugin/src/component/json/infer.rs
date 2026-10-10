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

//! Low-allocation JSON schema inference.
//!
//! Drop-in replacement for `arrow_json::reader::infer_json_schema` on
//! newline-delimited buffers: records are deserialized into a **borrowed**
//! tree ([`CowValue`] — strings and keys reference the input) instead of
//! `serde_json::Value`, so inference pays one parse pass without
//! materializing a per-record value tree.
//!
//! The merge state machine mirrors arrow-json's `reader/schema.rs` exactly —
//! down to the same `IndexMap`/`IndexSet` containers, so field order
//! (first-seen across records, document order within a record), keyed lookups
//! (no quadratic scans on wide records), number classification (`Int64` iff
//! `is_i64` else `Float64`), lazy `Any` for nulls, first-element array
//! classification, scalar⊕list coercion, and incompatible-merge errors all
//! match. The differential tests at the bottom pin both implementations to
//! byte-equal schemas on a boundary corpus and a randomized corpus.

use std::borrow::Cow;
use std::sync::Arc;

use arkflow_core::Error;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxBuildHasher;

/// `IndexMap` with the Fx hasher: upstream-parity keyed lookups (iteration
/// order is insertion-order regardless of hasher) without SipHash dominating
/// small records.
type ObjMap<K, V> = IndexMap<K, V, FxBuildHasher>;
type ObjSet<V> = IndexSet<V, FxBuildHasher>;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

/// A borrowed JSON tree. Objects keep `serde_json::Map` (IndexMap under this
/// workspace's `preserve_order`) semantics — insertion-ordered, last value
/// wins — via `IndexMap` over borrowed keys, matching the iteration order the
/// upstream inference observes on `serde_json::Value` without per-field
/// `String` allocations.
///
/// Scalar payloads are never consumed by the merge rules (they classify by
/// variant only) but are kept so inference errors can render the offending
/// value like upstream's `{v:?}` messages.
#[derive(Debug)]
#[allow(dead_code)]
enum CowValue<'de> {
    Null,
    Bool(bool),
    Int(i64),
    /// Positive integer that did not fit `i64` (`visit_u64`).
    UInt(u64),
    Float(f64),
    Str(Cow<'de, str>),
    Array(Vec<CowValue<'de>>),
    Object(ObjMap<Cow<'de, str>, CowValue<'de>>),
}

impl<'de> CowValue<'de> {
    /// `serde_json::Number::is_i64` parity: `Int` and small `UInt` are i64.
    fn numeric_type(&self) -> DataType {
        match self {
            CowValue::Int(_) => DataType::Int64,
            CowValue::UInt(u) if *u <= i64::MAX as u64 => DataType::Int64,
            _ => DataType::Float64,
        }
    }
}

impl<'de> Deserialize<'de> for CowValue<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CowValueVisitor;

        impl<'de> Visitor<'de> for CowValueVisitor {
            type Value = CowValue<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("any valid JSON value")
            }

            fn visit_unit<E: de::Error>(self) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Null)
            }

            fn visit_none<E: de::Error>(self) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Null)
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Bool(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Int(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<CowValue<'de>, E> {
                Ok(CowValue::UInt(v))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Float(v))
            }

            fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Str(Cow::Borrowed(v)))
            }

            // serde_json (slice input) routes unescaped strings to
            // `visit_borrowed_str`; this fallback only fires for non-borrowing
            // deserializers, so an owned copy is fine there.
            fn visit_str<E: de::Error>(self, v: &str) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Str(Cow::Owned(v.to_owned())))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<CowValue<'de>, E> {
                Ok(CowValue::Str(Cow::Owned(v)))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<CowValue<'de>, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = seq.next_element::<CowValue<'de>>()? {
                    out.push(v);
                }
                Ok(CowValue::Array(out))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<CowValue<'de>, A::Error> {
                let mut out = ObjMap::with_capacity_and_hasher(8, FxBuildHasher);
                while let Some((k, v)) = map.next_entry::<Cow<'de, str>, CowValue<'de>>()? {
                    // IndexMap::insert: first-seen position kept, value replaced.
                    out.insert(k, v);
                }
                Ok(CowValue::Object(out))
            }
        }

        deserializer.deserialize_any(CowValueVisitor)
    }
}

/// Mirror of arrow-json's private `InferredType`, with the same
/// `IndexMap`/`IndexSet` containers — keyed lookups (no quadratic scans on
/// wide records) and identical iteration order to upstream.
#[derive(Debug, Clone)]
enum InferredType {
    Scalar(ObjSet<DataType>),
    Array(Box<InferredType>),
    Object(ObjMap<String, InferredType>),
    Any,
}

impl InferredType {
    fn is_none_or_any(ty: Option<&Self>) -> bool {
        matches!(ty, None | Some(InferredType::Any))
    }

    fn merge(&mut self, other: InferredType) -> Result<(), Error> {
        match (self, other) {
            (InferredType::Array(s), InferredType::Array(o)) => s.merge(*o)?,
            (InferredType::Scalar(s), InferredType::Scalar(o)) => {
                s.extend(o);
            }
            (InferredType::Object(s), InferredType::Object(o)) => {
                for (k, v) in o {
                    s.entry(k).or_insert(InferredType::Any).merge(v)?;
                }
            }
            (s @ InferredType::Any, v) => *s = v,
            (_, InferredType::Any) => {}
            // convert a scalar type to a single-item scalar array type.
            (InferredType::Array(s), other @ InferredType::Scalar(_)) => s.merge(other)?,
            (s @ InferredType::Scalar(_), InferredType::Array(mut other_inner)) => {
                other_inner.merge(s.clone())?;
                *s = InferredType::Array(other_inner);
            }
            // incompatible types
            (s, o) => {
                return Err(Error::Process(format!(
                    "Schema inference error: Incompatible type found during schema inference: {s:?} v.s. {o:?}"
                )))
            }
        }
        Ok(())
    }
}

fn list_type_of(ty: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(ty, true)))
}

/// Coerce data type during inference (arrow-json parity):
/// * `Int64` and `Float64` should be `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * All other types are coerced to `Utf8`
fn coerce_data_type(dt: Vec<&DataType>) -> DataType {
    let mut dt_iter = dt.into_iter().cloned();
    let dt_init = dt_iter.next().unwrap_or(DataType::Utf8);

    dt_iter.fold(dt_init, |l, r| match (l, r) {
        (DataType::Null, o) | (o, DataType::Null) => o,
        (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Float64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::List(l), DataType::List(r)) => {
            list_type_of(coerce_data_type(vec![l.data_type(), r.data_type()]))
        }
        // coerce scalar and scalar array into scalar array
        (DataType::List(e), not_list) | (not_list, DataType::List(e)) => {
            list_type_of(coerce_data_type(vec![e.data_type(), &not_list]))
        }
        _ => DataType::Utf8,
    })
}

fn generate_datatype(t: &InferredType) -> Result<DataType, Error> {
    Ok(match t {
        InferredType::Scalar(hs) => coerce_data_type(hs.iter().collect()),
        InferredType::Object(spec) => DataType::Struct(generate_fields(spec)?),
        InferredType::Array(ele_type) => list_type_of(generate_datatype(ele_type)?),
        InferredType::Any => DataType::Null,
    })
}

fn generate_fields(spec: &ObjMap<String, InferredType>) -> Result<Fields, Error> {
    spec.iter()
        .map(|(k, types)| Ok(Field::new(k, generate_datatype(types)?, true)))
        .collect()
}

fn set_object_scalar_field_type(
    field_types: &mut ObjMap<String, InferredType>,
    key: &str,
    ftype: DataType,
) -> Result<(), Error> {
    if InferredType::is_none_or_any(field_types.get(key)) {
        field_types.insert(key.to_string(), InferredType::Scalar(ObjSet::default()));
    }

    match field_types.get_mut(key).expect("entry exists") {
        InferredType::Scalar(hs) => {
            hs.insert(ftype);
            Ok(())
        }
        // in case of column contains both scalar type and scalar array type,
        // we convert type of this column to scalar array.
        scalar_array @ InferredType::Array(_) => {
            scalar_array.merge(InferredType::Scalar(ObjSet::from_iter([ftype])))
        }
        t => Err(Error::Process(format!(
            "Schema inference error: Expected scalar or scalar array JSON type, found: {t:?}"
        ))),
    }
}

fn infer_scalar_array_type(array: &[CowValue]) -> Result<InferredType, Error> {
    let mut hs = ObjSet::default();

    for v in array {
        match v {
            CowValue::Null => {}
            CowValue::Int(_) | CowValue::UInt(_) | CowValue::Float(_) => {
                hs.insert(v.numeric_type());
            }
            CowValue::Bool(_) => {
                hs.insert(DataType::Boolean);
            }
            CowValue::Str(_) => {
                hs.insert(DataType::Utf8);
            }
            CowValue::Array(_) | CowValue::Object(_) => {
                return Err(Error::Process(format!(
                    "Schema inference error: Expected scalar value for scalar array, got: {v:?}"
                )))
            }
        }
    }

    Ok(InferredType::Scalar(hs))
}

fn infer_nested_array_type(array: &[CowValue]) -> Result<InferredType, Error> {
    let mut inner_ele_type = InferredType::Any;

    for v in array {
        match v {
            CowValue::Array(inner_array) => {
                inner_ele_type.merge(infer_array_element_type(inner_array)?)?;
            }
            x => {
                return Err(Error::Process(format!(
                    "Schema inference error: Got non array element in nested array: {x:?}"
                )))
            }
        }
    }

    Ok(InferredType::Array(Box::new(inner_ele_type)))
}

fn infer_struct_array_type(array: &[CowValue]) -> Result<InferredType, Error> {
    let mut field_types = ObjMap::default();

    for v in array {
        match v {
            CowValue::Object(map) => collect_field_types_from_object(&mut field_types, map)?,
            _ => {
                return Err(Error::Process(format!(
                    "Schema inference error: Expected struct value for struct array, got: {v:?}"
                )))
            }
        }
    }

    Ok(InferredType::Object(field_types))
}

fn infer_array_element_type(array: &[CowValue]) -> Result<InferredType, Error> {
    match array.first() {
        None => Ok(InferredType::Any), // empty array, return any type that can be updated later
        Some(a) => match a {
            CowValue::Array(_) => infer_nested_array_type(array),
            CowValue::Object(_) => infer_struct_array_type(array),
            _ => infer_scalar_array_type(array),
        },
    }
}

fn collect_field_types_from_object(
    field_types: &mut ObjMap<String, InferredType>,
    map: &ObjMap<Cow<'_, str>, CowValue<'_>>,
) -> Result<(), Error> {
    for (k, v) in map {
        match v {
            CowValue::Array(array) => {
                let ele_type = infer_array_element_type(array)?;

                // `is_none_or_any` parity: a missing field AND an Any
                // placeholder (seen only as null so far) both get replaced by
                // the array-shaped initial state.
                if InferredType::is_none_or_any(field_types.get(k.as_ref())) {
                    let init = match &ele_type {
                        InferredType::Scalar(_) => {
                            InferredType::Array(Box::new(InferredType::Scalar(ObjSet::default())))
                        }
                        InferredType::Object(_) => {
                            InferredType::Array(Box::new(InferredType::Object(ObjMap::default())))
                        }
                        InferredType::Any | InferredType::Array(_) => {
                            // set inner type to any for nested array as well
                            // so it can be updated properly from subsequent type merges
                            InferredType::Array(Box::new(InferredType::Any))
                        }
                    };
                    field_types.insert(k.to_string(), init);
                }

                match field_types.get_mut(k.as_ref()).expect("entry exists") {
                    InferredType::Array(inner_type) => {
                        inner_type.merge(ele_type)?;
                    }
                    // in case of column contains both scalar type and scalar array type, we
                    // convert type of this column to scalar array.
                    field_type @ InferredType::Scalar(_) => {
                        field_type.merge(ele_type)?;
                        *field_type = InferredType::Array(Box::new(field_type.clone()));
                    }
                    t => {
                        return Err(Error::Process(format!(
                            "Schema inference error: Expected array json type, found: {t:?}"
                        )))
                    }
                }
            }
            CowValue::Bool(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Boolean)?;
            }
            CowValue::Null => {
                // we treat json as nullable by default when inferring, so just
                // mark existence of a field if it wasn't known before
                if !field_types.contains_key(k.as_ref()) {
                    field_types.insert(k.to_string(), InferredType::Any);
                }
            }
            CowValue::Int(_) | CowValue::UInt(_) | CowValue::Float(_) => {
                set_object_scalar_field_type(field_types, k, v.numeric_type())?;
            }
            CowValue::Str(_) => {
                set_object_scalar_field_type(field_types, k, DataType::Utf8)?;
            }
            CowValue::Object(inner_map) => {
                if InferredType::is_none_or_any(field_types.get(k.as_ref())) {
                    field_types.insert(k.to_string(), InferredType::Object(ObjMap::default()));
                }
                match field_types.get_mut(k.as_ref()).expect("entry exists") {
                    InferredType::Object(inner_field_types) => {
                        collect_field_types_from_object(inner_field_types, inner_map)?;
                    }
                    t => {
                        return Err(Error::Process(format!(
                            "Schema inference error: Expected object json type, found: {t:?}"
                        )))
                    }
                }
            }
        }
    }

    Ok(())
}

/// Infer the schema of newline-delimited JSON records, scanning the whole
/// buffer without materializing per-record `serde_json::Value` trees.
///
/// Record extraction mirrors arrow-json's `ValueIter` exactly (line-oriented:
/// trim each line, skip blank lines, one JSON value per line with trailing
/// content rejected), so the failure set is identical too. Output is
/// byte-equal to `arrow_json::reader::infer_json_schema` (pinned by
/// differential tests), including field ordering, nullability and failures.
pub(super) fn infer_json_schema_streaming(content: &[u8]) -> Result<Schema, Error> {
    let mut field_types = ObjMap::default();

    for line in content.split(|&b| b == b'\n') {
        let line = std::str::from_utf8(line).map_err(|e| {
            Error::Process(format!(
                "Schema inference error: Failed to read JSON record: {e}"
            ))
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record = serde_json::from_str::<CowValue>(trimmed)
            .map_err(|e| Error::Process(format!("Schema inference error: Not valid JSON: {e}")))?;
        match record {
            CowValue::Object(map) => collect_field_types_from_object(&mut field_types, &map)?,
            value => {
                return Err(Error::Process(format!(
                    "Schema inference error: Expected JSON record to be an object, found {value:?}"
                )))
            }
        }
    }

    Ok(Schema::new(generate_fields(&field_types)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn infer_upstream(content: &[u8]) -> Result<Schema, String> {
        let mut cursor = Cursor::new(content.to_vec());
        arrow_json::reader::infer_json_schema(&mut cursor, None)
            .map(|(schema, _)| schema)
            .map_err(|e| e.to_string())
    }

    fn infer_ours(content: &[u8]) -> Result<Schema, String> {
        infer_json_schema_streaming(content).map_err(|e| e.to_string())
    }

    fn assert_parity(content: &[u8]) {
        let upstream = infer_upstream(content);
        let ours = infer_ours(content);
        match (upstream, ours) {
            (Ok(a), Ok(b)) => assert_eq!(
                a,
                b,
                "schema mismatch for input:\n{}\nupstream:\n{a:?}\nours:\n{b:?}",
                String::from_utf8_lossy(content)
            ),
            (Err(_), Err(_)) => {}
            (upstream, ours) => panic!(
                "failure-set mismatch for input:\n{}\nupstream: {upstream:?}\nours: {ours:?}",
                String::from_utf8_lossy(content)
            ),
        }
    }

    #[test]
    fn matches_upstream_on_boundary_corpus() {
        // Records mirroring arrow-json's own inference test cases.
        let mixed_arrays = r#"{"a":1, "b":["foo"], "c":[true], "d":["foo"]}
{"a":2.5, "b":[2], "c":[1.5], "d":[]}
{"a":null, "b":[1], "c":[1], "d":[null]}
{"a":4, "b":[[1]], "c":[[1]], "d":[[1]]}"#;

        let nested_structs = r#"{"c1": {"a": true, "b": {"c": "text"}}, "c2": 1}
{"c1": {"a": false, "b": null}, "c2": 0}
{"c1": {"a": true, "b": {"c": "text"}}, "c3": "ok"}"#;

        let struct_in_list = r#"{"c1": [{"a": "foo", "b": 100}], "c2": 1, "c3": []}
{"c1": [{"a": "bar", "b": 2}, {"a": "foo", "c": true}], "c2": 0, "c3": []}
{"c1": [], "c2": 0.5, "c3": []}"#;

        let nested_list = r#"{"c1": [], "c2": 12}
{"c1": [["a", "b"], ["c"]]}
{"c1": [["foo"]], "c2": 0.11}"#;

        let null_matrix = r#"{"in":1,    "ni":null, "ns":null, "sn":"4",  "n":null, "an":[],   "na": null, "nas":null}
{"in":null, "ni":2,    "ns":"3",  "sn":null, "n":null, "an":null, "na": [],   "nas":["8"]}
{"in":1,    "ni":null, "ns":null, "sn":"4",  "n":null, "an":[],   "na": null, "nas":[]}"#;

        let null_then_object = r#"{"obj":null}
{"obj":{"foo":1}}"#;

        let bigger_than_i64 = format!(
            r#"{{"n": {}}}
{{"n": {}}}
{{"n": 1}}
{{"n": -1.5}}"#,
            (i64::MAX as i128) + 1,
            (i64::MIN as i128) - 1
        );

        let scalar_list_coercion = r#"{"a": 1}
{"a": [1, 2]}
{"b": [true, false]}
{"b": true}
{"c": ["x"]}
{"c": 5}"#;

        let incompatible = r#"{"a": {"x": 1}}
{"a": 1}"#;

        let corpus: Vec<&str> = vec![
            mixed_arrays,
            nested_structs,
            struct_in_list,
            nested_list,
            null_matrix,
            null_then_object,
            &bigger_than_i64,
            scalar_list_coercion,
            incompatible,
            // invalid JSON / non-object records must fail identically
            "}",
            "42",
            "[1, 2]",
            r#""just a string""#,
            // single-record and packing variants
            r#"{"a":1}"#,
            r#"{"a":1}{"b":2}"#,
            "   \n \n ",
            "",
            // escaped and duplicate keys within one record
            r#"{"a\u0041b": 1, "aab": 2}"#,
            r#"{"dup": 1, "dup": "x"}"#,
            // deep nesting
            r#"{"deep": [[[[[1]]]]]}"#,
            r#"{"deep": {"a": {"b": {"c": [1, {"d": null}]}}}}"#,
        ];

        for case in corpus {
            assert_parity(case.as_bytes());
        }
    }

    #[test]
    fn matches_upstream_on_random_corpus() {
        // Deterministic xorshift; generates records with parameterized
        // nesting depth and type spread (including uint-over-i64, empty
        // containers and nulls — the shapes where merge rules bite).
        struct Rng(u64);
        impl Rng {
            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                self.0 = x;
                x
            }
            fn below(&mut self, n: u64) -> u64 {
                self.next() % n
            }
        }

        fn gen_value(rng: &mut Rng, depth: u32, out: &mut String) {
            match rng.below(if depth == 0 { 7 } else { 10 }) {
                0 => out.push_str("null"),
                1 => out.push_str(if rng.below(2) == 0 { "true" } else { "false" }),
                2 => out.push_str(&(rng.below(1_000_000) as i64).to_string()),
                3 => out.push_str(&(rng.below(1_000_000) + u64::MAX / 2).to_string()),
                4 => {
                    let f = rng.below(1_000_000) as f64 / 64.0;
                    out.push_str(&format!("{f}"));
                }
                5 => {
                    out.push('"');
                    let len = rng.below(6);
                    for _ in 0..len {
                        out.push((b'a' + rng.below(3) as u8) as char);
                    }
                    out.push('"');
                }
                6 => out.push_str("[]"),
                7 => {
                    out.push('[');
                    let len = rng.below(4);
                    for i in 0..len {
                        if i > 0 {
                            out.push(',');
                        }
                        gen_value(rng, depth - 1, out);
                    }
                    out.push(']');
                }
                8 => {
                    out.push('{');
                    let len = rng.below(4);
                    for i in 0..len {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push('"');
                        out.push((b'a' + rng.below(4) as u8) as char);
                        out.push_str("\":");
                        gen_value(rng, depth - 1, out);
                    }
                    out.push('}');
                }
                9 => out.push_str("{}"),
                _ => unreachable!(),
            }
        }

        let mut rng = Rng(0x9E3779B97F4A7C15);
        for case_idx in 0..300usize {
            let mut content = String::new();
            let records = 1 + rng.below(6) as usize;
            for r in 0..records {
                if r > 0 {
                    content.push('\n');
                }
                content.push('{');
                let fields = rng.below(6);
                for f in 0..fields {
                    if f > 0 {
                        content.push(',');
                    }
                    content.push('"');
                    content.push((b'k' + rng.below(5) as u8) as char);
                    content.push_str("\":");
                    gen_value(&mut rng, 2, &mut content);
                }
                content.push('}');
            }

            let bytes = content.as_bytes();
            let upstream = infer_upstream(bytes);
            let ours = infer_ours(bytes);
            match (upstream, ours) {
                (Ok(a), Ok(b)) => assert_eq!(
                    a, b,
                    "case {case_idx} schema mismatch for:\n{content}\nupstream:\n{a:?}\nours:\n{b:?}"
                ),
                (Err(_), Err(_)) => {}
                (upstream, ours) => panic!(
                    "case {case_idx} failure-set mismatch for:\n{content}\nupstream: {upstream:?}\nours: {ours:?}"
                ),
            }
        }
    }
}
