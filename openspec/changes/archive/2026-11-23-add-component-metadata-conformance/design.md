## Context

Every component registers a hand-written JSON Schema (`ComponentMetadata::with_schema(..., json!({...}))`) next to a serde-derived config struct. Nothing reconciles the two: `registry_consistency.rs` checks builder/metadata name alignment, `metadata_examples_validate.rs` exercises `config_example`, and the docs snapshot test only proves the committed inventory equals the registry. The survey behind this change found drift in ten components (fields missing from schemas, ghost fields, required/default drift, enum values serde never accepted, wrong descriptions). `component-config-honesty` already states the normative rule; what is missing is a gate that turns the next drift into a test failure.

Constraints: no new dependency (`Cargo.lock` must stay untouched — the workspace pins are fragile and every direct dependency becomes a dependabot major-bump target), and the gate must cover all 48 configured components plus anything registered later.

## Goals / Non-Goals

**Goals:**

- A CI gate that fails on any drift between a component's config struct and its registered schema: field coverage (both directions), `required`, `default`, string-enum values.
- Full coverage: a component registered without being mapped fails the gate with an actionable message.
- Deliberate exceptions only through explicit, staleness-checked allowlists.
- Fix all drift found today so the gate lands green.

**Non-Goals:**

- Auto-generating schemas from structs (schemars). The hand-written schemas stay the source of descriptive text; the gate only checks structural agreement.
- Documenting the internals of opaque nested nodes (e.g. `file` input `input_type`, a bare `{"type": "object"}`): a schema node without `additionalProperties: false` accepts any key, so an undocumented nested field is a completeness gap, not a false rejection.
- Variant-level `required` when an internally tagged enum is documented as one merged object without `oneOf` branches: coverage is checked on the union, because nothing in such a schema says which key belongs to which variant. With `oneOf` branches every variant is checked against its own branch.
- The *value* of a function-backed default (`default = "…"`) or of a type without a fixed `Default` (enums, maps, `Option`): the gate checks that the field may be omitted, not what the omitted value is. Defaults whose value is fixed (`bool`, numbers, `String`, `Vec`) are compared with the advertised one.
- Adjacent tagging (`tag` plus `content`): not modelled, nothing is checked there.
- Changing the `milvus` output `id_field` contract (optional on the struct, enforced by the builder with a domain-specific message).
- The top-level engine configuration schema (`build_config_schema`, job snippets) — separate drift, tracked by the docs audit.

## Decisions

**D1 — Source-scan gate with a std-only struct parser, no new dependency.**
Alternatives: (a) `schemars` derive on every config struct — needs derives on all nested types plus overrides for external types, and adds a direct dependency; (b) `syn` as a dev-dependency — robust but adds a direct dependency and a `Cargo.lock` change; (c) hand-maintained field lists — the list itself drifts, which is the bug being fixed. The chosen approach follows the existing structural tests (`connector_error_contract.rs` reads `src/` under `CARGO_MANIFEST_DIR`): the gate reads each config struct from source, so the field list can never go stale. The parser handles what the config structs actually use — container/field `#[serde(...)]` attributes (`rename`, `default`, `skip`, `skip_deserializing`, `flatten`, `tag`, `rename_all`), `Option<_>` / default-annotated optionality, multi-line generic types, doc comments, string literals, raw strings and lifetimes — and is fail-closed: a field whose name is not identifier-shaped or whose type text contains a stray field definition is a test failure, never a silent skip. A definition that does not parse, a bare name that several local definitions share, and a type the gate cannot read are reported (R0) where a documented field refers to them — never skipped. serde's `rename_all` conversions are reproduced character for character, and a unit test compares every rule with the real derive. It carries its own unit tests plus mutation checks (each rule must fail when its target drift is introduced).

**D2 — Explicit `(kind, name) → (file, config struct)` table, guarded for completeness and staleness.**
Deriving the mapping from builder code would need more free-text heuristics than it saves. The table is one line per component; the gate asserts (1) every registered component whose schema documents keys has a row and every row names a registered component, (2) the struct exists in the named file, and (3) the struct is deserialized inside the `impl` block of the builder registered under that name (the whole file is the fallback when the registration is not written in the usual shape), so a builder switching types cannot leave a stale row behind. Components that take no configuration are exempt: a unit schema (`config_schema == {"type":"object"}`, e.g. `output/drop`) needs nothing, and a closed, empty schema for a builder that ignores its config (e.g. `codec/debezium_json`) is listed in `NO_CONFIG`.

**D3 — Seven rules, applied per schema node with JSON Schema semantics.**

| Rule | Applies at | Meaning |
| --- | --- | --- |
| R0 readable | every referenced type | a type the gate cannot read, or an ambiguous bare name, is reported |
| R1 coverage | nodes with `additionalProperties: false` | every serde input field appears in `properties` |
| R2 no ghosts | any node with `properties` | every documented key is a serde input field |
| R3 required ⊆ serde-required | any node with `required` | the schema never rejects what serde accepts, and never requires a key nothing reads |
| R4 serde-required ⊆ required | any node with `properties` | the schema never accepts what serde rejects |
| R5 default ⇒ optional | any node with `properties` | an advertised default is real and, where its value is fixed, equals the value serde produces |
| R6 enum values | string-enum schema on a unit-variant enum | the listed values are exactly serde's variant names |

`#[serde(flatten)]` fields contribute the flattened type's fields inline (recursively); `skip`/`skip_deserializing` fields are not inputs and are ignored. Nested object properties recurse into the nested type. R1 deliberately does not apply at permissive nodes: without `additionalProperties: false` an undocumented key is still accepted.

**D4 — Enums follow serde's tagging.** Externally tagged enums (serde's default, e.g. `BackendConfig`) get one property per variant, so each variant's key is checked against the inner struct. Internally tagged enums (`#[serde(tag = "…")]`, e.g. the Redis and NATS modes) get coverage R1/R2 on the union of all variants first, then R1–R5 per variant against the `oneOf` branch that selects it (by the tag's `const`, or its membership in the tag's `enum` when the schema is one flat object). Untagged enums match a branch by containing every field of the variant. Unit-variant enums behind a string-enum schema get R6; tuple-payload variants fall back to the union checks, and adjacent tagging (`content`) is not modelled.

**D5 — Two allowlists, both self-checking.** `HIDDEN_FIELDS` (config field deliberately absent from the schema — starts empty) exempts a field from R1/R4; `BUILDER_REQUIRED` (schema-required field enforced by the builder rather than serde — `output/milvus` `id_field`) exempts a field from R3. A second pass asserts every entry is still needed; an entry whose field has since become conforming fails the gate, so the lists cannot rot.

**D6 — Fix direction follows which side is the truth.**
Schema-side when the struct is right: add missing fields (`kafka` input `transactional_offsets`, `pulsar` input/output `tls`, `milvus_search` `concurrency`), remove the `http` input ghost fields and the `nats` output `stream` key, correct the `pulsar` input `subscription_type` and `kafka` output `compression` values, fix the `http`/`redis` input descriptions, mark `http` output `timeout_ms` required, add the nested `required` lists. Struct-side only where the schema already promised a default: `#[serde(default)]` for `start_from_latest` (false) and `#[serde(default = "...")]` for the `http` output `method` (POST). `milvus` output `id_field` keeps its builder check (the message explains the upsert constraint) and is exempted from R3 through `BUILDER_REQUIRED`.

**D7 — Removed keys fail loudly.** Dropping `method`/`interval` from the `http` input schema would leave serde silently discarding them in existing configs. The builder rejects them with an explanatory error, following the `pulsar` `tls.ca_file` precedent required by `component-config-honesty`.

## Risks / Trade-offs

- [The std-only parser mis-parses a syntax shape it does not know] → fail-closed validation (identifier-shaped names, no stray `pub`/`:` in type text), unit tests over the tricky shapes, and the gate runs over all 48 components on every CI run, so a mis-parse surfaces immediately rather than silently dropping a field.
- [`required`/`default`/enum-value rules are stricter than the original registry item asked] → each rule has a real instance today (R2: `http` input, `nats` output; R3: none true, one builder-enforced; R4: `kafka` input, `http` output, window `join`, `schema_registry` `auth`; R5: `kafka` input, `http` output; R6: `pulsar` input, `kafka` output), and the exemptions are explicit and documented.
- [Omitting `start_from_latest` / `method` now succeeds where it used to fail] → this is the behavior the schema and the docs pages already advertise; existing configs that spell the field out are unaffected. Noted in CHANGELOG under "Changed".
- [`http` input configs carrying `method`/`interval` now fail at build] → they were never acted on (server-only input); the error names the key and the reason.
- [Regenerated inventory changes the published IDE schema] → expected and required; `docs_inventory_snapshot` enforces it.

## Migration Plan

One existing configuration changes outcome: an `http` **input** config that still carries `method` or `interval` — keys of the polling client mode, which does not exist — now fails at build with an error naming the key. Remove them. (The `http` **output** `method` is unaffected and stays supported.) Configs that omit `kafka` input `start_from_latest` or `http` output `method` used to fail and now take the documented default, so they need nothing. Run `--validate` after upgrading: it reports the first case. The generated inventory and config schema are regenerated in the same change. Rollback is a revert of the change.

## Open Questions

None.
