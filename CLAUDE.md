# CLAUDE.md

Agent guidance for this repository lives in [AGENTS.md](./AGENTS.md) — the
single checked-in authority for workspace layout, commands, architecture
rules, component conventions, and gotchas. It is kept current by the same
review and CI process as the code.

@AGENTS.md

Do not restate workspace facts (crate list, toolchain version, component
counts, config examples) in this file; duplicated facts drift. Anything you
need is either in `AGENTS.md` or in the generated, machine-checked surfaces:
`./target/release/arkflow components list`,
`docs/reference/component-inventory.json`.
