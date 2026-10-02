## Why

The VRL processor pins `vrl = "0.30"` (released mid-2025); the crate has since moved to 0.36 with six breaking minor lines. The VRL stdlib evolves function-by-function between releases, so staying multiple versions behind accumulates silent drift in expression semantics and blocks all transitive fixes. This is Stage 4 of the staged dependency-update track (#1290 lock refresh, #1291 jsonwebtoken 11, #1292 sqlx 0.9, #1293 redis 1.7 — all merged); each prior stage landed independently behind CI-green PRs.

## What Changes

- Bump `vrl` from 0.30 to 0.36 in `crates/arkflow-plugin/Cargo.toml` (feature set: `value`, `compiler`, `stdlib` — carried over unless 0.36 renamed them).
- Migrate the VRL processor (`crates/arkflow-plugin/src/processor/vrl.rs`) across any API breakage in `vrl::prelude`, `Program`, `VrlValue`, and stdlib wiring.
- Preserve every behavior codified in the existing `vrl-processor` spec — the six requirements (string round-trip, error observability, timestamp units, unsupported result shapes, and the rest) are the regression contract for this upgrade, verified by the existing processor test suite.
- No configuration surface changes: the user-facing `vrl` processor config (source, fallback semantics) is unchanged; user VRL programs keep compiling against the same stdlib surface modulo upstream removals.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `vrl-processor`: pin upgrade-compatibility requirements — user programs that compiled on vrl 0.30 SHALL still compile on 0.36 unless the function was removed upstream (in which case the compile diagnostic SHALL be surfaced to the operator unchanged), and all six existing behavioral requirements SHALL hold unchanged across the upgrade.
