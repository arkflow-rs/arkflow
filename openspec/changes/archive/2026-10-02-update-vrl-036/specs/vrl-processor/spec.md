## ADDED Requirements

### Requirement: Dependency upgrade preserves operator-facing behavior

The VRL processor's upgrade from vrl 0.30 to 0.36 SHALL NOT change its configuration surface, its error taxonomy, or any behavior codified by the existing `vrl-processor` requirements. User VRL source that compiles on vrl 0.30 SHALL compile identically on 0.36, except for expressions using functions removed upstream — those SHALL fail at processor build time with the VRL compiler diagnostic surfaced to the operator as a configuration error (the existing behavior for invalid source).

#### Scenario: Valid program keeps compiling after the upgrade
- **WHEN** a processor config with VRL source that compiled on vrl 0.30 (e.g. `.message = upcase(.message)`) is built against vrl 0.36
- **THEN** the program compiles and the processor is constructed successfully

#### Scenario: Removed-upstream function surfaces the compiler diagnostic
- **WHEN** a processor config uses a VRL function that no longer exists in the 0.36 stdlib
- **THEN** processor construction fails with the VRL compile diagnostic naming the function, following the existing invalid-source error path

#### Scenario: Existing behavioral requirements hold unchanged
- **WHEN** the full `vrl-processor` test suite runs against vrl 0.36
- **THEN** every requirement of the `vrl-processor` specification — string round-trip typing, runtime-error observability, all timestamp units, unsupported result shapes failing loudly — passes without modification to the asserted behaviors
