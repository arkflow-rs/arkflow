# Delta: control-plane-deployment

## ADDED Requirements

### Requirement: The Hub server CLI surface SHALL be documented in the CLI reference

The CLI reference page SHALL document the `arkflow-server` binary: its startup environment variables (`ARKFLOW_HUB_ADDRESS`, `ARKFLOW_NODE_TOKEN`, `ARKFLOW_HUB_INSECURE_LOCAL`, `ARKFLOW_HUB_STORAGE`, `ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`, `ARKFLOW_OPERATOR_TOKEN`, and the `ARKFLOW_HUB_HA_*` lease family) and the `migrate` subcommand contract. The documentation SHALL exist in both English and the zh-Hans tree and stay consistent with the binary's actual parsing behavior.

#### Scenario: An operator deploys the Hub from a binary

- **WHEN** an operator reads the CLI reference to configure a Hub deployment
- **THEN** every environment variable the binary reads is listed with its default and effect, including the HA lease variables

#### Scenario: A storage schema migration is needed

- **WHEN** an operator needs to run `arkflow-server migrate`
- **THEN** the CLI reference documents the subcommand's arguments and exit-code contract

#### Scenario: The binary's env surface changes

- **WHEN** a pull request adds or renames a startup environment variable in `arkflow-server`
- **THEN** the CLI reference (en and zh-Hans) is updated in the same pull request, keeping docs consistent with the binary
