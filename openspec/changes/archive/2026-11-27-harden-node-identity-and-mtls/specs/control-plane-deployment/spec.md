## MODIFIED Requirements

### Requirement: The Hub server CLI surface SHALL be documented in the CLI reference

The CLI reference page SHALL document the `arkflow-server` binary: its startup environment variables (`ARKFLOW_HUB_ADDRESS`, `ARKFLOW_NODE_TOKEN`, `ARKFLOW_NODE_BOOTSTRAP_DISABLED`, `ARKFLOW_ALLOW_SHARED_NODE_TOKEN_BREAK_GLASS`, `ARKFLOW_HUB_INSECURE_LOCAL`, `ARKFLOW_HUB_STORAGE`, `ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`/`ARKFLOW_HUB_TLS_CA`/`ARKFLOW_HUB_TLS_CLIENT_AUTH`, `ARKFLOW_OPERATOR_TOKEN`, the `ARKFLOW_HUB_HA_*` lease family, `ARKFLOW_LOG_LEVEL`, `ARKFLOW_OTEL_TRACING_ENABLED`, `ARKFLOW_OTEL_ENDPOINT`, `ARKFLOW_OTEL_SERVICE_NAME`, and `RUST_LOG`), Agent TLS/credential variables (`ARKFLOW_AGENT_TLS_CA`, `ARKFLOW_AGENT_TLS_CERT`, `ARKFLOW_AGENT_TLS_KEY`, `ARKFLOW_NODE_CREDENTIAL_FILE`), the `--config <FILE>` startup configuration file contract, and the `migrate`/`backup`/`restore` subcommand contracts. The documentation SHALL state the precedence chain for log level (`RUST_LOG` > `ARKFLOW_LOG_LEVEL` > file `logging.level` > default), the file-vs-environment precedence rule, and the usage-error exit code 2 contract. The deployment, Helm, and API docs SHALL document the per-node credential lifecycle, Helm `controlPlane.tls.enabled=false` opt-out switch, and node/data-plane threat model in both English and the zh-Hans tree.

#### Scenario: An operator deploys the Hub from a binary

- **WHEN** an operator reads the CLI reference to configure a Hub deployment
- **THEN** every environment variable the binary reads is listed with its default and effect, including the HA lease variables, Hub/Agent mTLS variables, node bootstrap/break-glass switches, the observability variables, and `RUST_LOG`

#### Scenario: An operator uses a startup configuration file

- **WHEN** an operator reads the CLI reference about `--config`
- **THEN** the page documents the accepted file formats, the covered sections (`ServerConfig` fields, `logging`, `observability`, `hub.operator_token`, `ha`), the precedence rule, secret references, the unknown-key rejection, and the exit code for usage errors

#### Scenario: A storage schema migration is needed

- **WHEN** an operator needs to run `arkflow-server migrate`
- **THEN** the CLI reference documents the subcommand's arguments and exit-code contract

#### Scenario: An operator reviews Helm mTLS defaults and threat model

- **WHEN** an operator reads the Helm and control-plane deployment pages (en and zh-Hans)
- **THEN** the pages document that `mode: control-plane` enables mTLS by default, how to disable it with `controlPlane.tls.enabled=false`, and the security boundaries of per-node credentials, shared bootstrap tokens, and per-edge shuffle secrets

#### Scenario: The binary's env surface changes

- **WHEN** a pull request adds or renames a startup environment variable or flag in `arkflow-server`
- **THEN** the CLI reference (en and zh-Hans) is updated in the same pull request, keeping docs consistent with the binary
