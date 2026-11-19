# control-plane-deployment — Delta

## ADDED Requirements

### Requirement: Hub startup configuration file

The `arkflow-server` binary SHALL accept `--config <FILE>` holding the full startup configuration: every `ServerConfig` field (including `logging` and `observability`), a `hub` section with `operator_token`, and an `ha` section mirroring `HubHaConfig`. The format SHALL be YAML, JSON, or TOML selected by file extension. Environment variables that are explicitly set SHALL override file values; unset variables SHALL fall back to file values, then built-in defaults, so an environment-only deployment behaves exactly as before. Secret references (`${env:VAR}`, `${file:PATH}`, `${secret:NAME}`) SHALL be resolved while loading the file; failures after secret resolution MUST NOT echo resolved values in error messages. Top-level keys that are not part of the startup configuration schema SHALL fail startup, naming the unknown keys. Usage errors — an unknown flag or stray positional argument on the serve path, a missing `--config` value, or an unreadable/unparseable/unknown-extension file — SHALL print a usage or error message to stderr and exit with code 2. The serve path SHALL reject any unrecognized first argument instead of silently starting the Hub.

#### Scenario: Environment overrides file
- **WHEN** the config file sets `address: 127.0.0.1:9000` and `ARKFLOW_HUB_ADDRESS=127.0.0.1:9001` is set
- **THEN** the Hub binds `127.0.0.1:9001`

#### Scenario: Unset environment falls back to file then default
- **WHEN** the config file sets `session_ttl_ms` and no related environment variable is set
- **THEN** the Hub uses the file value, and fields absent from both file and environment keep their built-in defaults

#### Scenario: Typo'd top-level key rejected
- **WHEN** the config file contains `adress: 127.0.0.1:9000`
- **THEN** startup fails naming `adress` as an unknown key instead of silently ignoring it

#### Scenario: Secret reference resolved without leaking
- **WHEN** the config file sets `hub.operator_token: ${env:HUB_TOKEN}` and `HUB_TOKEN` is set
- **THEN** the operator token is the resolved value, and a type error against another resolved field does not print that field's resolved value

#### Scenario: Unknown flag exits 2
- **WHEN** the serve path is invoked as `arkflow-server --confg x.yaml`
- **THEN** the process prints usage guidance to stderr and exits with code 2 instead of starting the Hub

#### Scenario: Unreadable config file exits 2
- **WHEN** `--config` names a file that does not exist or has an unknown extension
- **THEN** the process prints the error to stderr and exits with code 2

## MODIFIED Requirements

### Requirement: The Hub server CLI surface SHALL be documented in the CLI reference

The CLI reference page SHALL document the `arkflow-server` binary: its startup environment variables (`ARKFLOW_HUB_ADDRESS`, `ARKFLOW_NODE_TOKEN`, `ARKFLOW_HUB_INSECURE_LOCAL`, `ARKFLOW_HUB_STORAGE`, `ARKFLOW_HUB_TLS_CERT`/`ARKFLOW_HUB_TLS_KEY`, `ARKFLOW_OPERATOR_TOKEN`, the `ARKFLOW_HUB_HA_*` lease family, `ARKFLOW_LOG_LEVEL`, `ARKFLOW_OTEL_TRACING_ENABLED`, `ARKFLOW_OTEL_ENDPOINT`, `ARKFLOW_OTEL_SERVICE_NAME`, and `RUST_LOG`), the `--config <FILE>` startup configuration file contract, and the `migrate`/`backup`/`restore` subcommand contracts. The documentation SHALL state the precedence chain for log level (`RUST_LOG` > `ARKFLOW_LOG_LEVEL` > file `logging.level` > default), the file-vs-environment precedence rule, and the usage-error exit code 2 contract. The documentation SHALL exist in both English and the zh-Hans tree and stay consistent with the binary's actual parsing behavior.

#### Scenario: An operator deploys the Hub from a binary
- **WHEN** an operator reads the CLI reference to configure a Hub deployment
- **THEN** every environment variable the binary reads is listed with its default and effect, including the HA lease variables, the observability variables, and `RUST_LOG`

#### Scenario: An operator uses a startup configuration file
- **WHEN** an operator reads the CLI reference about `--config`
- **THEN** the page documents the accepted file formats, the covered sections (`ServerConfig` fields, `logging`, `observability`, `hub.operator_token`, `ha`), the precedence rule, secret references, the unknown-key rejection, and the exit code for usage errors

#### Scenario: A storage schema migration is needed
- **WHEN** an operator needs to run `arkflow-server migrate`
- **THEN** the CLI reference documents the subcommand's arguments and exit-code contract

#### Scenario: The binary's env surface changes
- **WHEN** a pull request adds or renames a startup environment variable or flag in `arkflow-server`
- **THEN** the CLI reference (en and zh-Hans) is updated in the same pull request, keeping docs consistent with the binary
