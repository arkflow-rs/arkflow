# Security Policy

## Supported versions

ArkFlow is in pre-1.0 active development. Security fixes are applied to the
latest `main` branch only; there are no backport branches yet.

| Version | Supported |
| ------- | --------- |
| latest `main` / newest tag | ✅ |
| older tags | ❌ |

When v1.0 ships, this table will list the supported minor release lines and
their fix windows.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Use one of these private channels:

1. **GitHub private vulnerability reporting** (preferred): use the
   ["Report a vulnerability"](https://github.com/arkflow-rs/arkflow/security/advisories/new)
   button on the repository's Security Advisories page.
2. **Email fallback**: contact the maintainer at
   [chenquan.dev@gmail.com](mailto:chenquan.dev@gmail.com) with `[arkflow
   security]` in the subject line.

Please include as much of the following as you can:

- The affected component (engine, Hub/Agent control plane, console, docs
  pipeline) and the commit or tag you tested.
- A proof of concept or step-by-step reproduction.
- Any relevant logs, configs (with secrets removed), and your assessment of
  impact.

## What to expect

- **Acknowledgement** within 7 days of a private report.
- An assessment and severity triage within 14 days.
- A fix on `main` coordinated with a release and a GitHub Security Advisory
  (credit given unless you prefer to remain anonymous). We aim for timely
  disclosure once a fix is available; reporters may request an embargo
  extension.

## Control-plane identity material

The Hub's managed identity layer is designed so a stolen database or backup
does not yield usable bearer tokens:

- **Managed credentials** are opaque `ark_<credential_id>_<secret>` tokens.
  The plaintext is returned exactly once at issuance or rotation; the store
  keeps only `SHA-256(credential_id ‖ 0x00 ‖ secret)` and compares in constant
  time. Secret material never appears in audit records, events, metrics
  labels, or logs.
- **Browser sessions** carry a 256-bit random session id; the store keeps its
  hash. Sessions are invalidated immediately by logout, by disabling or
  deleting the user, or by `POST /api/v1/users/{id}/revoke-tokens`, and they
  are deliberately excluded from backup/restore snapshots so disaster
  recovery never resurrects a login.
- **OIDC tokens** are validated for signature (asymmetric only), expiry,
  issuer, and audience. A subject bound to a managed user is additionally
  rejected when the user is disabled or when the token's `iat` predates the
  user's revocation threshold (60-second clock-skew tolerance); a bound token
  without `iat` is rejected.
- **The static operator credential** (`ARKFLOW_OPERATOR_TOKEN`) is compared in
  constant time. A malformed structured value refuses to match instead of
  degrading to admin, and the credential is the intentional break-glass path
  when every directory admin is unreachable.
- **Authentication is fail-closed**: an identity storage error during
  credential resolution denies the request rather than skipping the check.
  Managed credentials never fall back to OIDC validation.

Operationally, terminate TLS in front of the Hub, keep static break-glass
tokens in a secret manager, and prefer issuing per-client credentials over
sharing one token. Treat control-plane snapshots like the live database: they
contain credential hashes, user metadata, and audit history. Report exposure
of any of this material through the private channels above — it is in scope.

## Scope

In scope: the ArkFlow engine and its plugins (`crates/`), the Hub/Agent
control plane and web console, published container images, Helm charts, and
the documentation pipeline as it affects users.

Out of scope: vulnerabilities in third-party dependencies themselves (report
those upstream — we track and patch them through dependency refreshes),
attacks requiring physical access or a fully trusted operator account on your
own infrastructure, and denial of service through unbounded legitimate load on
publicly exposed endpoints you chose to expose without authentication.
