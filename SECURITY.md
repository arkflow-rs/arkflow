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

## Scope

In scope: the ArkFlow engine and its plugins (`crates/`), the Hub/Agent
control plane and web console, published container images, Helm charts, and
the documentation pipeline as it affects users.

Out of scope: vulnerabilities in third-party dependencies themselves (report
those upstream — we track and patch them through dependency refreshes),
attacks requiring physical access or a fully trusted operator account on your
own infrastructure, and denial of service through unbounded legitimate load on
publicly exposed endpoints you chose to expose without authentication.
