# Delta Spec: control-plane-console

## ADDED Requirements

### Requirement: Users and credentials SHALL be administrable from the console

The console SHALL provide a user and credential administration area visible and usable only for principals authorized with `ManageUsers` in Hub mode. It SHALL list users with their role, scope summary, and disabled state; create and edit users; disable, enable, and delete users; list credentials by metadata only; issue a credential with a one-time plaintext display and copy affordance; rotate and revoke credentials; and revoke all of a user's tokens. Destructive actions SHALL use the existing in-app confirmation pattern, rejected mutations SHALL surface the returned problem code, and the strings SHALL be translated in the English and Simplified Chinese dictionaries.

#### Scenario: Administrator manages users

- **WHEN** an admin opens the console Settings area in Hub mode
- **THEN** the user and credential administration area is offered and lists users without any credential material

#### Scenario: Issued credential is shown once

- **WHEN** an admin issues a credential for a user
- **THEN** the console displays the plaintext token once with a copy action and does not display it again on later list views

#### Scenario: Non-administrators see no administration area

- **WHEN** a principal without `ManageUsers` opens the console
- **THEN** the administration area is not offered, matching the existing action-gated navigation pattern

#### Scenario: Rejected administration surfaces the problem code

- **WHEN** an administration mutation is rejected (for example `last_admin_required`)
- **THEN** the console displays the returned problem code instead of silently ignoring the failure

### Requirement: The console SHALL support runtime operator credential entry

The console SHALL allow an operator to enter or replace the bearer credential it uses at runtime, without requiring a build-time token, and SHALL keep that credential only for the browser session. When a request fails with 401 and no OIDC login flow is available, the console SHALL prompt for a credential and retry the failed navigation after a successful entry; it SHALL also offer an explicit sign-out that clears the stored credential. The build-time token SHALL remain an optional default for existing deployments.

#### Scenario: Runtime credential entry recovers from 401

- **WHEN** the console has no credential and a Hub returns 401 while no OIDC login flow is configured
- **THEN** the console prompts for a bearer credential, stores it for the browser session, and retries the request successfully

#### Scenario: Sign-out clears the runtime credential

- **WHEN** an operator signs out of the console
- **THEN** the stored credential is cleared and subsequent requests prompt again

#### Scenario: Build-time token keeps working

- **WHEN** the console is built with a default token and the operator does not enter a runtime credential
- **THEN** existing authentication behavior is unchanged
