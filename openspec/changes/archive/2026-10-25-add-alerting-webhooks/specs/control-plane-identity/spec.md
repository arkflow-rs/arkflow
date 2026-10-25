## MODIFIED Requirements

### Requirement: Resource-scoped RBAC

The service SHALL authorize actions by principal, role, action, and resource scope, including fleet, node, Stream, configuration, rollout, webhook subscription, and audit read scopes. The action set SHALL include `ManageWebhooks` (webhook subscription replacement and test delivery), held by the admin and operator roles and denied to the viewer role, at fleet scope.

#### Scenario: Deny an out-of-scope action

- **WHEN** an authenticated principal lacks permission for the target node or requested action
- **THEN** the service returns 403 and leaves desired state, operations, and audit state unchanged

#### Scenario: Viewer cannot manage webhooks

- **WHEN** a viewer-role principal calls a webhook subscription replacement or test-delivery route
- **THEN** the service returns 403, the subscription set is unchanged, and the denial is audited

#### Scenario: Operator manages webhooks

- **WHEN** an operator-role principal calls a webhook subscription replacement route
- **THEN** the action is authorized and processed under the `ManageWebhooks` action
