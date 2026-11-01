//! Operator credential and OIDC authorization checks.

use super::nodes::parse_operator_credential;
use super::Hub;
use crate::api_contract::{OperatorAction, OperatorPrincipal};
use subtle::ConstantTimeEq;

impl Hub {
    pub async fn operator_authorized(&self, supplied: Option<&str>) -> bool {
        self.operator_principal(supplied).await.is_some()
    }

    /// Whether operator authentication actually rejects requests without a
    /// valid credential. Mirrors `operator_principal`: the credential-free
    /// legacy-operator branch runs only when no `operator_token` is
    /// configured at all and `insecure_local` is opted in — a blank token
    /// matches the outer `if let` and skips that branch, so authentication
    /// still requires the OIDC path there.
    pub fn operator_credential_enforced(&self) -> bool {
        self.config.operator_token.is_some() || !self.config.insecure_local
    }

    pub async fn operator_principal(&self, supplied: Option<&str>) -> Option<OperatorPrincipal> {
        if let Some(expected) = self.config.operator_token.as_deref() {
            if !expected.trim().is_empty() {
                if let Some(supplied) = supplied {
                    // A malformed structured credential (contains '|' but
                    // fails to parse) is unmatchable by design; it must
                    // never silently authenticate as Admin. Fall through to
                    // the managed/OIDC paths below.
                    if let Some((id, role, secret, scopes)) = parse_operator_credential(expected) {
                        if bool::from(supplied.as_bytes().ct_eq(secret.as_bytes())) {
                            self.record_auth_outcome("static", "ok");
                            return Some(OperatorPrincipal {
                                id: id.to_owned(),
                                roles: vec![role],
                                scopes,
                            });
                        }
                    }
                }
            }
        } else if self.config.insecure_local {
            self.record_auth_outcome("anonymous", "ok");
            return Some(OperatorPrincipal::legacy_operator());
        }

        // Managed per-user credentials are the directory-backed middle
        // layer: a token with the managed prefix resolves against
        // `cp_credentials` and never falls through to OIDC (an invalid
        // managed token must stay a 401 instead of being probed as a JWT).
        if let Some(token) = supplied {
            if token.starts_with(super::identity::MANAGED_TOKEN_PREFIX) {
                let principal = match self.managed_credential_principal(token).await {
                    Ok(principal) => principal,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "managed credential resolution failed; denying request"
                        );
                        None
                    }
                };
                self.record_auth_outcome(
                    "managed",
                    if principal.is_some() { "ok" } else { "denied" },
                );
                return principal;
            }
        }

        // Browser sessions resolve against the durable session table when a
        // store exists (so revocation and restarts work), else against the
        // federation's in-memory map.
        if let Some(token) = supplied {
            if let Some(session_id) = token.strip_prefix("session:") {
                let principal = self.resolve_session(session_id).await;
                self.record_auth_outcome(
                    "session",
                    if principal.is_some() { "ok" } else { "denied" },
                );
                return principal;
            }
        }

        // Static credentials did not match (or are absent); fall back to
        // OIDC JWT validation when federation is configured.
        let Some(federation) = self.oidc.as_ref() else {
            self.record_auth_outcome("anonymous", "denied");
            return None;
        };
        let Some(token) = supplied else {
            self.record_auth_outcome("anonymous", "denied");
            return None;
        };
        let Some((principal, issued_at_ms)) = federation.authenticate_with_iat(token).await else {
            self.record_auth_outcome("oidc", "denied");
            return None;
        };
        let issued_at_ms = issued_at_ms
            .and_then(|seconds| u64::try_from(seconds).ok())
            .map(|seconds| seconds.saturating_mul(1000));
        let resolved = self
            .apply_federated_directory(principal, issued_at_ms, federation.managed_users_only())
            .await;
        self.record_auth_outcome("oidc", if resolved.is_some() { "ok" } else { "denied" });
        resolved
    }

    pub async fn operator_can(&self, supplied: Option<&str>, action: OperatorAction) -> bool {
        self.operator_principal(supplied)
            .await
            .is_some_and(|principal| principal.can(action))
    }

    pub async fn operator_can_scope(
        &self,
        supplied: Option<&str>,
        action: OperatorAction,
        resource_type: &str,
        resource_id: Option<&str>,
    ) -> bool {
        self.operator_principal(supplied)
            .await
            .is_some_and(|principal| principal.can_scope(action, resource_type, resource_id))
    }
}
