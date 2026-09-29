//! Operator credential and OIDC authorization checks.

use super::*;

impl Hub {
    pub async fn operator_authorized(&self, supplied: Option<&str>) -> bool {
        self.operator_principal(supplied).await.is_some()
    }

    pub async fn operator_principal(&self, supplied: Option<&str>) -> Option<OperatorPrincipal> {
        if let Some(expected) = self.config.operator_token.as_deref() {
            if !expected.trim().is_empty() {
                {
                    let supplied = supplied?;
                    let (id, role, secret, scopes) = parse_operator_credential(expected);
                    if bool::from(supplied.as_bytes().ct_eq(secret.as_bytes())) {
                        return Some(OperatorPrincipal {
                            id: id.to_owned(),
                            roles: vec![role],
                            scopes,
                        });
                    }
                    // A bearer that is not the static credential falls
                    // through to OIDC below (JWTs never ct_eq-match).
                }
            }
        } else if self.config.insecure_local {
            return Some(OperatorPrincipal::legacy_operator());
        }

        // Static credentials did not match (or are absent); fall back to
        // browser session cookies and OIDC JWT validation when federation
        // is configured.
        let federation = self.oidc.as_ref()?;
        let token = supplied?;
        if let Some(session_id) = token.strip_prefix("session:") {
            return federation.resolve_session(session_id);
        }
        federation.authenticate(token).await
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
