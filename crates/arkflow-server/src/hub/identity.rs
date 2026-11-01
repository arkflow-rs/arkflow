//! Managed operator identity: user directory, credential tokens, and
//! authentication resolution.
//!
//! This is the storage-backed sibling of the static `operator_token` and the
//! OIDC federation: a per-user directory (`cp_users`), opaque per-user
//! credentials (`cp_credentials`, hash only), and browser sessions
//! (`cp_sessions`, wired through `oidc`). Static credentials keep priority
//! and unconfigured deployments behave exactly as before.

use super::error::HubError;
use super::{now_ms, Hub};
use crate::api_contract::{OperatorPrincipal, OperatorRole, ResourceScope};
use crate::storage::{AuditRecord, CredentialRecord, UserRecord};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Managed credential tokens are `ark_<credential_id>_<secret>`; the prefix
/// lets authentication route a bearer without touching the OIDC path.
pub(crate) const MANAGED_TOKEN_PREFIX: &str = "ark_";
pub(crate) const MAX_USERS: usize = 1024;
pub(crate) const MAX_CREDENTIALS_PER_USER: usize = 16;

pub(crate) fn generate_secret() -> Result<String, HubError> {
    use rand::TryRngCore;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| HubError::Invalid("OS random source is unavailable".into()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// `SHA-256(credential_id ‖ 0x00 ‖ secret)`: the credential id is public and
/// the secret is 256 bits of OS randomness, so a plain hash is sufficient
/// (there is no dictionary space to attack, unlike a password).
pub(crate) fn hash_secret(credential_id: &str, secret: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(credential_id.as_bytes());
    digest.update([0u8]);
    digest.update(secret.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn parse_managed_token(token: &str) -> Option<(&str, &str)> {
    let rest = token.strip_prefix(MANAGED_TOKEN_PREFIX)?;
    let (credential_id, secret) = rest.split_once('_')?;
    if credential_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((credential_id, secret))
}

/// User ids are stable and URL-safe: 1–64 chars of `[a-z0-9._-]`, starting
/// with a letter or digit.
pub(crate) fn validate_user_id(user_id: &str) -> Result<(), HubError> {
    if user_id.is_empty() || user_id.len() > 64 {
        return Err(HubError::Invalid("user id must be 1-64 characters".into()));
    }
    let mut bytes = user_id.bytes();
    let first = bytes.next().expect("non-empty checked above");
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(HubError::Invalid(
            "user id must start with a lowercase letter or digit".into(),
        ));
    }
    if !user_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        return Err(HubError::Invalid(
            "user id may only contain lowercase letters, digits, '.', '_' and '-'".into(),
        ));
    }
    Ok(())
}

pub(crate) fn parse_role(role: &str) -> Option<OperatorRole> {
    match role {
        "admin" => Some(OperatorRole::Admin),
        "operator" => Some(OperatorRole::Operator),
        "viewer" => Some(OperatorRole::Viewer),
        _ => None,
    }
}

/// `None` on a parse error: an empty scope list means "unrestricted" in
/// `OperatorPrincipal::can_scope`, so a corrupt `scopes_json` row must deny
/// rather than widen.
pub(crate) fn scopes_from_json(json: &str) -> Option<Vec<ResourceScope>> {
    serde_json::from_str(json).ok()
}

pub(crate) fn scopes_to_json(scopes: &[ResourceScope]) -> Result<String, HubError> {
    serde_json::to_string(scopes)
        .map_err(|error| HubError::Invalid(format!("invalid scopes: {error}")))
}

impl Hub {
    fn identity_storage(&self) -> Result<&crate::storage::StorageActor, HubError> {
        self.storage
            .as_ref()
            .ok_or(HubError::IdentityStoreUnavailable)
    }

    pub(crate) fn record_auth_outcome(&self, source: &str, outcome: &str) {
        self.command_metrics.record_auth_outcome(source, outcome);
    }

    /// Best-effort audit trail for identity mutations: scalar identifiers
    /// only, never token material or secret hashes.
    async fn record_identity_audit(
        &self,
        action: &str,
        actor: &str,
        user_id: Option<String>,
        credential_id: Option<String>,
        outcome: &str,
    ) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let message = match credential_id.as_deref() {
            Some(credential_id) => format!(
                "user={} credential={credential_id}",
                user_id.as_deref().unwrap_or("unknown")
            ),
            None => format!("user={}", user_id.as_deref().unwrap_or("unknown")),
        };
        let _ = storage
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some(actor.to_owned()),
                action: action.to_owned(),
                resource_type: "user".into(),
                resource_id: user_id,
                node_id: None,
                stream_id: None,
                correlation_id: None,
                outcome: outcome.to_owned(),
                failure_code: None,
                message: Some(message),
                occurred_at_ms: now_ms(),
            })
            .await;
    }

    /// Audit a rejected identity mutation (stable failure code, scalars only).
    pub(crate) async fn record_identity_rejection(
        &self,
        action: &str,
        actor: &str,
        user_id: Option<String>,
        credential_id: Option<String>,
        failure_code: &str,
    ) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let message = match credential_id.as_deref() {
            Some(credential_id) => format!(
                "user={} credential={credential_id} failure={failure_code}",
                user_id.as_deref().unwrap_or("unknown")
            ),
            None => format!(
                "user={} failure={failure_code}",
                user_id.as_deref().unwrap_or("unknown")
            ),
        };
        let _ = storage
            .record_audit(AuditRecord {
                event_id: 0,
                actor: Some(actor.to_owned()),
                action: action.to_owned(),
                resource_type: "user".into(),
                resource_id: user_id,
                node_id: None,
                stream_id: None,
                correlation_id: None,
                outcome: "rejected".into(),
                failure_code: Some(failure_code.to_owned()),
                message: Some(message),
                occurred_at_ms: now_ms(),
            })
            .await;
    }

    /// Parse and validate a user record supplied by an administration API.
    pub(crate) fn validated_user(mut user: UserRecord) -> Result<UserRecord, HubError> {
        validate_user_id(&user.user_id)?;
        if parse_role(&user.role).is_none() {
            return Err(HubError::Invalid(
                "role must be admin, operator, or viewer".into(),
            ));
        }
        if user.display_name.len() > 128 {
            return Err(HubError::Invalid(
                "display name must be at most 128 characters".into(),
            ));
        }
        if user.oidc_subject.as_deref() == Some("") {
            return Err(HubError::Invalid("oidc_subject must not be empty".into()));
        }
        let scopes: Vec<ResourceScope> = serde_json::from_str(&user.scopes_json)
            .map_err(|error| HubError::Invalid(format!("invalid scopes: {error}")))?;
        user.scopes_json = scopes_to_json(&scopes)?;
        user.updated_at_ms = now_ms();
        Ok(user)
    }

    pub async fn list_users(&self) -> Result<Vec<UserRecord>, HubError> {
        self.identity_storage()?
            .list_users()
            .await
            .map_err(HubError::from)
    }

    pub async fn user(&self, user_id: &str) -> Result<Option<UserRecord>, HubError> {
        self.identity_storage()?
            .get_user(user_id)
            .await
            .map_err(HubError::from)
    }

    /// Create a new user. Existing ids are rejected (administration updates
    /// go through `save_user`), as are duplicate OIDC subject bindings.
    pub async fn create_user(
        &self,
        mut user: UserRecord,
        actor: &str,
    ) -> Result<UserRecord, HubError> {
        let storage = self.identity_storage()?.clone();
        let users = storage.list_users().await.map_err(HubError::from)?;
        if users.len() >= MAX_USERS {
            return Err(HubError::UserLimit);
        }
        if users
            .iter()
            .any(|existing| existing.user_id == user.user_id)
        {
            return Err(HubError::Invalid(format!(
                "user '{}' already exists",
                user.user_id
            )));
        }
        user.created_at_ms = now_ms();
        user.tokens_valid_after_ms = 0;
        let user = Self::validated_user(user)?;
        let created = storage.upsert_user(user).await.map_err(map_user_write)?;
        self.record_identity_audit(
            "user.create",
            actor,
            Some(created.user_id.clone()),
            None,
            "accepted",
        )
        .await;
        Ok(created)
    }

    /// Replace an existing user record. The last enabled admin cannot be
    /// disabled, deleted, or demoted unless a static break-glass credential
    /// exists.
    pub async fn save_user(&self, user: UserRecord, actor: &str) -> Result<UserRecord, HubError> {
        let storage = self.identity_storage()?.clone();
        let current = storage
            .get_user(&user.user_id)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        let next = Self::validated_user(UserRecord {
            created_at_ms: current.created_at_ms,
            ..user
        })?;
        self.ensure_admin_reachability(&current, &next, &storage)
            .await?;
        let saved = storage.upsert_user(next).await.map_err(map_user_write)?;
        // Role and scopes are baked into stored session principals
        // (`resolve_session` returns the recorded `principal_json`), so an
        // authorization change must revoke the durable sessions that still
        // carry the old privileges. Unchanged authorization fields keep
        // sessions intact.
        if current.role != saved.role || current.scopes_json != saved.scopes_json {
            let removed = storage
                .delete_user_sessions(&saved.user_id)
                .await
                .map_err(HubError::from)?;
            if removed > 0 {
                tracing::info!(
                    user_id = %saved.user_id,
                    sessions = removed,
                    "revoked durable sessions after an authorization change"
                );
            }
        }
        self.record_identity_audit(
            "user.update",
            actor,
            Some(saved.user_id.clone()),
            None,
            "accepted",
        )
        .await;
        Ok(saved)
    }

    async fn ensure_admin_reachability(
        &self,
        current: &UserRecord,
        next: &UserRecord,
        storage: &crate::storage::StorageActor,
    ) -> Result<(), HubError> {
        let was_enabled_admin = current.role == "admin" && !current.disabled;
        let stays_enabled_admin = next.role == "admin" && !next.disabled;
        if !was_enabled_admin || stays_enabled_admin {
            return Ok(());
        }
        if self.config.operator_token.is_some() {
            return Ok(());
        }
        let users = storage.list_users().await.map_err(HubError::from)?;
        let others = users
            .iter()
            .filter(|user| user.user_id != current.user_id)
            .filter(|user| user.role == "admin" && !user.disabled)
            .count();
        if others == 0 {
            return Err(HubError::LastAdminRequired);
        }
        Ok(())
    }

    /// Delete a user, cascading its credentials and sessions. The last
    /// enabled admin is protected by the same reachability invariant.
    pub async fn delete_user(&self, user_id: &str, actor: &str) -> Result<bool, HubError> {
        let storage = self.identity_storage()?.clone();
        let current = storage
            .get_user(user_id)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        let disabled = UserRecord {
            disabled: true,
            ..current.clone()
        };
        self.ensure_admin_reachability(&current, &disabled, &storage)
            .await?;
        let deleted = storage.delete_user(user_id).await.map_err(HubError::from)?;
        if deleted {
            self.record_identity_audit(
                "user.delete",
                actor,
                Some(user_id.to_owned()),
                None,
                "accepted",
            )
            .await;
        }
        Ok(deleted)
    }

    /// Raise the user's federated-token invalidation threshold and drop all
    /// of the user's browser sessions.
    pub async fn revoke_user_tokens(&self, user_id: &str, actor: &str) -> Result<usize, HubError> {
        let storage = self.identity_storage()?.clone();
        let now = now_ms();
        let user = storage
            .bump_user_tokens_valid_after(user_id, now)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        let removed = storage
            .delete_user_sessions(user_id)
            .await
            .map_err(HubError::from)?;
        self.record_identity_audit(
            "user.revoke_tokens",
            actor,
            Some(user.user_id),
            None,
            "accepted",
        )
        .await;
        Ok(removed)
    }

    /// Issue a credential for a user. The plaintext token is returned exactly
    /// once; only its hash is stored.
    pub async fn issue_credential(
        &self,
        user_id: &str,
        label: &str,
        expires_at_ms: Option<u64>,
        actor: &str,
    ) -> Result<(CredentialRecord, String), HubError> {
        let storage = self.identity_storage()?.clone();
        storage
            .get_user(user_id)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        let existing = storage
            .list_credentials(user_id)
            .await
            .map_err(HubError::from)?;
        if existing
            .iter()
            .filter(|record| record.revoked_at_ms.is_none())
            .count()
            >= MAX_CREDENTIALS_PER_USER
        {
            return Err(HubError::CredentialLimit);
        }
        if label.len() > 128 {
            return Err(HubError::Invalid(
                "credential label must be at most 128 characters".into(),
            ));
        }
        let credential_id = generate_credential_id()?;
        let secret = generate_secret()?;
        let record = CredentialRecord {
            credential_id: credential_id.clone(),
            user_id: user_id.to_owned(),
            secret_hash: hash_secret(&credential_id, &secret),
            label: label.to_owned(),
            created_at_ms: now_ms(),
            expires_at_ms,
            last_used_at_ms: None,
            revoked_at_ms: None,
        };
        storage
            .create_credential(record.clone())
            .await
            .map_err(HubError::from)?;
        self.record_identity_audit(
            "credential.create",
            actor,
            Some(user_id.to_owned()),
            Some(credential_id.clone()),
            "accepted",
        )
        .await;
        let token = format!("{MANAGED_TOKEN_PREFIX}{credential_id}_{secret}");
        Ok((record, token))
    }

    pub async fn list_credentials(&self, user_id: &str) -> Result<Vec<CredentialRecord>, HubError> {
        self.identity_storage()?
            .list_credentials(user_id)
            .await
            .map_err(HubError::from)
    }

    /// Revoke a credential. Idempotent: a second revocation succeeds and
    /// keeps the first timestamp.
    pub async fn revoke_credential(
        &self,
        credential_id: &str,
        actor: &str,
    ) -> Result<bool, HubError> {
        let storage = self.identity_storage()?.clone();
        let record = storage
            .get_credential(credential_id)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        let revoked = storage
            .revoke_credential(credential_id, now_ms())
            .await
            .map_err(HubError::from)?;
        if revoked {
            self.record_identity_audit(
                "credential.revoke",
                actor,
                Some(record.user_id),
                Some(credential_id.to_owned()),
                "accepted",
            )
            .await;
        }
        Ok(revoked)
    }

    /// Rotate a credential: a fresh token is issued and the previous
    /// credential is revoked in one storage mutation.
    pub async fn rotate_credential(
        &self,
        credential_id: &str,
        actor: &str,
    ) -> Result<(CredentialRecord, String), HubError> {
        let storage = self.identity_storage()?.clone();
        let current = storage
            .get_credential(credential_id)
            .await
            .map_err(HubError::from)?
            .ok_or(HubError::NotFound)?;
        if current.revoked_at_ms.is_some() {
            return Err(HubError::Invalid(
                "cannot rotate an already revoked credential".into(),
            ));
        }
        let new_id = generate_credential_id()?;
        let secret = generate_secret()?;
        let record = CredentialRecord {
            credential_id: new_id.clone(),
            user_id: current.user_id.clone(),
            secret_hash: hash_secret(&new_id, &secret),
            label: current.label.clone(),
            created_at_ms: now_ms(),
            expires_at_ms: current.expires_at_ms,
            last_used_at_ms: None,
            revoked_at_ms: None,
        };
        storage
            .rotate_credential(record.clone(), credential_id, now_ms())
            .await
            .map_err(HubError::from)?;
        self.record_identity_audit(
            "credential.rotate",
            actor,
            Some(record.user_id.clone()),
            Some(new_id.clone()),
            "accepted",
        )
        .await;
        let token = format!("{MANAGED_TOKEN_PREFIX}{new_id}_{secret}");
        Ok((record, token))
    }

    /// Resolve a managed credential token to its user principal. Every
    /// failure mode (unknown/revoked/expired credential, disabled user,
    /// storage error, malformed token) is `Ok(None)` so callers return 401.
    pub async fn managed_credential_principal(
        &self,
        token: &str,
    ) -> Result<Option<OperatorPrincipal>, HubError> {
        let Some((credential_id, secret)) = parse_managed_token(token) else {
            return Ok(None);
        };
        let storage = self.identity_storage()?;
        let Some(record) = storage
            .get_credential(credential_id)
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        if record.revoked_at_ms.is_some() {
            return Ok(None);
        }
        if record
            .expires_at_ms
            .is_some_and(|expires_at| now_ms() >= expires_at)
        {
            return Ok(None);
        }
        let expected = hash_secret(&record.credential_id, secret);
        if !bool::from(expected.as_bytes().ct_eq(record.secret_hash.as_bytes())) {
            return Ok(None);
        }
        let Some(user) = storage
            .get_user(&record.user_id)
            .await
            .map_err(HubError::from)?
        else {
            return Ok(None);
        };
        if user.disabled {
            return Ok(None);
        }
        let Some(role) = parse_role(&user.role) else {
            return Ok(None);
        };
        let Some(scopes) = scopes_from_json(&user.scopes_json) else {
            return Ok(None);
        };
        Ok(Some(OperatorPrincipal {
            id: user.user_id,
            roles: vec![role],
            scopes,
        }))
    }

    /// Persist a browser session for a principal. The returned id is the
    /// random cookie value; only its hash is stored. `None` means the login
    /// must fail (no randomness, or no durable session store).
    pub async fn create_session(&self, principal: OperatorPrincipal) -> Option<String> {
        use rand::TryRngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes).ok()?;
        let session_id: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        let storage = self.storage.as_ref()?;
        let user_id = storage
            .get_user(&principal.id)
            .await
            .ok()
            .flatten()
            .map(|user| user.user_id);
        let now = now_ms();
        let record = crate::storage::SessionRecord {
            session_hash: session_hash(&session_id),
            user_id,
            principal_json: serde_json::to_string(&principal).ok()?,
            created_at_ms: now,
            expires_at_ms: now.saturating_add(crate::oidc::session_ttl_secs().saturating_mul(1000)),
            revoked_at_ms: None,
        };
        storage.upsert_session(record).await.ok()?;
        Some(session_id)
    }

    /// Resolve a browser session id: storage-backed on a durable Hub, the
    /// in-memory federation map otherwise. Revoked, expired, or disabled-user
    /// sessions are rejected.
    pub async fn resolve_session(&self, session_id: &str) -> Option<OperatorPrincipal> {
        if let Some(storage) = self.storage.as_ref() {
            let record = storage
                .get_session(&session_hash(session_id))
                .await
                .ok()??;
            if record.revoked_at_ms.is_some() || now_ms() >= record.expires_at_ms {
                return None;
            }
            if let Some(user_id) = record.user_id.as_deref() {
                match storage.get_user(user_id).await {
                    Ok(Some(user)) if user.disabled => return None,
                    Ok(None) => return None,
                    Err(_) => return None,
                    Ok(Some(_)) => {}
                }
            }
            return serde_json::from_str::<OperatorPrincipal>(&record.principal_json).ok();
        }
        self.oidc.as_ref()?.resolve_session(session_id)
    }

    pub async fn remove_session(&self, session_id: &str) {
        if let Some(storage) = self.storage.as_ref() {
            let _ = storage.delete_session(&session_hash(session_id)).await;
        }
        if let Some(federation) = self.oidc.as_ref() {
            federation.remove_session(session_id);
        }
    }

    /// Apply the managed user directory to a validated federated principal:
    /// bound subjects take the directory role/scopes and must not be
    /// disabled or carry an `iat` older than the revocation threshold.
    /// Unbound subjects keep claim-mapped behavior unless the deployment
    /// requires managed users only. Every error fails closed.
    pub(crate) async fn apply_federated_directory(
        &self,
        principal: OperatorPrincipal,
        issued_at_ms: Option<u64>,
        managed_users_only: bool,
    ) -> Option<OperatorPrincipal> {
        let Some(storage) = self.storage.as_ref() else {
            // A volatile Hub cannot check the directory; a deployment that
            // requires managed users must not admit unbound subjects.
            return (!managed_users_only).then_some(principal);
        };
        match storage.get_user_by_oidc_subject(&principal.id).await {
            Ok(Some(user)) => {
                if user.disabled {
                    return None;
                }
                let issued_at_ms = issued_at_ms?;
                const CLOCK_SKEW_MS: u64 = 60_000;
                if issued_at_ms.saturating_add(CLOCK_SKEW_MS) < user.tokens_valid_after_ms {
                    return None;
                }
                let role = parse_role(&user.role)?;
                let scopes = scopes_from_json(&user.scopes_json)?;
                Some(OperatorPrincipal {
                    id: user.user_id,
                    roles: vec![role],
                    scopes,
                })
            }
            Ok(None) if managed_users_only => None,
            Ok(None) => Some(principal),
            Err(error) => {
                tracing::warn!(%error, "identity lookup failed; denying federated request");
                None
            }
        }
    }

    /// Maintenance: drop expired or revoked browser sessions.
    pub async fn prune_identity_sessions(&self) -> Result<usize, HubError> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(0);
        };
        storage
            .prune_sessions(now_ms())
            .await
            .map_err(HubError::from)
    }
}

fn session_hash(session_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"session\0");
    digest.update(session_id.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn generate_credential_id() -> Result<String, HubError> {
    use rand::TryRngCore;
    let mut bytes = [0u8; 8];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| HubError::Invalid("OS random source is unavailable".into()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn map_user_write(error: crate::storage::StorageError) -> HubError {
    let text = error.to_string();
    if text.contains("UNIQUE") || text.contains("duplicate key") {
        HubError::OidcSubjectConflict
    } else {
        HubError::from(error)
    }
}
