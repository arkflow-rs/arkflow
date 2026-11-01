//! Managed user directory and credential administration (Hub only).
//!
//! Every route requires the `ManageUsers` action (admin role only). Secret
//! material appears exactly once, in the issuance/rotation response; all
//! other responses carry metadata only. Accepted and rejected mutations are
//! audited with the real actor.

use super::{problem, require_operator_action};
use crate::api_contract::{
    CreateCredentialRequest, CreateUserRequest, CredentialView, IssuedCredentialResponse,
    OperatorAction, RevokedTokensResponse, UpdateUserRequest, UserResponse,
};
use crate::hub;
use crate::storage::{CredentialRecord, UserRecord};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

fn user_response(record: UserRecord) -> UserResponse {
    UserResponse {
        scopes: serde_json::from_str(&record.scopes_json).unwrap_or_default(),
        user_id: record.user_id,
        display_name: record.display_name,
        role: record.role,
        disabled: record.disabled,
        oidc_subject: record.oidc_subject,
        tokens_valid_after_ms: record.tokens_valid_after_ms,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    }
}

fn credential_view(record: &CredentialRecord) -> CredentialView {
    CredentialView {
        credential_id: record.credential_id.clone(),
        label: record.label.clone(),
        created_at_ms: record.created_at_ms,
        expires_at_ms: record.expires_at_ms,
        last_used_at_ms: record.last_used_at_ms,
        revoked_at_ms: record.revoked_at_ms,
    }
}

#[derive(Clone, Copy)]
enum IdentityEntity {
    User,
    Credential,
}

fn identity_error_parts(
    error: &hub::HubError,
    entity: IdentityEntity,
) -> (StatusCode, &'static str) {
    let (not_found, invalid) = match entity {
        IdentityEntity::User => ("user_not_found", "invalid_user"),
        IdentityEntity::Credential => ("credential_not_found", "invalid_credential"),
    };
    match error {
        hub::HubError::NotFound => (StatusCode::NOT_FOUND, not_found),
        hub::HubError::Invalid(_) => (StatusCode::BAD_REQUEST, invalid),
        hub::HubError::IdentityStoreUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "identity_store_unavailable",
        ),
        hub::HubError::LastAdminRequired => (StatusCode::CONFLICT, "last_admin_required"),
        hub::HubError::UserLimit => (StatusCode::CONFLICT, "user_limit"),
        hub::HubError::CredentialLimit => (StatusCode::CONFLICT, "credential_limit"),
        hub::HubError::OidcSubjectConflict => (StatusCode::CONFLICT, "oidc_subject_conflict"),
        hub::HubError::StorageUnavailable | hub::HubError::Storage(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
        }
        other => (StatusCode::CONFLICT, {
            let _ = other;
            "agent_request_rejected"
        }),
    }
}

/// Map an identity error to a problem response without auditing (reads).
fn identity_problem(error: hub::HubError, entity: IdentityEntity) -> Response {
    let (status, code) = identity_error_parts(&error, entity);
    problem(status, code, error.to_string().chars().take(256).collect())
}

/// Map an identity **mutation** error, auditing the rejection with the real
/// actor before answering.
#[allow(clippy::result_large_err)]
async fn audited<T>(
    hub: &hub::Hub,
    action: &str,
    actor: &str,
    user_id: Option<&str>,
    credential_id: Option<&str>,
    entity: IdentityEntity,
    result: Result<T, hub::HubError>,
) -> Result<T, Response> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let (status, code) = identity_error_parts(&error, entity);
            hub.record_identity_rejection(
                action,
                actor,
                user_id.map(str::to_owned),
                credential_id.map(str::to_owned),
                code,
            )
            .await;
            Err(problem(
                status,
                code,
                error.to_string().chars().take(256).collect(),
            ))
        }
    }
}

/// Build the rejection response for a mutation that failed before reaching
/// the Hub (validation-stage failures still produce an audited record).
async fn rejection(
    hub: &hub::Hub,
    action: &str,
    actor: &str,
    user_id: Option<&str>,
    credential_id: Option<&str>,
    entity: IdentityEntity,
    error: hub::HubError,
) -> Response {
    audited::<()>(
        hub,
        action,
        actor,
        user_id,
        credential_id,
        entity,
        Err(error),
    )
    .await
    .expect_err("rejection")
}

#[allow(clippy::result_large_err)]
async fn require_manage_users(
    hub: &hub::Hub,
    headers: &HeaderMap,
    resource_id: Option<String>,
) -> Result<crate::api_contract::OperatorPrincipal, Response> {
    require_operator_action(
        hub,
        headers,
        OperatorAction::ManageUsers,
        "user",
        resource_id,
    )
    .await
}

pub(super) async fn hub_users(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    if let Err(response) = require_manage_users(&hub, &headers, None).await {
        return response;
    }
    match hub.list_users().await {
        Ok(users) => Json(users.into_iter().map(user_response).collect::<Vec<_>>()).into_response(),
        Err(error) => identity_problem(error, IdentityEntity::User),
    }
}

pub(super) async fn hub_user(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        return response;
    }
    match hub.user(&user_id).await {
        Ok(Some(user)) => Json(user_response(user)).into_response(),
        Ok(None) => identity_problem(hub::HubError::NotFound, IdentityEntity::User),
        Err(error) => identity_problem(error, IdentityEntity::User),
    }
}

pub(super) async fn hub_create_user(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<CreateUserRequest>,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, None).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let user_id = request.user_id.clone();
    let scopes_json = match serde_json::to_string(&request.scopes) {
        Ok(json) => json,
        Err(error) => {
            return rejection(
                &hub,
                "user.create",
                &principal.id,
                Some(&user_id),
                None,
                IdentityEntity::User,
                hub::HubError::Invalid(format!("invalid scopes: {error}")),
            )
            .await;
        }
    };
    let now = hub::now_ms_for_metrics();
    let record = UserRecord {
        user_id: request.user_id,
        display_name: request.display_name.unwrap_or_default(),
        role: request.role,
        scopes_json,
        disabled: false,
        oidc_subject: request.oidc_subject,
        tokens_valid_after_ms: 0,
        created_at_ms: now,
        updated_at_ms: now,
    };
    match audited(
        &hub,
        "user.create",
        &principal.id,
        Some(&user_id),
        None,
        IdentityEntity::User,
        hub.create_user(record, &principal.id).await,
    )
    .await
    {
        Ok(user) => (StatusCode::CREATED, Json(user_response(user))).into_response(),
        Err(response) => response,
    }
}

pub(super) async fn hub_update_user(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateUserRequest>,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if request.display_name.is_none()
        && request.role.is_none()
        && request.scopes.is_none()
        && request.disabled.is_none()
        && request.oidc_subject.is_none()
    {
        return rejection(
            &hub,
            "user.update",
            &principal.id,
            Some(&user_id),
            None,
            IdentityEntity::User,
            hub::HubError::Invalid("at least one field must be provided".into()),
        )
        .await;
    }
    let current = match hub.user(&user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return rejection(
                &hub,
                "user.update",
                &principal.id,
                Some(&user_id),
                None,
                IdentityEntity::User,
                hub::HubError::NotFound,
            )
            .await
        }
        Err(error) => {
            return rejection(
                &hub,
                "user.update",
                &principal.id,
                Some(&user_id),
                None,
                IdentityEntity::User,
                error,
            )
            .await
        }
    };
    let mut record = current;
    if let Some(display_name) = request.display_name {
        record.display_name = display_name;
    }
    if let Some(role) = request.role {
        record.role = role;
    }
    if let Some(disabled) = request.disabled {
        record.disabled = disabled;
    }
    if let Some(scopes) = request.scopes {
        match serde_json::to_string(&scopes) {
            Ok(json) => record.scopes_json = json,
            Err(error) => {
                return rejection(
                    &hub,
                    "user.update",
                    &principal.id,
                    Some(&user_id),
                    None,
                    IdentityEntity::User,
                    hub::HubError::Invalid(format!("invalid scopes: {error}")),
                )
                .await
            }
        }
    }
    if let Some(oidc_subject) = request.oidc_subject {
        record.oidc_subject = (!oidc_subject.is_empty()).then_some(oidc_subject);
    }
    match audited(
        &hub,
        "user.update",
        &principal.id,
        Some(&user_id),
        None,
        IdentityEntity::User,
        hub.save_user(record, &principal.id).await,
    )
    .await
    {
        Ok(user) => Json(user_response(user)).into_response(),
        Err(response) => response,
    }
}

pub(super) async fn hub_delete_user(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    match audited(
        &hub,
        "user.delete",
        &principal.id,
        Some(&user_id),
        None,
        IdentityEntity::User,
        hub.delete_user(&user_id, &principal.id).await,
    )
    .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => identity_problem(hub::HubError::NotFound, IdentityEntity::User),
        Err(response) => response,
    }
}

pub(super) async fn hub_user_credentials(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        return response;
    }
    match hub.user(&user_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return identity_problem(hub::HubError::NotFound, IdentityEntity::User),
        Err(error) => return identity_problem(error, IdentityEntity::User),
    }
    match hub.list_credentials(&user_id).await {
        Ok(records) => {
            Json(records.iter().map(credential_view).collect::<Vec<_>>()).into_response()
        }
        Err(error) => identity_problem(error, IdentityEntity::User),
    }
}

pub(super) async fn hub_create_credential(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CreateCredentialRequest>,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let label = request.label.unwrap_or_default();
    match audited(
        &hub,
        "credential.create",
        &principal.id,
        Some(&user_id),
        None,
        IdentityEntity::User,
        hub.issue_credential(&user_id, &label, request.expires_at_ms, &principal.id)
            .await,
    )
    .await
    {
        Ok((record, token)) => (
            StatusCode::CREATED,
            Json(IssuedCredentialResponse {
                credential: credential_view(&record),
                token,
            }),
        )
            .into_response(),
        Err(response) => response,
    }
}

pub(super) async fn hub_rotate_credential(
    State(hub): State<hub::Hub>,
    Path(credential_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, None).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    match audited(
        &hub,
        "credential.rotate",
        &principal.id,
        None,
        Some(&credential_id),
        IdentityEntity::Credential,
        hub.rotate_credential(&credential_id, &principal.id).await,
    )
    .await
    {
        Ok((record, token)) => Json(IssuedCredentialResponse {
            credential: credential_view(&record),
            token,
        })
        .into_response(),
        Err(response) => response,
    }
}

pub(super) async fn hub_revoke_credential(
    State(hub): State<hub::Hub>,
    Path(credential_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, None).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    match audited(
        &hub,
        "credential.revoke",
        &principal.id,
        None,
        Some(&credential_id),
        IdentityEntity::Credential,
        hub.revoke_credential(&credential_id, &principal.id).await,
    )
    .await
    {
        Ok(revoked) => Json(serde_json::json!({ "revoked": revoked })).into_response(),
        Err(response) => response,
    }
}

pub(super) async fn hub_revoke_user_tokens(
    State(hub): State<hub::Hub>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_manage_users(&hub, &headers, Some(user_id.clone())).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    match audited(
        &hub,
        "user.revoke_tokens",
        &principal.id,
        Some(&user_id),
        None,
        IdentityEntity::User,
        hub.revoke_user_tokens(&user_id, &principal.id).await,
    )
    .await
    {
        Ok(revoked_sessions) => Json(RevokedTokensResponse { revoked_sessions }).into_response(),
        Err(response) => response,
    }
}
