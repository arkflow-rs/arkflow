//! Webhook alerting subscription management (see the `webhook-alerting`
//! capability): full-replace set management and synchronous test delivery.

use super::{hub_problem, problem, require_operator_action};
use crate::api_contract::{OperatorAction, WebhookSubscriptionInput, WebhookSubscriptionResponse};
use crate::hub;
use crate::storage::WebhookSubscription;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

#[derive(Debug, serde::Deserialize)]
pub(super) struct ReplaceWebhooksRequest {
    #[serde(default)]
    pub subscriptions: Vec<WebhookSubscriptionInput>,
}

fn response_from(subscription: &WebhookSubscription) -> WebhookSubscriptionResponse {
    WebhookSubscriptionResponse {
        id: subscription.id.clone(),
        url: subscription.url.clone(),
        event_types: subscription.event_types.clone(),
        enabled: subscription.enabled,
        created_by: subscription.created_by.clone(),
        created_at_ms: subscription.created_at_ms,
        has_secret: subscription.secret.is_some(),
    }
}

pub(super) async fn hub_webhooks(State(hub): State<hub::Hub>, headers: HeaderMap) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageWebhooks,
        "webhook",
        None,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let _ = principal;
    match hub.list_webhooks().await {
        Ok(subscriptions) => Json(serde_json::json!({
            "subscriptions": subscriptions.iter().map(response_from).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_replace_webhooks(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<ReplaceWebhooksRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageWebhooks,
        "webhook",
        None,
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let correlation_id = headers
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let stored: Vec<WebhookSubscription> = request
        .subscriptions
        .iter()
        .map(|input| WebhookSubscription {
            id: input.id.clone(),
            url: input.url.clone(),
            event_types: input.event_types.clone(),
            enabled: input.enabled.unwrap_or(true),
            secret: input.secret.clone(),
            created_by: Some(principal.id.clone()),
            created_at_ms: hub::now_ms_for_metrics(),
        })
        .collect();
    match hub
        .replace_webhooks(stored, Some(principal.id), correlation_id)
        .await
    {
        Ok(subscriptions) => Json(serde_json::json!({
            "subscriptions": subscriptions.iter().map(response_from).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::BAD_REQUEST, "invalid_webhooks", message)
        }
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_test_webhook(
    State(hub): State<hub::Hub>,
    Path(webhook_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::ManageWebhooks,
        "webhook",
        Some(webhook_id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let _ = principal;
    match hub.test_webhook(&webhook_id).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(hub::HubError::NotFound) => problem(
            StatusCode::NOT_FOUND,
            "webhook_not_found",
            format!("no webhook subscription named {webhook_id}"),
        ),
        Err(hub::HubError::Invalid(message)) => {
            problem(StatusCode::BAD_REQUEST, "invalid_webhooks", message)
        }
        Err(error) => hub_problem(error),
    }
}
