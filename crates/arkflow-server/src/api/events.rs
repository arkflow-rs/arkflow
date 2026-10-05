//! Event list/stream (SSE) and audit handlers for the local plane and the Hub.
use super::{hub_problem, page_items, AuditQuery, EventQuery, PageQuery};
use crate::hub;
use arkflow_core::control::Page;
use arkflow_core::control_plane::ControlPlane;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::collections::VecDeque;
use std::convert::Infallible;

pub(super) async fn hub_events(
    State(hub): State<hub::Hub>,
    Query(query): Query<EventQuery>,
    _headers: HeaderMap,
) -> Response {
    let mut items = hub.events(query.node_id.as_deref()).await;
    if let Some(event_type) = query.event_type.as_deref() {
        items.retain(|item| item.event.event_type == event_type);
    }
    if let Some(stream_id) = query.stream_id.as_deref() {
        items.retain(|item| item.event.stream_id.as_deref() == Some(stream_id));
    }
    if let Some(correlation_id) = query.correlation_id.as_deref() {
        items.retain(|item| item.event.correlation_id.as_deref() == Some(correlation_id));
    }
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip(page.saturating_sub(1).saturating_mul(page_size))
            .take(page_size)
            .collect::<Vec<_>>(),
        page,
        page_size,
        total,
    })
    .into_response()
}

pub(super) async fn hub_event_stream(
    State(hub): State<hub::Hub>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> Response {
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let mut replay = VecDeque::new();
    let mut existing = hub.events(query.node_id.as_deref()).await;
    existing.retain(|event| event_matches(event, &query));
    if let Some(last_event_id) = last_event_id {
        let last_event_id = last_event_id.min(i64::MAX as u64) as i64;
        let oldest_id = existing.iter().filter_map(|event| event.event_id).min();
        if oldest_id.is_some_and(|event_id| last_event_id < event_id.saturating_sub(1)) {
            replay.push_back(None);
        }
        let mut pending = existing
            .into_iter()
            .filter(|event| {
                event
                    .event_id
                    .is_some_and(|event_id| event_id > last_event_id)
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|event| event.event_id);
        replay.extend(pending.into_iter().map(Some));
    }
    let receiver = hub.subscribe();
    let stream = futures_util::stream::unfold(
        (replay, receiver, query),
        |(mut replay, mut receiver, query)| async move {
            if let Some(update) = replay.pop_front() {
                let event = update
                    .map(|update| {
                        SseEvent::default()
                            .id(update
                                .event_id
                                .unwrap_or(update.event.occurred_at_ms as i64)
                                .to_string())
                            .event(update.event.event_type.clone())
                            .json_data(update)
                            .unwrap_or_else(|_| SseEvent::default().event("resync").data("{}"))
                    })
                    .unwrap_or_else(|| SseEvent::default().event("resync").data("{}"));
                return Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
            }
            loop {
                match receiver.recv().await {
                    Ok(update) if event_matches(&update, &query) => {
                        let event = SseEvent::default()
                            .id(update
                                .event_id
                                .unwrap_or(update.event.occurred_at_ms as i64)
                                .to_string())
                            .event(update.event.event_type.clone())
                            .json_data(update)
                            .unwrap_or_else(|_| SseEvent::default().event("resync").data("{}"));
                        break Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let event = SseEvent::default().event("resync").data("{}");
                        break Some((Ok::<_, Infallible>(event), (replay, receiver, query)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break None,
                }
            }
        },
    );
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

pub(super) fn event_matches(event: &hub::HubEvent, query: &EventQuery) -> bool {
    query
        .node_id
        .as_deref()
        .is_none_or(|node_id| event.node_id == node_id)
        && query
            .event_type
            .as_deref()
            .is_none_or(|event_type| event.event.event_type == event_type)
        && query
            .stream_id
            .as_deref()
            .is_none_or(|stream_id| event.event.stream_id.as_deref() == Some(stream_id))
        && query
            .correlation_id
            .as_deref()
            .is_none_or(|correlation_id| {
                event.event.correlation_id.as_deref() == Some(correlation_id)
            })
}

pub(super) async fn hub_audit(
    State(hub): State<hub::Hub>,
    Query(query): Query<AuditQuery>,
    _headers: HeaderMap,
) -> Response {
    match hub.audit(query.resource_id.as_deref()).await {
        Ok(records) => Json(page_items(
            records,
            &PageQuery {
                page: query.page,
                page_size: query.page_size,
                node_id: None,
            },
        ))
        .into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn events(
    State(cp): State<ControlPlane>,
    Query(query): Query<EventQuery>,
) -> Json<Page<arkflow_core::control::ControlEvent>> {
    let mut items = cp.events().await;
    if let Some(value) = query.event_type {
        items.retain(|item| item.event_type == value);
    }
    if let Some(value) = query.stream_id {
        items.retain(|item| item.stream_id.as_deref() == Some(value.as_str()));
    }
    if let Some(value) = query.correlation_id {
        items.retain(|item| item.correlation_id.as_deref() == Some(value.as_str()));
    }
    items.sort_by_key(|event| std::cmp::Reverse(event.occurred_at_ms));
    let total = items.len();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(50).clamp(1, 100);
    Json(Page {
        items: items
            .into_iter()
            .skip(page.saturating_sub(1).saturating_mul(page_size))
            .take(page_size)
            .collect(),
        page,
        page_size,
        total,
    })
}
