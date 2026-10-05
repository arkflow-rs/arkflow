//! Job CRUD, lifecycle actions, checkpoints/savepoints, and version upgrades
//! on the Hub API.
use super::{hub_problem, problem, problem_with_details, require_operator_action};
use crate::api_contract::{
    CreateJobRequest, JobDesiredStateRequest, JobUpgradeActionRequest, JobUpgradeRequest,
    OperatorAction, ValidateJobRequest,
};
use crate::hub;
use crate::storage::JobRecord;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub(super) async fn hub_jobs(State(hub): State<hub::Hub>, _headers: HeaderMap) -> Response {
    match hub.jobs().await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => hub_problem(error),
    }
}

/// Validate a Job through the same component, state-backend, and graph
/// construction path used by local execution. The HTTP Hub is also used
/// directly in tests and embedded deployments, so initialize the built-in
/// catalogue here as well as in `serve_hub`.
pub(crate) fn deep_validate_job(spec: &arkflow_core::job::JobSpec) -> Result<(), String> {
    arkflow_plugin::initialize()
        .and_then(|_| arkflow_core::executor::job_runner_adapter::validate_local_job(spec))
        .map_err(|error| error.to_string())
}

pub(super) async fn hub_job(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.job(&job_id).await {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_plan(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    let Some(job) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let spec: arkflow_core::job::JobSpec = match serde_json::from_str(&job.spec_json) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_persisted_job",
                error.to_string(),
            )
        }
    };
    match arkflow_core::job::JobPlan::compile(spec) {
        Ok(plan) => {
            Json(serde_json::json!({ "job_id": job.job_id, "version": job.version, "plan": plan }))
                .into_response()
        }
        Err(error) => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_plan",
            error.to_string(),
        ),
    }
}

pub(super) async fn hub_job_action(
    State(hub): State<hub::Hub>,
    Path((job_id, action)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let state = match action.as_str() {
        "start" | "restart" => "running",
        "stop" => "stopped",
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_action",
                "action must be start, stop, or restart".into(),
            )
        }
    };
    let current = match hub.job(&job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("Unknown Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    };
    match hub
        .update_job_desired_state(&job_id, state, current.generation)
        .await
    {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_checkpoint(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_job_recovery_artifact(hub, job_id, headers, "checkpoint").await
}

pub(super) async fn hub_job_savepoint(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    hub_job_recovery_artifact(hub, job_id, headers, "savepoint").await
}

pub(super) async fn hub_job_checkpoints(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    match hub.job_checkpoints(&job_id).await {
        Ok(records) => Json(records).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_validate_job(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<ValidateJobRequest>,
) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Configure, "job", None).await
    {
        return response;
    }
    let spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if let Err(error) = spec.validate() {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_spec",
            error.to_string(),
        );
    }
    let plan = match arkflow_core::job::JobPlan::compile(spec.clone()) {
        Ok(plan) => plan,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_job_plan",
                error.to_string(),
            )
        }
    };
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
    }
    let nodes = hub.nodes().await;
    let candidates = if request.node_ids.is_empty() {
        nodes.clone()
    } else {
        nodes
            .into_iter()
            .filter(|node| request.node_ids.iter().any(|id| id == &node.id))
            .collect::<Vec<_>>()
    };
    let required = ["job_runtime", "state_backend"];
    let compatibility = candidates
        .iter()
        .map(|node| {
            let missing = required
                .iter()
                .filter(|capability| !node.capabilities.iter().any(|item| item == **capability))
                .map(|capability| (*capability).to_owned())
                .collect::<Vec<_>>();
            let online = matches!(node.state, hub::NodeConnectionState::Online);
            serde_json::json!({
                "node_id": node.id,
                "state": node.state,
                "capabilities": node.capabilities,
                "compatible": online && missing.is_empty(),
                "missing_capabilities": if online { missing } else { vec!["online_lease".to_owned()] },
            })
        })
        .collect::<Vec<_>>();
    let compatible = compatibility
        .iter()
        .all(|node| node["compatible"].as_bool().unwrap_or(false));
    Json(serde_json::json!({
        "valid": compatible || compatibility.is_empty(),
        "plan": plan,
        "required_capabilities": required,
        "nodes": compatibility,
        "warnings": if compatibility.is_empty() { vec!["No online compute nodes selected".to_owned()] } else { Vec::new() },
    }))
    .into_response()
}

pub(super) async fn hub_job_detail(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    let Some(job) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let spec: arkflow_core::job::JobSpec = match serde_json::from_str(&job.spec_json) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_persisted_job",
                error.to_string(),
            )
        }
    };
    let plan = match arkflow_core::job::JobPlan::compile(spec) {
        Ok(plan) => plan,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_job_plan",
                error.to_string(),
            )
        }
    };
    let nodes = hub.nodes().await;
    let selected_nodes = if job.node_ids.is_empty() {
        nodes.clone()
    } else {
        nodes
            .into_iter()
            .filter(|node| job.node_ids.iter().any(|id| id == &node.id))
            .collect::<Vec<_>>()
    };
    // A placement that violates its own strategy (for example a split Job
    // whose side edges would cross nodes) must render as a detail-page error,
    // not a handler panic.
    let assignments = match plan.assignments_for_nodes(
        &selected_nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<Vec<_>>(),
        job.generation,
    ) {
        Ok(assignments) => assignments,
        Err(error) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_placement",
                error.to_string(),
            )
        }
    };
    let operations = hub
        .operations(None)
        .await
        .into_iter()
        .filter(|operation| operation.resource_id == job_id)
        .collect::<Vec<_>>();
    let checkpoints = match hub.job_checkpoints(&job_id).await {
        Ok(checkpoints) => checkpoints,
        Err(error) => return hub_problem(error),
    };
    // Observed runtime state: tasks the executing nodes report running, and
    // diagnostics scoped to this Job only (never fleet-wide sums).
    let observed_tasks = hub.observed_job_tasks(&job_id).await;
    let tasks = assignments
        .into_iter()
        .map(|attempt| {
            let mut value = serde_json::to_value(attempt).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                let task_id = object
                    .get("task_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                match observed_tasks.get(&task_id) {
                    Some(node_id) => {
                        object.insert("state".into(), serde_json::json!("running"));
                        object.insert("observed".into(), serde_json::json!(true));
                        object.insert("observed_node_id".into(), serde_json::json!(node_id));
                    }
                    None => {
                        object.insert("observed".into(), serde_json::json!(false));
                    }
                }
            }
            value
        })
        .collect::<Vec<_>>();
    let metrics = hub.job_detail_metrics(&job_id).await;
    Json(serde_json::json!({
        "job": job,
        "plan": plan,
        "tasks": tasks,
        "nodes": selected_nodes,
        "operations": operations,
        "checkpoints": checkpoints,
        // The high-frequency detail view omits the (potentially large)
        // target spec; the dedicated orchestration status endpoint keeps it.
        "active_upgrade": hub
            .active_job_upgrade_for(&job_id)
            .await
            .and_then(|record| {
                serde_json::to_value(&record).ok().map(|mut value| {
                    if let Some(object) = value.as_object_mut() {
                        object.remove("target_spec_json");
                    }
                    value
                })
            }),
        "metrics": metrics
    }))
    .into_response()
}

pub(super) async fn hub_job_versions(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    _headers: HeaderMap,
) -> Response {
    if matches!(hub.job(&job_id).await, Ok(None)) {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    }
    match hub.job_versions(&job_id).await {
        Ok(versions) => Json(versions).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_upgrade(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<JobUpgradeRequest>,
) -> Response {
    let principal = match require_operator_action(
        &hub,
        &headers,
        OperatorAction::Configure,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    if current.generation != request.expected_generation {
        return problem_with_details(
            StatusCode::CONFLICT,
            "generation_conflict",
            "Job changed while the upgrade was being prepared".into(),
            Some(
                serde_json::json!({"expected": request.expected_generation, "current": current.generation}),
            ),
        );
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let mode = request.mode.as_deref().unwrap_or("stopped");
    if !matches!(mode, "stopped" | "atomic") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_upgrade_mode",
            "mode must be stopped or atomic".into(),
        );
    }
    if mode == "atomic" {
        // Atomic mode is the inverse precondition: the Job must still be
        // running, and the orchestration takes its own savepoint.
        if current.desired_state != "running" {
            return problem(
                StatusCode::CONFLICT,
                "job_must_be_running",
                "The atomic upgrade mode requires a running Job".into(),
            );
        }
        let mut spec: arkflow_core::job::JobSpec =
            match serde_json::from_value(request.spec.clone()) {
                Ok(spec) => spec,
                Err(error) => {
                    return problem(
                        StatusCode::BAD_REQUEST,
                        "invalid_job_spec",
                        error.to_string(),
                    )
                }
            };
        return match hub
            .create_job_upgrade(
                &job_id,
                &mut spec,
                request.expected_generation,
                request.verify_timeout_ms,
                Some(principal.id.clone()),
                None,
            )
            .await
        {
            Ok(record) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "upgrade_id": record.upgrade_id,
                    "state": record.phase,
                    "savepoint_id": serde_json::Value::Null,
                    "job": current,
                })),
            )
                .into_response(),
            Err(hub::HubError::GenerationConflict { expected, current }) => problem(
                StatusCode::PRECONDITION_FAILED,
                "generation_conflict",
                format!("Expected generation {expected}, current generation {current}"),
            ),
            Err(hub::HubError::OrchestrationInProgress) => problem(
                StatusCode::CONFLICT,
                "orchestration_in_progress",
                "An atomic upgrade already owns this Job".into(),
            ),
            Err(error) => hub_problem(error),
        };
    }
    if current.desired_state != "stopped" || current.observed_state == "running" {
        return problem(
            StatusCode::CONFLICT,
            "job_must_be_stopped",
            "Stop and converge the current Job before upgrading".into(),
        );
    }
    let mut spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if spec.id.as_str() != job_id {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_spec",
            "upgrade spec id must match the Job id".into(),
        );
    }
    if spec.version.0 <= current.version {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_version",
            "upgrade version must be greater than the current version".into(),
        );
    }
    if let Err(error) = spec
        .validate()
        .and_then(|_| arkflow_core::job::JobPlan::compile(spec.clone()).map(|_| ()))
    {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_plan",
            error.to_string(),
        );
    }
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
    }
    let checkpoint = match hub.job_checkpoints(&job_id).await.map(|records| {
        records.into_iter().find(|record| {
            Some(record.checkpoint_id.as_str()) == request.savepoint_id.as_deref()
                && record.kind == "savepoint"
                && record.status == "completed"
        })
    }) {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            return problem(
                StatusCode::CONFLICT,
                "savepoint_not_ready",
                "The selected savepoint is not completed".into(),
            )
        }
        Err(error) => return hub_problem(error),
    };
    let format_version = spec
        .state
        .as_ref()
        .map(|state| state.format_version)
        .unwrap_or(1);
    if checkpoint.format_version != format_version {
        return problem(
            StatusCode::CONFLICT,
            "state_format_incompatible",
            "The savepoint state format is incompatible with the new Job version".into(),
        );
    }
    spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
    let upgraded = JobRecord {
        job_id: job_id.clone(),
        version: spec.version.0,
        spec_json: serde_json::to_string(&spec).unwrap_or_else(|_| request.spec.to_string()),
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending_recovery".into(),
        generation: current.generation,
        node_ids: if request.node_ids.is_empty() {
            current.node_ids.clone()
        } else {
            request.node_ids
        },
        checkpoint_id: Some(checkpoint.checkpoint_id.clone()),
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    // The generation fence covers the whole read-validate-write sequence: a
    // concurrent mutation that bumped the generation must surface as a
    // conflict instead of being silently overwritten by this older read.
    match hub
        .update_job_with_expected_generation(upgraded, request.expected_generation)
        .await
    {
        Ok(job) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "upgrade_id": format!("upgrade-{}", hub::now_ms_for_metrics()),
                "state": "pending_recovery",
                "savepoint_id": checkpoint.checkpoint_id,
                "job": job,
            })),
        )
            .into_response(),
        Err(hub::HubError::GenerationConflict { expected, current }) => problem(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_upgrade_rollback(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let versions = match hub.job_versions(&job_id).await {
        Ok(versions) => versions,
        Err(error) => return hub_problem(error),
    };
    // The console requests a specific version ("restore-v{N}"); honour it
    // instead of always stepping back to the immediately previous one.
    // An opaque upgrade id falls back to the previous-version semantics.
    let requested = upgrade_id
        .trim()
        .trim_start_matches("restore-")
        .trim_start_matches('v')
        .parse::<u64>()
        .ok();
    let previous = match requested {
        Some(target) => versions
            .into_iter()
            .find(|version| version.version == target)
            .filter(|version| version.version < current.version),
        None => versions
            .into_iter()
            .find(|version| version.version < current.version),
    };
    let Some(previous) = previous else {
        return problem(
            StatusCode::CONFLICT,
            "no_previous_job_version",
            match requested {
                Some(target) => format!(
                    "Job {job_id} has no restorable version {target} below the current version {}",
                    current.version
                ),
                None => "No previous Job version is available for recovery".into(),
            },
        );
    };
    // Apply the same state-format compatibility check the upgrade path
    // performs, against the artifact this Job would actually restore (its
    // current recovery pointer). Without it a rollback to a version whose
    // state layout differs is accepted, and recovery then silently discards
    // the incompatible artifact and starts the Job without state.
    if let Some(checkpoint_id) = current.checkpoint_id.as_deref() {
        let artifact_format = hub
            .job_checkpoints(&job_id)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .find(|record| record.checkpoint_id == checkpoint_id)
                    .map(|record| record.format_version)
            })
            .map_err(hub_problem);
        let artifact_format = match artifact_format {
            Ok(format) => format,
            Err(response) => return response,
        };
        let restored_format =
            serde_json::from_str::<arkflow_core::job::JobSpec>(&previous.spec_json)
                .ok()
                .and_then(|spec| spec.state.map(|state| state.format_version))
                .unwrap_or(1);
        let compatible = artifact_format == Some(restored_format);
        if !compatible {
            return problem(
                StatusCode::CONFLICT,
                "state_format_incompatible",
                format!(
                    "Job {job_id} cannot roll back to version {}: its state format is incompatible with the artifact the Job would restore",
                    previous.version
                ),
            );
        }
    }
    let restored_spec_json =
        match serde_json::from_str::<arkflow_core::job::JobSpec>(&previous.spec_json) {
            Ok(mut spec) => {
                spec.recovery = arkflow_core::job::RecoveryPolicy::LatestSavepoint;
                if let Err(error) = spec
                    .validate()
                    .and_then(|_| arkflow_core::job::JobPlan::compile(spec.clone()).map(|_| ()))
                {
                    return problem(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "invalid_job_plan",
                        error.to_string(),
                    );
                }
                if let Err(error) = deep_validate_job(&spec) {
                    return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid_job_plan", error);
                }
                match serde_json::to_string(&spec) {
                    Ok(spec_json) => spec_json,
                    Err(error) => {
                        return problem(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "invalid_persisted_job",
                            error.to_string(),
                        )
                    }
                }
            }
            Err(error) => {
                return problem(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid_persisted_job",
                    error.to_string(),
                )
            }
        };
    let restored = JobRecord {
        job_id: job_id.clone(),
        version: previous.version,
        spec_json: restored_spec_json,
        desired_state: "stopped".into(),
        observed_state: "stopped".into(),
        convergence: "pending_recovery".into(),
        generation: current.generation,
        node_ids: current.node_ids,
        checkpoint_id: current.checkpoint_id,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    // Same generation fence as the upgrade path: the version list may have
    // been read before a concurrent mutation bumped the generation.
    match hub
        .update_job_with_expected_generation(restored, current.generation)
        .await
    {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(hub::HubError::GenerationConflict { expected, current }) => problem(
            StatusCode::PRECONDITION_FAILED,
            "generation_conflict",
            format!("Expected generation {expected}, current generation {current}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_upgrade_status(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Read,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    match hub.job_upgrade(&upgrade_id).await {
        Ok(Some(record)) if record.job_id == job_id => Json(record).into_response(),
        Ok(Some(_)) | Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_upgrade_not_found",
            format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_upgrade_action(
    State(hub): State<hub::Hub>,
    Path((job_id, upgrade_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<JobUpgradeActionRequest>,
) -> Response {
    // Lifecycle-level actions align with job start/stop (Operate); rollback
    // swaps the Job's version and keeps the Configure level of the upgrade
    // endpoints proper.
    let action_level = match request.action.as_str() {
        "pause" | "resume" | "cancel" => OperatorAction::Operate,
        "rollback" => OperatorAction::Configure,
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_upgrade_action",
                "action must be pause, resume, cancel, or rollback".into(),
            )
        }
    };
    let principal =
        match require_operator_action(&hub, &headers, action_level, "job", Some(job_id.clone()))
            .await
        {
            Ok(principal) => principal,
            Err(response) => return response,
        };
    // Resolve before acting so an unknown id is a 404, not the generic
    // action-rejected conflict.
    match hub.job_upgrade(&upgrade_id).await {
        Ok(Some(record)) if record.job_id != job_id => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_upgrade_not_found",
                format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
            )
        }
        Ok(Some(_)) => {}
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_upgrade_not_found",
                format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    }
    match hub
        .act_job_upgrade(
            &upgrade_id,
            &request.action,
            Some(principal.id),
            None,
        )
        .await
    {
        Ok(record) if record.job_id == job_id => Json(record).into_response(),
        Ok(_) => problem(
            StatusCode::NOT_FOUND,
            "job_upgrade_not_found",
            format!("Unknown upgrade {upgrade_id} for Job {job_id}"),
        ),
        Err(hub::HubError::OrchestrationPhaseConflict) => problem(
            StatusCode::CONFLICT,
            "orchestration_conflict",
            "The upgrade phase changed while the action was being applied; retry against the fresh phase".into(),
        ),
        Err(hub::HubError::Invalid(message)) => problem(
            StatusCode::CONFLICT,
            "job_upgrade_action_rejected",
            message,
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_recovery_artifact(
    hub: hub::Hub,
    job_id: String,
    headers: HeaderMap,
    kind: &str,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    let Some(current) = (match hub.job(&job_id).await {
        Ok(job) => job,
        Err(error) => return hub_problem(error),
    }) else {
        return problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        );
    };
    let id = format!(
        "{kind}-{}-{}",
        current.generation,
        hub::now_ms_for_metrics()
    );
    let record = crate::storage::JobCheckpointRecord {
        job_id: job_id.clone(),
        job_version: current.version,
        checkpoint_id: id.clone(),
        kind: kind.into(),
        status: "pending".into(),
        manifest_uri: None,
        format_version: serde_json::from_str::<arkflow_core::job::JobSpec>(&current.spec_json)
            .ok()
            .and_then(|spec| spec.state.map(|state| state.format_version))
            .unwrap_or(1),
        created_at_ms: hub::now_ms_for_metrics(),
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    match hub.record_job_checkpoint(record).await {
        Ok(Some(job)) => {
            // The operator-triggered recovery artifact is the audited
            // mutation; periodic scheduling and the dispatch funnel are
            // mechanics and stay out of the audit trail.
            hub.record_job_operation_audit(
                if kind == "savepoint" {
                    "job_savepoint"
                } else {
                    "job_checkpoint"
                },
                &job_id,
                None,
                None,
                "accepted",
                None,
                format!(
                    "{kind} trigger accepted, checkpoint_id={id}, generation={}",
                    current.generation
                ),
            )
            .await;
            (StatusCode::ACCEPTED, Json(job)).into_response()
        }
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_create_job(
    State(hub): State<hub::Hub>,
    headers: HeaderMap,
    Json(request): Json<CreateJobRequest>,
) -> Response {
    if let Err(response) =
        require_operator_action(&hub, &headers, OperatorAction::Configure, "job", None).await
    {
        return response;
    }
    let spec: arkflow_core::job::JobSpec = match serde_json::from_value(request.spec.clone()) {
        Ok(spec) => spec,
        Err(error) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_job_spec",
                error.to_string(),
            )
        }
    };
    if let Err(error) = spec.validate() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_spec",
            error.to_string(),
        );
    }
    if let Err(error) = arkflow_core::job::JobPlan::compile(spec.clone()) {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_plan",
            error.to_string(),
        );
    }
    if let Err(error) = deep_validate_job(&spec) {
        return problem(StatusCode::BAD_REQUEST, "invalid_job_plan", error);
    }
    if !matches!(request.desired_state.as_str(), "stopped" | "running") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_state",
            "desired_state must be stopped or running".into(),
        );
    }
    let job = JobRecord {
        job_id: spec.id.to_string(),
        version: spec.version.0,
        spec_json: serde_json::to_string(&request.spec).unwrap_or_else(|_| "{}".into()),
        desired_state: request.desired_state,
        observed_state: "validated".into(),
        convergence: "pending".into(),
        generation: 1,
        node_ids: request.node_ids,
        checkpoint_id: None,
        last_error: None,
        updated_at_ms: hub::now_ms_for_metrics(),
    };
    match hub.upsert_job(job).await {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(error) => hub_problem(error),
    }
}

pub(super) async fn hub_job_desired_state(
    State(hub): State<hub::Hub>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<JobDesiredStateRequest>,
) -> Response {
    if let Err(response) = require_operator_action(
        &hub,
        &headers,
        OperatorAction::Operate,
        "job",
        Some(job_id.clone()),
    )
    .await
    {
        return response;
    }
    if !matches!(request.state.as_str(), "stopped" | "running") {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_job_state",
            "state must be stopped or running".into(),
        );
    }
    if let Err(response) = reject_if_job_upgrade_active(&hub, &job_id).await {
        return response;
    }
    let current = match hub.job(&job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return problem(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("Unknown Job {job_id}"),
            )
        }
        Err(error) => return hub_problem(error),
    };
    match hub
        .update_job_desired_state(&job_id, request.state.as_str(), current.generation)
        .await
    {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("Unknown Job {job_id}"),
        ),
        Err(error) => hub_problem(error),
    }
}

/// Reject job-level mutations while an atomic upgrade orchestration owns the
/// Job. `Ok(())` when no orchestration is active.
// Err carries a ready-made axum rejection response (middleware idiom).
#[allow(clippy::result_large_err)]
pub(super) async fn reject_if_job_upgrade_active(
    hub: &hub::Hub,
    job_id: &str,
) -> Result<(), Response> {
    if hub.active_job_upgrade_for(job_id).await.is_some() {
        return Err(problem(
            StatusCode::CONFLICT,
            "orchestration_in_progress",
            format!("Job {job_id} is owned by an active atomic upgrade"),
        ));
    }
    Ok(())
}
