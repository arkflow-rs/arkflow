//! ArkFlow's resource-oriented control-plane HTTP service.
//!
//! HTTP transport lives in [`api`]; the domain facade consumed by this crate
//! is `arkflow_core::control_plane::ControlPlane` and contains no Axum types.

pub mod agent;
pub mod api;
pub mod api_contract;
pub mod bootstrap;
pub mod hub;
pub mod metrics;
pub mod oidc;
pub mod storage;

pub use api::{
    hub_router, observability_router, router, serve, serve_hub, serve_observability, ServerConfig,
    API_VERSION,
};

// Root path kept alive for `hub::job_orchestration`.
pub(crate) use api::deep_validate_job;
// Root paths referenced by the `oidc` module's tests.
#[cfg(test)]
pub(crate) use api::{hub_oidc_callback, hub_oidc_login, hub_oidc_status};
