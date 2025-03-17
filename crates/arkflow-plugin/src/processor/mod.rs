//! Processor component module
//!
//! The processor component is responsible for transforming, filtering, enriching, and so on.

use std::sync::OnceLock;

pub mod batch;
pub mod json;
pub mod protobuf;
pub mod sql;
mod udf;

lazy_static::lazy_static! {
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        batch::init();
        json::init();
        protobuf::init();
        sql::init();
    });
}
