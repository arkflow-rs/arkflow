mod memory;
use arkflow_core::buffer::{Buffer, BufferBuilder};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

lazy_static::lazy_static! {
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        memory::init();
    });
}
