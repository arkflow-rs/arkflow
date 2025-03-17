//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use std::sync::OnceLock;

pub mod file;
pub mod generate;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;
pub mod sql;

lazy_static::lazy_static! {
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        file::init();
        generate::init();
        http::init();
        kafka::init();
        memory::init();
        mqtt::init();
        sql::init();
    });
}
