/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Distributed Acknowledgment Initialization
//!
//! This module provides initialization functions for distributed acknowledgment components.

use crate::Error;

/// Initialize all distributed acknowledgment components
pub fn init_distributed_ack_components() -> Result<(), Error> {
    // Register distributed acknowledgment input builder
    if let Err(e) = crate::input::distributed_ack_input::register_distributed_ack_input_builder() {
        log::error!("Failed to register distributed ack input builder: {}", e);
        return Err(e);
    }

    // Register distributed acknowledgment processor builder
    if let Err(e) =
        crate::processor::distributed_ack_processor::register_distributed_ack_processor_builder()
    {
        log::error!(
            "Failed to register distributed ack processor builder: {}",
            e
        );
        return Err(e);
    }

    log::info!("Distributed acknowledgment components initialized successfully");
    Ok(())
}

/// Initialize distributed acknowledgment components with logging
pub fn init_with_logging() -> Result<(), Error> {
    println!("Initializing distributed acknowledgment components...");

    match init_distributed_ack_components() {
        Ok(_) => {
            println!("✓ Distributed acknowledgment input registered as 'distributed_ack_input'");
            println!(
                "✓ Distributed acknowledgment processor registered as 'distributed_ack_processor'"
            );
            println!("✓ Stream configuration supports 'distributed_ack' field");
            println!("✓ Distributed acknowledgment system ready for use");
            Ok(())
        }
        Err(e) => {
            println!(
                "✗ Failed to initialize distributed acknowledgment components: {}",
                e
            );
            Err(e)
        }
    }
}
