//! Shared models, drivers and standalone multi-device components. Configuration,
//! port workers, registry and router are not connected to executable startup.

pub mod command_router;
pub mod config;
pub mod device_registry;
pub mod driver;
pub mod model;
pub mod pending;
pub mod port_worker;
